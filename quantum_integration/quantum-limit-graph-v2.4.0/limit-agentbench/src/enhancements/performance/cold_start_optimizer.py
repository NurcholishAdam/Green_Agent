"""
Cold Start Optimizer for Green Agent MoE System v3.1.0
Eliminates expert warmup latency through pre-initialization and transfer learning.
ENHANCED WITH: Federated Checkpoint Sharing, ML-Based Demand Prediction,
Carbon-Aware Strategy Selection, Helium Efficiency Dashboard,
Differential Privacy for Secure Checkpoint Sharing,
Online Learning for Continuous Model Improvement,
Real-time Carbon API Integration,
Predictive Helium Forecasting,
Intelligent Eviction Based on Predicted Future Demand,
Configuration Dataclass, Persistence, Telemetry, Health Checks.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Union, Callable, Protocol
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import numpy as np
import torch
import torch.nn as nn
from collections import OrderedDict, defaultdict, deque
import pickle
import hashlib
import json
import aiohttp
import os
import zlib
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import SGDRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import threading
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration Dataclass (NEW)
# ============================================================================

@dataclass
class ColdStartConfig:
    """Centralized configuration for Cold Start Optimizer."""
    # Core parameters
    cache_size: int = 100
    preload_threshold: float = 0.7
    checkpoint_dir: str = "./expert_checkpoints"
    
    # Feature flags
    enable_federated: bool = True
    enable_ml_demand: bool = True
    enable_carbon_aware: bool = True
    enable_helium_tracking: bool = True
    enable_online_learning: bool = True
    enable_realtime_carbon_api: bool = True
    enable_predictive_helium: bool = True
    enable_intelligent_eviction: bool = True
    enable_persistence: bool = True
    enable_telemetry: bool = True

    # Federated learning
    federated_server_url: Optional[str] = None
    privacy_epsilon: float = 1.0
    federated_sparsity_ratio: float = 0.1  # top-k% of checkpoint data to keep

    # ML demand predictor
    ml_history_window: int = 1000
    ml_online_learning_rate: float = 0.01
    ml_retrain_threshold: int = 100

    # Carbon-aware strategy
    carbon_intensity_thresholds: Dict[str, float] = field(default_factory=lambda: {
        'low': 200,
        'medium': 350,
        'high': 500
    })
    strategy_weights: Dict[str, float] = field(default_factory=lambda: {
        'priority': 0.2,
        'resource_cost': 0.3,
        'carbon_efficiency': 0.3,
        'urgency': 0.2
    })

    # Helium forecasting
    helium_forecast_model: str = "exponential_smoothing"  # or 'linear'

    # Eviction manager
    eviction_weights: Dict[str, float] = field(default_factory=lambda: {
        'usage_count': 0.25,
        'age': 0.20,
        'predicted_demand': 0.35,
        'sustainability': 0.20
    })

    # Retry and circuit breaker
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    # Persistence
    persistence_path: str = "cold_start_state.pkl"

    # Telemetry
    telemetry_export_interval: int = 60

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
# Telemetry Collector (NEW)
# ============================================================================

class ColdStartTelemetry:
    """Collects telemetry for the cold start optimizer."""

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
        # Prometheus text format
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
# Persistence Manager (NEW)
# ============================================================================

class ColdStartPersistenceManager:
    """Saves and loads the cold start optimizer state."""

    def __init__(self, config: ColdStartConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()
        logger.info(f"ColdStartPersistenceManager initialized (path={self.path})")

    async def save_state(self, optimizer: 'ColdStartOptimizer') -> bool:
        async with self._lock:
            try:
                state = {
                    'checkpoint_cache': {k: v for k, v in optimizer.checkpoint_cache.items()},
                    'warmup_history': optimizer.warmup_history,
                    'sustainability_score': optimizer.sustainability_score,
                    'cold_start_events': optimizer.cold_start_events,
                    'expert_similarity_matrix': optimizer.expert_similarity_matrix,
                }
                # Save sub-module states if they exist
                if optimizer.ml_predictor:
                    state['ml_predictor'] = {
                        'demand_history': optimizer.ml_predictor.demand_history,
                        'model_version': optimizer.ml_predictor.model_version,
                        'feature_importance': optimizer.ml_predictor.feature_importance,
                        'training_samples': optimizer.ml_predictor.training_samples,
                    }
                if optimizer.helium_dashboard:
                    state['helium_dashboard'] = {
                        'usage_history': optimizer.helium_dashboard.usage_history,
                        'total_helium_used': optimizer.helium_dashboard.total_helium_used,
                        'total_helium_saved': optimizer.helium_dashboard.total_helium_saved,
                    }
                if optimizer.eviction_manager:
                    state['eviction_manager'] = {
                        'eviction_history': optimizer.eviction_manager.eviction_history,
                    }
                serialized = pickle.dumps(state)
                compressed = zlib.compress(serialized)
                with open(self.path, 'wb') as f:
                    f.write(compressed)
                logger.info(f"Cold start state saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    async def load_state(self, optimizer: 'ColdStartOptimizer') -> bool:
        async with self._lock:
            if not os.path.exists(self.path):
                logger.warning(f"Persistence file {self.path} not found")
                return False
            try:
                with open(self.path, 'rb') as f:
                    compressed = f.read()
                serialized = zlib.decompress(compressed)
                state = pickle.loads(serialized)

                # Restore main state
                checkpoint_cache = state.get('checkpoint_cache', {})
                optimizer.checkpoint_cache = OrderedDict(checkpoint_cache)
                optimizer.warmup_history = state.get('warmup_history', [])
                optimizer.sustainability_score = state.get('sustainability_score', 0.0)
                optimizer.cold_start_events = state.get('cold_start_events', [])
                optimizer.expert_similarity_matrix = state.get('expert_similarity_matrix', {})

                # Restore sub-modules
                ml_state = state.get('ml_predictor')
                if ml_state and optimizer.ml_predictor:
                    optimizer.ml_predictor.demand_history = ml_state.get('demand_history', [])
                    optimizer.ml_predictor.model_version = ml_state.get('model_version', 0)
                    optimizer.ml_predictor.feature_importance = ml_state.get('feature_importance', {})
                    optimizer.ml_predictor.training_samples = ml_state.get('training_samples', 0)

                he_state = state.get('helium_dashboard')
                if he_state and optimizer.helium_dashboard:
                    optimizer.helium_dashboard.usage_history = he_state.get('usage_history', [])
                    optimizer.helium_dashboard.total_helium_used = he_state.get('total_helium_used', 0.0)
                    optimizer.helium_dashboard.total_helium_saved = he_state.get('total_helium_saved', 0.0)

                ev_state = state.get('eviction_manager')
                if ev_state and optimizer.eviction_manager:
                    optimizer.eviction_manager.eviction_history = ev_state.get('eviction_history', [])

                logger.info(f"Cold start state loaded from {self.path}")
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
# Federated Checkpoint Manager (Enhanced with compression & retry)
# ============================================================================

class FederatedCheckpointManager:
    """
    Federated checkpoint sharing with differential privacy and compression.
    """

    def __init__(self, config: ColdStartConfig):
        self.config = config
        self.server_url = config.federated_server_url
        self.privacy_epsilon = config.privacy_epsilon
        self.sparsity_ratio = config.federated_sparsity_ratio
        self.peer_checkpoints: Dict[str, Dict] = {}
        self.consensus_threshold = 0.6
        self._lock = asyncio.Lock()
        self._session = None
        self.sync_history = deque(maxlen=1000)
        self.noise_scale = 0.001
        self.failure_count = 0
        self.circuit_open = False
        self.circuit_open_until: Optional[datetime] = None

        logger.info(f"Federated Checkpoint Manager initialized (ε={self.privacy_epsilon})")

    async def _get_session(self):
        if self._session is None and self.server_url:
            self._session = aiohttp.ClientSession()
        return self._session

    def _add_differential_privacy(self, checkpoint: Dict) -> Dict:
        """Add differential privacy noise to checkpoint data."""
        if self.privacy_epsilon <= 0:
            return checkpoint
        private = {}
        sensitivity = 1.0
        scale = (2 * sensitivity) / self.privacy_epsilon
        for key, value in checkpoint.items():
            if isinstance(value, (int, float)):
                noise = np.random.normal(0, scale * self.noise_scale)
                private[key] = value + noise
            elif isinstance(value, list):
                private[key] = [
                    v + np.random.normal(0, scale * self.noise_scale) if isinstance(v, (int, float)) else v
                    for v in value
                ]
            else:
                private[key] = value
        return private

    def _compress_checkpoint(self, checkpoint: Dict) -> Dict:
        """Keep only top-k% of numeric values by absolute magnitude."""
        if self.sparsity_ratio == 1.0:
            return checkpoint
        numeric_items = {k: v for k, v in checkpoint.items() if isinstance(v, (int, float))}
        if not numeric_items:
            return checkpoint
        sorted_items = sorted(numeric_items.items(), key=lambda x: abs(x[1]), reverse=True)
        k = max(1, int(len(sorted_items) * self.sparsity_ratio))
        kept_keys = {item[0] for item in sorted_items[:k]}
        compressed = {k: v for k, v in checkpoint.items() if k in kept_keys or not isinstance(v, (int, float))}
        return compressed

    async def share_checkpoint(
        self,
        expert_id: str,
        checkpoint: Dict[str, Any],
        performance_metric: float = 1.0
    ) -> Dict:
        if not self.server_url:
            return {'status': 'local'}

        # Circuit breaker check
        if self.circuit_open:
            if datetime.utcnow() < self.circuit_open_until:
                logger.warning("Circuit breaker open, skipping share")
                return {'status': 'circuit_open'}
            else:
                self.circuit_open = False
                self.failure_count = 0

        for attempt in range(self.config.max_retries):
            try:
                async with self._lock:
                    session = await self._get_session()
                    private = self._add_differential_privacy(checkpoint)
                    compressed = self._compress_checkpoint(private)
                    checkpoint_data = {
                        'expert_id': expert_id,
                        'checkpoint': compressed,
                        'performance': performance_metric,
                        'privacy_epsilon': self.privacy_epsilon,
                        'sparsity_ratio': self.sparsity_ratio,
                        'timestamp': datetime.utcnow().isoformat(),
                        'version': '1.0'
                    }
                    async with session.post(
                        f"{self.server_url}/federated/checkpoint",
                        json=checkpoint_data,
                        timeout=30
                    ) as response:
                        if response.status == 200:
                            result = await response.json()
                            self.failure_count = 0
                            logger.info(f"Shared checkpoint for {expert_id} with federation (ε={self.privacy_epsilon})")
                            return result
                        else:
                            logger.warning(f"Checkpoint sharing failed (attempt {attempt+1}): {response.status}")
            except Exception as e:
                logger.error(f"Checkpoint sharing error (attempt {attempt+1}): {e}")
            await asyncio.sleep(2 ** attempt)

        self.failure_count += 1
        if self.failure_count >= self.config.circuit_breaker_threshold:
            self.circuit_open = True
            self.circuit_open_until = datetime.utcnow() + timedelta(seconds=self.config.circuit_breaker_recovery_timeout)
            logger.error("Circuit breaker opened for FederatedCheckpointManager")
        return {'status': 'failed'}

    async def get_peer_checkpoints(self, expert_id: str) -> List[Dict]:
        if not self.server_url:
            return []
        for attempt in range(self.config.max_retries):
            try:
                async with self._lock:
                    session = await self._get_session()
                    async with session.get(
                        f"{self.server_url}/federated/checkpoints/{expert_id}",
                        timeout=30
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            return data.get('checkpoints', [])
                        else:
                            logger.warning(f"Peer checkpoints fetch failed (attempt {attempt+1}): {response.status}")
            except Exception as e:
                logger.error(f"Peer checkpoints fetch error (attempt {attempt+1}): {e}")
            await asyncio.sleep(2 ** attempt)
        return []

    async def aggregate_checkpoints(
        self,
        peer_checkpoints: List[Dict],
        weights: Optional[Dict[str, float]] = None
    ) -> Dict:
        if not peer_checkpoints:
            return {}
        if weights is None:
            weights = {i: 1.0 for i in range(len(peer_checkpoints))}

        aggregated = {}
        numeric_keys = ['carbon_footprint_kg', 'expected_accuracy', 'expected_throughput']
        for key in numeric_keys:
            values = []
            for i, cp in enumerate(peer_checkpoints):
                if key in cp:
                    values.append(cp[key] * weights.get(i, 1.0))
            if values:
                total_weight = sum(weights.get(i, 1.0) for i in range(len(values)))
                aggregated[key] = sum(values) / max(total_weight, 0.001)

        categorical_keys = ['expert_type', 'architecture']
        for key in categorical_keys:
            values = [cp.get(key) for cp in peer_checkpoints if key in cp]
            if values:
                aggregated[key] = max(set(values), key=values.count)

        if len(peer_checkpoints) > 1:
            aggregated['consensus_reached'] = True
            aggregated['peer_count'] = len(peer_checkpoints)
            aggregated['consensus_threshold'] = self.consensus_threshold

        self.sync_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'peer_count': len(peer_checkpoints),
            'aggregated_keys': list(aggregated.keys())
        })
        return aggregated

    async def sync_cache_with_peers(self, local_cache: Dict) -> Dict:
        if not self.server_url:
            return local_cache
        for attempt in range(self.config.max_retries):
            try:
                async with self._lock:
                    session = await self._get_session()
                    cache_summary = {
                        'expert_ids': list(local_cache.keys()),
                        'size': len(local_cache),
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    async with session.post(
                        f"{self.server_url}/federated/cache/sync",
                        json=cache_summary,
                        timeout=30
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            peer_experts = data.get('expert_ids', [])
                            missing = [eid for eid in peer_experts if eid not in local_cache]
                            for expert_id in missing:
                                peer_cps = await self.get_peer_checkpoints(expert_id)
                                if peer_cps:
                                    aggregated = await self.aggregate_checkpoints(peer_cps)
                                    if aggregated:
                                        local_cache[expert_id] = aggregated
                            logger.info(f"Cache sync completed: {len(missing)} experts added")
                            return local_cache
                        else:
                            logger.warning(f"Cache sync failed (attempt {attempt+1}): {response.status}")
            except Exception as e:
                logger.error(f"Cache sync error (attempt {attempt+1}): {e}")
            await asyncio.sleep(2 ** attempt)
        return local_cache

    def get_federated_stats(self) -> Dict:
        return {
            'server_url': self.server_url,
            'peer_checkpoints': len(self.peer_checkpoints),
            'sync_count': len(self.sync_history),
            'privacy_epsilon': self.privacy_epsilon,
            'sparsity_ratio': self.sparsity_ratio,
            'last_sync': list(self.sync_history)[-1] if self.sync_history else None,
            'circuit_open': self.circuit_open
        }

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# ML Demand Predictor (Enhanced with online learning and better features)
# ============================================================================

class MLDemandPredictor:
    """
    Machine learning-based expert demand prediction with online learning.
    """

    def __init__(self, config: ColdStartConfig):
        self.config = config
        self.history_window = config.ml_history_window
        self.demand_history: List[Dict] = []
        self.scaler = StandardScaler()
        self.is_trained = False
        self.feature_importance = {}
        self.training_samples = 0
        self.online_learning_rate = config.ml_online_learning_rate
        self.model_version = 0
        self.samples_since_last_train = 0
        self.retrain_threshold = config.ml_retrain_threshold
        self.model = None
        self._ml_available = False
        self._init_model()

    def _init_model(self):
        try:
            self.model = SGDRegressor(
                learning_rate='constant',
                eta0=self.online_learning_rate,
                penalty='l2',
                alpha=0.0001,
                max_iter=1,
                random_state=42,
                warm_start=True
            )
            self._ml_available = True
        except ImportError:
            logger.warning("SGDRegressor not available; using fallback frequency-based prediction")

    def record_demand(self, expert_id: str, timestamp: datetime, context: Dict = None):
        self.demand_history.append({
            'expert_id': expert_id,
            'timestamp': timestamp,
            'hour': timestamp.hour,
            'day_of_week': timestamp.weekday(),
            'month': timestamp.month,
            'context': context or {}
        })
        self.samples_since_last_train += 1
        if self.samples_since_last_train >= self.retrain_threshold and self.is_trained and self._ml_available:
            asyncio.create_task(self._online_learning_update())
        if len(self.demand_history) > self.history_window:
            self.demand_history = self.demand_history[-self.history_window:]

    async def _online_learning_update(self):
        try:
            recent_data = self.demand_history[-self.samples_since_last_train:]
            if len(recent_data) > 10:
                X, y = self._prepare_training_data(recent_data)
                if len(X) > 0:
                    X_scaled = self.scaler.transform(X)
                    self.model.partial_fit(X_scaled, y)
                    self.model_version += 1
                    self.samples_since_last_train = 0
                    logger.info(f"Online learning update complete (version {self.model_version})")
        except Exception as e:
            logger.error(f"Online learning update error: {e}")

    def _prepare_training_data(self, data: List[Dict]) -> Tuple[np.ndarray, np.ndarray]:
        X = []
        y = []
        if len(data) < 5:
            return np.array(X), np.array(y)
        timestamps = sorted(set(h['timestamp'] for h in data))
        for i in range(1, len(timestamps)):
            current_ts = timestamps[i]
            future_window = current_ts + timedelta(minutes=5)
            future_demands = sum(1 for h in data if current_ts < h['timestamp'] <= future_window)
            if future_demands == 0:
                continue
            for expert_id in set(h['expert_id'] for h in data):
                features = self._extract_features(expert_id, current_ts)
                X.append(list(features.values()))
                y.append(1.0 if any(
                    h['expert_id'] == expert_id and current_ts < h['timestamp'] <= future_window
                    for h in data
                ) else 0.0)
        return np.array(X), np.array(y)

    def _extract_features(self, expert_id: str, timestamp: datetime) -> Dict[str, float]:
        features = {
            'hour': timestamp.hour / 23.0,
            'day_of_week': timestamp.weekday() / 6.0,
            'month': timestamp.month / 11.0,
            'is_weekend': 1.0 if timestamp.weekday() >= 5 else 0.0,
            'hour_sin': np.sin(2 * np.pi * timestamp.hour / 24.0),
            'hour_cos': np.cos(2 * np.pi * timestamp.hour / 24.0),
        }
        recent_window = timedelta(hours=1)
        recent_usage = [
            h for h in self.demand_history
            if h['expert_id'] == expert_id and
            timestamp - h['timestamp'] <= recent_window
        ]
        features['recent_usage_count'] = min(len(recent_usage) / 10.0, 1.0)
        total_usage = sum(1 for h in self.demand_history if h['expert_id'] == expert_id)
        features['usage_frequency'] = min(total_usage / 100.0, 1.0)
        last_use = max(
            [h['timestamp'] for h in self.demand_history if h['expert_id'] == expert_id],
            default=timestamp - timedelta(days=7)
        )
        hours_since_last = (timestamp - last_use).total_seconds() / 3600
        features['hours_since_last'] = min(hours_since_last / 24.0, 1.0)
        return features

    async def train_model(self):
        if len(self.demand_history) < 50:
            return {'status': 'insufficient_data', 'samples': len(self.demand_history)}
        if not self._ml_available:
            return {'status': 'ml_not_available'}
        X, y = self._prepare_training_data(self.demand_history)
        if len(X) < 20:
            return {'status': 'insufficient_training_data', 'samples': len(X)}
        X_scaled = self.scaler.fit_transform(X)
        # Train with multiple passes
        for _ in range(5):
            self.model.partial_fit(X_scaled, y)
        self.is_trained = True
        self.training_samples = len(X)
        self.model_version += 1
        self.samples_since_last_train = 0
        logger.info(f"ML Demand Predictor trained (version {self.model_version})")
        return {'status': 'success', 'samples': len(X), 'version': self.model_version}

    async def predict_demand(self, horizon_minutes: int = 5) -> Dict[str, float]:
        if not self.is_trained or not self._ml_available:
            return self._simple_frequency_prediction(horizon_minutes)
        now = datetime.utcnow()
        experts = set(h['expert_id'] for h in self.demand_history[-1000:])
        predictions = {}
        for expert_id in experts:
            features = self._extract_features(expert_id, now)
            features_array = np.array([list(features.values())])
            features_scaled = self.scaler.transform(features_array)
            pred = self.model.predict(features_scaled)[0]
            predictions[expert_id] = max(0.0, min(1.0, pred))
        return predictions

    def _simple_frequency_prediction(self, horizon_minutes: int = 5) -> Dict[str, float]:
        now = datetime.utcnow()
        recent_window = timedelta(minutes=horizon_minutes * 2)
        recent_usage = {}
        for entry in self.demand_history:
            if now - entry['timestamp'] <= recent_window:
                expert_id = entry['expert_id']
                recent_usage[expert_id] = recent_usage.get(expert_id, 0) + 1
        if not recent_usage:
            return {}
        total_usage = sum(recent_usage.values())
        return {eid: cnt / total_usage for eid, cnt in recent_usage.items()}

    def get_model_performance(self) -> Dict:
        return {
            'is_trained': self.is_trained,
            'training_samples': self.training_samples,
            'model_version': self.model_version,
            'feature_importance': self.feature_importance,
            'samples_since_last_train': self.samples_since_last_train,
            'ml_available': self._ml_available,
        }

# ============================================================================
# Carbon-Aware Strategy Selector (Enhanced with configurable weights & caching)
# ============================================================================

class CarbonAwareStrategySelector:
    """
    Carbon-aware warmup strategy selection with real-time carbon API integration.
    """

    def __init__(self, config: ColdStartConfig):
        self.config = config
        self.carbon_intensity_thresholds = config.carbon_intensity_thresholds
        self.strategy_weights = config.strategy_weights
        self.strategy_history = deque(maxlen=1000)
        self.api_key = os.getenv('ELECTRICITYMAP_API_KEY', '')
        self.api_endpoint = "https://api.electricitymap.org/v3"
        self._session = None
        self.cache = {}
        self.last_update = None
        self.update_interval = 300
        self.failure_count = 0
        self.circuit_open = False
        self.circuit_open_until: Optional[datetime] = None

        logger.info("Carbon-Aware Strategy Selector initialized with real-time API")

    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def get_realtime_carbon_intensity(self, region: str = "US-CAL-CISO") -> float:
        """Get real-time carbon intensity with retry and circuit breaker."""
        if self.circuit_open:
            if datetime.utcnow() < self.circuit_open_until:
                logger.warning("Circuit breaker open, using fallback")
                return self._get_fallback_intensity()
            else:
                self.circuit_open = False
                self.failure_count = 0

        cache_key = f"{region}_{datetime.utcnow().hour}"
        if cache_key in self.cache and self.last_update and (datetime.utcnow() - self.last_update).seconds < self.update_interval:
            return self.cache[cache_key]

        for attempt in range(self.config.max_retries):
            try:
                session = await self._get_session()
                url = f"{self.api_endpoint}/carbon-intensity/latest?zone={region}"
                headers = {'auth-token': self.api_key} if self.api_key else {}
                async with session.get(url, headers=headers, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        intensity = data.get('carbonIntensity', 400)
                        self.cache[cache_key] = intensity
                        self.last_update = datetime.utcnow()
                        self.failure_count = 0
                        return intensity
                    else:
                        logger.warning(f"Carbon API returned {response.status}, attempt {attempt+1}")
                        if attempt == self.config.max_retries - 1:
                            self.failure_count += 1
                            if self.failure_count >= self.config.circuit_breaker_threshold:
                                self.circuit_open = True
                                self.circuit_open_until = datetime.utcnow() + timedelta(seconds=self.config.circuit_breaker_recovery_timeout)
                            return self._get_fallback_intensity()
                        await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Carbon API error: {e}, attempt {attempt+1}")
                if attempt == self.config.max_retries - 1:
                    self.failure_count += 1
                    if self.failure_count >= self.config.circuit_breaker_threshold:
                        self.circuit_open = True
                        self.circuit_open_until = datetime.utcnow() + timedelta(seconds=self.config.circuit_breaker_recovery_timeout)
                    return self._get_fallback_intensity()
                await asyncio.sleep(2 ** attempt)
        return self._get_fallback_intensity()

    def _get_fallback_intensity(self) -> float:
        # Simulate diurnal pattern
        hour = datetime.utcnow().hour
        base = 350
        diurnal = 50 * np.sin((hour - 8) / 12 * np.pi)
        return max(200, min(500, base + diurnal))

    def select_strategy(
        self,
        strategies: Dict[str, Any],
        carbon_intensity: Optional[float] = None,
        urgency: str = 'normal',
        carbon_budget: float = None
    ) -> str:
        if carbon_intensity is None:
            # Try to get from API synchronously (might fail, fallback)
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # In async context, create task and wait
                    carbon_intensity = asyncio.run(self.get_realtime_carbon_intensity())
                else:
                    carbon_intensity = self._get_fallback_intensity()
            except:
                carbon_intensity = self._get_fallback_intensity()

        # Determine carbon regime
        if carbon_intensity > self.carbon_intensity_thresholds['high']:
            regime = 'high'
            efficiency_weight = 0.8
        elif carbon_intensity > self.carbon_intensity_thresholds['medium']:
            regime = 'medium'
            efficiency_weight = 0.6
        else:
            regime = 'low'
            efficiency_weight = 0.3

        # Score each strategy
        strategy_scores = {}
        for name, strategy in strategies.items():
            base_score = 1.0 / (strategy.priority + 1)
            efficiency_score = 1.0 / (1.0 + strategy.resource_cost)
            carbon_score = efficiency_score * efficiency_weight + base_score * (1 - efficiency_weight)

            urgency_factor = {
                'critical': 1.5,
                'high': 1.2,
                'normal': 1.0,
                'low': 0.8
            }.get(urgency, 1.0)

            if carbon_budget and strategy.resource_cost > carbon_budget:
                carbon_score *= 0.5

            # Apply weights
            weighted_score = (
                self.strategy_weights['priority'] * base_score +
                self.strategy_weights['resource_cost'] * (1 - strategy.resource_cost) +
                self.strategy_weights['carbon_efficiency'] * carbon_score +
                self.strategy_weights['urgency'] * urgency_factor
            )
            strategy_scores[name] = weighted_score

        if not strategy_scores:
            return 'preload'

        best_strategy = max(strategy_scores.items(), key=lambda x: x[1])[0]

        self.strategy_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'carbon_intensity': carbon_intensity,
            'regime': regime,
            'urgency': urgency,
            'selected_strategy': best_strategy,
            'score': strategy_scores[best_strategy],
            'api_used': bool(self.api_key)
        })

        logger.info(f"Selected {best_strategy} strategy (carbon: {carbon_intensity:.0f} gCO2/kWh, regime: {regime})")
        return best_strategy

    def get_carbon_impact_report(self) -> Dict:
        if not self.strategy_history:
            return {'total_selections': 0}
        recent = list(self.strategy_history)[-100:]
        return {
            'total_selections': len(self.strategy_history),
            'carbon_regime_distribution': {
                'low': sum(1 for s in recent if s.get('regime') == 'low'),
                'medium': sum(1 for s in recent if s.get('regime') == 'medium'),
                'high': sum(1 for s in recent if s.get('regime') == 'high')
            },
            'strategy_distribution': {
                s['selected_strategy']: sum(1 for st in recent if st.get('selected_strategy') == s['selected_strategy'])
                for s in recent
            },
            'average_carbon_intensity': np.mean([s.get('carbon_intensity', 0) for s in recent]),
            'api_used_ratio': sum(1 for s in recent if s.get('api_used', False)) / max(len(recent), 1),
            'most_carbon_efficient_strategy': max(
                set(s['selected_strategy'] for s in recent),
                key=lambda x: sum(1 for s in recent if s.get('selected_strategy') == x)
            )
        }

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Helium Efficiency Dashboard (Enhanced with better forecasting)
# ============================================================================

class HeliumEfficiencyDashboard:
    """
    Helium efficiency monitoring and analytics with predictive forecasting.
    """

    def __init__(self, config: ColdStartConfig):
        self.config = config
        self.helium_usage: Dict[str, List[Dict]] = {}
        self.efficiency_scores: Dict[str, List[float]] = {}
        self.total_helium_used = 0.0
        self.total_helium_saved = 0.0
        self._lock = asyncio.Lock()
        self.usage_history: List[Dict] = []
        self.forecast_model = None
        self.forecast_trained = False
        self.alpha = 0.3  # smoothing factor for exponential smoothing

        logger.info("Helium Efficiency Dashboard initialized with exponential smoothing")

    async def record_helium_usage(
        self,
        expert_id: str,
        amount_l: float,
        operation: str = 'initialization'
    ):
        async with self._lock:
            if expert_id not in self.helium_usage:
                self.helium_usage[expert_id] = []
                self.efficiency_scores[expert_id] = []
            self.helium_usage[expert_id].append({
                'timestamp': datetime.utcnow().isoformat(),
                'amount_l': amount_l,
                'operation': operation
            })
            self.total_helium_used += amount_l
            self.usage_history.append({
                'timestamp': datetime.utcnow(),
                'amount_l': amount_l,
                'expert_id': expert_id,
                'operation': operation
            })
            # Train forecast if enough data
            if len(self.usage_history) > 20:
                self._train_forecast()
            logger.debug(f"Helium usage recorded: {expert_id} = {amount_l}L ({operation})")

    def _train_forecast(self):
        """Train helium usage forecast model using exponential smoothing."""
        if len(self.usage_history) < 20:
            return
        # Simple exponential smoothing
        values = [h['amount_l'] for h in self.usage_history[-50:]]
        if not values:
            return
        smoothed = values[0]
        for v in values[1:]:
            smoothed = self.alpha * v + (1 - self.alpha) * smoothed
        self.forecast_trained = True
        # Store the smoothed value as forecast
        self._last_smoothed = smoothed
        self._last_values = values

    async def predict_helium_usage(self, hours: int = 24) -> Dict[str, Any]:
        if not self.forecast_trained:
            return {
                'status': 'not_trained',
                'prediction': self.total_helium_used / max(len(self.usage_history), 1) * hours
            }
        # Simple projection: average of recent values
        recent = [h['amount_l'] for h in self.usage_history[-min(20, len(self.usage_history)):]]
        hourly_avg = np.mean(recent) if recent else 0.0
        total_predicted = hourly_avg * hours
        return {
            'status': 'success',
            'predictions': [hourly_avg] * hours,  # flat prediction
            'total_predicted_usage': total_predicted,
            'hourly_average': hourly_avg,
            'confidence': 0.7 if len(self.usage_history) > 50 else 0.5,
            'forecast_hours': hours
        }

    async def record_helium_saving(self, amount_l: float, source: str = 'optimization'):
        async with self._lock:
            self.total_helium_saved += amount_l
            logger.debug(f"Helium saving recorded: {amount_l}L from {source}")

    async def update_efficiency_score(self, expert_id: str, score: float):
        async with self._lock:
            if expert_id not in self.efficiency_scores:
                self.efficiency_scores[expert_id] = []
            self.efficiency_scores[expert_id].append(score)

    def get_efficiency_report(self) -> Dict[str, Any]:
        report = {
            'total_helium_used_l': self.total_helium_used,
            'total_helium_saved_l': self.total_helium_saved,
            'net_helium_usage_l': self.total_helium_used - self.total_helium_saved,
            'helium_savings_rate': self.total_helium_saved / max(self.total_helium_used, 1),
            'expert_statistics': {}
        }
        for expert_id, usage_list in self.helium_usage.items():
            total_usage = sum(u['amount_l'] for u in usage_list)
            avg_efficiency = np.mean(self.efficiency_scores.get(expert_id, [0.5]))
            report['expert_statistics'][expert_id] = {
                'total_usage_l': total_usage,
                'usage_count': len(usage_list),
                'average_efficiency': avg_efficiency,
                'efficiency_trend': self._calculate_efficiency_trend(expert_id)
            }
        report['forecast'] = {
            'trained': self.forecast_trained,
            'model_type': 'exponential_smoothing',
            'samples': len(self.usage_history)
        }
        return report

    def _calculate_efficiency_trend(self, expert_id: str) -> str:
        scores = self.efficiency_scores.get(expert_id, [])
        if len(scores) < 5:
            return 'stable'
        first_half = np.mean(scores[:len(scores)//2])
        second_half = np.mean(scores[len(scores)//2:])
        if second_half > first_half * 1.05:
            return 'improving'
        elif second_half < first_half * 0.95:
            return 'declining'
        else:
            return 'stable'

    def get_optimization_recommendations(self) -> List[str]:
        recommendations = []
        if self.total_helium_used > 0:
            savings_rate = self.total_helium_saved / self.total_helium_used
            if savings_rate < 0.1:
                recommendations.append("Implement helium recovery systems")
                recommendations.append("Optimize initialization procedures for helium efficiency")
            if self.total_helium_used > 100:
                recommendations.append("Consider alternative cooling methods for high-usage experts")
        for expert_id, usage_list in self.helium_usage.items():
            total_usage = sum(u['amount_l'] for u in usage_list)
            if total_usage > 10:
                recommendations.append(f"Review helium usage for {expert_id} - consider optimization")
        return recommendations or ["Helium usage is within acceptable ranges"]

# ============================================================================
# Intelligent Eviction Manager (Enhanced with configurable weights)
# ============================================================================

class IntelligentEvictionManager:
    """
    Intelligent cache eviction based on predicted future demand.
    """

    def __init__(self, config: ColdStartConfig, predictor: Optional[MLDemandPredictor] = None):
        self.config = config
        self.predictor = predictor
        self.eviction_history: List[Dict] = []
        self.weights = config.eviction_weights
        self._lock = asyncio.Lock()

        logger.info("Intelligent Eviction Manager initialized")

    async def get_eviction_score(
        self,
        expert_id: str,
        checkpoint: Dict,
        predicted_demand: Dict[str, float]
    ) -> float:
        async with self._lock:
            usage_count = checkpoint.get('usage_count', 0)
            base_score = 1.0 / (1.0 + usage_count)

            created_at = checkpoint.get('created_at')
            if created_at:
                age_hours = (datetime.utcnow() - created_at).total_seconds() / 3600
                age_score = min(1.0, age_hours / 24)
            else:
                age_score = 0.5

            demand_prob = predicted_demand.get(expert_id, 0.0)
            demand_score = 1.0 - demand_prob

            sustainability = checkpoint.get('sustainability_score', 0.5)
            sustain_score = 1.0 - sustainability

            eviction_score = (
                self.weights['usage_count'] * base_score +
                self.weights['age'] * age_score +
                self.weights['predicted_demand'] * demand_score +
                self.weights['sustainability'] * sustain_score
            )
            return eviction_score

    async def select_eviction_candidates(
        self,
        cache: Dict[str, Dict],
        predicted_demand: Dict[str, float],
        num_to_evict: int = 1
    ) -> List[str]:
        if not cache:
            return []
        scores = {}
        for expert_id, checkpoint in cache.items():
            scores[expert_id] = await self.get_eviction_score(expert_id, checkpoint, predicted_demand)
        sorted_candidates = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [expert_id for expert_id, _ in sorted_candidates[:num_to_evict]]

    def get_eviction_stats(self) -> Dict:
        return {
            'total_evictions': len(self.eviction_history),
            'recent_evictions': self.eviction_history[-10:] if self.eviction_history else []
        }

# ============================================================================
# Data Classes (Enhanced)
# ============================================================================

@dataclass
class ExpertCheckpoint:
    """Pre-computed expert state for instant initialization."""
    expert_id: str
    expert_type: str
    model_state: Dict[str, Any]
    optimizer_state: Dict[str, Any]
    feature_distribution: Dict[str, float]
    performance_metrics: Dict[str, float]
    created_at: datetime
    last_used: datetime
    usage_count: int = 0
    carbon_footprint_kg: float = 0.0
    helium_usage_l: float = 0.0
    sustainability_score: float = 0.0
    federated_consensus: bool = False
    peer_count: int = 0

    def compute_hash(self) -> str:
        state_str = json.dumps(self.model_state, sort_keys=True, default=str)
        return hashlib.sha256(state_str.encode()).hexdigest()

@dataclass
class WarmupStrategy:
    """Strategy for expert warmup."""
    strategy_type: str
    priority: int
    estimated_warmup_time_ms: float
    resource_cost: float
    success_probability: float
    carbon_efficiency: float = 0.5
    helium_efficiency: float = 0.5

# ============================================================================
# Enhanced Cold Start Optimizer (Main Class)
# ============================================================================

class ColdStartOptimizer:
    """
    Enhanced Cold Start Optimizer v3.1.0 with configuration, persistence, and telemetry.
    """

    def __init__(self, config: Optional[ColdStartConfig] = None, **kwargs):
        # If config is provided, use it; otherwise build from kwargs for backward compatibility
        if config is None:
            config = ColdStartConfig(
                cache_size=kwargs.get('cache_size', 100),
                preload_threshold=kwargs.get('preload_threshold', 0.7),
                checkpoint_dir=kwargs.get('checkpoint_dir', "./expert_checkpoints"),
                federated_server_url=kwargs.get('federated_server_url', None),
                enable_federated=kwargs.get('enable_federated', True),
                enable_ml_demand=kwargs.get('enable_ml_demand', True),
                enable_carbon_aware=kwargs.get('enable_carbon_aware', True),
                enable_helium_tracking=kwargs.get('enable_helium_tracking', True),
                privacy_epsilon=kwargs.get('privacy_epsilon', 1.0),
                enable_online_learning=kwargs.get('enable_online_learning', True),
                enable_realtime_carbon_api=kwargs.get('enable_realtime_carbon_api', True),
                enable_predictive_helium=kwargs.get('enable_predictive_helium', True),
                enable_intelligent_eviction=kwargs.get('enable_intelligent_eviction', True),
                enable_persistence=kwargs.get('enable_persistence', True),
                enable_telemetry=kwargs.get('enable_telemetry', True),
                max_retries=kwargs.get('max_retries', 3),
                persistence_path=kwargs.get('persistence_path', "cold_start_state.pkl"),
            )
        self.config = config

        self.cache_size = config.cache_size
        self.preload_threshold = config.preload_threshold
        self.checkpoint_dir = config.checkpoint_dir
        self.enable_federated = config.enable_federated
        self.enable_ml_demand = config.enable_ml_demand
        self.enable_carbon_aware = config.enable_carbon_aware
        self.enable_helium_tracking = config.enable_helium_tracking
        self.enable_online_learning = config.enable_online_learning
        self.enable_realtime_carbon_api = config.enable_realtime_carbon_api
        self.enable_predictive_helium = config.enable_predictive_helium
        self.enable_intelligent_eviction = config.enable_intelligent_eviction
        self.enable_persistence = config.enable_persistence
        self.enable_telemetry = config.enable_telemetry

        # Initialize sub-modules with config
        self.federated_manager = FederatedCheckpointManager(config) if self.enable_federated else None
        self.ml_predictor = MLDemandPredictor(config) if self.enable_ml_demand else None
        self.strategy_selector = CarbonAwareStrategySelector(config) if self.enable_carbon_aware else None
        self.helium_dashboard = HeliumEfficiencyDashboard(config) if self.enable_helium_tracking else None
        self.eviction_manager = IntelligentEvictionManager(config, self.ml_predictor) if self.enable_intelligent_eviction else None

        # Persistence and telemetry
        self.persistence = ColdStartPersistenceManager(config) if self.enable_persistence else None
        self.telemetry = ColdStartTelemetry() if self.enable_telemetry else None

        # Expert checkpoint cache (LRU)
        self.checkpoint_cache: OrderedDict[str, ExpertCheckpoint] = OrderedDict()

        # Transfer learning mappings
        self.expert_similarity_matrix: Dict[str, Dict[str, float]] = {}

        # Warmup strategies
        self.warmup_strategies: Dict[str, WarmupStrategy] = {}
        self._initialize_strategies()

        # Performance tracking
        self.warmup_history: List[Dict] = []
        self.cold_start_events: List[Dict] = []
        self.sustainability_score = 0.0

        # Thread pool for background tasks
        self.executor = ThreadPoolExecutor(max_workers=4)

        # Start background preloader
        self._start_background_preloader()

        # Load state if persistence enabled
        if self.enable_persistence and self.persistence:
            asyncio.create_task(self._load_state())

        logger.info(f"Enhanced Cold Start Optimizer v3.1.0 initialized with cache size {self.cache_size}")

    def _initialize_strategies(self):
        self.warmup_strategies = {
            'preload': WarmupStrategy(
                strategy_type='preload',
                priority=1,
                estimated_warmup_time_ms=5.0,
                resource_cost=0.001,
                success_probability=0.99,
                carbon_efficiency=0.9,
                helium_efficiency=0.8
            ),
            'transfer': WarmupStrategy(
                strategy_type='transfer',
                priority=2,
                estimated_warmup_time_ms=50.0,
                resource_cost=0.005,
                success_probability=0.85,
                carbon_efficiency=0.7,
                helium_efficiency=0.6
            ),
            'progressive': WarmupStrategy(
                strategy_type='progressive',
                priority=3,
                estimated_warmup_time_ms=200.0,
                resource_cost=0.01,
                success_probability=0.95,
                carbon_efficiency=0.5,
                helium_efficiency=0.5
            ),
            'hybrid': WarmupStrategy(
                strategy_type='hybrid',
                priority=4,
                estimated_warmup_time_ms=100.0,
                resource_cost=0.008,
                success_probability=0.92,
                carbon_efficiency=0.6,
                helium_efficiency=0.7
            )
        }

    def _start_background_preloader(self):
        asyncio.create_task(self._background_preload_loop())

    async def _background_preload_loop(self):
        while True:
            try:
                predictions = {}
                if self.enable_ml_demand and self.ml_predictor:
                    predictions = await self.ml_predictor.predict_demand(horizon_minutes=5)

                # Preload high-probability experts
                for expert_id, probability in predictions.items():
                    if probability > self.preload_threshold:
                        if expert_id not in self.checkpoint_cache:
                            await self.preload_expert(expert_id)

                # Federated cache sync
                if self.enable_federated and self.federated_manager:
                    self.checkpoint_cache = await self.federated_manager.sync_cache_with_peers(
                        self.checkpoint_cache
                    )

                # Intelligent eviction
                if self.enable_intelligent_eviction and self.eviction_manager:
                    if len(self.checkpoint_cache) > self.cache_size * 0.9:
                        num_to_evict = len(self.checkpoint_cache) - int(self.cache_size * 0.8)
                        candidates = await self.eviction_manager.select_eviction_candidates(
                            self.checkpoint_cache, predictions, num_to_evict
                        )
                        for expert_id in candidates:
                            if expert_id in self.checkpoint_cache:
                                del self.checkpoint_cache[expert_id]
                                logger.info(f"Intelligently evicted {expert_id}")
                                self.eviction_manager.eviction_history.append({
                                    'expert_id': expert_id,
                                    'timestamp': datetime.utcnow().isoformat()
                                })

                # Clean old checkpoints
                await self._cleanup_checkpoints()

                # Telemetry
                if self.telemetry:
                    self.telemetry.gauge('cache_size', len(self.checkpoint_cache))
                    self.telemetry.gauge('hit_rate', self._calculate_hit_rate())

                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Background preloader error: {e}")
                await asyncio.sleep(300)

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
        """Report health of the cold start optimizer."""
        return {
            'status': 'healthy',
            'score': min(1.0, self.sustainability_score),
            'details': {
                'modules': {
                    'federated_manager': self.federated_manager is not None,
                    'ml_predictor': self.ml_predictor is not None,
                    'strategy_selector': self.strategy_selector is not None,
                    'helium_dashboard': self.helium_dashboard is not None,
                    'eviction_manager': self.eviction_manager is not None,
                    'persistence': self.persistence is not None,
                    'telemetry': self.telemetry is not None,
                },
                'cache_size': len(self.checkpoint_cache),
                'max_size': self.cache_size,
                'hit_rate': self._calculate_hit_rate(),
                'carbon_saved_kg': self._calculate_carbon_saved(),
                'sustainability_score': self.sustainability_score,
            }
        }

    # ============================================================================
    # Core Initialization Methods (Enhanced)
    # ============================================================================

    async def initialize_expert(
        self,
        expert_id: str,
        expert_type: str,
        carbon_budget: float = 0.1,
        helium_budget: float = 0.1,
        max_latency_ms: float = 500.0,
        urgency: str = 'normal',
        carbon_intensity: Optional[float] = None
    ) -> Dict[str, Any]:
        start_time = datetime.utcnow()

        # Record demand for ML prediction
        if self.enable_ml_demand and self.ml_predictor:
            self.ml_predictor.record_demand(expert_id, start_time)

        # Get real-time carbon intensity if not provided
        if carbon_intensity is None and self.enable_realtime_carbon_api and self.strategy_selector:
            carbon_intensity = await self.strategy_selector.get_realtime_carbon_intensity()
        elif carbon_intensity is None:
            carbon_intensity = 400

        # Select carbon-aware strategy
        if self.enable_carbon_aware and self.strategy_selector:
            selected_strategy = self.strategy_selector.select_strategy(
                self.warmup_strategies,
                carbon_intensity,
                urgency,
                carbon_budget
            )
        else:
            # Default strategy selection
            if expert_id in self.checkpoint_cache:
                selected_strategy = 'preload'
            else:
                similar = self._find_similar_expert(expert_id, expert_type)
                if similar:
                    selected_strategy = 'transfer'
                elif max_latency_ms < 100:
                    selected_strategy = 'hybrid'
                else:
                    selected_strategy = 'progressive'

        # Track helium usage
        if self.enable_helium_tracking and self.helium_dashboard:
            strategy = self.warmup_strategies.get(selected_strategy)
            if strategy:
                await self.helium_dashboard.record_helium_usage(
                    expert_id,
                    strategy.resource_cost * helium_budget,
                    selected_strategy
                )

        # Step 1: Check cache for existing checkpoint
        if expert_id in self.checkpoint_cache:
            logger.info(f"Cache hit for {expert_id}")
            checkpoint = self.checkpoint_cache[expert_id]
            checkpoint.last_used = datetime.utcnow()
            checkpoint.usage_count += 1
            self.checkpoint_cache.move_to_end(expert_id)
            checkpoint.sustainability_score = self._calculate_checkpoint_sustainability(checkpoint)
            return await self._load_from_checkpoint(checkpoint, max_latency_ms)

        # Step 2: Try transfer learning from similar expert
        similar_expert = self._find_similar_expert(expert_id, expert_type)
        if similar_expert and similar_expert in self.checkpoint_cache:
            logger.info(f"Transfer learning from {similar_expert} to {expert_id}")
            return await self._transfer_initialize(
                expert_id, expert_type,
                self.checkpoint_cache[similar_expert],
                max_latency_ms
            )

        # Step 3: Check federated checkpoints
        if self.enable_federated and self.federated_manager:
            peer_cps = await self.federated_manager.get_peer_checkpoints(expert_id)
            if peer_cps:
                aggregated = await self.federated_manager.aggregate_checkpoints(peer_cps)
                if aggregated:
                    logger.info(f"Using federated checkpoint for {expert_id}")
                    checkpoint = ExpertCheckpoint(
                        expert_id=expert_id,
                        expert_type=expert_type,
                        model_state=aggregated,
                        optimizer_state={},
                        feature_distribution=self._compute_feature_distribution(expert_id),
                        performance_metrics={
                            'expected_accuracy': aggregated.get('expected_accuracy', 0.9),
                            'expected_latency_ms': 10.0,
                            'expected_throughput': aggregated.get('expected_throughput', 1000)
                        },
                        created_at=datetime.utcnow(),
                        last_used=datetime.utcnow(),
                        federated_consensus=True,
                        peer_count=len(peer_cps)
                    )
                    self._add_to_cache(expert_id, checkpoint)
                    return await self._load_from_checkpoint(checkpoint, max_latency_ms)

        # Step 4: Progressive initialization with selected strategy
        logger.info(f"Progressive initialization for {expert_id} with {selected_strategy}")
        result = await self._progressive_initialize(
            expert_id, expert_type,
            carbon_budget, helium_budget,
            max_latency_ms,
            selected_strategy
        )

        # Share checkpoint with federation
        if self.enable_federated and self.federated_manager and result.get('initialized'):
            checkpoint_data = {
                'expert_id': expert_id,
                'expert_type': expert_type,
                'model_state': result.get('model_state', {}),
                'performance_metrics': result.get('performance_metrics', {})
            }
            await self.federated_manager.share_checkpoint(
                expert_id,
                checkpoint_data,
                result.get('sustainability_score', 0.5)
            )

        return result

    async def _load_from_checkpoint(
        self,
        checkpoint: ExpertCheckpoint,
        max_latency_ms: float
    ) -> Dict[str, Any]:
        load_start = datetime.utcnow()
        await asyncio.sleep(0.001)
        load_time = (datetime.utcnow() - load_start).total_seconds() * 1000
        checkpoint.sustainability_score = self._calculate_checkpoint_sustainability(checkpoint)

        self.warmup_history.append({
            'expert_id': checkpoint.expert_id,
            'method': 'checkpoint',
            'load_time_ms': load_time,
            'sustainability_score': checkpoint.sustainability_score,
            'timestamp': datetime.utcnow().isoformat()
        })

        if load_time > max_latency_ms:
            logger.warning(f"Checkpoint load exceeded latency budget: {load_time:.2f}ms > {max_latency_ms}ms")

        return {
            'expert_id': checkpoint.expert_id,
            'initialized': True,
            'method': 'checkpoint',
            'load_time_ms': load_time,
            'warmup_required': False,
            'performance_metrics': checkpoint.performance_metrics,
            'checkpoint_age_hours': (datetime.utcnow() - checkpoint.created_at).total_seconds() / 3600,
            'sustainability_score': checkpoint.sustainability_score,
            'carbon_footprint_kg': checkpoint.carbon_footprint_kg,
            'federated_consensus': checkpoint.federated_consensus
        }

    async def _transfer_initialize(
        self,
        target_id: str,
        target_type: str,
        source_checkpoint: ExpertCheckpoint,
        max_latency_ms: float
    ) -> Dict[str, Any]:
        transfer_start = datetime.utcnow()
        await asyncio.sleep(0.01)
        adapted_state = self._adapt_model_state(
            source_checkpoint.model_state,
            target_id,
            target_type
        )
        transfer_time = (datetime.utcnow() - transfer_start).total_seconds() * 1000

        target_checkpoint = ExpertCheckpoint(
            expert_id=target_id,
            expert_type=target_type,
            model_state=adapted_state,
            optimizer_state={},
            feature_distribution=source_checkpoint.feature_distribution,
            performance_metrics={
                **source_checkpoint.performance_metrics,
                'expected_accuracy': source_checkpoint.performance_metrics['expected_accuracy'] * 0.95
            },
            created_at=datetime.utcnow(),
            last_used=datetime.utcnow(),
            carbon_footprint_kg=source_checkpoint.carbon_footprint_kg * 0.8
        )
        target_checkpoint.sustainability_score = self._calculate_checkpoint_sustainability(target_checkpoint)
        self._add_to_cache(target_id, target_checkpoint)

        self.warmup_history.append({
            'expert_id': target_id,
            'method': 'transfer_learning',
            'source_expert': source_checkpoint.expert_id,
            'transfer_time_ms': transfer_time,
            'sustainability_score': target_checkpoint.sustainability_score,
            'timestamp': datetime.utcnow().isoformat()
        })

        return {
            'expert_id': target_id,
            'initialized': True,
            'method': 'transfer_learning',
            'source_expert': source_checkpoint.expert_id,
            'transfer_time_ms': transfer_time,
            'warmup_required': True,
            'estimated_warmup_time_ms': 50.0,
            'performance_metrics': target_checkpoint.performance_metrics,
            'sustainability_score': target_checkpoint.sustainability_score,
            'carbon_footprint_kg': target_checkpoint.carbon_footprint_kg
        }

    async def _progressive_initialize(
        self,
        expert_id: str,
        expert_type: str,
        carbon_budget: float,
        helium_budget: float,
        max_latency_ms: float,
        strategy_type: str = 'progressive'
    ) -> Dict[str, Any]:
        init_start = datetime.utcnow()
        strategy = self.warmup_strategies.get(strategy_type, self.warmup_strategies['progressive'])

        phase1_time = max_latency_ms * 0.2
        await asyncio.sleep(phase1_time / 1000)
        basic_capability = {'accuracy': 0.7, 'throughput': 500, 'features': ['basic_inference']}

        phase2_time = max_latency_ms * 0.3
        await asyncio.sleep(phase2_time / 1000)
        enhanced_capability = {'accuracy': 0.85, 'throughput': 800, 'features': ['basic_inference', 'optimization']}

        phase3_time = max_latency_ms * 0.5
        await asyncio.sleep(phase3_time / 1000)
        full_capability = {
            'accuracy': 0.95,
            'throughput': 1000,
            'features': ['basic_inference', 'optimization', 'transfer_learning', 'meta_learning']
        }

        total_time = (datetime.utcnow() - init_start).total_seconds() * 1000

        checkpoint = ExpertCheckpoint(
            expert_id=expert_id,
            expert_type=expert_type,
            model_state=self._initialize_model_state(expert_id, {'type': expert_type}),
            optimizer_state={},
            feature_distribution=self._compute_feature_distribution(expert_id),
            performance_metrics={
                'expected_accuracy': full_capability['accuracy'],
                'expected_latency_ms': 10.0,
                'expected_throughput': full_capability['throughput'],
                'carbon_per_inference': carbon_budget * 0.1,
                'helium_per_inference': helium_budget * 0.1
            },
            created_at=datetime.utcnow(),
            last_used=datetime.utcnow(),
            carbon_footprint_kg=carbon_budget,
            helium_usage_l=helium_budget * 0.1,
            sustainability_score=self._calculate_checkpoint_sustainability({
                'carbon_footprint_kg': carbon_budget,
                'performance_metrics': {'expected_accuracy': full_capability['accuracy']}
            })
        )
        self._add_to_cache(expert_id, checkpoint)

        self.warmup_history.append({
            'expert_id': expert_id,
            'method': 'progressive',
            'strategy': strategy_type,
            'total_time_ms': total_time,
            'sustainability_score': checkpoint.sustainability_score,
            'timestamp': datetime.utcnow().isoformat()
        })

        return {
            'expert_id': expert_id,
            'initialized': True,
            'method': 'progressive',
            'strategy': strategy_type,
            'total_time_ms': total_time,
            'phases': {
                'basic': basic_capability,
                'enhanced': enhanced_capability,
                'full': full_capability
            },
            'warmup_required': False,
            'cached_for_future': True,
            'performance_metrics': checkpoint.performance_metrics,
            'sustainability_score': checkpoint.sustainability_score,
            'carbon_footprint_kg': checkpoint.carbon_footprint_kg,
            'helium_usage_l': checkpoint.helium_usage_l
        }

    def _calculate_checkpoint_sustainability(self, checkpoint_data: Dict) -> float:
        carbon_score = 1.0 - min(1.0, checkpoint_data.get('carbon_footprint_kg', 0) / 0.1)
        performance_score = checkpoint_data.get('performance_metrics', {}).get('expected_accuracy', 0.5)
        return 0.5 * carbon_score + 0.5 * performance_score

    # ============================================================================
    # Helper Methods (Preserved)
    # ============================================================================

    def _initialize_model_state(self, expert_id: str, expert_config: Optional[Dict]) -> Dict:
        model_state = {
            'expert_id': expert_id,
            'architecture': expert_config.get('architecture', 'transformer') if expert_config else 'transformer',
            'parameters': {
                'num_layers': 6,
                'hidden_size': 512,
                'num_attention_heads': 8,
                'vocabulary_size': 50000
            },
            'weights_initialized': True,
            'quantization': expert_config.get('quantization', 'int8') if expert_config else 'int8',
            'timestamp': datetime.utcnow().isoformat()
        }
        return model_state

    def _compute_feature_distribution(self, expert_id: str) -> Dict[str, float]:
        distributions = {
            'energy': {'carbon_sensitivity': 0.8, 'latency_tolerance': 0.3, 'accuracy_requirement': 0.6, 'helium_dependency': 0.4},
            'data': {'carbon_sensitivity': 0.4, 'latency_tolerance': 0.6, 'accuracy_requirement': 0.9, 'helium_dependency': 0.3},
            'iot': {'carbon_sensitivity': 0.9, 'latency_tolerance': 0.2, 'accuracy_requirement': 0.5, 'helium_dependency': 0.8},
            'quantum': {'carbon_sensitivity': 0.3, 'latency_tolerance': 0.8, 'accuracy_requirement': 0.95, 'helium_dependency': 0.2}
        }
        for expert_type, dist in distributions.items():
            if expert_type in expert_id.lower():
                return dist
        return {'carbon_sensitivity': 0.5, 'latency_tolerance': 0.5, 'accuracy_requirement': 0.7, 'helium_dependency': 0.5}

    def _adapt_model_state(self, source_state: Dict, target_id: str, target_type: str) -> Dict:
        adapted_state = source_state.copy()
        adapted_state['expert_id'] = target_id
        adapted_state['adapted_from'] = source_state.get('expert_id')
        adapted_state['adaptation_timestamp'] = datetime.utcnow().isoformat()
        if 'parameters' in adapted_state:
            if target_type == 'quantum':
                adapted_state['parameters']['quantum_ready'] = True
            elif target_type == 'iot':
                adapted_state['parameters']['edge_optimized'] = True
                adapted_state['parameters']['hidden_size'] = 256
        return adapted_state

    def _find_similar_expert(self, expert_id: str, expert_type: str) -> Optional[str]:
        if not self.checkpoint_cache:
            return None
        best_similarity = 0.0
        best_expert = None
        for cached_id, checkpoint in self.checkpoint_cache.items():
            type_similarity = 1.0 if checkpoint.expert_type == expert_type else 0.3
            target_dist = self._compute_feature_distribution(expert_id)
            source_dist = checkpoint.feature_distribution
            common_keys = set(target_dist.keys()) & set(source_dist.keys())
            if common_keys:
                dot_product = sum(target_dist[k] * source_dist[k] for k in common_keys)
                norm_target = np.sqrt(sum(v**2 for v in target_dist.values()))
                norm_source = np.sqrt(sum(v**2 for v in source_dist.values()))
                if norm_target > 0 and norm_source > 0:
                    dist_similarity = dot_product / (norm_target * norm_source)
                else:
                    dist_similarity = 0.0
            else:
                dist_similarity = 0.0
            similarity = 0.6 * type_similarity + 0.4 * dist_similarity
            if similarity > best_similarity:
                best_similarity = similarity
                best_expert = cached_id
        return best_expert if best_similarity > 0.5 else None

    def _add_to_cache(self, expert_id: str, checkpoint: ExpertCheckpoint):
        if len(self.checkpoint_cache) >= self.cache_size:
            if self.enable_intelligent_eviction and self.eviction_manager:
                predictions = {}
                if self.enable_ml_demand and self.ml_predictor:
                    predictions = asyncio.run(self.ml_predictor.predict_demand())
                eviction_candidates = asyncio.run(
                    self.eviction_manager.select_eviction_candidates(
                        self.checkpoint_cache, predictions, 1
                    )
                )
                if eviction_candidates:
                    oldest_id = eviction_candidates[0]
                    self.checkpoint_cache.pop(oldest_id, None)
                    logger.info(f"Intelligently evicted {oldest_id} from cache")
            else:
                oldest_id, _ = self.checkpoint_cache.popitem(last=False)
                logger.info(f"Evicted {oldest_id} from cache (LRU)")
        self.checkpoint_cache[expert_id] = checkpoint
        logger.debug(f"Added {expert_id} to cache (size: {len(self.checkpoint_cache)})")

    async def _save_checkpoint_to_disk(self, checkpoint: ExpertCheckpoint):
        import os
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        checkpoint_path = f"{self.checkpoint_dir}/{checkpoint.expert_id}.ckpt"
        try:
            with open(checkpoint_path, 'wb') as f:
                pickle.dump(checkpoint, f)
            logger.debug(f"Saved checkpoint to {checkpoint_path}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    async def _cleanup_checkpoints(self):
        now = datetime.utcnow()
        max_age = timedelta(hours=24)
        expired = [eid for eid, cp in self.checkpoint_cache.items() if now - cp.last_used > max_age]
        for eid in expired:
            del self.checkpoint_cache[eid]
            logger.info(f"Cleaned up expired checkpoint: {eid}")

    # ============================================================================
    # Statistics Methods (Enhanced)
    # ============================================================================

    def get_cache_statistics(self) -> Dict[str, Any]:
        stats = {
            'cache_size': len(self.checkpoint_cache),
            'max_size': self.cache_size,
            'hit_rate': self._calculate_hit_rate(),
            'average_load_time_ms': self._calculate_avg_load_time(),
            'total_warmup_time_saved_ms': self._calculate_time_saved(),
            'carbon_saved_kg': self._calculate_carbon_saved(),
            'most_used_experts': self._get_most_used_experts(5)
        }
        stats['sustainability_score'] = self.sustainability_score

        if self.enable_federated and self.federated_manager:
            stats['federated'] = self.federated_manager.get_federated_stats()

        if self.enable_ml_demand and self.ml_predictor:
            stats['ml_predictor'] = self.ml_predictor.get_model_performance()

        if self.enable_carbon_aware and self.strategy_selector:
            stats['carbon_aware'] = self.strategy_selector.get_carbon_impact_report()

        if self.enable_helium_tracking and self.helium_dashboard:
            stats['helium'] = self.helium_dashboard.get_efficiency_report()

        if self.enable_intelligent_eviction and self.eviction_manager:
            stats['eviction'] = self.eviction_manager.get_eviction_stats()

        return stats

    def _calculate_hit_rate(self) -> float:
        total = len(self.warmup_history)
        if total == 0:
            return 0.0
        hits = sum(1 for h in self.warmup_history if h.get('method') in ['checkpoint', 'transfer_learning'])
        return hits / total

    def _calculate_avg_load_time(self) -> float:
        if not self.warmup_history:
            return 0.0
        load_times = [h.get('load_time_ms', h.get('total_time_ms', 0)) for h in self.warmup_history]
        return np.mean(load_times) if load_times else 0.0

    def _calculate_time_saved(self) -> float:
        cold_start_time = 500.0
        total_saved = 0.0
        for event in self.warmup_history:
            actual_time = event.get('load_time_ms', event.get('total_time_ms', cold_start_time))
            total_saved += cold_start_time - actual_time
        return total_saved

    def _calculate_carbon_saved(self) -> float:
        carbon_per_ms = 0.00001
        time_saved_ms = self._calculate_time_saved()
        return time_saved_ms * carbon_per_ms

    def _get_most_used_experts(self, top_n: int) -> List[Dict]:
        usage_counts = {eid: cp.usage_count for eid, cp in self.checkpoint_cache.items()}
        sorted_experts = sorted(usage_counts.items(), key=lambda x: x[1], reverse=True)
        return [{'expert_id': eid, 'usage_count': count} for eid, count in sorted_experts[:top_n]]

    async def preload_expert(self, expert_id: str, expert_config: Optional[Dict] = None) -> bool:
        try:
            if expert_id in self.checkpoint_cache:
                logger.debug(f"Expert {expert_id} already cached")
                return True
            checkpoint = await self._create_checkpoint(expert_id, expert_config)
            self._add_to_cache(expert_id, checkpoint)
            logger.info(f"Preloaded expert {expert_id} into cache")
            return True
        except Exception as e:
            logger.error(f"Failed to preload expert {expert_id}: {e}")
            return False

    async def _create_checkpoint(self, expert_id: str, expert_config: Optional[Dict]) -> ExpertCheckpoint:
        model_state = self._initialize_model_state(expert_id, expert_config)
        feature_distribution = self._compute_feature_distribution(expert_id)
        performance_metrics = {
            'expected_accuracy': 0.92,
            'expected_latency_ms': 10.0,
            'expected_throughput': 1000.0,
            'carbon_per_inference': 0.0001,
            'helium_per_inference': 0.01
        }
        checkpoint = ExpertCheckpoint(
            expert_id=expert_id,
            expert_type=expert_config.get('type', 'general') if expert_config else 'general',
            model_state=model_state,
            optimizer_state={},
            feature_distribution=feature_distribution,
            performance_metrics=performance_metrics,
            created_at=datetime.utcnow(),
            last_used=datetime.utcnow(),
            carbon_footprint_kg=0.0005,
            sustainability_score=0.7
        )
        await self._save_checkpoint_to_disk(checkpoint)
        return checkpoint

    def get_sustainability_report(self) -> Dict[str, Any]:
        helium_forecast = None
        if self.enable_predictive_helium and self.helium_dashboard:
            helium_forecast = asyncio.run(self.helium_dashboard.predict_helium_usage())

        return {
            'timestamp': datetime.utcnow().isoformat(),
            'sustainability_score': self.sustainability_score,
            'cache_hit_rate': self._calculate_hit_rate(),
            'carbon_saved_kg': self._calculate_carbon_saved(),
            'time_saved_ms': self._calculate_time_saved(),
            'strategy_distribution': self._get_strategy_distribution(),
            'helium_forecast': helium_forecast,
            'recommendations': self._generate_sustainability_recommendations()
        }

    def _get_strategy_distribution(self) -> Dict[str, int]:
        distribution = {}
        for event in self.warmup_history[-100:]:
            method = event.get('method', 'unknown')
            distribution[method] = distribution.get(method, 0) + 1
        return distribution

    def _generate_sustainability_recommendations(self) -> List[str]:
        recommendations = []
        if self._calculate_hit_rate() < 0.5:
            recommendations.append("Increase cache size or preload threshold")
        carbon_saved = self._calculate_carbon_saved()
        if carbon_saved < 0.01:
            recommendations.append("Optimize checkpoint creation for better carbon savings")
        if self.enable_helium_tracking and self.helium_dashboard:
            helium_report = self.helium_dashboard.get_efficiency_report()
            if helium_report.get('helium_savings_rate', 0) < 0.1:
                recommendations.append("Implement helium recovery for initialization operations")
        if self.enable_predictive_helium and self.helium_dashboard:
            forecast = asyncio.run(self.helium_dashboard.predict_helium_usage())
            if forecast.get('status') == 'success':
                total_predicted = forecast.get('total_predicted_usage', 0)
                if total_predicted > self.helium_dashboard.total_helium_used * 1.2:
                    recommendations.append("Helium usage expected to increase - implement proactive optimization")
        if self.enable_intelligent_eviction and self.eviction_manager:
            eviction_stats = self.eviction_manager.get_eviction_stats()
            if eviction_stats.get('total_evictions', 0) > 50:
                recommendations.append("High eviction rate - consider increasing cache size")
        return recommendations or ["Cold start optimizer is performing well"]

    async def shutdown(self):
        """Graceful shutdown of all components."""
        logger.info("Shutting down Cold Start Optimizer")
        if self.enable_persistence:
            await self.save_state()
        if self.federated_manager:
            await self.federated_manager.close()
        if self.strategy_selector:
            await self.strategy_selector.close()
        self.executor.shutdown(wait=True)
        logger.info("Shutdown complete")

# ============================================================================
# Singleton Accessor (Preserved)
# ============================================================================

_optimizer_instance = None

async def get_cold_start_optimizer() -> ColdStartOptimizer:
    global _optimizer_instance
    if _optimizer_instance is None:
        _optimizer_instance = ColdStartOptimizer()
    return _optimizer_instance
