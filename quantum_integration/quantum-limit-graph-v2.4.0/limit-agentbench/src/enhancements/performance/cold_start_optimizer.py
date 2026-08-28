#!/usr/bin/env python3
"""
Cold Start Optimizer for Green Agent MoE System v3.4.0
Eliminates expert warmup latency through pre-initialization and transfer learning.
ENHANCED WITH: Multi‑Teacher On‑Policy Distillation and Multi‑Objective Evolutionary Optimization (NSGA‑II).

Features:
- Adaptive strategy selection (preload, transfer, progressive, hybrid, federated)
  using Multi‑Teacher On‑Policy Distillation.
- Continuous strategy weight refinement via NSGA‑II with Pareto front and MODP selection.
- State‑aware decisions based on expert type, urgency, carbon/helium budgets,
  latency, and historical performance.
- Online learning from warmup outcomes.
- Teachers: rule‑based, historical ML, stateful Q.
- Student: linear softmax with distillation + REINFORCE.
- Persistence for Q‑teacher weights and interaction logs.
- Offline training for historical ML teacher.
- Unit tests for distillation components.
All previous features (federated, ML demand, carbon‑aware, helium, eviction, etc.) retained.

NEW IN v3.4.0:
- Added LIMIT Graph manager for expert/strategy relationships.
- Added MODP optimizer wrapper for storing decision states/policies.
- Added RLHF trainer for human preference collection on strategy choices.
- Added MoE gating network to blend strategies (experts).
- Integration with central Storage (optional) for new data persistence.
- New configuration flags for enabling/disabling each component.
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
import json
import hashlib
import os
import zlib
import pickle
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import SGDRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import threading
from concurrent.futures import ThreadPoolExecutor
import aiohttp
import aiofiles
import random
from abc import ABC, abstractmethod
from pathlib import Path
import uuid
import copy
import time

# Pydantic for configuration
from pydantic import BaseModel, Field, field_validator, ConfigDict, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

# Tenacity for retries
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, AsyncRetrying, RetryError

# Prometheus
try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server, generate_latest
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# scikit-learn for ML teacher
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

# Optional central storage
try:
    from ...storage import Storage  # adjust path if needed
    CENTRAL_STORAGE_AVAILABLE = True
except ImportError:
    CENTRAL_STORAGE_AVAILABLE = False
    Storage = None

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration with Pydantic (Environment-aware)
# ============================================================================

class ColdStartConfig(BaseSettings):
    """Centralized configuration for Cold Start Optimizer using Pydantic."""
    model_config = SettingsConfigDict(env_prefix="COLD_START_", case_sensitive=False)

    # Core parameters
    cache_size: int = Field(100, ge=1)
    preload_threshold: float = Field(0.7, ge=0, le=1)
    checkpoint_dir: str = Field("./expert_checkpoints")

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

    # NEW v3.4.0 flags
    enable_limit_graph: bool = True
    enable_modp: bool = True
    enable_rlhf: bool = True
    enable_moe: bool = True
    moe_expert_count: int = Field(5, ge=2)

    # Federated learning
    federated_server_url: Optional[str] = None
    privacy_epsilon: float = Field(1.0, ge=0)
    federated_sparsity_ratio: float = Field(0.1, ge=0, le=1)

    # ML demand predictor
    ml_history_window: int = Field(1000, ge=10)
    ml_online_learning_rate: float = Field(0.01, gt=0)
    ml_retrain_threshold: int = Field(100, ge=10)

    # Carbon-aware strategy
    carbon_intensity_thresholds: Dict[str, float] = Field(default_factory=lambda: {
        'low': 200, 'medium': 350, 'high': 500
    })
    strategy_weights: Dict[str, float] = Field(default_factory=lambda: {
        'priority': 0.2, 'resource_cost': 0.3, 'carbon_efficiency': 0.3, 'urgency': 0.2
    })

    # Helium forecasting
    helium_forecast_model: str = "exponential_smoothing"

    # Eviction manager
    eviction_weights: Dict[str, float] = Field(default_factory=lambda: {
        'usage_count': 0.25, 'age': 0.20, 'predicted_demand': 0.35, 'sustainability': 0.20
    })

    # Retry and circuit breaker
    max_retries: int = Field(3, ge=0)
    retry_base_delay_ms: float = Field(100.0, ge=0)
    retry_max_delay_ms: float = Field(5000.0, ge=0)
    circuit_breaker_failure_threshold: int = Field(5, ge=1)
    circuit_breaker_recovery_timeout: float = Field(30.0, ge=0)

    # Persistence
    persistence_path: str = Field("cold_start_state.json.gz")

    # Telemetry
    telemetry_export_interval: int = Field(60, ge=1)
    prometheus_port: Optional[int] = Field(None, ge=1024)

    # Distillation parameters
    distillation_epsilon: float = Field(0.1, ge=0, le=1)
    distillation_train_every: int = Field(10, ge=1)
    distillation_replay_size: int = Field(2000, ge=10)
    distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1)
    distill_weight: float = Field(0.7, ge=0, le=1)
    rl_weight: float = Field(0.3, ge=0, le=1)

    # MOEA parameters
    moea_enabled: bool = Field(True, description="Enable MOEA global weight optimization")
    moea_interval_seconds: int = Field(300, ge=60, description="MOEA run interval")
    moea_population_size: int = Field(20, ge=5)
    moea_generations: int = Field(10, ge=1)
    moea_mutation_rate: float = Field(0.2, ge=0.0, le=1.0)
    moea_crossover_rate: float = Field(0.8, ge=0.0, le=1.0)
    moea_tournament_size: int = Field(3, ge=2)
    moea_objective_weights: Optional[Dict[str, float]] = Field(
        default_factory=lambda: {
            'latency': 0.4,
            'carbon': 0.3,
            'cache_hit': 0.2,
            'sustainability': 0.1,
        }
    )
    moea_dynamic_weights: bool = Field(True)
    moea_pareto_path: str = Field("./cold_start_moea_pareto.json")

    # Persistence paths for distillation
    q_weights_path: str = Field("./cold_start_q_weights.json")
    interaction_logs_path: str = Field("./cold_start_interactions.csv")
    historical_model_path: str = Field("./cold_start_historical_model.pkl")

    @field_validator('eviction_weights')
    @classmethod
    def eviction_weights_sum(cls, v: Dict[str, float]) -> Dict[str, float]:
        if abs(sum(v.values()) - 1.0) > 0.01:
            raise ValueError("eviction_weights must sum to approximately 1.0")
        return v

    @field_validator('strategy_weights')
    @classmethod
    def strategy_weights_sum(cls, v: Dict[str, float]) -> Dict[str, float]:
        if abs(sum(v.values()) - 1.0) > 0.01:
            raise ValueError("strategy_weights must sum to approximately 1.0")
        return v

    @field_validator('carbon_intensity_thresholds')
    @classmethod
    def carbon_thresholds_ordered(cls, v: Dict[str, float]) -> Dict[str, float]:
        if v['low'] >= v['medium'] or v['medium'] >= v['high']:
            raise ValueError("carbon_intensity_thresholds must satisfy low < medium < high")
        return v


# ============================================================================
# Circuit Breaker with Half‑Open State (Thread‑safe)
# ============================================================================

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    """Async circuit breaker with half‑open state and thread‑safe lock."""

    def __init__(self, failure_threshold: int, recovery_timeout: float, name: str = "default"):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute the given async function with circuit breaker protection."""
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if self.last_failure_time:
                    elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
                    if elapsed >= self.recovery_timeout:
                        self.state = CircuitBreakerState.HALF_OPEN
                        self.failure_count = 0
                        logger.info(f"Circuit breaker {self.name} entered HALF_OPEN state")
                    else:
                        raise RuntimeError(f"Circuit breaker {self.name} OPEN (recovery in {self.recovery_timeout - elapsed:.1f}s)")
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} OPEN (no failure time)")

        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    logger.info(f"Circuit breaker {self.name} closed after successful half-open call")
                elif self.state == CircuitBreakerState.CLOSED:
                    self.failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = datetime.utcnow()
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker {self.name} opened due to failure in half-open state: {e}")
                elif self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker {self.name} opened after {self.failure_count} failures")
            raise e

    @property
    def is_open(self) -> bool:
        return self.state == CircuitBreakerState.OPEN

    async def reset(self):
        async with self._lock:
            self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0
            self.last_failure_time = None
            logger.info(f"Circuit breaker {self.name} manually reset")


# ============================================================================
# Retry Helper (using tenacity)
# ============================================================================

def is_retryable_exception(e: Exception) -> bool:
    return isinstance(e, (IOError, TimeoutError, ConnectionError, aiohttp.ClientError))

async def retry_async_with_tenacity(func: Callable, max_attempts: int = 3, *args, **kwargs) -> Any:
    for attempt in range(max_attempts):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            if attempt == max_attempts - 1:
                raise
            wait_time = min(2 ** attempt, 10)
            await asyncio.sleep(wait_time)
    raise RuntimeError("Max retries exceeded")

async def retry_call(func: Callable, *args, **kwargs):
    return await retry_async_with_tenacity(func, 3, *args, **kwargs)


# ============================================================================
# Telemetry Collector (Prometheus)
# ============================================================================

class ColdStartTelemetry:
    """Collects telemetry for the cold start optimizer."""

    def __init__(self, config: ColdStartConfig):
        self.config = config
        self.metrics: Dict[str, Any] = defaultdict(lambda: defaultdict(int))
        self._lock = asyncio.Lock()
        self._prometheus_metrics = None
        if PROMETHEUS_AVAILABLE and config.prometheus_port:
            self._setup_prometheus()
            self._start_prometheus_server()

    def _setup_prometheus(self):
        self._prometheus_metrics = {
            'cs_cache_size': Gauge('cs_cache_size', 'Number of cached checkpoints'),
            'cs_hit_rate': Gauge('cs_hit_rate', 'Cache hit rate'),
            'cs_sustainability_score': Gauge('cs_sustainability_score', 'Overall sustainability score'),
            'cs_carbon_saved_kg': Gauge('cs_carbon_saved_kg', 'Carbon saved (kg)'),
            'cs_time_saved_ms': Gauge('cs_time_saved_ms', 'Time saved (ms)'),
            'cs_scenarios_run': Counter('cs_scenarios_run', 'Total scenarios run'),
            'cs_helium_used_l': Gauge('cs_helium_used_l', 'Total helium used (L)'),
            'cs_evictions': Counter('cs_evictions', 'Number of cache evictions'),
        }

    def _start_prometheus_server(self):
        start_http_server(self.config.prometheus_port)
        logger.info(f"Prometheus metrics server started on port {self.config.prometheus_port}")

    def increment(self, metric_name: str, tags: Optional[Dict[str, str]] = None, value: float = 1.0):
        key = self._make_key(metric_name, tags)
        self.metrics['counters'][key] += value
        if self._prometheus_metrics and metric_name in self._prometheus_metrics:
            if isinstance(self._prometheus_metrics[metric_name], Counter):
                self._prometheus_metrics[metric_name].inc(value)

    def gauge(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, tags)
        self.metrics['gauges'][key] = value
        if self._prometheus_metrics and metric_name in self._prometheus_metrics:
            if isinstance(self._prometheus_metrics[metric_name], Gauge):
                self._prometheus_metrics[metric_name].set(value)

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
        if PROMETHEUS_AVAILABLE and self.config.prometheus_port:
            return generate_latest().decode('utf-8')
        # Fallback text format
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
# Persistence Manager (JSON + zlib + async I/O)
# ============================================================================

class ColdStartPersistenceManager:
    """Saves and loads the cold start optimizer state using JSON + compression."""

    def __init__(self, config: ColdStartConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=config.circuit_breaker_failure_threshold,
            recovery_timeout=config.circuit_breaker_recovery_timeout,
            name="persistence"
        )
        logger.info(f"ColdStartPersistenceManager initialized (path={self.path})")

    async def save_state(self, optimizer: 'ColdStartOptimizer') -> bool:
        """Save the optimizer state to disk."""
        async with self._lock:
            try:
                state = {
                    'version': '3.4.0',
                    'checkpoint_cache': {
                        k: {
                            'expert_id': v.expert_id,
                            'expert_type': v.expert_type,
                            'model_state': v.model_state,
                            'optimizer_state': v.optimizer_state,
                            'feature_distribution': v.feature_distribution,
                            'performance_metrics': v.performance_metrics,
                            'created_at': v.created_at.isoformat(),
                            'last_used': v.last_used.isoformat(),
                            'usage_count': v.usage_count,
                            'carbon_footprint_kg': v.carbon_footprint_kg,
                            'helium_usage_l': v.helium_usage_l,
                            'sustainability_score': v.sustainability_score,
                            'federated_consensus': v.federated_consensus,
                            'peer_count': v.peer_count,
                        }
                        for k, v in optimizer.checkpoint_cache.items()
                    },
                    'warmup_history': optimizer.warmup_history,
                    'sustainability_score': optimizer.sustainability_score,
                    'cold_start_events': optimizer.cold_start_events,
                    'expert_similarity_matrix': optimizer.expert_similarity_matrix,
                }
                # Save sub-module states
                if optimizer.ml_predictor:
                    state['ml_predictor'] = {
                        'demand_history': optimizer.ml_predictor.demand_history,
                        'model_version': optimizer.ml_predictor.model_version,
                        'feature_importance': optimizer.ml_predictor.feature_importance,
                        'training_samples': optimizer.ml_predictor.training_samples,
                        'model_weights': optimizer.ml_predictor._serialize_model(),
                    }
                if optimizer.helium_dashboard:
                    state['helium_dashboard'] = {
                        'usage_history': optimizer.helium_dashboard.usage_history,
                        'total_helium_used': optimizer.helium_dashboard.total_helium_used,
                        'total_helium_saved': optimizer.helium_dashboard.total_helium_saved,
                        'helium_usage': optimizer.helium_dashboard.helium_usage,
                        'efficiency_scores': optimizer.helium_dashboard.efficiency_scores,
                    }
                if optimizer.eviction_manager:
                    state['eviction_manager'] = {
                        'eviction_history': optimizer.eviction_manager.eviction_history,
                    }

                # Save distillation state (Q-teacher weights)
                state['q_teacher_weights'] = optimizer.strategy_optimizer.teachers[2].weights.tolist()

                # Serialize to JSON
                json_str = json.dumps(state, default=str, indent=2)
                compressed = zlib.compress(json_str.encode('utf-8'))
                async with aiofiles.open(self.path, 'wb') as f:
                    await f.write(compressed)
                logger.info(f"Cold start state saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    async def load_state(self, optimizer: 'ColdStartOptimizer') -> bool:
        """Load the optimizer state from disk."""
        async with self._lock:
            if not os.path.exists(self.path):
                logger.warning(f"Persistence file {self.path} not found")
                return False
            try:
                async with aiofiles.open(self.path, 'rb') as f:
                    compressed = await f.read()
                json_str = zlib.decompress(compressed).decode('utf-8')
                state = json.loads(json_str)

                # Version check
                version = state.get('version', '1.0.0')
                if version != '3.4.0':
                    logger.warning(f"State version mismatch: {version} != 3.4.0; attempting to load anyway")

                # Restore checkpoint cache
                cache_data = state.get('checkpoint_cache', {})
                optimizer.checkpoint_cache = OrderedDict()
                for k, v_data in cache_data.items():
                    cp = ExpertCheckpoint(
                        expert_id=v_data['expert_id'],
                        expert_type=v_data['expert_type'],
                        model_state=v_data['model_state'],
                        optimizer_state=v_data['optimizer_state'],
                        feature_distribution=v_data['feature_distribution'],
                        performance_metrics=v_data['performance_metrics'],
                        created_at=datetime.fromisoformat(v_data['created_at']),
                        last_used=datetime.fromisoformat(v_data['last_used']),
                        usage_count=v_data['usage_count'],
                        carbon_footprint_kg=v_data['carbon_footprint_kg'],
                        helium_usage_l=v_data['helium_usage_l'],
                        sustainability_score=v_data['sustainability_score'],
                        federated_consensus=v_data['federated_consensus'],
                        peer_count=v_data['peer_count'],
                    )
                    optimizer.checkpoint_cache[k] = cp

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
                    optimizer.ml_predictor._deserialize_model(ml_state.get('model_weights', {}))

                he_state = state.get('helium_dashboard')
                if he_state and optimizer.helium_dashboard:
                    optimizer.helium_dashboard.usage_history = he_state.get('usage_history', [])
                    optimizer.helium_dashboard.total_helium_used = he_state.get('total_helium_used', 0.0)
                    optimizer.helium_dashboard.total_helium_saved = he_state.get('total_helium_saved', 0.0)
                    optimizer.helium_dashboard.helium_usage = he_state.get('helium_usage', {})
                    optimizer.helium_dashboard.efficiency_scores = he_state.get('efficiency_scores', {})

                ev_state = state.get('eviction_manager')
                if ev_state and optimizer.eviction_manager:
                    optimizer.eviction_manager.eviction_history = ev_state.get('eviction_history', [])

                # Restore Q-teacher weights
                q_weights = state.get('q_teacher_weights')
                if q_weights is not None:
                    optimizer.strategy_optimizer.teachers[2].weights = np.array(q_weights)

                logger.info(f"Cold start state loaded from {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return False

    async def delete_state(self):
        async with self._lock:
            if os.path.exists(self.path):
                await aiofiles.os.remove(self.path)
                logger.info(f"Persistence file {self.path} deleted")
                return True
            return False


# ============================================================================
# Federated Checkpoint Manager (Enhanced)
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
        self._session: Optional[aiohttp.ClientSession] = None
        self.sync_history = deque(maxlen=1000)
        self.noise_scale = 0.001
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=config.circuit_breaker_failure_threshold,
            recovery_timeout=config.circuit_breaker_recovery_timeout,
            name="federated"
        )
        logger.info(f"Federated Checkpoint Manager initialized (ε={self.privacy_epsilon})")

    async def _get_session(self) -> aiohttp.ClientSession:
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

        async def _do_share():
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
                if response.status != 200:
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=f"API returned {response.status}"
                    )
                return await response.json()

        try:
            result = await self._circuit_breaker.call(_do_share)
            logger.info(f"Shared checkpoint for {expert_id} with federation (ε={self.privacy_epsilon})")
            return result
        except Exception as e:
            logger.error(f"Checkpoint sharing failed: {e}")
            return {'status': 'failed'}

    async def get_peer_checkpoints(self, expert_id: str) -> List[Dict]:
        if not self.server_url:
            return []

        async def _do_fetch():
            session = await self._get_session()
            async with session.get(
                f"{self.server_url}/federated/checkpoints/{expert_id}",
                timeout=30
            ) as response:
                if response.status != 200:
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=f"API returned {response.status}"
                    )
                data = await response.json()
                return data.get('checkpoints', [])

        try:
            return await self._circuit_breaker.call(_do_fetch)
        except Exception as e:
            logger.error(f"Peer checkpoints fetch failed: {e}")
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

        async def _do_sync():
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
                if response.status != 200:
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=f"API returned {response.status}"
                    )
                data = await response.json()
                return data

        try:
            data = await self._circuit_breaker.call(_do_sync)
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
        except Exception as e:
            logger.error(f"Cache sync failed: {e}")
            return local_cache

    def get_federated_stats(self) -> Dict:
        return {
            'server_url': self.server_url,
            'peer_checkpoints': len(self.peer_checkpoints),
            'sync_count': len(self.sync_history),
            'privacy_epsilon': self.privacy_epsilon,
            'sparsity_ratio': self.sparsity_ratio,
            'last_sync': list(self.sync_history)[-1] if self.sync_history else None,
            'circuit_open': self._circuit_breaker.is_open
        }

    async def close(self):
        if self._session:
            await self._session.close()


# ============================================================================
# ML Demand Predictor (Enhanced with Lock and Thread Offloading)
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
        self.model: Optional[SGDRegressor] = None
        self._ml_available = False
        self._lock = asyncio.Lock()
        self._train_lock = asyncio.Lock()
        self._init_model()
        self._executor = ThreadPoolExecutor(max_workers=1)

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

    def _serialize_model(self) -> Dict:
        """Serialize model weights for persistence."""
        if not self._ml_available or self.model is None:
            return {}
        return {
            'coef_': self.model.coef_.tolist() if hasattr(self.model, 'coef_') else [],
            'intercept_': self.model.intercept_.tolist() if hasattr(self.model, 'intercept_') else 0.0,
        }

    def _deserialize_model(self, weights: Dict):
        """Deserialize model weights from persistence."""
        if not self._ml_available or self.model is None or not weights:
            return
        if 'coef_' in weights:
            self.model.coef_ = np.array(weights['coef_'])
        if 'intercept_' in weights:
            self.model.intercept_ = np.array(weights['intercept_'])

    def record_demand(self, expert_id: str, timestamp: datetime, context: Dict = None):
        async def _record():
            async with self._lock:
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
        asyncio.create_task(_record())

    async def _online_learning_update(self):
        async with self._train_lock:
            try:
                recent_data = self.demand_history[-self.samples_since_last_train:]
                if len(recent_data) > 10:
                    X, y = self._prepare_training_data(recent_data)
                    if len(X) > 0:
                        # Offload scaling and training to thread
                        def train():
                            X_scaled = self.scaler.fit_transform(X) if not self.scaler.mean_ else self.scaler.transform(X)
                            self.model.partial_fit(X_scaled, y)
                            return True
                        await asyncio.to_thread(train)
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

    async def train_model(self) -> Dict:
        async with self._lock:
            if len(self.demand_history) < 50:
                return {'status': 'insufficient_data', 'samples': len(self.demand_history)}
            if not self._ml_available:
                return {'status': 'ml_not_available'}
            X, y = self._prepare_training_data(self.demand_history)
            if len(X) < 20:
                return {'status': 'insufficient_training_data', 'samples': len(X)}

        def train():
            X_scaled = self.scaler.fit_transform(X)
            for _ in range(5):
                self.model.partial_fit(X_scaled, y)
            return True

        await asyncio.to_thread(train)
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
            def predict():
                features_scaled = self.scaler.transform(features_array)
                pred = self.model.predict(features_scaled)[0]
                return pred
            pred = await asyncio.to_thread(predict)
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

    async def close(self):
        self._executor.shutdown(wait=True)


# ============================================================================
# Carbon-Aware Strategy Selector (Enhanced: Lock)
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
        self._session: Optional[aiohttp.ClientSession] = None
        self.cache = {}
        self.last_update: Optional[datetime] = None
        self.update_interval = 300
        self._lock = asyncio.Lock()
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=config.circuit_breaker_failure_threshold,
            recovery_timeout=config.circuit_breaker_recovery_timeout,
            name="carbon_api"
        )
        logger.info("Carbon-Aware Strategy Selector initialized with real-time API")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def get_realtime_carbon_intensity(self, region: str = "US-CAL-CISO") -> float:
        """Get real-time carbon intensity with retry and circuit breaker."""
        async def _do_fetch():
            session = await self._get_session()
            url = f"{self.api_endpoint}/carbon-intensity/latest?zone={region}"
            headers = {'auth-token': self.api_key} if self.api_key else {}
            async with session.get(url, headers=headers, timeout=10) as response:
                if response.status != 200:
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=f"API returned {response.status}"
                    )
                data = await response.json()
                return data.get('carbonIntensity', 400)

        cache_key = f"{region}_{datetime.utcnow().hour}"
        async with self._lock:
            if cache_key in self.cache and self.last_update and (datetime.utcnow() - self.last_update).seconds < self.update_interval:
                return self.cache[cache_key]

        try:
            intensity = await self._circuit_breaker.call(_do_fetch)
            async with self._lock:
                self.cache[cache_key] = intensity
                self.last_update = datetime.utcnow()
            return intensity
        except Exception as e:
            logger.warning(f"Carbon API error: {e}, using fallback")
            return self._get_fallback_intensity()

    def _get_fallback_intensity(self) -> float:
        # Simulate diurnal pattern
        hour = datetime.utcnow().hour
        base = 350
        diurnal = 50 * np.sin((hour - 8) / 12 * np.pi)
        return max(200, min(500, base + diurnal))

    async def select_strategy(
        self,
        strategies: Dict[str, Any],
        carbon_intensity: Optional[float] = None,
        urgency: str = 'normal',
        carbon_budget: float = None
    ) -> str:
        if carbon_intensity is None:
            carbon_intensity = await self.get_realtime_carbon_intensity()

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
# Helium Efficiency Dashboard (Enhanced with Lock)
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
        self.alpha = 0.3
        logger.info("Helium Efficiency Dashboard initialized")

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
            if len(self.usage_history) > 20:
                self._train_forecast()
            logger.debug(f"Helium usage recorded: {expert_id} = {amount_l}L ({operation})")

    def _train_forecast(self):
        """Train helium usage forecast model using exponential smoothing."""
        if len(self.usage_history) < 20:
            return
        values = [h['amount_l'] for h in self.usage_history[-50:]]
        if not values:
            return
        smoothed = values[0]
        for v in values[1:]:
            smoothed = self.alpha * v + (1 - self.alpha) * smoothed
        self.forecast_trained = True
        self._last_smoothed = smoothed
        self._last_values = values

    async def predict_helium_usage(self, hours: int = 24) -> Dict[str, Any]:
        if not self.forecast_trained:
            return {
                'status': 'not_trained',
                'prediction': self.total_helium_used / max(len(self.usage_history), 1) * hours
            }
        recent = [h['amount_l'] for h in self.usage_history[-min(20, len(self.usage_history)):]]
        hourly_avg = np.mean(recent) if recent else 0.0
        total_predicted = hourly_avg * hours
        return {
            'status': 'success',
            'predictions': [hourly_avg] * hours,
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
        async with self._lock:
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
        async with self._lock:
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
# Intelligent Eviction Manager (Enhanced with Lock)
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
        async with self._lock:
            return {
                'total_evictions': len(self.eviction_history),
                'recent_evictions': self.eviction_history[-10:] if self.eviction_history else []
            }


# ============================================================================
# Data Classes
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
# NEW: LIMIT Graph Manager
# ============================================================================
class LimitGraphManager:
    """
    Manages a graph of expert/strategy relationships for LIMIT.
    Nodes are experts or strategies, edges represent dependencies or fallback order.
    """
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self.graphs = {}

    def create_graph(self, graph_id: str, description: str, configuration: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_metadata'):
            self.storage.save_limit_graph_metadata(graph_id, description, configuration)
        else:
            self.graphs[graph_id] = {'description': description, 'configuration': configuration, 'nodes': {}, 'edges': {}}

    def add_node(self, graph_id: str, node_id: str, node_type: Optional[str], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_node'):
            self.storage.save_limit_graph_node(node_id, graph_id, node_type, attributes)
        else:
            if graph_id not in self.graphs:
                self.graphs[graph_id] = {'nodes': {}, 'edges': {}}
            self.graphs[graph_id]['nodes'][node_id] = {'node_type': node_type, 'attributes': attributes}

    def add_edge(self, graph_id: str, edge_id: str, source: str, target: str,
                 weight: Optional[float], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_edge'):
            self.storage.save_limit_graph_edge(edge_id, graph_id, source, target, weight, attributes)
        else:
            if graph_id not in self.graphs:
                self.graphs[graph_id] = {'nodes': {}, 'edges': {}}
            self.graphs[graph_id]['edges'][edge_id] = {'source': source, 'target': target, 'weight': weight, 'attributes': attributes}

    def get_nodes(self, graph_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_nodes'):
            return self.storage.get_limit_graph_nodes(graph_id)
        return list(self.graphs.get(graph_id, {}).get('nodes', {}).values())

    def get_edges(self, graph_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_edges'):
            return self.storage.get_limit_graph_edges(graph_id)
        return list(self.graphs.get(graph_id, {}).get('edges', {}).values())

    def get_metadata(self, graph_id: str) -> Optional[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_metadata'):
            return self.storage.get_limit_graph_metadata(graph_id)
        return self.graphs.get(graph_id, {})


# ============================================================================
# NEW: MODP Optimizer (wrapper)
# ============================================================================
class MODPOptimizer:
    """
    Multi‑Objective Dynamic Programming solver that stores decision states/policies.
    This complements the NSGA-II optimizer; MODP here is used for scalarized selection
    among Pareto front points and for persisting evolved policies.
    """
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self.states = {}

    def add_state(self, state_id: str, problem_id: str, state_attributes: Dict[str, Any],
                  objective_values: Dict[str, float], stage: int) -> None:
        if self.storage and hasattr(self.storage, 'save_modp_state'):
            self.storage.save_modp_state(state_id, problem_id, state_attributes, objective_values, stage)
        else:
            if problem_id not in self.states:
                self.states[problem_id] = []
            self.states[problem_id].append({
                'state_id': state_id, 'state_attributes': state_attributes,
                'objective_values': objective_values, 'stage': stage
            })

    def add_policy(self, policy_id: str, problem_id: str, state_id: str,
                   action: str, expected_objectives: Dict[str, float]) -> None:
        if self.storage and hasattr(self.storage, 'save_modp_policy'):
            self.storage.save_modp_policy(policy_id, problem_id, state_id, action, expected_objectives)

    def get_states(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_states'):
            return self.storage.get_modp_states(problem_id)
        return self.states.get(problem_id, [])

    def get_policies(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_policies'):
            return self.storage.get_modp_policies(problem_id)
        return []


# ============================================================================
# NEW: RLHF Trainer
# ============================================================================
class RLHFTrainer:
    """
    Collects human preference pairs for warmup strategy choices.
    """
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self.pairs = []

    def record_pair(self, pair_id: str, prompt: str, chosen: str, rejected: str,
                    reward_diff: float, metadata: Optional[Dict] = None) -> None:
        if self.storage and hasattr(self.storage, 'save_preference_pair'):
            self.storage.save_preference_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)
        else:
            self.pairs.append({
                'pair_id': pair_id, 'prompt': prompt, 'chosen': chosen,
                'rejected': rejected, 'reward_diff': reward_diff, 'metadata': metadata
            })

    def get_pairs(self, limit: int = 100) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_preference_pairs'):
            return self.storage.get_preference_pairs(limit)
        return self.pairs[-limit:]

    def train_reward_model(self):
        pairs = self.get_pairs()
        if len(pairs) < 5:
            logger.info("Not enough preference pairs for RLHF training.")
            return
        logger.info(f"Training reward model on {len(pairs)} preference pairs...")


# ============================================================================
# NEW: MoE Gating Network
# ============================================================================
class MoEGatingNetwork:
    """
    Mixture-of-Experts gating for warmup strategy selection.
    Experts correspond to predefined strategies (preload, transfer, progressive, hybrid, federated).
    The gating network learns to select the best strategy for a given context.
    """
    def __init__(self, storage: Optional[Storage] = None, config: Optional[Dict] = None):
        self.storage = storage
        self.config = config or {}
        self.expert_names = self.config.get('expert_names', ['preload', 'transfer', 'progressive', 'hybrid', 'federated'])
        self.num_experts = len(self.expert_names)
        # State dimension: we'll use 14 features (from ColdStartState)
        self.gating_weights = np.random.randn(self.num_experts, 14)
        self._training_samples = []

    def _encode_state(self, state: Union['ColdStartState', Dict]) -> np.ndarray:
        if isinstance(state, dict):
            features = [
                min(state.get('carbon_budget', 0) / 1.0, 1.0),
                min(state.get('helium_budget', 0) / 1.0, 1.0),
                min(state.get('max_latency_ms', 0) / 1000.0, 1.0),
                min(state.get('carbon_intensity', 0) / 1000.0, 1.0),
                state.get('cache_utilization', 0),
                state.get('recent_hit_rate', 0),
                state.get('strategy_success_rates', {}).get('preload', 0.5),
                state.get('strategy_success_rates', {}).get('transfer', 0.5),
                state.get('strategy_success_rates', {}).get('progressive', 0.5),
                state.get('strategy_success_rates', {}).get('hybrid', 0.5),
                state.get('strategy_success_rates', {}).get('federated', 0.5),
                min(state.get('avg_warmup_time_ms', 0) / 1000.0, 1.0),
                state.get('avg_sustainability_score', 0),
            ]
        else:
            features = state.to_feature_vector()  # already 14-dim
        return np.array(features, dtype=np.float32)

    async def select_expert(self, state: Union['ColdStartState', Dict]) -> Tuple[str, np.ndarray]:
        x = self._encode_state(state)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        expert_idx = np.argmax(probs)
        selected = self.expert_names[expert_idx]
        if self.storage and hasattr(self.storage, 'log_routing_decision'):
            sample_id = hashlib.sha256(str(state).encode()).hexdigest()[:16]
            self.storage.log_routing_decision(str(uuid.uuid4()), sample_id, selected, float(probs[expert_idx]))
        return selected, probs

    async def add_training_sample(self, state: Union['ColdStartState', Dict], selected_expert: str, reward: float):
        x = self._encode_state(state)
        expert_idx = self.expert_names.index(selected_expert)
        target = np.zeros(self.num_experts)
        target[expert_idx] = 1.0
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        grad = (probs - target)[:, None] * x[None, :]
        self.gating_weights -= 0.1 * grad


# ============================================================================
# Distillation components (unchanged, but included for completeness)
# ============================================================================
@dataclass
class ColdStartState:
    """State for the distillation agent."""
    expert_type: str
    urgency: str
    carbon_budget: float
    helium_budget: float
    max_latency_ms: float
    carbon_intensity: float
    cache_utilization: float
    recent_hit_rate: float
    strategy_success_rates: Dict[str, float]
    avg_warmup_time_ms: float
    avg_sustainability_score: float

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 14‑dim numeric feature vector."""
        type_map = {'energy': 0, 'data': 1, 'iot': 2, 'quantum': 3, 'general': 4}
        type_onehot = [0.0] * 5
        type_onehot[type_map.get(self.expert_type, 4)] = 1.0

        urgency_map = {'critical': 0, 'high': 1, 'normal': 2, 'low': 3}
        urgency_onehot = [0.0] * 4
        urgency_onehot[urgency_map.get(self.urgency, 2)] = 1.0

        success_preload = self.strategy_success_rates.get('preload', 0.5)
        success_transfer = self.strategy_success_rates.get('transfer', 0.5)
        success_progressive = self.strategy_success_rates.get('progressive', 0.5)
        success_hybrid = self.strategy_success_rates.get('hybrid', 0.5)
        success_federated = self.strategy_success_rates.get('federated', 0.5)

        features = [
            min(self.carbon_budget / 1.0, 1.0),
            min(self.helium_budget / 1.0, 1.0),
            min(self.max_latency_ms / 1000.0, 1.0),
            min(self.carbon_intensity / 1000.0, 1.0),
            self.cache_utilization,
            self.recent_hit_rate,
            success_preload,
            success_transfer,
            success_progressive,
            success_hybrid,
            success_federated,
            min(self.avg_warmup_time_ms / 1000.0, 1.0),
            self.avg_sustainability_score,
        ] + type_onehot + urgency_onehot

        return np.array(features, dtype=np.float32)


class Teacher(ABC):
    @abstractmethod
    def predict(self, state: ColdStartState) -> np.ndarray:
        pass

    @abstractmethod
    def confidence(self, state: ColdStartState) -> float:
        pass


class StrategyRuleBasedTeacher(Teacher):
    STRATEGIES = ['preload', 'transfer', 'progressive', 'hybrid', 'federated']

    def predict(self, state: ColdStartState) -> np.ndarray:
        probs = np.ones(5) * 0.1
        if state.recent_hit_rate > 0.8:
            probs[0] = 0.8
        elif state.expert_type == 'quantum' and state.max_latency_ms < 100:
            probs[3] = 0.7
        elif state.carbon_intensity > 500:
            probs[4] = 0.6
        elif state.urgency == 'critical':
            probs[1] = 0.7
        else:
            probs[2] = 0.6
        return probs / probs.sum()

    def confidence(self, state: ColdStartState) -> float:
        if state.recent_hit_rate > 0.8:
            return 0.6
        return 0.4


class StrategyHistoricalMLTeacher(Teacher):
    def __init__(self, model_path: Optional[str] = None):
        self.model = None
        self.label_encoder = None
        self.model_path = model_path or Path(ColdStartConfig().historical_model_path)
        if self.model_path.exists():
            try:
                with open(self.model_path, 'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
                logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")

    def predict(self, state: ColdStartState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: ColdStartState) -> float:
        return 0.7 if self.model is not None else 0.0


class StrategyStatefulQTeacher(Teacher):
    def __init__(self, lr: float = 0.1):
        self.lr = lr
        self.weights = np.zeros((14, 5))
        self._load_state()

    def _load_state(self):
        path = Path(ColdStartConfig().q_weights_path)
        if path.exists():
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                logger.info(f"Loaded Q‑teacher weights from {path}")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        path = Path(ColdStartConfig().q_weights_path)
        with open(path, 'w') as f:
            json.dump(self.weights.tolist(), f, indent=2)

    def predict(self, state: ColdStartState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: ColdStartState) -> float:
        return 0.5

    def update(self, state: ColdStartState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()


class DistillationStudent:
    def __init__(self, feature_dim: int = 14, n_classes: int = 5, lr: float = 0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray, num_classes: int) -> np.ndarray:
        if num_classes != self.n_classes:
            new_weights = np.zeros((self.weights.shape[0], num_classes))
            new_biases = np.zeros(num_classes)
            min_dim = min(self.n_classes, num_classes)
            new_weights[:, :min_dim] = self.weights[:, :min_dim]
            new_biases[:min_dim] = self.biases[:min_dim]
            self.weights = new_weights
            self.biases = new_biases
            self.n_classes = num_classes
        logits = state_vector @ self.weights + self.biases
        max_logit = np.max(logits)
        exp_logits = np.exp(logits - max_logit)
        return exp_logits / exp_logits.sum()

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action: int, distill_weight: float = 0.7, rl_weight: float = 0.3):
        current_probs = self.predict_proba(state_vector, self.n_classes)
        logits = state_vector @ self.weights + self.biases

        grad_distill = -(teacher_probs - current_probs)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current_probs)

        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1


class ReplayBuffer:
    def __init__(self, max_size: int = 2000):
        self.buffer = deque(maxlen=max_size)

    def push(self, state_vec: np.ndarray, action: int, reward: float,
             next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))

    def sample(self, batch_size: int = 32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (np.array(states), actions, np.array(rewards),
                np.array(next_states), np.array(teacher_probs))

    def __len__(self):
        return len(self.buffer)


class DistillationStrategyOptimizer:
    """
    Multi‑teacher on‑policy distillation agent for warmup strategy selection.
    Strategies: preload, transfer, progressive, hybrid, federated.
    """
    STRATEGIES = ['preload', 'transfer', 'progressive', 'hybrid', 'federated']

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            StrategyRuleBasedTeacher(),
            StrategyHistoricalMLTeacher(),
            StrategyStatefulQTeacher()
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_strategy(self, state: ColdStartState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        n = 5

        teacher_probs = np.zeros(n)
        total_conf = 0.0
        for teacher in self.teachers:
            prob = teacher.predict(state)
            conf = teacher.confidence(state)
            if len(prob) != n:
                if len(prob) < n:
                    prob = np.pad(prob, (0, n - len(prob)), 'constant')
                else:
                    prob = prob[:n]
            teacher_probs += prob * conf
            total_conf += conf
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(n) / n

        student_probs = self.student.predict_proba(state_vec, n)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, n - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = np.argmax(combined)

        return self.STRATEGIES[action_idx], action_idx, state_vec, teacher_probs

    async def update(self, state_vec: np.ndarray, action_idx: int, reward: float,
                     next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = self.replay_buffer.sample(8)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i])

    def get_stats(self) -> Dict:
        return {'student_counter': self.student.counter, 'buffer_size': len(self.replay_buffer)}


# ============================================================================
# NEW: Multi‑Objective Strategy Weight Optimizer (NSGA‑II)
# ============================================================================
@dataclass
class MOPDStrategyWeights:
    """A weight vector for the five warmup strategies, with its objective values."""
    vector_id: str
    weights: Dict[str, float]  # keys: preload, transfer, progressive, hybrid, federated (sum to 1)
    objectives: Dict[str, float]  # achieved values (higher is better)
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'vector_id': self.vector_id,
            'weights': self.weights,
            'objectives': self.objectives,
            'scalarised_score': self.scalarised_score,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDStrategyWeights':
        return cls(**data)


class NSGAIIStrategyOptimizer:
    """
    Multi‑objective genetic algorithm for evolving continuous strategy weights.
    Decision variables: weights for the five warmup strategies (sum to 1).
    Objectives (maximized): minimize latency, minimize carbon, maximize cache hit rate, maximize sustainability.
    The evaluation function is provided by the ColdStartOptimizer, which simulates or replays historical outcomes.
    """

    def __init__(
        self,
        evaluate_func: Callable[[Dict[str, float]], Awaitable[Dict[str, float]]],
        population_size: int = 20,
        generations: int = 10,
        mutation_rate: float = 0.2,
        crossover_rate: float = 0.8,
        tournament_size: int = 3,
        objective_weights: Optional[Dict[str, float]] = None,
        dynamic_weights: bool = True,
    ):
        self.evaluate_func = evaluate_func
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.objective_weights = objective_weights or {
            'latency': 0.4,
            'carbon': 0.3,
            'cache_hit': 0.2,
            'sustainability': 0.1,
        }
        self.dynamic_weights = dynamic_weights

        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.pareto_front: List[MOPDStrategyWeights] = []
        self._eval_cache: Dict[Tuple[float, ...], Dict[str, float]] = {}

    def _random_individual(self) -> Dict[str, float]:
        keys = ['preload', 'transfer', 'progressive', 'hybrid', 'federated']
        w = {k: random.random() for k in keys}
        total = sum(w.values())
        if total > 0:
            w = {k: v / total for k, v in w.items()}
        return w

    def _crossover(self, p1: Dict, p2: Dict) -> Dict:
        child = {}
        for key in p1:
            if random.random() < 0.5:
                u = random.random()
                if u <= 0.5:
                    beta = (2 * u) ** (1 / (20 + 1))
                else:
                    beta = (1 / (2 * (1 - u))) ** (1 / (20 + 1))
                child[key] = max(0.0, min(1.0, 0.5 * ((1 + beta) * p1[key] + (1 - beta) * p2[key])))
            else:
                child[key] = p1[key] if random.random() < 0.5 else p2[key]
        total = sum(child.values())
        if total > 0:
            child = {k: v / total for k, v in child.items()}
        return child

    def _mutate(self, ind: Dict) -> Dict:
        mutant = ind.copy()
        for key in mutant:
            if random.random() < self.mutation_rate:
                u = random.random()
                if u < 0.5:
                    delta = (2 * u) ** (1 / (20 + 1)) - 1
                else:
                    delta = 1 - (2 * (1 - u)) ** (1 / (20 + 1))
                mutant[key] = mutant[key] + delta
                mutant[key] = max(0.0, min(1.0, mutant[key]))
        total = sum(mutant.values())
        if total > 0:
            mutant = {k: v / total for k, v in mutant.items()}
        return mutant

    def _fast_non_dominated_sort(self, points: List[MOPDStrategyWeights]) -> List[List[MOPDStrategyWeights]]:
        fronts = []
        domination_count = {id(p): 0 for p in points}
        dominated_solutions = {id(p): [] for p in points}
        for i, p in enumerate(points):
            p_obj = p.objectives
            for j, q in enumerate(points):
                if i == j:
                    continue
                q_obj = q.objectives
                if all(p_obj[k] >= q_obj[k] for k in p_obj) and any(p_obj[k] > q_obj[k] for k in p_obj):
                    dominated_solutions[id(p)].append(q)
                elif all(q_obj[k] >= p_obj[k] for k in q_obj) and any(q_obj[k] > p_obj[k] for k in q_obj):
                    domination_count[id(p)] += 1
            if domination_count[id(p)] == 0:
                if not fronts:
                    fronts.append([])
                fronts[0].append(p)
        i = 0
        while i < len(fronts):
            next_front = []
            for p in fronts[i]:
                for q in dominated_solutions[id(p)]:
                    domination_count[id(q)] -= 1
                    if domination_count[id(q)] == 0:
                        next_front.append(q)
            if next_front:
                fronts.append(next_front)
            i += 1
        return fronts

    def _crowding_distance(self, front: List[MOPDStrategyWeights]) -> Dict[int, float]:
        if not front:
            return {}
        distances = {id(p): 0.0 for p in front}
        objective_keys = list(front[0].objectives.keys())
        for obj in objective_keys:
            sorted_front = sorted(front, key=lambda x: x.objectives[obj])
            distances[id(sorted_front[0])] = float('inf')
            distances[id(sorted_front[-1])] = float('inf')
            obj_min = sorted_front[0].objectives[obj]
            obj_max = sorted_front[-1].objectives[obj]
            if obj_max == obj_min:
                continue
            for i in range(1, len(sorted_front) - 1):
                distances[id(sorted_front[i])] += (sorted_front[i+1].objectives[obj] - sorted_front[i-1].objectives[obj]) / (obj_max - obj_min)
        return distances

    def _tournament_selection(self, population: List[Dict], fronts: List[List[MOPDStrategyWeights]],
                              crowding: Dict[int, float]) -> Dict:
        candidates = random.sample(population, self.tournament_size)
        ind_to_point = {}
        for ind, point in zip(population, self._all_points):
            ind_to_point[id(ind)] = point
        best = candidates[0]
        best_rank = float('inf')
        best_crowding = -float('inf')
        for cand in candidates:
            point = ind_to_point.get(id(cand))
            if not point:
                continue
            rank = len(fronts)
            for fi, front in enumerate(fronts):
                if point in front:
                    rank = fi
                    break
            cd = crowding.get(id(point), 0)
            if rank < best_rank or (rank == best_rank and cd > best_crowding):
                best = cand
                best_rank = rank
                best_crowding = cd
        return best

    def _compute_dynamic_weights(self) -> Dict[str, float]:
        weights = self.objective_weights.copy()
        if not self.dynamic_weights or not self.pareto_front:
            return weights
        obj_keys = list(weights.keys())
        avg = {k: np.mean([p.objectives[k] for p in self.pareto_front]) for k in obj_keys}
        max_val = {k: np.max([p.objectives[k] for p in self.pareto_front]) for k in obj_keys}
        for k in obj_keys:
            if max_val[k] > 0 and avg[k] < 0.5 * max_val[k]:
                weights[k] = min(0.6, weights.get(k, 0.0) * 1.5)
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        return weights

    def _select_best_from_pareto(self, pareto: List[MOPDStrategyWeights], weights: Dict[str, float]) -> Optional[MOPDStrategyWeights]:
        if not pareto:
            return None
        obj_keys = list(weights.keys())
        max_vals = {k: max(p.objectives[k] for p in pareto) for k in obj_keys}
        min_vals = {k: min(p.objectives[k] for p in pareto) for k in obj_keys}
        ranges = {k: max_vals[k] - min_vals[k] if max_vals[k] != min_vals[k] else 1.0 for k in obj_keys}
        best = None
        best_score = -float('inf')
        for p in pareto:
            score = 0.0
            for k in obj_keys:
                val = p.objectives[k]
                norm = (val - min_vals[k]) / ranges[k] if ranges[k] > 0 else 1.0
                score += weights.get(k, 0.0) * norm
            p.scalarised_score = score
            if score > best_score:
                best_score = score
                best = p
        return best

    async def evolve(self) -> List[MOPDStrategyWeights]:
        population = [self._random_individual() for _ in range(self.population_size)]
        points = []
        eval_tasks = [self.evaluate_func(ind) for ind in population]
        eval_results = await asyncio.gather(*eval_tasks)
        for ind, obj in zip(population, eval_results):
            point = MOPDStrategyWeights(vector_id=str(uuid.uuid4()), weights=ind, objectives=obj)
            points.append(point)
            self._eval_cache[tuple(sorted(ind.items()))] = obj
        self._all_points = points
        for gen in range(self.generations):
            fronts = self._fast_non_dominated_sort(points)
            crowding = {}
            for front in fronts:
                front_crowding = self._crowding_distance(front)
                crowding.update(front_crowding)
            offspring = []
            while len(offspring) < self.population_size:
                parent1 = self._tournament_selection(population, fronts, crowding)
                parent2 = self._tournament_selection(population, fronts, crowding)
                if random.random() < self.crossover_rate:
                    child = self._crossover(parent1, parent2)
                else:
                    child = copy.deepcopy(parent1)
                child = self._mutate(child)
                offspring.append(child)
            child_tasks = [self.evaluate_func(ind) for ind in offspring]
            child_results = await asyncio.gather(*child_tasks)
            child_points = []
            for ind, obj in zip(offspring, child_results):
                point = MOPDStrategyWeights(vector_id=str(uuid.uuid4()), weights=ind, objectives=obj)
                child_points.append(point)
                self._eval_cache[tuple(sorted(ind.items()))] = obj
            combined_inds = population + offspring
            combined_points = points + child_points
            unique_pairs = {}
            for ind, p in zip(combined_inds, combined_points):
                key = tuple(sorted(ind.items()))
                unique_pairs[key] = (ind, p)
            population = [v[0] for v in unique_pairs.values()]
            points = [v[1] for v in unique_pairs.values()]
            self._all_points = points
            fronts = self._fast_non_dominated_sort(points)
            new_population = []
            new_points = []
            for front in fronts:
                if len(new_population) + len(front) <= self.population_size:
                    for p in front:
                        for ind, p2 in zip(population, points):
                            if p2 is p:
                                new_population.append(ind)
                                new_points.append(p)
                                break
                else:
                    crowding = self._crowding_distance(front)
                    sorted_front = sorted(front, key=lambda x: crowding.get(id(x), 0), reverse=True)
                    for p in sorted_front:
                        if len(new_population) >= self.population_size:
                            break
                        for ind, p2 in zip(population, points):
                            if p2 is p:
                                new_population.append(ind)
                                new_points.append(p)
                                break
            population = new_population[:self.population_size]
            points = new_points[:self.population_size]
            self._all_points = points
            fronts = self._fast_non_dominated_sort(points)
            if fronts:
                self.pareto_front = fronts[0]
            logger.info(f"Generation {gen+1}/{self.generations}: Pareto front size={len(self.pareto_front)}")
        weights = self._compute_dynamic_weights()
        best = self._select_best_from_pareto(self.pareto_front, weights)
        if best:
            self.best_individual = best.weights
            self.best_fitness = best.scalarised_score
        return self.pareto_front


# ============================================================================
# Enhanced Cold Start Optimizer (Main Class)
# ============================================================================

class ColdStartOptimizer:
    """
    Enhanced Cold Start Optimizer v3.4.0 with adaptive strategy selection via distillation
    and multi‑objective evolutionary optimization (NSGA‑II) for global weight refinement.
    Added LIMIT Graph, MODP, RLHF, and MoE gating components.
    """

    def __init__(self, config: Optional[ColdStartConfig] = None, **kwargs):
        if config is None:
            config = ColdStartConfig(**{
                k: v for k, v in kwargs.items()
                if k in ColdStartConfig.model_fields
            })
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

        # Concurrency locks
        self._cache_lock = asyncio.Lock()
        self._history_lock = asyncio.Lock()
        self._similarity_lock = asyncio.Lock()

        # Initialize sub-modules
        self.federated_manager = FederatedCheckpointManager(config) if self.enable_federated else None
        self.ml_predictor = MLDemandPredictor(config) if self.enable_ml_demand else None
        self.strategy_selector = CarbonAwareStrategySelector(config) if self.enable_carbon_aware else None
        self.helium_dashboard = HeliumEfficiencyDashboard(config) if self.enable_helium_tracking else None
        self.eviction_manager = IntelligentEvictionManager(config, self.ml_predictor) if self.enable_intelligent_eviction else None

        # Persistence and telemetry
        self.persistence = ColdStartPersistenceManager(config) if self.enable_persistence else None
        self.telemetry = ColdStartTelemetry(config) if self.enable_telemetry else None

        # NEW: Distillation strategy optimizer
        self.strategy_optimizer = DistillationStrategyOptimizer({
            'distillation_epsilon': config.distillation_epsilon,
            'distillation_train_every': config.distillation_train_every,
            'distillation_replay_size': config.distillation_replay_size,
            'distillation_learning_rate': config.distillation_learning_rate,
        })

        # MOEA globals
        self.moea_enabled = config.moea_enabled
        self.moea_optimizer: Optional[NSGAIIStrategyOptimizer] = None
        self.global_best_weights: Optional[Dict[str, float]] = None
        self.pareto_front: List[MOPDStrategyWeights] = []
        self._moea_task: Optional[asyncio.Task] = None

        if self.moea_enabled:
            self._moea_task = asyncio.create_task(self._moea_loop())

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

        # Background preloader task
        self._preloader_task: Optional[asyncio.Task] = None
        self._start_background_preloader()

        # NEW v3.4.0 components
        self.storage = kwargs.get('storage', None)  # optional central storage
        self.limit_graph_manager = LimitGraphManager(self.storage) if config.enable_limit_graph else None
        self.modp_solver = MODPOptimizer(self.storage) if config.enable_modp else None
        self.rlhf_trainer = RLHFTrainer(self.storage) if config.enable_rlhf else None
        self.moe_gating = MoEGatingNetwork(
            self.storage,
            {'expert_names': self.strategy_optimizer.STRATEGIES}
        ) if config.enable_moe else None

        # Initialize LIMIT Graph if enabled
        if self.limit_graph_manager:
            self._init_limit_graph()

        # Load state if persistence enabled
        if self.enable_persistence and self.persistence:
            asyncio.create_task(self._load_state())

        # Interaction tracking for distillation
        self.interaction_log: List[Dict] = []
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None

        logger.info(f"Enhanced Cold Start Optimizer v3.4.0 initialized with cache size {self.cache_size}")

    def _init_limit_graph(self):
        graph_id = "cold_start_strategies"
        if not self.limit_graph_manager.get_metadata(graph_id):
            self.limit_graph_manager.create_graph(graph_id, "Warmup Strategy Relationships", {})
            for strat in self.strategy_optimizer.STRATEGIES:
                self.limit_graph_manager.add_node(graph_id, f"strategy_{strat}", strat, {})
            # Add edges from each strategy to others? Just a simple chain
            for i in range(len(self.strategy_optimizer.STRATEGIES) - 1):
                src = self.strategy_optimizer.STRATEGIES[i]
                dst = self.strategy_optimizer.STRATEGIES[i+1]
                self.limit_graph_manager.add_edge(graph_id, f"edge_{src}_{dst}", f"strategy_{src}", f"strategy_{dst}", 1.0, {})

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
        self._preloader_task = asyncio.create_task(self._background_preload_loop())

    async def _background_preload_loop(self):
        while True:
            try:
                predictions = {}
                if self.enable_ml_demand and self.ml_predictor:
                    predictions = await self.ml_predictor.predict_demand(horizon_minutes=5)

                # Preload high-probability experts
                for expert_id, probability in list(predictions.items()):
                    if probability > self.preload_threshold:
                        if expert_id not in self.checkpoint_cache:
                            await self.preload_expert(expert_id)

                # Federated cache sync
                if self.enable_federated and self.federated_manager:
                    async with self._cache_lock:
                        self.checkpoint_cache = await self.federated_manager.sync_cache_with_peers(
                            self.checkpoint_cache
                        )

                # Intelligent eviction
                if self.enable_intelligent_eviction and self.eviction_manager:
                    if len(self.checkpoint_cache) > self.cache_size * 0.9:
                        num_to_evict = len(self.checkpoint_cache) - int(self.cache_size * 0.8)
                        async with self._cache_lock:
                            cache_snapshot = dict(self.checkpoint_cache)
                        candidates = await self.eviction_manager.select_eviction_candidates(
                            cache_snapshot, predictions, num_to_evict
                        )
                        async with self._cache_lock:
                            for expert_id in candidates:
                                if expert_id in self.checkpoint_cache:
                                    del self.checkpoint_cache[expert_id]
                                    logger.info(f"Intelligently evicted {expert_id}")
                                    if self.telemetry:
                                        self.telemetry.increment('cs_evictions')
                                    self.eviction_manager.eviction_history.append({
                                        'expert_id': expert_id,
                                        'timestamp': datetime.utcnow().isoformat()
                                    })

                # Clean old checkpoints
                await self._cleanup_checkpoints()

                # Telemetry
                if self.telemetry:
                    async with self._cache_lock:
                        self.telemetry.gauge('cs_cache_size', len(self.checkpoint_cache))
                        self.telemetry.gauge('cs_hit_rate', self._calculate_hit_rate())
                        self.telemetry.gauge('cs_sustainability_score', self.sustainability_score)
                        self.telemetry.gauge('cs_carbon_saved_kg', self._calculate_carbon_saved())
                        self.telemetry.gauge('cs_time_saved_ms', self._calculate_time_saved())

                await asyncio.sleep(60)
            except asyncio.CancelledError:
                logger.info("Background preloader cancelled")
                break
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
                    'limit_graph': self.limit_graph_manager is not None,
                    'modp': self.modp_solver is not None,
                    'rlhf': self.rlhf_trainer is not None,
                    'moe': self.moe_gating is not None,
                },
                'cache_size': len(self.checkpoint_cache),
                'max_size': self.cache_size,
                'hit_rate': self._calculate_hit_rate(),
                'carbon_saved_kg': self._calculate_carbon_saved(),
                'sustainability_score': self.sustainability_score,
            }
        }

    # ============================================================================
    # MOEA Background Loop and Update
    # ============================================================================
    async def _moea_loop(self):
        interval = self.config.moea_interval_seconds
        while True:
            try:
                await asyncio.sleep(interval)
                await self.run_moea_update()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"MOEA loop error: {e}")
                await asyncio.sleep(60)

    async def run_moea_update(self) -> List[MOPDStrategyWeights]:
        """
        Run NSGA‑II to evolve a Pareto front of strategy weights.
        Uses historical warmup outcomes to estimate objectives.
        """
        if not self.moea_enabled or len(self.warmup_history) < 20:
            return []

        async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
            # Compute objectives from recent warmup history.
            strategy_metrics = {s: [] for s in self.strategy_optimizer.STRATEGIES}
            for event in self.warmup_history[-200:]:
                method = event.get('method', 'unknown')
                if method in strategy_metrics:
                    latency = event.get('load_time_ms', event.get('total_time_ms', 500))
                    carbon = event.get('carbon_footprint_kg', 0.1)
                    sustainability = event.get('sustainability_score', 0.5)
                    hit = 1.0 if method in ('checkpoint', 'transfer_learning') else 0.0
                    strategy_metrics[method].append({
                        'latency': latency,
                        'carbon': carbon,
                        'sustainability': sustainability,
                        'hit': hit,
                    })
            objectives = {}
            for metric in ['latency', 'carbon', 'sustainability', 'hit']:
                weighted_values = []
                for strategy, weight in weights.items():
                    if strategy in strategy_metrics and strategy_metrics[strategy]:
                        avg_val = np.mean([m[metric] for m in strategy_metrics[strategy]])
                        weighted_values.append(weight * avg_val)
                    else:
                        weighted_values.append(weight * 0.5)
                objectives[metric] = sum(weighted_values)

            max_lat = 1000.0
            max_carbon = 1.0
            return {
                'latency': 1.0 - min(1.0, objectives['latency'] / max_lat),
                'carbon': 1.0 - min(1.0, objectives['carbon'] / max_carbon),
                'cache_hit': objectives['hit'],
                'sustainability': objectives['sustainability'],
            }

        self.moea_optimizer = NSGAIIStrategyOptimizer(
            evaluate_func=evaluate,
            population_size=self.config.moea_population_size,
            generations=self.config.moea_generations,
            mutation_rate=self.config.moea_mutation_rate,
            crossover_rate=self.config.moea_crossover_rate,
            tournament_size=self.config.moea_tournament_size,
            objective_weights=self.config.moea_objective_weights,
            dynamic_weights=self.config.moea_dynamic_weights,
        )
        pareto = await self.moea_optimizer.evolve()
        self.pareto_front = pareto
        if pareto:
            weights = self.moea_optimizer._compute_dynamic_weights()
            best = self.moea_optimizer._select_best_from_pareto(pareto, weights)
            if best:
                self.global_best_weights = best.weights
                logger.info(f"MOEA selected best weights: {best.weights}")
                # MODP: store state
                if self.modp_solver:
                    self.modp_solver.add_state(
                        state_id=f"moea_best_{time.time()}",
                        problem_id="cold_start_strategy_evolution",
                        state_attributes={'weights': best.weights},
                        objective_values=best.objectives,
                        stage=1
                    )
                # LIMIT Graph: add node for best vector
                if self.limit_graph_manager:
                    self.limit_graph_manager.add_node(
                        "cold_start_strategies",
                        f"vector_{best.vector_id}",
                        "best_weight_vector",
                        {'weights': best.weights}
                    )
        return pareto

    # ============================================================================
    # Core Initialization Methods (Enhanced with new components)
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

        # Build state for distillation
        state = self._build_state(
            expert_type, urgency, carbon_budget, helium_budget, max_latency_ms,
            carbon_intensity
        )

        # Decide strategy: use MoE if available, else distillation
        if self.moe_gating:
            expert_name, _ = await self.moe_gating.select_expert(state)
            strategy = expert_name if expert_name in self.strategy_optimizer.STRATEGIES else 'preload'
            action_idx = self.strategy_optimizer.STRATEGIES.index(strategy)
            state_vec = state.to_feature_vector()
            teacher_probs = np.ones(5) / 5
            self._last_selected_expert = expert_name
        else:
            strategy, action_idx, state_vec, teacher_probs = await self.strategy_optimizer.select_strategy(state, exploration=True)

        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs

        logger.info(f"Selected strategy: {strategy} for {expert_id}")

        # Blend with MOEA global weights if available
        if self.global_best_weights is not None:
            one_hot = np.zeros(5)
            one_hot[action_idx] = 1.0
            moea_probs = np.array([self.global_best_weights[s] for s in self.strategy_optimizer.STRATEGIES])
            moea_probs = moea_probs / moea_probs.sum()
            blended = 0.7 * moea_probs + 0.3 * one_hot
            blended = blended / blended.sum()
            action_idx = np.argmax(blended)
            strategy = self.strategy_optimizer.STRATEGIES[action_idx]
            logger.info(f"Blended strategy after MOEA: {strategy}")

        # Check cache first
        async with self._cache_lock:
            if expert_id in self.checkpoint_cache:
                logger.info(f"Cache hit for {expert_id}")
                checkpoint = self.checkpoint_cache[expert_id]
                checkpoint.last_used = datetime.utcnow()
                checkpoint.usage_count += 1
                self.checkpoint_cache.move_to_end(expert_id)
                checkpoint.sustainability_score = self._calculate_checkpoint_sustainability(checkpoint)
                result = await self._load_from_checkpoint(checkpoint, max_latency_ms)
                reward = 0.8 + 0.1 * checkpoint.sustainability_score
                await self._update_agent(state_vec, action_idx, reward, state)
                # Update MoE if used
                if self.moe_gating and hasattr(self, '_last_selected_expert'):
                    await self.moe_gating.add_training_sample(state, self._last_selected_expert, reward)
                return result

        # Execute the selected strategy
        if strategy == 'preload':
            result = await self._preload_initialize(expert_id, expert_type, max_latency_ms)
        elif strategy == 'transfer':
            result = await self._transfer_initialize(expert_id, expert_type, max_latency_ms)
        elif strategy == 'progressive':
            result = await self._progressive_initialize(
                expert_id, expert_type, carbon_budget, helium_budget, max_latency_ms, 'progressive'
            )
        elif strategy == 'hybrid':
            result = await self._progressive_initialize(
                expert_id, expert_type, carbon_budget, helium_budget, max_latency_ms, 'hybrid'
            )
        elif strategy == 'federated':
            result = await self._federated_initialize(expert_id, expert_type, max_latency_ms)
        else:
            result = await self._progressive_initialize(
                expert_id, expert_type, carbon_budget, helium_budget, max_latency_ms, 'progressive'
            )

        # Compute reward
        reward = self._compute_reward(result, start_time, max_latency_ms, carbon_intensity)

        # Update agent (distillation and MoE)
        await self._update_agent(state_vec, action_idx, reward, state)

        # Log interaction
        self._log_interaction(state, strategy, reward, result)

        # RLHF: occasionally record preference pair
        if self.rlhf_trainer and random.random() < 0.05:
            chosen_strategy = strategy
            rejected_strategy = random.choice([s for s in self.strategy_optimizer.STRATEGIES if s != chosen_strategy])
            self.rlhf_trainer.record_pair(
                pair_id=str(uuid.uuid4()),
                prompt=f"Which warmup strategy is best for {expert_id}?",
                chosen=chosen_strategy,
                rejected=rejected_strategy,
                reward_diff=reward,
                metadata={'expert_id': expert_id, 'expert_type': expert_type}
            )

        # MODP: record state and policy
        if self.modp_solver:
            problem_id = "cold_start_initialization"
            state_id = f"{expert_id}_{datetime.utcnow().isoformat()}_{strategy}"
            self.modp_solver.add_state(
                state_id=state_id,
                problem_id=problem_id,
                state_attributes={'expert_id': expert_id, 'strategy': strategy},
                objective_values={'time': result.get('load_time_ms', 500), 'carbon': result.get('carbon_footprint_kg', 0.1), 'sustainability': result.get('sustainability_score', 0.5)},
                stage=0
            )
            self.modp_solver.add_policy(
                policy_id=f"policy_{state_id}",
                problem_id=problem_id,
                state_id=state_id,
                action=strategy,
                expected_objectives={'time': 0.0, 'carbon': 0.0, 'sustainability': 0.0}
            )

        # Share checkpoint with federation if applicable
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

    # ---------- Helper methods (unchanged) ----------
    def _build_state(self, expert_type, urgency, carbon_budget, helium_budget, max_latency_ms, carbon_intensity):
        async with self._cache_lock:
            cache_util = len(self.checkpoint_cache) / self.cache_size
        hit_rate = self._calculate_hit_rate()

        success_rates = {'preload': 0.5, 'transfer': 0.5, 'progressive': 0.5, 'hybrid': 0.5, 'federated': 0.5}
        for event in self.warmup_history[-100:]:
            method = event.get('method', 'unknown')
            if method in success_rates:
                success_rates[method] = min(1.0, success_rates.get(method, 0.5) + 0.01)

        if self.warmup_history:
            times = [h.get('load_time_ms', h.get('total_time_ms', 0)) for h in self.warmup_history[-50:]]
            avg_time = np.mean(times) if times else 500.0
            scores = [h.get('sustainability_score', 0.5) for h in self.warmup_history[-50:]]
            avg_sustainability = np.mean(scores) if scores else 0.5
        else:
            avg_time = 500.0
            avg_sustainability = 0.5

        return ColdStartState(
            expert_type=expert_type,
            urgency=urgency,
            carbon_budget=carbon_budget,
            helium_budget=helium_budget,
            max_latency_ms=max_latency_ms,
            carbon_intensity=carbon_intensity,
            cache_utilization=cache_util,
            recent_hit_rate=hit_rate,
            strategy_success_rates=success_rates,
            avg_warmup_time_ms=avg_time,
            avg_sustainability_score=avg_sustainability,
        )

    def _compute_reward(self, result, start_time, max_latency_ms, carbon_intensity):
        time_taken = result.get('load_time_ms', result.get('total_time_ms', 500))
        time_score = 1.0 - min(1.0, time_taken / max_latency_ms)
        sustainability = result.get('sustainability_score', 0.5)
        success_bonus = 0.2 if result.get('initialized', False) else 0.0
        carbon_score = 1.0 - min(1.0, carbon_intensity / 1000.0)
        reward = 0.4 * time_score + 0.3 * sustainability + 0.2 * success_bonus + 0.1 * carbon_score
        return max(0.0, min(1.0, reward))

    async def _update_agent(self, state_vec, action_idx, reward, state):
        next_state_vec = state.to_feature_vector()
        await self.strategy_optimizer.update(
            state_vec,
            action_idx,
            reward,
            next_state_vec,
            self.last_teacher_probs
        )

    def _log_interaction(self, state, strategy, reward, result):
        entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'strategy': strategy,
            'reward': reward,
            'result': result,
            'state_vector': state.to_feature_vector().tolist(),
        }
        self.interaction_log.append(entry)
        log_path = Path(self.config.interaction_logs_path)
        df_log = pd.DataFrame([entry])
        if log_path.exists():
            df_log.to_csv(log_path, mode='a', header=False, index=False)
        else:
            df_log.to_csv(log_path, index=False)

    # ---------- Strategy implementations ----------
    async def _load_from_checkpoint(self, checkpoint, max_latency_ms):
        load_start = datetime.utcnow()
        await asyncio.sleep(0.001)
        load_time = (datetime.utcnow() - load_start).total_seconds() * 1000
        checkpoint.sustainability_score = self._calculate_checkpoint_sustainability(checkpoint)

        async with self._history_lock:
            self.warmup_history.append({
                'expert_id': checkpoint.expert_id,
                'method': 'checkpoint',
                'load_time_ms': load_time,
                'sustainability_score': checkpoint.sustainability_score,
                'timestamp': datetime.utcnow().isoformat()
            })

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

    async def _transfer_initialize(self, target_id, target_type, max_latency_ms):
        similar = self._find_similar_expert(target_id, target_type)
        if not similar or similar not in self.checkpoint_cache:
            return await self._progressive_initialize(
                target_id, target_type, 0.1, 0.1, max_latency_ms, 'progressive'
            )

        async with self._cache_lock:
            source_checkpoint = self.checkpoint_cache[similar]

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

        async with self._cache_lock:
            self._add_to_cache(target_id, target_checkpoint)

        async with self._history_lock:
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

    async def _preload_initialize(self, expert_id, expert_type, max_latency_ms):
        checkpoint = await self._create_checkpoint(expert_id, {'type': expert_type})
        async with self._cache_lock:
            self._add_to_cache(expert_id, checkpoint)
        return await self._load_from_checkpoint(checkpoint, max_latency_ms)

    async def _federated_initialize(self, expert_id, expert_type, max_latency_ms):
        if not self.enable_federated or not self.federated_manager:
            return await self._progressive_initialize(
                expert_id, expert_type, 0.1, 0.1, max_latency_ms, 'progressive'
            )
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
                async with self._cache_lock:
                    self._add_to_cache(expert_id, checkpoint)
                return await self._load_from_checkpoint(checkpoint, max_latency_ms)
        return await self._progressive_initialize(
            expert_id, expert_type, 0.1, 0.1, max_latency_ms, 'progressive'
        )

    async def _progressive_initialize(self, expert_id, expert_type, carbon_budget, helium_budget, max_latency_ms, strategy_type='progressive'):
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

        async with self._cache_lock:
            self._add_to_cache(expert_id, checkpoint)

        async with self._history_lock:
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

    # ---------- Helper methods ----------
    def _calculate_checkpoint_sustainability(self, checkpoint_data: Dict) -> float:
        carbon_score = 1.0 - min(1.0, checkpoint_data.get('carbon_footprint_kg', 0) / 0.1)
        performance_score = checkpoint_data.get('performance_metrics', {}).get('expected_accuracy', 0.5)
        return 0.5 * carbon_score + 0.5 * performance_score

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
        async with self._cache_lock:
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
                oldest_id, _ = self.checkpoint_cache.popitem(last=False)
                logger.info(f"Evicted {oldest_id} from cache (LRU)")
                if self.telemetry:
                    self.telemetry.increment('cs_evictions')
            else:
                oldest_id, _ = self.checkpoint_cache.popitem(last=False)
                logger.info(f"Evicted {oldest_id} from cache (LRU)")
        self.checkpoint_cache[expert_id] = checkpoint
        logger.debug(f"Added {expert_id} to cache (size: {len(self.checkpoint_cache)})")

    async def _save_checkpoint_to_disk(self, checkpoint: ExpertCheckpoint):
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
        async with self._cache_lock:
            expired = [eid for eid, cp in self.checkpoint_cache.items() if now - cp.last_used > max_age]
            for eid in expired:
                del self.checkpoint_cache[eid]
                logger.info(f"Cleaned up expired checkpoint: {eid}")

    async def preload_expert(self, expert_id: str, expert_config: Optional[Dict] = None) -> bool:
        try:
            async with self._cache_lock:
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

    # ============================================================================
    # Statistics Methods
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

        stats['distillation'] = self.strategy_optimizer.get_stats()
        if self.moea_enabled:
            stats['moea'] = {
                'pareto_front_size': len(self.pareto_front),
                'best_weights': self.global_best_weights,
                'enabled': True,
            }
        if self.limit_graph_manager:
            stats['limit_graph'] = self.limit_graph_manager.get_metadata('cold_start_strategies')
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
        async with self._cache_lock:
            usage_counts = {eid: cp.usage_count for eid, cp in self.checkpoint_cache.items()}
        sorted_experts = sorted(usage_counts.items(), key=lambda x: x[1], reverse=True)
        return [{'expert_id': eid, 'usage_count': count} for eid, count in sorted_experts[:top_n]]

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
        if self._preloader_task:
            self._preloader_task.cancel()
            try:
                await self._preloader_task
            except asyncio.CancelledError:
                pass
        if self._moea_task:
            self._moea_task.cancel()
            await asyncio.gather(self._moea_task, return_exceptions=True)
        if self.enable_persistence:
            await self.save_state()
        if self.federated_manager:
            await self.federated_manager.close()
        if self.strategy_selector:
            await self.strategy_selector.close()
        if self.ml_predictor:
            await self.ml_predictor.close()
        self.executor.shutdown(wait=True)
        logger.info("Shutdown complete")


# ============================================================================
# Singleton Accessor
# ============================================================================

_optimizer_instance = None

async def get_cold_start_optimizer() -> ColdStartOptimizer:
    global _optimizer_instance
    if _optimizer_instance is None:
        _optimizer_instance = ColdStartOptimizer()
    return _optimizer_instance


# ============================================================================
# UNIT TESTS
# ============================================================================
import unittest
from unittest import IsolatedAsyncioTestCase

class TestDistillationComponents(IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = {
            'distillation_epsilon': 0.0,
            'distillation_replay_size': 10,
            'distillation_learning_rate': 0.01,
            'distillation_train_every': 10,
        }
        self.optimizer = DistillationStrategyOptimizer(self.config)

    def test_state_feature_vector(self):
        state = ColdStartState(
            expert_type='energy',
            urgency='normal',
            carbon_budget=0.5,
            helium_budget=0.5,
            max_latency_ms=200,
            carbon_intensity=400,
            cache_utilization=0.5,
            recent_hit_rate=0.6,
            strategy_success_rates={'preload':0.8, 'transfer':0.6, 'progressive':0.5, 'hybrid':0.7, 'federated':0.4},
            avg_warmup_time_ms=100,
            avg_sustainability_score=0.7,
        )
        vec = state.to_feature_vector()
        self.assertEqual(len(vec), 14)

    def test_rule_based_teacher(self):
        teacher = StrategyRuleBasedTeacher()
        state = ColdStartState(
            expert_type='energy',
            urgency='normal',
            carbon_budget=0.5,
            helium_budget=0.5,
            max_latency_ms=200,
            carbon_intensity=400,
            cache_utilization=0.5,
            recent_hit_rate=0.9,
            strategy_success_rates={},
            avg_warmup_time_ms=100,
            avg_sustainability_score=0.7,
        )
        probs = teacher.predict(state)
        self.assertAlmostEqual(sum(probs), 1.0)
        self.assertGreater(probs[0], probs[1])

    async def test_select_strategy(self):
        state = ColdStartState(
            expert_type='energy',
            urgency='normal',
            carbon_budget=0.5,
            helium_budget=0.5,
            max_latency_ms=200,
            carbon_intensity=400,
            cache_utilization=0.5,
            recent_hit_rate=0.6,
            strategy_success_rates={},
            avg_warmup_time_ms=100,
            avg_sustainability_score=0.7,
        )
        strategy, idx, state_vec, teacher_probs = await self.optimizer.select_strategy(state, exploration=False)
        self.assertIn(strategy, self.optimizer.STRATEGIES)

    def test_replay_buffer(self):
        buffer = ReplayBuffer(max_size=5)
        state_vec = np.random.randn(14)
        buffer.push(state_vec, 0, 1.0, state_vec, np.ones(5)/5)
        self.assertEqual(len(buffer), 1)
        batch = buffer.sample(1)
        self.assertEqual(len(batch[0]), 1)


# ============================================================================
# Offline Training for Historical ML
# ============================================================================
def train_historical_model(log_path: Path = Path(ColdStartConfig().interaction_logs_path),
                           model_path: Path = Path(ColdStartConfig().historical_model_path)):
    """
    Train a RandomForestClassifier from past interaction logs.
    """
    if not log_path.exists():
        logger.warning(f"Interaction logs not found at {log_path}. No model trained.")
        return

    df_logs = pd.read_csv(log_path)
    if len(df_logs) < 10:
        logger.warning("Not enough logs to train historical model (need at least 10).")
        return

    X_list = []
    y_list = []
    for _, row in df_logs.iterrows():
        state_vec = json.loads(row['state_vector'])
        X_list.append(state_vec)
        y_list.append(row['strategy'])

    X = np.array(X_list)
    y = np.array(y_list)

    le = LabelEncoder()
    y_encoded = le.fit_transform(y)

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X, y_encoded)

    with open(model_path, 'wb') as f:
        pickle.dump((model, le), f)
    logger.info(f"Historical ML model trained and saved to {model_path}")


# ============================================================================
# Example Usage (if run directly)
# ============================================================================
if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)

    async def main():
        config = ColdStartConfig()
        optimizer = ColdStartOptimizer(config)
        result = await optimizer.initialize_expert(
            expert_id="test_expert",
            expert_type="energy",
            urgency="normal"
        )
        print(f"Initialization result: {result}")
        print("Cache stats:", optimizer.get_cache_statistics())
        await optimizer.shutdown()

    asyncio.run(main())
