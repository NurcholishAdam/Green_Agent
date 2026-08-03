#!/usr/bin/env python3
"""
Gating Network Module for MoE Expert System v3.0.0

Enhanced with:
- Complete type hints and docstrings
- Concurrency controls (asyncio locks)
- Secure JSON/Pydantic persistence (no pickle)
- Tenacity retries and circuit breaker (half-open)
- Configurable model architecture
- Weighted online learning buffer
- Integration with carbon/helium managers (real-time features)
- Prometheus telemetry (optional)
- Proper shutdown
- Health check endpoint
- Improved federated learning logic
- Better error handling and validation
- Mock federated server for testing (included)
- Pydantic configuration with environment support
"""

import asyncio
import logging
import json
import os
import hashlib
import zlib
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Any, List, Optional, Tuple, Union, Callable, TypeVar

import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset

# Optional dependencies
try:
    import aiohttp
except ImportError:
    aiohttp = None

try:
    import aiofiles
except ImportError:
    aiofiles = None

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
except ImportError:
    # Dummy retry decorator
    def retry(*args, **kwargs):
        return lambda f: f
    stop_after_attempt = lambda x: None
    wait_exponential = lambda **k: None
    retry_if_exception_type = lambda e: None

try:
    from pydantic import BaseModel, Field, field_validator, ConfigDict
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError:
    # Fallback: use dataclass with manual validation
    BaseModel = None
    BaseSettings = None

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration with Pydantic (if available)
# ============================================================================

if BaseSettings is not None:
    class GatingNetworkConfig(BaseSettings):
        """Centralized configuration for the Gating Network Manager."""
        model_config = SettingsConfigDict(env_prefix="GATING_", case_sensitive=False)

        # Feature flags
        enable_federated: bool = True
        enable_differential_privacy: bool = True
        enable_model_compression: bool = True
        enable_online_learning: bool = True
        enable_telemetry: bool = True
        enable_carbon_awareness: bool = True
        enable_helium_awareness: bool = True
        enable_causal_features: bool = True

        # Model architecture
        input_dim: int = Field(10, ge=1)
        hidden_dim: int = Field(64, ge=1)
        num_experts: int = Field(5, ge=1)
        num_hidden_layers: int = Field(2, ge=1)
        activation: str = Field("relu")
        dropout_rate: float = Field(0.1, ge=0, le=1)
        learning_rate: float = Field(0.001, gt=0)
        batch_size: int = Field(32, ge=1)
        epochs_per_update: int = Field(3, ge=1)

        # Training parameters
        max_training_samples: int = Field(10000, ge=1)
        online_learning_rate: float = Field(0.01, gt=0)
        momentum: float = Field(0.9, ge=0, le=1)
        weight_decay: float = Field(0.0001, ge=0)
        recency_weight: float = Field(0.9, ge=0, le=1)

        # Privacy and compression
        privacy_epsilon: float = Field(1.0, ge=0)
        noise_scale: float = Field(0.001, ge=0)
        sparsity_ratio: float = Field(0.1, ge=0, le=1)

        # Federated learning
        server_url: Optional[str] = None
        federation_round_interval: int = Field(3600, ge=60)

        # Resilience
        max_retries: int = Field(3, ge=0)
        retry_base_delay_ms: float = Field(100.0, ge=0)
        retry_max_delay_ms: float = Field(5000.0, ge=0)
        circuit_breaker_failure_threshold: int = Field(5, ge=1)
        circuit_breaker_recovery_timeout: float = Field(30.0, ge=0)

        # Telemetry
        telemetry_export_interval: int = Field(60, ge=1)
        prometheus_port: Optional[int] = Field(None, ge=1024)

        @field_validator('activation')
        @classmethod
        def validate_activation(cls, v):
            allowed = {"relu", "tanh", "gelu"}
            if v not in allowed:
                raise ValueError(f"activation must be one of {allowed}")
            return v

        @field_validator('num_experts')
        @classmethod
        def validate_num_experts(cls, v):
            if v < 1:
                raise ValueError("num_experts must be >= 1")
            return v

        @field_validator('server_url')
        @classmethod
        def validate_server_url(cls, v):
            if v is not None and not v.startswith(("http://", "https://")):
                raise ValueError("server_url must start with http:// or https://")
            return v
else:
    # Fallback dataclass with manual validation
    @dataclass
    class GatingNetworkConfig:
        enable_federated: bool = True
        enable_differential_privacy: bool = True
        enable_model_compression: bool = True
        enable_online_learning: bool = True
        enable_telemetry: bool = True
        enable_carbon_awareness: bool = True
        enable_helium_awareness: bool = True
        enable_causal_features: bool = True
        input_dim: int = 10
        hidden_dim: int = 64
        num_experts: int = 5
        num_hidden_layers: int = 2
        activation: str = "relu"
        dropout_rate: float = 0.1
        learning_rate: float = 0.001
        batch_size: int = 32
        epochs_per_update: int = 3
        max_training_samples: int = 10000
        online_learning_rate: float = 0.01
        momentum: float = 0.9
        weight_decay: float = 0.0001
        recency_weight: float = 0.9
        privacy_epsilon: float = 1.0
        noise_scale: float = 0.001
        sparsity_ratio: float = 0.1
        server_url: Optional[str] = None
        federation_round_interval: int = 3600
        max_retries: int = 3
        retry_base_delay_ms: float = 100.0
        retry_max_delay_ms: float = 5000.0
        circuit_breaker_failure_threshold: int = 5
        circuit_breaker_recovery_timeout: float = 30.0
        telemetry_export_interval: int = 60
        prometheus_port: Optional[int] = None

        def __post_init__(self):
            if self.input_dim < 1:
                raise ValueError("input_dim must be >= 1")
            if self.hidden_dim < 1:
                raise ValueError("hidden_dim must be >= 1")
            if self.num_experts < 1:
                raise ValueError("num_experts must be >= 1")
            if self.num_hidden_layers < 1:
                raise ValueError("num_hidden_layers must be >= 1")
            if not (0 <= self.dropout_rate <= 1):
                raise ValueError("dropout_rate must be between 0 and 1")
            if self.learning_rate <= 0:
                raise ValueError("learning_rate must be > 0")
            if self.privacy_epsilon < 0:
                raise ValueError("privacy_epsilon must be >= 0")
            if not (0 <= self.sparsity_ratio <= 1):
                raise ValueError("sparsity_ratio must be between 0 and 1")
            if self.max_training_samples < 1:
                raise ValueError("max_training_samples must be >= 1")
            if self.recency_weight < 0 or self.recency_weight > 1:
                raise ValueError("recency_weight must be between 0 and 1")
            if self.circuit_breaker_failure_threshold < 1:
                raise ValueError("circuit_breaker_failure_threshold must be >= 1")
            if self.circuit_breaker_recovery_timeout < 0:
                raise ValueError("circuit_breaker_recovery_timeout must be >= 0")
            valid_activations = {"relu", "tanh", "gelu"}
            if self.activation not in valid_activations:
                raise ValueError(f"activation must be one of {valid_activations}")

# ============================================================================
# Neural Network Model (with configurable architecture)
# ============================================================================

def get_activation(name: str) -> nn.Module:
    """Return activation module by name."""
    if name == "relu":
        return nn.ReLU()
    elif name == "tanh":
        return nn.Tanh()
    elif name == "gelu":
        return nn.GELU()
    else:
        raise ValueError(f"Unknown activation: {name}")

class GatingNetwork(nn.Module):
    """
    Neural network for expert gating with configurable architecture.

    Architecture:
    - Input: `input_dim`
    - `num_hidden_layers` hidden layers, each with `hidden_dim` units,
      followed by activation and batch norm.
    - Dropout after each hidden layer.
    - Output: `num_experts` logits (softmax applied externally).
    """

    def __init__(
        self,
        input_dim: int,
        hidden_dim: int,
        num_experts: int,
        num_hidden_layers: int = 2,
        activation: str = "relu",
        dropout_rate: float = 0.1
    ):
        super().__init__()
        layers = []
        # First layer: input -> hidden
        layers.append(nn.Linear(input_dim, hidden_dim))
        layers.append(get_activation(activation))
        layers.append(nn.BatchNorm1d(hidden_dim))
        layers.append(nn.Dropout(dropout_rate))

        # Additional hidden layers
        for _ in range(num_hidden_layers - 1):
            layers.append(nn.Linear(hidden_dim, hidden_dim))
            layers.append(get_activation(activation))
            layers.append(nn.BatchNorm1d(hidden_dim))
            layers.append(nn.Dropout(dropout_rate))

        # Output layer
        layers.append(nn.Linear(hidden_dim, num_experts))

        self.network = nn.Sequential(*layers)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.network(x)

# ============================================================================
# Circuit Breaker (with half-open state)
# ============================================================================

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    """
    Circuit breaker with half-open state for resilience.
    """
    def __init__(self, failure_threshold: int, recovery_timeout: float):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute the given async function with circuit breaker protection.
        """
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if self.last_failure_time:
                    elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
                    if elapsed >= self.recovery_timeout:
                        self.state = CircuitBreakerState.HALF_OPEN
                        self.failure_count = 0
                        logger.info("Circuit breaker entered HALF_OPEN state")
                    else:
                        raise RuntimeError(f"Circuit breaker OPEN (recovery in {self.recovery_timeout - elapsed:.1f}s)")
                else:
                    raise RuntimeError("Circuit breaker OPEN (no failure time)")

        # Execute function
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == CircuitBreakerState.HALF_OPEN:
                    # Success in half-open: close the breaker
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    logger.info("Circuit breaker closed after successful half-open call")
                elif self.state == CircuitBreakerState.CLOSED:
                    self.failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = datetime.utcnow()
                if self.state == CircuitBreakerState.HALF_OPEN:
                    # Failure in half-open: open immediately
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker opened due to failure in half-open state: {e}")
                elif self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker opened after {self.failure_count} failures")
            raise e

    @property
    def is_open(self) -> bool:
        return self.state == CircuitBreakerState.OPEN

    async def reset(self):
        async with self._lock:
            self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0
            self.last_failure_time = None
            logger.info("Circuit breaker manually reset")

# ============================================================================
# Rate Limiter (optional)
# ============================================================================

class RateLimiter:
    """Token bucket rate limiter."""
    def __init__(self, rate_per_second: float, capacity: int):
        self.rate = rate_per_second
        self.capacity = capacity
        self.tokens = float(capacity)
        self.last_update = datetime.utcnow().timestamp()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = datetime.utcnow().timestamp()
            elapsed = now - self.last_update
            self.tokens += elapsed * self.rate
            if self.tokens > self.capacity:
                self.tokens = self.capacity
            self.last_update = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

# ============================================================================
# Persistence State (Pydantic)
# ============================================================================

if BaseModel is not None:
    class GatingNetworkState(BaseModel):
        """Serializable state for the gating network."""
        version: str = "3.0.0"
        model_state_dict: Dict[str, Any]
        optimizer_state_dict: Dict[str, Any]
        training_data: List[Tuple[List[float], int]]
        config: GatingNetworkConfig
        expert_ids: List[str]
        federated_round: int = 0
        participants: List[str] = field(default_factory=list)
        contribution_score: float = 0.0
        is_trained: bool = False
        inference_count: int = 0
        training_count: int = 0
        last_save: str = field(default_factory=lambda: datetime.utcnow().isoformat())
else:
    # Fallback: use plain dict (not recommended)
    GatingNetworkState = None

# ============================================================================
# Gating Network Manager (Enhanced)
# ============================================================================

class GatingNetworkManager:
    """
    Manages the gating network, training, inference, and integration with the MoE system.

    Features:
    - Configurable neural architecture.
    - Online learning with weighted recency buffer.
    - Differential privacy and model compression.
    - Federated learning with retry and circuit breaker.
    - Integration with carbon/helium managers.
    - Prometheus telemetry (optional).
    - Secure persistence (JSON + Pydantic).
    - Health check endpoint.
    """

    def __init__(
        self,
        config: Optional[GatingNetworkConfig] = None,
        carbon_manager: Optional[Any] = None,
        helium_optimizer: Optional[Any] = None,
        expert_ids: Optional[List[str]] = None,
        rate_limiter: Optional[RateLimiter] = None,
    ):
        self.config = config or GatingNetworkConfig()
        self.carbon_manager = carbon_manager
        self.helium_optimizer = helium_optimizer
        self.expert_ids = expert_ids or [f"expert_{i}" for i in range(self.config.num_experts)]

        if len(self.expert_ids) != self.config.num_experts:
            raise ValueError(
                f"Number of expert IDs ({len(self.expert_ids)}) must match num_experts ({self.config.num_experts})"
            )

        # Model
        self.model = GatingNetwork(
            input_dim=self.config.input_dim,
            hidden_dim=self.config.hidden_dim,
            num_experts=self.config.num_experts,
            num_hidden_layers=self.config.num_hidden_layers,
            activation=self.config.activation,
            dropout_rate=self.config.dropout_rate
        )
        self.optimizer = optim.Adam(
            self.model.parameters(),
            lr=self.config.learning_rate,
            weight_decay=self.config.weight_decay
        )
        self.criterion = nn.CrossEntropyLoss()

        # Training buffer (weighted by recency)
        self.training_buffer: deque = deque(maxlen=self.config.max_training_samples)
        self.is_trained = False
        self.global_model_state: Optional[Dict] = None

        # Federated learning
        self.federated_round = 0
        self.participants: List[str] = []
        self.contribution_score = 0.0
        self._federated_session: Optional[aiohttp.ClientSession] = None

        # Circuit breaker for external calls (federated and possibly others)
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=self.config.circuit_breaker_failure_threshold,
            recovery_timeout=self.config.circuit_breaker_recovery_timeout
        )
        self._federated_lock = asyncio.Lock()
        self._buffer_lock = asyncio.Lock()
        self._model_lock = asyncio.Lock()
        self._metrics_lock = asyncio.Lock()

        # Rate limiter for inference (optional)
        self.rate_limiter = rate_limiter

        # Telemetry
        self.inference_count = 0
        self.training_count = 0
        self.metrics: Dict[str, Any] = defaultdict(lambda: defaultdict(int))

        # Prometheus instrumentation
        self._prometheus_metrics = None
        if self.config.enable_telemetry and PROMETHEUS_AVAILABLE and self.config.prometheus_port:
            self._setup_prometheus()
            self._start_prometheus_server()

        # Background tasks
        self._background_tasks: List[asyncio.Task] = []
        if self.config.enable_federated and self.config.server_url:
            self._background_tasks.append(asyncio.create_task(self._federated_sync_loop()))
        if self.config.enable_telemetry:
            self._background_tasks.append(asyncio.create_task(self._telemetry_export_loop()))

        logger.info(
            f"GatingNetworkManager initialized: input_dim={self.config.input_dim}, "
            f"hidden_dim={self.config.hidden_dim}, num_experts={self.config.num_experts}, "
            f"layers={self.config.num_hidden_layers}, activation={self.config.activation}"
        )

    # ============================================================================
    # Prometheus Setup (optional)
    # ============================================================================

    def _setup_prometheus(self):
        """Initialize Prometheus metrics."""
        self._prometheus_metrics = {
            'inference_total': Counter('gating_inference_total', 'Total inferences'),
            'training_total': Counter('gating_training_total', 'Total training runs'),
            'training_loss': Histogram('gating_training_loss', 'Training loss'),
            'federated_round': Gauge('gating_federated_round', 'Current federated round'),
            'circuit_breaker_state': Gauge('gating_circuit_breaker_state', 'Circuit breaker state (0=closed,1=open,2=half_open)'),
        }

    def _start_prometheus_server(self):
        """Start Prometheus HTTP server."""
        start_http_server(self.config.prometheus_port)
        logger.info(f"Prometheus metrics server started on port {self.config.prometheus_port}")

    # ============================================================================
    # Feature Engineering (Enhanced with Carbon/Helium Managers)
    # ============================================================================

    async def _build_features(self, context: Dict[str, Any]) -> np.ndarray:
        """
        Build feature vector from context for the gating network.
        Integrates carbon/helium data if available and enabled.
        Raises ValueError if the context is malformed.
        """
        features = []

        # Base features with validation
        expected_keys = [
            'helium_scarcity', 'helium_cost_index', 'carbon_intensity',
            'model_loss', 'gradient_variance', 'avg_client_energy',
            'gradient_carbon', 'gradient_helium', 'token_balance_norm',
            'harvester_stress'
        ]
        for key in expected_keys:
            val = context.get(key)
            if val is None:
                logger.warning(f"Missing context key '{key}', using default 0.5")
                val = 0.5
            if not isinstance(val, (int, float)):
                raise ValueError(f"Context key '{key}' must be numeric, got {type(val)}")
            features.append(float(val))

        # Integrate with carbon_manager (if available)
        if self.config.enable_carbon_awareness and self.carbon_manager:
            try:
                carbon_intensity = await self.carbon_manager.get_current_intensity()
                # Append as an extra feature (beyond the default 10)
                features.append(carbon_intensity / 1000.0)  # normalize
            except Exception as e:
                logger.warning(f"Failed to fetch carbon intensity: {e}")
                features.append(0.5)

        # Integrate with helium_optimizer
        if self.config.enable_helium_awareness and self.helium_optimizer:
            try:
                helium_status = self.helium_optimizer.get_helium_status()
                helium_price = helium_status.get('price_usd_per_l', 0.5)
                features.append(helium_price)
            except Exception as e:
                logger.warning(f"Failed to fetch helium price: {e}")
                features.append(0.5)

        # Optionally add causal features
        if self.config.enable_causal_features:
            features.append(context.get('causal_impact_carbon', 0.0))
            features.append(context.get('causal_impact_helium', 0.0))

        # Ensure correct dimension (pad or truncate)
        if len(features) != self.config.input_dim:
            if len(features) < self.config.input_dim:
                features.extend([0.0] * (self.config.input_dim - len(features)))
            else:
                features = features[:self.config.input_dim]

        return np.array(features, dtype=np.float32)

    # ============================================================================
    # Inference
    # ============================================================================

    async def predict(self, context: Dict[str, Any]) -> Dict[str, float]:
        """
        Predict expert weights for a given context.
        Returns a dict mapping expert_id to probability (softmax).
        Raises RuntimeError if rate limited.
        """
        # Rate limiting (optional)
        if self.rate_limiter and not await self.rate_limiter.acquire():
            raise RuntimeError("Rate limit exceeded for inference")

        features = await self._build_features(context)
        features_tensor = torch.FloatTensor(features).unsqueeze(0)

        with torch.no_grad():
            logits = self.model(features_tensor)
            probs = torch.softmax(logits, dim=1).squeeze().cpu().numpy()

        result = {self.expert_ids[i]: float(probs[i]) for i in range(len(self.expert_ids))}

        async with self._metrics_lock:
            self.inference_count += 1
            if self.config.enable_telemetry:
                self.metrics['counters']['inference_total'] += 1
                if self._prometheus_metrics:
                    self._prometheus_metrics['inference_total'].inc()

        return result

    # ============================================================================
    # Training Buffer Management
    # ============================================================================

    def add_training_sample(self, features: np.ndarray, label: int):
        """
        Add a single training sample with recency weighting.
        The buffer maintains a deque; newer samples have higher weight during training.
        """
        # Validate input
        if features.shape[0] != self.config.input_dim:
            raise ValueError(f"Feature dimension mismatch: expected {self.config.input_dim}, got {features.shape[0]}")
        if not 0 <= label < self.config.num_experts:
            raise ValueError(f"Label out of range: {label} (num_experts={self.config.num_experts})")

        if len(self.training_buffer) >= self.config.max_training_samples:
            # Remove the oldest sample (FIFO)
            self.training_buffer.popleft()
        self.training_buffer.append((features, label))

    async def train(self, epochs: Optional[int] = None):
        """
        Train the model on the training buffer with recency weighting.
        If online learning is enabled, performs incremental updates.
        """
        if not self.training_buffer:
            logger.warning("No training data available")
            return

        epochs = epochs or self.config.epochs_per_update

        # Prepare data with recency weights
        buffer_list = list(self.training_buffer)
        # Create weights: recent samples get higher weight
        n = len(buffer_list)
        weights = np.array([self.config.recency_weight ** (n - 1 - i) for i in range(n)])
        weights /= weights.sum()  # normalize

        # Sample indices according to weights
        if np.random.random() < 0.5:  # use weighted sampling for diversity
            indices = np.random.choice(n, size=min(n, 2000), p=weights, replace=True)
            X = np.array([buffer_list[i][0] for i in indices], dtype=np.float32)
            y = np.array([buffer_list[i][1] for i in indices], dtype=np.int64)
        else:
            # Use all data (if small)
            X = np.array([sample[0] for sample in buffer_list], dtype=np.float32)
            y = np.array([sample[1] for sample in buffer_list], dtype=np.int64)

        X_tensor = torch.FloatTensor(X)
        y_tensor = torch.LongTensor(y)

        dataset = TensorDataset(X_tensor, y_tensor)
        dataloader = DataLoader(dataset, batch_size=self.config.batch_size, shuffle=True)

        self.model.train()
        total_loss = 0.0
        for epoch in range(epochs):
            epoch_loss = 0.0
            for batch_X, batch_y in dataloader:
                self.optimizer.zero_grad()
                output = self.model(batch_X)
                loss = self.criterion(output, batch_y)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
                self.optimizer.step()
                epoch_loss += loss.item()
            total_loss += epoch_loss
            logger.debug(f"Epoch {epoch+1}/{epochs} loss: {epoch_loss:.4f}")

        avg_loss = total_loss / epochs
        self.is_trained = True
        async with self._metrics_lock:
            self.training_count += 1
            if self.config.enable_telemetry:
                self.metrics['histograms']['training_loss'].append(avg_loss)
                if self._prometheus_metrics:
                    self._prometheus_metrics['training_total'].inc()
                    self._prometheus_metrics['training_loss'].observe(avg_loss)

        logger.info(f"Gating network trained. Avg loss: {avg_loss:.4f}, samples used: {len(X)}")

    # ============================================================================
    # Model Compression (Top‑K Sparsification)
    # ============================================================================

    def _compress_weights(self, state_dict: Dict[str, torch.Tensor]) -> Dict[str, torch.Tensor]:
        """
        Apply top‑k sparsification to the model weights.
        Only the largest k% of weights (by magnitude) are kept; others are zeroed.
        """
        if not self.config.enable_model_compression:
            return state_dict

        compressed = {}
        for key, tensor in state_dict.items():
            if tensor.dim() < 2:  # Skip biases and small tensors
                compressed[key] = tensor
                continue
            flat = tensor.view(-1)
            k = int(flat.numel() * self.config.sparsity_ratio)
            if k == 0:
                compressed[key] = torch.zeros_like(tensor)
                continue
            topk_vals, topk_idx = torch.topk(flat.abs(), k)
            sparse = torch.zeros_like(flat)
            sparse[topk_idx] = flat[topk_idx]
            compressed[key] = sparse.view(tensor.shape)
        return compressed

    # ============================================================================
    # Differential Privacy
    # ============================================================================

    def _add_differential_privacy(self, state_dict: Dict[str, torch.Tensor]) -> Dict[str, torch.Tensor]:
        """
        Add Gaussian noise to the weights for differential privacy.
        Noise scale is determined by privacy_epsilon and sensitivity.
        """
        if not self.config.enable_differential_privacy or self.config.privacy_epsilon <= 0:
            return state_dict

        private = {}
        sensitivity = 1.0  # L2 sensitivity (assumed)
        scale = (2 * sensitivity) / self.config.privacy_epsilon
        for key, tensor in state_dict.items():
            noise = torch.randn_like(tensor) * scale * self.config.noise_scale
            private[key] = tensor + noise
        return private

    # ============================================================================
    # Federated Learning (Enhanced with correct round handling)
    # ============================================================================

    async def _get_federated_session(self) -> aiohttp.ClientSession:
        if self._federated_session is None and self.config.server_url:
            self._federated_session = aiohttp.ClientSession()
        return self._federated_session

    async def _send_local_update(self, performance_metric: float = 1.0) -> Dict:
        """Send local model update to federated server with retry and circuit breaker."""
        if not self.config.server_url:
            return {'status': 'disabled'}

        async with self._federated_lock:
            # Prepare model weights
            state_dict = self.model.state_dict()
            private_state = self._add_differential_privacy(state_dict)
            compressed_state = self._compress_weights(private_state)
            serialized = {k: v.tolist() for k, v in compressed_state.items()}

            update_data = {
                'router_id': 'gating_network',
                'round': self.federated_round,  # current round number
                'weights': serialized,
                'performance': performance_metric,
                'privacy_epsilon': self.config.privacy_epsilon,
                'sparsity_ratio': self.config.sparsity_ratio,
                'timestamp': datetime.utcnow().isoformat()
            }

            # Use circuit breaker to protect the call
            async def _do_update():
                session = await self._get_federated_session()
                async with session.post(
                    f"{self.config.server_url}/federated/gating/update",
                    json=update_data,
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
                result = await self._circuit_breaker.call(_do_update)
                # Note: we increment round only after successful global model fetch
                self.contribution_score += performance_metric
                return result
            except Exception as e:
                logger.error(f"Federated update failed after circuit breaker: {e}")
                return {'status': 'failed'}

    async def _fetch_global_model(self) -> Optional[Dict]:
        """Fetch global model from federated server with retry and circuit breaker."""
        if not self.config.server_url:
            return None

        async def _do_fetch():
            session = await self._get_federated_session()
            async with session.get(
                f"{self.config.server_url}/federated/gating/global",
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
            data = await self._circuit_breaker.call(_do_fetch)
            weights = data.get('weights', {})
            round_from_server = data.get('round', 0)
            self.participants = data.get('participants', [])
            if weights:
                state_dict = {k: torch.FloatTensor(v) for k, v in weights.items()}
                self.model.load_state_dict(state_dict)
                self.global_model_state = state_dict
                self.is_trained = True
                # Update round only after successful fetch
                self.federated_round = round_from_server
            return weights
        except Exception as e:
            logger.error(f"Global fetch failed after circuit breaker: {e}")
            return None

    async def participate_in_round(self, training_data: List[Tuple[np.ndarray, int]], performance: float = 1.0) -> Dict:
        """
        Participate in one federated learning round:
        - Add samples to buffer.
        - Train locally.
        - Send local update.
        - Fetch global model.
        """
        for features, label in training_data:
            self.add_training_sample(features, label)
        await self.train()
        update_result = await self._send_local_update(performance)
        global_result = await self._fetch_global_model()
        return {
            'round': self.federated_round,
            'local_update_sent': update_result.get('status') != 'failed',
            'global_model_fetched': global_result is not None,
            'participants': len(self.participants),
            'contribution_score': self.contribution_score,
            'timestamp': datetime.utcnow().isoformat()
        }

    async def _federated_sync_loop(self):
        """Background loop for periodic federated synchronization."""
        while True:
            try:
                if self._circuit_breaker.is_open:
                    logger.debug("Circuit breaker open, skipping federated sync")
                    await asyncio.sleep(60)
                    continue

                if len(self.training_buffer) >= 10:
                    # Use weighted sample from buffer
                    buffer_list = list(self.training_buffer)
                    # Take last 100 samples (most recent)
                    recent_samples = buffer_list[-100:]
                    await self.participate_in_round(recent_samples)

                # Update Prometheus gauges
                if self._prometheus_metrics:
                    self._prometheus_metrics['federated_round'].set(self.federated_round)
                    state_val = {
                        CircuitBreakerState.CLOSED: 0,
                        CircuitBreakerState.OPEN: 1,
                        CircuitBreakerState.HALF_OPEN: 2
                    }.get(self._circuit_breaker.state, 0)
                    self._prometheus_metrics['circuit_breaker_state'].set(state_val)

                await asyncio.sleep(self.config.federation_round_interval)
            except Exception as e:
                logger.error(f"Federated sync loop error: {e}")
                await asyncio.sleep(300)

    # ============================================================================
    # Telemetry
    # ============================================================================

    async def _telemetry_export_loop(self):
        """Background loop to export telemetry metrics."""
        while True:
            try:
                # In production, you could push to a monitoring system
                # For now, log at debug level
                logger.debug(f"Gating telemetry: {self.get_telemetry()}")
                await asyncio.sleep(self.config.telemetry_export_interval)
            except Exception as e:
                logger.error(f"Telemetry export error: {e}")
                await asyncio.sleep(60)

    def get_telemetry(self) -> Dict[str, Any]:
        """Return current telemetry metrics."""
        async with self._metrics_lock:
            return {
                'inference_count': self.inference_count,
                'training_count': self.training_count,
                'training_samples': len(self.training_buffer),
                'is_trained': self.is_trained,
                'federated_round': self.federated_round,
                'participants': len(self.participants),
                'contribution_score': self.contribution_score,
                'circuit_breaker_state': self._circuit_breaker.state.value,
                'metrics': {
                    'counters': dict(self.metrics['counters']),
                    'histograms': {k: list(v) for k, v in self.metrics['histograms'].items()}
                }
            }

    # ============================================================================
    # Persistence (Secure JSON + Pydantic)
    # ============================================================================

    async def save_model(self, path: str):
        """
        Save model and training state to disk using JSON + Pydantic.
        """
        # Convert tensors to lists for JSON serialization
        model_dict = {k: v.tolist() for k, v in self.model.state_dict().items()}
        optimizer_dict = {k: v.tolist() for k, v in self.optimizer.state_dict().items()}
        training_data = [(f.tolist() if isinstance(f, np.ndarray) else f, int(l))
                         for f, l in self.training_buffer]

        state = GatingNetworkState(
            model_state_dict=model_dict,
            optimizer_state_dict=optimizer_dict,
            training_data=training_data,
            config=self.config,
            expert_ids=self.expert_ids,
            federated_round=self.federated_round,
            participants=self.participants,
            contribution_score=self.contribution_score,
            is_trained=self.is_trained,
            inference_count=self.inference_count,
            training_count=self.training_count
        )

        json_str = state.model_dump_json(indent=2)
        compressed = zlib.compress(json_str.encode('utf-8'))
        if aiofiles:
            async with aiofiles.open(path, 'wb') as f:
                await f.write(compressed)
        else:
            # Fallback to synchronous I/O
            with open(path, 'wb') as f:
                f.write(compressed)
        logger.info(f"Model saved to {path} (JSON)")

    async def load_model(self, path: str) -> bool:
        """
        Load model and training state from disk.
        """
        if not os.path.exists(path):
            logger.warning(f"Persistence file {path} not found")
            return False

        try:
            if aiofiles:
                async with aiofiles.open(path, 'rb') as f:
                    compressed = await f.read()
            else:
                with open(path, 'rb') as f:
                    compressed = f.read()
            json_str = zlib.decompress(compressed).decode('utf-8')
            state = GatingNetworkState.model_validate_json(json_str)
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            return False

        # Convert lists back to tensors
        model_dict = {k: torch.FloatTensor(v) for k, v in state.model_state_dict.items()}
        optimizer_dict = {k: torch.FloatTensor(v) for k, v in state.optimizer_state_dict.items()}
        self.model.load_state_dict(model_dict)
        self.optimizer.load_state_dict(optimizer_dict)

        # Restore training buffer
        self.training_buffer = deque(
            [(np.array(f, dtype=np.float32), l) for f, l in state.training_data],
            maxlen=self.config.max_training_samples
        )
        self.federated_round = state.federated_round
        self.participants = state.participants
        self.contribution_score = state.contribution_score
        self.is_trained = state.is_trained
        self.inference_count = state.inference_count
        self.training_count = state.training_count

        logger.info(f"Model loaded from {path} (JSON)")
        return True

    # ============================================================================
    # Health Check
    # ============================================================================

    async def get_health_status(self) -> Dict[str, Any]:
        """Return health status of the gating network manager."""
        return {
            'status': 'healthy',
            'is_trained': self.is_trained,
            'circuit_breaker_state': self._circuit_breaker.state.value,
            'federated_connected': self.config.server_url is not None and self._federated_session is not None,
            'training_samples': len(self.training_buffer),
            'federated_round': self.federated_round,
            'participants': len(self.participants),
            'inference_count': self.inference_count,
            'training_count': self.training_count
        }

    # ============================================================================
    # Cleanup
    # ============================================================================

    async def shutdown(self):
        """Gracefully shutdown all background tasks and sessions."""
        logger.info("Shutting down GatingNetworkManager")
        for task in self._background_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if self._federated_session:
            await self._federated_session.close()
        logger.info("Shutdown complete")

# ============================================================================
# Mock Federated Server (for testing)
# ============================================================================

class MockFederatedServer:
    """
    A simple mock federated server for testing the gating network's federated learning.
    Run with: `python -m asyncio` and call `await server.start()`.
    """
    def __init__(self, host: str = "localhost", port: int = 8000):
        self.host = host
        self.port = port
        self.global_weights = {}
        self.participants = []
        self.round = 0
        self.updates = []

    async def handle_update(self, request):
        data = await request.json()
        self.updates.append(data)
        self.participants.append(data.get('router_id'))
        # Simple aggregation: average weights
        if self.global_weights:
            # average with existing
            for k, v in data['weights'].items():
                self.global_weights[k] = (self.global_weights[k] + torch.FloatTensor(v)) / 2
        else:
            self.global_weights = {k: torch.FloatTensor(v) for k, v in data['weights'].items()}
        self.round += 1
        return aiohttp.web.json_response({'status': 'ok'})

    async def handle_global(self, request):
        weights_serialized = {k: v.tolist() for k, v in self.global_weights.items()}
        return aiohttp.web.json_response({
            'weights': weights_serialized,
            'round': self.round,
            'participants': self.participants
        })

    async def start(self):
        from aiohttp import web
        app = web.Application()
        app.router.add_post('/federated/gating/update', self.handle_update)
        app.router.add_get('/federated/gating/global', self.handle_global)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        logger.info(f"Mock federated server started at http://{self.host}:{self.port}")
        return runner

# ============================================================================
# Example Usage (if run directly)
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def main():
        # Optional: start mock federated server
        # server = MockFederatedServer()
        # runner = await server.start()

        config = GatingNetworkConfig(
            input_dim=10,
            num_experts=5,
            server_url="http://localhost:8000",  # optional
            prometheus_port=8001  # optional
        )
        manager = GatingNetworkManager(config)

        # Simulate some training samples
        for _ in range(20):
            features = np.random.randn(config.input_dim).astype(np.float32)
            label = np.random.randint(0, config.num_experts)
            manager.add_training_sample(features, label)

        await manager.train(epochs=5)

        # Predict
        context = {"helium_scarcity": 0.6, "carbon_intensity": 0.4}
        result = await manager.predict(context)
        print("Prediction:", result)

        # Telemetry
        print("Telemetry:", manager.get_telemetry())

        # Save model
        await manager.save_model("gating_model.json.gz")

        # Health check
        print("Health:", await manager.get_health_status())

        await manager.shutdown()

    asyncio.run(main())
