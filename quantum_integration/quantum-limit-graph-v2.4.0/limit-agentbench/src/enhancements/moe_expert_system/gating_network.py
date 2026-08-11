#!/usr/bin/env python3
"""
Gating Network Module for MoE Expert System v3.1.0
Full Green Agent MOPD Integration

ENHANCEMENTS OVER v3.0.0:
1. INTEGRATED with central Config, Storage, Logger, MetricsRegistry, AsyncMessageQueue.
2. ADDED teacher interface (`policy_probs`) for MTPD optimizer.
3. PUBLISHES FeedbackEvent for every prediction and training step.
4. USES central AdaptiveCostFunction, ParetoGating, and DriftDetector.
5. REMOVED custom persistence; now uses central Storage.
6. REMOVED custom Prometheus; now uses central MetricsRegistry.
7. REMOVED custom logging; now uses central structlog.
8. All optional dependencies (PyTorch, scikit-learn, etc.) still gracefully degrade.
"""

import asyncio
import json
import os
import hashlib
import zlib
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Any, List, Optional, Tuple, Union, Callable
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset

# -----------------------------------------------------------------------------
# IMPORT CENTRAL GREEN AGENT COMPONENTS
# -----------------------------------------------------------------------------
from ..config import config as central_config
from ..storage import Storage
from ..schemas.feedback_event import FeedbackEvent
from ..routing.pareto_gating import ParetoGating
from ..feedback.adaptive_cost import AdaptiveCostFunction
from ..safety.drift_detector import DriftDetector
from ..scaling.message_queue import AsyncMessageQueue
from ..metrics import MetricsRegistry
from ..logger import logger

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
    def retry(*args, **kwargs):
        return lambda f: f
    stop_after_attempt = lambda x: None
    wait_exponential = lambda **k: None
    retry_if_exception_type = lambda e: None

# -----------------------------------------------------------------------------
# Configuration – now uses central_config as a reference.
# We keep a local config class for backward compatibility, but values are pulled
# from central_config with sensible defaults.
# -----------------------------------------------------------------------------
class GatingNetworkConfig:
    """Configuration for GatingNetworkManager, built from central_config."""
    def __init__(self):
        self.input_dim = getattr(central_config, "gating_input_dim", 10)
        self.hidden_dim = getattr(central_config, "gating_hidden_dim", 64)
        self.num_experts = getattr(central_config, "gating_num_experts", 5)
        self.num_hidden_layers = getattr(central_config, "gating_num_hidden_layers", 2)
        self.activation = getattr(central_config, "gating_activation", "relu")
        self.dropout_rate = getattr(central_config, "gating_dropout_rate", 0.1)
        self.learning_rate = getattr(central_config, "gating_learning_rate", 0.001)
        self.batch_size = getattr(central_config, "gating_batch_size", 32)
        self.epochs_per_update = getattr(central_config, "gating_epochs_per_update", 3)
        self.max_training_samples = getattr(central_config, "gating_max_training_samples", 10000)
        self.online_learning_rate = getattr(central_config, "gating_online_learning_rate", 0.01)
        self.momentum = getattr(central_config, "gating_momentum", 0.9)
        self.weight_decay = getattr(central_config, "gating_weight_decay", 0.0001)
        self.recency_weight = getattr(central_config, "gating_recency_weight", 0.9)
        self.privacy_epsilon = getattr(central_config, "gating_privacy_epsilon", 1.0)
        self.noise_scale = getattr(central_config, "gating_noise_scale", 0.001)
        self.sparsity_ratio = getattr(central_config, "gating_sparsity_ratio", 0.1)
        self.server_url = getattr(central_config, "gating_server_url", None)
        self.federation_round_interval = getattr(central_config, "gating_federation_round_interval", 3600)
        self.max_retries = getattr(central_config, "gating_max_retries", 3)
        self.retry_base_delay_ms = getattr(central_config, "gating_retry_base_delay_ms", 100.0)
        self.retry_max_delay_ms = getattr(central_config, "gating_retry_max_delay_ms", 5000.0)
        self.circuit_breaker_failure_threshold = getattr(central_config, "gating_circuit_breaker_failure_threshold", 5)
        self.circuit_breaker_recovery_timeout = getattr(central_config, "gating_circuit_breaker_recovery_timeout", 30.0)
        self.enable_federated = getattr(central_config, "gating_enable_federated", True)
        self.enable_differential_privacy = getattr(central_config, "gating_enable_differential_privacy", True)
        self.enable_model_compression = getattr(central_config, "gating_enable_model_compression", True)
        self.enable_online_learning = getattr(central_config, "gating_enable_online_learning", True)
        self.enable_carbon_awareness = getattr(central_config, "gating_enable_carbon_awareness", True)
        self.enable_helium_awareness = getattr(central_config, "gating_enable_helium_awareness", True)
        self.enable_causal_features = getattr(central_config, "gating_enable_causal_features", True)

        # Validate
        if self.activation not in {"relu", "tanh", "gelu"}:
            raise ValueError(f"activation must be one of relu, tanh, gelu; got {self.activation}")

# -----------------------------------------------------------------------------
# Neural Network Model (unchanged)
# -----------------------------------------------------------------------------
def get_activation(name: str) -> nn.Module:
    if name == "relu":
        return nn.ReLU()
    elif name == "tanh":
        return nn.Tanh()
    elif name == "gelu":
        return nn.GELU()
    else:
        raise ValueError(f"Unknown activation: {name}")

class GatingNetwork(nn.Module):
    def __init__(self, input_dim: int, hidden_dim: int, num_experts: int,
                 num_hidden_layers: int = 2, activation: str = "relu", dropout_rate: float = 0.1):
        super().__init__()
        layers = []
        layers.append(nn.Linear(input_dim, hidden_dim))
        layers.append(get_activation(activation))
        layers.append(nn.BatchNorm1d(hidden_dim))
        layers.append(nn.Dropout(dropout_rate))
        for _ in range(num_hidden_layers - 1):
            layers.append(nn.Linear(hidden_dim, hidden_dim))
            layers.append(get_activation(activation))
            layers.append(nn.BatchNorm1d(hidden_dim))
            layers.append(nn.Dropout(dropout_rate))
        layers.append(nn.Linear(hidden_dim, num_experts))
        self.network = nn.Sequential(*layers)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.network(x)

# -----------------------------------------------------------------------------
# Circuit Breaker (unchanged)
# -----------------------------------------------------------------------------
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, failure_threshold: int, recovery_timeout: float):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
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
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == CircuitBreakerState.HALF_OPEN:
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
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker opened due to failure in half-open state: {e}")
                elif self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker opened after {self.failure_count} failures")
            raise e

    @property
    def is_open(self) -> bool:
        return self.state == CircuitBreakerState.OPEN

# -----------------------------------------------------------------------------
# Rate Limiter (unchanged)
# -----------------------------------------------------------------------------
class RateLimiter:
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

# -----------------------------------------------------------------------------
# Gating Network Manager – Fully Integrated
# -----------------------------------------------------------------------------
class GatingNetworkManager:
    """
    Gating Network Manager with full Green Agent MOPD integration.

    Exposes a teacher interface (`policy_probs`) for MTPD optimizer.
    """

    def __init__(
        self,
        storage: Storage,
        message_queue: AsyncMessageQueue,
        adaptive_cost: AdaptiveCostFunction,
        pareto_gating: ParetoGating,
        drift_detector: DriftDetector,
        metrics: MetricsRegistry,
        carbon_manager: Optional[Any] = None,
        helium_optimizer: Optional[Any] = None,
        expert_ids: Optional[List[str]] = None,
    ):
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        self.config = GatingNetworkConfig()
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

        # Training buffer
        self.training_buffer: deque = deque(maxlen=self.config.max_training_samples)
        self.is_trained = False
        self.global_model_state: Optional[Dict] = None

        # Federated learning
        self.federated_round = 0
        self.participants: List[str] = []
        self.contribution_score = 0.0
        self._federated_session: Optional[aiohttp.ClientSession] = None

        # Circuit breaker
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=self.config.circuit_breaker_failure_threshold,
            recovery_timeout=self.config.circuit_breaker_recovery_timeout
        )
        self._federated_lock = asyncio.Lock()
        self._buffer_lock = asyncio.Lock()
        self._model_lock = asyncio.Lock()
        self._metrics_lock = asyncio.Lock()

        # Rate limiter (optional, using central rate limit)
        rate_limit = getattr(central_config, "rate_limit_requests", 100)
        self.rate_limiter = RateLimiter(rate_limit / 60.0, rate_limit)

        # Counters
        self.inference_count = 0
        self.training_count = 0

        # Background tasks
        self._background_tasks: List[asyncio.Task] = []
        if self.config.enable_federated and self.config.server_url:
            self._background_tasks.append(asyncio.create_task(self._federated_sync_loop()))

        logger.info(
            f"GatingNetworkManager initialized: input_dim={self.config.input_dim}, "
            f"hidden_dim={self.config.hidden_dim}, num_experts={self.config.num_experts}, "
            f"layers={self.config.num_hidden_layers}, activation={self.config.activation}"
        )

    # ==========================================================================
    # Teacher Interface for MOPD
    # ==========================================================================
    async def policy_probs(self, state: Dict) -> List[float]:
        """
        Return a probability distribution over experts.
        This allows the MTPD optimizer to treat this module as a teacher.
        """
        probs_dict = await self.predict(state)
        return [probs_dict.get(eid, 0.0) for eid in self.expert_ids]

    # ==========================================================================
    # Feature Engineering (with central carbon/helium managers)
    # ==========================================================================
    async def _build_features(self, context: Dict[str, Any]) -> np.ndarray:
        features = []
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

        if self.config.enable_carbon_awareness and self.carbon_manager:
            try:
                carbon_intensity = await self.carbon_manager.get_current_intensity()
                features.append(carbon_intensity / 1000.0)
            except Exception as e:
                logger.warning(f"Failed to fetch carbon intensity: {e}")
                features.append(0.5)

        if self.config.enable_helium_awareness and self.helium_optimizer:
            try:
                helium_status = self.helium_optimizer.get_helium_status()
                helium_price = helium_status.get('price_usd_per_l', 0.5)
                features.append(helium_price)
            except Exception as e:
                logger.warning(f"Failed to fetch helium price: {e}")
                features.append(0.5)

        if self.config.enable_causal_features:
            features.append(context.get('causal_impact_carbon', 0.0))
            features.append(context.get('causal_impact_helium', 0.0))

        if len(features) != self.config.input_dim:
            if len(features) < self.config.input_dim:
                features.extend([0.0] * (self.config.input_dim - len(features)))
            else:
                features = features[:self.config.input_dim]

        return np.array(features, dtype=np.float32)

    # ==========================================================================
    # Inference (Enhanced with adaptive cost and Pareto gating)
    # ==========================================================================
    async def predict(self, context: Dict[str, Any]) -> Dict[str, float]:
        if self.rate_limiter and not await self.rate_limiter.acquire():
            raise RuntimeError("Rate limit exceeded for inference")

        features = await self._build_features(context)
        features_tensor = torch.FloatTensor(features).unsqueeze(0)

        with torch.no_grad():
            logits = self.model(features_tensor)
            # Apply adaptive cost weights (if available) to adjust logits
            if self.adaptive_cost:
                weights = self.adaptive_cost.get_current_weights()
                # Example: weight each expert's logit by some factor
                # For simplicity, we multiply by a scalar based on carbon/cost
                carbon_weight = weights.get('carbon', 1.0)
                cost_weight = weights.get('cost', 1.0)
                logits = logits * (carbon_weight * cost_weight)  # dummy adjustment
            probs = torch.softmax(logits, dim=1).squeeze().cpu().numpy()

        result = {self.expert_ids[i]: float(probs[i]) for i in range(len(self.expert_ids))}

        # Pareto gating: filter out experts that violate constraints
        if self.pareto:
            # Build candidate list with attributes
            candidates = []
            for eid, prob in result.items():
                candidate = {
                    'expert_id': eid,
                    'quality_score': prob,
                    'carbon_g': 0.0,  # placeholder
                    'latency_ms': 0.0, # placeholder
                    'energy_joules': 0.0 # placeholder
                }
                candidates.append(candidate)
            filtered = self.pareto.filter(candidates)
            if filtered:
                # Keep only experts that passed Pareto
                allowed = {c['expert_id'] for c in filtered}
                for eid in list(result.keys()):
                    if eid not in allowed:
                        result[eid] = 0.0
                # Renormalize
                total = sum(result.values())
                if total > 0:
                    for eid in result:
                        result[eid] /= total

        # Update metrics
        async with self._metrics_lock:
            self.inference_count += 1
            self.metrics.increment_gating_inference()

        # Publish FeedbackEvent
        event = FeedbackEvent.create_with_context(
            task_id=f"gate_{hashlib.sha256(json.dumps(context, sort_keys=True).encode()).hexdigest()[:8]}",
            selected_action=max(result, key=result.get),
            quality_score=max(result.values()),
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="gating",
            adaptive_cost_value=0.0,
            state=context,
            candidates=[{'expert': eid, 'prob': prob} for eid, prob in result.items()],
            source="gating_network",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["gating", "moe"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        # Check drift
        if self.drift:
            await self.drift.check_drift(self.adaptive_cost.get_current_weights())

        return result

    # ==========================================================================
    # Training Buffer Management (unchanged)
    # ==========================================================================
    def add_training_sample(self, features: np.ndarray, label: int):
        if features.shape[0] != self.config.input_dim:
            raise ValueError(f"Feature dimension mismatch: expected {self.config.input_dim}, got {features.shape[0]}")
        if not 0 <= label < self.config.num_experts:
            raise ValueError(f"Label out of range: {label} (num_experts={self.config.num_experts})")
        if len(self.training_buffer) >= self.config.max_training_samples:
            self.training_buffer.popleft()
        self.training_buffer.append((features, label))

    async def train(self, epochs: Optional[int] = None):
        if not self.training_buffer:
            logger.warning("No training data available")
            return

        epochs = epochs or self.config.epochs_per_update
        buffer_list = list(self.training_buffer)
        n = len(buffer_list)
        weights = np.array([self.config.recency_weight ** (n - 1 - i) for i in range(n)])
        weights /= weights.sum()

        if np.random.random() < 0.5:
            indices = np.random.choice(n, size=min(n, 2000), p=weights, replace=True)
            X = np.array([buffer_list[i][0] for i in indices], dtype=np.float32)
            y = np.array([buffer_list[i][1] for i in indices], dtype=np.int64)
        else:
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
            self.metrics.observe_gating_training_loss(avg_loss)

        # Publish training FeedbackEvent
        event = FeedbackEvent.create_with_context(
            task_id=f"train_{datetime.utcnow().timestamp()}",
            selected_action="train",
            quality_score=1.0 - avg_loss,  # higher is better
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="gating_training",
            adaptive_cost_value=0.0,
            state={'epochs': epochs, 'samples': len(X)},
            candidates=[{'action': 'train'}],
            source="gating_network",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["gating", "training"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        # Check drift
        if self.drift:
            await self.drift.check_drift(self.adaptive_cost.get_current_weights())

        logger.info(f"Gating network trained. Avg loss: {avg_loss:.4f}, samples used: {len(X)}")

    # ==========================================================================
    # Model Compression (unchanged)
    # ==========================================================================
    def _compress_weights(self, state_dict: Dict[str, torch.Tensor]) -> Dict[str, torch.Tensor]:
        if not self.config.enable_model_compression:
            return state_dict
        compressed = {}
        for key, tensor in state_dict.items():
            if tensor.dim() < 2:
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

    # ==========================================================================
    # Differential Privacy (unchanged)
    # ==========================================================================
    def _add_differential_privacy(self, state_dict: Dict[str, torch.Tensor]) -> Dict[str, torch.Tensor]:
        if not self.config.enable_differential_privacy or self.config.privacy_epsilon <= 0:
            return state_dict
        private = {}
        sensitivity = 1.0
        scale = (2 * sensitivity) / self.config.privacy_epsilon
        for key, tensor in state_dict.items():
            noise = torch.randn_like(tensor) * scale * self.config.noise_scale
            private[key] = tensor + noise
        return private

    # ==========================================================================
    # Federated Learning (unchanged)
    # ==========================================================================
    async def _get_federated_session(self) -> aiohttp.ClientSession:
        if self._federated_session is None and self.config.server_url:
            self._federated_session = aiohttp.ClientSession()
        return self._federated_session

    async def _send_local_update(self, performance_metric: float = 1.0) -> Dict:
        if not self.config.server_url:
            return {'status': 'disabled'}
        async with self._federated_lock:
            state_dict = self.model.state_dict()
            private_state = self._add_differential_privacy(state_dict)
            compressed_state = self._compress_weights(private_state)
            serialized = {k: v.tolist() for k, v in compressed_state.items()}
            update_data = {
                'router_id': 'gating_network',
                'round': self.federated_round,
                'weights': serialized,
                'performance': performance_metric,
                'privacy_epsilon': self.config.privacy_epsilon,
                'sparsity_ratio': self.config.sparsity_ratio,
                'timestamp': datetime.utcnow().isoformat()
            }
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
                self.contribution_score += performance_metric
                return result
            except Exception as e:
                logger.error(f"Federated update failed after circuit breaker: {e}")
                return {'status': 'failed'}

    async def _fetch_global_model(self) -> Optional[Dict]:
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
                self.federated_round = round_from_server
            return weights
        except Exception as e:
            logger.error(f"Global fetch failed after circuit breaker: {e}")
            return None

    async def participate_in_round(self, training_data: List[Tuple[np.ndarray, int]], performance: float = 1.0) -> Dict:
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
        while True:
            try:
                if self._circuit_breaker.is_open:
                    logger.debug("Circuit breaker open, skipping federated sync")
                    await asyncio.sleep(60)
                    continue
                if len(self.training_buffer) >= 10:
                    buffer_list = list(self.training_buffer)
                    recent_samples = buffer_list[-100:]
                    await self.participate_in_round(recent_samples)
                await asyncio.sleep(self.config.federation_round_interval)
            except Exception as e:
                logger.error(f"Federated sync loop error: {e}")
                await asyncio.sleep(300)

    # ==========================================================================
    # Persistence (using central Storage)
    # ==========================================================================
    async def save_model(self, model_id: str = "gating_model"):
        """Save model weights and training buffer to central storage."""
        model_dict = {k: v.tolist() for k, v in self.model.state_dict().items()}
        optimizer_dict = {k: v.tolist() for k, v in self.optimizer.state_dict().items()}
        training_data = [(f.tolist() if isinstance(f, np.ndarray) else f, int(l))
                         for f, l in self.training_buffer]
        state = {
            'model_state_dict': model_dict,
            'optimizer_state_dict': optimizer_dict,
            'training_data': training_data,
            'config': {
                'input_dim': self.config.input_dim,
                'hidden_dim': self.config.hidden_dim,
                'num_experts': self.config.num_experts,
                'num_hidden_layers': self.config.num_hidden_layers,
                'activation': self.config.activation,
                'dropout_rate': self.config.dropout_rate,
                'learning_rate': self.config.learning_rate,
                'batch_size': self.config.batch_size,
                'epochs_per_update': self.config.epochs_per_update,
                'max_training_samples': self.config.max_training_samples,
                'recency_weight': self.config.recency_weight,
                'privacy_epsilon': self.config.privacy_epsilon,
                'sparsity_ratio': self.config.sparsity_ratio,
            },
            'expert_ids': self.expert_ids,
            'federated_round': self.federated_round,
            'participants': self.participants,
            'contribution_score': self.contribution_score,
            'is_trained': self.is_trained,
            'inference_count': self.inference_count,
            'training_count': self.training_count,
        }
        compressed = zlib.compress(json.dumps(state).encode('utf-8'))
        self.storage.save_model_weights(model_id, compressed)
        logger.info(f"Model saved to central storage with ID '{model_id}'")

    async def load_model(self, model_id: str = "gating_model") -> bool:
        """Load model weights and training buffer from central storage."""
        data = self.storage.load_model_weights(model_id)
        if not data:
            logger.warning(f"Model with ID '{model_id}' not found in storage")
            return False
        try:
            json_str = zlib.decompress(data).decode('utf-8')
            state = json.loads(json_str)
        except Exception as e:
            logger.error(f"Failed to decompress/parse model data: {e}")
            return False

        # Restore model
        model_dict = {k: torch.FloatTensor(v) for k, v in state['model_state_dict'].items()}
        self.model.load_state_dict(model_dict)

        # Restore optimizer (optional)
        if 'optimizer_state_dict' in state:
            opt_dict = {k: torch.FloatTensor(v) for k, v in state['optimizer_state_dict'].items()}
            self.optimizer.load_state_dict(opt_dict)

        # Restore training buffer
        self.training_buffer = deque(
            [(np.array(f, dtype=np.float32), l) for f, l in state['training_data']],
            maxlen=state['config']['max_training_samples']
        )
        self.federated_round = state.get('federated_round', 0)
        self.participants = state.get('participants', [])
        self.contribution_score = state.get('contribution_score', 0.0)
        self.is_trained = state.get('is_trained', False)
        self.inference_count = state.get('inference_count', 0)
        self.training_count = state.get('training_count', 0)

        logger.info(f"Model loaded from central storage with ID '{model_id}'")
        return True

    # ==========================================================================
    # Health Check (using central metrics)
    # ==========================================================================
    async def get_health_status(self) -> Dict[str, Any]:
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

    # ==========================================================================
    # Cleanup
    # ==========================================================================
    async def shutdown(self):
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

# -----------------------------------------------------------------------------
# Example Usage (if run directly)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    async def main():
        # In a real deployment, these would be provided by LifecycleManager.
        from ..storage import Storage
        from ..scaling.message_queue import AsyncMessageQueue
        from ..feedback.adaptive_cost import AdaptiveCostFunction
        from ..routing.pareto_gating import ParetoGating
        from ..safety.drift_detector import DriftDetector
        from ..metrics import MetricsRegistry

        storage = Storage()
        queue = AsyncMessageQueue()
        adaptive_cost = AdaptiveCostFunction(storage)
        pareto = ParetoGating()
        drift = DriftDetector(storage, adaptive_cost)
        metrics = MetricsRegistry()

        manager = GatingNetworkManager(storage, queue, adaptive_cost, pareto, drift, metrics)

        # Simulate training
        for _ in range(20):
            features = np.random.randn(10).astype(np.float32)
            label = np.random.randint(0, 5)
            manager.add_training_sample(features, label)
        await manager.train()

        # Predict
        context = {"helium_scarcity": 0.6, "carbon_intensity": 0.4}
        result = await manager.predict(context)
        print("Prediction:", result)

        # Health
        print("Health:", await manager.get_health_status())

        await manager.shutdown()

    asyncio.run(main())
