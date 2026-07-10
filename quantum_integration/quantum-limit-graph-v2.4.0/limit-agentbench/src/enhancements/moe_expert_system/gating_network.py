"""
Gating Network Module for MoE Expert System v1.0.0

This module provides a neural gating network that selects which expert(s) to activate
for a given input. It is designed to be used with the ExpertRouter and integrates
with carbon/helix optimization, federated learning, predictive analytics, and
sustainability tracking.

Features:
- Configurable neural network architecture via dataclass.
- Online training with incremental updates.
- Differential privacy and model compression (top-k sparsification).
- Integration with real-time carbon/helix signals.
- Federated learning participation.
- Telemetry and metrics export.
- Resilience with retry and circuit breaker.
- Asynchronous inference and training.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Union, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
import aiohttp
import json
import os
import hashlib
import zlib
import pickle
from collections import deque, defaultdict

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration Dataclass
# ============================================================================

@dataclass
class GatingNetworkConfig:
    """Centralized configuration for the Gating Network Manager."""
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
    input_dim: int = 10
    hidden_dim: int = 64
    num_experts: int = 5
    dropout_rate: float = 0.1
    learning_rate: float = 0.001
    batch_size: int = 32
    epochs_per_update: int = 3

    # Training parameters
    max_training_samples: int = 10000
    online_learning_rate: float = 0.01
    momentum: float = 0.9
    weight_decay: float = 0.0001

    # Privacy and compression
    privacy_epsilon: float = 1.0
    noise_scale: float = 0.001
    sparsity_ratio: float = 0.1  # top-k% of weights to keep

    # Federated learning
    server_url: Optional[str] = None
    federation_round_interval: int = 3600  # seconds

    # Resilience
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    # Telemetry
    telemetry_export_interval: int = 60

    def __post_init__(self):
        # Ensure boolean flags
        for key, value in self.__dict__.items():
            if isinstance(value, bool):
                setattr(self, key, bool(value))

# ============================================================================
# Neural Network Model
# ============================================================================

class GatingNetwork(nn.Module):
    """
    Neural network for expert gating.

    Architecture:
    - Input layer: `input_dim` features
    - Hidden layers with ReLU and batch normalization
    - Dropout for regularization
    - Output layer: `num_experts` logits (softmax applied externally)
    """

    def __init__(self, input_dim: int, hidden_dim: int, num_experts: int, dropout_rate: float = 0.1):
        super().__init__()
        self.network = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.BatchNorm1d(hidden_dim),
            nn.Dropout(dropout_rate),
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.ReLU(),
            nn.BatchNorm1d(hidden_dim // 2),
            nn.Dropout(dropout_rate),
            nn.Linear(hidden_dim // 2, num_experts)
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.network(x)

# ============================================================================
# Gating Network Manager
# ============================================================================

class GatingNetworkManager:
    """
    Manages the gating network, training, inference, and integration with the MoE system.

    Features:
    - Online training with incremental updates.
    - Differential privacy and model compression.
    - Federated learning participation.
    - Integration with carbon/helix signals (via feature engineering).
    - Resilience with retry and circuit breaker for external calls.
    - Telemetry export.
    """

    def __init__(
        self,
        config: Optional[GatingNetworkConfig] = None,
        carbon_manager: Optional[Any] = None,   # CarbonIntensityManager
        helium_optimizer: Optional[Any] = None, # HeliumEfficiencyOptimizer
        expert_ids: Optional[List[str]] = None, # List of expert IDs (order matches output)
    ):
        self.config = config or GatingNetworkConfig()
        self.carbon_manager = carbon_manager
        self.helium_optimizer = helium_optimizer
        self.expert_ids = expert_ids or [f"expert_{i}" for i in range(self.config.num_experts)]

        # Validate expert count
        if len(self.expert_ids) != self.config.num_experts:
            raise ValueError(f"Number of expert IDs ({len(self.expert_ids)}) must match num_experts ({self.config.num_experts})")

        # Model
        self.model = GatingNetwork(
            input_dim=self.config.input_dim,
            hidden_dim=self.config.hidden_dim,
            num_experts=self.config.num_experts,
            dropout_rate=self.config.dropout_rate
        )
        self.optimizer = optim.Adam(
            self.model.parameters(),
            lr=self.config.learning_rate,
            weight_decay=self.config.weight_decay
        )
        self.criterion = nn.CrossEntropyLoss()

        # Training state
        self.training_data: List[Tuple[np.ndarray, int]] = []  # (features, label)
        self.is_trained = False
        self.global_model_state: Optional[Dict] = None

        # Federated learning
        self.federated_round = 0
        self.participants = []
        self.contribution_score = 0.0
        self._federated_session: Optional[aiohttp.ClientSession] = None
        self._federated_lock = asyncio.Lock()

        # Circuit breaker for external calls
        self.failure_count = 0
        self.circuit_open = False
        self.circuit_open_until: Optional[datetime] = None

        # Telemetry
        self.metrics: Dict[str, Any] = defaultdict(lambda: defaultdict(int))
        self.inference_count = 0
        self.training_count = 0

        # Background tasks
        self._background_tasks: List[asyncio.Task] = []
        if self.config.enable_federated and self.config.server_url:
            self._background_tasks.append(asyncio.create_task(self._federated_sync_loop()))
        if self.config.enable_telemetry:
            self._background_tasks.append(asyncio.create_task(self._telemetry_export_loop()))

        logger.info(f"GatingNetworkManager initialized with {self.config.num_experts} experts")

    # ============================================================================
    # Feature Engineering
    # ============================================================================

    def _build_features(self, context: Dict[str, Any]) -> np.ndarray:
        """
        Build feature vector from context for the gating network.
        Features can include carbon intensity, helium scarcity, gradients, etc.
        """
        features = []

        # Core features (from context)
        features.append(context.get('helium_scarcity', 0.5))
        features.append(context.get('helium_cost_index', 1.0))
        features.append(context.get('carbon_intensity', 0.5))
        features.append(context.get('model_loss', 0.0))
        features.append(context.get('gradient_variance', 0.0))
        features.append(context.get('avg_client_energy', 0.5))
        features.append(context.get('gradient_carbon', 0.5))
        features.append(context.get('gradient_helium', 0.5))
        features.append(context.get('token_balance_norm', 0.5))
        features.append(context.get('harvester_stress', 0.3))

        # Optionally add causal or signal integration features
        if self.config.enable_causal_features:
            features.append(context.get('causal_impact_carbon', 0.0))
            features.append(context.get('causal_impact_helium', 0.0))
            # If feature length > config.input_dim, we would need to truncate or pad.
            # For simplicity, we assume the context already provides exactly input_dim features.

        # Ensure correct dimension
        if len(features) != self.config.input_dim:
            # Pad or truncate
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
        """
        features = self._build_features(context)
        features_tensor = torch.FloatTensor(features).unsqueeze(0)

        with torch.no_grad():
            logits = self.model(features_tensor)
            probs = torch.softmax(logits, dim=1).squeeze().cpu().numpy()

        result = {self.expert_ids[i]: float(probs[i]) for i in range(len(self.expert_ids))}

        self.inference_count += 1
        if self.config.enable_telemetry:
            self.metrics['counters']['inference_total'] += 1

        return result

    # ============================================================================
    # Training
    # ============================================================================

    def add_training_sample(self, features: np.ndarray, label: int):
        """Add a single training sample (features, expert index) for online learning."""
        if len(self.training_data) >= self.config.max_training_samples:
            # Remove oldest sample
            self.training_data.pop(0)
        self.training_data.append((features, label))

    async def train(self, epochs: Optional[int] = None):
        """
        Train the model on accumulated training data.
        If online learning is enabled, performs incremental updates.
        """
        if not self.training_data:
            logger.warning("No training data available")
            return

        epochs = epochs or self.config.epochs_per_update

        # Prepare data
        X = np.array([sample[0] for sample in self.training_data], dtype=np.float32)
        y = np.array([sample[1] for sample in self.training_data], dtype=np.int64)

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
        self.training_count += 1

        if self.config.enable_telemetry:
            self.metrics['histograms']['training_loss'].append(avg_loss)

        logger.info(f"Gating network trained. Avg loss: {avg_loss:.4f}, samples: {len(self.training_data)}")

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
    # Federated Learning
    # ============================================================================

    async def _get_federated_session(self) -> aiohttp.ClientSession:
        if self._federated_session is None and self.config.server_url:
            self._federated_session = aiohttp.ClientSession()
        return self._federated_session

    async def _send_local_update(self, performance_metric: float = 1.0) -> Dict:
        """Send local model update to federated server."""
        if not self.config.server_url:
            return {'status': 'disabled'}

        async with self._federated_lock:
            # Prepare model weights
            state_dict = self.model.state_dict()
            # Apply differential privacy
            private_state = self._add_differential_privacy(state_dict)
            # Compress
            compressed_state = self._compress_weights(private_state)
            # Serialize
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

            # Retry with circuit breaker
            for attempt in range(self.config.max_retries):
                try:
                    session = await self._get_federated_session()
                    async with session.post(
                        f"{self.config.server_url}/federated/gating/update",
                        json=update_data,
                        timeout=30
                    ) as response:
                        if response.status == 200:
                            result = await response.json()
                            self.federated_round += 1
                            self.contribution_score += performance_metric
                            self.failure_count = 0
                            return result
                        else:
                            logger.warning(f"Federated update failed (attempt {attempt+1}): {response.status}")
                except Exception as e:
                    logger.error(f"Federated update error (attempt {attempt+1}): {e}")
                await asyncio.sleep(2 ** attempt)

            # Circuit breaker logic
            self.failure_count += 1
            if self.failure_count >= self.config.circuit_breaker_failure_threshold:
                self.circuit_open = True
                self.circuit_open_until = datetime.utcnow() + timedelta(seconds=self.config.circuit_breaker_recovery_timeout)
                logger.error("Circuit breaker opened for federated updates")
            return {'status': 'failed'}

    async def _fetch_global_model(self) -> Optional[Dict]:
        """Fetch global model from federated server."""
        if not self.config.server_url:
            return None

        async with self._federated_lock:
            for attempt in range(self.config.max_retries):
                try:
                    session = await self._get_federated_session()
                    async with session.get(
                        f"{self.config.server_url}/federated/gating/global",
                        timeout=30
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            weights = data.get('weights', {})
                            self.federated_round = data.get('round', 0)
                            self.participants = data.get('participants', [])
                            # Load weights into model
                            state_dict = {k: torch.FloatTensor(v) for k, v in weights.items()}
                            self.model.load_state_dict(state_dict)
                            self.global_model_state = state_dict
                            self.is_trained = True
                            return weights
                        else:
                            logger.warning(f"Global fetch failed (attempt {attempt+1}): {response.status}")
                except Exception as e:
                    logger.error(f"Global fetch error (attempt {attempt+1}): {e}")
                await asyncio.sleep(2 ** attempt)
            return None

    async def participate_in_round(self, training_data: List[Tuple[np.ndarray, int]], performance: float = 1.0) -> Dict:
        """Participate in one federated learning round."""
        # Add samples to local training data
        for features, label in training_data:
            self.add_training_sample(features, label)
        # Train locally
        await self.train()
        # Send update
        update_result = await self._send_local_update(performance)
        # Fetch global model
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
                if self.circuit_open:
                    if datetime.utcnow() < self.circuit_open_until:
                        await asyncio.sleep(60)
                        continue
                    else:
                        self.circuit_open = False
                        self.failure_count = 0
                        logger.info("Circuit breaker reset for federated sync")

                # Only sync if we have enough training data
                if len(self.training_data) >= 10:
                    # Use last 100 samples for federated update
                    recent_samples = self.training_data[-100:]
                    await self.participate_in_round(recent_samples)
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
                # For demonstration, just log metrics
                logger.debug(f"Gating telemetry: {self.get_telemetry()}")
                # In production, you would push to Prometheus or an endpoint
                await asyncio.sleep(self.config.telemetry_export_interval)
            except Exception as e:
                logger.error(f"Telemetry export error: {e}")
                await asyncio.sleep(60)

    def get_telemetry(self) -> Dict[str, Any]:
        """Return current telemetry metrics."""
        return {
            'inference_count': self.inference_count,
            'training_count': self.training_count,
            'training_samples': len(self.training_data),
            'is_trained': self.is_trained,
            'federated_round': self.federated_round,
            'participants': len(self.participants),
            'contribution_score': self.contribution_score,
            'circuit_open': self.circuit_open,
            'metrics': {
                'counters': dict(self.metrics['counters']),
                'histograms': {k: list(v) for k, v in self.metrics['histograms'].items()}
            }
        }

    # ============================================================================
    # Persistence
    # ============================================================================

    def save_model(self, path: str):
        """Save model and training state to disk."""
        state = {
            'model_state_dict': self.model.state_dict(),
            'optimizer_state_dict': self.optimizer.state_dict(),
            'training_data': self.training_data,
            'config': self.config,
            'expert_ids': self.expert_ids,
            'federated_round': self.federated_round,
            'participants': self.participants,
            'contribution_score': self.contribution_score,
            'is_trained': self.is_trained
        }
        with open(path, 'wb') as f:
            pickle.dump(state, f)
        logger.info(f"Model saved to {path}")

    def load_model(self, path: str):
        """Load model and training state from disk."""
        with open(path, 'rb') as f:
            state = pickle.load(f)
        self.model.load_state_dict(state['model_state_dict'])
        self.optimizer.load_state_dict(state['optimizer_state_dict'])
        self.training_data = state['training_data']
        # Note: config and expert_ids are not loaded to avoid mismatch; we trust the current config.
        self.federated_round = state.get('federated_round', 0)
        self.participants = state.get('participants', [])
        self.contribution_score = state.get('contribution_score', 0.0)
        self.is_trained = state.get('is_trained', False)
        logger.info(f"Model loaded from {path}")

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
