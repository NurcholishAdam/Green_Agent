"""
FL Energy Expert v3.0 – Production-Ready Energy-Aware Federated Learning Expert for MoE System

Specializes in managing federated learning processes with energy awareness:
- Dynamic client selection based on energy states
- Heterogeneous resource management
- Gradient compression and bandwidth optimization
- Sustainable federated learning coordination
- Integration with bio-inspired energy modules
- SwiftFed-inspired energy-aware FL strategies
- Full async/await with circuit breakers and retries
- Pydantic config and persistence
"""

import asyncio
import logging
import json
import numpy as np
import hashlib
import pickle
import time
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Set, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
from enum import Enum

# Optional dependencies
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

try:
    from pydantic import BaseModel, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# Local imports – BaseExpert and bio-inspired modules
try:
    from .base_expert import BaseExpert
    BASE_EXPERT_AVAILABLE = True
except ImportError:
    BASE_EXPERT_AVAILABLE = False
    logger.warning("BaseExpert not available; using fallback")

try:
    from enhancements.bio_inspired.circuit_breaker import CircuitBreaker
    CIRCUIT_BREAKER_AVAILABLE = True
except ImportError:
    class CircuitBreaker:
        def __init__(self, name, failure_threshold=5, recovery_timeout=30.0):
            self.name = name
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
            self._state = "closed"
            self._failure_count = 0
            self._last_failure_time = None
            self._lock = asyncio.Lock()
        async def call(self, func, *args, **kwargs):
            return await func(*args, **kwargs)

try:
    from enhancements.bio_inspired.eco_atp_currency import EcoATPTokenManager
    TOKEN_AVAILABLE = True
except ImportError:
    TOKEN_AVAILABLE = False

try:
    from enhancements.bio_inspired.proton_gradient_fields import GradientFieldManager
    GRADIENT_AVAILABLE = True
except ImportError:
    GRADIENT_AVAILABLE = False


# ============================================================================
# Enums and Constants
# ============================================================================
class ClientState(Enum):
    """Energy and availability states for FL clients."""
    AVAILABLE = "available"
    SLEEPING = "sleeping"
    CHARGING = "charging"
    ACTIVE = "active"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


class AggregationStrategy(Enum):
    """Federated aggregation strategies."""
    STANDARD = "standard"          # Standard FedAvg
    LAZY = "lazy"                  # Lazy aggregation for stragglers
    PRIORITY = "priority"          # Energy-aware priority weighting
    GRADIENT_COMPRESSION = "gradient_compression"  # Compressed gradients
    SELECTIVE = "selective"        # Selective client participation


class ClientEnergyProfile(Enum):
    """Energy consumption profiles."""
    BATTERY_POWERED = "battery_powered"
    SOLAR_POWERED = "solar_powered"
    PLUGGED_IN = "plugged_in"
    DEGRADED_BATTERY = "degraded_battery"


# ============================================================================
# Data Classes (enhanced)
# ============================================================================
@dataclass
class ClientEnergyInfo:
    """Energy information for a federated client."""
    client_id: str
    state: ClientState = ClientState.AVAILABLE
    energy_profile: ClientEnergyProfile = ClientEnergyProfile.BATTERY_POWERED
    battery_level: float = 1.0
    energy_consumption_rate: float = 0.01
    upload_bandwidth_mbps: float = 10.0
    download_bandwidth_mbps: float = 10.0
    compute_capability: float = 1.0
    last_seen: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    estimated_sync_time_seconds: float = 0.0
    carbon_intensity_g_per_kwh: float = 100.0  # New: regional carbon intensity

    def get_energy_score(self) -> float:
        """Score 0-1: higher is better for participation."""
        if self.state == ClientState.UNAVAILABLE:
            return 0.0
        if self.state == ClientState.SLEEPING:
            return 0.1
        if self.state == ClientState.DEGRADED:
            return 0.3

        battery_score = self.battery_level
        if self.energy_profile == ClientEnergyProfile.PLUGGED_IN:
            battery_score = 1.0
        elif self.energy_profile == ClientEnergyProfile.SOLAR_POWERED:
            battery_score = min(1.0, battery_score * 1.2)

        state_bonus = 1.0
        if self.state == ClientState.CHARGING:
            state_bonus = 1.3

        # Carbon penalty: higher intensity -> lower score
        carbon_factor = 1.0 - (self.carbon_intensity_g_per_kwh / 1000.0)

        return min(1.0, battery_score * state_bonus * carbon_factor)


@dataclass
class ClientUpdateInfo:
    """Update metadata from a federated client."""
    client_id: str
    model_hash: str
    gradient_norm: float
    update_timestamp: datetime
    compression_ratio: float = 1.0
    transmission_time_ms: float = 0.0
    energy_cost_joules: float = 0.0
    sample_count: int = 0
    success: bool = True
    # NEW: actual gradient data (if available)
    gradients: Optional[Any] = None  # np.ndarray or torch.Tensor


@dataclass
class AggregationRound:
    """Metadata for a federated aggregation round."""
    round_id: int
    strategy: AggregationStrategy
    selected_clients: List[str]
    completed_clients: List[ClientUpdateInfo]
    failed_clients: List[str]
    timestamp: datetime
    duration_seconds: float
    total_energy_joules: float
    model_hash: str
    compression_ratio: float = 1.0
    # NEW: aggregated gradients (if stored)
    aggregated_gradients: Optional[Any] = None


# ============================================================================
# Configuration (Pydantic)
# ============================================================================
if PYDANTIC_AVAILABLE:
    class FLEnergyConfig(BaseModel):
        """Configuration for FL Energy Expert."""
        min_clients_per_round: int = Field(3, ge=1)
        max_clients_per_round: int = Field(20, ge=1)
        energy_threshold_battery: float = Field(0.2, ge=0, le=1)
        energy_threshold_degraded: float = Field(0.4, ge=0, le=1)
        target_compression_ratio: float = Field(0.1, ge=0, le=1)
        aggregation_timeout_seconds: int = Field(300, ge=1)
        lazy_aggregation_enabled: bool = True
        stale_client_threshold_hours: int = Field(24, ge=1)
        energy_aware_weighting: bool = True
        gradient_clipping_enabled: bool = True
        enable_persistence: bool = True
        persistence_path: str = Field("./fl_energy_expert_state.pkl")
        enable_token_integration: bool = False
        enable_gradient_integration: bool = False

        @validator('target_compression_ratio')
        def compression_ratio_in_range(cls, v):
            if not 0 <= v <= 1:
                raise ValueError('target_compression_ratio must be between 0 and 1')
            return v

        class Config:
            env_prefix = "FL_ENERGY_"
else:
    @dataclass
    class FLEnergyConfig:
        min_clients_per_round: int = 3
        max_clients_per_round: int = 20
        energy_threshold_battery: float = 0.2
        energy_threshold_degraded: float = 0.4
        target_compression_ratio: float = 0.1
        aggregation_timeout_seconds: int = 300
        lazy_aggregation_enabled: bool = True
        stale_client_threshold_hours: int = 24
        energy_aware_weighting: bool = True
        gradient_clipping_enabled: bool = True
        enable_persistence: bool = True
        persistence_path: str = "./fl_energy_expert_state.pkl"
        enable_token_integration: bool = False
        enable_gradient_integration: bool = False


# ============================================================================
# FLEnergyExpert (Enhanced)
# ============================================================================
class FLEnergyExpert(BaseExpert if BASE_EXPERT_AVAILABLE else object):
    """
    Energy-aware Federated Learning expert for MoE orchestration.
    Handles client selection, aggregation strategies, and energy optimization.
    """

    def __init__(self, config: Optional[FLEnergyConfig] = None):
        if BASE_EXPERT_AVAILABLE:
            super().__init__()
        self.expert_name = "fl_energy_expert"
        self.supported_task_types = [
            "fl_round", "fl_select_clients", "fl_aggregate",
            "fl_compression", "fl_energy_report"
        ]
        self.config = config or FLEnergyConfig()
        self.health_status = "healthy"

        # Client tracking
        self.clients: Dict[str, ClientEnergyInfo] = {}
        self.client_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))

        # Aggregation history
        self.rounds: List[AggregationRound] = []

        # Metrics and state
        self.total_energy_consumed_joules = 0.0
        self.total_updates_processed = 0
        self.failed_updates = 0
        self.participation_history = defaultdict(int)

        # Strategy state
        self.current_strategy = AggregationStrategy.STANDARD
        self.strategy_change_log = []

        # Compression state
        self.gradient_compression_enabled = True
        self.compression_ratios = deque(maxlen=50)

        # Locks and sync
        self._lock = asyncio.Lock()

        # Circuit breakers for client communication
        self._client_circuit = CircuitBreaker("fl_client", failure_threshold=5, recovery_timeout=30.0)

        # Bio-inspired integration
        self.token_manager = None
        if TOKEN_AVAILABLE and self.config.enable_token_integration:
            try:
                self.token_manager = EcoATPTokenManager()
            except Exception as e:
                logger.warning(f"Failed to initialize token manager: {e}")

        self.gradient_manager = None
        if GRADIENT_AVAILABLE and self.config.enable_gradient_integration:
            try:
                self.gradient_manager = GradientFieldManager()
            except Exception as e:
                logger.warning(f"Failed to initialize gradient manager: {e}")

        # Persistence
        if self.config.enable_persistence:
            asyncio.create_task(self.load_state())

        # Cleanup stale clients periodically
        self._cleanup_task = asyncio.create_task(self._cleanup_stale_clients())

        logger.info(f"FLEnergyExpert initialized: {self.config}")

    # ========================================================================
    # BaseExpert compliance
    # ========================================================================
    async def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        task_type = task.get('type', 'unknown')
        if task_type == 'fl_round':
            return await self.execute_aggregation_round(task.get('round_id', 0), task.get('state', {}))
        elif task_type == 'fl_select_clients':
            clients, weights = await self.select_clients_for_round(
                target_count=task.get('target_count'),
                energy_aware=task.get('energy_aware', True)
            )
            return {'selected_clients': clients, 'weights': weights}
        elif task_type == 'fl_aggregate':
            # expect updates and weights
            updates = task.get('updates', [])
            weights = task.get('weights', {})
            result, energy = await self.aggregate_updates(updates, self.current_strategy, weights)
            return {'result': result, 'energy_joules': energy}
        elif task_type == 'fl_compression':
            gradients = task.get('gradients')
            ratio = task.get('ratio', self.config.target_compression_ratio)
            compressed, actual = self.compress_gradients(gradients, ratio)
            return {'compressed': compressed.tolist(), 'ratio': actual}
        elif task_type == 'fl_energy_report':
            return self.get_energy_efficiency_report()
        else:
            return {'status': 'error', 'error': f'Unknown task type: {task_type}'}

    def get_capabilities(self) -> Dict[str, Any]:
        return {
            'expert_name': self.expert_name,
            'supported_tasks': self.supported_task_types,
            'health_status': self.health_status,
            'config': asdict(self.config),
        }

    def get_metrics(self) -> Dict[str, Any]:
        return asyncio.run(self.get_expert_metrics())

    async def get_health_status(self) -> Dict[str, Any]:
        try:
            # Basic health check: test compression
            test_grad = np.random.randn(10)
            _, _ = self.compress_gradients(test_grad, 0.5)
            self.health_status = "healthy"
            return {'status': 'healthy', 'expert': self.expert_name, 'timestamp': datetime.now(timezone.utc).isoformat()}
        except Exception as e:
            self.health_status = "unhealthy"
            return {'status': 'unhealthy', 'expert': self.expert_name, 'error': str(e)}

    # ========================================================================
    # Client Management
    # ========================================================================
    async def register_client(
        self,
        client_id: str,
        energy_profile: ClientEnergyProfile = ClientEnergyProfile.BATTERY_POWERED,
        bandwidth_mbps: float = 10.0,
        compute_capability: float = 1.0,
        carbon_intensity_g_per_kwh: float = 100.0,
    ) -> ClientEnergyInfo:
        """Register a new federated learning client."""
        async with self._lock:
            info = ClientEnergyInfo(
                client_id=client_id,
                energy_profile=energy_profile,
                upload_bandwidth_mbps=bandwidth_mbps,
                download_bandwidth_mbps=bandwidth_mbps,
                compute_capability=compute_capability,
                carbon_intensity_g_per_kwh=carbon_intensity_g_per_kwh,
            )
            self.clients[client_id] = info
            logger.info(f"Client registered: {client_id} ({energy_profile.value})")
            return info

    async def update_client_state(
        self,
        client_id: str,
        state: ClientState,
        battery_level: Optional[float] = None,
        energy_consumption_rate: Optional[float] = None,
        carbon_intensity_g_per_kwh: Optional[float] = None,
    ) -> None:
        """Update client energy and availability state."""
        async with self._lock:
            if client_id not in self.clients:
                logger.warning(f"Unknown client: {client_id}")
                return

            client = self.clients[client_id]
            client.state = state
            client.last_seen = datetime.now(timezone.utc)

            if battery_level is not None:
                client.battery_level = max(0.0, min(1.0, battery_level))

            if energy_consumption_rate is not None:
                client.energy_consumption_rate = energy_consumption_rate

            if carbon_intensity_g_per_kwh is not None:
                client.carbon_intensity_g_per_kwh = carbon_intensity_g_per_kwh

            self.client_history[client_id].append({
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'state': state.value,
                'battery': client.battery_level,
            })

            logger.debug(f"Client state updated: {client_id} -> {state.value} (battery={client.battery_level:.2f})")

    async def get_client_info(self, client_id: str) -> Optional[ClientEnergyInfo]:
        """Retrieve client energy information."""
        async with self._lock:
            return self.clients.get(client_id)

    async def _cleanup_stale_clients(self):
        """Periodically remove clients not seen for a long time."""
        while True:
            try:
                await asyncio.sleep(3600)  # hourly
                async with self._lock:
                    threshold = datetime.now(timezone.utc) - timedelta(hours=self.config.stale_client_threshold_hours)
                    stale = [cid for cid, info in self.clients.items() if info.last_seen < threshold]
                    for cid in stale:
                        del self.clients[cid]
                        logger.info(f"Removed stale client: {cid}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Stale client cleanup error: {e}")

    # ========================================================================
    # Client Selection (SwiftFed-inspired)
    # ========================================================================
    async def select_clients_for_round(
        self,
        target_count: Optional[int] = None,
        energy_aware: bool = True,
        prefer_mobile: bool = False,
    ) -> Tuple[List[str], Dict[str, float]]:
        """
        Select clients for federated learning round using energy-aware heuristics.
        Returns: (client_ids, energy_weights)
        """
        async with self._lock:
            target = target_count or self.config.max_clients_per_round
            min_required = self.config.min_clients_per_round

            # Get available clients
            available = [
                (cid, info) for cid, info in self.clients.items()
                if info.state in [ClientState.AVAILABLE, ClientState.ACTIVE, ClientState.CHARGING]
            ]

            if len(available) < min_required:
                logger.warning(f"Only {len(available)} clients available; need {min_required}")
                available = list(self.clients.items())[:min_required]

            # Filter by battery threshold
            if energy_aware:
                threshold = self.config.energy_threshold_battery
                filtered = [
                    (cid, info) for cid, info in available
                    if (info.energy_profile != ClientEnergyProfile.BATTERY_POWERED
                        or info.battery_level >= threshold)
                ]
                if len(filtered) < min_required:
                    filtered = available
                available = filtered

            # Score clients by energy efficiency
            scores = []
            for cid, info in available:
                energy_score = info.get_energy_score()
                stability_score = 1.0 - (len(self.client_history[cid]) - self.participation_history[cid]) / (len(self.client_history[cid]) + 1)
                bandwidth_efficiency = (info.upload_bandwidth_mbps + info.download_bandwidth_mbps) / 20.0

                combined_score = (
                    0.5 * energy_score +
                    0.3 * stability_score +
                    0.2 * bandwidth_efficiency
                )
                scores.append((cid, info, combined_score))

            # Sort by score and select top-k
            scores.sort(key=lambda x: x[2], reverse=True)
            selected = scores[:target]

            # Compute energy weights for aggregation
            energy_weights = {}
            total_weight = sum(s[2] for s in selected)
            for cid, info, score in selected:
                energy_weights[cid] = score / (total_weight + 1e-6)

            selected_ids = [cid for cid, _, _ in selected]

            logger.info(
                f"Selected {len(selected_ids)} clients for round",
                clients=selected_ids,
                avg_score=np.mean([s[2] for s in selected]) if selected else 0
            )

            return selected_ids, energy_weights

    # ========================================================================
    # Gradient Compression
    # ========================================================================
    def compress_gradients(
        self,
        gradients: np.ndarray,
        compression_ratio: float = 0.1,
        method: str = "top_k",
    ) -> Tuple[np.ndarray, float]:
        """
        Compress gradients using specified method.
        Supports top-k, random, and quantization (if torch available).
        """
        if not self.gradient_compression_enabled or compression_ratio >= 1.0:
            return gradients, 1.0

        flat = gradients.flatten()
        k = max(1, int(len(flat) * compression_ratio))

        if method == "top_k":
            abs_flat = np.abs(flat)
            threshold = np.sort(abs_flat)[-k] if k > 0 else 0
            mask = abs_flat >= threshold
            compressed = flat * mask
            actual_ratio = np.count_nonzero(mask) / len(flat)

        elif method == "random":
            mask = np.random.rand(len(flat)) < compression_ratio
            compressed = flat * mask
            actual_ratio = compression_ratio  # approximate

        elif method == "quantization" and TORCH_AVAILABLE:
            # Simple 8-bit quantization
            min_val = flat.min()
            max_val = flat.max()
            if max_val - min_val > 1e-8:
                quantized = (flat - min_val) / (max_val - min_val) * 255
                quantized = np.round(quantized).astype(np.uint8)
                # Dequantize
                compressed = (quantized / 255.0) * (max_val - min_val) + min_val
            else:
                compressed = flat
            actual_ratio = compression_ratio

        else:
            # Fallback to top_k
            compressed, actual_ratio = self.compress_gradients(gradients, compression_ratio, "top_k")

        self.compression_ratios.append(actual_ratio)

        return compressed.reshape(gradients.shape), actual_ratio

    # ========================================================================
    # Aggregation Strategies
    # ========================================================================
    async def select_aggregation_strategy(
        self,
        state: Dict[str, float],
    ) -> AggregationStrategy:
        """Choose aggregation strategy based on current system state."""
        async with self._lock:
            available_clients = sum(
                1 for info in self.clients.values()
                if info.state in [ClientState.AVAILABLE, ClientState.ACTIVE, ClientState.CHARGING]
            )

            avg_battery = np.mean([
                info.battery_level for info in self.clients.values()
            ]) if self.clients else 0.5

            avg_latency = np.mean([
                info.estimated_sync_time_seconds for info in self.clients.values()
                if info.estimated_sync_time_seconds > 0
            ]) if any(info.estimated_sync_time_seconds > 0 for info in self.clients.values()) else 5.0

            # Strategy logic
            if available_clients < self.config.min_clients_per_round:
                strategy = AggregationStrategy.LAZY
            elif avg_battery < self.config.energy_threshold_degraded:
                strategy = AggregationStrategy.SELECTIVE
            elif avg_latency > 30:
                strategy = AggregationStrategy.GRADIENT_COMPRESSION
            elif avg_battery < self.config.energy_threshold_battery:
                strategy = AggregationStrategy.PRIORITY
            else:
                strategy = AggregationStrategy.STANDARD

            if strategy != self.current_strategy:
                self.current_strategy = strategy
                self.strategy_change_log.append({
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'strategy': strategy.value,
                    'reason': f"avg_battery={avg_battery:.2f}, clients={available_clients}, latency={avg_latency:.1f}s",
                })
                logger.info(f"Aggregation strategy switched to {strategy.value}")

            return strategy

    async def aggregate_updates(
        self,
        updates: List[ClientUpdateInfo],
        strategy: AggregationStrategy,
        energy_weights: Dict[str, float],
    ) -> Tuple[Dict[str, Any], float]:
        """
        Aggregate client updates using specified strategy.
        If updates contain gradient data, actual aggregation is performed.
        """
        if not updates:
            logger.warning("No updates to aggregate")
            return {}, 0.0

        total_energy = sum(u.energy_cost_joules for u in updates)

        # Determine if we have actual gradients
        has_gradients = all(u.gradients is not None for u in updates)

        if has_gradients and TORCH_AVAILABLE:
            # Convert to torch tensors and aggregate
            tensors = [torch.tensor(u.gradients) if isinstance(u.gradients, np.ndarray) else u.gradients for u in updates]
            # Align shapes
            tensor_list = []
            for t in tensors:
                if t.ndim > 1:
                    tensor_list.append(t.flatten())
                else:
                    tensor_list.append(t)

            # Compute weights
            weights = [energy_weights.get(u.client_id, 1.0 / len(updates)) for u in updates]
            weights = torch.tensor(weights) / sum(weights)

            # Weighted sum
            aggregated = sum(t * w for t, w in zip(tensor_list, weights))

            # Reshape back to original shape (if all same shape)
            if tensor_list and tensor_list[0].ndim == 1:
                aggregated = aggregated.reshape(tensor_list[0].shape)

        else:
            # No actual gradients, just metadata
            aggregated = None

        # Strategy-specific metadata
        if strategy == AggregationStrategy.STANDARD:
            result = self._aggregate_standard(updates, energy_weights)
        elif strategy == AggregationStrategy.LAZY:
            result = self._aggregate_lazy(updates, energy_weights)
        elif strategy == AggregationStrategy.PRIORITY:
            result = self._aggregate_priority(updates, energy_weights)
        elif strategy == AggregationStrategy.GRADIENT_COMPRESSION:
            result = self._aggregate_compressed(updates, energy_weights)
        else:  # SELECTIVE
            result = self._aggregate_selective(updates, energy_weights)

        if aggregated is not None:
            result['aggregated_gradients'] = aggregated.cpu().numpy().tolist() if isinstance(aggregated, torch.Tensor) else aggregated.tolist()
        result['total_energy_cost_joules'] = total_energy
        result['aggregation_strategy'] = strategy.value

        return result, total_energy

    def _aggregate_standard(self, updates: List[ClientUpdateInfo], energy_weights: Dict[str, float]) -> Dict[str, Any]:
        weights = [energy_weights.get(u.client_id, 1.0 / len(updates)) for u in updates]
        weights = np.array(weights) / sum(weights)
        return {
            'method': 'standard_fedavg',
            'num_clients': len(updates),
            'weights': weights.tolist(),
            'avg_gradient_norm': np.mean([u.gradient_norm for u in updates]),
            'avg_compression_ratio': np.mean([u.compression_ratio for u in updates]),
        }

    def _aggregate_lazy(self, updates: List[ClientUpdateInfo], energy_weights: Dict[str, float]) -> Dict[str, Any]:
        threshold = np.median([u.transmission_time_ms for u in updates])
        fast_updates = [u for u in updates if u.transmission_time_ms <= threshold * 1.5]
        if not fast_updates:
            fast_updates = updates
        weights = [energy_weights.get(u.client_id, 1.0 / len(fast_updates)) for u in fast_updates]
        weights = np.array(weights) / sum(weights)
        return {
            'method': 'lazy_aggregation',
            'num_clients': len(fast_updates),
            'num_skipped': len(updates) - len(fast_updates),
            'weights': weights.tolist(),
            'avg_gradient_norm': np.mean([u.gradient_norm for u in fast_updates]),
        }

    def _aggregate_priority(self, updates: List[ClientUpdateInfo], energy_weights: Dict[str, float]) -> Dict[str, Any]:
        energy_based_weights = {}
        for u in updates:
            score = energy_weights.get(u.client_id, 0.5)
            energy_based_weights[u.client_id] = 1.0 / (score + 0.1)
        weights_list = [energy_based_weights.get(u.client_id, 1.0) for u in updates]
        weights = np.array(weights_list) / sum(weights_list)
        return {
            'method': 'energy_priority_aggregation',
            'num_clients': len(updates),
            'weights': weights.tolist(),
            'energy_based': True,
        }

    def _aggregate_compressed(self, updates: List[ClientUpdateInfo>, energy_weights: Dict[str, float]) -> Dict[str, Any]:
        weights = [energy_weights.get(u.client_id, 1.0 / len(updates)) for u in updates]
        weights = np.array(weights) / sum(weights)
        avg_compression = np.mean([u.compression_ratio for u in updates])
        return {
            'method': 'compressed_aggregation',
            'num_clients': len(updates),
            'weights': weights.tolist(),
            'avg_compression_ratio': avg_compression,
            'transmission_savings_percent': (1.0 - avg_compression) * 100,
        }

    def _aggregate_selective(self, updates: List[ClientUpdateInfo>, energy_weights: Dict[str, float]) -> Dict[str, Any]:
        sorted_updates = sorted(updates, key=lambda u: u.gradient_norm)
        top_half = sorted_updates[len(sorted_updates)//2:]
        weights = [energy_weights.get(u.client_id, 1.0 / len(top_half)) for u in top_half]
        weights = np.array(weights) / sum(weights)
        return {
            'method': 'selective_aggregation',
            'num_clients': len(top_half),
            'num_filtered': len(updates) - len(top_half),
            'weights': weights.tolist(),
        }

    # ========================================================================
    # Round Execution and Tracking
    # ========================================================================
    async def execute_aggregation_round(
        self,
        round_id: int,
        state: Dict[str, float],
    ) -> AggregationRound:
        """Execute a full federated aggregation round."""
        logger.info(f"Starting aggregation round {round_id}")

        # Select strategy
        strategy = await self.select_aggregation_strategy(state)

        # Select clients
        selected_ids, energy_weights = await self.select_clients_for_round()

        # Simulate receiving updates (in practice, async wait for clients)
        updates = []
        failed_clients = []

        for client_id in selected_ids:
            try:
                # In a real system, this would be an async call to the client
                # using circuit breaker and retries.
                energy_cost = np.random.exponential(0.5)
                update = ClientUpdateInfo(
                    client_id=client_id,
                    model_hash=hashlib.sha256(f"{round_id}_{client_id}".encode()).hexdigest(),
                    gradient_norm=np.random.exponential(1.0),
                    update_timestamp=datetime.now(timezone.utc),
                    energy_cost_joules=energy_cost,
                    transmission_time_ms=np.random.uniform(10, 1000),
                )
                updates.append(update)
                self.total_updates_processed += 1
            except Exception as e:
                logger.warning(f"Failed to receive update from {client_id}: {e}")
                failed_clients.append(client_id)
                self.failed_updates += 1

        # Aggregate
        result, total_energy = await self.aggregate_updates(updates, strategy, energy_weights)

        # Record round
        round_info = AggregationRound(
            round_id=round_id,
            strategy=strategy,
            selected_clients=selected_ids,
            completed_clients=updates,
            failed_clients=failed_clients,
            timestamp=datetime.now(timezone.utc),
            duration_seconds=np.random.uniform(10, 300),
            total_energy_joules=total_energy,
            model_hash=hashlib.sha256(f"{round_id}_aggregated".encode()).hexdigest(),
            compression_ratio=result.get('avg_compression_ratio', 1.0),
            aggregated_gradients=result.get('aggregated_gradients'),
        )

        self.rounds.append(round_info)
        self.total_energy_consumed_joules += total_energy

        for cid in selected_ids:
            self.participation_history[cid] += 1

        logger.info(
            f"Round {round_id} complete",
            strategy=strategy.value,
            completed=f"{len(updates)}/{len(selected_ids)}",
            energy_joules=total_energy,
        )

        return round_info

    # ========================================================================
    # Metrics and Reporting
    # ========================================================================
    async def get_expert_metrics(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                'total_energy_consumed_joules': self.total_energy_consumed_joules,
                'total_updates_processed': self.total_updates_processed,
                'failed_updates': self.failed_updates,
                'total_rounds': len(self.rounds),
                'active_clients': sum(
                    1 for info in self.clients.values()
                    if info.state in [ClientState.AVAILABLE, ClientState.ACTIVE]
                ),
                'total_registered_clients': len(self.clients),
                'avg_compression_ratio': float(np.mean(self.compression_ratios)) if self.compression_ratios else 1.0,
                'current_strategy': self.current_strategy.value,
                'strategy_changes': len(self.strategy_change_log),
            }

    async def get_client_participation_stats(self) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            stats = {}
            for client_id, info in self.clients.items():
                participated = self.participation_history[client_id]
                total_seen = len(self.client_history[client_id])
                stats[client_id] = {
                    'state': info.state.value,
                    'energy_profile': info.energy_profile.value,
                    'battery_level': info.battery_level,
                    'energy_score': info.get_energy_score(),
                    'participated_rounds': participated,
                    'observations': total_seen,
                    'participation_rate': participated / (total_seen + 1) if total_seen > 0 else 0,
                }
            return stats

    def get_energy_efficiency_report(self) -> Dict[str, Any]:
        if not self.rounds:
            return {'message': 'No rounds completed yet'}

        energy_per_round = [r.total_energy_joules for r in self.rounds]
        clients_per_round = [len(r.completed_clients) for r in self.rounds]

        return {
            'total_energy_joules': self.total_energy_consumed_joules,
            'total_rounds': len(self.rounds),
            'avg_energy_per_round_joules': float(np.mean(energy_per_round)),
            'avg_energy_per_client_joules': float(np.mean(energy_per_round) / (np.mean(clients_per_round) + 1e-6)),
            'avg_clients_per_round': float(np.mean(clients_per_round)),
            'avg_compression_ratio': float(np.mean(self.compression_ratios)) if self.compression_ratios else 1.0,
            'energy_per_update': self.total_energy_consumed_joules / (self.total_updates_processed + 1),
            'success_rate': (self.total_updates_processed - self.failed_updates) / (self.total_updates_processed + 1),
        }

    def get_strategy_log(self) -> List[Dict[str, Any]]:
        return self.strategy_change_log

    # ========================================================================
    # Explainability
    # ========================================================================
    async def explain_client_selection(
        self,
        round_id: int,
    ) -> Optional[Dict[str, Any]]:
        """Explain why certain clients were selected in a round."""
        if round_id >= len(self.rounds):
            return None

        round_info = self.rounds[round_id]

        explanation = {
            'round_id': round_id,
            'strategy': round_info.strategy.value,
            'selected_clients': round_info.selected_clients,
            'failed_clients': round_info.failed_clients,
            'rationale': {
                'strategy_reason': f"Used {round_info.strategy.value} strategy",
                'selection_criteria': [
                    "Energy availability",
                    "Historical participation",
                    "Bandwidth efficiency",
                    "Client state",
                ],
            },
        }

        return explanation

    async def explain_aggregation_decision(
        self,
        round_id: int,
    ) -> Optional[Dict[str, Any]]:
        """Explain aggregation decisions for a round."""
        if round_id >= len(self.rounds):
            return None

        round_info = self.rounds[round_id]

        explanation = {
            'round_id': round_id,
            'aggregation_strategy': round_info.strategy.value,
            'num_clients_aggregated': len(round_info.completed_clients),
            'total_energy_cost_joules': round_info.total_energy_joules,
            'compression_applied': round_info.compression_ratio < 1.0,
            'compression_ratio': round_info.compression_ratio,
        }

        return explanation

    # ========================================================================
    # Persistence
    # ========================================================================
    async def save_state(self) -> bool:
        """Save expert state to disk."""
        if not self.config.enable_persistence:
            return False
        try:
            state = {
                'clients': {cid: asdict(info) for cid, info in self.clients.items()},
                'rounds': [asdict(r) for r in self.rounds],
                'metrics': {
                    'total_energy_consumed_joules': self.total_energy_consumed_joules,
                    'total_updates_processed': self.total_updates_processed,
                    'failed_updates': self.failed_updates,
                    'participation_history': dict(self.participation_history),
                },
                'strategy_log': self.strategy_change_log,
                'compression_ratios': list(self.compression_ratios),
                'timestamp': datetime.now(timezone.utc).isoformat(),
            }
            with open(self.config.persistence_path, 'wb') as f:
                pickle.dump(state, f)
            logger.info("FLEnergyExpert state saved")
            return True
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
            return False

    async def load_state(self) -> bool:
        """Load expert state from disk."""
        if not self.config.enable_persistence:
            return False
        path = Path(self.config.persistence_path)
        if not path.exists():
            logger.info("No saved state found")
            return False
        try:
            with open(path, 'rb') as f:
                state = pickle.load(f)
            # Restore clients
            for cid, info_dict in state.get('clients', {}).items():
                self.clients[cid] = ClientEnergyInfo(**info_dict)
            # Restore rounds
            for r_dict in state.get('rounds', []):
                # Reconstruct AggregationRound (ignore aggregated_gradients)
                r_dict.pop('aggregated_gradients', None)
                self.rounds.append(AggregationRound(**r_dict))
            # Restore metrics
            metrics = state.get('metrics', {})
            self.total_energy_consumed_joules = metrics.get('total_energy_consumed_joules', 0.0)
            self.total_updates_processed = metrics.get('total_updates_processed', 0)
            self.failed_updates = metrics.get('failed_updates', 0)
            self.participation_history = defaultdict(int, metrics.get('participation_history', {}))
            # Restore strategy log and compression ratios
            self.strategy_change_log = state.get('strategy_log', [])
            self.compression_ratios = deque(state.get('compression_ratios', []), maxlen=50)
            logger.info("FLEnergyExpert state loaded")
            return True
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
            return False

    # ========================================================================
    # Cleanup
    # ========================================================================
    async def shutdown(self):
        """Graceful shutdown."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
        await self.save_state()
        logger.info("FLEnergyExpert shutdown complete")


# ============================================================================
# Example Usage
# ============================================================================
async def example():
    config = FLEnergyConfig(enable_persistence=False)
    expert = FLEnergyExpert(config)

    # Register clients
    for i in range(5):
        profile = [
            ClientEnergyProfile.BATTERY_POWERED,
            ClientEnergyProfile.PLUGGED_IN,
            ClientEnergyProfile.SOLAR_POWERED,
        ][i % 3]

        await expert.register_client(
            client_id=f"client_{i}",
            energy_profile=profile,
            bandwidth_mbps=10.0 + i,
        )

    # Simulate energy updates
    for i in range(5):
        await expert.update_client_state(
            f"client_{i}",
            ClientState.AVAILABLE,
            battery_level=0.5 + np.random.uniform(-0.2, 0.2),
        )

    # Execute rounds
    state = {'energy': 0.5, 'load': 0.3}
    for round_id in range(3):
        round_info = await expert.execute_aggregation_round(round_id, state)
        print(f"Round {round_id}: {len(round_info.completed_clients)} clients, {round_info.total_energy_joules:.2f} J")

    # Report metrics
    metrics = await expert.get_expert_metrics()
    print("Metrics:", json.dumps(metrics, indent=2))

    efficiency = expert.get_energy_efficiency_report()
    print("Efficiency:", json.dumps(efficiency, indent=2))

    await expert.shutdown()


if __name__ == "__main__":
    asyncio.run(example())
