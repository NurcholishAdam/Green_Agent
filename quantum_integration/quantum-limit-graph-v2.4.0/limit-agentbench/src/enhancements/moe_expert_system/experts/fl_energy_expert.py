"""
FL Energy Expert v2.0 – Energy-Aware Federated Learning Expert for MoE System

Specializes in managing federated learning processes with energy awareness:
- Dynamic client selection based on energy states
- Heterogeneous resource management
- Gradient compression and bandwidth optimization
- Sustainable federated learning coordination
- Integration with bio-inspired energy modules
- SwiftFed-inspired energy-aware FL strategies
"""

import asyncio
import logging
import json
import numpy as np
from typing import Dict, Any, List, Optional, Tuple, Set
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
from enum import Enum
import hashlib

# Optional dependencies
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)


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
    STANDARD = "standard"  # Standard FedAvg
    LAZY = "lazy"  # Lazy aggregation for stragglers
    PRIORITY = "priority"  # Energy-aware priority weighting
    GRADIENT_COMPRESSION = "gradient_compression"  # Compressed gradients
    SELECTIVE = "selective"  # Selective client participation


class ClientEnergyProfile(Enum):
    """Energy consumption profiles."""
    BATTERY_POWERED = "battery_powered"  # Mobile/edge devices
    SOLAR_POWERED = "solar_powered"  # Renewable energy
    PLUGGED_IN = "plugged_in"  # Unlimited power
    DEGRADED_BATTERY = "degraded_battery"  # Low battery


# ============================================================================
# Data Classes
# ============================================================================
@dataclass
class ClientEnergyInfo:
    """Energy information for a federated client."""
    client_id: str
    state: ClientState = ClientState.AVAILABLE
    energy_profile: ClientEnergyProfile = ClientEnergyProfile.BATTERY_POWERED
    battery_level: float = 1.0  # 0-1
    energy_consumption_rate: float = 0.01  # units per second
    upload_bandwidth_mbps: float = 10.0
    download_bandwidth_mbps: float = 10.0
    compute_capability: float = 1.0  # relative to baseline
    last_seen: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    estimated_sync_time_seconds: float = 0.0
    
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
        
        return min(1.0, battery_score * state_bonus)


@dataclass
class ClientUpdateInfo:
    """Update metadata from a federated client."""
    client_id: str
    model_hash: str
    gradient_norm: float
    update_timestamp: datetime
    compression_ratio: float = 1.0  # 1.0 = no compression
    transmission_time_ms: float = 0.0
    energy_cost_joules: float = 0.0
    sample_count: int = 0
    success: bool = True


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


# ============================================================================
# FL Energy Expert
# ============================================================================
class FLEnergyExpert:
    """
    Energy-aware Federated Learning expert for MoE orchestration.
    Handles client selection, aggregation strategies, and energy optimization.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the FL Energy Expert."""
        self.config = config or self._default_config()
        
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
        
        logger.info("FLEnergyExpert initialized", config=self.config)
    
    @staticmethod
    def _default_config() -> Dict[str, Any]:
        return {
            'min_clients_per_round': 3,
            'max_clients_per_round': 20,
            'energy_threshold_battery': 0.2,  # Below 20%: exclude battery devices
            'energy_threshold_degraded': 0.4,  # Below 40%: prefer plugged-in
            'target_compression_ratio': 0.1,  # 10% of original gradient size
            'aggregation_timeout_seconds': 300,
            'lazy_aggregation_enabled': True,
            'stale_client_threshold_hours': 24,
            'energy_aware_weighting': True,
            'gradient_clipping_enabled': True,
        }
    
    # ========================================================================
    # Client Management
    # ========================================================================
    async def register_client(
        self,
        client_id: str,
        energy_profile: ClientEnergyProfile = ClientEnergyProfile.BATTERY_POWERED,
        bandwidth_mbps: float = 10.0,
        compute_capability: float = 1.0,
    ) -> ClientEnergyInfo:
        """Register a new federated learning client."""
        async with self._lock:
            info = ClientEnergyInfo(
                client_id=client_id,
                energy_profile=energy_profile,
                upload_bandwidth_mbps=bandwidth_mbps,
                download_bandwidth_mbps=bandwidth_mbps,
                compute_capability=compute_capability,
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
            target = target_count or self.config['max_clients_per_round']
            min_required = self.config['min_clients_per_round']
            
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
                threshold = self.config['energy_threshold_battery']
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
    ) -> Tuple[np.ndarray, float]:
        """
        Compress gradients using top-k sparsification.
        
        Args:
            gradients: Flattened gradient vector
            compression_ratio: Target ratio of kept values (0-1)
        
        Returns: (compressed_gradients, actual_compression_ratio)
        """
        if not self.gradient_compression_enabled or compression_ratio >= 1.0:
            return gradients, 1.0
        
        flat = gradients.flatten()
        k = max(1, int(len(flat) * compression_ratio))
        
        # Top-k selection
        abs_flat = np.abs(flat)
        threshold = np.sort(abs_flat)[-k] if k > 0 else 0
        mask = abs_flat >= threshold
        
        compressed = flat * mask
        actual_ratio = np.count_nonzero(mask) / len(flat)
        
        self.compression_ratios.append(actual_ratio)
        
        return compressed.reshape(gradients.shape), actual_ratio
    
    # ========================================================================
    # Aggregation Strategies
    # ========================================================================
    async def select_aggregation_strategy(
        self,
        state: Dict[str, float],
    ) -> AggregationStrategy:
        """
        Choose aggregation strategy based on current system state.
        """
        available_clients = sum(
            1 for info in self.clients.values()
            if info.state in [ClientState.AVAILABLE, ClientState.ACTIVE, ClientState.CHARGING]
        )
        
        # Get energy state
        avg_battery = np.mean([
            info.battery_level for info in self.clients.values()
        ]) if self.clients else 0.5
        
        avg_latency = np.mean([
            info.estimated_sync_time_seconds for info in self.clients.values()
            if info.estimated_sync_time_seconds > 0
        ]) if any(info.estimated_sync_time_seconds > 0 for info in self.clients.values()) else 5.0
        
        # Strategy logic
        if available_clients < self.config['min_clients_per_round']:
            strategy = AggregationStrategy.LAZY
        elif avg_battery < self.config['energy_threshold_degraded']:
            strategy = AggregationStrategy.SELECTIVE
        elif avg_latency > 30:
            strategy = AggregationStrategy.GRADIENT_COMPRESSION
        elif avg_battery < self.config['energy_threshold_battery']:
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
        
        Returns: (aggregated_result, total_energy_cost)
        """
        if not updates:
            logger.warning("No updates to aggregate")
            return {}, 0.0
        
        total_energy = sum(u.energy_cost_joules for u in updates)
        
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
        
        result['total_energy_cost_joules'] = total_energy
        result['aggregation_strategy'] = strategy.value
        
        return result, total_energy
    
    def _aggregate_standard(
        self,
        updates: List[ClientUpdateInfo],
        energy_weights: Dict[str, float],
    ) -> Dict[str, Any]:
        """FedAvg with optional energy weighting."""
        weights = [energy_weights.get(u.client_id, 1.0 / len(updates)) for u in updates]
        weights = np.array(weights) / sum(weights)
        
        avg_compression = np.mean([u.compression_ratio for u in updates])
        
        return {
            'method': 'standard_fedavg',
            'num_clients': len(updates),
            'weights': weights.tolist(),
            'avg_gradient_norm': np.mean([u.gradient_norm for u in updates]),
            'avg_compression_ratio': avg_compression,
        }
    
    def _aggregate_lazy(
        self,
        updates: List[ClientUpdateInfo],
        energy_weights: Dict[str, float],
    ) -> Dict[str, Any]:
        """Lazy aggregation: skip stragglers."""
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
    
    def _aggregate_priority(
        self,
        updates: List[ClientUpdateInfo],
        energy_weights: Dict[str, float],
    ) -> Dict[str, Any]:
        """Priority aggregation with energy-aware weighting."""
        # Higher weight for low-energy clients
        energy_based_weights = {}
        for u in updates:
            score = energy_weights.get(u.client_id, 0.5)
            # Inverse weighting: lower energy → higher weight to incentivize participation
            energy_based_weights[u.client_id] = 1.0 / (score + 0.1)
        
        weights_list = [energy_based_weights.get(u.client_id, 1.0) for u in updates]
        weights = np.array(weights_list) / sum(weights_list)
        
        return {
            'method': 'energy_priority_aggregation',
            'num_clients': len(updates),
            'weights': weights.tolist(),
            'energy_based': True,
        }
    
    def _aggregate_compressed(
        self,
        updates: List[ClientUpdateInfo],
        energy_weights: Dict[str, float],
    ) -> Dict[str, Any]:
        """Aggregation with gradient compression emphasis."""
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
    
    def _aggregate_selective(
        self,
        updates: List[ClientUpdateInfo],
        energy_weights: Dict[str, float],
    ) -> Dict[str, Any]:
        """Selective aggregation: only high-quality updates."""
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
                # Simulate update reception
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
        """Retrieve comprehensive metrics."""
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
        """Get participation statistics per client."""
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
        """Generate energy efficiency report."""
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
        """Get strategy change history."""
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


# ============================================================================
# Example Usage
# ============================================================================
async def example():
    expert = FLEnergyExpert()
    
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


if __name__ == "__main__":
    asyncio.run(example())
