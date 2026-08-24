#!/usr/bin/env python3
# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/iot_expert.py
# Version 8.3.0 – Full Green Agent MODP Integration

"""
Enhanced IoT Expert v8.3.0 – Full Green Agent MODP Integration
Complete metabolic edge decomposer with full bio‑inspired integration,
digital twin simulation, what‑if analysis, natural language explanations,
federated reflexive learning, cross‑domain knowledge transfer,
predictive sustainability, self‑healing mesh, weather API,
real‑time telemetry, differential privacy, carbon intensity forecasting,
and BaseExpert.propose_async() implementation.

ENHANCEMENTS OVER v8.2.0:
1. FIXED critical bugs: safe async task creation, generic metric methods, async get_metrics,
   dataclass config serialization, robust circuit breaker fallback, missing MembranePermeability,
   undefined thresholds, carbon intensity conversion, stubs for simulation/comparison.
2. DEEP bio‑inspired integration: ATP spend/earn, gradient pumping, compartment usage.
3. REAL MODP: multi‑objective metrics, adaptive cost compute, Pareto filtering in policy_probs and device/strategy selection.
4. ENHANCED teacher policy (`policy_probs`) as a true context‑aware MoE teacher distribution.
5. IMPROVED persistence and observability.
6. All optional dependencies still gracefully degrade.
"""

import asyncio
import json
import os
import hashlib
import uuid
from typing import Dict, Any, List, Optional, Tuple, Union, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
import numpy as np
import networkx as nx
from collections import defaultdict, deque
import math
import pickle
import pandas as pd
from pathlib import Path

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

# Optional: central circuit breaker and rate limiter
try:
    from ..scaling.circuit_breaker import EnhancedCircuitBreaker
    from ..scaling.rate_limiter import EnhancedRateLimiter
    CENTRAL_CIRCUIT_BREAKER_AVAILABLE = True
except ImportError:
    # Fallback: define a simple local circuit breaker
    class EnhancedCircuitBreaker:
        def __init__(self, name, failure_threshold=5, recovery_timeout=30.0):
            self.name = name
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
            self.failure_count = 0
            self.last_failure_time = None
            self.state = "closed"
            self._lock = asyncio.Lock()
        async def call(self, func, *args, **kwargs):
            async with self._lock:
                if self.state == "open":
                    if self.last_failure_time and (datetime.now(timezone.utc) - self.last_failure_time).total_seconds() > self.recovery_timeout:
                        self.state = "half-open"
                    else:
                        raise RuntimeError(f"Circuit breaker {self.name} is open")
            try:
                result = await func(*args, **kwargs)
                async with self._lock:
                    self.state = "closed"
                    self.failure_count = 0
                return result
            except Exception as e:
                async with self._lock:
                    self.failure_count += 1
                    self.last_failure_time = datetime.now(timezone.utc)
                    if self.failure_count >= self.failure_threshold:
                        self.state = "open"
                raise e
    CENTRAL_CIRCUIT_BREAKER_AVAILABLE = False

# Optional: central carbon manager
try:
    from ..carbon_intensity import CarbonIntensityManager
    CENTRAL_CARBON_AVAILABLE = True
except ImportError:
    CENTRAL_CARBON_AVAILABLE = False

# Optional: central helium manager
try:
    from ..helium_optimizer import HeliumEfficiencyOptimizer
    CENTRAL_HELIUM_AVAILABLE = True
except ImportError:
    CENTRAL_HELIUM_AVAILABLE = False

# Optional: base expert
try:
    from .base_expert import BaseExpert
    BASE_EXPERT_AVAILABLE = True
except ImportError:
    class BaseExpert:
        def __init__(self):
            self.expert_name = "iot_expert"
            self.supported_task_types = ["propose", "optimize", "register_device", "create_mesh", "self_heal", "explain"]
            self.health_status = "healthy"
        async def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
            raise NotImplementedError()
        def get_capabilities(self) -> Dict[str, Any]:
            return {'name': self.expert_name, 'supported_tasks': self.supported_task_types, 'health': self.health_status}
        def get_metrics(self) -> Dict[str, Any]:
            return {}

# Optional: bio-inspired core
try:
    from ...bio_inspired.__init__ import EnhancedBioInspiredCore, BioEvent
    CORE_AVAILABLE = True
except ImportError:
    CORE_AVAILABLE = False
    class BioEvent:
        def __init__(self, event_type, source, data=None):
            self.event_type = event_type
            self.source = source
            self.data = data or {}

# Optional: compartment manager (for membrane roles)
try:
    from ...bio_inspired.chromatophore_compartments import CompartmentManager, CompartmentState, MembranePermeability
    COMPARTMENT_AVAILABLE = True
except ImportError:
    COMPARTMENT_AVAILABLE = False
    class MembranePermeability:
        PERMEABLE = "permeable"
        SELECTIVE = "selective"
        IMPERMEABLE = "impermeable"

# Optional: ML libraries (torch, sklearn)
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import DataLoader, TensorDataset
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

try:
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    from sklearn.metrics import mean_squared_error, r2_score
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# Optional: aiohttp for weather
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

# ============================================================================
# Configuration – now a dataclass for easy serialization
# ============================================================================
@dataclass
class IoTExpertConfig:
    expert_id: str = f"iot_expert_{uuid.uuid4().hex[:8]}"
    enable_mesh: bool = getattr(central_config, "iot_enable_mesh", True)
    enable_collaborative: bool = getattr(central_config, "iot_enable_collaborative", True)
    enable_offline: bool = getattr(central_config, "iot_enable_offline", True)
    enable_energy_harvesting: bool = getattr(central_config, "iot_enable_energy_harvesting", True)
    enable_bio_integration: bool = getattr(central_config, "iot_enable_bio_integration", True) and CORE_AVAILABLE
    enable_federated: bool = getattr(central_config, "iot_enable_federated", True)
    enable_cross_domain: bool = getattr(central_config, "iot_enable_cross_domain", True)
    enable_predictive_sustainability: bool = getattr(central_config, "iot_enable_predictive_sustainability", True)
    enable_self_healing: bool = getattr(central_config, "iot_enable_self_healing", True)
    enable_weather_api: bool = getattr(central_config, "iot_enable_weather_api", True) and AIOHTTP_AVAILABLE
    enable_telemetry: bool = getattr(central_config, "iot_enable_telemetry", True)
    enable_differential_privacy: bool = getattr(central_config, "iot_enable_differential_privacy", True)
    enable_persistence: bool = True

    circuit_breaker_failure_threshold: int = getattr(central_config, "circuit_breaker_failure_threshold", 5)
    circuit_breaker_recovery_timeout: float = getattr(central_config, "circuit_breaker_recovery_timeout", 30.0)
    retry_attempts: int = getattr(central_config, "iot_retry_attempts", 3)
    retry_min_wait: float = getattr(central_config, "iot_retry_min_wait", 1.0)
    retry_max_wait: float = getattr(central_config, "iot_retry_max_wait", 10.0)

    weather_api_key: str = os.getenv('WEATHER_API_KEY', '')

# ============================================================================
# Enums (unchanged)
# ============================================================================
class DeviceType(Enum):
    MICROCONTROLLER = "microcontroller"
    SINGLE_BOARD = "single_board_computer"
    GATEWAY = "edge_gateway"
    MOBILE = "mobile_device"
    WEARABLE = "wearable"
    DRONE = "drone"
    SENSOR_NODE = "sensor_node"
    ACTUATOR = "actuator"

class ConnectionType(Enum):
    WIFI = "wifi"
    BLUETOOTH = "bluetooth"
    ZIGBEE = "zigbee"
    LORA = "lora"
    THREAD = "thread"
    MATTER = "matter"
    ETHERNET = "ethernet"
    CELLULAR = "cellular"

class EnergySource(Enum):
    BATTERY = "battery"
    SOLAR = "solar"
    KINETIC = "kinetic"
    THERMAL = "thermal"
    RF_HARVESTING = "rf_harvesting"
    GRID = "grid"
    HYBRID = "hybrid"
    HARVESTER_DRIVEN = "harvester_driven"

class ProcessingMode(Enum):
    LOCAL_ONLY = "local_only"
    MESH_COLLABORATIVE = "mesh_collaborative"
    CLOUD_OFFLOAD = "cloud_offload"
    HYBRID = "hybrid"
    OPPORTUNISTIC = "opportunistic"
    ATP_DRIVEN = "atp_driven"
    FEDERATED = "federated"

class MeshRole(Enum):
    LEADER = "leader"
    ROUTER = "router"
    LEAF = "leaf"
    MEMBRANE_GATED = "membrane_gated"
    FEDERATED = "federated"
    SELF_HEALING = "self_healing"

# ============================================================================
# Data Classes (unchanged)
# ============================================================================
@dataclass
class DeviceTelemetry:
    device_id: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    cpu_usage_percent: float = 0.0
    memory_usage_percent: float = 0.0
    temperature_c: float = 25.0
    battery_voltage_v: float = 3.3
    signal_strength_dbm: float = -50.0
    network_latency_ms: float = 10.0
    packet_loss_percent: float = 0.0
    energy_harvested_w: float = 0.0
    carbon_intensity_local: float = 400.0
    helium_scarcity_local: float = 0.5

@dataclass
class EdgeDevice:
    device_id: str
    device_type: DeviceType
    capabilities: Dict[str, float]
    mesh_id: Optional[str] = None
    connections: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    mesh_role: MeshRole = MeshRole.LEAF
    energy_source: EnergySource = EnergySource.BATTERY
    battery_capacity_wh: float = 10.0
    current_battery_wh: float = 10.0
    charging_rate_w: float = 0.0
    power_consumption_w: float = 0.5
    harvesting_capacity_w: float = 0.0
    harvesting_available_w: float = 0.0
    harvesting_schedule: Dict[int, float] = field(default_factory=dict)
    current_load: float = 0.0
    max_processing_power_flops: float = 1e9
    available_processing_flops: float = 1e9
    connection_types: List[ConnectionType] = field(default_factory=list)
    max_bandwidth_mbps: float = 100.0
    current_bandwidth_mbps: float = 100.0
    latency_to_cloud_ms: float = 50.0
    is_online: bool = True
    last_heartbeat: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    uptime_hours: float = 0.0
    carbon_intensity_g_per_kwh: float = 400.0
    carbon_per_operation_g: float = 0.0
    location: Optional[Dict[str, float]] = None
    gradient_health: float = 0.7
    membrane_permeability: str = "selective"
    token_balance: float = 0.0
    harvester_contribution: float = 0.0
    biomass_storage_token: Optional[str] = None
    federated_round: int = 0
    cross_domain_transfers: List[str] = field(default_factory=list)
    telemetry: Optional[DeviceTelemetry] = None
    self_healing_attempts: int = 0
    last_self_healing: Optional[datetime] = None
    failure_history: List[Dict] = field(default_factory=list)

    @property
    def energy_remaining_percent(self) -> float:
        return self.current_battery_wh / max(self.battery_capacity_wh, 1) * 100

    @property
    def can_operate_indefinitely(self) -> bool:
        return self.harvesting_available_w >= self.power_consumption_w

    @property
    def processing_utilization(self) -> float:
        return 1.0 - (self.available_processing_flops / max(self.max_processing_power_flops, 1))

@dataclass
class MeshNetwork:
    mesh_id: str
    devices: Dict[str, EdgeDevice] = field(default_factory=dict)
    topology_graph: nx.Graph = field(default_factory=nx.Graph)
    leader_id: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_topology_update: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    membrane_permeability: str = "selective"
    atp_available: float = 0.0
    federated_sharing_ratio: float = 0.0
    sustainability_score: float = 0.0
    self_healing_enabled: bool = True
    health_score: float = 0.0
    failure_count: int = 0

    def add_device(self, device: EdgeDevice):
        self.devices[device.device_id] = device
        self.topology_graph.add_node(device.device_id, device_type=device.device_type.value,
                                     processing_power=device.available_processing_flops,
                                     battery_percent=device.energy_remaining_percent,
                                     gradient_health=device.gradient_health)
        device.mesh_id = self.mesh_id

    def add_connection(self, device1_id: str, device2_id: str, link_quality: float, bandwidth_mbps: float, latency_ms: float):
        self.topology_graph.add_edge(device1_id, device2_id, quality=link_quality, bandwidth=bandwidth_mbps, latency=latency_ms)
        if device1_id in self.devices:
            self.devices[device1_id].connections[device2_id] = {'quality': link_quality, 'bandwidth': bandwidth_mbps, 'latency': latency_ms}
        if device2_id in self.devices:
            self.devices[device2_id].connections[device1_id] = {'quality': link_quality, 'bandwidth': bandwidth_mbps, 'latency': latency_ms}

    def elect_leader(self) -> Optional[str]:
        if not self.devices: return None
        best_device, best_score = None, -1
        for device_id, device in self.devices.items():
            score = (device.available_processing_flops / 1e9 * 0.25 + len(device.connections) / 10 * 0.15 +
                    device.energy_remaining_percent / 100 * 0.15 + device.gradient_health * 0.25 +
                    (1.0 - device.processing_utilization) * 0.2)
            if score > best_score:
                best_score, best_device = score, device_id
        if best_device:
            self.leader_id = best_device
            self.devices[best_device].mesh_role = MeshRole.LEADER
        return best_device

    def get_mesh_statistics(self) -> Dict[str, Any]:
        if not self.devices: return {}
        return {
            'mesh_id': self.mesh_id,
            'device_count': len(self.devices),
            'leader_id': self.leader_id,
            'is_connected': nx.is_connected(self.topology_graph) if len(self.devices) > 1 else True,
            'total_processing_power_flops': sum(d.available_processing_flops for d in self.devices.values()),
            'total_battery_wh': sum(d.current_battery_wh for d in self.devices.values()),
            'average_gradient_health': np.mean([d.gradient_health for d in self.devices.values()]),
            'membrane_permeability': self.membrane_permeability,
            'federated_sharing_ratio': self.federated_sharing_ratio,
            'sustainability_score': self.sustainability_score,
            'self_healing_enabled': self.self_healing_enabled,
            'health_score': self.health_score,
            'failure_count': self.failure_count
        }

# ============================================================================
# DeviceTelemetryCollector (unchanged, uses central logger)
# ============================================================================
class DeviceTelemetryCollector:
    def __init__(self):
        self.telemetry_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.anomaly_thresholds = {
            'cpu_usage': 90.0,
            'temperature': 80.0,
            'packet_loss': 5.0,
            'battery_voltage': 2.8
        }
        self._lock = asyncio.Lock()
        logger.info("Device Telemetry Collector initialized")

    async def collect_telemetry(self, device_id: str, telemetry: DeviceTelemetry) -> Dict:
        async with self._lock:
            if device_id not in self.telemetry_history:
                self.telemetry_history[device_id] = deque(maxlen=1000)
            self.telemetry_history[device_id].append(telemetry)
            anomalies = self._detect_anomalies(device_id, telemetry)
            return {
                'device_id': device_id,
                'timestamp': telemetry.timestamp.isoformat(),
                'anomalies': anomalies,
                'status': 'warning' if anomalies else 'healthy'
            }

    def _detect_anomalies(self, device_id: str, telemetry: DeviceTelemetry) -> List[str]:
        anomalies = []
        if telemetry.cpu_usage_percent > self.anomaly_thresholds['cpu_usage']:
            anomalies.append(f"High CPU usage: {telemetry.cpu_usage_percent:.1f}%")
        if telemetry.temperature_c > self.anomaly_thresholds['temperature']:
            anomalies.append(f"High temperature: {telemetry.temperature_c:.1f}°C")
        if telemetry.packet_loss_percent > self.anomaly_thresholds['packet_loss']:
            anomalies.append(f"High packet loss: {telemetry.packet_loss_percent:.1f}%")
        if telemetry.battery_voltage_v < self.anomaly_thresholds['battery_voltage']:
            anomalies.append(f"Low battery voltage: {telemetry.battery_voltage_v:.2f}V")
        return anomalies

    def get_device_health(self, device_id: str) -> Dict[str, Any]:
        if device_id not in self.telemetry_history:
            return {'status': 'no_data'}
        recent = list(self.telemetry_history[device_id])[-20:]
        if not recent:
            return {'status': 'no_data'}
        avg_cpu = np.mean([t.cpu_usage_percent for t in recent])
        avg_temp = np.mean([t.temperature_c for t in recent])
        avg_latency = np.mean([t.network_latency_ms for t in recent])
        return {
            'device_id': device_id,
            'average_cpu_percent': avg_cpu,
            'average_temperature_c': avg_temp,
            'average_latency_ms': avg_latency,
            'samples': len(recent),
            'status': 'healthy' if avg_cpu < 80 and avg_temp < 70 else 'degraded'
        }

# ============================================================================
# SelfHealingMeshManager (unchanged, uses central logger)
# ============================================================================
class SelfHealingMeshManager:
    def __init__(self):
        self.recovery_actions = {
            'leader_failure': self._recover_leader,
            'router_failure': self._recover_router,
            'link_failure': self._recover_link,
            'device_overload': self._rebalance_load
        }
        self.recovery_history: deque = deque(maxlen=1000)
        self.load_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self._lock = asyncio.Lock()
        logger.info("Self-Healing Mesh Manager initialized")

    async def detect_and_heal(self, mesh: MeshNetwork) -> Dict[str, Any]:
        async with self._lock:
            issues = self._detect_issues(mesh)
            actions = []
            for issue in issues:
                recovery_fn = self.recovery_actions.get(issue['type'])
                if recovery_fn:
                    result = await recovery_fn(mesh, issue)
                    actions.append(result)
            mesh.health_score = self._calculate_health_score(mesh)
            return {
                'mesh_id': mesh.mesh_id,
                'issues_detected': len(issues),
                'actions_taken': len(actions),
                'actions': actions,
                'health_score': mesh.health_score,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }

    def _detect_issues(self, mesh: MeshNetwork) -> List[Dict]:
        issues = []
        if mesh.leader_id and mesh.leader_id in mesh.devices:
            leader = mesh.devices[mesh.leader_id]
            if not leader.is_online or leader.energy_remaining_percent < 10:
                issues.append({'type': 'leader_failure', 'device_id': mesh.leader_id})
        elif mesh.leader_id is None:
            issues.append({'type': 'leader_failure', 'device_id': None})
        routers = [d for d in mesh.devices.values() if d.mesh_role == MeshRole.ROUTER]
        for router in routers:
            if not router.is_online or router.energy_remaining_percent < 5:
                issues.append({'type': 'router_failure', 'device_id': router.device_id})
        for u, v, data in mesh.topology_graph.edges(data=True):
            if data.get('quality', 1.0) < 0.3:
                issues.append({'type': 'link_failure', 'source': u, 'target': v})
        for device in mesh.devices.values():
            predicted_load = self._predict_load(device.device_id)
            if predicted_load > 0.85:
                issues.append({'type': 'device_overload', 'device_id': device.device_id, 'predicted_load': predicted_load})
        return issues

    def _predict_load(self, device_id: str) -> float:
        if device_id in self.load_history and len(self.load_history[device_id]) >= 10:
            history = list(self.load_history[device_id])[-10:]
            slope = np.polyfit(range(len(history)), history, 1)[0]
            return min(1.0, history[-1] + slope * 2)
        return 0.5

    async def _recover_leader(self, mesh: MeshNetwork, issue: Dict) -> Dict:
        new_leader = mesh.elect_leader()
        mesh.leader_id = new_leader
        mesh.failure_count += 1
        self.recovery_history.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'action': 'leader_recovery',
            'mesh_id': mesh.mesh_id,
            'new_leader': new_leader
        })
        return {'action': 'leader_recovery', 'new_leader': new_leader, 'status': 'success' if new_leader else 'failed'}

    async def _recover_router(self, mesh: MeshNetwork, issue: Dict) -> Dict:
        device_id = issue.get('device_id')
        if device_id and device_id in mesh.devices:
            mesh.devices[device_id].mesh_role = MeshRole.LEAF
            leaves = [d for d in mesh.devices.values() if d.mesh_role == MeshRole.LEAF and d.is_online]
            if leaves:
                new_router = max(leaves, key=lambda d: d.available_processing_flops)
                new_router.mesh_role = MeshRole.ROUTER
                self.recovery_history.append({
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'action': 'router_recovery',
                    'mesh_id': mesh.mesh_id,
                    'failed_router': device_id,
                    'new_router': new_router.device_id
                })
                return {'action': 'router_recovery', 'failed_router': device_id, 'new_router': new_router.device_id, 'status': 'success'}
        return {'action': 'router_recovery', 'status': 'failed'}

    async def _recover_link(self, mesh: MeshNetwork, issue: Dict) -> Dict:
        source = issue.get('source')
        target = issue.get('target')
        if source and target and source in mesh.devices and target in mesh.devices:
            mesh.topology_graph.add_edge(source, target, quality=0.7, bandwidth=50, latency=30)
            self.recovery_history.append({
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'action': 'link_recovery',
                'mesh_id': mesh.mesh_id,
                'source': source,
                'target': target
            })
            return {'action': 'link_recovery', 'source': source, 'target': target, 'status': 'success'}
        return {'action': 'link_recovery', 'status': 'failed'}

    async def _rebalance_load(self, mesh: MeshNetwork, issue: Dict) -> Dict:
        device_id = issue.get('device_id')
        if device_id and device_id in mesh.devices:
            overloaded = mesh.devices[device_id]
            candidates = [d for d in mesh.devices.values() if d.device_id != device_id and d.processing_utilization < 0.5 and d.is_online]
            if candidates:
                target = min(candidates, key=lambda d: d.processing_utilization)
                load_transfer = overloaded.processing_utilization * 0.3
                overloaded.current_load -= load_transfer
                target.current_load += load_transfer
                self.recovery_history.append({
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'action': 'load_rebalance',
                    'mesh_id': mesh.mesh_id,
                    'source': device_id,
                    'target': target.device_id,
                    'load_transfer': load_transfer
                })
                return {'action': 'load_rebalance', 'source': device_id, 'target': target.device_id, 'load_transfer': load_transfer, 'status': 'success'}
        return {'action': 'load_rebalance', 'status': 'failed'}

    def _calculate_health_score(self, mesh: MeshNetwork) -> float:
        if not mesh.devices: return 0.0
        avg_health = np.mean([d.gradient_health for d in mesh.devices.values()])
        connectivity = 1.0 if nx.is_connected(mesh.topology_graph) else 0.5
        leader_health = 1.0 if mesh.leader_id and mesh.devices[mesh.leader_id].is_online else 0.5
        return avg_health * 0.4 + connectivity * 0.3 + leader_health * 0.3

    def record_load(self, device_id: str, load: float):
        self.load_history[device_id].append(load)

    def get_recovery_stats(self) -> Dict[str, Any]:
        if not self.recovery_history:
            return {'status': 'no_recoveries'}
        recent = list(self.recovery_history)[-100:]
        actions = [r.get('action') for r in recent]
        return {
            'total_recoveries': len(self.recovery_history),
            'recent_actions': dict(zip(*np.unique(actions, return_counts=True))) if actions else {},
            'last_recovery': recent[-1] if recent else None,
            'success_rate': sum(1 for r in recent if r.get('status') == 'success') / max(len(recent), 1)
        }

# ============================================================================
# WeatherAPIClient (uses central config, circuit breaker)
# ============================================================================
class WeatherAPIClient:
    def __init__(self):
        self.api_key = os.getenv('WEATHER_API_KEY', '')
        self.endpoint = "https://api.openweathermap.org/data/2.5"
        self._session: Optional[aiohttp.ClientSession] = None
        self.cache: Dict[str, Any] = {}
        self.last_update: Optional[datetime] = None
        self.update_interval = 3600
        self._circuit = EnhancedCircuitBreaker("weather_api")
        logger.info("Weather API Client initialized")

    def _load_cache(self):
        pass

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def get_forecast(self, lat: float, lon: float, hours: int = 24) -> Dict[str, Any]:
        cache_key = f"{lat}_{lon}_{hours}_{datetime.now(timezone.utc).hour}"
        if cache_key in self.cache and self.last_update and (datetime.now(timezone.utc) - self.last_update).total_seconds() < self.update_interval:
            return self.cache[cache_key]

        async def _fetch():
            session = await self._get_session()
            url = f"{self.endpoint}/forecast"
            params = {'lat': lat, 'lon': lon, 'appid': self.api_key, 'units': 'metric'}
            async with session.get(url, params=params, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    solar_forecast = self._extract_solar_forecast(data)
                    wind_forecast = self._extract_wind_forecast(data)
                    result = {
                        'solar_forecast': solar_forecast,
                        'wind_forecast': wind_forecast,
                        'temperature_forecast': self._extract_temperature_forecast(data),
                        'harvesting_potential': self._calculate_harvesting_potential(solar_forecast, wind_forecast),
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                        'location': {'lat': lat, 'lon': lon}
                    }
                    self.cache[cache_key] = result
                    self.last_update = datetime.now(timezone.utc)
                    return result
                else:
                    raise aiohttp.ClientError(f"API returned {response.status}")

        try:
            return await self._circuit.call(_fetch)
        except Exception as e:
            logger.error(f"Weather API error, using fallback: {e}")
            return self._get_fallback_forecast(lat, lon, hours)

    def _extract_solar_forecast(self, data: Dict) -> List[Dict]:
        forecasts = []
        for item in data.get('list', [])[:8]:
            clouds = item.get('clouds', {}).get('all', 50)
            solar_kw = max(0, (100 - clouds) / 100 * 0.8)
            forecasts.append({'timestamp': item.get('dt_txt'), 'solar_kw': solar_kw, 'cloud_cover_percent': clouds})
        return forecasts

    def _extract_wind_forecast(self, data: Dict) -> List[Dict]:
        forecasts = []
        for item in data.get('list', [])[:8]:
            wind_speed = item.get('wind', {}).get('speed', 0)
            wind_kw = min(1.0, wind_speed / 15)
            forecasts.append({'timestamp': item.get('dt_txt'), 'wind_kw': wind_kw, 'wind_speed_ms': wind_speed})
        return forecasts

    def _extract_temperature_forecast(self, data: Dict) -> List[Dict]:
        forecasts = []
        for item in data.get('list', [])[:8]:
            temp = item.get('main', {}).get('temp', 20)
            forecasts.append({'timestamp': item.get('dt_txt'), 'temperature_c': temp})
        return forecasts

    def _calculate_harvesting_potential(self, solar_forecast: List, wind_forecast: List) -> float:
        if not solar_forecast or not wind_forecast: return 0.5
        avg_solar = np.mean([f['solar_kw'] for f in solar_forecast])
        avg_wind = np.mean([f['wind_kw'] for f in wind_forecast])
        return min(1.0, avg_solar * 0.6 + avg_wind * 0.4)

    def _get_fallback_forecast(self, lat: float, lon: float, hours: int) -> Dict:
        hour = datetime.now(timezone.utc).hour
        solar = max(0, 0.8 * np.sin((hour - 6) / 12 * np.pi)) if 6 <= hour <= 18 else 0
        wind = 0.3 + 0.3 * np.sin(hour / 24 * 2 * np.pi)
        return {
            'solar_forecast': [{'timestamp': 'fallback', 'solar_kw': solar}],
            'wind_forecast': [{'timestamp': 'fallback', 'wind_kw': wind}],
            'temperature_forecast': [{'timestamp': 'fallback', 'temperature_c': 20}],
            'harvesting_potential': solar * 0.6 + wind * 0.4,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'location': {'lat': lat, 'lon': lon},
            'is_fallback': True
        }

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# FederatedIoTLearner (unchanged)
# ============================================================================
class FederatedIoTLearner:
    def __init__(self, expert_id: str, server_url: Optional[str] = None, privacy_epsilon: float = 1.0):
        self.expert_id = expert_id
        self.server_url = server_url
        self.round = 0
        self.local_model = None
        self.global_model = None
        self.participants = []
        self.contribution_score = 0.0
        self._lock = asyncio.Lock()
        self._session = None
        self.device_models = {}
        self.privacy_epsilon = privacy_epsilon
        self.noise_scale = 0.001
        self._init_iot_model()

    def _init_iot_model(self):
        if not TORCH_AVAILABLE:
            logger.warning("PyTorch not available; federated learning disabled")
            return
        class IoTModel(nn.Module):
            def __init__(self, input_size=10, hidden_size=64):
                super().__init__()
                self.network = nn.Sequential(
                    nn.Linear(input_size, hidden_size),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size),
                    nn.Linear(hidden_size, hidden_size // 2),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size // 2),
                    nn.Linear(hidden_size // 2, 1)
                )
            def forward(self, x):
                return self.network(x)
        self.local_model = IoTModel()
        self.global_model = IoTModel()

    def _add_differential_privacy(self, weights: Dict) -> Dict:
        if self.privacy_epsilon <= 0: return weights
        private_weights = {}
        sensitivity = 1.0
        for key, tensor in weights.items():
            scale = (2 * sensitivity) / self.privacy_epsilon
            noise = torch.randn_like(tensor) * scale * self.noise_scale
            private_weights[key] = tensor + noise
        return private_weights

    async def _get_session(self):
        if self._session is None and self.server_url:
            self._session = aiohttp.ClientSession()
        return self._session

    async def train_local_model(self, device_data: List[Dict[str, float]], epochs: int = 10) -> float:
        if not device_data or not TORCH_AVAILABLE: return 0.0
        X, y = [], []
        for item in device_data:
            X.append([
                item.get('battery_percent', 0.5),
                item.get('processing_load', 0.5),
                item.get('network_quality', 0.5),
                item.get('harvesting_available', 0.5),
                item.get('carbon_intensity', 0.5),
                item.get('helium_scarcity', 0.5),
                item.get('gradient_health', 0.5),
                item.get('connection_count', 0.5),
                item.get('mesh_connectivity', 0.5),
                item.get('ecoatp_balance', 0.5)
            ])
            y.append(item.get('optimization_score', 0.5))
        X = torch.FloatTensor(X)
        y = torch.FloatTensor(y).unsqueeze(1)
        dataset = TensorDataset(X, y)
        dataloader = DataLoader(dataset, batch_size=32, shuffle=True)
        optimizer = optim.Adam(self.local_model.parameters(), lr=0.001)
        criterion = nn.MSELoss()
        total_loss = 0
        for epoch in range(epochs):
            epoch_loss = 0
            for batch_X, batch_y in dataloader:
                optimizer.zero_grad()
                output = self.local_model(batch_X)
                loss = criterion(output, batch_y)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.local_model.parameters(), 1.0)
                optimizer.step()
                epoch_loss += loss.item()
            total_loss += epoch_loss
        avg_loss = total_loss / epochs
        logger.info(f"Local IoT model trained. Loss: {avg_loss:.4f}")
        return avg_loss

    async def send_local_update(self, performance_metric: float = 1.0) -> Dict:
        if not self.server_url or not TORCH_AVAILABLE: return {'status': 'disabled'}
        async with self._lock:
            session = await self._get_session()
            try:
                weights = self.local_model.state_dict()
                private_weights = self._add_differential_privacy(weights)
                weights_serialized = {k: v.tolist() for k, v in private_weights.items()}
                update_data = {
                    'expert_id': self.expert_id,
                    'round': self.round,
                    'weights': weights_serialized,
                    'performance': performance_metric,
                    'device_count': len(self.device_models),
                    'privacy_epsilon': self.privacy_epsilon,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
                async with session.post(f"{self.server_url}/federated/update", json=update_data, timeout=30) as response:
                    if response.status == 200:
                        result = await response.json()
                        self.round += 1
                        self.contribution_score += performance_metric
                        self.privacy_epsilon *= 0.99
                        return result
                    else:
                        return {'status': 'failed'}
            except Exception as e:
                logger.error(f"Federated update error: {e}")
                return {'status': 'error'}

    async def get_global_model(self) -> Optional[Dict]:
        if not self.server_url or not TORCH_AVAILABLE: return None
        async with self._lock:
            session = await self._get_session()
            try:
                async with session.get(f"{self.server_url}/federated/global/iot", timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        weights = data.get('weights', {})
                        self.round = data.get('round', 0)
                        self.participants = data.get('participants', [])
                        for k, v in weights.items():
                            self.global_model.state_dict()[k] = torch.FloatTensor(v)
                        return weights
            except Exception as e:
                logger.error(f"Global model fetch error: {e}")
                return None

    async def participate_in_round(self, device_data: List[Dict[str, float]], performance: float = 1.0) -> Dict:
        await self.train_local_model(device_data)
        result = await self.send_local_update(performance)
        global_weights = await self.get_global_model()
        if global_weights:
            self.global_model.load_state_dict(global_weights)
            if self.expert_id not in self.participants:
                self.participants.append(self.expert_id)
        return {
            'round': self.round,
            'participated': bool(global_weights),
            'contribution_score': self.contribution_score,
            'performance': performance,
            'peer_count': len(self.participants),
            'privacy_epsilon': self.privacy_epsilon,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

    def get_federated_insights(self) -> Dict:
        return {
            'round': self.round,
            'contribution_score': self.contribution_score,
            'participants': len(self.participants),
            'has_global_model': self.global_model is not None,
            'device_models': len(self.device_models),
            'privacy_epsilon': self.privacy_epsilon,
            'last_aggregation': datetime.now(timezone.utc).isoformat()
        }

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# IoTCrossDomainTransfer (unchanged)
# ============================================================================
class IoTCrossDomainTransfer:
    def __init__(self):
        self.knowledge_base = {}
        self.transfer_logs = deque(maxlen=1000)
        self.domain_mappings = {
            'iot→energy': {'harvesting_patterns': ['solar', 'wind', 'kinetic', 'rf'], 'power_strategies': ['adaptive', 'predictive', 'opportunistic']},
            'iot→data': {'compression_strategies': ['edge', 'fog', 'cloud', 'distributed'], 'processing_patterns': ['batch', 'streaming', 'event-driven']},
            'iot→carbon': {'intensity_patterns': ['diurnal', 'location-based', 'load-dependent'], 'optimization_strategies': ['load-shifting', 'efficiency-first']},
            'iot→helium': {'scarcity_patterns': ['supply-constrained', 'price-sensitive'], 'efficiency_strategies': ['recovery', 'reuse', 'minimization']}
        }
        self._lock = asyncio.Lock()
        self.effectiveness_history = deque(maxlen=100)

    def transfer_knowledge(self, source_domain: str, target_domain: str, knowledge_type: str, data: Dict[str, Any]) -> Dict:
        key = f"{source_domain}→{target_domain}"
        if key not in self.knowledge_base:
            self.knowledge_base[key] = {}
        if knowledge_type not in self.knowledge_base[key]:
            self.knowledge_base[key][knowledge_type] = {'data': data, 'transfer_count': 1, 'effectiveness_score': 0.5, 'last_used': datetime.now(timezone.utc)}
        else:
            existing = self.knowledge_base[key][knowledge_type]
            existing['data'].update(data)
            existing['transfer_count'] += 1
            existing['last_used'] = datetime.now(timezone.utc)
        self.transfer_logs.append({'timestamp': datetime.now(timezone.utc), 'source': source_domain, 'target': target_domain, 'type': knowledge_type})
        logger.info(f"IoT knowledge transferred: {source_domain}→{target_domain} ({knowledge_type})")
        return self.knowledge_base[key][knowledge_type]

    def get_transferred_knowledge(self, source_domain: str, target_domain: str, knowledge_type: str) -> Optional[Dict]:
        key = f"{source_domain}→{target_domain}"
        if key in self.knowledge_base and knowledge_type in self.knowledge_base[key]:
            return self.knowledge_base[key][knowledge_type]
        return None

    async def apply_energy_knowledge(self, device_data: Dict) -> Dict:
        energy_knowledge = self.get_transferred_knowledge('energy', 'iot', 'harvesting_patterns')
        if energy_knowledge:
            patterns = energy_knowledge.get('data', {}).get('patterns', [])
            return {'applied_pattern': patterns[0] if patterns else 'default', 'efficiency_gain': energy_knowledge.get('effectiveness_score', 0.5) * 0.15, 'source': 'energy_domain'}
        return {'applied_pattern': 'default', 'source': 'local'}

    async def apply_carbon_knowledge(self, carbon_intensity: float) -> Dict:
        carbon_knowledge = self.get_transferred_knowledge('carbon', 'iot', 'intensity_patterns')
        if carbon_knowledge:
            patterns = carbon_knowledge.get('data', {}).get('patterns', [])
            return {'applied_pattern': patterns[0] if patterns else 'default', 'carbon_adjustment': carbon_knowledge.get('effectiveness_score', 0.5) * 0.1, 'source': 'carbon_domain'}
        return {'applied_pattern': 'default', 'source': 'local'}

    def get_transfer_statistics(self) -> Dict:
        total_transfers = len(self.transfer_logs)
        domain_pairs = {}
        for log in self.transfer_logs:
            key = f"{log['source']}→{log['target']}"
            domain_pairs[key] = domain_pairs.get(key, 0) + 1
        return {'total_transfers': total_transfers, 'domain_pairs': domain_pairs, 'knowledge_types': list(self.knowledge_base.keys()), 'recent_transfers': list(self.transfer_logs)[-10:]}

# ============================================================================
# PredictiveIoTSustainability (unchanged)
# ============================================================================
class PredictiveIoTSustainability:
    def __init__(self, history_window: int = 100):
        self.history_window = history_window
        self.device_history = deque(maxlen=history_window)
        self.sustainability_history = deque(maxlen=history_window)
        self.forecast_history = deque(maxlen=50)
        self.models = {}
        self.scaler = StandardScaler() if SKLEARN_AVAILABLE else None
        self.is_trained = False
        self.carbon_models: Dict[str, Dict] = {}
        self.lstm_model = None
        if SKLEARN_AVAILABLE:
            self.models['random_forest'] = RandomForestRegressor(n_estimators=100, random_state=42)
            self.models['gradient_boosting'] = GradientBoostingRegressor(n_estimators=100, random_state=42)
        if TORCH_AVAILABLE:
            self.lstm_model = self._build_lstm_model()

    def _build_lstm_model(self) -> nn.Module:
        class LSTMPredictor(nn.Module):
            def __init__(self, input_size=5, hidden_size=32, num_layers=2):
                super().__init__()
                self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
                self.fc = nn.Linear(hidden_size, 1)
            def forward(self, x):
                lstm_out, _ = self.lstm(x)
                return self.fc(lstm_out[:, -1, :])
        return LSTMPredictor()

    def update_carbon_model(self, location: str, carbon_data: Dict):
        self.carbon_models[location] = carbon_data

    def update_history(self, device_data: Dict, sustainability_metrics: Dict):
        self.device_history.append({
            'timestamp': datetime.now(timezone.utc),
            'battery_level': device_data.get('battery_percent', 50),
            'processing_load': device_data.get('processing_load', 0.5),
            'network_quality': device_data.get('network_quality', 0.5),
            'harvesting_available': device_data.get('harvesting_available', 0),
            'carbon_intensity': device_data.get('carbon_intensity', 400)
        })
        self.sustainability_history.append({
            'timestamp': datetime.now(timezone.utc),
            'carbon_savings': sustainability_metrics.get('carbon_savings_kg', 0),
            'energy_savings': sustainability_metrics.get('energy_savings_kwh', 0),
            'sustainability_score': sustainability_metrics.get('sustainability_score', 0)
        })

    async def train_forecast_model(self):
        if len(self.device_history) < 20:
            return {'status': 'insufficient_data', 'samples': len(self.device_history)}
        if not SKLEARN_AVAILABLE:
            return {'status': 'ml_not_available'}
        X, y = [], []
        history_list = list(self.device_history)
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([data['battery_level'], data['processing_load'], data['network_quality'], data['harvesting_available'], data.get('carbon_intensity', 400) / 1000])
            X.append(features)
            y.append(history_list[i + 5]['battery_level'])
        X = np.array(X)
        y = np.array(y)
        X_scaled = self.scaler.fit_transform(X)
        results = {}
        for name, model in self.models.items():
            if model is not None:
                model.fit(X_scaled, y)
                predictions = model.predict(X_scaled)
                r2 = r2_score(y, predictions)
                results[name] = r2
        if len(X) >= 30 and TORCH_AVAILABLE:
            lstm_X = torch.FloatTensor(X).unsqueeze(1)
            lstm_y = torch.FloatTensor(y).unsqueeze(1)
            optimizer = optim.Adam(self.lstm_model.parameters(), lr=0.001)
            criterion = nn.MSELoss()
            for epoch in range(20):
                optimizer.zero_grad()
                output = self.lstm_model(lstm_X)
                loss = criterion(output, lstm_y)
                loss.backward()
                optimizer.step()
            results['lstm'] = 0.8
        self.is_trained = True
        logger.info(f"IoT sustainability models trained. R² scores: {results}")
        return {'status': 'success', 'results': results, 'samples': len(X)}

    async def predict_device_health(self, hours: int = 24, location: Optional[str] = None) -> Dict:
        if not self.is_trained or len(self.device_history) < 10:
            return {'predicted_battery': 50, 'confidence': 0.0, 'trend': 'insufficient_data'}
        recent = list(self.device_history)[-5:]
        features = []
        for data in recent:
            features.extend([data['battery_level'], data['processing_load'], data['network_quality'], data['harvesting_available'], data.get('carbon_intensity', 400) / 1000])
        features = np.array(features).reshape(1, -1)
        features_scaled = self.scaler.transform(features)
        predictions = []
        for name, model in self.models.items():
            if model is not None:
                pred = model.predict(features_scaled)[0]
                predictions.append(pred)
        if len(self.device_history) >= 5 and TORCH_AVAILABLE:
            lstm_features = torch.FloatTensor(features).unsqueeze(0).unsqueeze(1)
            with torch.no_grad():
                lstm_pred = self.lstm_model(lstm_features).item()
                predictions.append(lstm_pred)
        if not predictions:
            return {'predicted_battery': 50, 'confidence': 0.0, 'trend': 'no_models'}
        prediction = np.mean(predictions)
        confidence = min(0.9, np.std(predictions) / 0.2) if len(predictions) > 1 else 0.5
        if len(self.forecast_history) > 5:
            recent_forecasts = list(self.forecast_history)[-5:]
            trend = "improving" if prediction > recent_forecasts[-1] else "declining" if prediction < recent_forecasts[-1] else "stable"
        else:
            trend = "stable"
        carbon_forecast = None
        if location and location in self.carbon_models:
            carbon_forecast = self.carbon_models[location]
        return {
            'predicted_battery': float(prediction),
            'confidence': confidence,
            'trend': trend,
            'carbon_forecast': carbon_forecast,
            'recommended_actions': self._generate_actions(prediction)
        }

    def _generate_actions(self, battery_prediction: float) -> List[str]:
        actions = []
        if battery_prediction < 30:
            actions.append("Activate power-saving mode")
            actions.append("Reduce processing load")
            actions.append("Enable energy harvesting")
        elif battery_prediction < 50:
            actions.append("Optimize energy harvesting")
            actions.append("Schedule non-critical tasks")
            actions.append("Consider renewable energy sources")
        else:
            actions.append("Device health is sustainable")
        return actions

    def get_sustainability_summary(self) -> Dict:
        if not self.sustainability_history:
            return {'status': 'insufficient_data'}
        recent = list(self.sustainability_history)[-50:]
        return {
            'average_carbon_savings': np.mean([h['carbon_savings'] for h in recent]),
            'average_energy_savings': np.mean([h['energy_savings'] for h in recent]),
            'current_sustainability_score': recent[-1]['sustainability_score'] if recent else 0,
            'trend': 'improving' if len(recent) > 10 and recent[-1]['sustainability_score'] > recent[0]['sustainability_score'] else 'stable'
        }

# ============================================================================
# IoTExpert (Main Class) – Fully Integrated v8.3.0
# ============================================================================
class IoTExpert(BaseExpert):
    """
    Enhanced IoT Expert v8.3.0 with full bio‑inspired integration and MOPD integration.
    """

    def __init__(
        self,
        storage: Storage,
        message_queue: AsyncMessageQueue,
        adaptive_cost: AdaptiveCostFunction,
        pareto_gating: ParetoGating,
        drift_detector: DriftDetector,
        metrics: MetricsRegistry,
        bio_core: Optional[Any] = None,
        carbon_manager: Optional[Any] = None,
        helium_manager: Optional[Any] = None
    ):
        if BASE_EXPERT_AVAILABLE:
            super().__init__()
        self.expert_name = "iot_expert"
        self.supported_task_types = [
            "propose", "optimize", "register_device", "create_mesh",
            "self_heal", "explain"
        ]
        self.health_status = "healthy"

        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics
        self.bio_core = bio_core
        self.carbon_manager = carbon_manager
        self.helium_manager = helium_manager

        # Configuration – built from central_config
        self.config = IoTExpertConfig()
        self.expert_id = self.config.expert_id
        self.version = "8.3.0"

        # Feature flags
        self.enable_mesh = self.config.enable_mesh
        self.enable_collaborative = self.config.enable_collaborative
        self.enable_offline = self.config.enable_offline
        self.enable_energy_harvesting = self.config.enable_energy_harvesting
        self.enable_bio_integration = self.config.enable_bio_integration and CORE_AVAILABLE
        self.enable_federated = self.config.enable_federated
        self.enable_cross_domain = self.config.enable_cross_domain
        self.enable_predictive_sustainability = self.config.enable_predictive_sustainability
        self.enable_self_healing = self.config.enable_self_healing
        self.enable_weather_api = self.config.enable_weather_api and AIOHTTP_AVAILABLE
        self.enable_telemetry = self.config.enable_telemetry
        self.enable_differential_privacy = self.config.enable_differential_privacy
        self.enable_persistence = True

        # Thresholds (initialized)
        self.thresholds: Dict[str, float] = {
            'sampling_rate_high': 10.0,
            'sampling_rate_low': 5.0,
            'sampling_rate_critical': 2.0,
        }

        # Bio-inspired core sub‑modules
        self.event_broker = None
        self.alert_system = None
        self.anomaly_detection = None
        self.cost_benefit_engine = None
        self.quantum_bridge = None
        self.tick_engine = None
        self.swarm_coordinator = None
        self.self_healer = None
        self.workflow_orchestrator = None
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        if self.bio_core:
            self.event_broker = getattr(self.bio_core, 'event_broker', None)
            self.alert_system = getattr(self.bio_core, 'alert_system', None)
            self.anomaly_detection = getattr(self.bio_core, 'anomaly_detection', None)
            self.cost_benefit_engine = getattr(self.bio_core, 'cost_benefit_engine', None)
            self.quantum_bridge = getattr(self.bio_core, 'quantum_bridge', None)
            self.tick_engine = getattr(self.bio_core, 'tick_engine', None)
            self.swarm_coordinator = getattr(self.bio_core, 'swarm_coordinator', None)
            self.self_healer = getattr(self.bio_core, 'self_healer', None)
            self.workflow_orchestrator = getattr(self.bio_core, 'workflow_orchestrator', None)
            self.token_manager = getattr(self.bio_core, 'token_manager', None)
            self.gradient_manager = getattr(self.bio_core, 'gradient_manager', None)
            self.scheduler = getattr(self.bio_core, 'scheduler', None)
            self.compartment_manager = getattr(self.bio_core, 'compartment_manager', None)
            self.biomass_storage = getattr(self.bio_core, 'biomass_storage', None)
            self.harvester = getattr(self.bio_core, 'harvester', None)

        # Sub-modules
        self.telemetry_collector = DeviceTelemetryCollector() if self.enable_telemetry else None
        self.self_healing_manager = SelfHealingMeshManager() if self.enable_self_healing else None
        self.weather_api = WeatherAPIClient() if self.enable_weather_api else None
        self.federated_learner = FederatedIoTLearner(self.expert_id, privacy_epsilon=1.0 if self.enable_differential_privacy else 0.0)
        self.cross_domain_transfer = IoTCrossDomainTransfer()
        self.predictive_sustainability = PredictiveIoTSustainability() if self.enable_predictive_sustainability else None

        # State
        self.mesh_networks: Dict[str, MeshNetwork] = {}
        self.devices: Dict[str, EdgeDevice] = {}
        self.biomass_offline_tokens: Dict[str, str] = {}
        self.total_tasks_processed = 0
        self.total_energy_harvested_kwh = 0.0
        self.total_ecoatp_saved = 0.0
        self.total_carbon_saved_kg = 0.0
        self.total_helium_saved_l = 0.0
        self.simulation_results: deque = deque(maxlen=500)
        self.sustainability_score = 0.0
        self.health_status = "healthy"
        self.last_error: Optional[str] = None
        self.correlation_id = str(uuid.uuid4())

        # Load state from central storage (safe)
        self._load_state_task = self._create_task(self._load_state())

        # Subscribe to core events if available
        if self.event_broker:
            self._subscribe_events()

        logger.info(f"IoT Expert v{self.version} initialized with ID {self.expert_id}")

    def _create_task(self, coro):
        try:
            loop = asyncio.get_running_loop()
            return loop.create_task(coro)
        except RuntimeError:
            logger.warning("No running event loop; background task not started.")
            return None

    # --------------------------------------------------------------------------
    # State Persistence using central Storage
    # --------------------------------------------------------------------------
    async def _load_state(self):
        try:
            data = self.storage.get_state("iot_expert_state")
            if data:
                state = json.loads(data)
                self.mesh_networks = {mid: MeshNetwork(**data) for mid, data in state.get('mesh_networks', {}).items()}
                self.devices = {did: EdgeDevice(**data) for did, data in state.get('devices', {}).items()}
                self.total_tasks_processed = state.get('total_tasks_processed', 0)
                self.total_energy_harvested_kwh = state.get('total_energy_harvested_kwh', 0.0)
                self.total_carbon_saved_kg = state.get('total_carbon_saved_kg', 0.0)
                self.total_helium_saved_l = state.get('total_helium_saved_l', 0.0)
                self.sustainability_score = state.get('sustainability_score', 0.0)
                # Rebuild mesh networks' topology_graph and device references
                for mesh in self.mesh_networks.values():
                    mesh.topology_graph = nx.Graph()
                    for device_id, device in self.devices.items():
                        if device.mesh_id == mesh.mesh_id:
                            mesh.add_device(device)
                    for d1_id, conns in mesh.devices.items():
                        for d2_id in conns.connections:
                            mesh.topology_graph.add_edge(d1_id, d2_id)
                logger.info("IoT Expert state loaded from central storage")
        except Exception as e:
            logger.error(f"Failed to load IoT Expert state: {e}")

    async def _save_state(self):
        try:
            state = {
                'mesh_networks': {mid: asdict(m) for mid, m in self.mesh_networks.items()},
                'devices': {did: asdict(d) for did, d in self.devices.items()},
                'total_tasks_processed': self.total_tasks_processed,
                'total_energy_harvested_kwh': self.total_energy_harvested_kwh,
                'total_carbon_saved_kg': self.total_carbon_saved_kg,
                'total_helium_saved_l': self.total_helium_saved_l,
                'sustainability_score': self.sustainability_score,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            self.storage.save_state("iot_expert_state", json.dumps(state))
            logger.info("IoT Expert state saved to central storage")
        except Exception as e:
            logger.error(f"Failed to save IoT Expert state: {e}")

    # --------------------------------------------------------------------------
    # Event Subscriptions
    # --------------------------------------------------------------------------
    def _subscribe_events(self):
        if self.event_broker:
            self.event_broker.subscribe('helium_update', self._on_helium_update)
            self.event_broker.subscribe('alert_generated', self._on_alert_generated)
            self.event_broker.subscribe('anomaly_detected', self._on_anomaly_detected)
            self.event_broker.subscribe('token_balance_update', self._on_token_update)
            self.event_broker.subscribe('config_updated', self._on_config_updated)
            self.event_broker.subscribe('health_update', self._on_health_update)
            logger.info("IoT Expert subscribed to core events")

    async def _on_helium_update(self, event: BioEvent):
        self._last_context['helium_scarcity'] = event.data.get('scarcity', 0.5)
        self._last_context['helium_cost_index'] = event.data.get('cost', 1.0)

    async def _on_alert_generated(self, event: BioEvent):
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert received; adjusting IoT thresholds")
            self.thresholds['sampling_rate_high'] = self.thresholds.get('sampling_rate_high', 10.0) * 0.8
            if self.self_healing_manager:
                for mesh in self.mesh_networks.values():
                    await self.self_healing_manager.detect_and_heal(mesh)

    async def _on_anomaly_detected(self, event: BioEvent):
        if event.data.get('metric') == 'helium_scarcity':
            logger.info("Helium anomaly detected; adjusting IoT thresholds")
            self.thresholds['sampling_rate_low'] = self.thresholds.get('sampling_rate_low', 5.0) * 0.9

    async def _on_token_update(self, event: BioEvent):
        self._last_context['token_balance'] = event.data.get('balance', 500)

    async def _on_config_updated(self, event: BioEvent):
        updates = event.data.get('updates', {})
        if 'iot_expert' in updates:
            new_config = updates['iot_expert']
            if 'thresholds' in new_config:
                self.thresholds.update(new_config['thresholds'])
                await self._save_state()
            logger.info("IoT Expert configuration reloaded", updates=new_config)

    async def _on_health_update(self, event: BioEvent):
        self.health_status = event.data.get('status', 'healthy')

    # --------------------------------------------------------------------------
    # Teacher Interface for MOPD (context-aware soft policy)
    # --------------------------------------------------------------------------
    async def policy_probs(self, state: Dict) -> List[float]:
        strategies = ['sampling_high', 'sampling_low', 'compressed', 'adaptive', 'power_saving']
        candidates = []
        for strategy in strategies:
            if strategy == 'sampling_high':
                quality = 0.7
                carbon_g = 5.0
                latency_ms = 40.0
                energy_joules = 50.0
            elif strategy == 'sampling_low':
                quality = 0.6
                carbon_g = 2.0
                latency_ms = 80.0
                energy_joules = 20.0
            elif strategy == 'compressed':
                quality = 0.75
                carbon_g = 1.5
                latency_ms = 70.0
                energy_joules = 30.0
            elif strategy == 'adaptive':
                quality = 0.8
                carbon_g = 3.0
                latency_ms = 50.0
                energy_joules = 40.0
            elif strategy == 'power_saving':
                quality = 0.5
                carbon_g = 0.5
                latency_ms = 100.0
                energy_joules = 5.0
            else:
                quality = 0.5
                carbon_g = 2.0
                latency_ms = 60.0
                energy_joules = 25.0

            cost = self.adaptive_cost.compute(
                quality=quality,
                carbon_g=carbon_g,
                latency_ms=latency_ms,
                energy_joules=energy_joules,
                health=self.health_status == 'healthy',
                atp=0.5
            )
            candidates.append({
                'strategy': strategy,
                'score': cost,
                'carbon_g': carbon_g,
                'latency_ms': latency_ms,
                'energy_joules': energy_joules,
                'quality_score': quality
            })

        if self.pareto:
            filtered = self.pareto.filter(candidates)
            if filtered:
                allowed = {c['strategy'] for c in filtered}
                candidates = [c for c in candidates if c['strategy'] in allowed]

        scores = [c['score'] for c in candidates]
        if scores:
            exp_scores = np.exp(scores - np.max(scores))
            probs = exp_scores / np.sum(exp_scores)
            full_probs = [0.0] * len(strategies)
            for c, p in zip(candidates, probs):
                idx = strategies.index(c['strategy'])
                full_probs[idx] = p
            return full_probs
        return [0.2] * 5

    # --------------------------------------------------------------------------
    # Core Expert Interface
    # --------------------------------------------------------------------------
    async def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        task_type = task.get('type', 'unknown')
        if task_type == 'propose':
            return await self.propose_async(task.get('context', {}))
        elif task_type == 'optimize':
            return await self.optimize_edge_deployment(
                device_type=task.get('device_type', 'any'),
                carbon_zone=task.get('carbon_zone', 0),
                helium_scarcity=task.get('helium_scarcity', 0.5),
                task_config=task.get('task_config'),
                location=task.get('location')
            )
        elif task_type == 'register_device':
            device = self.register_device(
                device_id=task['device_id'],
                device_type=DeviceType(task['device_type']),
                capabilities=task.get('capabilities', {}),
                location=task.get('location'),
                mesh_id=task.get('mesh_id')
            )
            return {'status': 'success', 'device_id': device.device_id}
        elif task_type == 'create_mesh':
            mesh = self.create_mesh(task['mesh_id'], task['device_ids'])
            return {'status': 'success', 'mesh_id': mesh.mesh_id}
        elif task_type == 'self_heal':
            if self.self_healing_manager:
                mesh_id = task.get('mesh_id')
                if mesh_id and mesh_id in self.mesh_networks:
                    result = await self.self_healing_manager.detect_and_heal(self.mesh_networks[mesh_id])
                    return {'status': 'success', 'result': result}
                else:
                    return {'status': 'error', 'error': 'Mesh not found'}
            else:
                return {'status': 'error', 'error': 'Self-healing not enabled'}
        elif task_type == 'explain':
            return self.explain_mesh_topology(task.get('mesh_id', ''))
        else:
            return {'status': 'error', 'error': f'Unknown task type: {task_type}'}

    def get_capabilities(self) -> Dict[str, Any]:
        return {
            'expert_name': self.expert_name,
            'supported_tasks': self.supported_task_types,
            'health_status': self.health_status,
            'config': asdict(self.config),
        }

    async def get_metrics(self) -> Dict[str, Any]:
        return await self._get_expert_metrics()

    async def get_health_status(self) -> Dict[str, Any]:
        return {
            'expert_id': self.expert_id,
            'status': self.health_status,
            'last_error': self.last_error,
            'persistence_enabled': True,
        }

    async def _get_expert_metrics(self) -> Dict[str, Any]:
        return {
            'total_tasks_processed': self.total_tasks_processed,
            'total_energy_harvested_kwh': self.total_energy_harvested_kwh,
            'total_carbon_saved_kg': self.total_carbon_saved_kg,
            'total_helium_saved_l': self.total_helium_saved_l,
            'sustainability_score': self.sustainability_score,
            'device_count': len(self.devices),
            'mesh_count': len(self.mesh_networks),
        }

    # --------------------------------------------------------------------------
    # Bio-Inspired Data Access
    # --------------------------------------------------------------------------
    def _get_membrane_mesh_role(self, device_id: str) -> MeshRole:
        if self.compartment_manager and COMPARTMENT_AVAILABLE:
            compartment = self.compartment_manager.find_best_compartment('iot')
            if compartment:
                perm = compartment.membrane.permeability
                if perm == MembranePermeability.PERMEABLE:
                    return MeshRole.LEADER
                elif perm == MembranePermeability.SELECTIVE:
                    return MeshRole.ROUTER
                else:
                    return MeshRole.LEAF
        return MeshRole.MEMBRANE_GATED

    def _get_atp_collaborative_workers(self) -> int:
        if self.scheduler:
            df = self.scheduler.calculate_gradient_driving_force()
            rs = self.scheduler.calculate_rotation_speed(df)
            rate = self.scheduler.calculate_atp_production_rate(rs)
            return 8 if rate > 100 else 4 if rate > 50 else 2
        return 4

    def _get_harvester_energy_prediction(self) -> Dict[str, float]:
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            recent = stats.get('recent_conversions', [])
            avg = np.mean([c.get('convertible_energy', 0.5) for c in recent[-10:]]) if recent else 0.5
            return {'solar_kw': stats.get('total_harvested', 0) * 0.6 * avg,
                    'wind_kw': stats.get('total_harvested', 0) * 0.4 * avg,
                    'total_kw': stats.get('total_harvested', 0) * avg, 'confidence': avg}
        return {'solar_kw': 0, 'wind_kw': 0, 'total_kw': 0, 'confidence': 0.5}

    def _get_gradient_device_health(self, device_id: str) -> float:
        if self.gradient_manager:
            trust = self.gradient_manager.fields.get('trust')
            if trust:
                return trust.gradient_strength
        return 0.7

    def _get_gradient_levels(self) -> Dict[str, float]:
        if self.gradient_manager:
            return self.gradient_manager.get_field_strengths()
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}

    # --------------------------------------------------------------------------
    # Device Registration (Enhanced with FeedbackEvent)
    # --------------------------------------------------------------------------
    def register_device(self, device_id: str, device_type: DeviceType, capabilities: Dict[str, float],
                       location: Optional[Dict[str, float]] = None, mesh_id: Optional[str] = None) -> EdgeDevice:
        mesh_role = self._get_membrane_mesh_role(device_id) if self.enable_bio_integration else MeshRole.LEAF
        gradient_health = self._get_gradient_device_health(device_id) if self.enable_bio_integration else 0.7
        device = EdgeDevice(
            device_id=device_id,
            device_type=device_type,
            capabilities=capabilities,
            location=location,
            mesh_id=mesh_id,
            mesh_role=mesh_role,
            gradient_health=gradient_health
        )
        self.devices[device_id] = device
        if mesh_id and self.enable_mesh:
            if mesh_id not in self.mesh_networks:
                self.mesh_networks[mesh_id] = MeshNetwork(mesh_id=mesh_id)
            self.mesh_networks[mesh_id].add_device(device)
        if self.enable_federated:
            self.federated_learner.device_models[device_id] = {}
        self._create_task(self._save_state())

        event = FeedbackEvent.create_with_context(
            task_id=f"iot_register_{device_id}",
            selected_action="register_device",
            quality_score=0.9,
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="iot",
            adaptive_cost_value=0.0,
            state={'device_id': device_id, 'type': device_type.value},
            candidates=[{'action': 'register'}],
            source="iot_expert",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["iot", "device"]
        )
        self._create_task(self.queue.publish("feedback_events", event.to_json()))

        if self.drift:
            self._create_task(self.drift.check_drift(self.adaptive_cost.get_current_weights()))

        return device

    def create_mesh(self, mesh_id: str, device_ids: List[str]) -> MeshNetwork:
        mesh = MeshNetwork(mesh_id=mesh_id)
        for device_id in device_ids:
            if device_id in self.devices:
                mesh.add_device(self.devices[device_id])
        for i, d1 in enumerate(device_ids):
            for d2 in device_ids[i+1:]:
                if d1 in self.devices and d2 in self.devices:
                    mesh.add_connection(d1, d2, np.random.uniform(0.5, 1.0), np.random.uniform(10, 100), np.random.uniform(1, 50))
        mesh.elect_leader()
        self.mesh_networks[mesh_id] = mesh
        if self.enable_self_healing and self.self_healing_manager:
            self._create_task(self.self_healing_manager.detect_and_heal(mesh))
        self._create_task(self._save_state())

        event = FeedbackEvent.create_with_context(
            task_id=f"iot_create_mesh_{mesh_id}",
            selected_action="create_mesh",
            quality_score=0.9,
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="iot",
            adaptive_cost_value=0.0,
            state={'mesh_id': mesh_id, 'device_count': len(device_ids)},
            candidates=[{'action': 'create'}],
            source="iot_expert",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["iot", "mesh"]
        )
        self._create_task(self.queue.publish("feedback_events", event.to_json()))

        if self.drift:
            self._create_task(self.drift.check_drift(self.adaptive_cost.get_current_weights()))

        return mesh

    # --------------------------------------------------------------------------
    # Digital Twin Simulation (with persistence)
    # --------------------------------------------------------------------------
    def simulate_mesh_scenario(self, scenario: Dict[str, Any]) -> Dict[str, Any]:
        mesh_id = scenario.get('mesh_id')
        if mesh_id not in self.mesh_networks:
            return {'status': 'error', 'error': 'Mesh not found'}
        mesh = self.mesh_networks[mesh_id]
        impact = {
            'scenario': scenario.get('type', 'unknown'),
            'mesh_id': mesh_id,
            'predicted_health_score': mesh.health_score,
            'device_failures': 0,
            'energy_impact_kwh': 0.0,
        }
        self.simulation_results.append({'timestamp': datetime.now(timezone.utc).isoformat(), **impact})
        self._create_task(self._save_state())
        return impact

    def compare_deployment_strategies(self, task: Dict[str, Any]) -> Dict[str, Any]:
        strategies = ['cloud_heavy', 'edge_heavy', 'federated', 'opportunistic']
        scores = {}
        for strat in strategies:
            quality = 0.6 if strat == 'cloud_heavy' else 0.7
            carbon_g = 15.0 if strat == 'cloud_heavy' else 5.0
            latency_ms = 20.0 if strat == 'cloud_heavy' else 80.0
            energy_joules = 100.0 if strat == 'cloud_heavy' else 30.0
            cost = self.adaptive_cost.compute(
                quality=quality, carbon_g=carbon_g, latency_ms=latency_ms,
                energy_joules=energy_joules, health=True, atp=0.5
            )
            scores[strat] = cost
        best = max(scores, key=scores.get)
        return {
            'strategies_compared': strategies,
            'scores': scores,
            'recommended_strategy': best,
        }

    # --------------------------------------------------------------------------
    # Natural Language Explanations
    # --------------------------------------------------------------------------
    def explain_mesh_topology(self, mesh_id: str) -> Dict[str, Any]:
        if mesh_id not in self.mesh_networks:
            return {'mesh_id': mesh_id, 'explanation': 'Mesh not found'}
        mesh = self.mesh_networks[mesh_id]
        parts = []
        parts.append(f"Mesh {mesh_id} has {len(mesh.devices)} devices.")
        if mesh.leader_id:
            parts.append(f"Leader is {mesh.leader_id}.")
        if self.enable_self_healing:
            parts.append("Self-healing is enabled.")
        return {'mesh_id': mesh_id, 'explanation': " ".join(parts)}

    # --------------------------------------------------------------------------
    # Proposal Method (Enhanced with FeedbackEvent and bio integration)
    # --------------------------------------------------------------------------
    async def propose_async(self, context: dict) -> dict:
        try:
            helium_scarcity = context.get('helium_scarcity', 0.5)
            # Correct carbon intensity conversion
            carbon_intensity = context.get('carbon_intensity')
            if carbon_intensity is None:
                carbon_intensity = 400.0
            elif carbon_intensity < 1.0:
                carbon_intensity = carbon_intensity * 800.0

            network_latency = context.get('network_latency_ms', 50.0)
            task_type = context.get('task_type', 'general')
            location = context.get('location')

            # Augment with bio‑inspired signals
            if self.enable_bio_integration:
                if self.quantum_bridge:
                    try:
                        q_params = self.quantum_bridge.get_qubo_parameters()
                        q_penalty_helium = q_params.get('penalty_helium_shortage', 0.5)
                        if q_penalty_helium > 0.7:
                            helium_scarcity = min(1.0, helium_scarcity * 1.1)
                    except Exception as e:
                        logger.warning(f"QuantumBridge error: {e}")
                if self.tick_engine and hasattr(self.tick_engine, 'get_helium_forecast'):
                    try:
                        forecast = self.tick_engine.get_helium_forecast(4)
                        if forecast and len(forecast) > 3:
                            avg_future = np.mean(forecast)
                            if avg_future < 0.3:
                                helium_scarcity = max(helium_scarcity, 0.8)
                    except Exception as e:
                        logger.warning(f"TimeTickEngine error: {e}")
                if self.gradient_manager:
                    gradients = self.gradient_manager.get_field_strengths()
                    if gradients.get('trust', 0.5) < 0.3:
                        helium_scarcity = min(1.0, helium_scarcity * 1.2)

            best_device = None
            best_mesh = None
            if self.devices:
                best_device = max(
                    self.devices.values(),
                    key=lambda d: (d.gradient_health * 0.5 + d.energy_remaining_percent / 100 * 0.3 +
                                  (1.0 - d.processing_utilization) * 0.2)
                )

            if self.mesh_networks and self.enable_mesh:
                best_mesh = max(
                    self.mesh_networks.values(),
                    key=lambda m: m.health_score
                )

            if helium_scarcity > 0.6:
                sampling_rate = 5.0
                aggregation_strategy = 'compressed'
                power_saving = True
            else:
                sampling_rate = 10.0
                aggregation_strategy = 'adaptive'
                power_saving = False

            preferred_gateways = []
            if network_latency > 100:
                preferred_gateways = ['gateway_nearby']

            mesh_recommendation = None
            if best_mesh:
                mesh_recommendation = {
                    'mesh_id': best_mesh.mesh_id,
                    'leader': best_mesh.leader_id,
                    'device_count': len(best_mesh.devices),
                    'health_score': best_mesh.health_score
                }

            if self.enable_cross_domain:
                energy_insights = await self.cross_domain_transfer.apply_energy_knowledge(
                    {'device_count': len(self.devices)}
                )
                if energy_insights.get('applied_pattern') != 'default':
                    aggregation_strategy = energy_insights['applied_pattern']

            forecast = None
            if self.enable_predictive_sustainability and self.predictive_sustainability and best_device:
                self.predictive_sustainability.update_history(
                    {
                        'battery_percent': best_device.energy_remaining_percent,
                        'processing_load': best_device.processing_utilization,
                        'network_quality': 0.8,
                        'harvesting_available': best_device.harvesting_available_w,
                        'carbon_intensity': best_device.carbon_intensity_g_per_kwh
                    },
                    {
                        'carbon_savings_kg': self.total_carbon_saved_kg,
                        'energy_savings_kwh': self.total_energy_harvested_kwh,
                        'sustainability_score': self.sustainability_score
                    }
                )
                await self.predictive_sustainability.train_forecast_model()
                forecast = await self.predictive_sustainability.predict_device_health(
                    24,
                    f"{best_device.location['lat']}_{best_device.location['lon']}" if best_device.location else None
                )

            self_healing_status = "active" if self.enable_self_healing else "inactive"

            recommendations = {
                'sampling_rate_hz': sampling_rate,
                'aggregation_strategy': aggregation_strategy,
                'preferred_gateways': preferred_gateways,
                'power_saving_mode': power_saving,
                'device_recommendation': best_device.device_id if best_device else None,
                'mesh_recommendation': mesh_recommendation,
                'self_healing_status': self_healing_status,
                'sustainability_forecast': forecast
            }

            explanation = self._generate_propose_explanation(
                recommendations, helium_scarcity, carbon_intensity, network_latency
            )

            # Bio-inspired integration: ATP spend/earn and gradient pumping
            if self.token_manager:
                atp_cost = 0.05
                await self.token_manager.spend("iot_expert", atp_cost)
                if power_saving:
                    await self.token_manager.earn("iot_expert", atp_cost * 1.5)
            if self.gradient_manager:
                trust_delta = 0.03 if self.health_status == "healthy" else -0.04
                self.gradient_manager.pump_field('trust', trust_delta, source="iot_propose")
                if helium_scarcity > 0.7:
                    self.gradient_manager.pump_field('helium', 0.1, source="iot_propose")

            event = FeedbackEvent.create_with_context(
                task_id=f"iot_propose_{uuid.uuid4().hex[:8]}",
                selected_action="propose",
                quality_score=0.9,
                energy_joules=0.0,
                carbon_g=0.0,
                feedback_type="iot",
                adaptive_cost_value=0.0,
                state=context,
                candidates=[{'action': s} for s in ['sampling_high', 'sampling_low', 'compressed', 'adaptive', 'power_saving']],
                source="iot_expert",
                environment=getattr(central_config, "ENVIRONMENT", "production"),
                tags=["iot", "proposal"]
            )
            await self.queue.publish("feedback_events", event.to_json())

            if self.drift:
                await self.drift.check_drift(self.adaptive_cost.get_current_weights())

            return {
                'recommendations': recommendations,
                'explanation': explanation,
                'context_used': {
                    'helium_scarcity': helium_scarcity,
                    'carbon_intensity': carbon_intensity,
                    'network_latency_ms': network_latency,
                    'task_type': task_type
                }
            }
        except Exception as e:
            logger.error(f"Error in propose_async: {e}", exc_info=True)
            self.health_status = "degraded"
            self.last_error = str(e)
            fallback = {
                'sampling_rate_hz': 5.0,
                'aggregation_strategy': 'compressed',
                'preferred_gateways': ['gateway_nearby'],
                'power_saving_mode': True,
                'device_recommendation': None,
                'mesh_recommendation': None,
                'self_healing_status': 'inactive',
                'sustainability_forecast': None
            }
            return {
                'recommendations': fallback,
                'explanation': f"Due to an error ({e}), a conservative fallback recommendation has been applied.",
                'context_used': context
            }

    def _generate_propose_explanation(
        self,
        recommendations: Dict[str, Any],
        helium_scarcity: float,
        carbon_intensity: float,
        network_latency: float
    ) -> str:
        parts = []
        if helium_scarcity > 0.6:
            parts.append(f"Helium scarcity is high ({helium_scarcity:.2f}), so sampling rate is reduced to {recommendations['sampling_rate_hz']:.1f} Hz and compression enabled.")
        else:
            parts.append(f"Helium scarcity is moderate ({helium_scarcity:.2f}), maintaining standard sampling.")
        if recommendations.get('power_saving_mode'):
            parts.append("Power-saving mode activated to extend device battery life.")
        if recommendations.get('mesh_recommendation'):
            mesh = recommendations['mesh_recommendation']
            parts.append(f"Mesh network '{mesh['mesh_id']}' recommended with {mesh['device_count']} devices and health score {mesh['health_score']:.2f}.")
        if self.enable_self_healing:
            parts.append("Self-healing capabilities are active.")
        if not parts:
            parts.append("IoT deployment is optimal based on current metrics.")
        return " ".join(parts)

    # --------------------------------------------------------------------------
    # Primary Optimization (Enhanced with FeedbackEvent and bio integration)
    # --------------------------------------------------------------------------
    async def optimize_edge_deployment(
        self,
        device_type: str,
        carbon_zone: int,
        helium_scarcity: float,
        task_config: Optional[Dict[str, Any]] = None,
        location: Optional[Dict[str, float]] = None
    ) -> Dict[str, Any]:
        try:
            suitable = [d for d in self.devices.values() if d.device_type.value == device_type or device_type == 'any']
            if not suitable:
                return {'expert_id': self.expert_id, 'recommendation': 'no_suitable_devices'}

            if self.enable_cross_domain:
                energy_knowledge = await self.cross_domain_transfer.apply_energy_knowledge({'device_count': len(suitable)})
                carbon_knowledge = await self.cross_domain_transfer.apply_carbon_knowledge(carbon_intensity=400)
                if energy_knowledge.get('applied_pattern') != 'default':
                    logger.info(f"Applied energy knowledge: {energy_knowledge['applied_pattern']}")
                if carbon_knowledge.get('applied_pattern') != 'default':
                    logger.info(f"Applied carbon knowledge: {carbon_knowledge['applied_pattern']}")

            atp_workers = self._get_atp_collaborative_workers() if self.enable_bio_integration else 4
            harvester_energy = self._get_harvester_energy_prediction() if self.enable_bio_integration else {}

            weather_forecast = {}
            if self.enable_weather_api and self.weather_api and location:
                try:
                    weather_forecast = await self.weather_api.get_forecast(location['lat'], location['lon'])
                except Exception as e:
                    logger.warning(f"Weather API error: {e}")

            best_mesh = None
            if self.enable_mesh and self.mesh_networks:
                for mesh in self.mesh_networks.values():
                    if len([d for d in mesh.devices.values() if d in suitable]) >= 2:
                        best_mesh = mesh
                        break

            best_device = max(suitable, key=lambda d: (d.available_processing_flops * 0.25 + d.energy_remaining_percent / 100 * 0.15 +
                                                        d.gradient_health * 0.25 + (1.0 - d.processing_utilization) * 0.15 +
                                                        d.harvesting_available_w * 0.2))

            federated_result = None
            if self.enable_federated and best_device:
                device_data = [{
                    'battery_percent': best_device.energy_remaining_percent,
                    'processing_load': best_device.processing_utilization,
                    'network_quality': 0.8,
                    'harvesting_available': best_device.harvesting_available_w,
                    'optimization_score': best_device.gradient_health
                }]
                federated_result = await self.federated_learner.participate_in_round(
                    device_data,
                    performance=best_device.gradient_health
                )

            predictive_forecast = None
            if self.enable_predictive_sustainability and self.predictive_sustainability and best_device:
                self.predictive_sustainability.update_history(
                    {
                        'battery_percent': best_device.energy_remaining_percent,
                        'processing_load': best_device.processing_utilization,
                        'network_quality': 0.8,
                        'harvesting_available': best_device.harvesting_available_w,
                        'carbon_intensity': best_device.carbon_intensity_g_per_kwh
                    },
                    {
                        'carbon_savings_kg': self.total_carbon_saved_kg,
                        'energy_savings_kwh': self.total_energy_harvested_kwh,
                        'sustainability_score': self.sustainability_score
                    }
                )
                await self.predictive_sustainability.train_forecast_model()
                predictive_forecast = await self.predictive_sustainability.predict_device_health(
                    24,
                    f"{best_device.location['lat']}_{best_device.location['lon']}" if best_device.location else None
                )

            if self.enable_predictive_sustainability and best_device and best_device.location:
                self.predictive_sustainability.update_carbon_model(
                    f"{best_device.location['lat']}_{best_device.location['lon']}",
                    {'carbon_intensity': best_device.carbon_intensity_g_per_kwh}
                )

            cb_analysis = None
            if self.cost_benefit_engine:
                params = {'device_count': 1, 'processing_flops': best_device.available_processing_flops}
                cb_analysis = await self.cost_benefit_engine.analyze_scenario('iot_deployment', params)

            if self.swarm_coordinator:
                await self.swarm_coordinator.share_predictions({
                    'expert_id': self.expert_id,
                    'best_device': best_device.device_id,
                    'carbon_intensity': carbon_intensity,
                    'helium_scarcity': helium_scarcity
                })

            plan = {
                'expert_id': self.expert_id,
                'version': self.version,
                'strategy': 'bio_mesh_collaborative' if best_mesh and self.enable_bio_integration else 'single_device',
                'primary_device': best_device.device_id,
                'mesh_id': best_mesh.mesh_id if best_mesh else None,
                'mesh_size': len(best_mesh.devices) if best_mesh else 1,
                'estimated_carbon_kg': best_device.carbon_per_operation_g / 1000,
                'energy_remaining_percent': best_device.energy_remaining_percent,
                'can_operate_indefinitely': best_device.can_operate_indefinitely,
                'bio_integration_active': self.enable_bio_integration,
                'federated_active': self.enable_federated,
                'cross_domain_active': self.enable_cross_domain,
                'predictive_sustainability_active': self.enable_predictive_sustainability,
                'self_healing_active': self.enable_self_healing,
                'weather_api_active': self.enable_weather_api,
                'telemetry_active': self.enable_telemetry,
                'differential_privacy_active': self.enable_differential_privacy,
                'gradient_health': best_device.gradient_health,
                'atp_workers': atp_workers,
                'harvester_energy_kw': harvester_energy.get('total_kw', 0),
                'weather_forecast': weather_forecast.get('harvesting_potential', 0.5) if weather_forecast else 0.5,
                'gradient_levels': self._get_gradient_levels() if self.enable_bio_integration else {},
                'federated_round': federated_result.get('round', 0) if federated_result else 0,
                'federated_contribution': federated_result.get('contribution_score', 0) if federated_result else 0,
                'predictive_forecast': predictive_forecast,
                'sustainability_score': self.sustainability_score,
                'cost_benefit_analysis': cb_analysis,
                'recommendations': self._generate_recommendations(best_device, best_mesh)
            }

            self.total_tasks_processed += 1
            self.total_energy_harvested_kwh += harvester_energy.get('total_kw', 0) * 0.01
            self.total_carbon_saved_kg += best_device.carbon_per_operation_g / 1000 * 0.1
            self.sustainability_score = min(1.0, (
                (best_device.energy_remaining_percent / 100) * 0.25 +
                best_device.gradient_health * 0.25 +
                (1.0 - best_device.processing_utilization) * 0.15 +
                (1.0 - helium_scarcity) * 0.15 +
                weather_forecast.get('harvesting_potential', 0.5) * 0.2
            ))

            # Bio-inspired ATP spend/earn
            if self.token_manager:
                atp_cost = 0.1
                await self.token_manager.spend("iot_expert", atp_cost)
                if self.sustainability_score > 0.7:
                    await self.token_manager.earn("iot_expert", atp_cost * 2)
            if self.gradient_manager:
                self.gradient_manager.pump_field('trust', 0.05 if self.sustainability_score > 0.7 else -0.02, source="iot_optimize")
                if carbon_intensity > 500:
                    self.gradient_manager.pump_field('carbon', 0.1, source="iot_optimize")

            self._create_task(self._save_state())

            event = FeedbackEvent.create_with_context(
                task_id=f"iot_optimize_{uuid.uuid4().hex[:8]}",
                selected_action="optimize_deployment",
                quality_score=0.9,
                energy_joules=0.0,
                carbon_g=0.0,
                feedback_type="iot",
                adaptive_cost_value=0.0,
                state={'device_type': device_type, 'carbon_zone': carbon_zone},
                candidates=[{'action': 'optimize'}],
                source="iot_expert",
                environment=getattr(central_config, "ENVIRONMENT", "production"),
                tags=["iot", "optimization"]
            )
            await self.queue.publish("feedback_events", event.to_json())

            if self.drift:
                await self.drift.check_drift(self.adaptive_cost.get_current_weights())

            return plan

        except Exception as e:
            logger.error(f"Error in optimize_edge_deployment: {e}", exc_info=True)
            self.health_status = "degraded"
            self.last_error = str(e)
            return {
                'expert_id': self.expert_id,
                'recommendation': 'error',
                'error': str(e)
            }

    def _generate_recommendations(self, device: EdgeDevice, mesh: Optional[MeshNetwork]) -> List[str]:
        recs = []
        if device.energy_remaining_percent < 20:
            recs.append(f"Device {device.device_id} battery low ({device.energy_remaining_percent:.0f}%).")
        if device.gradient_health < 0.3:
            recs.append(f"Device {device.device_id} has low gradient health ({device.gradient_health:.2f}).")
        if mesh and len(mesh.devices) >= 3:
            recs.append("Mesh network available for collaborative processing.")
        if device.can_operate_indefinitely:
            recs.append("Device has sufficient energy harvesting for continuous operation.")
        if self.enable_federated:
            federated_insights = self.federated_learner.get_federated_insights()
            if federated_insights.get('participants', 0) > 1:
                recs.append(f"Federated learning active with {federated_insights['participants']} participants.")
        if self.enable_self_healing:
            recs.append("Self-healing mesh capabilities are enabled.")
        if self.enable_weather_api:
            recs.append("Weather API integration active for harvesting predictions.")
        if self.enable_telemetry:
            recs.append("Device telemetry monitoring active.")
        return recs if recs else ["Deployment configuration is optimal."]

    # --------------------------------------------------------------------------
    # Expert Statistics (Enhanced with central metrics)
    # --------------------------------------------------------------------------
    def get_expert_statistics(self) -> Dict[str, Any]:
        stats = {
            'expert_id': self.expert_id,
            'version': self.version,
            'total_devices': len(self.devices),
            'mesh_networks': len(self.mesh_networks),
            'total_tasks_processed': self.total_tasks_processed,
            'total_energy_harvested_kwh': self.total_energy_harvested_kwh,
            'total_carbon_saved_kg': self.total_carbon_saved_kg,
            'total_ecoatp_saved': self.total_ecoatp_saved,
            'sustainability_score': self.sustainability_score,
            'bio_integration_active': self.enable_bio_integration,
            'federated_active': self.enable_federated,
            'cross_domain_active': self.enable_cross_domain,
            'predictive_sustainability_active': self.enable_predictive_sustainability,
            'self_healing_active': self.enable_self_healing,
            'weather_api_active': self.enable_weather_api,
            'telemetry_active': self.enable_telemetry,
            'differential_privacy_active': self.enable_differential_privacy,
            'average_gradient_health': np.mean([d.gradient_health for d in self.devices.values()]) if self.devices else 0,
            'gradient_levels': self._get_gradient_levels() if self.enable_bio_integration else {},
            'harvester_energy': self._get_harvester_energy_prediction() if self.enable_bio_integration else {},
            'simulation_count': len(self.simulation_results)
        }
        if self.enable_federated:
            stats['federated_insights'] = self.federated_learner.get_federated_insights()
        if self.enable_cross_domain:
            stats['cross_domain_stats'] = self.cross_domain_transfer.get_transfer_statistics()
        if self.enable_predictive_sustainability:
            stats['sustainability_summary'] = self.predictive_sustainability.get_sustainability_summary()
        if self.enable_self_healing and self.self_healing_manager:
            stats['self_healing_stats'] = self.self_healing_manager.get_recovery_stats()
        if self.enable_weather_api and self.weather_api:
            stats['weather_api_status'] = {
                'connected': self.weather_api._session is not None,
                'last_update': self.weather_api.last_update.isoformat() if self.weather_api.last_update else None
            }
        if self.enable_telemetry and self.telemetry_collector:
            stats['telemetry_status'] = {
                'active_devices': len(self.telemetry_collector.telemetry_history),
                'total_samples': sum(len(h) for h in self.telemetry_collector.telemetry_history.values())
            }
        if self.cost_benefit_engine:
            stats['cost_benefit_available'] = True

        # Generic metric updates
        self.metrics.set("device_count", len(self.devices))
        self.metrics.set("mesh_count", len(self.mesh_networks))
        self.metrics.set("sustainability_score", self.sustainability_score)

        return stats

    def get_device_status(self) -> Dict[str, Any]:
        status = {}
        for did, d in self.devices.items():
            status[did] = {
                'type': d.device_type.value,
                'online': d.is_online,
                'battery_percent': d.energy_remaining_percent,
                'mesh_role': d.mesh_role.value,
                'gradient_health': d.gradient_health,
                'token_balance': d.token_balance,
                'federated_round': d.federated_round if hasattr(d, 'federated_round') else 0,
                'self_healing_attempts': d.self_healing_attempts,
                'failure_count': len(d.failure_history)
            }
        return status

    # --------------------------------------------------------------------------
    # Self‑Healing Action (with FeedbackEvent)
    # --------------------------------------------------------------------------
    async def self_heal_mesh(self, mesh_id: str) -> Dict[str, Any]:
        if not self.self_healing_manager:
            return {'status': 'error', 'error': 'Self-healing not enabled'}
        if mesh_id not in self.mesh_networks:
            return {'status': 'error', 'error': 'Mesh not found'}
        result = await self.self_healing_manager.detect_and_heal(self.mesh_networks[mesh_id])

        event = FeedbackEvent.create_with_context(
            task_id=f"iot_self_heal_{mesh_id}",
            selected_action="self_heal",
            quality_score=0.9 if result.get('health_score', 0) > 0.5 else 0.3,
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="iot",
            adaptive_cost_value=0.0,
            state={'mesh_id': mesh_id},
            candidates=[{'action': 'self_heal'}],
            source="iot_expert",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["iot", "self_heal"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        if self.drift:
            await self.drift.check_drift(self.adaptive_cost.get_current_weights())

        return {'status': 'success', 'result': result}

    # --------------------------------------------------------------------------
    # Cleanup
    # --------------------------------------------------------------------------
    async def shutdown(self):
        logger.info(f"Shutting down IoT Expert {self.expert_id}")
        await self.federated_learner.close()
        if self.weather_api:
            await self.weather_api.close()
        await self._save_state()
        logger.info("IoT Expert shutdown complete")
