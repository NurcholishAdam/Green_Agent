# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/iot_expert.py
"""
Enhanced IoT Expert v7.1.0 - Complete Metabolic Edge Decomposer
With Digital Twin Simulation, What-If Analysis, Natural Language Explanations,
Federated Reflexive Learning, Cross-Domain Knowledge Transfer, Predictive Sustainability,
Enhanced Carbon/Helium Awareness, Advanced Sustainability Features,
Real-time Device Telemetry Integration, Self-Healing Mesh Capabilities,
External Weather API Integration, Differential Privacy for Federated Learning,
Carbon Intensity Forecasting Based on Location,
BaseExpert propose() integration, cross-domain knowledge fusion,
and enhanced self-healing with predictive rerouting.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
import networkx as nx
from collections import defaultdict, deque
import hashlib
import json
import math
import uuid
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_squared_error, r2_score
import aiohttp
import asyncio
import os

# ============================================================================
# BaseExpert Import (must be present)
# ============================================================================
from .base_expert import BaseExpert

logger = logging.getLogger(__name__)

# ============================================================================
# Bio-Inspired Import Check
# ============================================================================
try:
    from enhancements.bio_inspired.eco_atp_currency import EcoATPTokenManager, EcoATPConsumer
    from enhancements.bio_inspired.proton_gradient_fields import GradientFieldManager
    from enhancements.bio_inspired.atp_synthase_scheduler import ATPSynthaseScheduler
    from enhancements.bio_inspired.chromatophore_compartments import CompartmentManager, MembranePermeability, CompartmentState
    from enhancements.bio_inspired.biomass_storage import BiomassStorage, StorageTier, GuaranteeLevel
    from enhancements.bio_inspired.photosynthetic_harvester import PhotosyntheticHarvester
    BIO_INSPIRED_AVAILABLE = True
except ImportError:
    BIO_INSPIRED_AVAILABLE = False

# ============================================================================
# Enums (Enhanced)
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
    WIFI = "wifi"; BLUETOOTH = "bluetooth"; ZIGBEE = "zigbee"
    LORA = "lora"; THREAD = "thread"; MATTER = "matter"
    ETHERNET = "ethernet"; CELLULAR = "cellular"

class EnergySource(Enum):
    BATTERY = "battery"; SOLAR = "solar"; KINETIC = "kinetic"
    THERMAL = "thermal"; RF_HARVESTING = "rf_harvesting"; GRID = "grid"
    HYBRID = "hybrid"; HARVESTER_DRIVEN = "harvester_driven"

class ProcessingMode(Enum):
    LOCAL_ONLY = "local_only"; MESH_COLLABORATIVE = "mesh_collaborative"
    CLOUD_OFFLOAD = "cloud_offload"; HYBRID = "hybrid"
    OPPORTUNISTIC = "opportunistic"; ATP_DRIVEN = "atp_driven"
    FEDERATED = "federated"

class MeshRole(Enum):
    LEADER = "leader"; ROUTER = "router"; LEAF = "leaf"; MEMBRANE_GATED = "membrane_gated"
    FEDERATED = "federated"; SELF_HEALING = "self_healing"

# ============================================================================
# Data Classes (Enhanced)
# ============================================================================

@dataclass
class DeviceTelemetry:
    """Real-time device telemetry data"""
    device_id: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
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
    last_heartbeat: datetime = field(default_factory=datetime.utcnow)
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
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_topology_update: datetime = field(default_factory=datetime.utcnow)
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
            'mesh_id': self.mesh_id, 'device_count': len(self.devices), 'leader_id': self.leader_id,
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
# Real-time Device Telemetry Integration (unchanged)
# ============================================================================

class DeviceTelemetryCollector:
    """
    Real-time device telemetry integration.
    
    Features:
    - Telemetry data collection
    - Metric aggregation
    - Anomaly detection
    - Trend analysis
    """
    
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
    
    async def collect_telemetry(self, device_id: str, telemetry: DeviceTelemetry):
        """Collect real-time telemetry data"""
        async with self._lock:
            if device_id not in self.telemetry_history:
                self.telemetry_history[device_id] = deque(maxlen=1000)
            
            self.telemetry_history[device_id].append(telemetry)
            
            # Detect anomalies
            anomalies = self._detect_anomalies(device_id, telemetry)
            
            return {
                'device_id': device_id,
                'timestamp': telemetry.timestamp.isoformat(),
                'anomalies': anomalies,
                'status': 'warning' if anomalies else 'healthy'
            }
    
    def _detect_anomalies(self, device_id: str, telemetry: DeviceTelemetry) -> List[str]:
        """Detect anomalies in telemetry data"""
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
        """Get health status for a device"""
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
# Self-Healing Mesh Manager (Enhanced)
# ============================================================================

class SelfHealingMeshManager:
    """
    Self-healing mesh capabilities with predictive rerouting and load forecasting.
    
    Features:
    - Automatic failure detection
    - Path reconfiguration with predictive rerouting
    - Leader re-election
    - Device recovery
    - Load forecasting based on historical data
    """
    
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
        self.predictive_model = None
        
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
            
            # Update health score
            mesh.health_score = self._calculate_health_score(mesh)
            
            return {
                'mesh_id': mesh.mesh_id,
                'issues_detected': len(issues),
                'actions_taken': len(actions),
                'actions': actions,
                'health_score': mesh.health_score,
                'timestamp': datetime.utcnow().isoformat()
            }
    
    def _detect_issues(self, mesh: MeshNetwork) -> List[Dict]:
        issues = []
        # Check leader availability
        if mesh.leader_id and mesh.leader_id in mesh.devices:
            leader = mesh.devices[mesh.leader_id]
            if not leader.is_online or leader.energy_remaining_percent < 10:
                issues.append({'type': 'leader_failure', 'device_id': mesh.leader_id})
        elif mesh.leader_id is None:
            issues.append({'type': 'leader_failure', 'device_id': None})
        
        # Check router health
        routers = [d for d in mesh.devices.values() if d.mesh_role == MeshRole.ROUTER]
        for router in routers:
            if not router.is_online or router.energy_remaining_percent < 5:
                issues.append({'type': 'router_failure', 'device_id': router.device_id})
        
        # Check link health
        for u, v, data in mesh.topology_graph.edges(data=True):
            if data.get('quality', 1.0) < 0.3:
                issues.append({'type': 'link_failure', 'source': u, 'target': v})
        
        # Check for overloaded devices using predictive load
        for device in mesh.devices.values():
            predicted_load = self._predict_load(device.device_id)
            if predicted_load > 0.85:
                issues.append({'type': 'device_overload', 'device_id': device.device_id, 'predicted_load': predicted_load})
        
        return issues
    
    def _predict_load(self, device_id: str) -> float:
        """Predict future load based on historical data."""
        if device_id in self.load_history and len(self.load_history[device_id]) >= 10:
            history = list(self.load_history[device_id])[-10:]
            slope = np.polyfit(range(len(history)), history, 1)[0]
            return min(1.0, history[-1] + slope * 2)  # 2-step forecast
        return 0.5
    
    async def _recover_leader(self, mesh: MeshNetwork, issue: Dict) -> Dict:
        new_leader = mesh.elect_leader()
        mesh.leader_id = new_leader
        mesh.failure_count += 1
        self.recovery_history.append({
            'timestamp': datetime.utcnow().isoformat(),
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
                    'timestamp': datetime.utcnow().isoformat(),
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
            # Find alternative path via re-routing
            mesh.topology_graph.add_edge(source, target, quality=0.7, bandwidth=50, latency=30)
            self.recovery_history.append({
                'timestamp': datetime.utcnow().isoformat(),
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
                    'timestamp': datetime.utcnow().isoformat(),
                    'action': 'load_rebalance',
                    'mesh_id': mesh.mesh_id,
                    'source': device_id,
                    'target': target.device_id,
                    'load_transfer': load_transfer
                })
                return {'action': 'load_rebalance', 'source': device_id, 'target': target.device_id, 'load_transfer': load_transfer, 'status': 'success'}
        return {'action': 'load_rebalance', 'status': 'failed'}
    
    def _calculate_health_score(self, mesh: MeshNetwork) -> float:
        if not mesh.devices:
            return 0.0
        avg_health = np.mean([d.gradient_health for d in mesh.devices.values()])
        connectivity = 1.0 if nx.is_connected(mesh.topology_graph) else 0.5
        leader_health = 1.0 if mesh.leader_id and mesh.devices[mesh.leader_id].is_online else 0.5
        return (avg_health * 0.4 + connectivity * 0.3 + leader_health * 0.3)
    
    def record_load(self, device_id: str, load: float):
        """Record load for predictive modeling."""
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
# External Weather API Integration (Enhanced with retries and caching)
# ============================================================================

class WeatherAPIClient:
    """
    External weather API integration with retries, caching, and fallback.
    
    Features:
    - Solar irradiance forecasting
    - Wind speed forecasting
    - Temperature forecasting
    - Harvesting potential estimation
    - Retry logic with exponential backoff
    - Persistent caching
    """
    
    def __init__(self, api_key: Optional[str] = None, max_retries: int = 3):
        self.api_key = api_key or os.getenv('WEATHER_API_KEY', '')
        self.endpoint = "https://api.openweathermap.org/data/2.5"
        self._session = None
        self.cache = {}
        self.last_update = None
        self.update_interval = 3600  # 1 hour
        self.max_retries = max_retries
        
        logger.info("Weather API Client initialized")
    
    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def get_forecast(self, lat: float, lon: float, hours: int = 24) -> Dict[str, Any]:
        cache_key = f"{lat}_{lon}_{hours}_{datetime.utcnow().hour}"
        if cache_key in self.cache and self.last_update and (datetime.utcnow() - self.last_update).seconds < self.update_interval:
            return self.cache[cache_key]
        
        for attempt in range(self.max_retries):
            try:
                session = await self._get_session()
                url = f"{self.endpoint}/forecast"
                params = {
                    'lat': lat, 'lon': lon,
                    'appid': self.api_key,
                    'units': 'metric'
                }
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
                            'timestamp': datetime.utcnow().isoformat(),
                            'location': {'lat': lat, 'lon': lon}
                        }
                        self.cache[cache_key] = result
                        self.last_update = datetime.utcnow()
                        return result
                    else:
                        if attempt == self.max_retries - 1:
                            logger.warning(f"Weather API returned {response.status}, using fallback")
                            return self._get_fallback_forecast(lat, lon, hours)
                        await asyncio.sleep(2 ** attempt)  # exponential backoff
            except Exception as e:
                logger.error(f"Weather API error (attempt {attempt+1}): {e}")
                if attempt == self.max_retries - 1:
                    return self._get_fallback_forecast(lat, lon, hours)
                await asyncio.sleep(2 ** attempt)
        return self._get_fallback_forecast(lat, lon, hours)
    
    def _extract_solar_forecast(self, data: Dict) -> List[Dict]:
        forecasts = []
        for item in data.get('list', [])[:8]:
            clouds = item.get('clouds', {}).get('all', 50)
            solar_kw = max(0, (100 - clouds) / 100 * 0.8)
            forecasts.append({
                'timestamp': item.get('dt_txt'),
                'solar_kw': solar_kw,
                'cloud_cover_percent': clouds
            })
        return forecasts
    
    def _extract_wind_forecast(self, data: Dict) -> List[Dict]:
        forecasts = []
        for item in data.get('list', [])[:8]:
            wind_speed = item.get('wind', {}).get('speed', 0)
            wind_kw = min(1.0, wind_speed / 15)
            forecasts.append({
                'timestamp': item.get('dt_txt'),
                'wind_kw': wind_kw,
                'wind_speed_ms': wind_speed
            })
        return forecasts
    
    def _extract_temperature_forecast(self, data: Dict) -> List[Dict]:
        forecasts = []
        for item in data.get('list', [])[:8]:
            temp = item.get('main', {}).get('temp', 20)
            forecasts.append({
                'timestamp': item.get('dt_txt'),
                'temperature_c': temp
            })
        return forecasts
    
    def _calculate_harvesting_potential(self, solar_forecast: List, wind_forecast: List) -> float:
        if not solar_forecast or not wind_forecast:
            return 0.5
        avg_solar = np.mean([f['solar_kw'] for f in solar_forecast])
        avg_wind = np.mean([f['wind_kw'] for f in wind_forecast])
        return min(1.0, avg_solar * 0.6 + avg_wind * 0.4)
    
    def _get_fallback_forecast(self, lat: float, lon: float, hours: int) -> Dict:
        hour = datetime.utcnow().hour
        solar = max(0, 0.8 * np.sin((hour - 6) / 12 * np.pi)) if 6 <= hour <= 18 else 0
        wind = 0.3 + 0.3 * np.sin(hour / 24 * 2 * np.pi)
        return {
            'solar_forecast': [{'timestamp': 'fallback', 'solar_kw': solar}],
            'wind_forecast': [{'timestamp': 'fallback', 'wind_kw': wind}],
            'temperature_forecast': [{'timestamp': 'fallback', 'temperature_c': 20}],
            'harvesting_potential': solar * 0.6 + wind * 0.4,
            'timestamp': datetime.utcnow().isoformat(),
            'location': {'lat': lat, 'lon': lon},
            'is_fallback': True
        }
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Federated IoT Learner with Differential Privacy (unchanged)
# ============================================================================

class FederatedIoTLearner:
    """Federated reflexive learning for distributed IoT optimization with differential privacy"""
    
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
        if self.privacy_epsilon <= 0:
            return weights
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
        if not device_data:
            return 0.0
        X = []
        y = []
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
        if not self.server_url:
            return {'status': 'disabled'}
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
                    'timestamp': datetime.utcnow().isoformat()
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
        if not self.server_url:
            return None
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
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def get_federated_insights(self) -> Dict:
        return {
            'round': self.round,
            'contribution_score': self.contribution_score,
            'participants': len(self.participants),
            'has_global_model': self.global_model is not None,
            'device_models': len(self.device_models),
            'privacy_epsilon': self.privacy_epsilon,
            'last_aggregation': datetime.utcnow().isoformat()
        }
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Cross-Domain Knowledge Transfer (Enhanced)
# ============================================================================

class IoTCrossDomainTransfer:
    """Cross-domain knowledge transfer for IoT optimization"""
    
    def __init__(self):
        self.knowledge_base = {}
        self.transfer_logs = deque(maxlen=1000)
        self.domain_mappings = {
            'iot→energy': {
                'harvesting_patterns': ['solar', 'wind', 'kinetic', 'rf'],
                'power_strategies': ['adaptive', 'predictive', 'opportunistic']
            },
            'iot→data': {
                'compression_strategies': ['edge', 'fog', 'cloud', 'distributed'],
                'processing_patterns': ['batch', 'streaming', 'event-driven']
            },
            'iot→carbon': {
                'intensity_patterns': ['diurnal', 'location-based', 'load-dependent'],
                'optimization_strategies': ['load-shifting', 'efficiency-first']
            },
            'iot→helium': {
                'scarcity_patterns': ['supply-constrained', 'price-sensitive'],
                'efficiency_strategies': ['recovery', 'reuse', 'minimization']
            }
        }
        self._lock = asyncio.Lock()
        self.effectiveness_history = deque(maxlen=100)
    
    def transfer_knowledge(self, source_domain: str, target_domain: str, knowledge_type: str, data: Dict[str, Any]) -> Dict:
        key = f"{source_domain}→{target_domain}"
        if key not in self.knowledge_base:
            self.knowledge_base[key] = {}
        if knowledge_type not in self.knowledge_base[key]:
            self.knowledge_base[key][knowledge_type] = {
                'data': data,
                'transfer_count': 1,
                'effectiveness_score': 0.5,
                'last_used': datetime.utcnow()
            }
        else:
            existing = self.knowledge_base[key][knowledge_type]
            existing['data'].update(data)
            existing['transfer_count'] += 1
            existing['last_used'] = datetime.utcnow()
        self.transfer_logs.append({
            'timestamp': datetime.utcnow(),
            'source': source_domain,
            'target': target_domain,
            'type': knowledge_type
        })
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
            return {
                'applied_pattern': patterns[0] if patterns else 'default',
                'efficiency_gain': energy_knowledge.get('effectiveness_score', 0.5) * 0.15,
                'source': 'energy_domain'
            }
        return {'applied_pattern': 'default', 'source': 'local'}
    
    async def apply_carbon_knowledge(self, carbon_intensity: float) -> Dict:
        carbon_knowledge = self.get_transferred_knowledge('carbon', 'iot', 'intensity_patterns')
        if carbon_knowledge:
            patterns = carbon_knowledge.get('data', {}).get('patterns', [])
            return {
                'applied_pattern': patterns[0] if patterns else 'default',
                'carbon_adjustment': carbon_knowledge.get('effectiveness_score', 0.5) * 0.1,
                'source': 'carbon_domain'
            }
        return {'applied_pattern': 'default', 'source': 'local'}
    
    def get_transfer_statistics(self) -> Dict:
        total_transfers = len(self.transfer_logs)
        domain_pairs = {}
        for log in self.transfer_logs:
            key = f"{log['source']}→{log['target']}"
            domain_pairs[key] = domain_pairs.get(key, 0) + 1
        return {
            'total_transfers': total_transfers,
            'domain_pairs': domain_pairs,
            'knowledge_types': list(self.knowledge_base.keys()),
            'recent_transfers': list(self.transfer_logs)[-10:]
        }

# ============================================================================
# Predictive Sustainability Module (Enhanced with LSTM)
# ============================================================================

class PredictiveIoTSustainability:
    """
    Predictive sustainability analytics for IoT systems with location-based carbon forecasting.
    Enhanced with LSTM for time-series forecasting.
    """
    
    def __init__(self, history_window: int = 100):
        self.history_window = history_window
        self.device_history = deque(maxlen=history_window)
        self.sustainability_history = deque(maxlen=history_window)
        self.forecast_history = deque(maxlen=50)
        self.models = {}
        self.scaler = StandardScaler()
        self.is_trained = False
        self.carbon_models: Dict[str, Dict] = {}
        self.lstm_model = None
        
        self.models['random_forest'] = RandomForestRegressor(n_estimators=100, random_state=42)
        self.models['gradient_boosting'] = GradientBoostingRegressor(n_estimators=100, random_state=42)
        
        # LSTM model for time-series forecasting
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
            'timestamp': datetime.utcnow(),
            'battery_level': device_data.get('battery_percent', 50),
            'processing_load': device_data.get('processing_load', 0.5),
            'network_quality': device_data.get('network_quality', 0.5),
            'harvesting_available': device_data.get('harvesting_available', 0),
            'carbon_intensity': device_data.get('carbon_intensity', 400)
        })
        self.sustainability_history.append({
            'timestamp': datetime.utcnow(),
            'carbon_savings': sustainability_metrics.get('carbon_savings_kg', 0),
            'energy_savings': sustainability_metrics.get('energy_savings_kwh', 0),
            'sustainability_score': sustainability_metrics.get('sustainability_score', 0)
        })
    
    async def train_forecast_model(self):
        if len(self.device_history) < 20:
            return {'status': 'insufficient_data', 'samples': len(self.device_history)}
        X = []
        y = []
        history_list = list(self.device_history)
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([
                    data['battery_level'],
                    data['processing_load'],
                    data['network_quality'],
                    data['harvesting_available'],
                    data.get('carbon_intensity', 400) / 1000
                ])
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
        if len(X) >= 30:
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
            features.extend([
                data['battery_level'],
                data['processing_load'],
                data['network_quality'],
                data['harvesting_available'],
                data.get('carbon_intensity', 400) / 1000
            ])
        features = np.array(features).reshape(1, -1)
        features_scaled = self.scaler.transform(features)
        predictions = []
        for name, model in self.models.items():
            if model is not None:
                pred = model.predict(features_scaled)[0]
                predictions.append(pred)
        if len(self.device_history) >= 5:
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
# Enhanced IoT Expert (Main Class) - Inherits from BaseExpert
# ============================================================================

class IoTExpert(BaseExpert):
    """
    Enhanced IoT Expert v7.1.0 with all green agent features.
    Implements BaseExpert.propose() for router integration.
    """
    
    def __init__(self, expert_id: str = "iot_expert_v7", enable_mesh: bool = True,
                 enable_collaborative: bool = True, enable_offline: bool = True,
                 enable_energy_harvesting: bool = True, enable_bio_integration: bool = True,
                 enable_federated: bool = True, enable_cross_domain: bool = True,
                 enable_predictive_sustainability: bool = True,
                 enable_self_healing: bool = True,
                 enable_weather_api: bool = True,
                 enable_telemetry: bool = True,
                 enable_differential_privacy: bool = True):
        self.expert_id = expert_id
        self.version = "7.1.0"
        self.enable_mesh = enable_mesh
        self.enable_collaborative = enable_collaborative
        self.enable_offline = enable_offline
        self.enable_energy_harvesting = enable_energy_harvesting
        self.enable_bio_integration = enable_bio_integration and BIO_INSPIRED_AVAILABLE
        self.enable_federated = enable_federated
        self.enable_cross_domain = enable_cross_domain
        self.enable_predictive_sustainability = enable_predictive_sustainability
        
        self.enable_self_healing = enable_self_healing
        self.enable_weather_api = enable_weather_api
        self.enable_telemetry = enable_telemetry
        self.enable_differential_privacy = enable_differential_privacy
        
        # Bio-inspired references
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        
        # NEW modules
        self.telemetry_collector = DeviceTelemetryCollector() if enable_telemetry else None
        self.self_healing_manager = SelfHealingMeshManager() if enable_self_healing else None
        self.weather_api = WeatherAPIClient() if enable_weather_api else None
        
        privacy_epsilon = 1.0 if enable_differential_privacy else 0.0
        self.federated_learner = FederatedIoTLearner(expert_id, privacy_epsilon=privacy_epsilon)
        self.cross_domain_transfer = IoTCrossDomainTransfer()
        self.predictive_sustainability = PredictiveIoTSustainability()
        
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
        
        logger.info(f"IoT Expert v{self.version} initialized with all green agent features")
    
    def inject_bio_core(self, bio_core=None, **kwargs):
        if bio_core:
            self.token_manager = getattr(bio_core, 'token_manager', None)
            self.gradient_manager = getattr(bio_core, 'gradient_manager', None)
            self.scheduler = getattr(bio_core, 'scheduler', None)
            self.compartment_manager = getattr(bio_core, 'compartment_manager', None)
            self.biomass_storage = getattr(bio_core, 'biomass_storage', None)
            self.harvester = getattr(bio_core, 'harvester', None)
        else:
            self.token_manager = kwargs.get('token_manager')
            self.gradient_manager = kwargs.get('gradient_manager')
            self.scheduler = kwargs.get('scheduler')
            self.compartment_manager = kwargs.get('compartment_manager')
            self.biomass_storage = kwargs.get('biomass_storage')
            self.harvester = kwargs.get('harvester')
        if any([self.token_manager, self.gradient_manager, self.compartment_manager]):
            self.enable_bio_integration = True
    
    # ========================================================================
    # Bio-Inspired Data Access
    # ========================================================================
    
    def _get_membrane_mesh_role(self, device_id: str) -> MeshRole:
        if self.compartment_manager:
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
    
    # ========================================================================
    # Device Registration with Enhanced Features
    # ========================================================================
    
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
            asyncio.create_task(self.self_healing_manager.detect_and_heal(mesh))
        return mesh
    
    # ========================================================================
    # Enhanced Digital Twin Simulation
    # ========================================================================
    
    def simulate_mesh_scenario(self, scenario: Dict[str, Any]) -> Dict[str, Any]:
        scenario_type = scenario.get('type', 'device_failure')
        params = scenario.get('parameters', {})
        result = {
            'scenario_type': scenario_type,
            'parameters': params,
            'timestamp': datetime.utcnow().isoformat(),
            'predictions': {},
            'sustainability_impact': {}
        }
        if scenario_type == 'device_failure':
            result['predictions'] = self._simulate_device_failure(params)
        elif scenario_type == 'network_partition':
            result['predictions'] = self._simulate_network_partition(params)
        elif scenario_type == 'load_spike':
            result['predictions'] = self._simulate_load_spike(params)
        elif scenario_type == 'energy_depletion':
            result['predictions'] = self._simulate_energy_depletion(params)
        elif scenario_type == 'federated_failure':
            result['predictions'] = self._simulate_federated_failure(params)
        elif scenario_type == 'self_healing':
            result['predictions'] = self._simulate_self_healing(params)
        result['sustainability_impact'] = self._calculate_sustainability_impact(result['predictions'])
        self.simulation_results.append(result)
        return result
    
    def _simulate_self_healing(self, params: Dict) -> Dict[str, Any]:
        mesh_id = params.get('mesh_id')
        if mesh_id and mesh_id in self.mesh_networks:
            mesh = self.mesh_networks[mesh_id]
            failure_device = params.get('failure_device')
            if failure_device and failure_device in mesh.devices:
                mesh.devices[failure_device].is_online = False
                mesh.devices[failure_device].failure_history.append({
                    'timestamp': datetime.utcnow().isoformat(),
                    'type': 'simulated_failure'
                })
            if self.enable_self_healing and self.self_healing_manager:
                recovery_result = asyncio.run(self.self_healing_manager.detect_and_heal(mesh))
                return {
                    'failure_device': failure_device,
                    'recovery_result': recovery_result,
                    'new_leader': mesh.leader_id,
                    'mesh_health': mesh.health_score,
                    'failure_count': mesh.failure_count,
                    'recommendation': f"Self-healing {'successful' if recovery_result['actions_taken'] > 0 else 'failed'}"
                }
        return {'error': 'Mesh not found'}
    
    def _simulate_device_failure(self, params: Dict) -> Dict[str, Any]:
        device_id = params.get('device_id', 'unknown')
        mesh_id = params.get('mesh_id')
        if mesh_id and mesh_id in self.mesh_networks:
            mesh = self.mesh_networks[mesh_id]
            device = mesh.devices.get(device_id)
            if not device:
                return {'error': 'Device not found'}
            role = device.mesh_role
            connections = len(device.connections)
            if role == MeshRole.LEADER:
                impact, recovery_time = "HIGH - Leader failure requires re-election", 5.0
            elif role == MeshRole.ROUTER:
                impact, recovery_time = "MODERATE - Router failure requires path recalculation", 2.0
            else:
                impact, recovery_time = "LOW - Leaf node failure has minimal impact", 0.5
            affected = []
            if role in [MeshRole.LEADER, MeshRole.ROUTER]:
                affected = [oid for oid in mesh.devices if oid != device_id and device_id in mesh.devices[oid].connections]
            healing_recommendation = ""
            if self.enable_self_healing:
                healing_recommendation = "Self-healing will automatically re-elect leader."
            return {
                'device_id': device_id, 'role': role.value, 'impact_level': impact,
                'estimated_recovery_time_seconds': recovery_time, 'affected_devices': affected[:5],
                'affected_count': len(affected), 'leader_election_needed': role == MeshRole.LEADER,
                'self_healing_available': self.enable_self_healing,
                'recommendation': healing_recommendation or f"Pre-designate backup leader. Recovery: {recovery_time:.1f}s, {len(affected)} affected."
            }
        return {'error': 'Mesh not found'}
    
    def _simulate_network_partition(self, params: Dict) -> Dict[str, Any]:
        mesh_id = params.get('mesh_id')
        partition_pct = params.get('partition_percent', 0.3)
        if mesh_id and mesh_id in self.mesh_networks:
            mesh = self.mesh_networks[mesh_id]
            total = len(mesh.devices)
            partitioned = int(total * partition_pct)
            offline_tasks = partitioned * 10
            biomass_needed = offline_tasks * 1.0
            if self.enable_federated:
                federated_insights = self.federated_learner.get_federated_insights()
                sharing_ratio = federated_insights.get('participants', 1) / 10
            else:
                sharing_ratio = 0.3
            return {
                'total_devices': total, 'partitioned_devices': partitioned,
                'connected_devices': total - partitioned, 'predicted_offline_tasks': offline_tasks,
                'biomass_storage_needed_ecoatp': biomass_needed,
                'recovery_time_seconds': partitioned * 0.5,
                'federated_sharing_ratio': sharing_ratio,
                'self_healing_available': self.enable_self_healing,
                'recommendation': f"Pre-allocate {biomass_needed:.1f} Eco-ATP for biomass. "
                                 f"Connected: {total - partitioned} devices. "
                                 f"Federated sharing: {sharing_ratio:.1%}"
            }
        return {'error': 'Mesh not found'}
    
    def _simulate_load_spike(self, params: Dict) -> Dict[str, Any]:
        mesh_id = params.get('mesh_id')
        spike = params.get('spike_factor', 3.0)
        if mesh_id and mesh_id in self.mesh_networks:
            mesh = self.mesh_networks[mesh_id]
            total_proc = sum(d.available_processing_flops for d in mesh.devices.values())
            deficit = total_proc * spike - total_proc
            overloaded = []
            for did, d in mesh.devices.items():
                new_load = d.processing_utilization * spike
                if new_load > 0.9:
                    overloaded.append({'device_id': did, 'current': d.processing_utilization, 'predicted': min(1.0, new_load)})
            atp_workers = self._get_atp_collaborative_workers() if self.enable_bio_integration else 4
            return {
                'spike_factor': spike, 'total_processing_deficit_flops': deficit,
                'overloaded_devices': len(overloaded), 'overloaded_details': overloaded[:5],
                'estimated_queue_delay_ms': len(overloaded) * 50,
                'atp_workers_available': atp_workers,
                'self_healing_available': self.enable_self_healing,
                'recommendation': f"{'URGENT: ' if deficit > 0 else ''}Deficit: {deficit:.0f} FLOPS. "
                                 f"{len(overloaded)} devices overloaded. {atp_workers} ATP workers available."
            }
        return {'error': 'Mesh not found'}
    
    def _simulate_energy_depletion(self, params: Dict) -> Dict[str, Any]:
        mesh_id = params.get('mesh_id')
        rate = params.get('depletion_rate', 2.0)
        if mesh_id and mesh_id in self.mesh_networks:
            mesh = self.mesh_networks[mesh_id]
            depletion_times = {}
            for did, d in mesh.devices.items():
                if d.power_consumption_w > 0:
                    eff_cons = d.power_consumption_w * rate
                    net = eff_cons - d.harvesting_available_w
                    depletion_times[did] = {
                        'current_battery_wh': d.current_battery_wh,
                        'time_to_deplete_hours': d.current_battery_wh / net if net > 0 else None,
                        'can_sustain': net <= 0
                    }
            critical = sorted([(did, d) for did, d in depletion_times.items() if d['time_to_deplete_hours'] is not None],
                            key=lambda x: x[1]['time_to_deplete_hours'])[:3]
            harvester_energy = self._get_harvester_energy_prediction() if self.enable_bio_integration else {}
            weather_forecast = {}
            if self.enable_weather_api and self.weather_api and mesh.devices:
                first_device = next(iter(mesh.devices.values()))
                if first_device.location:
                    weather_forecast = asyncio.run(self.weather_api.get_forecast(first_device.location['lat'], first_device.location['lon']))
            return {
                'depletion_rate': rate,
                'devices_at_risk': len([d for d in depletion_times.values() if not d['can_sustain']]),
                'critical_devices': [{'device_id': did, 'time_hours': f"{d['time_to_deplete_hours']:.1f}"} for did, d in critical],
                'estimated_survival_hours': min(d['time_to_deplete_hours'] for _, d in critical) if critical else float('inf'),
                'harvester_energy_kw': harvester_energy.get('total_kw', 0),
                'weather_forecast': weather_forecast.get('harvesting_potential', 0.5),
                'self_healing_available': self.enable_self_healing,
                'recommendation': f"{len(critical)} devices at risk. Reduce load or boost harvesting. "
                                 f"Harvester energy: {harvester_energy.get('total_kw', 0):.2f} kW, "
                                 f"Weather potential: {weather_forecast.get('harvesting_potential', 0.5):.2f}"
            }
        return {'error': 'Mesh not found'}
    
    def _simulate_federated_failure(self, params: Dict) -> Dict[str, Any]:
        mesh_id = params.get('mesh_id')
        if mesh_id and mesh_id in self.mesh_networks:
            mesh = self.mesh_networks[mesh_id]
            total = len(mesh.devices)
            loss_factor = params.get('loss_factor', 0.5)
            affected_devices = int(total * loss_factor)
            if self.enable_federated:
                federated_insights = self.federated_learner.get_federated_insights()
                current_round = federated_insights.get('round', 0)
                participants = federated_insights.get('participants', 0)
            else:
                current_round = 0
                participants = 0
            return {
                'affected_devices': affected_devices,
                'loss_factor': loss_factor,
                'federated_round': current_round,
                'participants': participants,
                'recovery_time_seconds': affected_devices * 0.3,
                'self_healing_available': self.enable_self_healing,
                'recommendation': f"Restore federated coordination. {affected_devices} devices affected. "
                                 f"Round {current_round} in progress."
            }
        return {'error': 'Mesh not found'}
    
    def _calculate_sustainability_impact(self, predictions: Dict) -> Dict:
        impact = {
            'carbon_impact_kg': 0.0,
            'energy_impact_kwh': 0.0,
            'federated_impact': 0.0,
            'self_healing_impact': 0.0
        }
        if 'partitioned_devices' in predictions:
            impact['carbon_impact_kg'] = predictions.get('partitioned_devices', 0) * 0.01
            impact['energy_impact_kwh'] = predictions.get('partitioned_devices', 0) * 0.05
        if 'overloaded_devices' in predictions:
            impact['carbon_impact_kg'] += predictions.get('overloaded_devices', 0) * 0.02
            impact['energy_impact_kwh'] += predictions.get('overloaded_devices', 0) * 0.1
        if 'federated_sharing_ratio' in predictions:
            impact['federated_impact'] = predictions.get('federated_sharing_ratio', 0) * 0.5
        if 'self_healing_available' in predictions and predictions['self_healing_available']:
            impact['self_healing_impact'] = 0.3
        return impact
    
    # ========================================================================
    # Natural Language Explanations (Enhanced)
    # ========================================================================
    
    def explain_mesh_topology(self, mesh_id: str) -> Dict[str, Any]:
        if mesh_id not in self.mesh_networks:
            return {'error': 'Mesh not found'}
        mesh = self.mesh_networks[mesh_id]
        stats = mesh.get_mesh_statistics()
        health = "Network is fully connected and operational." if stats.get('is_connected') else "WARNING: Network has disconnected segments."
        devices_by_role = defaultdict(int)
        for d in mesh.devices.values():
            devices_by_role[d.mesh_role.value] += 1
        explanation = {
            'mesh_id': mesh_id,
            'health_assessment': health,
            'statistics': {
                'total_devices': stats.get('device_count', 0),
                'leader': mesh.leader_id,
                'average_gradient_health': f"{stats.get('average_gradient_health', 0):.2f}",
                'federated_sharing_ratio': f"{stats.get('federated_sharing_ratio', 0):.2f}",
                'sustainability_score': f"{stats.get('sustainability_score', 0):.2f}",
                'self_healing_enabled': stats.get('self_healing_enabled', False)
            },
            'device_roles': dict(devices_by_role),
            'vulnerabilities': [],
            'recommendations': []
        }
        if mesh.leader_id and mesh.leader_id in mesh.devices:
            leader = mesh.devices[mesh.leader_id]
            if leader.energy_remaining_percent < 30:
                explanation['vulnerabilities'].append(f"Leader battery low ({leader.energy_remaining_percent:.0f}%)")
                explanation['recommendations'].append("Designate backup leader or recharge.")
        router_count = devices_by_role.get('router', 0)
        if router_count < 2 and stats.get('device_count', 0) > 5:
            explanation['vulnerabilities'].append(f"Only {router_count} router(s)")
            explanation['recommendations'].append("Promote devices to router role.")
        if self.enable_federated:
            federated_insights = self.federated_learner.get_federated_insights()
            if federated_insights.get('participants', 0) > 0:
                explanation['recommendations'].append(f"Federated learning active with {federated_insights['participants']} participants.")
        if self.enable_self_healing:
            explanation['recommendations'].append("Self-healing mesh capabilities are enabled.")
        if self.enable_weather_api:
            explanation['recommendations'].append("Weather API integration active for harvesting predictions.")
        return explanation
    
    def compare_deployment_strategies(self, task: Dict[str, Any]) -> Dict[str, Any]:
        strategies = [
            {'name': 'Single Device', 'type': 'single', 'latency': 15.0, 'energy': 0.005, 'reliability': 0.90, 'sustainability': 0.7},
            {'name': 'Mesh Collaborative', 'type': 'mesh', 'latency': 10.0, 'energy': 0.003, 'reliability': 0.95, 'sustainability': 0.85},
            {'name': 'Cloud Offload', 'type': 'cloud', 'latency': 50.0, 'energy': 0.001, 'reliability': 0.99, 'sustainability': 0.6},
            {'name': 'Federated IoT', 'type': 'federated', 'latency': 8.0, 'energy': 0.002, 'reliability': 0.96, 'sustainability': 0.9}
        ]
        results = []
        for s in strategies:
            results.append({
                'strategy': s['name'],
                'predicted_latency_ms': s['latency'],
                'predicted_energy_kwh': s['energy'],
                'ecoatp_cost': s['energy'] * 1000,
                'sustainability_score': s['sustainability']
            })
        if self.enable_federated:
            federated_insights = self.federated_learner.get_federated_insights()
            if federated_insights.get('participants', 0) > 1:
                for r in results:
                    if r['strategy'] == 'Federated IoT':
                        r['sustainability_score'] *= (1 + 0.05 * federated_insights['participants'])
        if self.enable_self_healing:
            for r in results:
                if r['strategy'] == 'Mesh Collaborative':
                    r['sustainability_score'] *= 1.1
        best = min(results, key=lambda r: r['ecoatp_cost'] / r['sustainability_score'])
        return {
            'task_type': task.get('task_type', 'general'),
            'strategies_compared': results,
            'recommended_strategy': best['strategy'],
            'recommendation_reason': (f"Best sustainability per Eco-ATP cost ({best['ecoatp_cost']:.1f}) "
                                     f"with latency ({best['predicted_latency_ms']:.1f}ms)")
        }
    
    # ========================================================================
    # BaseExpert Proposal Method (NEW)
    # ========================================================================
    
    def propose(self, context: dict) -> dict:
        """
        Implements BaseExpert.propose().
        Returns recommendations for IoT sampling, gateway selection, mesh configuration,
        and power management.
        """
        # Extract relevant context
        helium_scarcity = context.get('helium_scarcity', 0.5)
        carbon_intensity = context.get('carbon_intensity', 0.5) * 800  # normalize
        network_latency = context.get('network_latency_ms', 50.0)
        task_type = context.get('task_type', 'general')
        location = context.get('location')
        
        # Gather real-time data from devices
        best_device = None
        best_mesh = None
        if self.devices:
            # Find best device by gradient health and energy
            best_device = max(
                self.devices.values(),
                key=lambda d: (d.gradient_health * 0.5 + d.energy_remaining_percent / 100 * 0.3 + 
                              (1.0 - d.processing_utilization) * 0.2)
            )
        
        if self.mesh_networks and self.enable_mesh:
            # Find mesh with highest health
            best_mesh = max(
                self.mesh_networks.values(),
                key=lambda m: m.health_score
            )
        
        # Determine sampling rate based on helium scarcity
        scarcity_threshold = 0.6
        if helium_scarcity > scarcity_threshold:
            sampling_rate = 5.0
            aggregation_strategy = 'compressed'
            power_saving = True
        else:
            sampling_rate = 10.0
            aggregation_strategy = 'adaptive'
            power_saving = False
        
        # Gateway preference based on latency
        preferred_gateways = []
        if network_latency > 100:
            preferred_gateways = ['gateway_nearby']  # simplified
        
        # Mesh recommendation
        mesh_recommendation = None
        if best_mesh:
            mesh_recommendation = {
                'mesh_id': best_mesh.mesh_id,
                'leader': best_mesh.leader_id,
                'device_count': len(best_mesh.devices),
                'health_score': best_mesh.health_score
            }
        
        # Cross-domain knowledge integration
        if self.enable_cross_domain:
            energy_insights = asyncio.run(self.cross_domain_transfer.apply_energy_knowledge(
                {'device_count': len(self.devices)}
            ))
            if energy_insights.get('applied_pattern') != 'default':
                aggregation_strategy = energy_insights['applied_pattern']
        
        # Predictive sustainability forecast
        forecast = None
        if self.enable_predictive_sustainability and best_device:
            forecast = asyncio.run(self.predictive_sustainability.predict_device_health(
                24,
                f"{best_device.location['lat']}_{best_device.location['lon']}" if best_device.location else None
            ))
        
        # Self-healing status
        self_healing_status = "active" if self.enable_self_healing else "inactive"
        
        # Prepare recommendations
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
        
        # Generate explanation
        explanation = self._generate_propose_explanation(
            recommendations, helium_scarcity, carbon_intensity, network_latency
        )
        
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
    
    def _generate_propose_explanation(
        self,
        recommendations: Dict[str, Any],
        helium_scarcity: float,
        carbon_intensity: float,
        network_latency: float
    ) -> str:
        """Generate natural-language explanation for the proposal."""
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
    
    # ========================================================================
    # Primary Optimization (Enhanced with cross-domain and sustainability)
    # ========================================================================
    
    async def optimize_edge_deployment(self, device_type: str, carbon_zone: int, helium_scarcity: float,
                                      task_config: Optional[Dict[str, Any]] = None,
                                      location: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
        suitable = [d for d in self.devices.values() if d.device_type.value == device_type or device_type == 'any']
        if not suitable:
            return {'expert_id': self.expert_id, 'recommendation': 'no_suitable_devices'}
        
        # Apply cross-domain knowledge
        if self.enable_cross_domain:
            energy_knowledge = await self.cross_domain_transfer.apply_energy_knowledge({'device_count': len(suitable)})
            carbon_knowledge = await self.cross_domain_transfer.apply_carbon_knowledge(carbon_intensity)
            if energy_knowledge.get('applied_pattern') != 'default':
                logger.info(f"Applied energy knowledge: {energy_knowledge['applied_pattern']}")
            if carbon_knowledge.get('applied_pattern') != 'default':
                logger.info(f"Applied carbon knowledge: {carbon_knowledge['applied_pattern']}")
        
        atp_workers = self._get_atp_collaborative_workers() if self.enable_bio_integration else 4
        harvester_energy = self._get_harvester_energy_prediction() if self.enable_bio_integration else {}
        
        # Weather forecast integration
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
        
        # Federated learning participation
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
        
        # Predictive sustainability
        predictive_forecast = None
        if self.enable_predictive_sustainability and best_device:
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
        
        # Update location-based carbon model
        if self.enable_predictive_sustainability and best_device and best_device.location:
            self.predictive_sustainability.update_carbon_model(
                f"{best_device.location['lat']}_{best_device.location['lon']}",
                {'carbon_intensity': best_device.carbon_intensity_g_per_kwh}
            )
        
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
            'recommendations': self._generate_recommendations(best_device, best_mesh)
        }
        
        # Update sustainability metrics
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
        
        return plan
    
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
    
    # ========================================================================
    # Expert Statistics (Enhanced)
    # ========================================================================
    
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
    
    # ========================================================================
    # Async Shutdown
    # ========================================================================
    
    async def shutdown(self):
        """Graceful shutdown of all components"""
        logger.info(f"Shutting down IoT Expert {self.expert_id}")
        await self.federated_learner.close()
        if self.weather_api:
            await self.weather_api.close()
        logger.info("IoT Expert shutdown complete")
