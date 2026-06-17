# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/iot_expert.py
# Complete enhanced file with digital twin simulation and mesh explanations

"""
Enhanced IoT Expert v5.0.0 - Complete Metabolic Edge Decomposer
With Digital Twin Simulation, What-If Analysis, and Natural Language Explanations
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
# Enums
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

class MeshRole(Enum):
    LEADER = "leader"; ROUTER = "router"; LEAF = "leaf"; MEMBRANE_GATED = "membrane_gated"

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
            score = (device.available_processing_flops / 1e9 * 0.3 + len(device.connections) / 10 * 0.2 +
                    device.energy_remaining_percent / 100 * 0.2 + device.gradient_health * 0.3)
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
            'membrane_permeability': self.membrane_permeability
        }

# ============================================================================
# Enhanced IoT Expert
# ============================================================================
class IoTExpert:
    """Enhanced IoT Expert v5.0.0 with Digital Twin and Explanations"""
    
    def __init__(self, expert_id: str = "iot_expert_v5", enable_mesh: bool = True,
                 enable_collaborative: bool = True, enable_offline: bool = True,
                 enable_energy_harvesting: bool = True, enable_bio_integration: bool = True):
        self.expert_id = expert_id
        self.version = "5.0.0"
        self.enable_mesh = enable_mesh
        self.enable_collaborative = enable_collaborative
        self.enable_offline = enable_offline
        self.enable_energy_harvesting = enable_energy_harvesting
        self.enable_bio_integration = enable_bio_integration and BIO_INSPIRED_AVAILABLE
        
        # Bio-inspired references
        self.token_manager = None; self.gradient_manager = None; self.scheduler = None
        self.compartment_manager = None; self.biomass_storage = None; self.harvester = None
        
        self.mesh_networks: Dict[str, MeshNetwork] = {}
        self.devices: Dict[str, EdgeDevice] = {}
        self.biomass_offline_tokens: Dict[str, str] = {}
        self.total_tasks_processed = 0
        self.total_energy_harvested_kwh = 0.0
        self.total_ecoatp_saved = 0.0
        self.simulation_results: deque = deque(maxlen=500)
        
        logger.info(f"IoT Expert v{self.version} initialized")
    
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
                if perm == MembranePermeability.PERMEABLE: return MeshRole.LEADER
                elif perm == MembranePermeability.SELECTIVE: return MeshRole.ROUTER
                else: return MeshRole.LEAF
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
            if trust: return trust.gradient_strength
        return 0.7
    
    def _get_gradient_levels(self) -> Dict[str, float]:
        if self.gradient_manager: return self.gradient_manager.get_field_strengths()
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}
    
    # ========================================================================
    # Device Registration
    # ========================================================================
    def register_device(self, device_id: str, device_type: DeviceType, capabilities: Dict[str, float],
                       location: Optional[Dict[str, float]] = None, mesh_id: Optional[str] = None) -> EdgeDevice:
        mesh_role = self._get_membrane_mesh_role(device_id) if self.enable_bio_integration else MeshRole.LEAF
        gradient_health = self._get_gradient_device_health(device_id) if self.enable_bio_integration else 0.7
        
        device = EdgeDevice(device_id=device_id, device_type=device_type, capabilities=capabilities,
                           location=location, mesh_id=mesh_id, mesh_role=mesh_role,
                           gradient_health=gradient_health)
        self.devices[device_id] = device
        if mesh_id and self.enable_mesh:
            if mesh_id not in self.mesh_networks:
                self.mesh_networks[mesh_id] = MeshNetwork(mesh_id=mesh_id)
            self.mesh_networks[mesh_id].add_device(device)
        return device
    
    def create_mesh(self, mesh_id: str, device_ids: List[str]) -> MeshNetwork:
        mesh = MeshNetwork(mesh_id=mesh_id)
        for device_id in device_ids:
            if device_id in self.devices: mesh.add_device(self.devices[device_id])
        for i, d1 in enumerate(device_ids):
            for d2 in device_ids[i+1:]:
                if d1 in self.devices and d2 in self.devices:
                    mesh.add_connection(d1, d2, np.random.uniform(0.5, 1.0), np.random.uniform(10, 100), np.random.uniform(1, 50))
        mesh.elect_leader()
        self.mesh_networks[mesh_id] = mesh
        return mesh
    
    # ========================================================================
    # Digital Twin Simulation
    # ========================================================================
    def simulate_mesh_scenario(self, scenario: Dict[str, Any]) -> Dict[str, Any]:
        scenario_type = scenario.get('type', 'device_failure')
        params = scenario.get('parameters', {})
        result = {'scenario_type': scenario_type, 'parameters': params, 'timestamp': datetime.utcnow().isoformat(), 'predictions': {}}
        
        if scenario_type == 'device_failure':
            result['predictions'] = self._simulate_device_failure(params)
        elif scenario_type == 'network_partition':
            result['predictions'] = self._simulate_network_partition(params)
        elif scenario_type == 'load_spike':
            result['predictions'] = self._simulate_load_spike(params)
        elif scenario_type == 'energy_depletion':
            result['predictions'] = self._simulate_energy_depletion(params)
        
        self.simulation_results.append(result)
        return result
    
    def _simulate_device_failure(self, params: Dict) -> Dict[str, Any]:
        device_id = params.get('device_id', 'unknown')
        mesh_id = params.get('mesh_id')
        if mesh_id and mesh_id in self.mesh_networks:
            mesh = self.mesh_networks[mesh_id]
            device = mesh.devices.get(device_id)
            if not device: return {'error': 'Device not found'}
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
            
            return {
                'device_id': device_id, 'role': role.value, 'impact_level': impact,
                'estimated_recovery_time_seconds': recovery_time, 'affected_devices': affected[:5],
                'affected_count': len(affected), 'leader_election_needed': role == MeshRole.LEADER,
                'recommendation': f"Pre-designate backup leader. Recovery: {recovery_time:.1f}s, {len(affected)} affected."
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
            return {
                'total_devices': total, 'partitioned_devices': partitioned,
                'connected_devices': total - partitioned, 'predicted_offline_tasks': offline_tasks,
                'biomass_storage_needed_ecoatp': biomass_needed,
                'recovery_time_seconds': partitioned * 0.5,
                'recommendation': f"Pre-allocate {biomass_needed:.1f} Eco-ATP for biomass. Connected: {total - partitioned} devices."
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
            return {
                'spike_factor': spike, 'total_processing_deficit_flops': deficit,
                'overloaded_devices': len(overloaded), 'overloaded_details': overloaded[:5],
                'estimated_queue_delay_ms': len(overloaded) * 50,
                'recommendation': f"{'URGENT: ' if deficit > 0 else ''}Deficit: {deficit:.0f} FLOPS. {len(overloaded)} devices overloaded."
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
            return {
                'depletion_rate': rate,
                'devices_at_risk': len([d for d in depletion_times.values() if not d['can_sustain']]),
                'critical_devices': [{'device_id': did, 'time_hours': f"{d['time_to_deplete_hours']:.1f}"} for did, d in critical],
                'estimated_survival_hours': min(d['time_to_deplete_hours'] for _, d in critical) if critical else float('inf'),
                'recommendation': f"{len(critical)} devices at risk. Reduce load or boost harvesting."
            }
        return {'error': 'Mesh not found'}
    
    # ========================================================================
    # Natural Language Explanations
    # ========================================================================
    def explain_mesh_topology(self, mesh_id: str) -> Dict[str, Any]:
        if mesh_id not in self.mesh_networks: return {'error': 'Mesh not found'}
        mesh = self.mesh_networks[mesh_id]
        stats = mesh.get_mesh_statistics()
        health = "Network is fully connected and operational." if stats.get('is_connected') else "WARNING: Network has disconnected segments."
        
        devices_by_role = defaultdict(int)
        for d in mesh.devices.values(): devices_by_role[d.mesh_role.value] += 1
        
        explanation = {
            'mesh_id': mesh_id, 'health_assessment': health,
            'statistics': {'total_devices': stats.get('device_count', 0), 'leader': mesh.leader_id,
                          'average_gradient_health': f"{stats.get('average_gradient_health', 0):.2f}"},
            'device_roles': dict(devices_by_role), 'vulnerabilities': [], 'recommendations': []
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
        
        return explanation
    
    def compare_deployment_strategies(self, task: Dict[str, Any]) -> Dict[str, Any]:
        strategies = [
            {'name': 'Single Device', 'type': 'single', 'latency': 15.0, 'energy': 0.005, 'reliability': 0.90},
            {'name': 'Mesh Collaborative', 'type': 'mesh', 'latency': 10.0, 'energy': 0.003, 'reliability': 0.95},
            {'name': 'Cloud Offload', 'type': 'cloud', 'latency': 50.0, 'energy': 0.001, 'reliability': 0.99}
        ]
        results = [{'strategy': s['name'], 'predicted_latency_ms': s['latency'],
                    'predicted_energy_kwh': s['energy'], 'ecoatp_cost': s['energy'] * 1000} for s in strategies]
        best = min(results, key=lambda r: r['ecoatp_cost'])
        return {
            'task_type': task.get('task_type', 'general'), 'strategies_compared': results,
            'recommended_strategy': best['strategy'],
            'recommendation_reason': f"Lowest Eco-ATP cost ({best['ecoatp_cost']:.1f}) with acceptable latency ({best['predicted_latency_ms']:.1f}ms)"
        }
    
    # ========================================================================
    # Primary Optimization
    # ========================================================================
    async def optimize_edge_deployment(self, device_type: str, carbon_zone: int, helium_scarcity: float,
                                      task_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        suitable = [d for d in self.devices.values() if d.device_type.value == device_type or device_type == 'any']
        if not suitable: return {'expert_id': self.expert_id, 'recommendation': 'no_suitable_devices'}
        
        atp_workers = self._get_atp_collaborative_workers() if self.enable_bio_integration else 4
        harvester_energy = self._get_harvester_energy_prediction() if self.enable_bio_integration else {}
        
        best_mesh = None
        if self.enable_mesh and self.mesh_networks:
            for mesh in self.mesh_networks.values():
                if len([d for d in mesh.devices.values() if d in suitable]) >= 2:
                    best_mesh = mesh; break
        
        best_device = max(suitable, key=lambda d: (d.available_processing_flops * 0.3 + d.energy_remaining_percent / 100 * 0.2 +
                                                    d.gradient_health * 0.3 + (1.0 - d.processing_utilization) * 0.2))
        
        return {
            'expert_id': self.expert_id, 'version': self.version,
            'strategy': 'bio_mesh_collaborative' if best_mesh and self.enable_bio_integration else 'single_device',
            'primary_device': best_device.device_id, 'mesh_id': best_mesh.mesh_id if best_mesh else None,
            'mesh_size': len(best_mesh.devices) if best_mesh else 1,
            'estimated_carbon_kg': best_device.carbon_per_operation_g / 1000,
            'energy_remaining_percent': best_device.energy_remaining_percent,
            'can_operate_indefinitely': best_device.can_operate_indefinitely,
            'bio_integration_active': self.enable_bio_integration,
            'gradient_health': best_device.gradient_health,
            'atp_workers': atp_workers,
            'harvester_energy_kw': harvester_energy.get('total_kw', 0),
            'gradient_levels': self._get_gradient_levels() if self.enable_bio_integration else {},
            'recommendations': self._generate_recommendations(best_device, best_mesh)
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
        return recs if recs else ["Deployment configuration is optimal."]
    
    def get_expert_statistics(self) -> Dict[str, Any]:
        return {
            'expert_id': self.expert_id, 'version': self.version,
            'total_devices': len(self.devices), 'mesh_networks': len(self.mesh_networks),
            'bio_integration_active': self.enable_bio_integration,
            'average_gradient_health': np.mean([d.gradient_health for d in self.devices.values()]) if self.devices else 0,
            'gradient_levels': self._get_gradient_levels() if self.enable_bio_integration else {},
            'harvester_energy': self._get_harvester_energy_prediction() if self.enable_bio_integration else {},
            'simulation_count': len(self.simulation_results)
        }
    
    def get_device_status(self) -> Dict[str, Any]:
        return {did: {'type': d.device_type.value, 'online': d.is_online,
                      'battery_percent': d.energy_remaining_percent, 'mesh_role': d.mesh_role.value,
                      'gradient_health': d.gradient_health, 'token_balance': d.token_balance}
                for did, d in self.devices.items()}
