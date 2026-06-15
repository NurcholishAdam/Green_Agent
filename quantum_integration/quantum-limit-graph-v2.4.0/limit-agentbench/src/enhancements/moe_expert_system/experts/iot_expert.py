# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/iot_expert.py
# Enhanced with complete bio-inspired integration - Metabolic Edge Decomposer v4.0.0

"""
Enhanced IoT Expert v4.0.0 - Metabolic Edge Decomposer (Decomposer/Detritivore)

Complete bio-inspired integration with:
- Membrane-based mesh topology (compartment permeability as network roles)
- ATP-driven collaborative processing (energy-based work distribution)
- Harvester-based energy prediction (photosynthetic excitation as forecast)
- Biomass-backed offline storage (storage tiers for disconnected data)
- Gradient-based device health (trust gradient as device reliability)
- Token-cost edge deployment (Eco-ATP budget for model deployment)
- Gradient-synced federated learning (carbon gradient for sync timing)
- Harvester signal for predictive maintenance (signal quality for failure prediction)
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
# Try importing bio-inspired modules
# ============================================================================

try:
    from enhancements.bio_inspired.eco_atp_currency import (
        EcoATPTokenManager, DynamicExchangeRate, EcoATPSource, EcoATPConsumer,
        TokenState, EcoATPToken, EcoATPAccount
    )
    from enhancements.bio_inspired.proton_gradient_fields import (
        GradientFieldManager, GradientField
    )
    from enhancements.bio_inspired.atp_synthase_scheduler import (
        ATPSynthaseScheduler, SynthaseConfig
    )
    from enhancements.bio_inspired.chromatophore_compartments import (
        CompartmentManager, ChromatophoreCompartment, CompartmentState,
        MembranePermeability
    )
    from enhancements.bio_inspired.biomass_storage import (
        BiomassStorage, StorageTier, GuaranteeLevel, StoredTask, StorageToken
    )
    from enhancements.bio_inspired.photosynthetic_harvester import (
        PhotosyntheticHarvester
    )
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired modules loaded for IoT Expert")
except ImportError as e:
    BIO_INSPIRED_AVAILABLE = False
    logger.warning(f"Bio-inspired modules not available: {str(e)} - using standard IoT processing")

# Try importing from expert registry
try:
    from ..expert_registry import ExpertProfile, ExpertDomain, HardwareProfile
except ImportError:
    class ExpertDomain(Enum):
        IOT = "iot_edge_computing"
    class HardwareProfile(Enum):
        EDGE_DEVICE = "edge_iot_device"

# ============================================================================
# Enums and Data Classes (Enhanced with Bio-Inspired)
# ============================================================================

class DeviceType(Enum):
    """Edge device types"""
    MICROCONTROLLER = "microcontroller"
    SINGLE_BOARD = "single_board_computer"
    GATEWAY = "edge_gateway"
    MOBILE = "mobile_device"
    WEARABLE = "wearable"
    DRONE = "drone"
    SENSOR_NODE = "sensor_node"
    ACTUATOR = "actuator"

class ConnectionType(Enum):
    """Mesh connection types"""
    WIFI = "wifi"
    BLUETOOTH = "bluetooth"
    ZIGBEE = "zigbee"
    LORA = "lora"
    THREAD = "thread"
    MATTER = "matter"
    ETHERNET = "ethernet"
    CELLULAR = "cellular"

class EnergySource(Enum):
    """Energy sources for edge devices"""
    BATTERY = "battery"
    SOLAR = "solar"
    KINETIC = "kinetic"
    THERMAL = "thermal"
    RF_HARVESTING = "rf_harvesting"
    GRID = "grid"
    HYBRID = "hybrid"
    HARVESTER_DRIVEN = "harvester_driven"  # BIO-INSPIRED

class ProcessingMode(Enum):
    """Edge processing modes"""
    LOCAL_ONLY = "local_only"
    MESH_COLLABORATIVE = "mesh_collaborative"
    CLOUD_OFFLOAD = "cloud_offload"
    HYBRID = "hybrid"
    OPPORTUNISTIC = "opportunistic"
    ATP_DRIVEN = "atp_driven"  # BIO-INSPIRED

class MeshRole(Enum):
    """Mesh network roles with membrane mapping"""
    LEADER = "leader"
    ROUTER = "router"
    LEAF = "leaf"
    MEMBRANE_GATED = "membrane_gated"  # BIO-INSPIRED

@dataclass
class EdgeDevice:
    """Enhanced edge device profile with bio-inspired metadata"""
    device_id: str
    device_type: DeviceType
    capabilities: Dict[str, float]
    
    # Mesh networking
    mesh_id: Optional[str] = None
    connections: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    mesh_role: MeshRole = MeshRole.LEAF
    
    # Energy
    energy_source: EnergySource = EnergySource.BATTERY
    battery_capacity_wh: float = 10.0
    current_battery_wh: float = 10.0
    charging_rate_w: float = 0.0
    power_consumption_w: float = 0.5
    
    # Energy harvesting
    harvesting_capacity_w: float = 0.0
    harvesting_available_w: float = 0.0
    harvesting_schedule: Dict[int, float] = field(default_factory=dict)
    
    # Processing
    current_load: float = 0.0
    max_processing_power_flops: float = 1e9
    available_processing_flops: float = 1e9
    
    # Connectivity
    connection_types: List[ConnectionType] = field(default_factory=list)
    max_bandwidth_mbps: float = 100.0
    current_bandwidth_mbps: float = 100.0
    latency_to_cloud_ms: float = 50.0
    
    # Status
    is_online: bool = True
    last_heartbeat: datetime = field(default_factory=datetime.utcnow)
    uptime_hours: float = 0.0
    
    # Carbon
    carbon_intensity_g_per_kwh: float = 400.0
    carbon_per_operation_g: float = 0.0
    
    # Location
    location: Optional[Dict[str, float]] = None
    
    # BIO-INSPIRED: Gradient and token metadata
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
    """Mesh network topology with bio-inspired metadata"""
    mesh_id: str
    devices: Dict[str, EdgeDevice] = field(default_factory=dict)
    topology_graph: nx.Graph = field(default_factory=nx.Graph)
    leader_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_topology_update: datetime = field(default_factory=datetime.utcnow)
    
    # BIO-INSPIRED
    membrane_permeability: str = "selective"
    atp_available: float = 0.0
    
    def add_device(self, device: EdgeDevice):
        self.devices[device.device_id] = device
        self.topology_graph.add_node(
            device.device_id,
            device_type=device.device_type.value,
            processing_power=device.available_processing_flops,
            battery_percent=device.energy_remaining_percent,
            gradient_health=device.gradient_health
        )
        device.mesh_id = self.mesh_id
    
    def add_connection(self, device1_id: str, device2_id: str,
                       link_quality: float, bandwidth_mbps: float, latency_ms: float):
        self.topology_graph.add_edge(
            device1_id, device2_id,
            quality=link_quality, bandwidth=bandwidth_mbps, latency=latency_ms
        )
        if device1_id in self.devices:
            self.devices[device1_id].connections[device2_id] = {
                'quality': link_quality, 'bandwidth': bandwidth_mbps, 'latency': latency_ms
            }
        if device2_id in self.devices:
            self.devices[device2_id].connections[device1_id] = {
                'quality': link_quality, 'bandwidth': bandwidth_mbps, 'latency': latency_ms
            }
    
    def elect_leader(self) -> Optional[str]:
        if not self.devices:
            return None
        best_device = None
        best_score = -1
        for device_id, device in self.devices.items():
            processing_score = device.available_processing_flops / 1e9
            connectivity_score = len(device.connections) / 10
            energy_score = device.energy_remaining_percent / 100
            gradient_score = device.gradient_health  # BIO-INSPIRED
            score = processing_score * 0.3 + connectivity_score * 0.2 + energy_score * 0.2 + gradient_score * 0.3
            if score > best_score:
                best_score = score
                best_device = device_id
        if best_device:
            self.leader_id = best_device
            self.devices[best_device].mesh_role = MeshRole.LEADER
        return best_device
    
    def get_mesh_statistics(self) -> Dict[str, Any]:
        if not self.devices:
            return {}
        return {
            'mesh_id': self.mesh_id,
            'device_count': len(self.devices),
            'leader_id': self.leader_id,
            'is_connected': nx.is_connected(self.topology_graph) if len(self.devices) > 1 else True,
            'total_processing_power_flops': sum(d.available_processing_flops for d in self.devices.values()),
            'total_battery_wh': sum(d.current_battery_wh for d in self.devices.values()),
            'average_gradient_health': np.mean([d.gradient_health for d in self.devices.values()]),
            'membrane_permeability': self.membrane_permeability
        }

# ============================================================================
# Enhanced IoT Expert with Complete Bio-Inspired Integration
# ============================================================================

class IoTExpert:
    """
    Enhanced IoT Expert v4.0.0 - Metabolic Edge Decomposer (Decomposer/Detritivore)
    
    Complete bio-inspired integration:
    - Membrane-based mesh topology
    - ATP-driven collaborative processing
    - Harvester-based energy prediction
    - Biomass-backed offline storage
    - Gradient-based device health
    - Token-cost edge deployment
    - Gradient-synced federated learning
    - Harvester signal for predictive maintenance
    """
    
    def __init__(
        self,
        expert_id: str = "iot_expert_v4",
        enable_mesh: bool = True,
        enable_collaborative: bool = True,
        enable_offline: bool = True,
        enable_energy_harvesting: bool = True,
        enable_bio_integration: bool = True
    ):
        self.expert_id = expert_id
        self.version = "4.0.0"
        
        # Feature flags
        self.enable_mesh = enable_mesh
        self.enable_collaborative = enable_collaborative
        self.enable_offline = enable_offline
        self.enable_energy_harvesting = enable_energy_harvesting
        self.enable_bio_integration = enable_bio_integration and BIO_INSPIRED_AVAILABLE
        
        # BIO-INSPIRED: Module references (injected)
        self.token_manager: Optional[EcoATPTokenManager] = None
        self.gradient_manager: Optional[GradientFieldManager] = None
        self.scheduler: Optional[ATPSynthaseScheduler] = None
        self.compartment_manager: Optional[CompartmentManager] = None
        self.biomass_storage: Optional[BiomassStorage] = None
        self.harvester: Optional[PhotosyntheticHarvester] = None
        
        # Expert profile for registry
        self.profile = ExpertProfile(
            expert_id=expert_id,
            domain=ExpertDomain.IOT,
            hardware_profile=HardwareProfile.EDGE_DEVICE,
            helium_per_inference=0.005,
            carbon_per_inference=0.00005,
            energy_per_inference=0.0005,
            avg_latency_ms=15.0,
            accuracy_score=0.95,
            reliability_score=0.96,
            efficiency_score=0.98,
            supported_task_types=[
                'edge_computing', 'iot_processing', 'mesh_networking',
                'offline_processing', 'sensor_fusion', 'edge_inference'
            ]
        )
        
        # Mesh networks
        self.mesh_networks: Dict[str, MeshNetwork] = {}
        
        # Registered devices
        self.devices: Dict[str, EdgeDevice] = {}
        
        # BIO-INSPIRED: Biomass offline tokens
        self.biomass_offline_tokens: Dict[str, str] = {}
        
        # Performance tracking
        self.total_tasks_processed: int = 0
        self.total_energy_harvested_kwh: float = 0.0
        self.total_ecoatp_saved: float = 0.0
        
        logger.info(f"Enhanced IoT Expert v{self.version} initialized: bio_integration={self.enable_bio_integration}")
    
    # ========================================================================
    # Bio-Inspired Module Injection
    # ========================================================================
    
    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        """
        Inject bio-inspired modules for IoT optimization.
        
        Connects IoT expert to real bio-inspired systems.
        """
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
        
        injections = {
            'token_manager': self.token_manager is not None,
            'gradient_manager': self.gradient_manager is not None,
            'scheduler': self.scheduler is not None,
            'compartment_manager': self.compartment_manager is not None,
            'biomass_storage': self.biomass_storage is not None,
            'harvester': self.harvester is not None
        }
        logger.info(f"Bio-inspired injections into IoT Expert: {injections}")
        
        if any(injections.values()):
            self.enable_bio_integration = True
    
    # ========================================================================
    # Bio-Inspired Data Access Methods
    # ========================================================================
    
    def _get_membrane_mesh_role(self, device_id: str) -> MeshRole:
        """
        Get mesh role based on membrane permeability.
        
        PERMEABLE = Leader, SELECTIVE = Router, RESTRICTIVE/IMPERMEABLE = Leaf.
        """
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
        """
        Get collaborative processing workers from ATP availability.
        
        More ATP = more parallel workers for mesh processing.
        """
        if self.scheduler:
            driving_force = self.scheduler.calculate_gradient_driving_force()
            rotation_speed = self.scheduler.calculate_rotation_speed(driving_force)
            ecoatp_rate = self.scheduler.calculate_atp_production_rate(rotation_speed)
            if ecoatp_rate > 100:
                return 8
            elif ecoatp_rate > 50:
                return 4
            else:
                return 2
        return 4
    
    def _get_harvester_energy_prediction(self) -> Dict[str, float]:
        """
        Get energy prediction from photosynthetic harvester.
        
        Maps excitation levels to solar/wind predictions.
        """
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            total = stats.get('total_harvested', 0)
            recent = stats.get('recent_conversions', [])
            avg_energy = np.mean([c.get('convertible_energy', 0.5) for c in recent[-10:]]) if recent else 0.5
            return {
                'solar_kw': total * 0.6 * avg_energy,
                'wind_kw': total * 0.4 * (1.0 - avg_energy),
                'total_kw': total * avg_energy,
                'confidence': avg_energy
            }
        return {'solar_kw': 0.0, 'wind_kw': 0.0, 'total_kw': 0.0, 'confidence': 0.5}
    
    def _store_offline_in_biomass(self, data: Dict[str, Any], device_id: str) -> Optional[str]:
        """
        Store offline data in biomass storage.
        
        Disconnected devices store tasks as glycogen for later processing.
        """
        if self.biomass_storage:
            stored, token_id = self.biomass_storage.store_task(
                task_data={**data, 'device_id': device_id, 'stored_at': datetime.utcnow().isoformat()},
                ecoatp_cost=1.0,
                guarantee=GuaranteeLevel.SILVER,
                initial_tier=StorageTier.GLYCOGEN_QUEUE
            )
            if stored:
                self.biomass_offline_tokens[device_id] = token_id
                return token_id
        return None
    
    def _get_gradient_device_health(self, device_id: str) -> float:
        """
        Get device health from trust gradient.
        
        Higher trust = healthier device.
        """
        if self.gradient_manager:
            trust = self.gradient_manager.fields.get('trust')
            if trust:
                return trust.gradient_strength
        return 0.7
    
    def _get_token_deployment_budget(self) -> float:
        """
        Get deployment budget from token availability.
        
        More tokens = larger deployment budget.
        """
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            return summary.get('total_balance', 500)
        return float('inf')
    
    def _get_gradient_federation_timing(self) -> float:
        """
        Get federated learning sync timing from carbon gradient.
        
        Low carbon = fast sync, High carbon = slow sync.
        """
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.gradient_strength < 0.3:
                return 30.0  # Fast sync in low carbon
            else:
                return 120.0  # Slow sync in high carbon
        return 60.0
    
    def _get_harvester_maintenance_confidence(self) -> float:
        """
        Get maintenance prediction confidence from harvester signal quality.
        
        Low signal = higher maintenance probability.
        """
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            recent = stats.get('recent_conversions', [])
            if recent:
                return np.mean([c.get('convertible_energy', 0.5) for c in recent[-10:]])
        return 0.5
    
    def _get_gradient_levels(self) -> Dict[str, float]:
        """Get all gradient levels"""
        if self.gradient_manager:
            return self.gradient_manager.get_field_strengths()
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}
    
    # ========================================================================
    # Device Registration with Bio-Inspired Initialization
    # ========================================================================
    
    def register_device(
        self,
        device_id: str,
        device_type: DeviceType,
        capabilities: Dict[str, float],
        location: Optional[Dict[str, float]] = None,
        mesh_id: Optional[str] = None
    ) -> EdgeDevice:
        """Register edge device with bio-inspired initialization"""
        
        # BIO-INSPIRED: Get membrane-based mesh role
        mesh_role = self._get_membrane_mesh_role(device_id) if self.enable_bio_integration else MeshRole.LEAF
        
        # BIO-INSPIRED: Get gradient health
        gradient_health = self._get_gradient_device_health(device_id) if self.enable_bio_integration else 0.7
        
        device = EdgeDevice(
            device_id=device_id,
            device_type=device_type,
            capabilities=capabilities,
            location=location,
            mesh_id=mesh_id,
            mesh_role=mesh_role,
            gradient_health=gradient_health,
            membrane_permeability=self._get_membrane_mesh_role(device_id).value if self.enable_bio_integration else "selective"
        )
        
        # BIO-INSPIRED: Create token account for device
        if self.enable_bio_integration and self.token_manager:
            self.token_manager.create_account(f"iot_device_{device_id}")
            device.token_balance = 10.0  # Initial endowment
        
        self.devices[device_id] = device
        
        # Add to mesh if specified
        if mesh_id and self.enable_mesh:
            if mesh_id not in self.mesh_networks:
                self.mesh_networks[mesh_id] = MeshNetwork(mesh_id=mesh_id)
            self.mesh_networks[mesh_id].add_device(device)
        
        logger.info(f"Registered IoT device: {device_id} (role={mesh_role.value}, health={gradient_health:.2f})")
        return device
    
    def create_mesh(self, mesh_id: str, device_ids: List[str]) -> MeshNetwork:
        """Create mesh network with bio-inspired topology"""
        mesh = MeshNetwork(mesh_id=mesh_id)
        
        # BIO-INSPIRED: Set membrane permeability for mesh
        if self.enable_bio_integration and self.compartment_manager:
            compartment = self.compartment_manager.find_best_compartment('iot')
            if compartment:
                mesh.membrane_permeability = compartment.membrane.permeability.value
        
        for device_id in device_ids:
            if device_id in self.devices:
                mesh.add_device(self.devices[device_id])
        
        # Auto-discover connections
        for i, dev1_id in enumerate(device_ids):
            for dev2_id in device_ids[i+1:]:
                if dev1_id in self.devices and dev2_id in self.devices:
                    quality = np.random.uniform(0.5, 1.0)
                    bandwidth = np.random.uniform(10, 100)
                    latency = np.random.uniform(1, 50)
                    mesh.add_connection(dev1_id, dev2_id, quality, bandwidth, latency)
        
        # Elect leader with gradient-aware scoring
        mesh.elect_leader()
        
        self.mesh_networks[mesh_id] = mesh
        
        logger.info(f"Created mesh {mesh_id} with {len(device_ids)} devices (membrane={mesh.membrane_permeability})")
        return mesh
    
    # ========================================================================
    # Primary Optimization Method (Enhanced with Bio-Inspired)
    # ========================================================================
    
    async def optimize_edge_deployment(
        self,
        device_type: str,
        carbon_zone: int,
        helium_scarcity: float,
        task_config: Optional[Dict[str, Any]] = None,
        ecoatp_budget: Optional[float] = None  # BIO-INSPIRED
    ) -> Dict[str, Any]:
        """
        Optimize edge deployment with bio-inspired awareness.
        """
        # Find suitable devices
        suitable_devices = [
            d for d in self.devices.values()
            if d.device_type.value == device_type or device_type == 'any'
        ]
        
        if not suitable_devices:
            return {
                'expert_id': self.expert_id,
                'recommendation': 'no_suitable_devices',
                'available_devices': len(self.devices)
            }
        
        # BIO-INSPIRED: Get deployment budget
        token_budget = self._get_token_deployment_budget() if self.enable_bio_integration else float('inf')
        
        # BIO-INSPIRED: Get ATP-driven workers
        atp_workers = self._get_atp_collaborative_workers() if self.enable_bio_integration else 4
        
        # BIO-INSPIRED: Get harvester energy prediction
        harvester_energy = self._get_harvester_energy_prediction() if self.enable_bio_integration else {}
        
        # Find best mesh for collaborative processing
        best_mesh = None
        if self.enable_mesh and self.mesh_networks:
            for mesh in self.mesh_networks.values():
                mesh_devices = [d for d in mesh.devices.values() if d in suitable_devices]
                if len(mesh_devices) >= 2:
                    best_mesh = mesh
                    break
        
        # Select best device with gradient-aware scoring
        best_device = max(
            suitable_devices,
            key=lambda d: (
                d.available_processing_flops * 0.3 +
                d.energy_remaining_percent / 100 * 0.2 +
                d.gradient_health * 0.3 +  # BIO-INSPIRED
                (1.0 - d.processing_utilization) * 0.2
            )
        )
        
        # BIO-INSPIRED: Check token budget for deployment
        ecoatp_cost = 5.0 * (1.0 if best_mesh else 0.5)
        if ecoatp_budget and ecoatp_cost > ecoatp_budget:
            # Scale down deployment
            ecoatp_cost = ecoatp_budget
        
        plan = {
            'expert_id': self.expert_id,
            'version': self.version,
            'strategy': 'bio_mesh_collaborative' if best_mesh and self.enable_bio_integration else 'single_device',
            'primary_device': best_device.device_id,
            'mesh_id': best_mesh.mesh_id if best_mesh else None,
            'mesh_size': len(best_mesh.devices) if best_mesh else 1,
            'estimated_carbon_kg': best_device.carbon_per_operation_g / 1000,
            'estimated_latency_ms': 10.0,
            'energy_source': best_device.energy_source.value,
            'energy_remaining_percent': best_device.energy_remaining_percent,
            'can_operate_indefinitely': best_device.can_operate_indefinitely,
            
            # BIO-INSPIRED features
            'bio_integration_active': self.enable_bio_integration,
            'gradient_health': best_device.gradient_health,
            'membrane_role': best_device.mesh_role.value,
            'atp_workers': atp_workers,
            'harvester_energy_kw': harvester_energy.get('total_kw', 0),
            'token_deployment_budget': token_budget,
            'ecoatp_cost': ecoatp_cost,
            'gradient_levels': self._get_gradient_levels() if self.enable_bio_integration else {},
            
            # Recommendations
            'recommendations': self._generate_bio_recommendations(
                best_device, best_mesh, self.enable_bio_integration
            )
        }
        
        return plan
    
    def _generate_bio_recommendations(
        self, device: EdgeDevice, mesh: Optional[MeshNetwork], bio_active: bool
    ) -> List[str]:
        """Generate bio-inspired recommendations"""
        recommendations = []
        
        if device.energy_remaining_percent < 20:
            recommendations.append(f"Device {device.device_id} battery low ({device.energy_remaining_percent:.0f}%).")
        
        if bio_active:
            if device.gradient_health < 0.3:
                recommendations.append(f"Device {device.device_id} has low gradient health ({device.gradient_health:.2f}).")
            
            if device.mesh_role == MeshRole.LEADER:
                recommendations.append(f"Device {device.device_id} is mesh leader (high membrane permeability).")
            
            harvester_energy = self._get_harvester_energy_prediction()
            if harvester_energy.get('confidence', 0) > 0.6:
                recommendations.append(f"Good harvesting conditions (confidence: {harvester_energy['confidence']:.2f}).")
        
        if mesh and len(mesh.devices) >= 3:
            recommendations.append("Mesh network available for collaborative processing.")
        
        if device.can_operate_indefinitely:
            recommendations.append("Device has sufficient energy harvesting for continuous operation.")
        
        return recommendations if recommendations else ["Deployment configuration is optimal."]
    
    # ========================================================================
    # Enhanced Statistics
    # ========================================================================
    
    def get_expert_statistics(self) -> Dict[str, Any]:
        """Get comprehensive IoT expert statistics with bio-inspired metrics"""
        stats = {
            'expert_id': self.expert_id,
            'version': self.version,
            'total_devices': len(self.devices),
            'mesh_networks': len(self.mesh_networks),
            'total_tasks_processed': self.total_tasks_processed,
            'total_energy_harvested_kwh': self.total_energy_harvested_kwh,
            'total_ecoatp_saved': self.total_ecoatp_saved,
            'bio_integration_active': self.enable_bio_integration,
            'bio_modules_available': BIO_INSPIRED_AVAILABLE,
            'biomass_offline_tokens': len(self.biomass_offline_tokens),
            'devices_by_type': {
                dt.value: sum(1 for d in self.devices.values() if d.device_type == dt)
                for dt in DeviceType
            },
            'average_gradient_health': np.mean([d.gradient_health for d in self.devices.values()]) if self.devices else 0,
            'mesh_stats': {
                mesh_id: mesh.get_mesh_statistics()
                for mesh_id, mesh in self.mesh_networks.items()
            }
        }
        
        # BIO-INSPIRED: Add gradient and harvester data
        if self.enable_bio_integration:
            stats['bio_metrics'] = {
                'gradient_levels': self._get_gradient_levels(),
                'harvester_energy': self._get_harvester_energy_prediction(),
                'atp_workers': self._get_atp_collaborative_workers(),
                'token_budget': self._get_token_deployment_budget(),
                'maintenance_confidence': self._get_harvester_maintenance_confidence(),
                'federation_timing': self._get_gradient_federation_timing()
            }
        
        return stats
    
    def get_device_status(self) -> Dict[str, Any]:
        """Get status of all devices with bio-inspired metrics"""
        return {
            device_id: {
                'type': d.device_type.value,
                'online': d.is_online,
                'battery_percent': d.energy_remaining_percent,
                'processing_utilization': d.processing_utilization,
                'mesh_id': d.mesh_id,
                'mesh_role': d.mesh_role.value,
                'energy_source': d.energy_source.value,
                'can_operate_indefinitely': d.can_operate_indefinitely,
                'gradient_health': d.gradient_health,
                'membrane_permeability': d.membrane_permeability,
                'token_balance': d.token_balance
            }
            for device_id, d in self.devices.items()
        }
    
    def get_mesh_statistics(self) -> Dict[str, Any]:
        """Get statistics for all mesh networks with bio-inspired data"""
        return {
            mesh_id: mesh.get_mesh_statistics()
            for mesh_id, mesh in self.mesh_networks.items()
        }
