# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/iot_expert_enhanced.py

"""
Enhanced IoT Expert with Mesh Networking
Version: 2.0.0

Features:
- Mesh network topology management
- Collaborative edge processing
- Edge-aware model deployment
- Offline-first processing
- Energy harvesting awareness
- Edge-to-edge federated learning
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

logger = logging.getLogger(__name__)

# ============================================================================
# Enums and Data Classes
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

class ProcessingMode(Enum):
    """Edge processing modes"""
    LOCAL_ONLY = "local_only"
    MESH_COLLABORATIVE = "mesh_collaborative"
    CLOUD_OFFLOAD = "cloud_offload"
    HYBRID = "hybrid"
    OPPORTUNISTIC = "opportunistic"

@dataclass
class EdgeDevice:
    """Enhanced edge device profile"""
    device_id: str
    device_type: DeviceType
    capabilities: Dict[str, float]  # compute_flops, memory_gb, storage_gb
    
    # Mesh networking
    mesh_id: Optional[str] = None
    connections: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # neighbor_id -> link_info
    mesh_role: str = "node"  # node, router, leader
    
    # Energy
    energy_source: EnergySource = EnergySource.BATTERY
    battery_capacity_wh: float = 10.0
    current_battery_wh: float = 10.0
    charging_rate_w: float = 0.0
    power_consumption_w: float = 0.5
    
    # Energy harvesting
    harvesting_capacity_w: float = 0.0
    harvesting_available_w: float = 0.0
    harvesting_schedule: Dict[int, float] = field(default_factory=dict)  # hour -> watts
    
    # Processing
    current_load: float = 0.0  # 0-1
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
    location: Optional[Dict[str, float]] = None  # lat, lon, alt
    
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
    """Mesh network topology"""
    mesh_id: str
    devices: Dict[str, EdgeDevice] = field(default_factory=dict)
    topology_graph: nx.Graph = field(default_factory=nx.Graph)
    leader_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_topology_update: datetime = field(default_factory=datetime.utcnow)
    
    def add_device(self, device: EdgeDevice):
        """Add device to mesh"""
        self.devices[device.device_id] = device
        self.topology_graph.add_node(
            device.device_id,
            device_type=device.device_type.value,
            processing_power=device.available_processing_flops,
            battery_percent=device.energy_remaining_percent
        )
        device.mesh_id = self.mesh_id
    
    def add_connection(
        self,
        device1_id: str,
        device2_id: str,
        link_quality: float,
        bandwidth_mbps: float,
        latency_ms: float
    ):
        """Add connection between devices"""
        self.topology_graph.add_edge(
            device1_id, device2_id,
            quality=link_quality,
            bandwidth=bandwidth_mbps,
            latency=latency_ms
        )
        
        # Update device connection info
        if device1_id in self.devices:
            self.devices[device1_id].connections[device2_id] = {
                'quality': link_quality,
                'bandwidth': bandwidth_mbps,
                'latency': latency_ms
            }
        if device2_id in self.devices:
            self.devices[device2_id].connections[device1_id] = {
                'quality': link_quality,
                'bandwidth': bandwidth_mbps,
                'latency': latency_ms
            }
    
    def elect_leader(self) -> Optional[str]:
        """Elect mesh leader based on capabilities"""
        if not self.devices:
            return None
        
        best_device = None
        best_score = -1
        
        for device_id, device in self.devices.items():
            # Score based on processing power, connectivity, and energy
            processing_score = device.available_processing_flops / 1e9
            connectivity_score = len(device.connections) / 10
            energy_score = device.energy_remaining_percent / 100
            
            score = processing_score * 0.4 + connectivity_score * 0.3 + energy_score * 0.3
            
            if score > best_score:
                best_score = score
                best_device = device_id
        
        if best_device:
            self.leader_id = best_device
            self.devices[best_device].mesh_role = "leader"
        
        return best_device
    
    def get_optimal_route(
        self,
        source_id: str,
        target_id: str,
        metric: str = 'latency'
    ) -> Optional[List[str]]:
        """Find optimal route between devices"""
        try:
            if metric == 'latency':
                path = nx.shortest_path(
                    self.topology_graph,
                    source=source_id,
                    target=target_id,
                    weight='latency'
                )
            elif metric == 'bandwidth':
                # Invert bandwidth for shortest path (higher bandwidth = lower weight)
                path = nx.shortest_path(
                    self.topology_graph,
                    source=source_id,
                    target=target_id,
                    weight=lambda u, v, d: 1.0 / max(d.get('bandwidth', 1), 0.1)
                )
            else:
                path = nx.shortest_path(
                    self.topology_graph,
                    source=source_id,
                    target=target_id,
                    weight='quality'
                )
            
            return path
            
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return None
    
    def get_mesh_statistics(self) -> Dict[str, Any]:
        """Get mesh network statistics"""
        if not self.devices:
            return {}
        
        return {
            'mesh_id': self.mesh_id,
            'device_count': len(self.devices),
            'leader_id': self.leader_id,
            'is_connected': nx.is_connected(self.topology_graph) if len(self.devices) > 1 else True,
            'network_diameter': nx.diameter(self.topology_graph) if len(self.devices) > 1 and nx.is_connected(self.topology_graph) else 0,
            'average_path_length': nx.average_shortest_path_length(self.topology_graph) if len(self.devices) > 1 and nx.is_connected(self.topology_graph) else 0,
            'total_processing_power_flops': sum(d.available_processing_flops for d in self.devices.values()),
            'total_battery_wh': sum(d.current_battery_wh for d in self.devices.values()),
            'devices_by_type': {
                dt.value: sum(1 for d in self.devices.values() if d.device_type == dt)
                for dt in DeviceType
            },
            'average_energy_percent': np.mean([d.energy_remaining_percent for d in self.devices.values()]),
            'can_operate_indefinitely_count': sum(1 for d in self.devices.values() if d.can_operate_indefinitely)
        }

# ============================================================================
# Collaborative Edge Processing
# ============================================================================

class CollaborativeEdgeProcessor:
    """
    Manages collaborative processing across edge mesh.
    
    Features:
    - Workload partitioning across devices
    - Model parallelism
    - Data parallelism
    - Fault tolerance
    """
    
    def __init__(self):
        self.active_jobs: Dict[str, Dict[str, Any]] = {}
        self.processing_history: deque = deque(maxlen=1000)
        
        logger.info("Collaborative Edge Processor initialized")
    
    async def partition_workload(
        self,
        mesh: MeshNetwork,
        total_computation_flops: float,
        data_size_mb: float,
        latency_requirement_ms: float,
        energy_budget_wh: float
    ) -> Dict[str, Dict[str, Any]]:
        """
        Partition workload across mesh devices.
        
        Returns:
            Device assignments with computation share
        """
        # Get available devices
        available_devices = [
            d for d in mesh.devices.values()
            if d.is_online and d.available_processing_flops > 0
        ]
        
        if not available_devices:
            return {}
        
        # Sort by available processing power
        available_devices.sort(
            key=lambda d: d.available_processing_flops,
            reverse=True
        )
        
        # Calculate optimal partition
        total_available = sum(d.available_processing_flops for d in available_devices)
        
        assignments = {}
        remaining_computation = total_computation_flops
        remaining_data = data_size_mb
        
        for device in available_devices:
            if remaining_computation <= 0:
                break
            
            # Calculate share based on processing power
            share = min(
                1.0,
                device.available_processing_flops / total_available * 1.5  # Slight overallocation
            )
            
            device_computation = total_computation_flops * share
            device_data = data_size_mb * share
            
            # Check energy budget
            energy_required = device_computation / device.max_processing_power_flops * device.power_consumption_w / 3600
            if energy_required > device.current_battery_wh * 0.8:  # Max 80% battery use
                share *= 0.5
                device_computation *= 0.5
            
            # Check latency
            if device.latency_to_cloud_ms > latency_requirement_ms * 0.5:
                # Reduce share for high-latency devices
                share *= 0.7
            
            assignments[device.device_id] = {
                'computation_share': share,
                'computation_flops': device_computation,
                'data_mb': device_data,
                'estimated_energy_wh': energy_required,
                'estimated_time_ms': device_computation / max(device.available_processing_flops, 1) * 1000
            }
            
            remaining_computation -= device_computation
            remaining_data -= device_data
        
        # Distribute remaining computation if any
        if remaining_computation > 0 and assignments:
            # Add to most capable devices
            for device_id in assignments:
                extra = remaining_computation / len(assignments)
                assignments[device_id]['computation_flops'] += extra
                assignments[device_id]['computation_share'] += extra / total_computation_flops
        
        return assignments
    
    async def execute_collaborative(
        self,
        mesh: MeshNetwork,
        assignments: Dict[str, Dict[str, Any]],
        task_function: callable,
        timeout_seconds: float = 30.0
    ) -> Dict[str, Any]:
        """
        Execute task collaboratively across mesh.
        
        Uses consensus for result aggregation.
        """
        job_id = f"job_{datetime.utcnow().timestamp()}"
        
        self.active_jobs[job_id] = {
            'assignments': assignments,
            'status': 'running',
            'started_at': datetime.utcnow(),
            'results': {}
        }
        
        try:
            # Execute on each device asynchronously
            tasks = []
            for device_id, assignment in assignments.items():
                if device_id in mesh.devices:
                    task = asyncio.create_task(
                        self._execute_on_device(
                            mesh.devices[device_id],
                            assignment,
                            task_function,
                            timeout_seconds
                        )
                    )
                    tasks.append((device_id, task))
            
            # Wait for results with timeout
            results = {}
            for device_id, task in tasks:
                try:
                    result = await asyncio.wait_for(task, timeout=timeout_seconds)
                    results[device_id] = result
                except asyncio.TimeoutError:
                    logger.warning(f"Device {device_id} timed out")
                    results[device_id] = None
            
            # Aggregate results (majority voting)
            aggregated = self._aggregate_results(results)
            
            self.active_jobs[job_id]['status'] = 'completed'
            self.active_jobs[job_id]['results'] = results
            
            return {
                'job_id': job_id,
                'success': len([r for r in results.values() if r is not None]) > len(results) / 2,
                'aggregated_result': aggregated,
                'device_results': results,
                'participating_devices': len(results),
                'successful_devices': len([r for r in results.values() if r is not None])
            }
            
        except Exception as e:
            logger.error(f"Collaborative execution failed: {str(e)}")
            self.active_jobs[job_id]['status'] = 'failed'
            raise
    
    async def _execute_on_device(
        self,
        device: EdgeDevice,
        assignment: Dict[str, Any],
        task_function: callable,
        timeout: float
    ) -> Any:
        """Execute task on a single device (simulated)"""
        # Simulate device processing
        processing_time = assignment['estimated_time_ms'] / 1000
        await asyncio.sleep(min(processing_time, timeout))
        
        # Execute task function
        return await task_function(assignment)
    
    def _aggregate_results(
        self,
        results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Aggregate results from multiple devices using consensus"""
        valid_results = {
            k: v for k, v in results.items()
            if v is not None
        }
        
        if not valid_results:
            return {'error': 'No valid results'}
        
        # If results are numeric, use median (robust to outliers)
        if all(isinstance(v, (int, float)) for v in valid_results.values()):
            return {'value': np.median(list(valid_results.values()))}
        
        # If results are dicts, merge with voting
        if all(isinstance(v, dict) for v in valid_results.values()):
            merged = {}
            for key in set().union(*[v.keys() for v in valid_results.values()]):
                values = [v[key] for v in valid_results.values() if key in v]
                if values:
                    if all(isinstance(v, (int, float)) for v in values):
                        merged[key] = np.mean(values)
                    else:
                        # Most common value
                        from collections import Counter
                        merged[key] = Counter(values).most_common(1)[0][0]
            return merged
        
        # Default: return first valid result
        return list(valid_results.values())[0]

# ============================================================================
# Edge-Aware Model Deployment
# ============================================================================

class EdgeModelDeployer:
    """
    Deploys optimized models to edge devices.
    
    Features:
    - Hardware-specific model compilation
    - Adaptive quantization
    - Progressive model loading
    """
    
    def __init__(self):
        self.deployed_models: Dict[str, Dict[str, Any]] = {}
        self.compilation_cache: Dict[str, Any] = {}
        
        # Hardware-specific optimizations
        self.hardware_profiles = {
            DeviceType.MICROCONTROLLER: {
                'max_model_size_kb': 256,
                'supported_precision': ['int8', 'int4'],
                'use_tflite_micro': True
            },
            DeviceType.SINGLE_BOARD: {
                'max_model_size_mb': 100,
                'supported_precision': ['fp16', 'int8'],
                'use_onnx': True
            },
            DeviceType.GATEWAY: {
                'max_model_size_mb': 500,
                'supported_precision': ['fp32', 'fp16', 'int8'],
                'use_tensorrt': True
            },
            DeviceType.MOBILE: {
                'max_model_size_mb': 50,
                'supported_precision': ['fp16', 'int8'],
                'use_coreml': True
            }
        }
        
        logger.info("Edge Model Deployer initialized")
    
    async def deploy_model(
        self,
        device: EdgeDevice,
        model_config: Dict[str, Any],
        carbon_budget_g: float = 1.0
    ) -> Dict[str, Any]:
        """
        Deploy optimized model to edge device.
        
        Returns deployment result.
        """
        profile = self.hardware_profiles.get(
            device.device_type,
            self.hardware_profiles[DeviceType.SINGLE_BOARD]
        )
        
        # Select optimal precision
        precision = self._select_precision(device, profile)
        
        # Compress model for device
        compressed_model = await self._compress_model(
            model_config, profile, precision, carbon_budget_g
        )
        
        # Estimate performance
        performance = self._estimate_performance(
            device, compressed_model
        )
        
        deployment = {
            'device_id': device.device_id,
            'model_hash': hashlib.sha256(
                json.dumps(compressed_model, sort_keys=True).encode()
            ).hexdigest()[:16],
            'precision': precision,
            'model_size_kb': compressed_model.get('size_kb', 0),
            'estimated_latency_ms': performance['latency_ms'],
            'estimated_energy_per_inference_wh': performance['energy_wh'],
            'estimated_carbon_per_inference_g': performance['carbon_g'],
            'deployed_at': datetime.utcnow().isoformat()
        }
        
        self.deployed_models[device.device_id] = deployment
        
        logger.info(
            f"Deployed model to {device.device_id}: "
            f"{deployment['model_size_kb']:.0f}KB, "
            f"{performance['latency_ms']:.1f}ms/inference"
        )
        
        return deployment
    
    def _select_precision(
        self,
        device: EdgeDevice,
        profile: Dict[str, Any]
    ) -> str:
        """Select optimal precision based on device and energy"""
        supported = profile['supported_precision']
        
        # If battery is low, use lowest precision
        if device.energy_remaining_percent < 20:
            return supported[-1]  # Lowest precision
        
        # If harvesting, can afford higher precision
        if device.can_operate_indefinitely:
            return supported[0]  # Highest precision
        
        # Balance: use medium precision
        return supported[len(supported) // 2]
    
    async def _compress_model(
        self,
        model_config: Dict[str, Any],
        profile: Dict[str, Any],
        precision: str,
        carbon_budget_g: float
    ) -> Dict[str, Any]:
        """Compress model for edge deployment"""
        original_size = model_config.get('size_mb', 10) * 1024  # Convert to KB
        
        # Compression ratios
        compression_ratios = {
            'fp32': 1.0,
            'fp16': 2.0,
            'int8': 4.0,
            'int4': 8.0
        }
        
        ratio = compression_ratios.get(precision, 2.0)
        compressed_size = original_size / ratio
        
        # Check against device limits
        max_size = profile.get('max_model_size_kb', profile.get('max_model_size_mb', 1) * 1024)
        if compressed_size > max_size:
            # Need additional compression
            additional_compression = max_size / compressed_size
            compressed_size = max_size
        
        return {
            'original_size_kb': original_size,
            'compressed_size_kb': compressed_size,
            'compression_ratio': original_size / max(compressed_size, 1),
            'precision': precision,
            'device_compatible': compressed_size <= max_size
        }
    
    def _estimate_performance(
        self,
        device: EdgeDevice,
        model: Dict[str, Any]
    ) -> Dict[str, float]:
        """Estimate inference performance on device"""
        model_size = model.get('compressed_size_kb', 100)
        
        # Latency estimation
        base_latency = model_size / 100  # ms per KB
        latency = base_latency / (device.available_processing_flops / 1e9)
        
        # Energy estimation
        energy = device.power_consumption_w * latency / 1000 / 3600  # Wh
        
        # Carbon estimation
        carbon = energy * device.carbon_intensity_g_per_kwh / 1000  # g
        
        return {
            'latency_ms': latency,
            'energy_wh': energy,
            'carbon_g': carbon
        }

# ============================================================================
# Offline-First Processing
# ============================================================================

class OfflineProcessor:
    """
    Offline-first processing with sync capabilities.
    
    Features:
    - Local task queuing
    - CRDT-based state sync
    - Conflict resolution
    - Vector clock ordering
    """
    
    def __init__(self):
        self.offline_queues: Dict[str, deque] = defaultdict(deque)
        self.vector_clocks: Dict[str, Dict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )
        self.crdt_state: Dict[str, Dict[str, Any]] = {}
        self.sync_log: deque = deque(maxlen=10000)
        
        logger.info("Offline Processor initialized")
    
    async def process_offline(
        self,
        device_id: str,
        task: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process task in offline mode"""
        # Add to offline queue
        self.offline_queues[device_id].append({
            'task': task,
            'queued_at': datetime.utcnow(),
            'status': 'queued'
        })
        
        # Update vector clock
        self.vector_clocks[device_id][device_id] += 1
        
        # Process locally if possible
        result = await self._process_locally(device_id, task)
        
        # Store result in CRDT state
        task_id = task.get('task_id', hashlib.sha256(str(task).encode()).hexdigest())
        self.crdt_state[task_id] = {
            'device_id': device_id,
            'result': result,
            'vector_clock': dict(self.vector_clocks[device_id]),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        return result
    
    async def _process_locally(
        self,
        device_id: str,
        task: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process task locally (simulated)"""
        await asyncio.sleep(np.random.exponential(0.01))
        
        return {
            'task_id': task.get('task_id', 'unknown'),
            'result': f"Processed offline by {device_id}",
            'timestamp': datetime.utcnow().isoformat()
        }
    
    async def sync_with_cloud(
        self,
        device_id: str,
        force_sync: bool = False
    ) -> Dict[str, Any]:
        """
        Synchronize offline results with cloud.
        
        Uses CRDT merge for conflict resolution.
        """
        # Get pending results
        pending = [
            (task_id, state)
            for task_id, state in self.crdt_state.items()
            if state['device_id'] == device_id
        ]
        
        if not pending and not force_sync:
            return {'synced': 0, 'conflicts': 0}
        
        sync_result = {
            'device_id': device_id,
            'synced_at': datetime.utcnow().isoformat(),
            'synced_tasks': 0,
            'conflicts_resolved': 0,
            'vector_clock': dict(self.vector_clocks[device_id])
        }
        
        for task_id, state in pending:
            # Merge CRDT state
            merged = self._merge_crdt(task_id, state)
            
            if merged:
                sync_result['synced_tasks'] += 1
                if merged.get('conflict_resolved'):
                    sync_result['conflicts_resolved'] += 1
        
        # Record sync
        self.sync_log.append(sync_result)
        
        logger.info(
            f"Synced {sync_result['synced_tasks']} tasks from {device_id}"
        )
        
        return sync_result
    
    def _merge_crdt(
        self,
        task_id: str,
        new_state: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Merge CRDT state with conflict resolution"""
        if task_id not in self.crdt_state:
            self.crdt_state[task_id] = new_state
            return new_state
        
        existing = self.crdt_state[task_id]
        
        # Compare vector clocks
        new_clock = new_state['vector_clock']
        existing_clock = existing['vector_clock']
        
        # Check if new state dominates existing
        new_dominates = all(
            new_clock.get(k, 0) >= existing_clock.get(k, 0)
            for k in set(new_clock.keys()) | set(existing_clock.keys())
        )
        
        if new_dominates:
            self.crdt_state[task_id] = new_state
            return new_state
        
        # Conflict: use LWW (Last-Write-Wins)
        if new_state['timestamp'] > existing['timestamp']:
            self.crdt_state[task_id] = new_state
            new_state['conflict_resolved'] = True
            return new_state
        
        return None
    
    def get_sync_status(self) -> Dict[str, Any]:
        """Get synchronization status"""
        pending_devices = set(
            state['device_id'] for state in self.crdt_state.values()
        )
        
        return {
            'devices_with_pending_sync': len(pending_devices),
            'total_pending_tasks': len(self.crdt_state),
            'offline_queue_sizes': {
                device_id: len(queue)
                for device_id, queue in self.offline_queues.items()
            },
            'last_sync_times': {
                device_id: datetime.utcnow().isoformat()
                for device_id in pending_devices
            },
            'recent_syncs': list(self.sync_log)[-10:]
        }

# ============================================================================
# Energy Harvesting Scheduler
# ============================================================================

class EnergyHarvestingScheduler:
    """
    Schedules tasks based on energy harvesting predictions.
    
    Features:
    - Solar prediction based on time/location
    - Task prioritization for energy windows
    - Battery-aware scheduling
    """
    
    def __init__(self):
        self.task_queues: Dict[str, List[Dict]] = defaultdict(list)
        self.scheduling_history: deque = deque(maxlen=1000)
        
        logger.info("Energy Harvesting Scheduler initialized")
    
    def predict_solar_availability(
        self,
        latitude: float,
        longitude: float,
        hour: int,
        cloud_cover_percent: float = 0.0
    ) -> float:
        """
        Predict solar energy availability.
        
        Returns estimated watts per square meter.
        """
        # Solar constant
        solar_constant = 1000  # W/m²
        
        # Time of day factor
        if 6 <= hour <= 18:
            # Parabolic curve peaking at noon
            time_factor = math.sin(math.pi * (hour - 6) / 12)
        else:
            time_factor = 0.0
        
        # Seasonal factor (simplified)
        day_of_year = datetime.utcnow().timetuple().tm_yday
        seasonal_factor = 0.5 + 0.5 * math.sin(
            2 * math.pi * (day_of_year - 80) / 365
        )
        
        # Cloud cover reduction
        cloud_factor = 1.0 - cloud_cover_percent * 0.75
        
        # Latitude factor (simplified)
        latitude_factor = math.cos(math.radians(abs(latitude) - 23.5 * seasonal_factor))
        latitude_factor = max(0.1, latitude_factor)
        
        estimated_w = (
            solar_constant *
            time_factor *
            seasonal_factor *
            cloud_factor *
            latitude_factor
        )
        
        return max(0, estimated_w)
    
    def schedule_task_for_energy_window(
        self,
        device: EdgeDevice,
        task: Dict[str, Any],
        energy_required_wh: float,
        deadline_hours: float = 24.0
    ) -> Optional[datetime]:
        """
        Schedule task during optimal energy window.
        
        Returns scheduled time or None if cannot be scheduled.
        """
        if device.can_operate_indefinitely:
            # Can run immediately
            return datetime.utcnow()
        
        # Find next energy window
        best_time = None
        best_energy = 0
        
        current_hour = datetime.utcnow().hour
        
        for hour_offset in range(min(int(deadline_hours), 48)):
            target_hour = (current_hour + hour_offset) % 24
            predicted_energy = device.harvesting_schedule.get(
                target_hour,
                self.predict_solar_availability(
                    device.location.get('lat', 0) if device.location else 0,
                    device.location.get('lon', 0) if device.location else 0,
                    target_hour
                )
            )
            
            if predicted_energy > best_energy:
                best_energy = predicted_energy
                best_time = datetime.utcnow() + timedelta(hours=hour_offset)
        
        if best_time and best_energy >= device.power_consumption_w:
            return best_time
        
        # If insufficient energy, schedule for earliest possible
        if device.current_battery_wh >= energy_required_wh:
            return datetime.utcnow()
        
        # Calculate charging time needed
        energy_needed = energy_required_wh - device.current_battery_wh
        
        if device.charging_rate_w > 0:
            charging_hours = energy_needed / device.charging_rate_w
            return datetime.utcnow() + timedelta(hours=charging_hours)
        
        return None  # Cannot schedule
    
    def get_energy_forecast(
        self,
        device: EdgeDevice,
        hours_ahead: int = 24
    ) -> List[Dict[str, Any]]:
        """Get energy availability forecast"""
        forecast = []
        current_hour = datetime.utcnow().hour
        
        for hour_offset in range(hours_ahead):
            target_hour = (current_hour + hour_offset) % 24
            
            if device.location:
                predicted_w = self.predict_solar_availability(
                    device.location['lat'],
                    device.location['lon'],
                    target_hour
                )
            else:
                predicted_w = device.harvesting_schedule.get(target_hour, 0)
            
            forecast.append({
                'hour': target_hour,
                'time': (datetime.utcnow() + timedelta(hours=hour_offset)).isoformat(),
                'predicted_watts': predicted_w,
                'can_operate': predicted_w >= device.power_consumption_w,
                'battery_remaining_wh': device.current_battery_wh
            })
        
        return forecast

# ============================================================================
# Enhanced IoT Expert
# ============================================================================

class EnhancedIoTExpert:
    """
    Enhanced IoT Expert with mesh networking capabilities.
    
    Integrates all edge computing enhancements.
    """
    
    def __init__(
        self,
        expert_id: str = "iot_expert_v2",
        enable_mesh: bool = True,
        enable_collaborative: bool = True,
        enable_offline: bool = True,
        enable_energy_harvesting: bool = True
    ):
        self.expert_id = expert_id
        self.version = "2.0.0"
        
        # Feature flags
        self.enable_mesh = enable_mesh
        self.enable_collaborative = enable_collaborative
        self.enable_offline = enable_offline
        self.enable_energy_harvesting = enable_energy_harvesting
        
        # Sub-modules
        self.mesh_networks: Dict[str, MeshNetwork] = {}
        self.collaborative_processor = CollaborativeEdgeProcessor() if enable_collaborative else None
        self.model_deployer = EdgeModelDeployer()
        self.offline_processor = OfflineProcessor() if enable_offline else None
        self.energy_scheduler = EnergyHarvestingScheduler() if enable_energy_harvesting else None
        
        # Registered devices
        self.devices: Dict[str, EdgeDevice] = {}
        
        logger.info(f"Enhanced IoT Expert initialized: {expert_id}")
    
    def register_device(
        self,
        device_id: str,
        device_type: DeviceType,
        capabilities: Dict[str, float],
        location: Optional[Dict[str, float]] = None,
        mesh_id: Optional[str] = None
    ) -> EdgeDevice:
        """Register edge device"""
        device = EdgeDevice(
            device_id=device_id,
            device_type=device_type,
            capabilities=capabilities,
            location=location,
            mesh_id=mesh_id
        )
        
        self.devices[device_id] = device
        
        # Add to mesh if specified
        if mesh_id and self.enable_mesh:
            if mesh_id not in self.mesh_networks:
                self.mesh_networks[mesh_id] = MeshNetwork(mesh_id=mesh_id)
            self.mesh_networks[mesh_id].add_device(device)
        
        logger.info(f"Registered device: {device_id} ({device_type.value})")
        
        return device
    
    def create_mesh(
        self,
        mesh_id: str,
        device_ids: List[str]
    ) -> MeshNetwork:
        """Create mesh network"""
        mesh = MeshNetwork(mesh_id=mesh_id)
        
        for device_id in device_ids:
            if device_id in self.devices:
                mesh.add_device(self.devices[device_id])
        
        # Auto-discover connections
        for i, dev1_id in enumerate(device_ids):
            for dev2_id in device_ids[i+1:]:
                if dev1_id in self.devices and dev2_id in self.devices:
                    # Simulate link quality
                    quality = np.random.uniform(0.5, 1.0)
                    bandwidth = np.random.uniform(10, 100)
                    latency = np.random.uniform(1, 50)
                    
                    mesh.add_connection(dev1_id, dev2_id, quality, bandwidth, latency)
        
        # Elect leader
        mesh.elect_leader()
        
        self.mesh_networks[mesh_id] = mesh
        
        logger.info(f"Created mesh {mesh_id} with {len(device_ids)} devices")
        
        return mesh
    
    async def optimize_edge_deployment(
        self,
        device_type: str,
        carbon_zone: int,
        helium_scarcity: float,
        task_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Optimize edge deployment for given constraints.
        
        This is the main expert interface method.
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
        
        # Find best mesh for collaborative processing
        best_mesh = None
        if self.enable_mesh and self.mesh_networks:
            for mesh in self.mesh_networks.values():
                mesh_devices = [
                    d for d in mesh.devices.values()
                    if d in suitable_devices
                ]
                if len(mesh_devices) >= 2:
                    best_mesh = mesh
                    break
        
        # Select best device
        best_device = max(
            suitable_devices,
            key=lambda d: (
                d.available_processing_flops * 0.4 +
                d.energy_remaining_percent / 100 * 0.3 +
                (1.0 - d.processing_utilization) * 0.3
            )
        )
        
        plan = {
            'expert_id': self.expert_id,
            'strategy': 'mesh_collaborative' if best_mesh else 'single_device',
            'primary_device': best_device.device_id,
            'mesh_id': best_mesh.mesh_id if best_mesh else None,
            'mesh_size': len(best_mesh.devices) if best_mesh else 1,
            'estimated_carbon_kg': best_device.carbon_per_operation_g / 1000,
            'estimated_latency_ms': 10.0,
            'energy_source': best_device.energy_source.value,
            'energy_remaining_percent': best_device.energy_remaining_percent,
            'can_operate_indefinitely': best_device.can_operate_indefinitely,
            'recommendations': self._generate_recommendations(best_device, best_mesh)
        }
        
        return plan
    
    def _generate_recommendations(
        self,
        device: EdgeDevice,
        mesh: Optional[MeshNetwork]
    ) -> List[str]:
        """Generate deployment recommendations"""
        recommendations = []
        
        if device.energy_remaining_percent < 20:
            recommendations.append(
                f"Device {device.device_id} battery low ({device.energy_remaining_percent:.0f}%). "
                "Consider deferring non-critical tasks."
            )
        
        if mesh and len(mesh.devices) >= 3:
            recommendations.append(
                "Mesh network available for collaborative processing. "
                "Consider partitioning workload across devices."
            )
        
        if device.can_operate_indefinitely:
            recommendations.append(
                "Device has sufficient energy harvesting for continuous operation."
            )
        
        if device.processing_utilization > 0.8:
            recommendations.append(
                f"Device {device.device_id} under high load ({device.processing_utilization:.0%}). "
                "Consider offloading to mesh or cloud."
            )
        
        return recommendations
    
    def get_mesh_statistics(self) -> Dict[str, Any]:
        """Get statistics for all mesh networks"""
        return {
            mesh_id: mesh.get_mesh_statistics()
            for mesh_id, mesh in self.mesh_networks.items()
        }
    
    def get_device_status(self) -> Dict[str, Any]:
        """Get status of all devices"""
        return {
            device_id: {
                'type': d.device_type.value,
                'online': d.is_online,
                'battery_percent': d.energy_remaining_percent,
                'processing_utilization': d.processing_utilization,
                'mesh_id': d.mesh_id,
                'energy_source': d.energy_source.value,
                'can_operate_indefinitely': d.can_operate_indefinitely
            }
            for device_id, d in self.devices.items()
        }
