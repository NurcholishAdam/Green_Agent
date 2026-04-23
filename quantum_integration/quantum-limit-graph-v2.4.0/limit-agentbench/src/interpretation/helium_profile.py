# src/interpretation/helium_profile.py

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional
from datetime import datetime

class HeliumDependencyLevel(Enum):
    """Helium dependency classification for workloads"""
    CRITICAL = "critical"      # Large GPU/TPU training clusters, Quantum
    HIGH = "high"              # Single GPU training, inference servers
    MODERATE = "moderate"      # CPU-only but data-intensive
    LOW = "low"                # Basic CPU inference
    NEGLIGIBLE = "negligible"  # Edge devices, quantized models

class HardwareType(Enum):
    """Hardware types with their helium footprints"""
    GPU_CLUSTER = "gpu_cluster"
    SINGLE_GPU = "single_gpu"
    TPU = "tpu"
    CPU_ONLY = "cpu"
    QUANTUM = "quantum"
    EDGE = "edge"
    
    @property
    def helium_footprint(self) -> float:
        """Helium dependency score (0.0 to 1.0)"""
        footprints = {
            HardwareType.GPU_CLUSTER: 0.95,
            HardwareType.QUANTUM: 0.99,
            HardwareType.TPU: 0.85,
            HardwareType.SINGLE_GPU: 0.75,
            HardwareType.CPU_ONLY: 0.10,
            HardwareType.EDGE: 0.05
        }
        return footprints[self]

@dataclass
class HeliumProfile:
    """Helium dependency profile for a workload"""
    
    # Core metrics
    dependency_score: float  # 0.0 to 1.0 (1.0 = highest helium dependency)
    hardware_type: HardwareType
    estimated_helium_impact: float  # Arbitrary units per task
    scarcity_tolerance: float  # 0.0-1.0 (ability to run on constrained helium)
    
    # Detailed breakdown
    gpu_count: int = 0
    memory_bandwidth_gbs: float = 0.0
    estimated_training_hours: float = 0.0
    model_size_gb: float = 0.0
    
    # Optimization flags
    can_use_distilled_model: bool = False
    can_run_on_cpu: bool = False
    has_quantized_version: bool = False
    
    # Metadata
    timestamp: datetime = field(default_factory=datetime.now)
    
    @classmethod
    def from_task_config(cls, task_config: Dict) -> 'HeliumProfile':
        """Create HeliumProfile from task configuration"""
        
        hardware_req = task_config.get('hardware_requirements', {})
        model_config = task_config.get('model_config', {})
        
        # Determine hardware type
        gpu_count = hardware_req.get('gpu_count', 0)
        tpu_accelerator = hardware_req.get('tpu_accelerator', False)
        quantum_circuit = task_config.get('quantum_circuit', False)
        
        if quantum_circuit:
            hardware_type = HardwareType.QUANTUM
        elif tpu_accelerator:
            hardware_type = HardwareType.TPU
        elif gpu_count > 4:
            hardware_type = HardwareType.GPU_CLUSTER
        elif gpu_count > 0:
            hardware_type = HardwareType.SINGLE_GPU
        elif task_config.get('edge_deployment', False):
            hardware_type = HardwareType.EDGE
        else:
            hardware_type = HardwareType.CPU_ONLY
        
        # Calculate dependency score
        base_score = hardware_type.helium_footprint
        
        # Adjust for model size and training duration
        model_size_gb = model_config.get('size_gb', 0)
        training_hours = task_config.get('estimated_training_hours', 0)
        
        adjustment = 1.0
        if model_size_gb > 50:
            adjustment *= 1.3
        if training_hours > 100:
            adjustment *= 1.2
            
        dependency_score = min(1.0, base_score * adjustment)
        
        # Calculate scarcity tolerance (inverse of dependency)
        scarcity_tolerance = 1.0 - dependency_score
        
        # Check optimization availability
        can_use_distilled = model_config.get('has_distilled_version', False)
        can_run_on_cpu = model_config.get('cpu_fallback_available', False)
        has_quantized = model_config.get('quantized_available', False)
        
        return cls(
            dependency_score=dependency_score,
            hardware_type=hardware_type,
            estimated_helium_impact=dependency_score * 100,
            scarcity_tolerance=scarcity_tolerance,
            gpu_count=gpu_count,
            memory_bandwidth_gbs=hardware_req.get('memory_bandwidth_gbs', 0),
            estimated_training_hours=training_hours,
            model_size_gb=model_size_gb,
            can_use_distilled_model=can_use_distilled,
            can_run_on_cpu=can_run_on_cpu,
            has_quantized_version=has_quantized
        )
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return {
            'dependency_score': self.dependency_score,
            'hardware_type': self.hardware_type.value,
            'estimated_helium_impact': self.estimated_helium_impact,
            'scarcity_tolerance': self.scarcity_tolerance,
            'gpu_count': self.gpu_count,
            'memory_bandwidth_gbs': self.memory_bandwidth_gbs,
            'estimated_training_hours': self.estimated_training_hours,
            'model_size_gb': self.model_size_gb,
            'can_use_distilled_model': self.can_use_distilled_model,
            'can_run_on_cpu': self.can_run_on_cpu,
            'has_quantized_version': self.has_quantized_version,
            'timestamp': self.timestamp.isoformat()
        }
