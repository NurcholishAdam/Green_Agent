# File: enhancements/moe_expert_system/expert_registry.py

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json

logger = logging.getLogger(__name__)

class ExpertDomain(Enum):
    ENERGY = "energy_optimization"
    DATA = "data_engineering"
    IOT = "iot_edge_computing"
    QUANTUM = "quantum_computing"
    HELIUM = "helium_aware_computing"
    CARBON = "carbon_optimization"

class HardwareProfile(Enum):
    CPU_EFFICIENT = "cpu_low_power"
    GPU_ACCELERATED = "gpu_cuda"
    QUANTUM_BACKEND = "quantum_processor"
    EDGE_DEVICE = "edge_iot_device"
    HYBRID = "hybrid_cpu_gpu"

@dataclass
class ExpertProfile:
    """Profile defining an expert's capabilities and resource requirements"""
    expert_id: str
    domain: ExpertDomain
    hardware_profile: HardwareProfile
    
    # Resource consumption metrics
    helium_per_inference: float  # Helium units per inference
    carbon_per_inference: float  # kg CO2 per inference
    energy_per_inference: float  # kWh per inference
    avg_latency_ms: float  # Average latency in milliseconds
    
    # Capability scores (0.0 to 1.0)
    accuracy_score: float
    reliability_score: float
    efficiency_score: float
    
    # Constraints
    min_carbon_zone: int = 0  # Minimum carbon zone required (0-15)
    max_helium_scarcity: float = 1.0  # Maximum helium scarcity tolerable
    supported_task_types: List[str] = field(default_factory=list)
    
    # Status
    is_active: bool = True
    is_quantum_dependent: bool = False
    
    def to_dict(self) -> Dict:
        return {
            'expert_id': self.expert_id,
            'domain': self.domain.value,
            'hardware_profile': self.hardware_profile.value,
            'helium_per_inference': self.helium_per_inference,
            'carbon_per_inference': self.carbon_per_inference,
            'energy_per_inference': self.energy_per_inference,
            'avg_latency_ms': self.avg_latency_ms,
            'accuracy_score': self.accuracy_score,
            'reliability_score': self.reliability_score,
            'efficiency_score': self.efficiency_score,
            'is_active': self.is_active
        }
    
    def compute_hash(self) -> str:
        """Generate hash for immutable ledger logging"""
        profile_str = json.dumps(self.to_dict(), sort_keys=True)
        return hashlib.sha256(profile_str.encode()).hexdigest()

class ExpertRegistry:
    """
    Central registry for all MoE experts.
    Manages expert lifecycle, discovery, and health status.
    Integrates with Layer 8 (Immutable Dual Ledger) for audit trails.
    """
    
    def __init__(self):
        self._experts: Dict[str, ExpertProfile] = {}
        self._domain_index: Dict[ExpertDomain, List[str]] = {}
        self._hardware_index: Dict[HardwareProfile, List[str]] = {}
        self._performance_history: Dict[str, List[Dict]] = {}
        
        logger.info("Initialized Expert Registry")
    
    def register_expert(self, profile: ExpertProfile) -> bool:
        """
        Register a new expert in the system.
        Returns True if registration successful.
        """
        if profile.expert_id in self._experts:
            logger.warning(f"Expert {profile.expert_id} already registered")
            return False
        
        self._experts[profile.expert_id] = profile
        
        # Index by domain
        if profile.domain not in self._domain_index:
            self._domain_index[profile.domain] = []
        self._domain_index[profile.domain].append(profile.expert_id)
        
        # Index by hardware
        if profile.hardware_profile not in self._hardware_index:
            self._hardware_index[profile.hardware_profile] = []
        self._hardware_index[profile.hardware_profile].append(profile.expert_id)
        
        # Initialize performance history
        self._performance_history[profile.expert_id] = []
        
        logger.info(f"Registered expert: {profile.expert_id} in domain {profile.domain.value}")
        return True
    
    def unregister_expert(self, expert_id: str) -> bool:
        """Remove an expert from the registry"""
        if expert_id not in self._experts:
            return False
        
        profile = self._experts[expert_id]
        
        # Remove from indexes
        self._domain_index[profile.domain].remove(expert_id)
        self._hardware_index[profile.hardware_profile].remove(expert_id)
        
        # Clean up
        del self._experts[expert_id]
        del self._performance_history[expert_id]
        
        logger.info(f"Unregistered expert: {expert_id}")
        return True
    
    def get_expert(self, expert_id: str) -> Optional[ExpertProfile]:
        """Retrieve expert profile by ID"""
        return self._experts.get(expert_id)
    
    def get_experts_by_domain(self, domain: ExpertDomain) -> List[ExpertProfile]:
        """Get all experts in a specific domain"""
        expert_ids = self._domain_index.get(domain, [])
        return [self._experts[eid] for eid in expert_ids if eid in self._experts]
    
    def get_experts_by_hardware(self, hardware: HardwareProfile) -> List[ExpertProfile]:
        """Get all experts running on specific hardware"""
        expert_ids = self._hardware_index.get(hardware, [])
        return [self._experts[eid] for eid in expert_ids if eid in self._experts]
    
    def filter_experts(
        self,
        domain: Optional[ExpertDomain] = None,
        max_helium: Optional[float] = None,
        max_carbon: Optional[float] = None,
        min_accuracy: Optional[float] = None,
        hardware: Optional[HardwareProfile] = None,
        exclude_quantum: bool = False
    ) -> List[ExpertProfile]:
        """
        Filter experts based on multiple criteria.
        Used by Layer 2 (Neuro-Symbolic) for constraint-based filtering.
        """
        candidates = list(self._experts.values())
        
        if domain:
            candidates = [e for e in candidates if e.domain == domain]
        
        if max_helium is not None:
            candidates = [e for e in candidates if e.helium_per_inference <= max_helium]
        
        if max_carbon is not None:
            candidates = [e for e in candidates if e.carbon_per_inference <= max_carbon]
        
        if min_accuracy is not None:
            candidates = [e for e in candidates if e.accuracy_score >= min_accuracy]
        
        if hardware:
            candidates = [e for e in candidates if e.hardware_profile == hardware]
        
        if exclude_quantum:
            candidates = [e for e in candidates if not e.is_quantum_dependent]
        
        return candidates
    
    def update_performance(
        self,
        expert_id: str,
        metrics: Dict[str, Any]
    ):
        """Record expert performance for meta-cognitive learning"""
        if expert_id in self._performance_history:
            self._performance_history[expert_id].append({
                **metrics,
                'timestamp': metrics.get('timestamp', 0)
            })
            
            # Keep only last 1000 records
            if len(self._performance_history[expert_id]) > 1000:
                self._performance_history[expert_id] = \
                    self._performance_history[expert_id][-1000:]
    
    def get_expert_performance(self, expert_id: str) -> List[Dict]:
        """Retrieve performance history for an expert"""
        return self._performance_history.get(expert_id, [])
    
    def get_all_active_experts(self) -> List[ExpertProfile]:
        """Get all currently active experts"""
        return [e for e in self._experts.values() if e.is_active]
    
    def get_registry_stats(self) -> Dict:
        """Get registry statistics for monitoring"""
        return {
            'total_experts': len(self._experts),
            'active_experts': len(self.get_all_active_experts()),
            'domains': {
                domain.value: len(experts)
                for domain, experts in self._domain_index.items()
            },
            'hardware_distribution': {
                hw.value: len(experts)
                for hw, experts in self._hardware_index.items()
            }
        }
