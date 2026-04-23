# src/interpretation/workload_interpreter.py (EXTENDED)

from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
from .helium_profile import HeliumProfile, HardwareType

@dataclass
class WorkloadProfile:
    """Enhanced workload profile with helium awareness"""
    task_id: str
    complexity_score: float  # 0.0-1.0
    energy_estimate_kwh: float
    carbon_estimate_kg: float
    resource_requirements: Dict[str, Any]
    deferrable: bool
    priority: int  # 1-10
    
    # NEW: Helium awareness
    helium_profile: Optional[HeliumProfile] = None
    
    # Metadata
    timestamp: datetime = field(default_factory=datetime.now)
    source: str = "workload_interpreter"

class WorkloadInterpreter:
    """
    Enhanced workload interpreter with helium dependency scoring
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.enable_helium_analysis = self.config.get('enable_helium_analysis', True)
        
    def analyze_task(self, task_json: Dict[str, Any]) -> WorkloadProfile:
        """
        Analyze incoming task and create workload profile with helium metrics
        """
        
        # Existing complexity analysis
        complexity_score = self._calculate_complexity(task_json)
        energy_estimate = self._estimate_energy(task_json)
        carbon_estimate = self._estimate_carbon(energy_estimate)
        
        # Resource requirements
        resource_reqs = self._extract_resource_requirements(task_json)
        
        # Priority and deferrability
        priority = task_json.get('priority', 5)
        deferrable = task_json.get('deferrable', True)
        
        # Create base profile
        profile = WorkloadProfile(
            task_id=task_json.get('task_id', 'unknown'),
            complexity_score=complexity_score,
            energy_estimate_kwh=energy_estimate,
            carbon_estimate_kg=carbon_estimate,
            resource_requirements=resource_reqs,
            deferrable=deferrable,
            priority=priority
        )
        
        # NEW: Helium dependency analysis
        if self.enable_helium_analysis:
            profile.helium_profile = HeliumProfile.from_task_config(task_json)
            
            # Adjust energy estimate based on helium constraints
            if profile.helium_profile.dependency_score > 0.8:
                profile.energy_estimate_kwh *= 1.2  # High-dependency tasks use more energy
        
        return profile
    
    def _calculate_complexity(self, task_json: Dict) -> float:
        """Calculate task complexity score"""
        model_size = task_json.get('model_config', {}).get('size_gb', 0)
        data_volume = task_json.get('data_volume_gb', 0)
        
        # Simple complexity heuristic
        complexity = min(1.0, (model_size / 100) + (data_volume / 1000))
        return complexity
    
    def _estimate_energy(self, task_json: Dict) -> float:
        """Estimate energy consumption in kWh"""
        # Placeholder - in production, use actual power models
        hardware_req = task_json.get('hardware_requirements', {})
        gpu_count = hardware_req.get('gpu_count', 0)
        
        base_energy = 0.1  # kWh
        gpu_energy = gpu_count * 0.3  # kWh per GPU
        
        return base_energy + gpu_energy
    
    def _estimate_carbon(self, energy_kwh: float) -> float:
        """Estimate carbon emissions in kg CO2"""
        # Average grid carbon intensity ~0.4 kg CO2/kWh
        return energy_kwh * 0.4
    
    def _extract_resource_requirements(self, task_json: Dict) -> Dict:
        """Extract resource requirements from task config"""
        return {
            'cpu_cores': task_json.get('hardware_requirements', {}).get('cpu_cores', 2),
            'memory_gb': task_json.get('hardware_requirements', {}).get('memory_gb', 8),
            'gpu_count': task_json.get('hardware_requirements', {}).get('gpu_count', 0),
            'storage_gb': task_json.get('data_volume_gb', 10)
        }
