"""
Green Agent v5.0.0 - Workload Interpreter
Layer 0: Analyzes task requirements and complexity
File: src/interpretation/workload_interpreter.py
"""

from typing import Dict
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import logging

logger = logging.getLogger(__name__)


@dataclass
class WorkloadProfile:
    """Profile of a workload's resource and carbon requirements"""
    task_id: str
    complexity: float  # 0.0-1.0
    energy_estimate_kwh: float
    carbon_estimate_kg: float
    memory_estimate_mb: float
    cpu_estimate_percent: float
    deferrable: bool
    priority: int  # 1-10
    deadline: datetime = None


class WorkloadInterpreter:
    """
    Interprets task specifications into executable workload profiles
    """
    
    def __init__(self, config: Dict):
        self.config = config
    
    async def initialize(self):
        """Initialize the interpreter"""
        logger.info("WorkloadInterpreter initialized")
    
    async def analyze(self, task: Dict) -> WorkloadProfile:
        """
        Analyze a task and create a workload profile
        
        Args:
            task: Task dictionary with type, priority, etc.
            
        Returns:
            WorkloadProfile with resource estimates
        """
        task_id = task.get('id', 'unknown')
        task_type = task.get('type', 'unknown')
        
        # Estimate complexity based on task type
        complexity = self._estimate_complexity(task_type, task)
        
        # Estimate resource requirements
        energy = self._estimate_energy(complexity, task)
        carbon = energy * 0.4  # Simplified: 0.4 kg CO2 per kWh
        
        return WorkloadProfile(
            task_id=task_id,
            complexity=complexity,
            energy_estimate_kwh=energy,
            carbon_estimate_kg=carbon,
            memory_estimate_mb=complexity * 1000,
            cpu_estimate_percent=complexity * 100,
            deferrable=task.get('deferrable', False),
            priority=task.get('priority', 5),
            deadline=task.get('deadline')
        )
    
    def _estimate_complexity(self, task_type: str, task: Dict) -> float:
        """Estimate task complexity from type and parameters"""
        # Base complexity by task type
        base_complexity = {
            'ml_inference': 0.5,
            'ml_training': 0.9,
            'data_processing': 0.4,
            'api_call': 0.2,
            'batch_job': 0.6,
            'quantum_simulation': 0.8,
        }.get(task_type, 0.5)
        
        # Adjust based on task parameters
        model_size = task.get('model_size', 'small')
        size_factor = {'tiny': 0.3, 'small': 0.5, 'medium': 0.7, 'large': 0.9, 'xlarge': 1.0}.get(model_size, 0.5)
        
        input_size = task.get('input_size_mb', 0)
        data_factor = min(1.0, input_size / 1000)  # Cap at 1.0
        
        # Combine factors
        complexity = base_complexity * (0.5 + 0.3 * size_factor + 0.2 * data_factor)
        return min(1.0, max(0.0, complexity))
    
    def _estimate_energy(self, complexity: float, task: Dict) -> float:
        """Estimate energy consumption from complexity"""
        # Base energy per complexity unit (kWh)
        base_energy = 1.5
        
        # Adjust for task-specific factors
        model_type = task.get('model', 'generic')
        model_factor = {
            'llama-7b': 1.2,
            'llama-13b': 1.8,
            'llama-70b': 3.5,
            'bert-base': 0.8,
            'generic': 1.0,
        }.get(model_type, 1.0)
        
        # Calculate final estimate
        energy = base_energy * complexity * model_factor
        return round(energy, 3)

# src/interpretation/workload_interpreter.py (Extended)

class HeliumDependencyLevel(Enum):
    CRITICAL = "critical"      # Large GPU/TPU training clusters
    HIGH = "high"              # Single GPU training, inference servers
    MODERATE = "moderate"      # CPU-only but data-intensive
    LOW = "low"                # Basic CPU inference
    NEGLIGIBLE = "negligible"  # Edge devices, quantized models

@dataclass
class HeliumProfile:
    dependency_score: float  # 0.0 to 1.0 (1.0 = highest helium dependency)
    hardware_type: str       # 'gpu_cluster', 'single_gpu', 'cpu', 'quantum', 'edge'
    estimated_helium_impact: float  # Arbitrary units per task
    scarcity_tolerance: float  # 0.0-1.0 (ability to run on constrained helium)

class WorkloadInterpreter:
    def analyze_task(self, task_json: dict) -> WorkloadProfile:
        # Existing complexity analysis...
        
        # NEW: Helium dependency scoring
        helium_profile = self._calculate_helium_dependency(task_json)
        
        workload_profile.helium_profile = helium_profile
        return workload_profile
    
    def _calculate_helium_dependency(self, task_json: dict) -> HeliumProfile:
        hardware_req = task_json.get('hardware_requirements', {})
        
        # Score based on hardware type
        if hardware_req.get('gpu_count', 0) > 4:
            dependency_score = 0.95
            hardware_type = 'gpu_cluster'
            scarcity_tolerance = 0.2  # Low tolerance - needs helium cooling
        elif hardware_req.get('gpu_count', 0) > 0:
            dependency_score = 0.75
            hardware_type = 'single_gpu'
            scarcity_tolerance = 0.4
        elif hardware_req.get('tpu_accelerator', False):
            dependency_score = 0.85
            hardware_type = 'tpu'
            scarcity_tolerance = 0.3
        elif hardware_req.get('quantum_circuit', False):
            dependency_score = 0.99  # Quantum computing extremely helium-dependent
            hardware_type = 'quantum'
            scarcity_tolerance = 0.1
        else:
            dependency_score = 0.1
            hardware_type = 'cpu'
            scarcity_tolerance = 0.9
        
        # Adjust for model size and training duration
        model_size_gb = task_json.get('model_size_gb', 0)
        training_hours = task_json.get('estimated_training_hours', 0)
        
        if model_size_gb > 50 or training_hours > 100:
            dependency_score = min(1.0, dependency_score * 1.3)
        
        return HeliumProfile(
            dependency_score=dependency_score,
            hardware_type=hardware_type,
            estimated_helium_impact=dependency_score * 100,  # Example scaling
            scarcity_tolerance=scarcity_tolerance
        )
