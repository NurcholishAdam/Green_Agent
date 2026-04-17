"""
Green Agent v5.0.0 - Workload Interpreter
Layer 0: Analyzes task requirements and complexity
File: src/interpretation/workload_interpreter.py
"""

from typing import Dict
from dataclasses import dataclass
from datetime import datetime
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
