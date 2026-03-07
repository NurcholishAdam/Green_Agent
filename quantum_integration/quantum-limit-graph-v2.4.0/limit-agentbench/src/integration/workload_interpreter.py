"""
Workload Interpreter

NEW: Analyzes and profiles incoming tasks
"""

from typing import Dict, Any
from dataclasses import dataclass

@dataclass
class WorkloadProfile:
    """Profile of a workload task"""
    task_type: str
    complexity: float
    energy_estimate: float
    carbon_estimate: float
    priority: int
    deferrable: bool
    deadline: float = None
    memory_estimate: float = 0.0
    cpu_estimate: float = 0.0

class WorkloadInterpreter:
    """
    Analyze and profile incoming workloads
    
    Features:
    - Task complexity estimation
    - Resource requirement prediction
    - Energy/carbon estimation
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.complexity_models = {
            'ml_inference': self._estimate_ml_complexity,
            'ml_training': self._estimate_training_complexity,
            'data_processing': self._estimate_data_complexity,
            'default': self._estimate_default_complexity
        }
    
    async def initialize(self):
        """Initialize interpreter"""
        print("✅ Workload interpreter initialized")
    
    async def analyze(self, task: Dict) -> WorkloadProfile:
        """
        Analyze task and create workload profile
        
        Args:
            task: Task dictionary with metadata
        
        Returns:
            WorkloadProfile with estimated requirements
        """
        task_type = task.get('type', 'default')
        model_size = task.get('model_size', '100M')
        input_size = task.get('input_size', '1MB')
        priority = task.get('priority', 5)
        deadline = task.get('deadline')
        
        # Get complexity estimator
        estimator = self.complexity_models.get(
            task_type, 
            self.complexity_models['default']
        )
        
        # Estimate complexity
        complexity = estimator(task)
        
        # Estimate resources
        energy_estimate = complexity * 1.5  # kWh
        carbon_estimate = energy_estimate * 0.4  # kg CO2
        memory_estimate = complexity * 1000  # MB
        cpu_estimate = complexity * 100  # %
        
        return WorkloadProfile(
            task_type=task_type,
            complexity=complexity,
            energy_estimate=energy_estimate,
            carbon_estimate=carbon_estimate,
            priority=priority,
            deferrable=task.get('deferrable', False),
            deadline=deadline,
            memory_estimate=memory_estimate,
            cpu_estimate=cpu_estimate
        )
    
    def _estimate_ml_complexity(self, task: Dict) -> float:
        """Estimate complexity for ML inference"""
        model_size = task.get('model_size', '100M')
        input_size = task.get('input_size', '1MB')
        
        # Map model size to complexity
        size_map = {
            '10M': 0.1, '50M': 0.3, '100M': 0.5,
            '500M': 0.7, '1B': 0.9, '10B': 1.0
        }
        
        base_complexity = size_map.get(model_size, 0.5)
        
        # Adjust for input size
        input_factor = 1.0 + (int(input_size.replace('MB', '')) / 100)
        
        return min(1.0, base_complexity * input_factor)
    
    def _estimate_training_complexity(self, task: Dict) -> float:
        """Estimate complexity for ML training"""
        model_size = task.get('model_size', '100M')
        dataset_size = task.get('dataset_size', '1GB')
        epochs = task.get('epochs', 10)
        
        size_map = {'100M': 0.3, '500M': 0.6, '1B': 0.9, '10B': 1.0}
        base = size_map.get(model_size, 0.5)
        
        # Training is more complex than inference
        return min(1.0, base * 2.0)
    
    def _estimate_data_complexity(self, task: Dict) -> float:
        """Estimate complexity for data processing"""
        data_size = task.get('data_size', '1GB')
        
        # Simple: 0.1 per GB
        size_gb = int(data_size.replace('GB', ''))
        return min(1.0, size_gb * 0.1)
    
    def _estimate_default_complexity(self, task: Dict) -> float:
        """Default complexity estimation"""
        return 0.5
