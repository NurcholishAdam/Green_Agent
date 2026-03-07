# File: src/integration/unified_orchestrator.py

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import datetime
import json

@dataclass
class UnifiedResult:
    """
    Unified result object for Green Agent execution
    
    FIXED: Added proper metrics attribute and all required fields
    """
    # Core execution results
    success: bool = True
    task_id: str = ""
    execution_time: float = 0.0
    accuracy: float = 0.0
    
    # Resource metrics
    energy_consumed: float = 0.0  # kWh
    carbon_emitted: float = 0.0   # kg CO₂
    memory_used: float = 0.0      # MB
    cpu_usage: float = 0.0        # %
    
    # Sustainability metrics
    negawatt_reward: float = 0.0
    carbon_saved: float = 0.0     # kg CO₂ saved vs baseline
    efficiency_score: float = 0.0
    
    # ⚠️ FIXED: Proper metrics dictionary
    metrics: Dict[str, Any] = field(default_factory=dict)
    
    # Additional metadata
    mode: str = "unified"
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    
    # Quantum-specific (if enabled)
    quantum_advantage: float = 0.0
    circuit_depth: int = 0
    error_mitigation_applied: bool = False
    
    # Decision metadata
    decision_reason: str = ""
    carbon_zone: str = ""
    policy_applied: str = ""
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'success': self.success,
            'task_id': self.task_id,
            'execution_time': self.execution_time,
            'accuracy': self.accuracy,
            'energy_consumed': self.energy_consumed,
            'carbon_emitted': self.carbon_emitted,
            'negawatt_reward': self.negawatt_reward,
            'metrics': self.metrics,
            'mode': self.mode,
            'timestamp': self.timestamp,
            'warnings': self.warnings,
            'errors': self.errors
        }
    
    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), indent=indent)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'UnifiedResult':
        """Create UnifiedResult from dictionary"""
        return cls(**data)
    
    def add_metric(self, key: str, value: Any):
        """Add a metric to the metrics dictionary"""
        self.metrics[key] = value
    
    def add_warning(self, warning: str):
        """Add a warning message"""
        self.warnings.append(warning)
    
    def add_error(self, error: str):
        """Add an error message"""
        self.errors.append(error)
        self.success = False
