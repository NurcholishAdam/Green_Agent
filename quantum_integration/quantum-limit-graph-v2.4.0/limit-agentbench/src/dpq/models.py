# src/dpq/models.py

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from enum import Enum

class ModelPrecision(str, Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"
    INT4 = "int4"

class CarbonZone(str, Enum):
    GREEN = "green"
    YELLOW = "yellow"
    RED = "red"
    CRITICAL = "critical"

@dataclass
class ModelVariant:
    """Represents a model at a specific precision level"""
    model_name: str
    precision: ModelPrecision
    size_mb: float
    accuracy: float
    conversion_time_ms: float
    backend: str
    created_at: datetime
    last_used: Optional[datetime] = None
    usage_count: int = 0
    
    def is_available(self) -> bool:
        """Check if variant is ready for use"""
        return self.accuracy >= self._min_accuracy()
        
    def _min_accuracy(self) -> float:
        """Minimum accuracy threshold for this precision"""
        thresholds = {
            ModelPrecision.FP32: 0.95,
            ModelPrecision.FP16: 0.93,
            ModelPrecision.INT8: 0.90,
            ModelPrecision.INT4: 0.85,
        }
        return thresholds[self.precision]

@dataclass
class PrecisionTransition:
    """Records a precision transition event"""
    transition_id: str
    region: str
    model_name: str
    from_precision: Optional[ModelPrecision]
    to_precision: ModelPrecision
    triggered_by: str  # "carbon_zone_change", "manual", "accuracy_guardian"
    carbon_intensity: float
    carbon_zone: CarbonZone
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "pending"  # pending, in_progress, completed, failed
    conversion_results: List[ConversionResult] = field(default_factory=list)
    error_message: Optional[str] = None
    
    def duration_ms(self) -> Optional[float]:
        """Calculate transition duration in milliseconds"""
        if self.end_time is None:
            return None
        return (self.end_time - self.start_time).total_seconds() * 1000

@dataclass
class DPQMetrics:
    """Aggregated metrics for DPQ module"""
    region: str
    timestamp: datetime
    
    # Precision distribution
    precision_counts: Dict[ModelPrecision, int]
    
    # Energy metrics
    energy_fp32_baseline_kwh: float
    energy_current_kwh: float
    energy_savings_percent: float
    
    # Accuracy metrics
    accuracy_fp32_baseline: float
    accuracy_current: float
    accuracy_delta: float
    
    # Performance metrics
    avg_conversion_time_ms: float
    p99_conversion_time_ms: float
    transition_count_24h: int
    
    # Carbon metrics
    carbon_intensity_current: float
    carbon_zone_current: CarbonZone
    estimated_carbon_saved_kg: float
