# src/dpq/precision_controller.py

from typing import Dict, Optional, List
from dataclasses import dataclass
from enum import Enum
import asyncio

class ModelPrecision(Enum):
    FP32 = "fp32"    # 32-bit floating point (baseline)
    FP16 = "fp16"    # 16-bit floating point
    INT8 = "int8"    # 8-bit integer
    INT4 = "int4"    # 4-bit integer (experimental)

@dataclass
class PrecisionPolicy:
    """Policy mapping carbon zones to precision levels"""
    green_zone_precision: ModelPrecision = ModelPrecision.FP32
    yellow_zone_precision: ModelPrecision = ModelPrecision.FP16
    red_zone_precision: ModelPrecision = ModelPrecision.INT8
    critical_zone_precision: ModelPrecision = ModelPrecision.INT4
    
    # Accuracy thresholds per precision
    min_accuracy_fp32: float = 0.95
    min_accuracy_fp16: float = 0.93
    min_accuracy_int8: float = 0.90
    min_accuracy_int4: float = 0.85
    
    # Transition thresholds (hysteresis to prevent oscillation)
    zone_transition_buffer: float = 10.0  # gCO2/kWh buffer

class PrecisionController:
    """
    Dynamic precision adjustment engine
    
    Responsibilities:
    - Map carbon zones to precision levels per policy
    - Trigger model conversion on zone changes
    - Coordinate with WorkerPoolManager for scaling
    - Enforce accuracy thresholds via AccuracyGuardian
    """
    
    def __init__(
        self,
        policy: PrecisionPolicy,
        model_converter: 'ModelConverter',
        worker_pool_manager: 'WorkerPoolManager',
        accuracy_guardian: 'AccuracyGuardian'
    ):
        self.policy = policy
        self.model_converter = model_converter
        self.worker_pool_manager = worker_pool_manager
        self.accuracy_guardian = accuracy_guardian
        
        self._current_precision: Dict[str, ModelPrecision] = {}
        self._transition_locks: Dict[str, asyncio.Lock] = {}
        
    async def on_carbon_zone_change(self, update: CarbonIntensityUpdate):
        """Handle carbon zone change notification"""
        region = update.region
        target_precision = self._get_precision_for_zone(update.zone)
        
        # Acquire lock to prevent concurrent transitions
        if region not in self._transition_locks:
            self._transition_locks[region] = asyncio.Lock()
            
        async with self._transition_locks[region]:
            current = self._current_precision.get(region)
            
            # Skip if already at target precision
            if current == target_precision:
                return
                
            # Check hysteresis buffer to prevent oscillation
            if self._should_transition(current, target_precision, update.intensity_gco2_kwh):
                logger.info(f"Transitioning {region} from {current} to {target_precision}")
                
                # Trigger model conversion
                await self.model_converter.convert_models(
                    region=region,
                    from_precision=current,
                    to_precision=target_precision
                )
                
                # Scale worker pools
                await self.worker_pool_manager.scale_precision_pool(
                    region=region,
                    precision=target_precision,
                    target_replicas=self._calculate_replicas(target_precision)
                )
                
                # Update current state
                self._current_precision[region] = target_precision
                
                # Notify monitoring
                await self._emit_transition_metric(region, current, target_precision)
                
    def _get_precision_for_zone(self, zone: CarbonZone) -> ModelPrecision:
        """Map carbon zone to precision level per policy"""
        mapping = {
            CarbonZone.GREEN: self.policy.green_zone_precision,
            CarbonZone.YELLOW: self.policy.yellow_zone_precision,
            CarbonZone.RED: self.policy.red_zone_precision,
            CarbonZone.CRITICAL: self.policy.critical_zone_precision,
        }
        return mapping[zone]
        
    def _should_transition(
        self,
        current: Optional[ModelPrecision],
        target: ModelPrecision,
        intensity: float
    ) -> bool:
        """Check if transition should proceed (hysteresis logic)"""
        if current is None:
            return True  # First transition
            
        # Get threshold for current → target transition
        threshold = self._get_transition_threshold(current, target)
        
        # Apply buffer to prevent oscillation
        if target == ModelPrecision.FP32:
            # Upgrading precision: require intensity well below threshold
            return intensity < threshold - self.policy.zone_transition_buffer
        else:
            # Downgrading precision: require intensity well above threshold
            return intensity > threshold + self.policy.zone_transition_buffer
            
    def _get_transition_threshold(
        self,
        from_prec: ModelPrecision,
        to_prec: ModelPrecision
    ) -> float:
        """Get carbon intensity threshold for precision transition"""
        # Thresholds based on policy zone boundaries
        thresholds = {
            (ModelPrecision.FP32, ModelPrecision.FP16): 50.0,   # Green → Yellow
            (ModelPrecision.FP16, ModelPrecision.INT8): 200.0,  # Yellow → Red
            (ModelPrecision.INT8, ModelPrecision.INT4): 400.0,  # Red → Critical
            # Reverse transitions use same thresholds
            (ModelPrecision.FP16, ModelPrecision.FP32): 50.0,
            (ModelPrecision.INT8, ModelPrecision.FP16): 200.0,
            (ModelPrecision.INT4, ModelPrecision.INT8): 400.0,
        }
        return thresholds.get((from_prec, to_prec), 200.0)  # Default to Yellow boundary
        
    def _calculate_replicas(self, precision: ModelPrecision) -> int:
        """Calculate target replica count for precision pool"""
        # Higher precision = more resources = fewer replicas
        replica_mapping = {
            ModelPrecision.FP32: 4,   # Baseline
            ModelPrecision.FP16: 6,   # +50% replicas
            ModelPrecision.INT8: 10,  # +150% replicas
            ModelPrecision.INT4: 16,  # +300% replicas
        }
        return replica_mapping[precision]
        
    async def _emit_transition_metric(
        self,
        region: str,
        from_prec: Optional[ModelPrecision],
        to_prec: ModelPrecision
    ):
        """Emit Prometheus metric for precision transition"""
        # Implementation: Prometheus client
        pass
