"""
Eco-Mode Controller
===================

Dynamically throttles AI workloads based on carbon intensity forecasts.

Location: src/carbon/eco_mode_controller.py
"""

from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum
from datetime import datetime, timedelta
import logging
import asyncio

logger = logging.getLogger(__name__)


class EcoMode(Enum):
    """Eco-mode throttling levels"""
    PERFORMANCE = "performance"        # 100% compute, no throttling
    BALANCED = "balanced"              # 80% compute, minor throttling
    GREEN = "green"                    # 50% compute, moderate throttling
    EXTREME_GREEN = "extreme_green"    # 25% compute, aggressive throttling
    EMERGENCY = "emergency"            # Defer tasks, minimal compute


@dataclass
class EcoModeConfig:
    """Configuration for an eco-mode"""
    mode: EcoMode
    compute_limit_percent: int  # % of normal compute
    max_tokens_multiplier: float  # multiply max_tokens by this
    temperature_override: Optional[float]  # force temperature if set
    top_k_override: Optional[int]  # force top_k if set
    use_quantized_model: bool  # use INT8/FP16 quantization
    use_cache_aggressively: bool  # maximize cache usage
    early_stopping_threshold: float  # stop when confidence >= this
    quality_degradation_acceptable: float  # acceptable quality loss (0-1)


# Pre-defined eco-mode configurations
ECO_MODE_CONFIGS = {
    EcoMode.PERFORMANCE: EcoModeConfig(
        mode=EcoMode.PERFORMANCE,
        compute_limit_percent=100,
        max_tokens_multiplier=1.0,
        temperature_override=None,
        top_k_override=None,
        use_quantized_model=False,
        use_cache_aggressively=False,
        early_stopping_threshold=0.95,
        quality_degradation_acceptable=0.0
    ),
    EcoMode.BALANCED: EcoModeConfig(
        mode=EcoMode.BALANCED,
        compute_limit_percent=80,
        max_tokens_multiplier=0.8,
        temperature_override=None,
        top_k_override=None,
        use_quantized_model=False,
        use_cache_aggressively=True,
        early_stopping_threshold=0.90,
        quality_degradation_acceptable=0.05
    ),
    EcoMode.GREEN: EcoModeConfig(
        mode=EcoMode.GREEN,
        compute_limit_percent=50,
        max_tokens_multiplier=0.5,
        temperature_override=0.3,
        top_k_override=10,
        use_quantized_model=False,
        use_cache_aggressively=True,
        early_stopping_threshold=0.85,
        quality_degradation_acceptable=0.10
    ),
    EcoMode.EXTREME_GREEN: EcoModeConfig(
        mode=EcoMode.EXTREME_GREEN,
        compute_limit_percent=25,
        max_tokens_multiplier=0.25,
        temperature_override=0.1,
        top_k_override=5,
        use_quantized_model=True,
        use_cache_aggressively=True,
        early_stopping_threshold=0.80,
        quality_degradation_acceptable=0.15
    ),
    EcoMode.EMERGENCY: EcoModeConfig(
        mode=EcoMode.EMERGENCY,
        compute_limit_percent=10,
        max_tokens_multiplier=0.1,
        temperature_override=0.01,
        top_k_override=3,
        use_quantized_model=True,
        use_cache_aggressively=True,
        early_stopping_threshold=0.75,
        quality_degradation_acceptable=0.20
    )
}


@dataclass
class ThrottlingDecision:
    """Result of throttling decision"""
    original_task: Dict[str, Any]
    throttled_task: Dict[str, Any]
    eco_mode: EcoMode
    carbon_intensity: float  # gCO2/kWh
    estimated_energy_reduction_percent: float
    estimated_quality_impact_percent: float
    should_defer: bool
    defer_until: Optional[datetime]


class EcoModeController:
    """
    Controls eco-mode based on carbon intensity forecasts
    
    Strategy:
    - <150 gCO2/kWh: PERFORMANCE (very clean grid)
    - 150-250: BALANCED (moderately clean)
    - 250-400: GREEN (getting dirty)
    - 400-600: EXTREME_GREEN (very dirty)
    - >600: EMERGENCY (extremely dirty, defer tasks)
    """
    
    def __init__(
        self,
        carbon_forecaster,
        carbon_thresholds: Optional[Dict[EcoMode, float]] = None
    ):
        self.carbon_forecaster = carbon_forecaster
        
        # Carbon intensity thresholds for each mode
        self.carbon_thresholds = carbon_thresholds or {
            EcoMode.PERFORMANCE: 150,      # <150: full performance
            EcoMode.BALANCED: 250,         # 150-250: balanced
            EcoMode.GREEN: 400,            # 250-400: green
            EcoMode.EXTREME_GREEN: 600,    # 400-600: extreme green
            EcoMode.EMERGENCY: float('inf')  # >600: emergency
        }
        
        self.current_mode = EcoMode.BALANCED
        self.mode_history: List[tuple] = []
        
        # Statistics
        self.total_tasks_processed = 0
        self.total_tasks_deferred = 0
        self.total_energy_saved_kwh = 0.0
        self.quality_impact_sum = 0.0
        
        logger.info("Eco-mode controller initialized")
    
    async def determine_eco_mode(self) -> EcoMode:
        """Determine appropriate eco-mode based on carbon forecasts"""
        
        # Get current carbon intensity
        current_intensity = await self.carbon_forecaster.get_current_intensity()
        
        # Get 1-hour forecast
        forecasts = await self.carbon_forecaster.predict(
            horizon="1h",
            interval_minutes=15
        )
        avg_forecast = sum(f.predicted_intensity for f in forecasts) / len(forecasts)
        
        # Use worse of current vs forecast (conservative)
        effective_intensity = max(current_intensity, avg_forecast)
        
        # Determine mode based on thresholds
        if effective_intensity < self.carbon_thresholds[EcoMode.PERFORMANCE]:
            mode = EcoMode.PERFORMANCE
        elif effective_intensity < self.carbon_thresholds[EcoMode.BALANCED]:
            mode = EcoMode.BALANCED
        elif effective_intensity < self.carbon_thresholds[EcoMode.GREEN]:
            mode = EcoMode.GREEN
        elif effective_intensity < self.carbon_thresholds[EcoMode.EXTREME_GREEN]:
            mode = EcoMode.EXTREME_GREEN
        else:
            mode = EcoMode.EMERGENCY
        
        # Update mode if changed
        if mode != self.current_mode:
            logger.info(
                f"Eco-mode changed: {self.current_mode.value} → {mode.value} "
                f"(intensity: {effective_intensity:.0f} gCO2/kWh)"
            )
            self.current_mode = mode
            self.mode_history.append((datetime.now(), mode, effective_intensity))
        
        return mode
    
    async def apply_throttling(
        self,
        task: Dict[str, Any],
        force_mode: Optional[EcoMode] = None
    ) -> ThrottlingDecision:
        """
        Apply eco-mode throttling to task
        
        Args:
            task: Original task configuration
            force_mode: Override automatic mode selection
        
        Returns:
            Throttling decision with modified task
        """
        
        # Determine eco-mode
        if force_mode:
            eco_mode = force_mode
        else:
            eco_mode = await self.determine_eco_mode()
        
        config = ECO_MODE_CONFIGS[eco_mode]
        
        # Get current carbon intensity
        carbon_intensity = await self.carbon_forecaster.get_current_intensity()
        
        # Check if task should be deferred
        should_defer = False
        defer_until = None
        
        if eco_mode == EcoMode.EMERGENCY and task.get('deferrable', True):
            # Find optimal execution window
            try:
                deadline = task.get('deadline', datetime.now() + timedelta(hours=48))
                optimal_window = await self.carbon_forecaster.find_optimal_execution_window(
                    duration_hours=task.get('estimated_duration_hours', 1.0),
                    deadline=deadline
                )
                should_defer = True
                defer_until = optimal_window.start_time
                
                logger.info(
                    f"Task deferred to {defer_until} "
                    f"(carbon savings: {optimal_window.carbon_savings_percent:.1f}%)"
                )
            except Exception as e:
                logger.warning(f"Could not find optimal window: {e}")
                should_defer = False
        
        # Apply throttling to task parameters
        throttled_task = task.copy()
        
        # Adjust max_tokens
        if 'max_tokens' in throttled_task:
            throttled_task['max_tokens'] = int(
                throttled_task['max_tokens'] * config.max_tokens_multiplier
            )
        
        # Adjust temperature
        if config.temperature_override is not None:
            throttled_task['temperature'] = config.temperature_override
        
        # Adjust top_k
        if config.top_k_override is not None:
            throttled_task['top_k'] = config.top_k_override
        
        # Set quantization flag
        throttled_task['use_quantized_model'] = config.use_quantized_model
        
        # Set cache flag
        throttled_task['use_cache_aggressively'] = config.use_cache_aggressively
        
        # Set early stopping
        throttled_task['early_stopping_threshold'] = config.early_stopping_threshold
        
        # Estimate energy reduction
        energy_reduction_percent = (
            100 - config.compute_limit_percent
        )
        
        # Estimate quality impact
        quality_impact_percent = config.quality_degradation_acceptable * 100
        
        # Update statistics
        self.total_tasks_processed += 1
        if should_defer:
            self.total_tasks_deferred += 1
        
        # Estimate energy saved (rough heuristic)
        baseline_energy_kwh = task.get('estimated_energy_kwh', 0.001)
        energy_saved_kwh = baseline_energy_kwh * (energy_reduction_percent / 100)
        self.total_energy_saved_kwh += energy_saved_kwh
        self.quality_impact_sum += quality_impact_percent
        
        return ThrottlingDecision(
            original_task=task,
            throttled_task=throttled_task,
            eco_mode=eco_mode,
            carbon_intensity=carbon_intensity,
            estimated_energy_reduction_percent=energy_reduction_percent,
            estimated_quality_impact_percent=quality_impact_percent,
            should_defer=should_defer,
            defer_until=defer_until
        )
    
    async def batch_throttle(
        self,
        tasks: List[Dict[str, Any]]
    ) -> List[ThrottlingDecision]:
        """Apply throttling to multiple tasks"""
        
        decisions = []
        
        for task in tasks:
            decision = await self.apply_throttling(task)
            decisions.append(decision)
        
        return decisions
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get controller statistics"""
        
        if self.total_tasks_processed == 0:
            return {
                "total_tasks_processed": 0,
                "current_mode": self.current_mode.value
            }
        
        return {
            "current_mode": self.current_mode.value,
            "total_tasks_processed": self.total_tasks_processed,
            "total_tasks_deferred": self.total_tasks_deferred,
            "deferral_rate_percent": (
                self.total_tasks_deferred / self.total_tasks_processed * 100
            ),
            "total_energy_saved_kwh": self.total_energy_saved_kwh,
            "avg_energy_saved_per_task_kwh": (
                self.total_energy_saved_kwh / self.total_tasks_processed
            ),
            "avg_quality_impact_percent": (
                self.quality_impact_sum / self.total_tasks_processed
            ),
            "mode_history": [
                {
                    "timestamp": ts.isoformat(),
                    "mode": mode.value,
                    "carbon_intensity": intensity
                }
                for ts, mode, intensity in self.mode_history[-10:]  # Last 10 changes
            ]
        }
    
    async def get_current_recommendation(self) -> Dict[str, Any]:
        """Get current eco-mode recommendation"""
        
        current_intensity = await self.carbon_forecaster.get_current_intensity()
        eco_mode = await self.determine_eco_mode()
        config = ECO_MODE_CONFIGS[eco_mode]
        
        # Get forecast for next 6 hours
        forecasts = await self.carbon_forecaster.predict(
            horizon="6h",
            interval_minutes=60
        )
        
        # Find when intensity will be lowest
        min_forecast = min(forecasts, key=lambda f: f.predicted_intensity)
        
        return {
            "current_intensity_gco2kwh": current_intensity,
            "recommended_mode": eco_mode.value,
            "compute_limit_percent": config.compute_limit_percent,
            "quality_degradation_acceptable_percent": config.quality_degradation_acceptable * 100,
            "should_defer_new_tasks": eco_mode == EcoMode.EMERGENCY,
            "optimal_time_next_6h": {
                "timestamp": min_forecast.timestamp.isoformat(),
                "intensity_gco2kwh": min_forecast.predicted_intensity,
                "hours_from_now": (min_forecast.timestamp - datetime.now()).total_seconds() / 3600
            },
            "forecast_next_6h": [
                {
                    "timestamp": f.timestamp.isoformat(),
                    "intensity": f.predicted_intensity,
                    "confidence": f.confidence
                }
                for f in forecasts
            ]
        }


# Convenience function
def create_eco_mode_controller(
    carbon_forecaster,
    custom_thresholds: Optional[Dict[EcoMode, float]] = None
) -> EcoModeController:
    """Create eco-mode controller with forecaster"""
    
    return EcoModeController(
        carbon_forecaster=carbon_forecaster,
        carbon_thresholds=custom_thresholds
    )


if __name__ == "__main__":
    # Example usage
    import asyncio
    from .forecasting_engine import create_forecaster
    
    async def main():
        # Create forecaster
        forecaster = await create_forecaster(region="US-CA", train_days=30)
        
        # Create eco-mode controller
        controller = create_eco_mode_controller(forecaster)
        
        # Get current recommendation
        recommendation = await controller.get_current_recommendation()
        print(f"Current recommendation:")
        print(f"  Mode: {recommendation['recommended_mode']}")
        print(f"  Carbon intensity: {recommendation['current_intensity_gco2kwh']:.0f} gCO2/kWh")
        print(f"  Compute limit: {recommendation['compute_limit_percent']}%")
        
        # Apply throttling to task
        task = {
            "task_id": "example_task",
            "max_tokens": 1000,
            "temperature": 0.7,
            "top_k": 50,
            "deferrable": True,
            "estimated_duration_hours": 1.0,
            "estimated_energy_kwh": 0.002
        }
        
        decision = await controller.apply_throttling(task)
        
        print(f"\nThrottling decision:")
        print(f"  Eco-mode: {decision.eco_mode.value}")
        print(f"  Energy reduction: {decision.estimated_energy_reduction_percent:.1f}%")
        print(f"  Quality impact: {decision.estimated_quality_impact_percent:.1f}%")
        print(f"  Should defer: {decision.should_defer}")
        
        if decision.should_defer:
            print(f"  Defer until: {decision.defer_until}")
        
        print(f"\nThrottled task:")
        print(f"  Max tokens: {task['max_tokens']} → {decision.throttled_task['max_tokens']}")
        print(f"  Temperature: {task['temperature']} → {decision.throttled_task.get('temperature', 'N/A')}")
        print(f"  Top K: {task['top_k']} → {decision.throttled_task.get('top_k', 'N/A')}")
        
        # Statistics
        stats = controller.get_statistics()
        print(f"\nController statistics: {stats}")
    
    asyncio.run(main())
