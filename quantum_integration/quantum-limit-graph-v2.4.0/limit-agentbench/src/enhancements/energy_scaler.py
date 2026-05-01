# src/enhancements/energy_scaler.py

"""
Energy-Proportional Scaling for Green Agent
Scientific basis: Koomey's law and Dennard scaling observations

Reference: "Energy-Proportional Computing" (IEEE Computer, 2007)
"""

import math
import numpy as np
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class PrecisionLevel(Enum):
    """Available precision levels with energy characteristics"""
    FP32 = "fp32"
    FP16 = "fp16"
    BF16 = "bf16"
    INT8 = "int8"
    INT4 = "int4"
    BINARY = "binary"


@dataclass
class PrecisionCharacteristics:
    """Energy and accuracy characteristics for a precision level"""
    energy_per_flop_joules: float
    memory_bandwidth_gb_s: float
    model_size_reduction: float
    accuracy_impact_percent: float
    helium_footprint: float


@dataclass
class ScaledModel:
    """Output from energy-proportional scaling"""
    precision: PrecisionLevel
    parallelism: int
    expected_energy_joules: float
    expected_latency_ms: float
    accuracy_impact_percent: float
    helium_usage: float
    meets_constraints: bool
    scaling_factors: Dict[str, float]


@dataclass
class ScalingDecision:
    """Decision output from energy scaler"""
    optimal_precision: PrecisionLevel
    optimal_parallelism: int
    energy_savings_percent: float
    accuracy_tradeoff_percent: float
    helium_reduction_percent: float
    meets_power_budget: bool
    recommendation: str


class EnergyProportionalScaler:
    """
    Energy-proportional scaling optimizer.
    
    Scientific basis: Energy-proportional systems aim for P ∝ utilization.
    Dynamic precision scaling provides fine-grained energy control.
    
    Precision energy coefficients based on:
    - FP32: 1x baseline
    - FP16: 0.4x energy, 0.5x memory bandwidth
    - INT8: 0.1x energy, 0.25x memory bandwidth
    - INT4: 0.05x energy, 0.125x memory bandwidth
    """
    
    # Precision characteristics (typical for 5nm GPUs)
    PRECISION_CHARS = {
        PrecisionLevel.FP32: PrecisionCharacteristics(
            energy_per_flop_joules=1.5e-11,
            memory_bandwidth_gb_s=2000.0,
            model_size_reduction=1.0,
            accuracy_impact_percent=0.0,
            helium_footprint=0.95
        ),
        PrecisionLevel.FP16: PrecisionCharacteristics(
            energy_per_flop_joules=0.6e-11,
            memory_bandwidth_gb_s=2000.0,
            model_size_reduction=0.5,
            accuracy_impact_percent=2.0,
            helium_footprint=0.85
        ),
        PrecisionLevel.BF16: PrecisionCharacteristics(
            energy_per_flop_joules=0.7e-11,
            memory_bandwidth_gb_s=2000.0,
            model_size_reduction=0.5,
            accuracy_impact_percent=1.0,
            helium_footprint=0.85
        ),
        PrecisionLevel.INT8: PrecisionCharacteristics(
            energy_per_flop_joules=0.15e-11,
            memory_bandwidth_gb_s=1000.0,
            model_size_reduction=0.25,
            accuracy_impact_percent=5.0,
            helium_footprint=0.60
        ),
        PrecisionLevel.INT4: PrecisionCharacteristics(
            energy_per_flop_joules=0.075e-11,
            memory_bandwidth_gb_s=500.0,
            model_size_reduction=0.125,
            accuracy_impact_percent=15.0,
            helium_footprint=0.40
        ),
        PrecisionLevel.BINARY: PrecisionCharacteristics(
            energy_per_flop_joules=0.01e-11,
            memory_bandwidth_gb_s=100.0,
            model_size_reduction=0.03125,
            accuracy_impact_percent=30.0,
            helium_footprint=0.20
        )
    }
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.max_parallelism = self.config.get('max_parallelism', 8)
        self.min_parallelism = self.config.get('min_parallelism', 1)
        self.gpu_efficiency_curve = self.config.get('gpu_efficiency_curve', [1.0, 0.95, 0.85, 0.75, 0.65, 0.55, 0.45, 0.35])
        self.accuracy_tolerance = self.config.get('accuracy_tolerance', 0.10)  # 10% max drop
        
    def calculate_energy_proportionality(self, current_power_watts: float, 
                                         peak_power_watts: float) -> float:
        """
        Calculate energy proportionality factor.
        
        Ideal: 1.0 (P ∝ utilization)
        Poor: < 0.5 (significant static power)
        """
        utilization = current_power_watts / peak_power_watts if peak_power_watts > 0 else 0
        
        # Static power (power at idle)
        idle_power_watts = peak_power_watts * 0.3  # Assume 30% static power
        
        # Proportionality = (P_total - P_static) / (P_peak - P_static)
        dynamic_power = max(0, current_power_watts - idle_power_watts)
        max_dynamic_power = peak_power_watts - idle_power_watts
        
        proportionality = dynamic_power / max_dynamic_power if max_dynamic_power > 0 else 0
        
        return min(1.0, max(0.0, proportionality))
    
    def find_optimal_precision(self, energy_budget_joules: float, 
                               total_flops: float,
                               helium_zone: Optional[str] = None) -> PrecisionLevel:
        """
        Find optimal precision level given energy budget.
        
        Energy per FLOP equation:
        E_total = FLOPs * E_per_FLOP(precision) + overhead
        
        Args:
            energy_budget_joules: Maximum energy allowed
            total_flops: Total operations to perform
            helium_zone: Helium scarcity zone (red/critical triggers aggressive scaling)
            
        Returns:
            Optimal precision level
        """
        # Calculate required energy per FLOP from budget
        required_efficiency = energy_budget_joules / total_flops if total_flops > 0 else float('inf')
        
        # HELIUM OVERRIDE: More aggressive scaling during scarcity
        helium_multiplier = 1.0
        if helium_zone in ['red', 'critical']:
            helium_multiplier = 0.5  # Halve energy budget effectively
            logger.info(f"Helium {helium_zone} zone: applying aggressive scaling")
        
        adjusted_required_efficiency = required_efficiency * helium_multiplier
        
        # Find precision that meets efficiency requirement
        best_precision = PrecisionLevel.FP32
        best_efficiency = self.PRECISION_CHARS[PrecisionLevel.FP32].energy_per_flop_joules
        
        for precision, chars in self.PRECISION_CHARS.items():
            efficiency = chars.energy_per_flop_joules
            
            if efficiency <= adjusted_required_efficiency:
                # This precision meets the budget
                if chars.accuracy_impact_percent <= self.accuracy_tolerance * 100:
                    best_precision = precision
                    best_efficiency = efficiency
                    break
        
        logger.info(f"Optimal precision: {best_precision.value} (E/FLOP={best_efficiency:.2e}J, "
                   f"required={adjusted_required_efficiency:.2e}J)")
        
        return best_precision
    
    def calculate_optimal_parallelism(self, model_flops: float, 
                                      target_latency_ms: float,
                                      power_budget_watts: float) -> int:
        """
        Calculate optimal parallelism (number of active cores/GPUs).
        
        Amdahl's law with energy constraints:
        Speedup = 1 / ((1-P) + P/N)
        Power ~ N^α (α < 1 due to diminishing returns)
        
        Args:
            model_flops: Total FLOPs of the model
            target_latency_ms: Desired latency
            power_budget_watts: Power budget from decision core
            
        Returns:
            Optimal number of parallel units
        """
        # Calculate required FLOPs per second
        required_flops = (model_flops / target_latency_ms) * 1000 if target_latency_ms > 0 else 0
        
        # Find minimum parallelism meeting requirement
        optimal_parallelism = 1
        
        for i, efficiency in enumerate(self.gpu_efficiency_curve):
            parallelism = i + 1
            effective_flops = model_flops * efficiency * parallelism
            
            if effective_flops >= required_flops:
                optimal_parallelism = parallelism
                break
        
        # Power constraint check
        estimated_power = self._estimate_power_at_parallelism(optimal_parallelism, model_flops)
        
        if estimated_power > power_budget_watts and optimal_parallelism > 1:
            # Reduce parallelism to meet power budget
            for p in range(optimal_parallelism - 1, 0, -1):
                estimated_power = self._estimate_power_at_parallelism(p, model_flops)
                if estimated_power <= power_budget_watts:
                    optimal_parallelism = p
                    break
        
        # Clamp to valid range
        optimal_parallelism = max(self.min_parallelism, min(self.max_parallelism, optimal_parallelism))
        
        logger.info(f"Optimal parallelism: {optimal_parallelism} (est. power: {estimated_power:.1f}W, "
                   f"budget: {power_budget_watts:.1f}W)")
        
        return optimal_parallelism
    
    def _estimate_power_at_parallelism(self, parallelism: int, flops: float) -> float:
        """Estimate power consumption at given parallelism level"""
        if parallelism == 0:
            return 0.0
        
        # Each GPU has base power + dynamic power
        base_power_per_gpu = 50.0  # Watts (idle)
        dynamic_power_per_gpu = 200.0  # Watts at full load
        
        # Efficiency scaling (diminishing returns)
        efficiency_index = min(parallelism - 1, len(self.gpu_efficiency_curve) - 1)
        efficiency = self.gpu_efficiency_curve[efficiency_index] if efficiency_index >= 0 else 1.0
        
        # Utilization factor (flops relative to peak)
        peak_flops = parallelism * 1e12  # Assume 1 TFLOPS per GPU
        utilization = min(1.0, flops / peak_flops if peak_flops > 0 else 0)
        
        total_power = parallelism * (base_power_per_gpu + dynamic_power_per_gpu * utilization * efficiency)
        
        return total_power
    
    def scale_model(self, model_config: Dict, energy_budget_joules: float,
                   power_budget_watts: float, target_latency_ms: float,
                   helium_zone: Optional[str] = None) -> ScaledModel:
        """
        Main scaling function for energy-proportional execution.
        
        Args:
            model_config: Model configuration dictionary
            energy_budget_joules: Total energy budget
            power_budget_watts: Instantaneous power budget
            target_latency_ms: Target latency
            helium_zone: Helium scarcity zone
            
        Returns:
            ScaledModel with optimization decisions
        """
        total_flops = model_config.get('total_flops', 1e12)
        
        # Find optimal precision
        optimal_precision = self.find_optimal_precision(energy_budget_joules, total_flops, helium_zone)
        precision_chars = self.PRECISION_CHARS[optimal_precision]
        
        # Calculate energy with chosen precision
        compute_energy = total_flops * precision_chars.energy_per_flop_joules
        
        # Add overhead (memory, etc.)
        overhead_factor = 1.2
        expected_energy = compute_energy * overhead_factor
        
        # Check if energy budget is met
        meets_energy_budget = expected_energy <= energy_budget_joules
        
        # Calculate parallelism
        adjusted_flops = total_flops * precision_chars.model_size_reduction
        optimal_parallelism = self.calculate_optimal_parallelism(
            adjusted_flops, target_latency_ms, power_budget_watts
        )
        
        # Calculate expected latency
        efficiency = self.gpu_efficiency_curve[min(optimal_parallelism - 1, len(self.gpu_efficiency_curve) - 1)]
        effective_flops = adjusted_flops * efficiency * optimal_parallelism
        expected_latency_ms = (total_flops / effective_flops) * 1000 if effective_flops > 0 else 0
        
        # Calculate savings
        baseline_energy = total_flops * self.PRECISION_CHARS[PrecisionLevel.FP32].energy_per_flop_joules * 1.2
        energy_savings = (baseline_energy - expected_energy) / baseline_energy * 100
        
        # Helium reduction
        baseline_helium = self.PRECISION_CHARS[PrecisionLevel.FP32].helium_footprint
        helium_reduction = (baseline_helium - precision_chars.helium_footprint) / baseline_helium * 100
        
        scaling_factors = {
            'energy_ratio': expected_energy / baseline_energy if baseline_energy > 0 else 1.0,
            'latency_ratio': expected_latency_ms / target_latency_ms if target_latency_ms > 0 else 1.0,
            'precision_ratio': precision_chars.model_size_reduction,
            'parallelism_ratio': optimal_parallelism / self.max_parallelism
        }
        
        return ScaledModel(
            precision=optimal_precision,
            parallelism=optimal_parallelism,
            expected_energy_joules=expected_energy,
            expected_latency_ms=expected_latency_ms,
            accuracy_impact_percent=precision_chars.accuracy_impact_percent,
            helium_usage=precision_chars.helium_footprint,
            meets_constraints=meets_energy_budget and expected_latency_ms <= target_latency_ms * 1.5,
            scaling_factors=scaling_factors
        )
    
    def get_scaling_decision(self, workload_profile, execution_decision) -> ScalingDecision:
        """
        Generate scaling decision integrated with execution decision.
        
        This is the main interface for Layer 4 integration.
        """
        # Extract constraints
        power_budget = execution_decision.power_budget if hasattr(execution_decision, 'power_budget') else 1.0
        absolute_power_budget = power_budget * 300.0  # Assume 300W peak
        
        # Estimate workload FLOPs
        total_flops = self._estimate_workload_flops(workload_profile)
        
        # Energy budget from carbon/helium constraints
        energy_budget_joules = self._calculate_energy_budget(workload_profile, execution_decision)
        
        # Helium zone
        helium_zone = None
        if hasattr(execution_decision, 'helium_zone') and execution_decision.helium_zone:
            helium_zone = execution_decision.helium_zone.value
        
        # Target latency (default to 1 second per task)
        target_latency_ms = 1000.0
        
        # Scale model
        model_config = {
            'total_flops': total_flops,
            'model_size_gb': getattr(workload_profile, 'model_size_gb', 1.0)
        }
        
        scaled = self.scale_model(model_config, energy_budget_joules, absolute_power_budget,
                                  target_latency_ms, helium_zone)
        
        return ScalingDecision(
            optimal_precision=scaled.precision,
            optimal_parallelism=scaled.parallelism,
            energy_savings_percent=(1 - scaled.scaling_factors['energy_ratio']) * 100,
            accuracy_tradeoff_percent=scaled.accuracy_impact_percent,
            helium_reduction_percent=(1 - scaled.helium_usage / self.PRECISION_CHARS[PrecisionLevel.FP32].helium_footprint) * 100,
            meets_power_budget=scaled.meets_constraints,
            recommendation=self._generate_recommendation(scaled, execution_decision)
        )
    
    def _estimate_workload_flops(self, workload_profile) -> float:
        """Estimate total FLOPs for the workload"""
        model_size = getattr(workload_profile, 'model_size_gb', 1.0)
        training_steps = getattr(workload_profile, 'training_steps', 1000)
        batch_size = getattr(workload_profile, 'batch_size', 32)
        
        # Rough estimate: 2 * model_params * batch_size * steps
        model_params = model_size * 1e9 / 4  # 4 bytes per param
        flops = 2 * model_params * batch_size * training_steps
        
        return flops
    
    def _calculate_energy_budget(self, workload_profile, execution_decision) -> float:
        """Calculate energy budget in Joules from constraints"""
        # Start with baseline energy
        baseline_energy_joules = 1e6  # 1 MJ baseline
        
        # Adjust by power budget
        power_budget = execution_decision.power_budget if hasattr(execution_decision, 'power_budget') else 1.0
        adjusted_energy = baseline_energy_joules * power_budget
        
        # Adjust by helium zone
        if hasattr(execution_decision, 'helium_zone') and execution_decision.helium_zone:
            helium_zone = execution_decision.helium_zone.value
            if helium_zone == 'helium_critical':
                adjusted_energy *= 0.3
            elif helium_zone == 'helium_red':
                adjusted_energy *= 0.5
            elif helium_zone == 'helium_yellow':
                adjusted_energy *= 0.7
        
        return adjusted_energy
    
    def _generate_recommendation(self, scaled: ScaledModel, execution_decision) -> str:
        """Generate human-readable recommendation"""
        parts = []
        
        if scaled.meets_constraints:
            parts.append(f"Use {scaled.precision.value.upper()} precision with {scaled.parallelism} parallel units")
        else:
            parts.append(f"⚠️ Constraints may not be met with current settings")
        
        if scaled.energy_savings_percent > 30:
            parts.append(f"Potential {scaled.energy_savings_percent:.0f}% energy savings")
        
        if scaled.helium_reduction_percent > 30:
            parts.append(f"Helium reduction: {scaled.helium_reduction_percent:.0f}%")
        
        if scaled.accuracy_tradeoff_percent > 10:
            parts.append(f"⚠️ Accuracy impact: {scaled.accuracy_tradeoff_percent:.0f}%")
        
        return " | ".join(parts)
