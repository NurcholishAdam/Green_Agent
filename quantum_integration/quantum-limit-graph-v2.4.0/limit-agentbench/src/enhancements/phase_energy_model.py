# src/enhancements/phase_energy_model.py

"""
Phase-Aware Energy Modeling for ML Workloads
Scientific basis: Fine-grained energy modeling using hardware performance counters

Reference: "Phase-Aware Energy Modeling for Deep Learning" (MLSys, 2024)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import Enum
import numpy as np
import logging

logger = logging.getLogger(__name__)


class PhaseType(Enum):
    """Types of execution phases in ML workloads"""
    DATA_LOAD = "data_load"
    PREPROCESS = "preprocess"
    COMPUTE = "compute"
    COMMUNICATION = "communication"
    MEMORY_TRANSFER = "memory_transfer"
    CHECKPOINT = "checkpoint"
    SYNCHRONIZATION = "synchronization"
    IDLE = "idle"


@dataclass
class WorkloadPhase:
    """Individual phase of a workload"""
    type: PhaseType
    duration_ms: float
    flops: float
    bytes_transferred: float
    message_size_bytes: float
    arithmetic_intensity: float  # FLOPs/byte
    estimated_energy_joules: float
    optimization_potential: float  # 0-1


@dataclass
class PhaseEnergyProfile:
    """Complete phase energy profile for a workload"""
    total_energy_joules: float
    phase_breakdown: Dict[PhaseType, float]
    optimization_opportunities: List[Dict]
    predicted_energy_kwh: float
    confidence: float
    recommendations: List[str]


class PhaseAwareEnergyModel:
    """
    Phase-aware energy prediction model for ML workloads.
    
    Scientific basis: Different phases have vastly different energy profiles.
    - Compute phases: dominated by FLOPs (energy proportional to operations)
    - Communication phases: dominated by data movement
    - I/O phases: dominated by disk/network energy
    """
    
    # Energy coefficients (Joules per unit)
    ENERGY_PER_FLOP_FP32 = 1.5e-11  # 15 pJ/FLOP for 5nm
    ENERGY_PER_FLOP_FP16 = 0.8e-11  # 8 pJ/FLOP
    ENERGY_PER_BYTE_DRAM = 2.5e-11  # 25 pJ/byte
    ENERGY_PER_BYTE_NETWORK = 1.0e-10  # 100 pJ/byte (10x DRAM)
    ENERGY_PER_BYTE_DISK = 5.0e-9  # 5 nJ/byte
    ENERGY_PER_MESSAGE = 1.0e-6  # 1 μJ per message (overhead)
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.phase_history: List[List[WorkloadPhase]] = []
        self.calibration_factor = self.config.get('calibration_factor', 1.0)
        
    def decompose_workload(self, task_config: Dict) -> List[WorkloadPhase]:
        """
        Decompose workload into phases based on task configuration.
        
        Args:
            task_config: Task configuration from Layer 0
            
        Returns:
            List of WorkloadPhase objects
        """
        phases = []
        
        # Get workload characteristics
        model_size_gb = task_config.get('model_config', {}).get('size_gb', 1.0)
        data_volume_gb = task_config.get('data_volume_gb', 10.0)
        training_steps = task_config.get('training_steps', 1000)
        batch_size = task_config.get('batch_size', 32)
        gpu_count = task_config.get('hardware_requirements', {}).get('gpu_count', 1)
        
        # Estimate FLOPs per step
        flops_per_step = self._estimate_flops_per_step(model_size_gb, batch_size)
        total_flops = flops_per_step * training_steps
        
        # Phase 1: Data Loading
        data_load_phase = self._create_data_load_phase(data_volume_gb)
        phases.append(data_load_phase)
        
        # Phase 2: Preprocessing
        preprocess_phase = self._create_preprocess_phase(data_volume_gb)
        phases.append(preprocess_phase)
        
        # Phase 3: Compute (training steps)
        compute_phase = self._create_compute_phase(total_flops, training_steps, gpu_count)
        phases.append(compute_phase)
        
        # Phase 4: Communication (if multi-GPU)
        if gpu_count > 1:
            comm_phase = self._create_communication_phase(model_size_gb, training_steps, gpu_count)
            phases.append(comm_phase)
        
        # Phase 5: Checkpointing
        checkpoint_phase = self._create_checkpoint_phase(model_size_gb, training_steps)
        phases.append(checkpoint_phase)
        
        # Phase 6: Memory transfers
        memory_phase = self._create_memory_transfer_phase(model_size_gb, training_steps)
        phases.append(memory_phase)
        
        # Calculate energy for each phase
        for phase in phases:
            phase.estimated_energy_joules = self._calculate_phase_energy(phase)
        
        # Store in history
        self.phase_history.append(phases)
        if len(self.phase_history) > 100:
            self.phase_history = self.phase_history[-100:]
        
        return phases
    
    def _estimate_flops_per_step(self, model_size_gb: float, batch_size: int) -> float:
        """Estimate FLOPs per training step"""
        # Rough estimation: 2 * model_size * batch_size * 3 (forward+backward+update)
        model_params = model_size_gb * 1e9 / 4.0  # Assuming 4 bytes per parameter
        flops_per_step = 2 * model_params * batch_size * 3
        return flops_per_step
    
    def _create_data_load_phase(self, data_volume_gb: float) -> WorkloadPhase:
        """Create data loading phase"""
        # Time estimation: assume 1 GB/s read speed
        duration_ms = (data_volume_gb / 1.0) * 1000  # seconds to ms
        
        return WorkloadPhase(
            type=PhaseType.DATA_LOAD,
            duration_ms=duration_ms,
            flops=0,
            bytes_transferred=data_volume_gb * 1e9,
            message_size_bytes=0,
            arithmetic_intensity=0,
            estimated_energy_joules=0,
            optimization_potential=0.3  # Can be optimized via caching
        )
    
    def _create_preprocess_phase(self, data_volume_gb: float) -> WorkloadPhase:
        """Create preprocessing phase"""
        # Rough: 10^8 operations per GB
        flops = data_volume_gb * 1e8
        
        duration_ms = (data_volume_gb / 0.5) * 1000  # 0.5 GB/s processing
        
        return WorkloadPhase(
            type=PhaseType.PREPROCESS,
            duration_ms=duration_ms,
            flops=flops,
            bytes_transferred=data_volume_gb * 1e9 * 2,  # Read + write
            message_size_bytes=0,
            arithmetic_intensity=0.5,  # Mixed compute/IO
            estimated_energy_joules=0,
            optimization_potential=0.2
        )
    
    def _create_compute_phase(self, total_flops: float, steps: int, gpu_count: int) -> WorkloadPhase:
        """Create compute phase"""
        # Duration: assume 1e12 FLOPs per second per GPU
        flops_per_second_per_gpu = 1e12  # 1 TFLOPS per GPU (conservative)
        total_flops_per_second = flops_per_second_per_gpu * gpu_count
        
        duration_ms = (total_flops / total_flops_per_second) * 1000
        
        return WorkloadPhase(
            type=PhaseType.COMPUTE,
            duration_ms=duration_ms,
            flops=total_flops,
            bytes_transferred=0,
            message_size_bytes=0,
            arithmetic_intensity=total_flops / (total_flops * 2) if total_flops > 0 else 1.0,
            estimated_energy_joules=0,
            optimization_potential=0.4  # Can optimize via quantization/pruning
        )
    
    def _create_communication_phase(self, model_size_gb: float, steps: int, gpu_count: int) -> WorkloadPhase:
        """Create multi-GPU communication phase"""
        # All-reduce communication per step
        bytes_per_allreduce = model_size_gb * 1e9 * 2  # Send + receive
        total_bytes = bytes_per_allreduce * steps
        
        # Duration: assume 10 GB/s interconnect (NVLink)
        duration_ms = (total_bytes / 10e9) * 1000
        
        return WorkloadPhase(
            type=PhaseType.COMMUNICATION,
            duration_ms=duration_ms,
            flops=0,
            bytes_transferred=total_bytes,
            message_size_bytes=model_size_gb * 1e9,
            arithmetic_intensity=0,
            estimated_energy_joules=0,
            optimization_potential=0.5  # Can optimize via gradient compression
        )
    
    def _create_checkpoint_phase(self, model_size_gb: float, steps: int) -> WorkloadPhase:
        """Create checkpoint phase"""
        # Checkpoint every 100 steps
        checkpoint_frequency = 100
        num_checkpoints = steps // checkpoint_frequency
        
        total_bytes_written = model_size_gb * 1e9 * num_checkpoints
        
        # Duration: assume 0.5 GB/s write speed
        duration_ms = (total_bytes_written / 0.5e9) * 1000
        
        return WorkloadPhase(
            type=PhaseType.CHECKPOINT,
            duration_ms=duration_ms,
            flops=0,
            bytes_transferred=total_bytes_written,
            message_size_bytes=0,
            arithmetic_intensity=0,
            estimated_energy_joules=0,
            optimization_potential=0.6  # Can optimize via incremental checkpointing
        )
    
    def _create_memory_transfer_phase(self, model_size_gb: float, steps: int) -> WorkloadPhase:
        """Create memory transfer phase"""
        # Each step transfers data between CPU and GPU
        bytes_per_step = model_size_gb * 1e9 * 2  # Host to device + device to host
        total_bytes = bytes_per_step * steps
        
        # Duration: assume 20 GB/s PCIe bandwidth
        duration_ms = (total_bytes / 20e9) * 1000
        
        return WorkloadPhase(
            type=PhaseType.MEMORY_TRANSFER,
            duration_ms=duration_ms,
            flops=0,
            bytes_transferred=total_bytes,
            message_size_bytes=0,
            arithmetic_intensity=0,
            estimated_energy_joules=0,
            optimization_potential=0.3  # Can optimize via pinned memory
        )
    
    def _calculate_phase_energy(self, phase: WorkloadPhase) -> float:
        """Calculate energy for a specific phase in Joules"""
        energy = 0.0
        
        if phase.type == PhaseType.COMPUTE:
            # Energy from FLOPs
            energy += phase.flops * self.ENERGY_PER_FLOP_FP32
            
        elif phase.type in [PhaseType.COMMUNICATION, PhaseType.MEMORY_TRANSFER]:
            # Energy from data movement
            energy += phase.bytes_transferred * self.ENERGY_PER_BYTE_NETWORK
            
            # Add per-message overhead
            if phase.message_size_bytes > 0:
                num_messages = phase.bytes_transferred / phase.message_size_bytes
                energy += num_messages * self.ENERGY_PER_MESSAGE
        
        elif phase.type == PhaseType.DATA_LOAD:
            # Energy from disk I/O
            energy += phase.bytes_transferred * self.ENERGY_PER_BYTE_DISK
        
        elif phase.type == PhaseType.PREPROCESS:
            # Mixed: FLOPs + memory accesses
            energy += phase.flops * self.ENERGY_PER_FLOP_FP32
            energy += phase.bytes_transferred * self.ENERGY_PER_BYTE_DRAM
        
        elif phase.type == PhaseType.CHECKPOINT:
            # Energy from disk writes
            energy += phase.bytes_transferred * self.ENERGY_PER_BYTE_DISK
        
        # Apply calibration factor
        energy *= self.calibration_factor
        
        return energy
    
    def predict_phase_energy(self, task_config: Dict) -> PhaseEnergyProfile:
        """
        Predict energy consumption across phases for a given task.
        
        Returns:
            PhaseEnergyProfile with detailed breakdown
        """
        # Decompose workload
        phases = self.decompose_workload(task_config)
        
        # Calculate totals
        total_energy_joules = sum(p.estimated_energy_joules for p in phases)
        
        # Build phase breakdown
        breakdown = {}
        for phase in phases:
            breakdown[phase.type] = breakdown.get(phase.type, 0) + phase.estimated_energy_joules
        
        # Find optimization opportunities
        optimization_opportunities = []
        recommendations = []
        
        for phase in phases:
            if phase.optimization_potential > 0.3 and phase.estimated_energy_joules > total_energy_joules * 0.1:
                opportunity = {
                    'phase': phase.type.value,
                    'current_energy_joules': phase.estimated_energy_joules,
                    'potential_savings_joules': phase.estimated_energy_joules * phase.optimization_potential,
                    'optimization_strategy': self._get_optimization_strategy(phase.type)
                }
                optimization_opportunities.append(opportunity)
                
                # Add recommendation
                recommendations.append(
                    f"{phase.type.value}: {opportunity['optimization_strategy']} "
                    f"(potential {opportunity['potential_savings_joules']/1000:.1f} kJ savings)"
                )
        
        # Calculate confidence based on historical accuracy
        confidence = self._calculate_confidence()
        
        return PhaseEnergyProfile(
            total_energy_joules=total_energy_joules,
            phase_breakdown=breakdown,
            optimization_opportunities=optimization_opportunities,
            predicted_energy_kwh=total_energy_joules / 3.6e6,
            confidence=confidence,
            recommendations=recommendations
        )
    
    def _get_optimization_strategy(self, phase_type: PhaseType) -> str:
        """Get optimization strategy for a phase"""
        strategies = {
            PhaseType.DATA_LOAD: "Use caching and prefetching",
            PhaseType.PREPROCESS: "Use GPU-accelerated preprocessing",
            PhaseType.COMPUTE: "Apply quantization and pruning",
            PhaseType.COMMUNICATION: "Use gradient compression",
            PhaseType.MEMORY_TRANSFER: "Use pinned memory and async transfers",
            PhaseType.CHECKPOINT: "Use incremental checkpointing",
            PhaseType.SYNCHRONIZATION: "Reduce synchronization frequency",
            PhaseType.IDLE: "Use power gating"
        }
        return strategies.get(phase_type, "General optimization")
    
    def _calculate_confidence(self) -> float:
        """Calculate confidence in prediction based on historical accuracy"""
        if len(self.phase_history) < 5:
            return 0.7  # Default confidence with limited data
        
        # Simplified confidence calculation
        # In production, compare predicted vs actual energy
        return 0.85
    
    def update_calibration(self, actual_energy_joules: float, predicted_energy_joules: float):
        """Update calibration factor based on actual measurements"""
        if predicted_energy_joules > 0:
            ratio = actual_energy_joules / predicted_energy_joules
            # Exponential moving average
            self.calibration_factor = 0.9 * self.calibration_factor + 0.1 * ratio
            logger.info(f"Updated calibration factor: {self.calibration_factor:.3f}")
    
    def get_energy_hotspots(self, task_config: Dict) -> List[Dict]:
        """Identify energy hotspots for optimization"""
        profile = self.predict_phase_energy(task_config)
        
        # Sort phases by energy consumption
        hotspots = sorted(
            profile.optimization_opportunities,
            key=lambda x: x['potential_savings_joules'],
            reverse=True
        )
        
        return hotspots[:3]  # Top 3 hotspots
