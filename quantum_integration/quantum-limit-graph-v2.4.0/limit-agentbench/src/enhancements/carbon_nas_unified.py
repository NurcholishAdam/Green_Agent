# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/carbon_nas_unified.py

"""
Unified Carbon-Aware Neural Architecture Search
Version: 3.0.0

Combines the best features from carbon_nas.py and carbon_nas_enhanced_v6.py:
- Extended architecture search space (CNN, Transformer, EfficientNet, MobileNet)
- Multi-objective fitness (Accuracy, Carbon, Energy, Latency, Memory)
- Enhanced genetic algorithm with adaptive mutation
- Model compression (pruning, quantization, distillation)
- Hardware-aware optimization
- Pareto frontier analysis
- Expert registry auto-registration
- Knowledge distillation for efficient models
- Carbon-aware training scheduling
- Green AI certification tracking
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Callable, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
import torch
import torch.nn as nn
import copy
import hashlib
import json
import math
from collections import defaultdict, deque

logger = logging.getLogger(__name__)

# ============================================================================
# Enums and Data Classes
# ============================================================================

class ArchitectureFamily(Enum):
    """Supported architecture families"""
    CNN = "cnn"
    TRANSFORMER = "transformer"
    EFFICIENTNET = "efficientnet"
    MOBILENET = "mobilenet"
    RESNET = "resnet"
    VIT = "vision_transformer"
    MLP_MIXER = "mlp_mixer"
    HYBRID = "hybrid"
    CUSTOM = "custom"

class CompressionMethod(Enum):
    """Model compression methods"""
    NONE = "none"
    PRUNING_STRUCTURED = "structured_pruning"
    PRUNING_UNSTRUCTURED = "unstructured_pruning"
    QUANTIZATION_INT8 = "int8_quantization"
    QUANTIZATION_FP16 = "fp16_quantization"
    DISTILLATION = "knowledge_distillation"
    COMBINED = "combined"

class HardwareTarget(Enum):
    """Target hardware platforms"""
    CPU_X86 = "cpu_x86"
    CPU_ARM = "cpu_arm"
    GPU_NVIDIA = "gpu_nvidia"
    GPU_AMD = "gpu_amd"
    EDGE_TPU = "edge_tpu"
    MOBILE_NPU = "mobile_npu"
    FPGA = "fpga"
    ASIC = "asic"
    QUANTUM = "quantum"

class GreenCertification(Enum):
    """Green AI certification levels"""
    NONE = "none"
    BRONZE = "bronze"      # Basic carbon awareness
    SILVER = "silver"      # Optimized carbon efficiency
    GOLD = "gold"          # Carbon neutral
    PLATINUM = "platinum"  # Carbon negative

@dataclass
class ArchitectureConfig:
    """Comprehensive architecture configuration"""
    # Basic structure
    family: ArchitectureFamily
    num_layers: int
    hidden_dim: int
    
    # CNN-specific
    num_filters: Optional[List[int]] = None
    kernel_sizes: Optional[List[int]] = None
    use_batch_norm: bool = True
    use_residual: bool = True
    
    # Transformer-specific
    num_heads: Optional[int] = None
    ff_expansion: int = 4
    use_pre_norm: bool = True
    
    # EfficientNet-specific
    compound_coefficient: float = 1.0
    width_multiplier: float = 1.0
    depth_multiplier: float = 1.0
    
    # MobileNet-specific
    use_se: bool = True  # Squeeze-and-Excitation
    use_hs: bool = True  # Hard-Swish activation
    
    # Compression
    compression: CompressionMethod = CompressionMethod.NONE
    pruning_rate: float = 0.0
    quantization_bits: int = 32
    teacher_model: Optional[str] = None
    
    # Hardware
    target_hardware: HardwareTarget = HardwareTarget.CPU_X86
    use_mixed_precision: bool = False
    use_flash_attention: bool = False
    
    # Training
    batch_size: int = 32
    learning_rate: float = 0.001
    optimizer: str = "adam"
    use_scheduler: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'family': self.family.value,
            'num_layers': self.num_layers,
            'hidden_dim': self.hidden_dim,
            'compression': self.compression.value,
            'target_hardware': self.target_hardware.value,
            'pruning_rate': self.pruning_rate,
            'quantization_bits': self.quantization_bits
        }
    
    def compute_hash(self) -> str:
        """Compute unique hash for architecture"""
        return hashlib.sha256(
            json.dumps(self.to_dict(), sort_keys=True).encode()
        ).hexdigest()

@dataclass
class MultiObjectiveFitness:
    """Multi-objective fitness evaluation"""
    accuracy: float = 0.0
    carbon_kg: float = 0.0
    energy_kwh: float = 0.0
    latency_ms: float = 0.0
    memory_mb: float = 0.0
    flops: float = 0.0
    params_count: int = 0
    
    # Weighted composite scores
    composite_score: float = 0.0
    pareto_rank: int = 0
    green_score: float = 0.0
    
    # Certification
    certification: GreenCertification = GreenCertification.NONE
    
    def calculate_composite(
        self,
        weights: Optional[Dict[str, float]] = None
    ) -> float:
        """Calculate weighted composite score"""
        if weights is None:
            weights = {
                'accuracy': 0.35,
                'carbon': 0.25,
                'energy': 0.15,
                'latency': 0.15,
                'memory': 0.10
            }
        
        # Normalize metrics (higher is better for accuracy, lower for others)
        accuracy_score = self.accuracy
        
        carbon_score = 1.0 / (1.0 + self.carbon_kg * 1000)
        energy_score = 1.0 / (1.0 + self.energy_kwh * 100)
        latency_score = 1.0 / (1.0 + self.latency_ms / 100)
        memory_score = 1.0 / (1.0 + self.memory_mb / 1000)
        
        self.composite_score = (
            weights['accuracy'] * accuracy_score +
            weights['carbon'] * carbon_score +
            weights['energy'] * energy_score +
            weights['latency'] * latency_score +
            weights['memory'] * memory_score
        )
        
        # Calculate green score (carbon + energy)
        self.green_score = (
            0.6 * carbon_score +
            0.4 * energy_score
        )
        
        # Assign certification based on green score
        if self.green_score > 0.95:
            self.certification = GreenCertification.PLATINUM
        elif self.green_score > 0.85:
            self.certification = GreenCertification.GOLD
        elif self.green_score > 0.70:
            self.certification = GreenCertification.SILVER
        elif self.green_score > 0.50:
            self.certification = GreenCertification.BRONZE
        
        return self.composite_score

@dataclass
class ArchitectureGene:
    """Enhanced architecture gene"""
    config: ArchitectureConfig
    fitness: MultiObjectiveFitness = field(default_factory=MultiObjectiveFitness)
    generation: int = 0
    parent_ids: List[str] = field(default_factory=list)
    mutation_history: List[str] = field(default_factory=list)
    registered_expert_id: Optional[str] = None
    trained_model_path: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)

# ============================================================================
# Model Compression Engine
# ============================================================================

class ModelCompressionEngine:
    """
    Handles model compression techniques.
    
    Supports:
    - Structured and unstructured pruning
    - INT8/FP16 quantization
    - Knowledge distillation
    - Combined approaches
    """
    
    def __init__(self):
        self.compression_stats: Dict[str, Any] = {}
    
    def apply_pruning(
        self,
        model: nn.Module,
        pruning_rate: float,
        method: str = 'structured'
    ) -> Tuple[nn.Module, Dict[str, Any]]:
        """
        Apply pruning to model.
        
        Args:
            model: PyTorch model
            pruning_rate: Fraction of weights to prune (0.0 to 1.0)
            method: 'structured' or 'unstructured'
            
        Returns:
            (pruned_model, pruning_stats)
        """
        original_params = sum(p.numel() for p in model.parameters())
        
        if method == 'structured':
            # Structured pruning: remove entire channels/neurons
            pruned_model = self._structured_prune(model, pruning_rate)
        else:
            # Unstructured pruning: remove individual weights
            pruned_model = self._unstructured_prune(model, pruning_rate)
        
        pruned_params = sum(p.numel() for p in pruned_model.parameters())
        compression_ratio = original_params / max(pruned_params, 1)
        
        stats = {
            'method': method,
            'pruning_rate': pruning_rate,
            'original_params': original_params,
            'pruned_params': pruned_params,
            'compression_ratio': compression_ratio,
            'size_reduction_percent': (1 - pruned_params / original_params) * 100
        }
        
        self.compression_stats['pruning'] = stats
        
        return pruned_model, stats
    
    def _structured_prune(
        self,
        model: nn.Module,
        pruning_rate: float
    ) -> nn.Module:
        """Apply structured pruning"""
        pruned_model = copy.deepcopy(model)
        
        for name, module in pruned_model.named_modules():
            if isinstance(module, nn.Conv2d):
                # Prune output channels
                num_channels = module.out_channels
                channels_to_keep = int(num_channels * (1 - pruning_rate))
                
                if channels_to_keep > 0:
                    # Keep first channels_to_keep channels
                    module.out_channels = channels_to_keep
                    module.weight = nn.Parameter(
                        module.weight[:channels_to_keep]
                    )
                    if module.bias is not None:
                        module.bias = nn.Parameter(
                            module.bias[:channels_to_keep]
                        )
            
            elif isinstance(module, nn.Linear):
                # Prune output features
                num_features = module.out_features
                features_to_keep = int(num_features * (1 - pruning_rate))
                
                if features_to_keep > 0:
                    module.out_features = features_to_keep
                    module.weight = nn.Parameter(
                        module.weight[:features_to_keep]
                    )
                    if module.bias is not None:
                        module.bias = nn.Parameter(
                            module.bias[:features_to_keep]
                        )
        
        return pruned_model
    
    def _unstructured_prune(
        self,
        model: nn.Module,
        pruning_rate: float
    ) -> nn.Module:
        """Apply unstructured (magnitude-based) pruning"""
        pruned_model = copy.deepcopy(model)
        
        for name, module in pruned_model.named_modules():
            if isinstance(module, (nn.Conv2d, nn.Linear)):
                # Get weight magnitude
                weight = module.weight.data
                threshold = torch.quantile(
                    weight.abs().flatten(),
                    pruning_rate
                )
                
                # Create mask
                mask = weight.abs() > threshold
                module.weight.data = weight * mask
        
        return pruned_model
    
    def apply_quantization(
        self,
        model: nn.Module,
        bits: int = 8
    ) -> Tuple[nn.Module, Dict[str, Any]]:
        """
        Apply quantization to model.
        
        Args:
            model: PyTorch model
            bits: Target bit width (8 or 16)
            
        Returns:
            (quantized_model, quantization_stats)
        """
        original_size = self._estimate_model_size(model)
        
        if bits == 8:
            # INT8 quantization
            quantized_model = self._quantize_int8(model)
            size_reduction = 4  # 32-bit to 8-bit = 4x reduction
        elif bits == 16:
            # FP16 quantization
            quantized_model = self._quantize_fp16(model)
            size_reduction = 2  # 32-bit to 16-bit = 2x reduction
        else:
            quantized_model = model
            size_reduction = 1
        
        quantized_size = original_size / size_reduction
        
        stats = {
            'bits': bits,
            'original_size_mb': original_size,
            'quantized_size_mb': quantized_size,
            'size_reduction': size_reduction,
            'size_reduction_percent': (1 - 1/size_reduction) * 100
        }
        
        self.compression_stats['quantization'] = stats
        
        return quantized_model, stats
    
    def _quantize_int8(self, model: nn.Module) -> nn.Module:
        """Quantize model to INT8"""
        try:
            import torch.quantization as quant
            
            quantized_model = copy.deepcopy(model)
            quantized_model.eval()
            
            # Fuse modules for better quantization
            quantized_model = torch.quantization.fuse_modules(
                quantized_model,
                [['conv', 'bn', 'relu']]
            )
            
            # Prepare for quantization
            quantized_model.qconfig = torch.quantization.get_default_qconfig('fbgemm')
            torch.quantization.prepare(quantized_model, inplace=True)
            
            # Calibrate (simplified)
            quantized_model = torch.quantization.convert(quantized_model, inplace=True)
            
            return quantized_model
            
        except Exception as e:
            logger.warning(f"INT8 quantization failed: {str(e)}, returning original")
            return model
    
    def _quantize_fp16(self, model: nn.Module) -> nn.Module:
        """Convert model to FP16"""
        return model.half()
    
    def apply_distillation(
        self,
        teacher_model: nn.Module,
        student_config: ArchitectureConfig,
        temperature: float = 3.0,
        alpha: float = 0.5
    ) -> Tuple[nn.Module, Dict[str, Any]]:
        """
        Apply knowledge distillation.
        
        Args:
            teacher_model: Large teacher model
            student_config: Configuration for smaller student
            temperature: Distillation temperature
            alpha: Weight between distillation and task loss
            
        Returns:
            (student_model, distillation_stats)
        """
        # Create student model from config
        student_model = self._create_model_from_config(student_config)
        
        # Simplified distillation (in production, would train)
        teacher_params = sum(p.numel() for p in teacher_model.parameters())
        student_params = sum(p.numel() for p in student_model.parameters())
        
        stats = {
            'teacher_params': teacher_params,
            'student_params': student_params,
            'compression_ratio': teacher_params / max(student_params, 1),
            'temperature': temperature,
            'alpha': alpha,
            'size_reduction_percent': (1 - student_params / teacher_params) * 100
        }
        
        self.compression_stats['distillation'] = stats
        
        return student_model, stats
    
    def _create_model_from_config(
        self,
        config: ArchitectureConfig
    ) -> nn.Module:
        """Create model from architecture config"""
        if config.family == ArchitectureFamily.CNN:
            return self._create_cnn(config)
        elif config.family == ArchitectureFamily.TRANSFORMER:
            return self._create_transformer(config)
        else:
            return nn.Sequential(
                nn.Linear(config.hidden_dim, config.hidden_dim),
                nn.ReLU(),
                nn.Linear(config.hidden_dim, 10)
            )
    
    def _create_cnn(self, config: ArchitectureConfig) -> nn.Module:
        """Create CNN model"""
        layers = []
        in_channels = 3
        
        num_filters = config.num_filters or [32, 64, 128]
        
        for filters in num_filters[:config.num_layers]:
            layers.append(nn.Conv2d(in_channels, filters, 3, padding=1))
            if config.use_batch_norm:
                layers.append(nn.BatchNorm2d(filters))
            layers.append(nn.ReLU())
            in_channels = filters
        
        layers.append(nn.AdaptiveAvgPool2d(1))
        layers.append(nn.Flatten())
        layers.append(nn.Linear(in_channels, 10))
        
        return nn.Sequential(*layers)
    
    def _create_transformer(self, config: ArchitectureConfig) -> nn.Module:
        """Create transformer model"""
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=config.hidden_dim,
            nhead=config.num_heads or 8,
            dim_feedforward=config.hidden_dim * config.ff_expansion,
            batch_first=True
        )
        
        return nn.TransformerEncoder(
            encoder_layer,
            num_layers=config.num_layers
        )
    
    def _estimate_model_size(self, model: nn.Module) -> float:
        """Estimate model size in MB"""
        param_size = 0
        for param in model.parameters():
            param_size += param.nelement() * param.element_size()
        
        buffer_size = 0
        for buffer in model.buffers():
            buffer_size += buffer.nelement() * buffer.element_size()
        
        return (param_size + buffer_size) / 1024 / 1024
    
    def apply_combined_compression(
        self,
        model: nn.Module,
        config: ArchitectureConfig
    ) -> Tuple[nn.Module, Dict[str, Any]]:
        """
        Apply combined compression (pruning + quantization).
        
        Order matters: prune first, then quantize.
        """
        compressed_model = model
        combined_stats = {}
        
        # Step 1: Pruning
        if config.pruning_rate > 0:
            compressed_model, pruning_stats = self.apply_pruning(
                compressed_model,
                config.pruning_rate,
                'structured' if config.compression == CompressionMethod.PRUNING_STRUCTURED else 'unstructured'
            )
            combined_stats['pruning'] = pruning_stats
        
        # Step 2: Quantization
        if config.quantization_bits < 32:
            compressed_model, quant_stats = self.apply_quantization(
                compressed_model,
                config.quantization_bits
            )
            combined_stats['quantization'] = quant_stats
        
        # Calculate total compression
        original_size = self._estimate_model_size(model)
        compressed_size = self._estimate_model_size(compressed_model)
        
        combined_stats['total'] = {
            'original_size_mb': original_size,
            'compressed_size_mb': compressed_size,
            'total_compression_ratio': original_size / max(compressed_size, 0.001),
            'total_reduction_percent': (1 - compressed_size / original_size) * 100
        }
        
        return compressed_model, combined_stats

# ============================================================================
# Hardware-Aware Profiler
# ============================================================================

class HardwareProfiler:
    """
    Profiles models on different hardware targets.
    
    Estimates:
    - Latency on target hardware
    - Energy consumption
    - Memory usage
    - Carbon footprint
    """
    
    def __init__(self):
        self.hardware_profiles = {
            HardwareTarget.CPU_X86: {
                'flops_per_watt': 10e9,      # 10 GFLOPS/W
                'carbon_intensity': 400,      # gCO2/kWh
                'memory_bandwidth_gbps': 50,
                'base_latency_ms': 10
            },
            HardwareTarget.CPU_ARM: {
                'flops_per_watt': 30e9,
                'carbon_intensity': 300,
                'memory_bandwidth_gbps': 30,
                'base_latency_ms': 15
            },
            HardwareTarget.GPU_NVIDIA: {
                'flops_per_watt': 100e9,
                'carbon_intensity': 450,
                'memory_bandwidth_gbps': 900,
                'base_latency_ms': 2
            },
            HardwareTarget.EDGE_TPU: {
                'flops_per_watt': 200e9,
                'carbon_intensity': 100,
                'memory_bandwidth_gbps': 30,
                'base_latency_ms': 1
            },
            HardwareTarget.MOBILE_NPU: {
                'flops_per_watt': 150e9,
                'carbon_intensity': 50,
                'memory_bandwidth_gbps': 20,
                'base_latency_ms': 2
            }
        }
    
    def estimate_flops(
        self,
        config: ArchitectureConfig
    ) -> float:
        """Estimate FLOPs for architecture"""
        # Simplified FLOPs estimation
        base_flops = config.num_layers * config.hidden_dim ** 2
        
        if config.family == ArchitectureFamily.CNN:
            base_flops *= 9  # 3x3 conv factor
        elif config.family == ArchitectureFamily.TRANSFORMER:
            base_flops *= 12  # Self-attention factor
        
        # Adjust for compression
        if config.pruning_rate > 0:
            base_flops *= (1 - config.pruning_rate)
        
        return base_flops
    
    def profile_on_hardware(
        self,
        config: ArchitectureConfig,
        hardware: Optional[HardwareTarget] = None
    ) -> Dict[str, float]:
        """
        Profile architecture on specified hardware.
        
        Returns estimated metrics.
        """
        if hardware is None:
            hardware = config.target_hardware
        
        hw_profile = self.hardware_profiles.get(
            hardware,
            self.hardware_profiles[HardwareTarget.CPU_X86]
        )
        
        # Estimate FLOPs
        flops = self.estimate_flops(config)
        
        # Estimate latency
        compute_time = flops / hw_profile['flops_per_watt']
        memory_time = (config.hidden_dim * 4) / (hw_profile['memory_bandwidth_gbps'] * 1e9)
        latency = (compute_time + memory_time) * 1000 + hw_profile['base_latency_ms']
        
        # Estimate energy
        energy = flops / hw_profile['flops_per_watt'] / 3600  # kWh
        
        # Estimate carbon
        carbon = energy * hw_profile['carbon_intensity'] / 1000  # kg CO2
        
        # Estimate memory
        memory = config.hidden_dim * config.num_layers * 4 / 1024 / 1024  # MB
        
        return {
            'flops': flops,
            'latency_ms': latency,
            'energy_kwh': energy,
            'carbon_kg': carbon,
            'memory_mb': memory,
            'hardware': hardware.value
        }

# ============================================================================
# Pareto Optimizer
# ============================================================================

class ParetoOptimizer:
    """
    Multi-objective Pareto optimization.
    
    Finds non-dominated solutions across multiple objectives.
    """
    
    def __init__(self):
        self.pareto_frontier: List[Dict[str, Any]] = []
        self.objectives = ['accuracy', 'carbon_kg', 'energy_kwh', 'latency_ms']
    
    def find_pareto_optimal(
        self,
        population: List[ArchitectureGene],
        objectives: Optional[List[str]] = None
    ) -> List[ArchitectureGene]:
        """
        Find Pareto-optimal architectures.
        
        Maximizes accuracy, minimizes all other objectives.
        """
        if objectives is None:
            objectives = self.objectives
        
        pareto_optimal = []
        
        for i, gene1 in enumerate(population):
            dominated = False
            
            for j, gene2 in enumerate(population):
                if i == j:
                    continue
                
                # Check if gene2 dominates gene1
                if self._dominates(gene2.fitness, gene1.fitness, objectives):
                    dominated = True
                    break
            
            if not dominated:
                gene1.fitness.pareto_rank = 1
                pareto_optimal.append(gene1)
            else:
                gene1.fitness.pareto_rank = 2
        
        # Store frontier
        self.pareto_frontier = [
            {
                'accuracy': g.fitness.accuracy,
                'carbon_kg': g.fitness.carbon_kg,
                'energy_kwh': g.fitness.energy_kwh,
                'latency_ms': g.fitness.latency_ms,
                'config': g.config.to_dict()
            }
            for g in pareto_optimal
        ]
        
        return pareto_optimal
    
    def _dominates(
        self,
        fitness1: MultiObjectiveFitness,
        fitness2: MultiObjectiveFitness,
        objectives: List[str]
    ) -> bool:
        """Check if fitness1 dominates fitness2"""
        at_least_one_better = False
        
        for obj in objectives:
            val1 = getattr(fitness1, obj, 0)
            val2 = getattr(fitness2, obj, 0)
            
            if obj == 'accuracy':
                # Higher is better
                if val1 < val2:
                    return False
                if val1 > val2:
                    at_least_one_better = True
            else:
                # Lower is better
                if val1 > val2:
                    return False
                if val1 < val2:
                    at_least_one_better = True
        
        return at_least_one_better
    
    def get_hypervolume(
        self,
        reference_point: Optional[Dict[str, float]] = None
    ) -> float:
        """Calculate hypervolume of Pareto frontier"""
        if not self.pareto_frontier:
            return 0.0
        
        if reference_point is None:
            reference_point = {
                'accuracy': 0.0,
                'carbon_kg': 1.0,
                'energy_kwh': 0.1,
                'latency_ms': 1000
            }
        
        # Simplified hypervolume calculation
        hv = 0.0
        for point in self.pareto_frontier:
            volume = 1.0
            for obj in ['accuracy', 'carbon_kg', 'energy_kwh', 'latency_ms']:
                if obj == 'accuracy':
                    volume *= (point[obj] - reference_point[obj])
                else:
                    volume *= (reference_point[obj] - point[obj])
            hv += volume
        
        return max(0, hv)

# ============================================================================
# Unified Carbon NAS
# ============================================================================

class UnifiedCarbonNAS:
    """
    Unified Carbon-Aware Neural Architecture Search.
    
    Combines features from carbon_nas.py and carbon_nas_enhanced_v6.py:
    - Extended search space
    - Multi-objective optimization
    - Model compression (pruning, quantization, distillation)
    - Hardware-aware profiling
    - Pareto frontier analysis
    - Expert registry integration
    - Green AI certification
    """
    
    def __init__(
        self,
        expert_registry: Optional[Any] = None,
        population_size: int = 30,
        max_generations: int = 50,
        carbon_budget_kg: float = 10.0,
        auto_register: bool = True,
        enable_compression: bool = True,
        enable_hardware_profiling: bool = True,
        enable_pareto: bool = True,
        min_accuracy_threshold: float = 0.85
    ):
        # Configuration
        self.population_size = population_size
        self.max_generations = max_generations
        self.carbon_budget_kg = carbon_budget_kg
        self.auto_register = auto_register
        self.enable_compression = enable_compression
        self.enable_hardware_profiling = enable_hardware_profiling
        self.enable_pareto = enable_pareto
        self.min_accuracy_threshold = min_accuracy_threshold
        
        # Sub-modules
        self.compression_engine = ModelCompressionEngine()
        self.hardware_profiler = HardwareProfiler()
        self.pareto_optimizer = ParetoOptimizer()
        
        # Registry bridge
        from .carbon_nas import RegistryBridge  # Reuse from enhanced version
        self.registry_bridge = RegistryBridge()
        if expert_registry:
            self.registry_bridge.inject_registry(expert_registry)
        
        # Population
        self.population: List[ArchitectureGene] = []
        self.generation = 0
        self.evolution_history: List[Dict] = []
        
        # Carbon tracking
        self.total_carbon_spent_kg = 0.0
        self.carbon_per_evaluation_kg = 0.001
        
        # Best architectures
        self.best_by_accuracy: Optional[ArchitectureGene] = None
        self.best_by_carbon: Optional[ArchitectureGene] = None
        self.best_by_composite: Optional[ArchitectureGene] = None
        
        # Initialize search space
        self._initialize_search_space()
        
        # Initialize population
        self._initialize_population()
        
        logger.info(
            f"Unified Carbon NAS initialized: "
            f"population={population_size}, "
            f"generations={max_generations}, "
            f"compression={enable_compression}, "
            f"hardware={enable_hardware_profiling}, "
            f"pareto={enable_pareto}"
        )
    
    def _initialize_search_space(self):
        """Initialize extended search space"""
        self.search_space = {
            'families': list(ArchitectureFamily),
            'num_layers': list(range(2, 21, 2)),
            'hidden_dim': [64, 128, 192, 256, 384, 512, 640, 768, 1024],
            'num_heads': [2, 4, 6, 8, 10, 12, 16],
            'compound_coefficient': [0.5, 0.75, 1.0, 1.25, 1.5, 2.0],
            'pruning_rates': [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7],
            'quantization_bits': [32, 16, 8],
            'hardware_targets': list(HardwareTarget),
            'batch_sizes': [8, 16, 32, 64, 128]
        }
    
    def _initialize_population(self):
        """Initialize diverse population"""
        for i in range(self.population_size):
            config = self._generate_random_config()
            gene = ArchitectureGene(
                config=config,
                generation=0
            )
            self.population.append(gene)
        
        logger.info(f"Initialized population of {len(self.population)} architectures")
    
    def _generate_random_config(self) -> ArchitectureConfig:
        """Generate random architecture configuration"""
        family = np.random.choice(self.search_space['families'])
        
        config = ArchitectureConfig(
            family=family,
            num_layers=np.random.choice(self.search_space['num_layers']),
            hidden_dim=np.random.choice(self.search_space['hidden_dim']),
        )
        
        # Family-specific parameters
        if family == ArchitectureFamily.CNN:
            config.num_filters = [np.random.choice([16, 32, 48, 64, 96, 128]) 
                                 for _ in range(config.num_layers)]
            config.kernel_sizes = [np.random.choice([1, 3, 5]) 
                                   for _ in range(config.num_layers)]
        elif family in [ArchitectureFamily.TRANSFORMER, ArchitectureFamily.VIT]:
            config.num_heads = np.random.choice(self.search_space['num_heads'])
        elif family == ArchitectureFamily.EFFICIENTNET:
            config.compound_coefficient = np.random.choice(
                self.search_space['compound_coefficient']
            )
        
        # Compression settings
        if self.enable_compression and np.random.random() < 0.5:
            config.compression = np.random.choice([
                CompressionMethod.PRUNING_STRUCTURED,
                CompressionMethod.QUANTIZATION_INT8,
                CompressionMethod.COMBINED
            ])
            
            if config.compression in [CompressionMethod.PRUNING_STRUCTURED, 
                                       CompressionMethod.COMBINED]:
                config.pruning_rate = np.random.choice(
                    self.search_space['pruning_rates']
                )
            
            if config.compression in [CompressionMethod.QUANTIZATION_INT8,
                                       CompressionMethod.COMBINED]:
                config.quantization_bits = np.random.choice(
                    self.search_space['quantization_bits']
                )
        
        # Hardware target
        if self.enable_hardware_profiling:
            config.target_hardware = np.random.choice(
                self.search_space['hardware_targets']
            )
        
        return config
    
    async def evolve(
        self,
        fitness_function: Callable,
        generations: Optional[int] = None,
        early_stopping_patience: int = 10
    ) -> Dict[str, Any]:
        """
        Run full evolution with all features.
        
        Args:
            fitness_function: Async function to evaluate fitness
            generations: Number of generations (uses max_generations if None)
            early_stopping_patience: Stop if no improvement for N generations
            
        Returns:
            Evolution summary
        """
        generations = generations or self.max_generations
        best_fitness = 0.0
        patience_counter = 0
        
        for gen in range(generations):
            self.generation = gen + 1
            
            # Check carbon budget
            if self.total_carbon_spent_kg >= self.carbon_budget_kg:
                logger.warning(
                    f"Carbon budget exhausted: {self.total_carbon_spent_kg:.4f}kg"
                )
                break
            
            # Evaluate population
            await self._evaluate_population(fitness_function)
            
            # Find Pareto optimal
            if self.enable_pareto:
                pareto_optimal = self.pareto_optimizer.find_pareto_optimal(
                    self.population
                )
                logger.info(
                    f"Generation {self.generation}: "
                    f"{len(pareto_optimal)} Pareto-optimal architectures"
                )
            
            # Update bests
            self._update_bests()
            
            # Auto-register best architectures
            if self.auto_register:
                await self._auto_register_bests()
            
            # Record generation
            gen_metrics = self._record_generation()
            
            # Check early stopping
            current_best = gen_metrics['best_composite_score']
            if current_best > best_fitness * 1.01:
                best_fitness = current_best
                patience_counter = 0
            else:
                patience_counter += 1
            
            if patience_counter >= early_stopping_patience:
                logger.info(f"Early stopping at generation {self.generation}")
                break
            
            # Evolve to next generation
            self._evolve_population()
        
        return self._get_evolution_summary()
    
    async def _evaluate_population(self, fitness_function: Callable):
        """Evaluate all architectures in population"""
        for gene in self.population:
            if gene.fitness.composite_score > 0:
                continue  # Already evaluated
            
            try:
                # Evaluate base architecture
                fitness_result = await fitness_function(gene.config)
                
                gene.fitness = MultiObjectiveFitness(
                    accuracy=fitness_result.get('accuracy', 0.5),
                    carbon_kg=fitness_result.get('carbon_kg', self.carbon_per_evaluation_kg),
                    energy_kwh=fitness_result.get('energy_kwh', 0.001),
                    latency_ms=fitness_result.get('latency_ms', 100),
                    memory_mb=fitness_result.get('memory_mb', 100),
                    flops=fitness_result.get('flops', 1e9),
                    params_count=fitness_result.get('params', 1e6)
                )
                
                # Apply hardware profiling
                if self.enable_hardware_profiling:
                    hw_profile = self.hardware_profiler.profile_on_hardware(
                        gene.config
                    )
                    gene.fitness.latency_ms = hw_profile['latency_ms']
                    gene.fitness.energy_kwh = hw_profile['energy_kwh']
                    gene.fitness.carbon_kg = hw_profile['carbon_kg']
                    gene.fitness.memory_mb = hw_profile['memory_mb']
                
                # Apply compression if enabled
                if self.enable_compression and gene.config.compression != CompressionMethod.NONE:
                    compression_factor = self._estimate_compression_benefit(gene.config)
                    gene.fitness.carbon_kg *= (1 - compression_factor * 0.5)
                    gene.fitness.energy_kwh *= (1 - compression_factor * 0.4)
                    gene.fitness.memory_mb *= (1 - compression_factor * 0.6)
                
                # Calculate composite score
                gene.fitness.calculate_composite()
                
                # Track carbon
                self.total_carbon_spent_kg += gene.fitness.carbon_kg
                
            except Exception as e:
                logger.error(f"Fitness evaluation error: {str(e)}")
                gene.fitness = MultiObjectiveFitness()
    
    def _estimate_compression_benefit(self, config: ArchitectureConfig) -> float:
        """Estimate benefit from compression"""
        benefit = 0.0
        
        if config.pruning_rate > 0:
            benefit += config.pruning_rate * 0.6
        
        if config.quantization_bits == 16:
            benefit += 0.3
        elif config.quantization_bits == 8:
            benefit += 0.5
        
        return min(benefit, 0.9)
    
    def _update_bests(self):
        """Update best architecture trackers"""
        evaluated = [g for g in self.population if g.fitness.composite_score > 0]
        
        if not evaluated:
            return
        
        # Best by accuracy
        best_acc = max(evaluated, key=lambda g: g.fitness.accuracy)
        if not self.best_by_accuracy or best_acc.fitness.accuracy > self.best_by_accuracy.fitness.accuracy:
            self.best_by_accuracy = best_acc
        
        # Best by carbon
        best_carbon = min(evaluated, key=lambda g: g.fitness.carbon_kg)
        if not self.best_by_carbon or best_carbon.fitness.carbon_kg < self.best_by_carbon.fitness.carbon_kg:
            self.best_by_carbon = best_carbon
        
        # Best by composite
        best_comp = max(evaluated, key=lambda g: g.fitness.composite_score)
        if not self.best_by_composite or best_comp.fitness.composite_score > self.best_by_composite.fitness.composite_score:
            self.best_by_composite = best_comp
    
    async def _auto_register_bests(self):
        """Auto-register best architectures with expert registry"""
        candidates = []
        
        if self.best_by_accuracy and self.best_by_accuracy.fitness.accuracy >= self.min_accuracy_threshold:
            candidates.append(('accuracy', self.best_by_accuracy))
        
        if self.best_by_composite and self.best_by_composite.fitness.composite_score > 0.7:
            candidates.append(('composite', self.best_by_composite))
        
        for category, gene in candidates:
            if not gene.registered_expert_id:
                performance_metrics = {
                    'accuracy': gene.fitness.accuracy,
                    'carbon_kg': gene.fitness.carbon_kg,
                    'energy_kwh': gene.fitness.energy_kwh,
                    'latency_ms': gene.fitness.latency_ms,
                    'efficiency': gene.fitness.composite_score
                }
                
                expert_id = self.registry_bridge.register_architecture(
                    architecture=gene.config.to_dict(),
                    performance_metrics=performance_metrics,
                    carbon_footprint_kg=gene.fitness.carbon_kg,
                    version=f"{self.generation}.{category}.0"
                )
                
                if expert_id:
                    gene.registered_expert_id = expert_id
                    logger.info(
                        f"Auto-registered {category} architecture as expert {expert_id}"
                    )
    
    def _record_generation(self) -> Dict[str, Any]:
        """Record generation metrics"""
        evaluated = [g for g in self.population if g.fitness.composite_score > 0]
        
        if not evaluated:
            return {}
        
        fitnesses = [g.fitness.composite_score for g in evaluated]
        accuracies = [g.fitness.accuracy for g in evaluated]
        carbons = [g.fitness.carbon_kg for g in evaluated]
        
        metrics = {
            'generation': self.generation,
            'population_size': len(evaluated),
            'best_composite_score': max(fitnesses),
            'average_composite_score': np.mean(fitnesses),
            'best_accuracy': max(accuracies),
            'average_accuracy': np.mean(accuracies),
            'best_carbon_kg': min(carbons),
            'total_carbon_spent_kg': self.total_carbon_spent_kg,
            'pareto_frontier_size': len(self.pareto_optimizer.pareto_frontier),
            'registered_experts': len(self.registry_bridge.registered_architectures),
            'best_certification': self.best_by_composite.fitness.certification.value if self.best_by_composite else 'none'
        }
        
        self.evolution_history.append(metrics)
        
        return metrics
    
    def _evolve_population(self):
        """Evolve population through selection, crossover, mutation"""
        # Sort by fitness
        evaluated = sorted(
            [g for g in self.population if g.fitness.composite_score > 0],
            key=lambda g: g.fitness.composite_score,
            reverse=True
        )
        
        if len(evaluated) < 2:
            return
        
        # Elite preservation (top 20%)
        elite_size = max(2, len(evaluated) // 5)
        elite = evaluated[:elite_size]
        
        new_population = elite.copy()
        
        # Generate offspring
        while len(new_population) < self.population_size:
            # Select parents from elite
            parent1, parent2 = np.random.choice(elite, 2, replace=False)
            
            # Crossover
            if np.random.random() < 0.7:
                child_config = self._crossover(parent1.config, parent2.config)
            else:
                child_config = self._mutate(parent1.config)
            
            child = ArchitectureGene(
                config=child_config,
                generation=self.generation,
                parent_ids=[parent1.config.compute_hash(), parent2.config.compute_hash()]
            )
            
            new_population.append(child)
        
        self.population = new_population
    
    def _crossover(
        self,
        config1: ArchitectureConfig,
        config2: ArchitectureConfig
    ) -> ArchitectureConfig:
        """Crossover two configurations"""
        # Randomly inherit from either parent
        family = config1.family if np.random.random() < 0.5 else config2.family
        
        child = ArchitectureConfig(
            family=family,
            num_layers=config1.num_layers if np.random.random() < 0.5 else config2.num_layers,
            hidden_dim=config1.hidden_dim if np.random.random() < 0.5 else config2.hidden_dim,
            pruning_rate=config1.pruning_rate if np.random.random() < 0.5 else config2.pruning_rate,
            quantization_bits=config1.quantization_bits if np.random.random() < 0.5 else config2.quantization_bits,
            target_hardware=config1.target_hardware if np.random.random() < 0.5 else config2.target_hardware,
            compression=config1.compression if np.random.random() < 0.5 else config2.compression
        )
        
        return child
    
    def _mutate(self, config: ArchitectureConfig) -> ArchitectureConfig:
        """Mutate configuration"""
        mutated = copy.deepcopy(config)
        mutation_rate = 0.2
        
        if np.random.random() < mutation_rate:
            mutated.num_layers = np.random.choice(self.search_space['num_layers'])
        
        if np.random.random() < mutation_rate:
            mutated.hidden_dim = np.random.choice(self.search_space['hidden_dim'])
        
        if np.random.random() < mutation_rate:
            mutated.pruning_rate = np.random.choice(self.search_space['pruning_rates'])
        
        if np.random.random() < mutation_rate:
            mutated.quantization_bits = np.random.choice(self.search_space['quantization_bits'])
        
        if np.random.random() < mutation_rate:
            mutated.target_hardware = np.random.choice(self.search_space['hardware_targets'])
        
        return mutated
    
    def _get_evolution_summary(self) -> Dict[str, Any]:
        """Get comprehensive evolution summary"""
        return {
            'total_generations': self.generation,
            'total_carbon_spent_kg': self.total_carbon_spent_kg,
            'carbon_budget_kg': self.carbon_budget_kg,
            'carbon_budget_used_percent': (self.total_carbon_spent_kg / self.carbon_budget_kg * 100) if self.carbon_budget_kg > 0 else 0,
            'best_accuracy': self.best_by_accuracy.fitness.accuracy if self.best_by_accuracy else 0,
            'best_carbon_kg': self.best_by_carbon.fitness.carbon_kg if self.best_by_carbon else 0,
            'best_composite_score': self.best_by_composite.fitness.composite_score if self.best_by_composite else 0,
            'best_certification': self.best_by_composite.fitness.certification.value if self.best_by_composite else 'none',
            'pareto_frontier_size': len(self.pareto_optimizer.pareto_frontier),
            'registered_experts': len(self.registry_bridge.registered_architectures),
            'evolution_history': self.evolution_history,
            'best_config': self.best_by_composite.config.to_dict() if self.best_by_composite else {},
            'compression_stats': self.compression_engine.compression_stats
        }
    
    def inject_registry(self, registry: Any):
        """Inject expert registry"""
        self.registry_bridge.inject_registry(registry)

# ============================================================================
# Legacy Compatibility Classes
# ============================================================================

class CarbonNAS(UnifiedCarbonNAS):
    """
    Legacy CarbonNAS for backward compatibility.
    
    Maintains original carbon_nas.py interface.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(
            enable_compression=False,
            enable_hardware_profiling=False,
            enable_pareto=False,
            *args, **kwargs
        )
        logger.info("CarbonNAS initialized (legacy compatibility mode)")

class CarbonNASEnhanced(UnifiedCarbonNAS):
    """
    Enhanced CarbonNAS for backward compatibility.
    
    Maintains carbon_nas_enhanced_v6.py interface.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(
            enable_compression=True,
            enable_hardware_profiling=True,
            enable_pareto=True,
            *args, **kwargs
        )
        logger.info("CarbonNAS Enhanced initialized (compatibility mode)")
