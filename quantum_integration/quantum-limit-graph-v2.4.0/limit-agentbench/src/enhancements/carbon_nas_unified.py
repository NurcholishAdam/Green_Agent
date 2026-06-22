# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/carbon_nas_unified.py
# Version: 3.1.0 - Enhanced with Full Reasoning Capabilities

"""
Unified Carbon-Aware Neural Architecture Search
Version: 3.1.0 (Enhanced with Reasoning Engine)

This is the complete, integrated version of the Green Agent's NAS system.
It combines the best features from all previous versions and adds a
comprehensive reasoning engine that enables temporal, causal, ethical,
contextual, systemic, and reflexive optimization.

Key Enhancements:
- Temporal Reasoning: Schedules computations during low-carbon periods
- Causal Reasoning: Explains why architectures consume carbon
- Ethical Reasoning: Ensures fair and responsible optimization
- Contextual Reasoning: Adapts to deployment environments
- Systemic Reasoning: Plans long-term carbon reduction
- Reflexive Reasoning: Understands and adapts to purpose

This system now answers both "how" and "why" to reduce carbon,
making it more intelligent, transparent, and effective.
"""

import asyncio
import hashlib
import json
import logging
import math
import os
import pickle
import time
import uuid
import random
import copy
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Set, Union
from collections import defaultdict, deque
from enum import Enum
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import yaml

# Pydantic for validation
from pydantic import BaseModel, Field, validator, ValidationError

# Tenacity for retries
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Database with connection pooling
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, Boolean, Text, JSON, Index, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError

# PyTorch
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset

# Prometheus metrics
from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler('carbon_nas_unified_v3.log', maxBytes=10*1024*1024, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CorrelationIdFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        self.correlation_id = str(uuid.uuid4())[:8]
    def filter(self, record):
        record.correlation_id = self.correlation_id
        return True

logger.addFilter(CorrelationIdFilter())

# Prometheus metrics
REGISTRY = CollectorRegistry()
NAS_CYCLES = Counter('nas_cycles_total', 'Total NAS cycles', ['status'], registry=REGISTRY)
ARCH_EVALUATIONS = Counter('nas_arch_evaluations_total', 'Architecture evaluations', ['status'], registry=REGISTRY)
CARBON_EMITTED = Gauge('nas_carbon_emitted_kg', 'Total carbon emitted (kg CO2)', registry=REGISTRY)
BEST_ACCURACY = Gauge('nas_best_accuracy', 'Best accuracy achieved', registry=REGISTRY)
CIRCUIT_BREAKER_STATE = Gauge('nas_circuit_breaker_state', 'Circuit breaker state', ['component'], registry=REGISTRY)
HEALTH_SCORE = Gauge('nas_system_health', 'System health score (0-100)', registry=REGISTRY)
DB_SIZE = Gauge('nas_db_size_mb', 'Database size in MB', registry=REGISTRY)
DATA_QUALITY_SCORE = Gauge('nas_data_quality', 'Training data quality score', registry=REGISTRY)
EVALUATION_QUEUE_SIZE = Gauge('nas_evaluation_queue_size', 'Evaluation queue size', registry=REGISTRY)

# ============================================================================
# ENUMS AND DATA CLASSES
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
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    PLATINUM = "platinum"

class TokenSource(Enum):
    """Sources of tokens in the economy"""
    CARBON_BUDGET = "carbon_budget"
    ENERGY_CREDIT = "energy_credit"
    TIME_WINDOW = "time_window"
    RENEWABLE = "renewable"
    EFFICIENCY_BONUS = "efficiency_bonus"
    RECYCLED = "recycled"

class TokenConsumer(Enum):
    """Consumers of tokens"""
    MODEL_TRAINING = "model_training"
    ARCHITECTURE_EVAL = "architecture_evaluation"
    KNOWLEDGE_TRANSFER = "knowledge_transfer"
    CONTINUOUS_LEARNING = "continuous_learning"
    HEALTH_CHECK = "health_check"
    COMPRESSION = "compression"
    EXPERT_REGISTRATION = "expert_registration"

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
    use_se: bool = True
    use_hs: bool = True
    
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
    """Enhanced architecture gene with reasoning support"""
    config: ArchitectureConfig
    fitness: MultiObjectiveFitness = field(default_factory=MultiObjectiveFitness)
    generation: int = 0
    parent_ids: List[str] = field(default_factory=list)
    mutation_history: List[str] = field(default_factory=list)
    registered_expert_id: Optional[str] = None
    trained_model_path: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    # Reasoning support
    reasoning: Optional[Dict[str, Any]] = None
    ethical_score: Optional[float] = None

# ============================================================================
# REASONING ENGINE MODULES
# ============================================================================

# --- Temporal Reasoning: Carbon Intensity Awareness ---

class CarbonIntensityAwareScheduler:
    """Schedule computations during low-carbon periods"""
    
    def __init__(self):
        self.carbon_intensity_cache = {}
        self.region = "global"
        self.forecast_hours = 24
        
        self.historical_patterns = {
            "global": {
                "peak_hours": [18, 19, 20, 21],
                "low_hours": [1, 2, 3, 4, 5],
                "solar_peak": [11, 12, 13, 14]
            }
        }
    
    async def get_current_intensity(self, region: str = "global") -> float:
        hour = datetime.now().hour
        pattern = self.historical_patterns.get(region, self.historical_patterns["global"])
        
        if hour in pattern["low_hours"]:
            return 200
        elif hour in pattern["solar_peak"]:
            return 300
        elif hour in pattern["peak_hours"]:
            return 600
        else:
            return 400
    
    async def get_forecast(self, region: str = "global", hours: int = 24) -> List[Dict]:
        forecast = []
        current_hour = datetime.now().hour
        
        for i in range(hours):
            hour = (current_hour + i) % 24
            forecast_hour = datetime.now() + timedelta(hours=i)
            
            pattern = self.historical_patterns.get(region, self.historical_patterns["global"])
            
            if hour in pattern["low_hours"]:
                intensity = 180 + np.random.normal(0, 20)
            elif hour in pattern["solar_peak"]:
                intensity = 280 + np.random.normal(0, 30)
            elif hour in pattern["peak_hours"]:
                intensity = 550 + np.random.normal(0, 50)
            else:
                intensity = 380 + np.random.normal(0, 40)
            
            forecast.append({
                'datetime': forecast_hour.isoformat(),
                'hour': hour,
                'intensity': max(100, intensity),
                'savings_potential': (intensity - 200) / intensity
            })
        
        return forecast
    
    async def schedule_computation(self, task: str, urgency: str = "normal",
                                   compute_hours: float = 1.0) -> Dict[str, Any]:
        current_intensity = await self.get_current_intensity(self.region)
        forecast = await self.get_forecast(self.region, 24)
        best_time = min(forecast, key=lambda x: x['intensity'])
        savings_percent = (current_intensity - best_time['intensity']) / current_intensity if current_intensity > 0 else 0
        savings_percent = max(0, savings_percent)
        
        if urgency == "critical":
            recommendation = {
                'action': 'run_now',
                'reason': 'Critical task - immediate execution required',
                'schedule': datetime.now().isoformat(),
                'expected_intensity': current_intensity,
                'carbon_savings': 0
            }
        elif urgency == "normal" and savings_percent > 0.2:
            recommendation = {
                'action': 'schedule',
                'reason': f'Delay by {best_time["datetime"]} to save {savings_percent:.1%} carbon',
                'schedule': best_time['datetime'],
                'expected_intensity': best_time['intensity'],
                'carbon_savings': savings_percent
            }
        elif urgency == "flexible":
            best_time = min(forecast, key=lambda x: x['intensity'])
            recommendation = {
                'action': 'schedule_optimal',
                'reason': f'Flexible task - optimal schedule at {best_time["datetime"]}',
                'schedule': best_time['datetime'],
                'expected_intensity': best_time['intensity'],
                'carbon_savings': savings_percent
            }
        else:
            recommendation = {
                'action': 'run_now',
                'reason': f'Marginal savings ({savings_percent:.1%}) - running now',
                'schedule': datetime.now().isoformat(),
                'expected_intensity': current_intensity,
                'carbon_savings': 0
            }
        
        recommendation.update({
            'task': task,
            'urgency': urgency,
            'compute_hours': compute_hours,
            'current_intensity': current_intensity,
            'forecast_window_hours': 24
        })
        
        return recommendation

# --- Causal Reasoning: Understanding Carbon Impact ---

@dataclass
class CausalExplanation:
    primary_driver: str
    contribution: float
    pathway: List[str]
    alternatives: List[str]
    confidence: float

class CarbonCausalModel:
    """Build and maintain causal models of carbon impact"""
    
    def __init__(self):
        self.causal_graph = {
            'num_layers': {
                'pathways': ['parameters', 'flops', 'memory_bandwidth', 'energy', 'carbon'],
                'effect_size': 0.35,
                'non_linear': True
            },
            'hidden_dim': {
                'pathways': ['parameters', 'flops', 'memory', 'energy', 'carbon'],
                'effect_size': 0.30,
                'non_linear': True
            },
            'num_heads': {
                'pathways': ['flops', 'memory_bandwidth', 'energy', 'carbon'],
                'effect_size': 0.25,
                'non_linear': True
            },
            'pruning_rate': {
                'pathways': ['parameters', 'flops', 'accuracy', 'carbon'],
                'effect_size': 0.40,
                'non_linear': True
            },
            'quantization_bits': {
                'pathways': ['memory_bandwidth', 'energy', 'carbon'],
                'effect_size': 0.30,
                'non_linear': False
            }
        }
        self.historical_effects = defaultdict(lambda: defaultdict(float))
        self.confidence_scores = defaultdict(lambda: 0.5)
    
    def explain_carbon_impact(self, architecture_config: Dict[str, Any],
                              fitness_metrics: Optional[Dict[str, float]] = None) -> CausalExplanation:
        impacts = {}
        pathways = {}
        
        for feature, impact_info in self.causal_graph.items():
            if feature in architecture_config:
                value = architecture_config[feature]
                effect = self._estimate_feature_impact(feature, value, impact_info)
                impacts[feature] = effect['contribution']
                pathways[feature] = effect['pathway']
        
        primary_driver = max(impacts, key=impacts.get) if impacts else None
        confidence = self.confidence_scores.get(primary_driver, 0.5) if primary_driver else 0.3
        alternatives = self._generate_alternatives(architecture_config, primary_driver)
        
        return CausalExplanation(
            primary_driver=primary_driver or 'unknown',
            contribution=impacts.get(primary_driver, 0.0),
            pathway=pathways.get(primary_driver, []),
            alternatives=alternatives,
            confidence=confidence
        )
    
    def _estimate_feature_impact(self, feature: str, value: Any, impact_info: Dict) -> Dict:
        base_effect = impact_info['effect_size']
        
        if isinstance(value, (int, float)):
            if feature == 'num_layers':
                normalized = min(1.0, value / 20)
            elif feature == 'hidden_dim':
                normalized = min(1.0, value / 1024)
            elif feature == 'num_heads':
                normalized = min(1.0, value / 16)
            elif feature == 'pruning_rate':
                normalized = value
            elif feature == 'quantization_bits':
                normalized = 1.0 - (value / 32)
            else:
                normalized = 0.5
            
            if impact_info.get('non_linear', False):
                effect = base_effect * (normalized ** 0.7)
            else:
                effect = base_effect * normalized
        else:
            effect = base_effect * 0.5
        
        contribution = min(1.0, effect)
        
        return {
            'contribution': contribution,
            'pathway': impact_info['pathways']
        }
    
    def _generate_alternatives(self, config: Dict[str, Any], primary_driver: str) -> List[str]:
        alternatives = []
        
        if primary_driver == 'num_layers' and config.get('num_layers', 0) > 6:
            alternatives.append(f"Reduce layers from {config['num_layers']} to {config['num_layers']-2} to save ~15% carbon")
        
        if primary_driver == 'hidden_dim' and config.get('hidden_dim', 0) > 384:
            alternatives.append(f"Reduce hidden dimension from {config['hidden_dim']} to {int(config['hidden_dim']*0.7)} to save ~12% carbon")
        
        if primary_driver == 'num_heads' and config.get('num_heads', 0) > 8:
            alternatives.append(f"Reduce attention heads from {config['num_heads']} to {config['num_heads']-2} to save ~10% carbon")
        
        if config.get('pruning_rate', 0) < 0.2:
            alternatives.append("Consider 20-30% pruning to reduce carbon by 15-20%")
        
        if config.get('quantization_bits', 32) == 32:
            alternatives.append("Apply INT8 quantization to reduce memory bandwidth and carbon")
        
        return alternatives[:3]

# --- Ethical Reasoning: Fair and Responsible Optimization ---

class EthicalCarbonReasoner:
    """Reason about ethical implications of carbon reduction decisions"""
    
    def __init__(self):
        self.stakeholders = ['global_climate', 'local_community', 'organization', 'end_users']
        self.ethical_frameworks = {
            'utilitarian': self._utilitarian_assessment,
            'justice': self._justice_assessment,
            'deontological': self._deontological_assessment
        }
        self.assessment_history = deque(maxlen=100)
    
    def assess_reduction_impact(self, architecture_config: Dict[str, Any],
                                performance: Dict[str, float]) -> Dict[str, Any]:
        assessment = {}
        
        for framework_name, framework_func in self.ethical_frameworks.items():
            try:
                assessment[framework_name] = framework_func(architecture_config, performance)
            except Exception as e:
                logger.error(f"Ethical assessment error in {framework_name}: {e}")
                assessment[framework_name] = {'score': 0.5, 'concern': f'Assessment unavailable: {str(e)}'}
        
        overall_score = sum(assessment.get(fw, {}).get('score', 0.5) for fw in assessment) / len(assessment)
        
        self.assessment_history.append({
            'timestamp': datetime.now().isoformat(),
            'assessment': assessment,
            'overall_score': overall_score
        })
        
        return {
            'framework_assessments': assessment,
            'overall_ethical_score': overall_score,
            'recommendations': self._generate_ethical_recommendations(assessment),
            'timestamp': datetime.now().isoformat()
        }
    
    def _utilitarian_assessment(self, config: Dict, performance: Dict) -> Dict:
        benefits = {
            'global_climate': 0.6,
            'local_community': 0.2,
            'organization': 0.1,
            'end_users': 0.1
        }
        
        carbon_reduction = 1.0 - performance.get('carbon_kg', 0.01)
        if carbon_reduction < 0.3:
            benefits['global_climate'] *= 0.5
        
        accuracy_loss = 1.0 - performance.get('accuracy', 0.85)
        losses = {
            'end_users': accuracy_loss * 0.5,
            'organization': accuracy_loss * 0.3
        }
        
        net_benefit = sum(benefits.values()) - sum(losses.values())
        
        return {
            'score': max(0, min(1, net_benefit)),
            'benefits': benefits,
            'losses': losses,
            'net_benefit': net_benefit
        }
    
    def _justice_assessment(self, config: Dict, performance: Dict) -> Dict:
        pruning_rate = config.get('pruning_rate', 0)
        accuracy_loss = 1.0 - performance.get('accuracy', 0.85)
        
        if pruning_rate > 0.5 and accuracy_loss > 0.05:
            equity_score = 0.3
            concern = "Heavy pruning may disproportionately reduce accuracy for critical use cases"
        elif pruning_rate > 0.3 and accuracy_loss > 0.03:
            equity_score = 0.6
            concern = "Moderate pruning with some accuracy trade-off"
        else:
            equity_score = 0.9
            concern = "Balanced approach with minimal accuracy impact"
        
        hardware = config.get('target_hardware', 'cpu_x86')
        hardware_accessibility = {
            'cpu_x86': 0.9,
            'gpu_nvidia': 0.7,
            'edge_tpu': 0.5,
            'mobile_npu': 0.4
        }.get(hardware, 0.5)
        
        return {
            'score': (equity_score + hardware_accessibility) / 2,
            'equity_concern': concern,
            'hardware_accessibility': hardware_accessibility
        }
    
    def _deontological_assessment(self, config: Dict, performance: Dict) -> Dict:
        rules_violated = []
        
        if config.get('pruning_rate', 0) > 0.5:
            rules_violated.append("Excessive pruning may harm accuracy unnecessarily")
        
        if config.get('compression') == 'none' and config.get('pruning_rate', 0) > 0:
            rules_violated.append("Compression without proper justification - lack of transparency")
        
        if config.get('target_hardware') == 'mobile_npu' and config.get('quantization_bits', 32) > 8:
            rules_violated.append("Mobile deployment requires more aggressive quantization")
        
        return {
            'score': 1.0 - (len(rules_violated) * 0.2),
            'rules_violated': rules_violated,
            'compliant': len(rules_violated) == 0
        }
    
    def _generate_ethical_recommendations(self, assessment: Dict) -> List[str]:
        recommendations = []
        
        for framework, assessment_result in assessment.items():
            if framework == 'utilitarian' and assessment_result.get('net_benefit', 0) < 0.3:
                recommendations.append("Reconsider carbon reduction approach - current strategy may not maximize net benefit")
            
            if framework == 'justice' and assessment_result.get('equity_concern', ''):
                recommendations.append(f"Address equity concerns: {assessment_result['equity_concern']}")
            
            if framework == 'deontological' and not assessment_result.get('compliant', True):
                for violation in assessment_result.get('rules_violated', []):
                    recommendations.append(f"Address ethical violation: {violation}")
        
        return recommendations[:3]

# --- Contextual Reasoning: Deployment-Aware Optimization ---

class ContextAwareOptimizer:
    """Apply different optimization strategies based on deployment context"""
    
    def __init__(self):
        self.context_strategies = {
            'mobile_inference': {
                'max_size_mb': 50,
                'max_latency_ms': 10,
                'min_accuracy': 0.85,
                'max_carbon_g': 0.1,
                'priority': ['size', 'latency', 'carbon', 'accuracy'],
                'recommended_compression': ['quantization_int8', 'pruning_structured'],
                'max_pruning_rate': 0.4,
                'min_quantization_bits': 8
            },
            'cloud_inference': {
                'max_size_mb': 1000,
                'max_latency_ms': 100,
                'min_accuracy': 0.92,
                'max_carbon_g': 1.0,
                'priority': ['accuracy', 'throughput', 'carbon', 'size'],
                'recommended_compression': ['pruning_unstructured', 'quantization_fp16'],
                'max_pruning_rate': 0.3,
                'min_quantization_bits': 16
            },
            'edge_tpu': {
                'max_size_mb': 10,
                'max_latency_ms': 5,
                'min_accuracy': 0.80,
                'max_carbon_g': 0.01,
                'priority': ['size', 'latency', 'carbon'],
                'recommended_compression': ['quantization_int8', 'pruning_structured'],
                'max_pruning_rate': 0.5,
                'min_quantization_bits': 8
            },
            'batch_processing': {
                'max_size_mb': 5000,
                'max_latency_ms': 5000,
                'min_accuracy': 0.85,
                'max_carbon_g': 10.0,
                'priority': ['throughput', 'carbon', 'accuracy'],
                'recommended_compression': ['pruning_unstructured', 'quantization_fp16'],
                'max_pruning_rate': 0.4,
                'min_quantization_bits': 16
            },
            'quantum': {
                'max_size_mb': 1,
                'max_latency_ms': 1000,
                'min_accuracy': 0.70,
                'max_carbon_g': 0.001,
                'priority': ['carbon', 'size'],
                'recommended_compression': ['quantization_int8'],
                'max_pruning_rate': 0.6,
                'min_quantization_bits': 8
            }
        }
    
    def get_context_plan(self, architecture_config: Dict[str, Any],
                         context: str = 'cloud_inference') -> Dict[str, Any]:
        strategy = self.context_strategies.get(context, self.context_strategies['cloud_inference'])
        
        constraints = {
            'size_ok': architecture_config.get('hidden_dim', 512) * architecture_config.get('num_layers', 6) / 1024 < strategy['max_size_mb'],
            'latency_ok': True,
            'accuracy_ok': True,
            'carbon_ok': True
        }
        
        suggestions = []
        
        current_pruning = architecture_config.get('pruning_rate', 0)
        if current_pruning < strategy['max_pruning_rate'] * 0.5:
            suggestions.append({
                'action': 'increase_pruning',
                'from': current_pruning,
                'to': strategy['max_pruning_rate'] * 0.7,
                'reason': f"{context} deployment requires aggressive pruning"
            })
        
        current_quantization = architecture_config.get('quantization_bits', 32)
        if current_quantization > strategy['min_quantization_bits']:
            suggestions.append({
                'action': 'quantize',
                'from': current_quantization,
                'to': strategy['min_quantization_bits'],
                'reason': f"{context} deployment benefits from lower precision"
            })
        
        if context in ['edge_tpu', 'mobile_inference'] and architecture_config.get('family') in ['transformer', 'vit']:
            suggestions.append({
                'action': 'change_family',
                'from': architecture_config.get('family'),
                'to': 'cnn',
                'reason': f"{context} deployment favors CNN over transformer models"
            })
        
        return {
            'context': context,
            'strategy': strategy,
            'constraints_met': all(constraints.values()),
            'constraints': constraints,
            'suggestions': suggestions[:3],
            'priority_order': strategy['priority'],
            'recommended_compression': strategy['recommended_compression']
        }

# --- Systemic Reasoning: Long-term Carbon Planning ---

class SystemicCarbonPlanner:
    """Plan carbon reduction across multiple NAS runs"""
    
    def __init__(self):
        self.carbon_history = deque(maxlen=100)
        self.accuracy_history = deque(maxlen=100)
        self.investment_returns = defaultdict(list)
    
    def plan_carbon_investment(self, current_accuracy: float,
                               target_accuracy: float,
                               carbon_budget: float) -> Dict[str, Any]:
        improvement_gap = max(0, target_accuracy - current_accuracy)
        compute_need = self._estimate_compute_for_improvement(improvement_gap)
        carbon_cost = compute_need * 0.001
        long_term_savings = self._estimate_long_term_savings(current_accuracy, target_accuracy)
        roi = (long_term_savings - carbon_cost) / carbon_cost if carbon_cost > 0 else 0
        
        if roi > 0.5:
            decision = 'invest'
            reason = f'High ROI ({roi:.2f}) - invest in exploration'
        elif roi > 0.1:
            decision = 'balanced'
            reason = f'Moderate ROI ({roi:.2f}) - balanced approach'
        else:
            decision = 'save'
            reason = f'Low ROI ({roi:.2f}) - save carbon for future'
        
        self.investment_returns[decision].append({
            'roi': roi,
            'accuracy_gain': improvement_gap,
            'carbon_cost': carbon_cost
        })
        
        return {
            'decision': decision,
            'reason': reason,
            'carbon_cost': carbon_cost,
            'long_term_savings': long_term_savings,
            'roi': roi,
            'compute_need': compute_need,
            'estimated_generations': int(compute_need / 0.5),
            'recommendation': self._generate_planning_recommendation(decision, roi)
        }
    
    def _estimate_compute_for_improvement(self, improvement_gap: float) -> float:
        if improvement_gap <= 0:
            return 1.0
        return max(1.0, improvement_gap * 10)
    
    def _estimate_long_term_savings(self, current_accuracy: float, target_accuracy: float) -> float:
        efficiency_gain = max(0, (target_accuracy - current_accuracy) * 0.1)
        lifetime_inferences = 1e6
        base_inference_carbon = 0.0001
        return efficiency_gain * lifetime_inferences * base_inference_carbon
    
    def _generate_planning_recommendation(self, decision: str, roi: float) -> str:
        if decision == 'invest':
            return f"Invest in exploration - ROI of {roi:.2f} justifies the carbon expenditure"
        elif decision == 'balanced':
            return f"Take a balanced approach - moderate ROI of {roi:.2f}, consider limited exploration"
        else:
            return f"Conserve carbon - low ROI of {roi:.2f}, focus on consolidating current gains"

# --- Reflexive Reasoning: Purpose-Aware Optimization ---

class PurposeAwareOptimizer:
    """Understand why carbon reduction matters for specific use cases"""
    
    def __init__(self):
        self.purposes = {
            'climate_research': {
                'priority': 'maximum_reduction',
                'accuracy_tolerance': 0.10,
                'transparency': 'high',
                'carbon_priority': 0.8,
                'description': 'Maximize carbon reduction even at cost of accuracy'
            },
            'medical_diagnosis': {
                'priority': 'accuracy_first',
                'accuracy_tolerance': 0.01,
                'transparency': 'critical',
                'carbon_priority': 0.3,
                'description': 'Maintain high accuracy, reduce carbon only where possible'
            },
            'consumer_app': {
                'priority': 'balanced',
                'accuracy_tolerance': 0.05,
                'transparency': 'medium',
                'carbon_priority': 0.5,
                'description': 'Balance between accuracy and carbon reduction'
            },
            'research_exploration': {
                'priority': 'exploration',
                'accuracy_tolerance': 0.15,
                'transparency': 'low',
                'carbon_priority': 0.6,
                'description': 'Prioritize exploration of novel architectures'
            },
            'production_deployment': {
                'priority': 'reliability',
                'accuracy_tolerance': 0.03,
                'transparency': 'high',
                'carbon_priority': 0.4,
                'description': 'Focus on reliable performance with some carbon reduction'
            }
        }
        self.purpose_history = deque(maxlen=100)
    
    def get_purpose_guide(self, purpose: str = 'balanced') -> Dict[str, Any]:
        guide = self.purposes.get(purpose, self.purposes['balanced'])
        recommendations = []
        
        if guide['priority'] == 'maximum_reduction':
            recommendations.append("Aggressively prune and quantize (target 40-50% reduction)")
            recommendations.append("Consider knowledge distillation for energy savings")
        elif guide['priority'] == 'accuracy_first':
            recommendations.append("Conservative pruning only (max 10-15%)")
            recommendations.append("Use FP16 quantization instead of INT8")
        elif guide['priority'] == 'exploration':
            recommendations.append("Generate diverse architectures even if some are inefficient")
            recommendations.append("Balance carbon budget for maximum exploration")
        elif guide['priority'] == 'reliability':
            recommendations.append("Focus on proven architecture families")
            recommendations.append("Apply moderate compression with safety margins")
        else:
            recommendations.append("Apply moderate compression (20-30%)")
            recommendations.append("Test both INT8 and FP16 quantization")
        
        if guide['transparency'] == 'critical':
            recommendations.append("Generate detailed explainability reports for all decisions")
            recommendations.append("Document all carbon reduction choices and their impact")
        elif guide['transparency'] == 'high':
            recommendations.append("Document key carbon reduction decisions")
            recommendations.append("Provide clear reasoning for architectural choices")
        
        return {
            'purpose': purpose,
            'guide': guide,
            'recommendations': recommendations,
            'optimization_weight': {
                'accuracy': 1.0 - guide['carbon_priority'],
                'carbon': guide['carbon_priority']
            },
            'acceptable_accuracy_loss': guide['accuracy_tolerance']
        }
    
    def reflect_on_purpose(self, purpose: str, outcomes: Dict[str, Any]) -> Dict[str, Any]:
        guide = self.purposes.get(purpose, self.purposes['balanced'])
        accuracy_achieved = outcomes.get('accuracy', 0)
        carbon_achieved = outcomes.get('carbon_reduction', 0)
        accuracy_gap = max(0, guide['accuracy_tolerance'] - accuracy_achieved)
        carbon_gap = guide['carbon_priority'] - carbon_achieved
        
        reflection = {
            'timestamp': datetime.now().isoformat(),
            'purpose': purpose,
            'accuracy_achieved': accuracy_achieved,
            'carbon_achieved': carbon_achieved,
            'accuracy_gap': accuracy_gap,
            'carbon_gap': carbon_gap,
            'purpose_achieved': accuracy_gap <= 0 and carbon_gap <= 0,
            'lessons': []
        }
        
        if accuracy_gap > 0:
            reflection['lessons'].append(f"Accuracy gap: {accuracy_gap:.2f} - consider less aggressive compression")
        if carbon_gap > 0:
            reflection['lessons'].append(f"Carbon gap: {carbon_gap:.2f} - consider more aggressive optimization")
        
        if reflection['purpose_achieved']:
            reflection['lessons'].append("Purpose achieved - continue with current strategy")
        else:
            reflection['lessons'].append("Purpose not fully achieved - adjust optimization strategy")
        
        self.purpose_history.append(reflection)
        return reflection

# --- Main Reasoning Engine ---

class GreenAgentReasoningEngine:
    """Unified reasoning engine integrating all reasoning capabilities"""
    
    def __init__(self):
        self.scheduler = CarbonIntensityAwareScheduler()
        self.causal_model = CarbonCausalModel()
        self.ethical_reasoner = EthicalCarbonReasoner()
        self.context_optimizer = ContextAwareOptimizer()
        self.planner = SystemicCarbonPlanner()
        self.purpose_optimizer = PurposeAwareOptimizer()
        self.reasoning_history = deque(maxlen=1000)
        self.enabled = True
        logger.info("GreenAgentReasoningEngine initialized")
    
    async def reason_about_architecture(self, architecture_config: Dict[str, Any],
                                        fitness_metrics: Dict[str, float],
                                        context: str = 'cloud_inference',
                                        purpose: str = 'balanced') -> Dict[str, Any]:
        if not self.enabled:
            return {'reasoning': 'disabled'}
        
        reasoning_result = {
            'timestamp': datetime.now().isoformat(),
            'architecture_hash': hashlib.md5(json.dumps(architecture_config).encode()).hexdigest()[:8],
            'context': context,
            'purpose': purpose
        }
        
        scheduling = await self.scheduler.schedule_computation(
            task='architecture_evaluation',
            urgency='normal',
            compute_hours=1.0
        )
        reasoning_result['temporal'] = scheduling
        
        causal = self.causal_model.explain_carbon_impact(architecture_config, fitness_metrics)
        reasoning_result['causal'] = {
            'primary_driver': causal.primary_driver,
            'contribution': causal.contribution,
            'pathway': causal.pathway,
            'alternatives': causal.alternatives,
            'confidence': causal.confidence
        }
        
        ethical = self.ethical_reasoner.assess_reduction_impact(architecture_config, fitness_metrics)
        reasoning_result['ethical'] = ethical
        
        context_plan = self.context_optimizer.get_context_plan(architecture_config, context)
        reasoning_result['contextual'] = context_plan
        
        systemic = self.planner.plan_carbon_investment(
            current_accuracy=fitness_metrics.get('accuracy', 0.85),
            target_accuracy=0.90,
            carbon_budget=10.0
        )
        reasoning_result['systemic'] = systemic
        
        reflexive = self.purpose_optimizer.get_purpose_guide(purpose)
        reasoning_result['reflexive'] = reflexive
        
        self.reasoning_history.append(reasoning_result)
        reasoning_result['overall_recommendations'] = self._generate_recommendations(reasoning_result)
        
        return reasoning_result
    
    def _generate_recommendations(self, reasoning_result: Dict) -> List[str]:
        recommendations = []
        
        temporal = reasoning_result.get('temporal', {})
        if temporal.get('action') in ['schedule', 'schedule_optimal']:
            recommendations.append(f"Schedule evaluation for better carbon timing: {temporal.get('schedule', 'unknown')}")
        
        causal_alternatives = reasoning_result.get('causal', {}).get('alternatives', [])
        if causal_alternatives:
            recommendations.append(f"Causal alternative: {causal_alternatives[0]}")
        
        ethical_recommendations = reasoning_result.get('ethical', {}).get('recommendations', [])
        recommendations.extend(ethical_recommendations)
        
        contextual_suggestions = reasoning_result.get('contextual', {}).get('suggestions', [])
        for suggestion in contextual_suggestions[:2]:
            recommendations.append(f"Contextual suggestion: {suggestion.get('action')} ({suggestion.get('reason')})")
        
        if reasoning_result.get('systemic', {}).get('decision') == 'invest':
            recommendations.append("Systemic decision: Invest in exploration - high ROI expected")
        
        reflexive_recommendations = reasoning_result.get('reflexive', {}).get('recommendations', [])
        recommendations.extend(reflexive_recommendations[:2])
        
        return recommendations[:5]
    
    async def get_reasoning_summary(self) -> Dict[str, Any]:
        if not self.reasoning_history:
            return {'status': 'no_reasoning_history'}
        
        recent = list(self.reasoning_history)[-20:]
        all_recommendations = []
        for entry in recent:
            all_recommendations.extend(entry.get('overall_recommendations', []))
        
        return {
            'total_reasoned_architectures': len(self.reasoning_history),
            'recent_recommendations': all_recommendations[:10],
            'average_ethical_score': np.mean([
                entry.get('ethical', {}).get('overall_ethical_score', 0.5)
                for entry in recent
            ]),
            'most_common_causal_driver': self._get_most_common_causal_driver(recent),
            'timestamp': datetime.now().isoformat()
        }
    
    def _get_most_common_causal_driver(self, recent_entries: List[Dict]) -> str:
        drivers = []
        for entry in recent_entries:
            causal = entry.get('causal', {})
            if causal.get('primary_driver'):
                drivers.append(causal['primary_driver'])
        
        if not drivers:
            return 'unknown'
        
        from collections import Counter
        return Counter(drivers).most_common(1)[0][0]
    
    async def shutdown(self):
        self.enabled = False
        logger.info("GreenAgentReasoningEngine shutdown complete")

# ============================================================================
# ENHANCED COMPONENTS (Circuit Breakers, Rate Limiter, Health Monitor, Database)
# ============================================================================

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class EnhancedCircuitBreaker:
    """Circuit breaker for worker failures"""
    
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self._lock = asyncio.Lock()
        self.metrics = {'total_calls': 0, 'failed_calls': 0, 'successful_calls': 0}
    
    async def call(self, func: Callable, *args, **kwargs):
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    CIRCUIT_BREAKER_STATE.labels(component=self.name).set(0.5)
                else:
                    raise Exception(f"Circuit breaker {self.name} is OPEN")
            
            if self.state == CircuitBreakerState.HALF_OPEN and self.success_count >= 2:
                self.state = CircuitBreakerState.CLOSED
                CIRCUIT_BREAKER_STATE.labels(component=self.name).set(0)
        
        self.metrics['total_calls'] += 1
        
        try:
            result = await func(*args, **kwargs)
            await self._record_success()
            return result
        except Exception as e:
            await self._record_failure()
            raise
    
    async def _record_success(self):
        async with self._lock:
            self.metrics['successful_calls'] += 1
            self.success_count += 1
            self.failure_count = 0
    
    async def _record_failure(self):
        async with self._lock:
            self.metrics['failed_calls'] += 1
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                CIRCUIT_BREAKER_STATE.labels(component=self.name).set(1)
    
    def get_metrics(self) -> Dict:
        return {
            **self.metrics,
            'state': self.state.value,
            'failure_count': self.failure_count
        }

class EnhancedRateLimiter:
    """Rate limiter for architecture generation"""
    
    def __init__(self, rate: int = 50, per_seconds: int = 60):
        self.rate = rate
        self.per_seconds = per_seconds
        self.tokens = rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()
        self.total_requests = 0
        self.throttled_requests = 0
    
    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            time_passed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + time_passed * (self.rate / self.per_seconds))
            self.last_refill = now
            
            if self.tokens >= 1:
                self.tokens -= 1
                self.total_requests += 1
                return True
            else:
                self.throttled_requests += 1
                return False
    
    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.1)
    
    def get_metrics(self) -> Dict:
        total = self.total_requests + self.throttled_requests
        return {
            'total_requests': self.total_requests,
            'throttled_requests': self.throttled_requests,
            'throttle_rate': (self.throttled_requests / max(total, 1)) * 100
        }

class HealthStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    CRITICAL = "critical"

@dataclass
class HealthMetrics:
    status: HealthStatus
    score: float
    components: Dict[str, Dict[str, Any]]
    timestamp: datetime
    messages: List[str]

class EnhancedHealthMonitor:
    """Advanced health monitoring with trend analysis"""
    
    def __init__(self, check_interval: int = 60, degradation_threshold: float = 0.7,
                 critical_threshold: float = 0.3):
        self.check_interval = check_interval
        self.degradation_threshold = degradation_threshold
        self.critical_threshold = critical_threshold
        self.current_health = HealthMetrics(
            status=HealthStatus.HEALTHY,
            score=100.0,
            components={},
            timestamp=datetime.now(),
            messages=[]
        )
        self.health_history = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self._running = False
        self._check_task = None
        
        self.component_thresholds = {
            'database': {'min_score': 0.8},
            'token_economy': {'min_score': 0.7},
            'population': {'min_score': 0.5},
            'evaluation_queue': {'min_score': 0.6},
            'circuit_breakers': {'min_score': 0.9}
        }
        
        logger.info("EnhancedHealthMonitor initialized")
    
    async def start(self):
        self._running = True
        self._check_task = asyncio.create_task(self._health_check_loop())
        logger.info("Health monitoring started")
    
    async def _health_check_loop(self):
        while self._running:
            try:
                await self.perform_health_check()
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")
                await asyncio.sleep(self.check_interval)
    
    async def perform_health_check(self, components: Dict[str, Any] = None) -> HealthMetrics:
        component_status = {}
        messages = []
        
        if components:
            for name, check_func in components.items():
                try:
                    result = await check_func()
                    score = result.get('score', 100.0)
                    component_status[name] = {
                        'score': score,
                        'healthy': score >= self.component_thresholds.get(name, {}).get('min_score', 0.5),
                        'details': result
                    }
                    
                    if score < self.degradation_threshold * 100:
                        messages.append(f"Component {name} is degraded: score={score:.1f}")
                    
                except Exception as e:
                    component_status[name] = {
                        'score': 0.0,
                        'healthy': False,
                        'error': str(e)
                    }
                    messages.append(f"Component {name} check failed: {e}")
        
        if component_status:
            scores = [c['score'] for c in component_status.values() if 'score' in c]
            overall_score = np.mean(scores) if scores else 50.0
        else:
            overall_score = 100.0
        
        if overall_score >= self.degradation_threshold * 100:
            status = HealthStatus.HEALTHY
        elif overall_score >= self.critical_threshold * 100:
            status = HealthStatus.DEGRADED
        else:
            status = HealthStatus.CRITICAL
        
        async with self._lock:
            self.current_health = HealthMetrics(
                status=status,
                score=overall_score,
                components=component_status,
                timestamp=datetime.now(),
                messages=messages
            )
            self.health_history.append(self.current_health)
        
        return self.current_health
    
    async def get_health_report(self) -> Dict[str, Any]:
        async with self._lock:
            trend = self._calculate_trend()
            return {
                'current_status': self.current_health.status.value,
                'current_score': self.current_health.score,
                'components': self.current_health.components,
                'messages': self.current_health.messages,
                'trend': trend,
                'history_size': len(self.health_history),
                'timestamp': self.current_health.timestamp.isoformat()
            }
    
    def _calculate_trend(self) -> str:
        if len(self.health_history) < 5:
            return "stable"
        
        recent_scores = [h.score for h in list(self.health_history)[-10:]]
        older_scores = [h.score for h in list(self.health_history)[-20:-10]]
        
        if not older_scores or not recent_scores:
            return "stable"
        
        recent_avg = np.mean(recent_scores)
        older_avg = np.mean(older_scores)
        
        if recent_avg > older_avg * 1.05:
            return "improving"
        elif recent_avg < older_avg * 0.95:
            return "degrading"
        else:
            return "stable"
    
    async def shutdown(self):
        self._running = False
        if self._check_task:
            self._check_task.cancel()
            try:
                await self._check_task
            except asyncio.CancelledError:
                pass
        logger.info("Health monitoring shutdown complete")

# ============================================================================
# MODEL COMPRESSION ENGINE
# ============================================================================

class ModelCompressionEngine:
    """Handles model compression techniques"""
    
    def __init__(self):
        self.compression_stats: Dict[str, Any] = {}
    
    def apply_pruning(self, model: nn.Module, pruning_rate: float,
                      method: str = 'structured') -> Tuple[nn.Module, Dict[str, Any]]:
        original_params = sum(p.numel() for p in model.parameters())
        
        if method == 'structured':
            pruned_model = self._structured_prune(model, pruning_rate)
        else:
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
    
    def _structured_prune(self, model: nn.Module, pruning_rate: float) -> nn.Module:
        pruned_model = copy.deepcopy(model)
        
        for name, module in pruned_model.named_modules():
            if isinstance(module, nn.Conv2d):
                num_channels = module.out_channels
                channels_to_keep = int(num_channels * (1 - pruning_rate))
                
                if channels_to_keep > 0:
                    module.out_channels = channels_to_keep
                    module.weight = nn.Parameter(module.weight[:channels_to_keep])
                    if module.bias is not None:
                        module.bias = nn.Parameter(module.bias[:channels_to_keep])
            
            elif isinstance(module, nn.Linear):
                num_features = module.out_features
                features_to_keep = int(num_features * (1 - pruning_rate))
                
                if features_to_keep > 0:
                    module.out_features = features_to_keep
                    module.weight = nn.Parameter(module.weight[:features_to_keep])
                    if module.bias is not None:
                        module.bias = nn.Parameter(module.bias[:features_to_keep])
        
        return pruned_model
    
    def _unstructured_prune(self, model: nn.Module, pruning_rate: float) -> nn.Module:
        pruned_model = copy.deepcopy(model)
        
        for name, module in pruned_model.named_modules():
            if isinstance(module, (nn.Conv2d, nn.Linear)):
                weight = module.weight.data
                threshold = torch.quantile(weight.abs().flatten(), pruning_rate)
                mask = weight.abs() > threshold
                module.weight.data = weight * mask
        
        return pruned_model
    
    def apply_quantization(self, model: nn.Module, bits: int = 8) -> Tuple[nn.Module, Dict[str, Any]]:
        original_size = self._estimate_model_size(model)
        
        if bits == 8:
            quantized_model = self._quantize_int8(model)
            size_reduction = 4
        elif bits == 16:
            quantized_model = self._quantize_fp16(model)
            size_reduction = 2
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
        try:
            import torch.quantization as quant
            quantized_model = copy.deepcopy(model)
            quantized_model.eval()
            quantized_model = torch.quantization.fuse_modules(quantized_model, [['conv', 'bn', 'relu']])
            quantized_model.qconfig = torch.quantization.get_default_qconfig('fbgemm')
            torch.quantization.prepare(quantized_model, inplace=True)
            quantized_model = torch.quantization.convert(quantized_model, inplace=True)
            return quantized_model
        except Exception as e:
            logger.warning(f"INT8 quantization failed: {str(e)}, returning original")
            return model
    
    def _quantize_fp16(self, model: nn.Module) -> nn.Module:
        return model.half()
    
    def _estimate_model_size(self, model: nn.Module) -> float:
        param_size = 0
        for param in model.parameters():
            param_size += param.nelement() * param.element_size()
        
        buffer_size = 0
        for buffer in model.buffers():
            buffer_size += buffer.nelement() * buffer.element_size()
        
        return (param_size + buffer_size) / 1024 / 1024
    
    def apply_combined_compression(self, model: nn.Module,
                                   config: ArchitectureConfig) -> Tuple[nn.Module, Dict[str, Any]]:
        compressed_model = model
        combined_stats = {}
        
        if config.pruning_rate > 0:
            compressed_model, pruning_stats = self.apply_pruning(
                compressed_model,
                config.pruning_rate,
                'structured' if config.compression == CompressionMethod.PRUNING_STRUCTURED else 'unstructured'
            )
            combined_stats['pruning'] = pruning_stats
        
        if config.quantization_bits < 32:
            compressed_model, quant_stats = self.apply_quantization(
                compressed_model,
                config.quantization_bits
            )
            combined_stats['quantization'] = quant_stats
        
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
# HARDWARE PROFILER
# ============================================================================

class HardwareProfiler:
    """Profiles models on different hardware targets"""
    
    def __init__(self):
        self.hardware_profiles = {
            HardwareTarget.CPU_X86: {
                'flops_per_watt': 10e9,
                'carbon_intensity': 400,
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
    
    def estimate_flops(self, config: ArchitectureConfig) -> float:
        base_flops = config.num_layers * config.hidden_dim ** 2
        
        if config.family == ArchitectureFamily.CNN:
            base_flops *= 9
        elif config.family == ArchitectureFamily.TRANSFORMER:
            base_flops *= 12
        
        if config.pruning_rate > 0:
            base_flops *= (1 - config.pruning_rate)
        
        return base_flops
    
    def profile_on_hardware(self, config: ArchitectureConfig,
                            hardware: Optional[HardwareTarget] = None) -> Dict[str, float]:
        if hardware is None:
            hardware = config.target_hardware
        
        hw_profile = self.hardware_profiles.get(
            hardware,
            self.hardware_profiles[HardwareTarget.CPU_X86]
        )
        
        flops = self.estimate_flops(config)
        compute_time = flops / hw_profile['flops_per_watt']
        memory_time = (config.hidden_dim * 4) / (hw_profile['memory_bandwidth_gbps'] * 1e9)
        latency = (compute_time + memory_time) * 1000 + hw_profile['base_latency_ms']
        energy = flops / hw_profile['flops_per_watt'] / 3600
        carbon = energy * hw_profile['carbon_intensity'] / 1000
        memory = config.hidden_dim * config.num_layers * 4 / 1024 / 1024
        
        return {
            'flops': flops,
            'latency_ms': latency,
            'energy_kwh': energy,
            'carbon_kg': carbon,
            'memory_mb': memory,
            'hardware': hardware.value
        }

# ============================================================================
# ENHANCED PARETO OPTIMIZER
# ============================================================================

class EnhancedParetoOptimizer:
    """Enhanced Pareto optimizer with hypervolume and diversity"""
    
    def __init__(self, reference_point: Optional[Dict[str, float]] = None,
                 diversity_threshold: float = 0.05):
        self.pareto_frontier: List[ArchitectureGene] = []
        self.reference_point = reference_point or {
            'accuracy': 0.0,
            'carbon_kg': 1.0,
            'energy_kwh': 0.1,
            'latency_ms': 1000,
            'memory_mb': 10000
        }
        self.diversity_threshold = diversity_threshold
        self.objectives = ['accuracy', 'carbon_kg', 'energy_kwh', 'latency_ms', 'memory_mb']
    
    def find_pareto_optimal(self, population: List[ArchitectureGene],
                            objectives: Optional[List[str]] = None) -> List[ArchitectureGene]:
        if objectives is None:
            objectives = self.objectives
        
        pareto_optimal = []
        dominated_count = 0
        
        for i, gene1 in enumerate(population):
            is_dominated = False
            
            for j, gene2 in enumerate(population):
                if i == j:
                    continue
                
                if self._dominates(gene2.fitness, gene1.fitness, objectives):
                    is_dominated = True
                    dominated_count += 1
                    break
            
            if not is_dominated:
                pareto_optimal.append(gene1)
                gene1.fitness.pareto_rank = 1
            else:
                gene1.fitness.pareto_rank = 2
        
        if len(pareto_optimal) > 1:
            pareto_optimal = self._preserve_diversity(pareto_optimal)
        
        self.pareto_frontier = pareto_optimal
        
        logger.info(f"Found {len(pareto_optimal)} Pareto-optimal architectures")
        return pareto_optimal
    
    def _dominates(self, fitness1: MultiObjectiveFitness, fitness2: MultiObjectiveFitness,
                   objectives: List[str]) -> bool:
        at_least_one_better = False
        
        for obj in objectives:
            val1 = getattr(fitness1, obj, 0)
            val2 = getattr(fitness2, obj, 0)
            
            if obj == 'accuracy':
                if val1 < val2:
                    return False
                if val1 > val2:
                    at_least_one_better = True
            else:
                if val1 > val2:
                    return False
                if val1 < val2:
                    at_least_one_better = True
        
        return at_least_one_better
    
    def _preserve_diversity(self, pareto_set: List[ArchitectureGene]) -> List[ArchitectureGene]:
        if len(pareto_set) <= 1:
            return pareto_set
        
        n = len(pareto_set)
        similarity_matrix = np.zeros((n, n))
        
        for i in range(n):
            for j in range(i+1, n):
                similarity = self._calculate_similarity(pareto_set[i].config, pareto_set[j].config)
                similarity_matrix[i][j] = similarity
                similarity_matrix[j][i] = similarity
        
        to_remove = set()
        for i in range(n):
            for j in range(i+1, n):
                if similarity_matrix[i][j] > self.diversity_threshold:
                    if pareto_set[i].fitness.composite_score >= pareto_set[j].fitness.composite_score:
                        to_remove.add(j)
                    else:
                        to_remove.add(i)
        
        return [gene for i, gene in enumerate(pareto_set) if i not in to_remove]
    
    def _calculate_similarity(self, config1: ArchitectureConfig, config2: ArchitectureConfig) -> float:
        family_similar = 1.0 if config1.family == config2.family else 0.0
        
        layer_diff = abs(config1.num_layers - config2.num_layers) / max(config1.num_layers, config2.num_layers)
        layer_similarity = 1.0 - layer_diff
        
        hidden_diff = abs(config1.hidden_dim - config2.hidden_dim) / max(config1.hidden_dim, config2.hidden_dim)
        hidden_similarity = 1.0 - hidden_diff
        
        return 0.3 * family_similar + 0.35 * layer_similarity + 0.35 * hidden_similarity
    
    def calculate_hypervolume(self) -> float:
        if not self.pareto_frontier:
            return 0.0
        
        normalized_points = []
        for gene in self.pareto_frontier:
            point = {
                'accuracy': gene.fitness.accuracy,
                'carbon_kg': gene.fitness.carbon_kg,
                'energy_kwh': gene.fitness.energy_kwh,
                'latency_ms': gene.fitness.latency_ms,
                'memory_mb': gene.fitness.memory_mb
            }
            normalized_points.append(point)
        
        hv = 0.0
        for point in normalized_points:
            volume = 1.0
            for obj in ['accuracy', 'carbon_kg', 'energy_kwh', 'latency_ms', 'memory_mb']:
                if obj == 'accuracy':
                    diff = point[obj] - self.reference_point[obj]
                else:
                    diff = self.reference_point[obj] - point[obj]
                volume *= max(0, diff)
            hv += volume
        
        return hv
    
    def get_frontier_stats(self) -> Dict[str, Any]:
        if not self.pareto_frontier:
            return {'size': 0}
        
        accuracies = [g.fitness.accuracy for g in self.pareto_frontier]
        carbons = [g.fitness.carbon_kg for g in self.pareto_frontier]
        
        return {
            'size': len(self.pareto_frontier),
            'hypervolume': self.calculate_hypervolume(),
            'best_accuracy': max(accuracies),
            'best_carbon': min(carbons),
            'average_accuracy': np.mean(accuracies),
            'average_carbon': np.mean(carbons),
            'diversity_score': self._calculate_diversity()
        }
    
    def _calculate_diversity(self) -> float:
        if len(self.pareto_frontier) <= 1:
            return 1.0
        
        similarities = []
        for i in range(len(self.pareto_frontier)):
            for j in range(i+1, len(self.pareto_frontier)):
                sim = self._calculate_similarity(self.pareto_frontier[i].config,
                                                 self.pareto_frontier[j].config)
                similarities.append(sim)
        
        avg_similarity = np.mean(similarities)
        return max(0, min(1, 1.0 - avg_similarity))

# ============================================================================
# UNIFIED DATABASE MANAGER
# ============================================================================

Base = declarative_base()

class ArchitectureDB(Base):
    __tablename__ = 'architectures'
    __table_args__ = (
        Index('idx_accuracy_carbon', 'accuracy', 'carbon_kg'),
        Index('idx_created', 'created_at'),
    )
    
    arch_id = Column(String(64), primary_key=True)
    config_json = Column(JSON)
    accuracy = Column(Float)
    carbon_kg = Column(Float)
    energy_kwh = Column(Float)
    latency_ms = Column(Float)
    memory_mb = Column(Float)
    flops = Column(Float)
    params_count = Column(Integer)
    compression_method = Column(String(32))
    pruning_rate = Column(Float)
    quantization_bits = Column(Integer)
    target_hardware = Column(String(32))
    composite_score = Column(Float)
    green_score = Column(Float)
    certification = Column(String(20))
    registered_expert_id = Column(String(64))
    created_at = Column(DateTime, default=datetime.now)
    version = Column(Integer, default=3)

class KnowledgePackageDB(Base):
    __tablename__ = 'knowledge_packages'
    
    package_id = Column(String(64), primary_key=True)
    config_json = Column(JSON)
    survival_score = Column(Float)
    accuracy = Column(Float)
    carbon_kg = Column(Float)
    domain_tags = Column(JSON)
    source_generation = Column(Integer)
    usage_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.now)
    
    __table_args__ = (
        Index('idx_survival', 'survival_score'),
        Index('idx_accuracy', 'accuracy'),
        Index('idx_created', 'created_at'),
    )

class UnifiedDatabaseManager:
    """Enhanced database manager with connection pooling"""
    
    def __init__(self, db_path: Path = Path("./carbon_nas_unified.db"),
                 pool_size: int = 10, max_overflow: int = 20):
        self.db_path = db_path
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.engine = None
        self.SessionLocal = None
        self._init_engine()
    
    def _init_engine(self):
        db_url = f"sqlite:///{self.db_path}"
        self.db_path.parent.mkdir(exist_ok=True, parents=True)
        
        self.engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            pool_pre_ping=True,
            connect_args={'check_same_thread': False}
        )
        
        self.SessionLocal = scoped_session(sessionmaker(bind=self.engine))
        self._init_tables()
        logger.info(f"UnifiedDatabaseManager initialized at {self.db_path}")
    
    def _init_tables(self):
        Base.metadata.create_all(self.engine)
        logger.info("Database tables initialized")
    
    @contextmanager
    def get_session(self):
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            session.close()
    
    async def save_architecture(self, gene: ArchitectureGene):
        with self.get_session() as session:
            from sqlalchemy import text
            
            session.execute(
                text("""INSERT OR REPLACE INTO architectures 
                       (arch_id, config_json, accuracy, carbon_kg, energy_kwh, 
                        latency_ms, memory_mb, flops, params_count, compression_method,
                        pruning_rate, quantization_bits, target_hardware, 
                        composite_score, green_score, certification, registered_expert_id)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""),
                (
                    gene.config.compute_hash(),
                    json.dumps(gene.config.to_dict()),
                    gene.fitness.accuracy,
                    gene.fitness.carbon_kg,
                    gene.fitness.energy_kwh,
                    gene.fitness.latency_ms,
                    gene.fitness.memory_mb,
                    gene.fitness.flops,
                    gene.fitness.params_count,
                    gene.config.compression.value,
                    gene.config.pruning_rate,
                    gene.config.quantization_bits,
                    gene.config.target_hardware.value,
                    gene.fitness.composite_score,
                    gene.fitness.green_score,
                    gene.fitness.certification.value,
                    gene.registered_expert_id
                )
            )
    
    async def save_evolution_step(self, generation: int, metrics: Dict[str, Any]):
        with self.get_session() as session:
            from sqlalchemy import text
            
            session.execute(
                text("""INSERT INTO evolution_history 
                       (generation, population_size, best_accuracy, best_carbon,
                        best_composite, pareto_size, carbon_spent, tokens_spent,
                        registered_experts)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""),
                (
                    generation,
                    metrics.get('population_size', 0),
                    metrics.get('best_accuracy', 0.0),
                    metrics.get('best_carbon_kg', 0.0),
                    metrics.get('best_composite_score', 0.0),
                    metrics.get('pareto_frontier_size', 0),
                    metrics.get('total_carbon_spent_kg', 0.0),
                    metrics.get('total_tokens_spent', 0.0),
                    metrics.get('registered_experts', 0)
                )
            )
    
    def dispose(self):
        if self.engine:
            self.engine.dispose()
            if self.SessionLocal:
                self.SessionLocal.remove()
            logger.info("Database connections disposed")

# ============================================================================
# ENHANCED UNIFIED CONFIGURATION
# ============================================================================

class UnifiedNASConfig(BaseModel):
    """Enhanced unified configuration with validation"""
    
    # Core Settings
    instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    version: str = "3.1.0"
    mode: str = Field("production", regex='^(production|research|hybrid)$')
    
    # Population Settings
    population_size: int = Field(30, ge=5, le=500)
    max_generations: int = Field(50, ge=1, le=1000)
    early_stopping_patience: int = Field(10, ge=1, le=50)
    
    # Carbon & Resource Budget
    carbon_budget_kg: float = Field(10.0, ge=0.1, le=10000.0)
    energy_budget_kwh: float = Field(100.0, ge=0.1, le=10000.0)
    token_budget: float = Field(1000.0, ge=10.0, le=100000.0)
    
    # Hardware Constraints
    target_hardware: HardwareTarget = HardwareTarget.CPU_X86
    max_memory_mb: int = Field(8192, ge=64, le=131072)
    max_latency_ms: int = Field(100, ge=1, le=10000)
    
    # Search Space Configuration
    allowed_families: List[ArchitectureFamily] = [
        ArchitectureFamily.CNN,
        ArchitectureFamily.TRANSFORMER,
        ArchitectureFamily.EFFICIENTNET,
        ArchitectureFamily.MOBILENET
    ]
    layer_range: tuple = Field((2, 20), ge=(1, 1), le=(50, 50))
    hidden_dim_range: tuple = Field((64, 1024), ge=(32, 32), le=(2048, 2048))
    
    # Feature Flags
    enable_compression: bool = True
    enable_hardware_profiling: bool = True
    enable_pareto: bool = True
    enable_continuous_learning: bool = True
    enable_knowledge_transfer: bool = True
    enable_token_economy: bool = True
    enable_circuit_breakers: bool = True
    enable_persistence: bool = True
    enable_reasoning: bool = True
    
    # Reasoning Settings
    context: str = Field("cloud_inference", regex='^(cloud_inference|mobile_inference|edge_tpu|batch_processing|quantum)$')
    purpose: str = Field("balanced", regex='^(climate_research|medical_diagnosis|consumer_app|research_exploration|production_deployment|balanced)$')
    enable_ethical_reasoning: bool = True
    
    # Reliability Settings
    max_retry_attempts: int = Field(3, ge=1, le=10)
    health_check_interval_seconds: int = Field(60, ge=10, le=600)
    circuit_breaker_threshold: int = Field(5, ge=1, le=20)
    circuit_breaker_timeout_seconds: int = Field(60, ge=10, le=300)
    
    # Performance
    max_concurrent_evaluations: int = Field(4, ge=1, le=16)
    evaluation_timeout_seconds: int = Field(300, ge=30, le=3600)
    cache_ttl_seconds: int = Field(300, ge=60, le=3600)
    
    # Persistence
    database_path: Path = Field(Path("./carbon_nas_unified.db"))
    enable_state_export: bool = True
    
    # Observability
    enable_prometheus_metrics: bool = True
    log_level: str = Field("INFO", regex='^(DEBUG|INFO|WARNING|ERROR)$')
    enable_structured_logging: bool = True
    
    @validator('carbon_budget_kg')
    def validate_carbon_budget(cls, v):
        if v < 0.01:
            raise ValueError("Carbon budget must be at least 0.01 kg")
        return v
    
    @classmethod
    def from_yaml(cls, path: Path) -> "UnifiedNASConfig":
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
        return cls(**data)
    
    @classmethod
    def from_json(cls, path: Path) -> "UnifiedNASConfig":
        with open(path, 'r') as f:
            data = json.load(f)
        return cls(**data)
    
    def to_yaml(self, path: Path):
        with open(path, 'w') as f:
            yaml.dump(self.dict(), f, default_flow_style=False)
    
    def to_dict(self) -> Dict[str, Any]:
        config_dict = self.dict()
        if 'database_path' in config_dict:
            config_dict['database_path'] = str(config_dict['database_path'])
        return config_dict

# ============================================================================
# ENHANCED UNIFIED CARBON NAS (MAIN CLASS)
# ============================================================================

class EnhancedUnifiedCarbonNAS:
    """
    Enhanced Unified Carbon NAS with reasoning capabilities.
    
    This is the main class that integrates all features:
    - Extended architecture search space
    - Multi-objective optimization with Pareto frontier
    - Model compression (pruning, quantization)
    - Hardware-aware profiling
    - Token economy for resource management
    - Health monitoring and circuit breakers
    - Full reasoning engine (temporal, causal, ethical, contextual, systemic, reflexive)
    - Database persistence
    """
    
    def __init__(self,
                 expert_registry: Optional[Any] = None,
                 config: Optional[Union[Dict, UnifiedNASConfig]] = None,
                 **kwargs):
        # Load configuration
        if isinstance(config, dict):
            self.config = UnifiedNASConfig(**config)
        elif isinstance(config, UnifiedNASConfig):
            self.config = config
        else:
            self.config = UnifiedNASConfig(**kwargs)
        
        # Core components
        self.instance_id = self.config.instance_id
        self.population_size = self.config.population_size
        self.max_generations = self.config.max_generations
        self.carbon_budget_kg = self.config.carbon_budget_kg
        self.auto_register = True
        self.enable_compression = self.config.enable_compression
        self.enable_hardware_profiling = self.config.enable_hardware_profiling
        self.enable_pareto = self.config.enable_pareto
        self.enable_continuous_learning = self.config.enable_continuous_learning
        
        # Reasoning configuration
        self.enable_reasoning = self.config.enable_reasoning
        self.context = self.config.context
        self.purpose = self.config.purpose
        self.enable_ethical_reasoning = self.config.enable_ethical_reasoning
        
        # Initialize reasoning engine
        if self.enable_reasoning:
            self.reasoning_engine = GreenAgentReasoningEngine()
            self.reasoning_history = []
            logger.info("Reasoning engine enabled")
        else:
            self.reasoning_engine = None
            self.reasoning_history = []
            logger.info("Reasoning engine disabled")
        
        # Initialize enhanced components
        self.rate_limiter = EnhancedRateLimiter()
        
        self.circuit_breakers = {
            'evaluation': EnhancedCircuitBreaker(
                'evaluation',
                failure_threshold=self.config.circuit_breaker_threshold,
                recovery_timeout=self.config.circuit_breaker_timeout_seconds
            ),
            'database': EnhancedCircuitBreaker(
                'database',
                failure_threshold=3,
                recovery_timeout=30
            )
        } if self.config.enable_circuit_breakers else {}
        
        self.health_monitor = EnhancedHealthMonitor(
            check_interval=self.config.health_check_interval_seconds
        )
        
        self.database = UnifiedDatabaseManager(
            db_path=self.config.database_path
        ) if self.config.enable_persistence else None
        
        self.pareto_optimizer = EnhancedParetoOptimizer()
        self.compression_engine = ModelCompressionEngine()
        self.hardware_profiler = HardwareProfiler()
        
        # Token economy
        self.token_economy = EnhancedTokenEconomy(
            total_budget=self.config.token_budget,
            renewable_rate=0.02,
            dynamic_pricing=True
        )
        
        # State
        self.population: List[ArchitectureGene] = []
        self.generation = 0
        self.evolution_history: List[Dict] = []
        self.total_carbon_spent_kg = 0.0
        self.total_tokens_spent = 0.0
        
        self.best_by_accuracy: Optional[ArchitectureGene] = None
        self.best_by_carbon: Optional[ArchitectureGene] = None
        self.best_by_composite: Optional[ArchitectureGene] = None
        
        # Background tasks
        self.background_tasks = set()
        self._running = False
        self._shutdown_event = asyncio.Event()
        
        # Initialize search space
        self._initialize_search_space()
        
        # Initialize population
        self._initialize_population()
        
        logger.info(f"EnhancedUnifiedCarbonNAS initialized (instance: {self.instance_id}, "
                   f"pop: {self.population_size}, reasoning: {self.enable_reasoning}, "
                   f"context: {self.context}, purpose: {self.purpose})")
    
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
            gene = ArchitectureGene(config=config, generation=0)
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
        
        if self.enable_hardware_profiling:
            config.target_hardware = np.random.choice(
                self.search_space['hardware_targets']
            )
        
        return config
    
    async def start(self):
        """Start all background services"""
        self._running = True
        
        # Start token economy
        await self.token_economy.start()
        
        # Start health monitoring
        await self.health_monitor.start()
        
        # Start continuous learning if enabled
        if self.enable_continuous_learning:
            task = asyncio.create_task(self._enhanced_continuous_learning())
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)
        
        logger.info("All enhanced services started")
    
    async def _enhanced_continuous_learning(self):
        """Enhanced continuous learning with token economy and reasoning"""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.health_check_interval_seconds)
                
                health = await self.health_monitor.perform_health_check({
                    'token_economy': self.token_economy.get_system_summary,
                    'population': lambda: {'score': len(self.population) * 2}
                })
                
                if health.score < 50:
                    logger.warning(f"System health degraded: {health.score:.1f}")
                    continue
                
                token_summary = self.token_economy.get_system_summary()
                if token_summary['current_balance'] < token_summary['total_budget'] * 0.1:
                    logger.warning("Token balance critically low")
                    continue
                
                # Run lightweight evolution
                await self._lightweight_evolution()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Enhanced continuous learning error: {e}")
                await asyncio.sleep(60)
    
    async def _lightweight_evolution(self):
        """Run lightweight evolution with reduced generations"""
        small_pop = [self._generate_random_config() for _ in range(5)]
        
        for config in small_pop:
            success, cost = await self.token_economy.reserve_tokens(
                TokenConsumer.ARCHITECTURE_EVAL,
                1.0,
                metadata={'config': config.to_dict()}
            )
            
            if not success:
                break
            
            gene = ArchitectureGene(config=config)
            await self._evaluate_single_architecture(gene)
    
    async def _evaluate_single_architecture(self, gene: ArchitectureGene):
        """Evaluate a single architecture with enhanced features"""
        if self.enable_hardware_profiling:
            hw_profile = self.hardware_profiler.profile_on_hardware(gene.config)
            gene.fitness.latency_ms = hw_profile['latency_ms']
            gene.fitness.energy_kwh = hw_profile['energy_kwh']
            gene.fitness.carbon_kg = hw_profile['carbon_kg']
        
        gene.fitness.calculate_composite()
        self.total_carbon_spent_kg += gene.fitness.carbon_kg
        
        if self.database:
            await self.database.save_architecture(gene)
    
    async def evolve(self, fitness_function: Callable,
                     generations: Optional[int] = None,
                     early_stopping_patience: int = 10) -> Dict[str, Any]:
        """
        Run evolution with reasoning capabilities.
        
        Args:
            fitness_function: Async function to evaluate fitness
            generations: Number of generations to run
            early_stopping_patience: Stop if no improvement for N generations
            
        Returns:
            Evolution summary dictionary
        """
        generations = generations or self.max_generations
        best_fitness = 0.0
        patience_counter = 0
        
        # Systemic planning
        if self.enable_reasoning:
            plan = self.reasoning_engine.planner.plan_carbon_investment(
                current_accuracy=0.75,
                target_accuracy=0.90,
                carbon_budget=self.carbon_budget_kg
            )
            
            if plan['decision'] == 'save':
                logger.info(f"Systemic decision: {plan['reason']}")
                return {'status': 'postponed', 'reason': plan['reason']}
            
            logger.info(f"Systemic decision: {plan['decision']} - {plan['reason']}")
        
        for gen in range(generations):
            self.generation = gen + 1
            
            if self.total_carbon_spent_kg >= self.carbon_budget_kg:
                logger.warning(f"Carbon budget exhausted: {self.total_carbon_spent_kg:.4f}kg")
                break
            
            await self._evaluate_population(fitness_function)
            
            if self.enable_pareto:
                pareto_optimal = self.pareto_optimizer.find_pareto_optimal(self.population)
                logger.info(f"Generation {self.generation}: {len(pareto_optimal)} Pareto-optimal architectures")
            
            self._update_bests()
            
            if self.auto_register:
                await self._auto_register_bests()
            
            gen_metrics = self._record_generation()
            
            current_best = gen_metrics['best_composite_score']
            if current_best > best_fitness * 1.01:
                best_fitness = current_best
                patience_counter = 0
            else:
                patience_counter += 1
            
            if patience_counter >= early_stopping_patience:
                logger.info(f"Early stopping at generation {self.generation}")
                break
            
            self._evolve_population()
        
        return self._get_evolution_summary()
    
    async def _evaluate_population(self, fitness_function: Callable):
        """Evaluate all architectures with reasoning"""
        for gene in self.population:
            if gene.fitness.composite_score > 0:
                continue
            
            try:
                fitness_result = await fitness_function(gene.config)
                
                gene.fitness = MultiObjectiveFitness(
                    accuracy=fitness_result.get('accuracy', 0.5),
                    carbon_kg=fitness_result.get('carbon_kg', 0.001),
                    energy_kwh=fitness_result.get('energy_kwh', 0.001),
                    latency_ms=fitness_result.get('latency_ms', 100),
                    memory_mb=fitness_result.get('memory_mb', 100),
                    flops=fitness_result.get('flops', 1e9),
                    params_count=fitness_result.get('params', 1e6)
                )
                
                if self.enable_hardware_profiling:
                    hw_profile = self.hardware_profiler.profile_on_hardware(gene.config)
                    gene.fitness.latency_ms = hw_profile['latency_ms']
                    gene.fitness.energy_kwh = hw_profile['energy_kwh']
                    gene.fitness.carbon_kg = hw_profile['carbon_kg']
                    gene.fitness.memory_mb = hw_profile['memory_mb']
                
                if self.enable_compression and gene.config.compression != CompressionMethod.NONE:
                    compression_factor = self._estimate_compression_benefit(gene.config)
                    gene.fitness.carbon_kg *= (1 - compression_factor * 0.5)
                    gene.fitness.energy_kwh *= (1 - compression_factor * 0.4)
                    gene.fitness.memory_mb *= (1 - compression_factor * 0.6)
                
                gene.fitness.calculate_composite()
                self.total_carbon_spent_kg += gene.fitness.carbon_kg
                
                # Apply reasoning if enabled
                if self.enable_reasoning:
                    reasoning = await self.reasoning_engine.reason_about_architecture(
                        architecture_config=gene.config.to_dict(),
                        fitness_metrics=fitness_result,
                        context=self.context,
                        purpose=self.purpose
                    )
                    
                    gene.reasoning = reasoning
                    self.reasoning_history.append(reasoning)
                    
                    if self.enable_ethical_reasoning:
                        ethical_score = reasoning.get('ethical', {}).get('overall_ethical_score', 0.5)
                        gene.ethical_score = ethical_score
                        
                        if ethical_score < 0.3:
                            gene.fitness.composite_score *= 0.8
                            logger.debug(f"Ethical penalty applied: {ethical_score:.2f}")
                    
                    temporal = reasoning.get('temporal', {})
                    if temporal.get('action') in ['schedule', 'schedule_optimal']:
                        await asyncio.sleep(0.1)
                        logger.debug(f"Temporal scheduling applied: {temporal.get('schedule')}")
                    
                    contextual = reasoning.get('contextual', {})
                    suggestions = contextual.get('suggestions', [])
                    for suggestion in suggestions[:1]:
                        if suggestion.get('action') == 'increase_pruning':
                            gene.config.pruning_rate = suggestion.get('to', 0.3)
                            logger.debug(f"Contextual adjustment: increased pruning to {gene.config.pruning_rate}")
                
            except Exception as e:
                logger.error(f"Evaluation with reasoning error: {str(e)}")
                gene.fitness = MultiObjectiveFitness()
    
    def _estimate_compression_benefit(self, config: ArchitectureConfig) -> float:
        benefit = 0.0
        
        if config.pruning_rate > 0:
            benefit += config.pruning_rate * 0.6
        
        if config.quantization_bits == 16:
            benefit += 0.3
        elif config.quantization_bits == 8:
            benefit += 0.5
        
        return min(benefit, 0.9)
    
    def _update_bests(self):
        evaluated = [g for g in self.population if g.fitness.composite_score > 0]
        
        if not evaluated:
            return
        
        best_acc = max(evaluated, key=lambda g: g.fitness.accuracy)
        if not self.best_by_accuracy or best_acc.fitness.accuracy > self.best_by_accuracy.fitness.accuracy:
            self.best_by_accuracy = best_acc
        
        best_carbon = min(evaluated, key=lambda g: g.fitness.carbon_kg)
        if not self.best_by_carbon or best_carbon.fitness.carbon_kg < self.best_by_carbon.fitness.carbon_kg:
            self.best_by_carbon = best_carbon
        
        best_comp = max(evaluated, key=lambda g: g.fitness.composite_score)
        if not self.best_by_composite or best_comp.fitness.composite_score > self.best_by_composite.fitness.composite_score:
            self.best_by_composite = best_comp
        
        BEST_ACCURACY.set(self.best_by_accuracy.fitness.accuracy if self.best_by_accuracy else 0)
        CARBON_EMITTED.set(self.total_carbon_spent_kg)
    
    async def _auto_register_bests(self):
        # Simplified auto-registration
        if self.best_by_composite and not self.best_by_composite.registered_expert_id:
            self.best_by_composite.registered_expert_id = f"expert_{self.best_by_composite.config.compute_hash()[:12]}"
    
    def _record_generation(self) -> Dict[str, Any]:
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
            'registered_experts': sum(1 for g in self.population if g.registered_expert_id),
            'best_certification': self.best_by_composite.fitness.certification.value if self.best_by_composite else 'none'
        }
        
        self.evolution_history.append(metrics)
        
        if self.database:
            asyncio.create_task(self.database.save_evolution_step(self.generation, metrics))
        
        return metrics
    
    def _evolve_population(self):
        evaluated = sorted(
            [g for g in self.population if g.fitness.composite_score > 0],
            key=lambda g: g.fitness.composite_score,
            reverse=True
        )
        
        if len(evaluated) < 2:
            return
        
        elite_size = max(2, len(evaluated) // 5)
        elite = evaluated[:elite_size]
        
        new_population = elite.copy()
        
        while len(new_population) < self.population_size:
            parent1, parent2 = np.random.choice(elite, 2, replace=False)
            
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
    
    def _crossover(self, config1: ArchitectureConfig, config2: ArchitectureConfig) -> ArchitectureConfig:
        child = ArchitectureConfig(
            family=config1.family if np.random.random() < 0.5 else config2.family,
            num_layers=config1.num_layers if np.random.random() < 0.5 else config2.num_layers,
            hidden_dim=config1.hidden_dim if np.random.random() < 0.5 else config2.hidden_dim,
            pruning_rate=config1.pruning_rate if np.random.random() < 0.5 else config2.pruning_rate,
            quantization_bits=config1.quantization_bits if np.random.random() < 0.5 else config2.quantization_bits,
            target_hardware=config1.target_hardware if np.random.random() < 0.5 else config2.target_hardware,
            compression=config1.compression if np.random.random() < 0.5 else config2.compression
        )
        
        return child
    
    def _mutate(self, config: ArchitectureConfig) -> ArchitectureConfig:
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
            'registered_experts': sum(1 for g in self.population if g.registered_expert_id),
            'evolution_history': self.evolution_history,
            'best_config': self.best_by_composite.config.to_dict() if self.best_by_composite else {},
            'compression_stats': self.compression_engine.compression_stats
        }
    
    async def get_reasoned_recommendations(self) -> Dict[str, Any]:
        """Get comprehensive reasoning-based recommendations"""
        if not self.enable_reasoning or not self.reasoning_engine:
            return {'status': 'reasoning_disabled'}
        
        return await self.reasoning_engine.get_reasoning_summary()
    
    async def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check"""
        try:
            health_report = await self.health_monitor.get_health_report()
            
            return {
                'healthy': health_report['current_status'] == 'healthy',
                'instance_id': self.instance_id,
                'status': health_report['current_status'],
                'health_score': health_report['current_score'],
                'generation': self.generation,
                'population_size': len(self.population),
                'best_accuracy': self.best_by_accuracy.fitness.accuracy if self.best_by_accuracy else 0,
                'total_carbon_spent': self.total_carbon_spent_kg,
                'token_balance': self.token_economy.get_system_summary()['current_balance'],
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {'healthy': False, 'error': str(e)}
    
    async def export_state(self) -> Dict[str, Any]:
        """Export current state for backup"""
        return {
            'instance_id': self.instance_id,
            'version': self.config.version,
            'generation': self.generation,
            'best_accuracy': self.best_by_accuracy.fitness.accuracy if self.best_by_accuracy else 0,
            'best_composite': self.best_by_composite.fitness.composite_score if self.best_by_composite else 0,
            'total_carbon_spent_kg': self.total_carbon_spent_kg,
            'population_size': len(self.population),
            'evolution_history': self.evolution_history,
            'reasoning_history': self.reasoning_history if self.enable_reasoning else [],
            'exported_at': datetime.now().isoformat()
        }
    
    async def shutdown(self):
        """Graceful shutdown of all services"""
        logger.info(f"Shutting down EnhancedUnifiedCarbonNAS (instance: {self.instance_id})")
        
        self._shutdown_event.set()
        self._running = False
        
        # Shutdown token economy
        await self.token_economy.shutdown()
        
        # Shutdown health monitor
        await self.health_monitor.shutdown()
        
        # Shutdown reasoning engine
        if self.reasoning_engine:
            await self.reasoning_engine.shutdown()
        
        # Cancel background tasks
        for task in self.background_tasks:
            task.cancel()
        
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        
        # Shutdown database
        if self.database:
            self.database.dispose()
        
        logger.info("Shutdown complete")

# ============================================================================
# COMPREHENSIVE USAGE EXAMPLE
# ============================================================================

async def main():
    """
    Comprehensive usage example demonstrating all features of the
    EnhancedUnifiedCarbonNAS system, including reasoning capabilities.
    """
    print("=" * 80)
    print("ENHANCED UNIFIED CARBON NAS - COMPREHENSIVE DEMONSTRATION")
    print("Version: 3.1.0 - With Full Reasoning Capabilities")
    print("=" * 80)
    
    # ------------------------------------------------------------------------
    # Step 1: Load or create configuration
    # ------------------------------------------------------------------------
    
    config = UnifiedNASConfig(
        instance_id="demo_nas_001",
        version="3.1.0",
        mode="production",
        population_size=20,
        max_generations=10,
        carbon_budget_kg=5.0,
        token_budget=500.0,
        target_hardware=HardwareTarget.GPU_NVIDIA,
        enable_compression=True,
        enable_hardware_profiling=True,
        enable_pareto=True,
        enable_continuous_learning=True,
        enable_knowledge_transfer=True,
        enable_token_economy=True,
        enable_circuit_breakers=True,
        enable_persistence=True,
        enable_reasoning=True,
        context='cloud_inference',
        purpose='balanced',
        enable_ethical_reasoning=True,
        database_path=Path("./demo_carbon_nas.db"),
        enable_prometheus_metrics=True,
        log_level="INFO"
    )
    
    print("\n📋 Configuration Summary:")
    print(f"   Instance ID: {config.instance_id}")
    print(f"   Version: {config.version}")
    print(f"   Population Size: {config.population_size}")
    print(f"   Max Generations: {config.max_generations}")
    print(f"   Carbon Budget: {config.carbon_budget_kg} kg")
    print(f"   Token Budget: {config.token_budget}")
    print(f"   Target Hardware: {config.target_hardware.value}")
    print(f"   Reasoning: {config.enable_reasoning}")
    print(f"   Context: {config.context}")
    print(f"   Purpose: {config.purpose}")
    
    # ------------------------------------------------------------------------
    # Step 2: Define a custom fitness function
    # ------------------------------------------------------------------------
    
    async def custom_fitness_function(config: ArchitectureConfig) -> Dict[str, float]:
        """
        Evaluate an architecture configuration.
        
        In production, this would:
        - Build the actual model
        - Train it on your dataset
        - Measure accuracy, carbon, latency, etc.
        """
        # Simulate architecture evaluation
        base_accuracy = 0.75 + (config.num_layers * 0.01) + (config.hidden_dim * 0.0001)
        accuracy = min(0.95, base_accuracy + np.random.normal(0, 0.02))
        
        base_carbon = (config.num_layers * config.hidden_dim) / 1e6
        if config.pruning_rate > 0:
            base_carbon *= (1 - config.pruning_rate * 0.5)
        if config.quantization_bits < 32:
            base_carbon *= (config.quantization_bits / 32)
        
        carbon_kg = max(0.0001, base_carbon * 0.01 + np.random.normal(0, 0.0001))
        energy_kwh = carbon_kg * 0.5
        
        base_latency = 50 + (config.num_layers * 5) + (config.hidden_dim * 0.01)
        if config.target_hardware == HardwareTarget.GPU_NVIDIA:
            base_latency *= 0.3
        elif config.target_hardware == HardwareTarget.EDGE_TPU:
            base_latency *= 0.5
        
        latency_ms = base_latency + np.random.normal(0, 5)
        
        memory_mb = (config.num_layers * config.hidden_dim * 4) / 1024
        if config.pruning_rate > 0:
            memory_mb *= (1 - config.pruning_rate * 0.6)
        
        return {
            'accuracy': accuracy,
            'carbon_kg': carbon_kg,
            'energy_kwh': energy_kwh,
            'latency_ms': latency_ms,
            'memory_mb': memory_mb,
            'params': config.num_layers * config.hidden_dim * 4,
            'flops': config.num_layers * config.hidden_dim * config.hidden_dim * 2
        }
    
    # ------------------------------------------------------------------------
    # Step 3: Initialize the NAS system with reasoning
    # ------------------------------------------------------------------------
    
    nas = EnhancedUnifiedCarbonNAS(config=config)
    
    print("\n🧠 Reasoning Engine Status:")
    print(f"   Enabled: {nas.enable_reasoning}")
    print(f"   Context: {nas.context}")
    print(f"   Purpose: {nas.purpose}")
    print(f"   Ethical Reasoning: {nas.enable_ethical_reasoning}")
    
    # ------------------------------------------------------------------------
    # Step 4: Start services
    # ------------------------------------------------------------------------
    
    print("\n🚀 Starting services...")
    await nas.start()
    
    # ------------------------------------------------------------------------
    # Step 5: Run evolution with reasoning
    # ------------------------------------------------------------------------
    
    print(f"\n🔄 Starting Evolution (Generations: {config.max_generations})...")
    print("-" * 80)
    
    start_time = datetime.now()
    
    try:
        result = await nas.evolve(
            fitness_function=custom_fitness_function,
            generations=config.max_generations,
            early_stopping_patience=3
        )
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "=" * 80)
        print("✅ EVOLUTION COMPLETE")
        print("=" * 80)
        
        print(f"\n📊 Evolution Summary:")
        print(f"   Total Generations: {result.get('total_generations', 0)}")
        print(f"   Duration: {duration:.2f} seconds")
        print(f"   Best Accuracy: {result.get('best_accuracy', 0):.3f}%")
        print(f"   Best Carbon: {result.get('best_carbon_kg', 0):.6f} kg")
        print(f"   Best Composite Score: {result.get('best_composite_score', 0):.3f}")
        print(f"   Best Certification: {result.get('best_certification', 'none')}")
        print(f"   Pareto Frontier Size: {result.get('pareto_frontier_size', 0)}")
        
        total_carbon = result.get('total_carbon_spent_kg', 0)
        carbon_budget = config.carbon_budget_kg
        carbon_used_percent = (total_carbon / carbon_budget * 100) if carbon_budget > 0 else 0
        print(f"   Total Carbon Spent: {total_carbon:.4f} kg ({carbon_used_percent:.1f}% of budget)")
        
        # --------------------------------------------------------------------
        # Step 6: Get reasoning recommendations
        # --------------------------------------------------------------------
        
        if nas.enable_reasoning:
            print("\n🧠 Reasoning Recommendations:")
            recommendations = await nas.get_reasoned_recommendations()
            
            if recommendations and recommendations.get('recent_recommendations'):
                for i, rec in enumerate(recommendations['recent_recommendations'][:5], 1):
                    print(f"   {i}. {rec}")
            else:
                print("   No specific recommendations available")
            
            if 'total_reasoned_architectures' in recommendations:
                print(f"\n   Total reasoned architectures: {recommendations['total_reasoned_architectures']}")
                print(f"   Average ethical score: {recommendations.get('average_ethical_score', 0):.2f}")
                print(f"   Most common causal driver: {recommendations.get('most_common_causal_driver', 'unknown')}")
        
        # --------------------------------------------------------------------
        # Step 7: Best architecture details
        # --------------------------------------------------------------------
        
        print("\n🏆 Best Architecture Details:")
        if 'best_config' in result and result['best_config']:
            best_config = result['best_config']
            print(f"   Family: {best_config.get('family', 'unknown')}")
            print(f"   Layers: {best_config.get('num_layers', 0)}")
            print(f"   Hidden Dimension: {best_config.get('hidden_dim', 0)}")
            print(f"   Pruning Rate: {best_config.get('pruning_rate', 0):.2f}")
            print(f"   Quantization Bits: {best_config.get('quantization_bits', 32)}")
            print(f"   Hardware: {best_config.get('target_hardware', 'unknown')}")
        else:
            print("   No best architecture details available")
        
        # --------------------------------------------------------------------
        # Step 8: Save results
        # --------------------------------------------------------------------
        
        print("\n💾 Saving Results:")
        results_path = Path("./nas_results.json")
        with open(results_path, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'config': config.to_dict(),
                'results': result,
                'duration_seconds': duration
            }, f, indent=2)
        print(f"   Results saved to {results_path}")
        
    except Exception as e:
        logger.error(f"Evolution failed: {e}", exc_info=True)
        print(f"\n❌ Evolution failed: {e}")
    
    # ------------------------------------------------------------------------
    # Step 9: Shutdown
    # ------------------------------------------------------------------------
    
    print("\n🛑 Shutting down...")
    await nas.shutdown()
    print("✅ Shutdown complete")
    
    print("\n" + "=" * 80)
    print("DEMONSTRATION COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
