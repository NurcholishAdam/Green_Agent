# src/enhancements/carbon_nas.py

"""
Carbon-Aware Neural Architecture Search (NAS) for Green Agent
Scientific basis: Energy consumption of training is proportional to parameters × steps

Reference: "Carbon-Aware Neural Architecture Search" (NeurIPS, 2023)
"""

import numpy as np
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
import itertools
import logging

logger = logging.getLogger(__name__)


class OperationType(Enum):
    """Neural network operation types"""
    CONV3x3 = "conv3x3"
    CONV5x5 = "conv5x5"
    CONV7x7 = "conv7x7"
    MAXPOOL = "maxpool"
    AVGPOOL = "avgpool"
    IDENTITY = "identity"
    SKIP_CONNECT = "skip_connect"
    LINEAR = "linear"
    ATTENTION = "attention"
    MLP = "mlp"


@dataclass
class ArchitectureConfig:
    """Configuration for a neural architecture"""
    num_layers: int
    hidden_size: int
    num_heads: int
    operations: List[OperationType]
    quantization: str  # 'FP32', 'FP16', 'INT8', 'INT4'
    parallelism: int


@dataclass
class ArchitectureMetrics:
    """Metrics for an architecture"""
    accuracy: float
    latency_ms: float
    training_energy_joules: float
    inference_energy_joules: float
    total_carbon_kg: float
    params_millions: float
    flops_billions: float
    helium_footprint: float


@dataclass
class ParetoPoint:
    """Point on Pareto frontier"""
    config: ArchitectureConfig
    metrics: ArchitectureMetrics
    dominates: List[int] = field(default_factory=list)
    dominated_by: List[int] = field(default_factory=list)


class CarbonAwareNAS:
    """
    Carbon-Aware Neural Architecture Search.
    
    Search for architectures that minimize:
    C_total = C_train + Σ(C_inference_i * n_i)
    
    Where C = Energy * Carbon_Intensity * (1 + Helium_Weight)
    """
    
    # Energy per operation (Joules per FLOP)
    ENERGY_PER_OP = {
        OperationType.CONV3x3: 2.0e-11,
        OperationType.CONV5x5: 5.0e-11,
        OperationType.CONV7x7: 1.0e-10,
        OperationType.MAXPOOL: 1.0e-12,
        OperationType.AVGPOOL: 1.0e-12,
        OperationType.IDENTITY: 0,
        OperationType.SKIP_CONNECT: 1.0e-13,
        OperationType.LINEAR: 1.0e-11,
        OperationType.ATTENTION: 5.0e-11,
        OperationType.MLP: 2.0e-11
    }
    
    # Search space bounds
    SEARCH_SPACE = {
        'num_layers': [6, 12, 24, 48],
        'hidden_size': [128, 256, 512, 1024],
        'num_heads': [4, 8, 12, 16],
        'operations': list(OperationType),
        'quantization': ['FP32', 'FP16', 'INT8', 'INT4'],
        'parallelism': [1, 2, 4, 8]
    }
    
    # Carbon intensity by region (gCO2/kWh)
    CARBON_INTENSITY = {
        'us-east': 380,
        'us-west': 250,
        'eu-north': 80,
        'asia-pacific': 550
    }
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.region = self.config.get('region', 'us-east')
        self.expected_inferences = self.config.get('expected_inferences', 1_000_000)
        self.carbon_intensity = self.CARBON_INTENSITY.get(self.region, 400)
        self.helium_weight = self.config.get('helium_weight', 0.3)
        
        # Storage for searched architectures
        self.explored_architectures: List[Tuple[ArchitectureConfig, ArchitectureMetrics]] = []
        self.pareto_frontier: List[ParetoPoint] = []
        
    def estimate_training_flops(self, config: ArchitectureConfig) -> float:
        """Estimate training FLOPs for an architecture"""
        # Simplified model: FLOPs = hidden^2 * layers * 3 (forward+backward+update)
        flops_per_forward = config.hidden_size ** 2 * config.num_layers
        training_flops = flops_per_forward * 1000  # Assume 1000 training steps
        
        # Adjust for operations
        for op in config.operations:
            if op in [OperationType.ATTENTION]:
                training_flops *= 1.5
            elif op in [OperationType.MLP]:
                training_flops *= 1.2
        
        return training_flops
    
    def estimate_inference_flops(self, config: ArchitectureConfig) -> float:
        """Estimate inference FLOPs for an architecture"""
        flops_per_forward = config.hidden_size ** 2 * config.num_layers
        
        # Quantization reduces compute
        quantization_factors = {
            'FP32': 1.0,
            'FP16': 0.8,
            'INT8': 0.4,
            'INT4': 0.2
        }
        quant_factor = quantization_factors.get(config.quantization, 1.0)
        
        total_flops = flops_per_forward * quant_factor * self.expected_inferences
        
        return total_flops
    
    def calculate_training_energy(self, flops: float, config: ArchitectureConfig) -> float:
        """Calculate training energy in Joules"""
        # Average energy per FLOP based on operations
        avg_energy_per_flop = np.mean([self.ENERGY_PER_OP.get(op, 1e-11) 
                                        for op in config.operations])
        
        # Parallelism reduces per-device load but adds communication
        energy = flops * avg_energy_per_flop / config.parallelism
        
        # Quantization reduces energy
        quantization_factors = {
            'FP32': 1.0,
            'FP16': 0.6,
            'INT8': 0.25,
            'INT4': 0.15
        }
        energy *= quantization_factors.get(config.quantization, 1.0)
        
        return energy
    
    def calculate_inference_energy(self, flops: float, config: ArchitectureConfig) -> float:
        """Calculate inference energy in Joules"""
        avg_energy_per_flop = np.mean([self.ENERGY_PER_OP.get(op, 1e-11) 
                                        for op in config.operations])
        
        energy = flops * avg_energy_per_flop / config.parallelism
        
        # Amortized over expected inferences
        return energy
    
    def estimate_carbon(self, energy_joules: float) -> float:
        """Estimate carbon emissions in kg CO2"""
        energy_kwh = energy_joules / 3.6e6
        carbon_kg = energy_kwh * self.carbon_intensity / 1000
        return carbon_kg
    
    def estimate_helium_footprint(self, config: ArchitectureConfig) -> float:
        """Estimate helium footprint based on hardware parallelism and quantization"""
        base_footprint = config.parallelism * 0.1
        
        # Quantization reduces helium needs
        quantization_factors = {
            'FP32': 1.0,
            'FP16': 0.9,
            'INT8': 0.7,
            'INT4': 0.5
        }
        footprint = base_footprint * quantization_factors.get(config.quantization, 1.0)
        
        # Larger models need more helium
        footprint *= np.log2(config.hidden_size) / 10
        
        return min(1.0, footprint)
    
    def evaluate_architecture(self, config: ArchitectureConfig) -> ArchitectureMetrics:
        """
        Evaluate an architecture configuration.
        
        Returns metrics including carbon footprint.
        """
        # Estimate FLOPs
        train_flops = self.estimate_training_flops(config)
        inference_flops = self.estimate_inference_flops(config)
        
        # Estimate energy
        train_energy = self.calculate_training_energy(train_flops, config)
        inference_energy = self.calculate_inference_energy(inference_flops, config)
        
        # Estimate carbon (training + inference)
        train_carbon = self.estimate_carbon(train_energy)
        inference_carbon = self.estimate_carbon(inference_energy)
        total_carbon = train_carbon + inference_carbon
        
        # Estimate accuracy (simplified model)
        accuracy = 0.7 + 0.3 * (1 - 1 / np.log2(config.hidden_size + 1))
        # Quantization reduces accuracy
        quantization_impacts = {
            'FP32': 0,
            'FP16': 0.01,
            'INT8': 0.03,
            'INT4': 0.08
        }
        accuracy -= quantization_impacts.get(config.quantization, 0)
        accuracy = max(0.6, min(0.95, accuracy))
        
        # Estimate latency
        latency_base = config.num_layers * config.hidden_size / 1e6
        latency_ms = latency_base * 1000 / config.parallelism
        
        # Estimated parameters
        params_millions = config.hidden_size ** 2 * config.num_layers / 1e6
        
        # Helium footprint
        helium_footprint = self.estimate_helium_footprint(config)
        
        # FLOPs (billions)
        flops_billions = (train_flops + inference_flops) / 1e9
        
        return ArchitectureMetrics(
            accuracy=accuracy,
            latency_ms=latency_ms,
            training_energy_joules=train_energy,
            inference_energy_joules=inference_energy,
            total_carbon_kg=total_carbon,
            params_millions=params_millions,
            flops_billions=flops_billions,
            helium_footprint=helium_footprint
        )
    
    def search_pareto_frontier(self, max_architectures: int = 100) -> List[ParetoPoint]:
        """
        Search for Pareto-optimal architectures.
        
        Objectives (minimize):
        1. Carbon emissions
        2. Latency
        3. Helium footprint
        
        Maximize:
        1. Accuracy
        """
        # Generate random samples (simplified)
        import random
        
        self.explored_architectures = []
        
        for _ in range(max_architectures):
            config = ArchitectureConfig(
                num_layers=random.choice(self.SEARCH_SPACE['num_layers']),
                hidden_size=random.choice(self.SEARCH_SPACE['hidden_size']),
                num_heads=random.choice(self.SEARCH_SPACE['num_heads']),
                operations=random.sample(self.SEARCH_SPACE['operations'], 3),
                quantization=random.choice(self.SEARCH_SPACE['quantization']),
                parallelism=random.choice(self.SEARCH_SPACE['parallelism'])
            )
            
            metrics = self.evaluate_architecture(config)
            self.explored_architectures.append((config, metrics))
        
        # Compute Pareto frontier
        self.pareto_frontier = self._compute_pareto_frontier()
        
        return self.pareto_frontier
    
    def _compute_pareto_frontier(self) -> List[ParetoPoint]:
        """
        Compute Pareto frontier for 4 objectives:
        Maximize accuracy, minimize carbon, latency, helium
        """
        points = []
        
        for i, (config, metrics) in enumerate(self.explored_architectures):
            points.append(ParetoPoint(config=config, metrics=metrics))
        
        # Check dominance
        for i, point in enumerate(points):
            for j, other in enumerate(points):
                if i != j:
                    # Check if point dominates other
                    if (point.metrics.accuracy >= other.metrics.accuracy and
                        point.metrics.total_carbon_kg <= other.metrics.total_carbon_kg and
                        point.metrics.latency_ms <= other.metrics.latency_ms and
                        point.metrics.helium_footprint <= other.metrics.helium_footprint and
                        (point.metrics.accuracy > other.metrics.accuracy or
                         point.metrics.total_carbon_kg < other.metrics.total_carbon_kg or
                         point.metrics.latency_ms < other.metrics.latency_ms or
                         point.metrics.helium_footprint < other.metrics.helium_footprint)):
                        point.dominates.append(j)
                    
                    # Check if dominated by other
                    if (other.metrics.accuracy >= point.metrics.accuracy and
                        other.metrics.total_carbon_kg <= point.metrics.total_carbon_kg and
                        other.metrics.latency_ms <= point.metrics.latency_ms and
                        other.metrics.helium_footprint <= point.metrics.helium_footprint and
                        (other.metrics.accuracy > point.metrics.accuracy or
                         other.metrics.total_carbon_kg < point.metrics.total_carbon_kg or
                         other.metrics.latency_ms < point.metrics.latency_ms or
                         other.metrics.helium_footprint < point.metrics.helium_footprint)):
                        point.dominated_by.append(j)
        
        # Pareto optimal = points with no dominating points
        pareto_optimal = [p for p in points if len(p.dominated_by) == 0]
        
        logger.info(f"Found {len(pareto_optimal)} Pareto-optimal architectures out of {len(points)}")
        
        return pareto_optimal
    
    def select_optimal_architecture(self, carbon_budget_kg: float = float('inf'),
                                   latency_budget_ms: float = float('inf'),
                                   helium_budget: float = 1.0,
                                   min_accuracy: float = 0.7) -> Optional[ArchitectureConfig]:
        """
        Select optimal architecture given constraints.
        
        Uses weighted scoring when multiple Pareto-optimal options exist.
        """
        if not self.pareto_frontier:
            self.search_pareto_frontier()
        
        # Filter by constraints
        feasible = []
        for point in self.pareto_frontier:
            m = point.metrics
            if (m.total_carbon_kg <= carbon_budget and
                m.latency_ms <= latency_budget_ms and
                m.helium_footprint <= helium_budget and
                m.accuracy >= min_accuracy):
                feasible.append(point)
        
        if not feasible:
            logger.warning("No feasible architectures found with given constraints")
            return None
        
        # Score feasible architectures
        for point in feasible:
            m = point.metrics
            
            # Normalize metrics (0-1 scale)
            carbon_score = 1 - (m.total_carbon_kg / carbon_budget) if carbon_budget > 0 else 1
            latency_score = 1 - (m.latency_ms / latency_budget_ms) if latency_budget_ms > 0 else 1
            helium_score = 1 - m.helium_footprint / helium_budget if helium_budget > 0 else 1
            accuracy_score = m.accuracy
            
            # Weighted sum
            point.score = (0.3 * carbon_score + 
                          0.2 * latency_score + 
                          0.2 * helium_score + 
                          0.3 * accuracy_score)
        
        # Select best
        best = max(feasible, key=lambda x: x.score)
        
        logger.info(f"Selected architecture: {best.config.num_layers} layers, "
                   f"{best.config.hidden_size} hidden, {best.config.quantization}")
        logger.info(f"  Carbon: {best.metrics.total_carbon_kg:.2f}kg, "
                   f"Latency: {best.metrics.latency_ms:.1f}ms, "
                   f"Accuracy: {best.metrics.accuracy:.2%}")
        
        return best.config
    
    def get_carbon_optimal_architecture(self, task_constraints: Dict) -> ArchitectureConfig:
        """
        Main interface for Layer 0/4 integration.
        
        Returns carbon-optimal architecture for given task.
        """
        carbon_budget = task_constraints.get('carbon_budget_kg', 100.0)
        latency_budget = task_constraints.get('latency_budget_ms', 100.0)
        helium_budget = task_constraints.get('helium_budget', 1.0)
        min_accuracy = task_constraints.get('min_accuracy', 0.7)
        
        return self.select_optimal_architecture(
            carbon_budget_kg=carbon_budget,
            latency_budget_ms=latency_budget,
            helium_budget=helium_budget,
            min_accuracy=min_accuracy
        )
