# ============================================================================
# ENHANCED MODULE 1: Advanced Configuration Management
# ============================================================================

from pydantic import BaseModel, Field, validator, ValidationError
from typing import Optional, List, Dict, Any, Union
from pathlib import Path
import asyncio, logging, time, uuid, hashlib, json, math, copy, random
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Callable, Set, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict, deque
import numpy as np
import torch
import torch.nn as nn
from contextlib import contextmanager
import yaml
from pydantic import BaseModel, Field, validator, ValidationError
from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, Boolean, Text, JSON, Index, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import yaml
import json


# Add to imports at top of carbon_nas_unified.py
from src.enhancements.reasoning_engine import (
    GreenAgentReasoningEngine,
    CarbonIntensityAwareScheduler,
    CarbonCausalModel,
    EthicalCarbonReasoner,
    ContextAwareOptimizer,
    SystemicCarbonPlanner,
    PurposeAwareOptimizer
)

# Modify UnifiedCarbonNAS class to include reasoning
class UnifiedCarbonNAS:
    """Enhanced Unified Carbon NAS with reasoning capabilities"""
    
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
        min_accuracy_threshold: float = 0.85,
        # New reasoning parameters
        enable_reasoning: bool = True,
        context: str = 'cloud_inference',
        purpose: str = 'balanced',
        enable_ethical_reasoning: bool = True
    ):
        # Original initialization
        # ... [existing code] ...
        
        # New: Reasoning engine
        self.enable_reasoning = enable_reasoning
        self.context = context
        self.purpose = purpose
        self.enable_ethical_reasoning = enable_ethical_reasoning
        
        if enable_reasoning:
            self.reasoning_engine = GreenAgentReasoningEngine()
            self.reasoning_history = []
            logger.info("Reasoning engine enabled")
        else:
            self.reasoning_engine = None
            logger.info("Reasoning engine disabled")

    # Modified evaluation with reasoning
    async def _evaluate_population(self, fitness_function: Callable):
        """Evaluate all architectures with reasoning"""
        for gene in self.population:
            if gene.fitness.composite_score > 0:
                continue
            
            try:
                # Original evaluation
                fitness_result = await fitness_function(gene.config)
                
                # ... [existing fitness calculation] ...
                
                # Apply reasoning if enabled
                if self.enable_reasoning:
                    reasoning = await self.reasoning_engine.reason_about_architecture(
                        architecture_config=gene.config.to_dict(),
                        fitness_metrics=fitness_result,
                        context=self.context,
                        purpose=self.purpose
                    )
                    
                    # Store reasoning result
                    gene.reasoning = reasoning
                    self.reasoning_history.append(reasoning)
                    
                    # Apply reasoning insights
                    if self.enable_ethical_reasoning:
                        ethical_score = reasoning.get('ethical', {}).get('overall_ethical_score', 0.5)
                        # Adjust fitness based on ethical score
                        if ethical_score < 0.3:
                            # Penalize unethical architectures
                            gene.fitness.composite_score *= 0.8
                            logger.debug(f"Ethical penalty applied: {ethical_score:.2f}")
                    
                    # Apply temporal recommendations
                    temporal = reasoning.get('temporal', {})
                    if temporal.get('action') == 'schedule':
                        # Postpone evaluation to better time
                        await asyncio.sleep(0.1)  # Simulated delay
                        logger.debug(f"Temporal scheduling applied: {temporal.get('schedule')}")
                    
                    # Apply contextual recommendations
                    contextual = reasoning.get('contextual', {})
                    suggestions = contextual.get('suggestions', [])
                    for suggestion in suggestions[:1]:
                        if suggestion.get('action') == 'increase_pruning':
                            gene.config.pruning_rate = suggestion.get('to', 0.3)
                            logger.debug(f"Contextual adjustment: increased pruning to {gene.config.pruning_rate}")
            
            except Exception as e:
                logger.error(f"Evaluation with reasoning error: {str(e)}")
                # Fallback to original evaluation
                gene.fitness = MultiObjectiveFitness()

    # New method: Get reasoned recommendations
    async def get_reasoned_recommendations(self) -> Dict[str, Any]:
        """Get comprehensive reasoning-based recommendations"""
        if not self.enable_reasoning:
            return {'status': 'reasoning_disabled'}
        
        return await self.reasoning_engine.get_reasoning_summary()

    # Modified evolution method
    async def evolve(
        self,
        fitness_function: Callable,
        generations: Optional[int] = None,
        early_stopping_patience: int = 10
    ) -> Dict[str, Any]:
        """Enhanced evolution with reasoning capabilities"""
        
        # Systemic planning
        if self.enable_reasoning:
            plan = self.reasoning_engine.planner.plan_carbon_investment(
                current_accuracy=0.75,  # Estimate from initial population
                target_accuracy=0.90,
                carbon_budget=self.carbon_budget_kg
            )
            
            if plan['decision'] == 'save':
                logger.info(f"Systemic decision: {plan['reason']}")
                return {'status': 'postponed', 'reason': plan['reason']}
            
            logger.info(f"Systemic decision: {plan['decision']} - {plan['reason']}")
        
        # Rest of evolution with reasoning
        # ... [existing evolution code with reasoning integration] ...
        
        return super().evolve(fitness_function, generations, early_stopping_patience)

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
    auto_backup_interval_hours: int = Field(24, ge=1, le=168)
    
    # Observability
    enable_prometheus_metrics: bool = True
    log_level: str = Field("INFO", regex='^(DEBUG|INFO|WARNING|ERROR)$')
    enable_structured_logging: bool = True
    metrics_port: int = Field(9090, ge=1024, le=65535)
    
    @validator('carbon_budget_kg')
    def validate_carbon_budget(cls, v):
        if v < 0.01:
            raise ValueError("Carbon budget must be at least 0.01 kg")
        return v
    
    @classmethod
    def from_yaml(cls, path: Path) -> "UnifiedNASConfig":
        """Load configuration from YAML file"""
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
        return cls(**data)
    
    @classmethod
    def from_json(cls, path: Path) -> "UnifiedNASConfig":
        """Load configuration from JSON file"""
        with open(path, 'r') as f:
            data = json.load(f)
        return cls(**data)
    
    def to_yaml(self, path: Path):
        """Save configuration to YAML file"""
        with open(path, 'w') as f:
            yaml.dump(self.dict(), f, default_flow_style=False)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with proper type handling"""
        config_dict = self.dict()
        # Convert Path objects to strings
        if 'database_path' in config_dict:
            config_dict['database_path'] = str(config_dict['database_path'])
        return config_dict

# ============================================================================
# ENHANCED MODULE 2: Advanced Token Economy System
# ============================================================================

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
class TokenTransaction:
    """Detailed token transaction record"""
    consumer: TokenConsumer
    amount: float
    source: TokenSource
    timestamp: float = field(default_factory=time.time)
    success: bool = True
    correlation_id: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

class EnhancedTokenEconomy:
    """
    Advanced token economy system with:
    - Dynamic pricing based on demand
    - Renewable token injection
    - Efficiency bonuses for optimized architectures
    - Transaction history with analytics
    """
    
    def __init__(self,
                 total_budget: float = 1000.0,
                 renewable_rate: float = 0.02,
                 max_overflow: float = 0.3,
                 dynamic_pricing: bool = True):
        self.total_budget = total_budget
        self.current_balance = total_budget
        self.renewable_rate = renewable_rate
        self.max_overflow = max_overflow
        self.dynamic_pricing = dynamic_pricing
        
        self._lock = asyncio.Lock()
        self.transactions: List[TokenTransaction] = []
        self._last_injection_time = time.time()
        
        # Pricing tiers (cost per token)
        self.base_prices = {
            TokenConsumer.MODEL_TRAINING: 1.0,
            TokenConsumer.ARCHITECTURE_EVAL: 0.5,
            TokenConsumer.KNOWLEDGE_TRANSFER: 0.3,
            TokenConsumer.COMPRESSION: 0.2,
            TokenConsumer.EXPERT_REGISTRATION: 0.1
        }
        
        # Metrics
        self.metrics = {
            'total_allocated': 0.0,
            'total_rejected': 0.0,
            'total_renewed': 0.0,
            'consumer_usage': defaultdict(float),
            'source_usage': defaultdict(float),
            'average_transaction_cost': 0.0,
            'peak_balance': total_budget,
            'lowest_balance': total_budget
        }
        
        # Background task
        self._running = False
        self._injection_task = None
        
        logger.info(f"EnhancedTokenEconomy initialized with budget: {total_budget}")
    
    async def start(self):
        """Start background token renewal"""
        self._running = True
        self._injection_task = asyncio.create_task(self._renewal_loop())
        logger.info("Token economy renewal loop started")
    
    async def _renewal_loop(self):
        """Background token renewal with adaptive rates"""
        while self._running:
            try:
                # Adaptive renewal based on current balance
                balance_ratio = self.current_balance / self.total_budget
                if balance_ratio < 0.2:
                    # Low balance: inject more tokens
                    renewal_multiplier = 2.0
                elif balance_ratio > 0.8:
                    # High balance: inject fewer tokens
                    renewal_multiplier = 0.5
                else:
                    renewal_multiplier = 1.0
                
                await asyncio.sleep(60)  # Renew every minute
                await self._inject_renewable_tokens(renewal_multiplier)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Token renewal error: {e}")
                await asyncio.sleep(60)
    
    async def _inject_renewable_tokens(self, multiplier: float = 1.0):
        """Inject renewable tokens with dynamic rate"""
        async with self._lock:
            now = time.time()
            time_diff = now - self._last_injection_time
            
            # Base injection amount
            base_injection = self.total_budget * self.renewable_rate * (time_diff / 60)
            injection_amount = base_injection * multiplier
            
            # Apply cap based on max overflow
            max_balance = self.total_budget * (1 + self.max_overflow)
            if self.current_balance + injection_amount > max_balance:
                injection_amount = max(0, max_balance - self.current_balance)
            
            if injection_amount > 0:
                self.current_balance += injection_amount
                self._last_injection_time = now
                self.metrics['total_renewed'] += injection_amount
                
                # Update metrics
                if self.current_balance > self.metrics['peak_balance']:
                    self.metrics['peak_balance'] = self.current_balance
                
                # Record transaction
                self.transactions.append(TokenTransaction(
                    consumer=TokenConsumer.CONTINUOUS_LEARNING,
                    amount=injection_amount,
                    source=TokenSource.RENEWABLE,
                    metadata={'multiplier': multiplier}
                ))
                
                logger.debug(f"Injected {injection_amount:.2f} renewable tokens (multiplier: {multiplier:.2f})")
    
    async def reserve_tokens(self,
                           consumer: TokenConsumer,
                           amount: float,
                           source: TokenSource = TokenSource.CARBON_BUDGET,
                           metadata: Optional[Dict] = None) -> Tuple[bool, float]:
        """
        Reserve tokens with dynamic pricing.
        
        Returns:
            (success, actual_cost)
        """
        async with self._lock:
            # Calculate dynamic price
            actual_cost = amount
            if self.dynamic_pricing:
                price_multiplier = self._calculate_price_multiplier()
                actual_cost = amount * price_multiplier
            
            # Check if enough balance
            if self.current_balance < actual_cost:
                self.metrics['total_rejected'] += actual_cost
                logger.warning(f"Token reservation failed for {consumer}: "
                             f"insufficient balance ({self.current_balance:.2f} < {actual_cost:.2f})")
                return False, actual_cost
            
            # Deduct from balance
            self.current_balance -= actual_cost
            self.metrics['total_allocated'] += actual_cost
            self.metrics['consumer_usage'][consumer] += actual_cost
            
            if source in self.metrics['source_usage']:
                self.metrics['source_usage'][source] += actual_cost
            
            # Update lowest balance
            if self.current_balance < self.metrics['lowest_balance']:
                self.metrics['lowest_balance'] = self.current_balance
            
            # Record transaction
            self.transactions.append(TokenTransaction(
                consumer=consumer,
                amount=actual_cost,
                source=source,
                success=True,
                metadata=metadata or {}
            ))
            
            # Update average cost
            total_cost = sum(t.amount for t in self.transactions)
            self.metrics['average_transaction_cost'] = total_cost / len(self.transactions)
            
            logger.debug(f"Reserved {actual_cost:.2f} tokens for {consumer} "
                        f"(remaining: {self.current_balance:.2f})")
            return True, actual_cost
    
    def _calculate_price_multiplier(self) -> float:
        """Calculate dynamic price multiplier based on demand"""
        balance_ratio = self.current_balance / self.total_budget
        
        if balance_ratio < 0.1:
            return 3.0  # Very expensive when almost out
        elif balance_ratio < 0.25:
            return 2.0
        elif balance_ratio < 0.5:
            return 1.5
        elif balance_ratio < 0.75:
            return 1.0
        elif balance_ratio < 0.9:
            return 0.8
        else:
            return 0.6  # Cheap when abundant
    
    async def get_efficiency_bonus(self, architecture: 'ArchitectureGene') -> float:
        """
        Calculate efficiency bonus based on architecture quality.
        Returns bonus multiplier (0.8-1.2) that can reduce token costs.
        """
        bonus = 1.0
        
        if architecture.fitness:
            # Higher accuracy -> bonus
            if architecture.fitness.accuracy > 0.9:
                bonus *= 0.8  # 20% discount
            elif architecture.fitness.accuracy > 0.8:
                bonus *= 0.9  # 10% discount
            
            # Lower carbon -> bonus
            if architecture.fitness.carbon_kg < 0.0005:
                bonus *= 0.85
            elif architecture.fitness.carbon_kg < 0.001:
                bonus *= 0.95
            
            # Compression -> bonus
            if architecture.config.compression != CompressionMethod.NONE:
                bonus *= 0.9
        
        return max(0.7, min(1.3, bonus))  # Clamp between 0.7 and 1.3
    
    def get_system_summary(self) -> Dict[str, Any]:
        """Get comprehensive system summary"""
        consumed = self.total_budget - self.current_balance
        
        return {
            'total_budget': self.total_budget,
            'current_balance': self.current_balance,
            'consumed_percentage': (consumed / self.total_budget) * 100,
            'total_allocated': self.metrics['total_allocated'],
            'total_rejected': self.metrics['total_rejected'],
            'total_renewed': self.metrics['total_renewed'],
            'peak_balance': self.metrics['peak_balance'],
            'lowest_balance': self.metrics['lowest_balance'],
            'average_transaction_cost': self.metrics['average_transaction_cost'],
            'consumer_usage': dict(self.metrics['consumer_usage']),
            'source_usage': dict(self.metrics['source_usage']),
            'transaction_count': len(self.transactions),
            'last_transaction': self.transactions[-1] if self.transactions else None
        }
    
    async def shutdown(self):
        """Clean shutdown"""
        self._running = False
        if self._injection_task:
            self._injection_task.cancel()
            try:
                await self._injection_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Token economy shutdown complete")

# ============================================================================
# ENHANCED MODULE 3: Advanced Pareto Optimization with Hypervolume
# ============================================================================

class EnhancedParetoOptimizer:
    """
    Enhanced Pareto optimizer with:
    - Hypervolume calculation
    - Reference point adaptation
    - Diversity preservation
    - Multi-frontier support
    """
    
    def __init__(self,
                 reference_point: Optional[Dict[str, float]] = None,
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
    
    def find_pareto_optimal(self,
                           population: List[ArchitectureGene],
                           objectives: Optional[List[str]] = None) -> List[ArchitectureGene]:
        """
        Find Pareto-optimal architectures with diversity preservation.
        """
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
        
        # Apply diversity preservation
        if len(pareto_optimal) > 1:
            pareto_optimal = self._preserve_diversity(pareto_optimal)
        
        # Store frontier
        self.pareto_frontier = pareto_optimal
        
        logger.info(f"Found {len(pareto_optimal)} Pareto-optimal architectures "
                   f"(dominated {dominated_count} others)")
        
        return pareto_optimal
    
    def _dominates(self,
                  fitness1: MultiObjectiveFitness,
                  fitness2: MultiObjectiveFitness,
                  objectives: List[str]) -> bool:
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
    
    def _preserve_diversity(self, pareto_set: List[ArchitectureGene]) -> List[ArchitectureGene]:
        """
        Preserve diversity in Pareto set by removing similar solutions.
        """
        if len(pareto_set) <= 1:
            return pareto_set
        
        # Calculate similarity matrix
        n = len(pareto_set)
        similarity_matrix = np.zeros((n, n))
        
        for i in range(n):
            for j in range(i+1, n):
                similarity = self._calculate_similarity(
                    pareto_set[i].config,
                    pareto_set[j].config
                )
                similarity_matrix[i][j] = similarity
                similarity_matrix[j][i] = similarity
        
        # Remove solutions that are too similar
        to_remove = set()
        for i in range(n):
            for j in range(i+1, n):
                if similarity_matrix[i][j] > self.diversity_threshold:
                    # Keep the one with better fitness
                    if pareto_set[i].fitness.composite_score >= pareto_set[j].fitness.composite_score:
                        to_remove.add(j)
                    else:
                        to_remove.add(i)
        
        diverse_set = [gene for i, gene in enumerate(pareto_set) if i not in to_remove]
        
        return diverse_set
    
    def _calculate_similarity(self,
                             config1: ArchitectureConfig,
                             config2: ArchitectureConfig) -> float:
        """Calculate similarity between two architectures"""
        # Compare family
        family_similar = 1.0 if config1.family == config2.family else 0.0
        
        # Compare layers
        layer_diff = abs(config1.num_layers - config2.num_layers) / max(config1.num_layers, config2.num_layers)
        layer_similarity = 1.0 - layer_diff
        
        # Compare hidden dimension
        hidden_diff = abs(config1.hidden_dim - config2.hidden_dim) / max(config1.hidden_dim, config2.hidden_dim)
        hidden_similarity = 1.0 - hidden_diff
        
        # Weighted average
        return 0.3 * family_similar + 0.35 * layer_similarity + 0.35 * hidden_similarity
    
    def calculate_hypervolume(self) -> float:
        """
        Calculate hypervolume of the Pareto frontier.
        Higher is better (indicates better coverage of objective space).
        """
        if not self.pareto_frontier:
            return 0.0
        
        # Normalize objectives
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
        
        # Calculate hypervolume using inclusion-exclusion (simplified)
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
        """Get statistics about the Pareto frontier"""
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
        """Calculate diversity score of the frontier"""
        if len(self.pareto_frontier) <= 1:
            return 1.0
        
        # Calculate average similarity
        similarities = []
        for i in range(len(self.pareto_frontier)):
            for j in range(i+1, len(self.pareto_frontier)):
                sim = self._calculate_similarity(
                    self.pareto_frontier[i].config,
                    self.pareto_frontier[j].config
                )
                similarities.append(sim)
        
        avg_similarity = np.mean(similarities)
        diversity = 1.0 - avg_similarity  # Lower similarity = higher diversity
        
        return max(0, min(1, diversity))

# ============================================================================
# ENHANCED MODULE 4: Advanced Health Monitoring & Circuit Breaker
# ============================================================================

class HealthStatus(Enum):
    """Health status levels"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    CRITICAL = "critical"

@dataclass
class HealthMetrics:
    """Detailed health metrics"""
    status: HealthStatus
    score: float  # 0-100
    components: Dict[str, Dict[str, Any]]
    timestamp: datetime
    messages: List[str]

class EnhancedHealthMonitor:
    """
    Advanced health monitoring with:
    - Component-level health checks
    - Degradation detection
    - Recovery recommendations
    - Historical health tracking
    """
    
    def __init__(self,
                 check_interval: int = 60,
                 degradation_threshold: float = 0.7,
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
        
        # Component health thresholds
        self.component_thresholds = {
            'database': {'min_score': 0.8},
            'token_economy': {'min_score': 0.7},
            'population': {'min_score': 0.5},
            'evaluation_queue': {'min_score': 0.6},
            'circuit_breakers': {'min_score': 0.9}
        }
        
        logger.info("EnhancedHealthMonitor initialized")
    
    async def start(self):
        """Start background health checking"""
        self._running = True
        self._check_task = asyncio.create_task(self._health_check_loop())
        logger.info("Health monitoring started")
    
    async def _health_check_loop(self):
        """Background health check loop"""
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
        """
        Perform comprehensive health check.
        
        Args:
            components: Dictionary of component health check functions
        """
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
        
        # Calculate overall health
        if component_status:
            scores = [c['score'] for c in component_status.values() if 'score' in c]
            overall_score = np.mean(scores) if scores else 50.0
        else:
            overall_score = 100.0
        
        # Determine status
        if overall_score >= self.degradation_threshold * 100:
            status = HealthStatus.HEALTHY
        elif overall_score >= self.critical_threshold * 100:
            status = HealthStatus.DEGRADED
        else:
            status = HealthStatus.CRITICAL
        
        # Store health record
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
        """Get detailed health report"""
        async with self._lock:
            # Calculate trend
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
        """Calculate health trend from history"""
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
        """Clean shutdown"""
        self._running = False
        if self._check_task:
            self._check_task.cancel()
            try:
                await self._check_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Health monitoring shutdown complete")

# ============================================================================
# ENHANCED MODULE 5: Unified Database Manager with Migration Support
# ============================================================================

class UnifiedDatabaseManager:
    """
    Enhanced database manager with:
    - Async SQLite and PostgreSQL support
    - Schema migration
    - Connection pooling
    - Query optimization
    - Backup and restore
    """
    
    def __init__(self,
                 db_path: Path = Path("./carbon_nas_unified.db"),
                 pool_size: int = 10,
                 max_overflow: int = 20):
        self.db_path = db_path
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.engine = None
        self.SessionLocal = None
        self._migration_lock = asyncio.Lock()
        self._init_engine()
    
    def _init_engine(self):
        """Initialize database engine with connection pooling"""
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
        self._ensure_schema_version()
        logger.info(f"UnifiedDatabaseManager initialized at {self.db_path}")
    
    def _init_tables(self):
        """Initialize all database tables with enhanced schemas"""
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
        
        class EvolutionDB(Base):
            __tablename__ = 'evolution_history'
            
            id = Column(Integer, primary_key=True)
            generation = Column(Integer, index=True)
            population_size = Column(Integer)
            best_accuracy = Column(Float)
            best_carbon = Column(Float)
            best_composite = Column(Float)
            pareto_size = Column(Integer)
            carbon_spent = Column(Float)
            tokens_spent = Column(Float)
            registered_experts = Column(Integer)
            created_at = Column(DateTime, default=datetime.now)
            
            __table_args__ = (
                Index('idx_generation', 'generation'),
                Index('idx_best_accuracy', 'best_accuracy'),
                Index('idx_carbon_spent', 'carbon_spent'),
            )
        
        class HealthHistoryDB(Base):
            __tablename__ = 'health_history'
            
            id = Column(Integer, primary_key=True)
            status = Column(String(20))
            score = Column(Float)
            components_json = Column(JSON)
            messages = Column(JSON)
            created_at = Column(DateTime, default=datetime.now)
            
            __table_args__ = (
                Index('idx_created', 'created_at'),
                Index('idx_status', 'status'),
            )
        
        Base.metadata.create_all(self.engine)
        logger.info("Database tables initialized")
    
    def _ensure_schema_version(self):
        """Ensure schema version table exists and is up to date"""
        with self.get_session() as session:
            try:
                session.execute("SELECT 1 FROM schema_version")
            except:
                session.execute("CREATE TABLE schema_version (version INTEGER)")
                session.execute("INSERT INTO schema_version (version) VALUES (3)")
                session.commit()
    
    @contextmanager
    def get_session(self):
        """Get a database session with context manager"""
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
        """Save architecture with full metadata"""
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
        """Save evolution history"""
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
    
    async def save_knowledge_package(self, package: KnowledgePackage):
        """Save knowledge package"""
        with self.get_session() as session:
            from sqlalchemy import text
            
            session.execute(
                text("""INSERT OR REPLACE INTO knowledge_packages 
                       (package_id, config_json, survival_score, accuracy, carbon_kg,
                        domain_tags, source_generation, usage_count)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""),
                (
                    package.package_id,
                    json.dumps(package.architecture_config),
                    package.survival_score,
                    package.performance_metrics.get('accuracy', 0.0),
                    package.performance_metrics.get('carbon_kg', 0.0),
                    json.dumps(package.domain_tags),
                    package.source_generation,
                    package.usage_count
                )
            )
    
    async def save_health_record(self, health_metrics: HealthMetrics):
        """Save health check record"""
        with self.get_session() as session:
            from sqlalchemy import text
            
            # Prepare component data (simplified for storage)
            components_dict = {
                name: {
                    'score': comp.get('score', 0.0),
                    'healthy': comp.get('healthy', False)
                }
                for name, comp in health_metrics.components.items()
            }
            
            session.execute(
                text("""INSERT INTO health_history 
                       (status, score, components_json, messages)
                       VALUES (?, ?, ?, ?)"""),
                (
                    health_metrics.status.value,
                    health_metrics.score,
                    json.dumps(components_dict),
                    json.dumps(health_metrics.messages)
                )
            )
    
    async def get_best_architectures(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get best architectures from history"""
        with self.get_session() as session:
            result = session.execute(
                text("""SELECT * FROM architectures 
                       ORDER BY composite_score DESC LIMIT ?"""),
                (limit,)
            )
            
            rows = result.fetchall()
            return [dict(row) for row in rows]
    
    def dispose(self):
        """Dispose of database connections"""
        if self.engine:
            self.engine.dispose()
            if self.SessionLocal:
                self.SessionLocal.remove()
            logger.info("Database connections disposed")

# ============================================================================
# INTEGRATION: Enhanced Unified Carbon NAS
# ============================================================================

class EnhancedUnifiedCarbonNAS(UnifiedCarbonNAS):
    """
    Enhanced version of UnifiedCarbonNAS with all improvements integrated.
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
        
        # Initialize enhanced components
        self.token_economy = EnhancedTokenEconomy(
            total_budget=self.config.token_budget,
            renewable_rate=0.02,
            dynamic_pricing=True
        )
        
        self.health_monitor = EnhancedHealthMonitor(
            check_interval=self.config.health_check_interval_seconds
        )
        
        self.database = UnifiedDatabaseManager(
            db_path=self.config.database_path
        ) if self.config.enable_persistence else None
        
        self.pareto_optimizer = EnhancedParetoOptimizer()
        
        # Call parent initialization
        super().__init__(
            expert_registry=expert_registry,
            population_size=self.config.population_size,
            max_generations=self.config.max_generations,
            carbon_budget_kg=self.config.carbon_budget_kg,
            auto_register=True,
            enable_compression=self.config.enable_compression,
            enable_hardware_profiling=self.config.enable_hardware_profiling,
            enable_pareto=self.config.enable_pareto,
            **kwargs
        )
        
        # Override with enhanced components
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
        
        logger.info(f"EnhancedUnifiedCarbonNAS initialized with config version {self.config.version}")
    
    async def start(self):
        """Start all background services"""
        # Start token economy
        await self.token_economy.start()
        
        # Start health monitoring
        await self.health_monitor.start()
        
        # Start original background tasks
        if self.config.enable_continuous_learning:
            asyncio.create_task(self._enhanced_continuous_learning())
        
        logger.info("All enhanced services started")
    
    async def _enhanced_continuous_learning(self):
        """Enhanced continuous learning with token economy and health checks"""
        while True:
            try:
                await asyncio.sleep(self.config.health_check_interval_seconds)
                
                # Check health
                health = await self.health_monitor.perform_health_check({
                    'token_economy': self.token_economy.get_system_summary,
                    'population': lambda: {'score': len(self.population) * 2}
                })
                
                if health.score < 50:
                    logger.warning(f"System health degraded: {health.score:.1f}")
                    continue
                
                # Check token balance
                token_summary = self.token_economy.get_system_summary()
                if token_summary['current_balance'] < token_summary['total_budget'] * 0.1:
                    logger.warning("Token balance critically low")
                    continue
                
                # Run lightweight evolution
                if self.enable_continuous_learning:
                    await self._lightweight_evolution()
                    
            except Exception as e:
                logger.error(f"Enhanced continuous learning error: {e}")
                await asyncio.sleep(60)
    
    async def _lightweight_evolution(self):
        """Run lightweight evolution with reduced generations"""
        # Generate small population
        small_pop = [self._generate_random_config() for _ in range(5)]
        
        # Evaluate with token constraints
        for config in small_pop:
            # Reserve tokens for evaluation
            success, cost = await self.token_economy.reserve_tokens(
                TokenConsumer.ARCHITECTURE_EVAL,
                1.0,
                metadata={'config': config.to_dict()}
            )
            
            if not success:
                break
            
            # Evaluate architecture
            gene = ArchitectureGene(config=config)
            await self._evaluate_single_architecture(gene)
    
    async def _evaluate_single_architecture(self, gene: ArchitectureGene):
        """Evaluate a single architecture with enhanced features"""
        # Hardware profiling
        if self.enable_hardware_profiling:
            hw_profile = self.hardware_profiler.profile_on_hardware(
                gene.config
            )
            gene.fitness.latency_ms = hw_profile['latency_ms']
            gene.fitness.energy_kwh = hw_profile['energy_kwh']
            gene.fitness.carbon_kg = hw_profile['carbon_kg']
        
        # Calculate composite score
        gene.fitness.calculate_composite()
        
        # Track carbon
        self.total_carbon_spent_kg += gene.fitness.carbon_kg
        
        # Save to database
        if self.database:
            await self.database.save_architecture(gene)
    
    async def shutdown(self):
        """Graceful shutdown of all services"""
        logger.info("Shutting down EnhancedUnifiedCarbonNAS...")
        
        # Shutdown token economy
        await self.token_economy.shutdown()
        
        # Shutdown health monitor
        await self.health_monitor.shutdown()
        
        # Shutdown database
        if self.database:
            self.database.dispose()
        
        # Shutdown parent
        await super().shutdown()
        
        logger.info("Shutdown complete")


async def main():
    # Load configuration from YAML or use defaults
    config = UnifiedNASConfig(
        population_size=30,
        carbon_budget_kg=10.0,
        token_budget=1000.0,
        enable_continuous_learning=True,
        enable_pareto=True,
        enable_compression=True,
        enable_hardware_profiling=True
    )
    
    nas = EnhancedUnifiedCarbonNAS(config=config)
    await nas.start()
    
    # Run evolution
    result = await nas.evolve(
        fitness_function=your_fitness_function,
        generations=10
    )
    
    print(f"Evolution complete: {result['best_accuracy']:.2f}% accuracy, "
          f"{result['best_carbon_kg']:.4f}kg CO2")
    
    await nas.shutdown()
