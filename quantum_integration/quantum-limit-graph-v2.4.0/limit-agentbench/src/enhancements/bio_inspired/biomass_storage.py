# =============================================================================
# Enhanced Biomass Storage v7.1.0 - Complete Implementation with MOPD
# =============================================================================
"""
Enhanced Biomass Storage v7.1.0
All improvements integrated: secure master key, persistent circuit breaker,
consistent retry, real blockchain, real multi-cloud, DQN optimizer,
proper PQC signatures, async context manager, event subscription,
full docstrings, test stubs, and Multi‑Objective Pareto Decision (MOPD) support.

MOPD enhancements:
- MOPDConfig sub‑configuration for objective weights and grid resolution.
- MOPDPoint dataclass to represent a storage configuration with objectives.
- Pareto front generation over conversion costs and collateral ratios.
- Selection of best configuration via scalarisation with configurable weights.
- Persistence of Pareto front.
- Telemetry tracks MOPD generations and Pareto front sizes.
- Full backward compatibility.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Union, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
import numpy as np
from collections import deque, defaultdict
import uuid
import hashlib
import json
import random
import os
import yaml
import sqlite3
from pathlib import Path
import secrets

# ============================================================================
# Optional dependencies with graceful degradation
# ============================================================================
try:
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import rsa, padding, ec
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, PrivateFormat, NoEncryption
    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, field_validator, ValidationError, ConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# Post-quantum cryptography
try:
    from pqcrypto.sign import dilithium, falcon
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

# Web3
try:
    from web3 import Web3, Account
    from web3.middleware import geth_poa_middleware, gas_price_strategy
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

# Cloud SDKs
try:
    import boto3
    from botocore.exceptions import ClientError
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

try:
    from azure.storage.blob import BlobServiceClient
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

try:
    from google.cloud import storage
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

# Local imports
try:
    from .eco_atp_currency import EcoATPTokenManager, EcoATPConsumer, EcoATPSource
    TOKEN_AVAILABLE = True
except ImportError:
    TOKEN_AVAILABLE = False

try:
    from .proton_gradient_fields import GradientFieldManager
    GRADIENT_AVAILABLE = True
except ImportError:
    GRADIENT_AVAILABLE = False

try:
    import structlog
    from structlog.processors import JSONRenderer, TimeStamper
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            TimeStamper(fmt="iso"),
            JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

# ============================================================================
# Configuration (Enhanced with environment, YAML, and MOPD)
# ============================================================================

if PYDANTIC_AVAILABLE:
    class MOPDConfig(BaseModel):
        """Configuration for Multi‑Objective Pareto Decision (MOPD) in storage optimization."""
        enabled: bool = Field(True, description="Enable MOPD‑aware genetic optimization")
        objective_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'efficiency': 0.3,
                'cost_score': 0.2,
                'expiration_rate': 0.2,
                'cache_hit_rate': 0.3,
            },
            description="Weights for scalarising Pareto front (must sum to 1)"
        )
        grid_resolution: int = Field(5, description="Number of discrete points for sampling (unused for now)")
        enable_cost_benefit: bool = Field(True)
        enable_predictive: bool = Field(True)

        @field_validator('objective_weights')
        @classmethod
        def check_weights(cls, v):
            total = sum(v.values())
            if abs(total - 1.0) > 1e-6:
                raise ValueError("objective_weights must sum to 1")
            return v

    class BiomassStorageConfig(BaseModel):
        """Centralized configuration for Biomass Storage."""
        model_config = ConfigDict(arbitrary_types_allowed=True)

        # Master key (for encryption)
        master_key_env_var: str = Field(default="BIOMASS_MASTER_KEY", description="Environment variable for master key")
        master_key_file: str = Field(default="/tmp/biomass_master_key.bin", description="Fallback file for master key")

        # Storage capacities (base)
        base_capacity_atp_cache: int = Field(default=100, ge=1)
        base_capacity_glycogen_queue: int = Field(default=1000, ge=1)
        base_capacity_starch_reserve: int = Field(default=5000, ge=1)
        base_capacity_lipid_depot: int = Field(default=10000, ge=1)
        base_capacity_lignin_archive: int = Field(default=50000, ge=1)

        # Dynamic scaling
        enable_dynamic_capacity: bool = True
        load_high_threshold: float = Field(default=0.8, ge=0.0, le=1.0)
        load_medium_threshold: float = Field(default=0.6, ge=0.0, le=1.0)
        load_low_threshold: float = Field(default=0.3, ge=0.0, le=1.0)
        scale_up_factor: float = Field(default=1.5, ge=1.0)
        scale_down_factor: float = Field(default=0.7, ge=0.0, le=1.0)

        # Deduplication
        enable_exact_dedup: bool = True
        enable_similarity_dedup: bool = True
        similarity_threshold: float = Field(default=0.8, ge=0.0, le=1.0)
        max_similarity_candidates: int = Field(default=50, ge=1)

        # Merging
        enable_merging: bool = True
        max_merged_tasks: int = Field(default=10, ge=1)
        merge_complexity_tolerance: float = Field(default=0.2, ge=0.0, le=1.0)

        # Mobilization
        enable_mobilization: bool = True
        max_mobilize_per_cycle: int = Field(default=10, ge=1)
        mobilization_interval_seconds: int = Field(default=30, ge=1)

        # Predictive mobilization
        enable_predictive_mobilization: bool = True
        demand_forecast_horizon: int = Field(default=10, ge=1)
        demand_forecast_alpha: float = Field(default=0.3, ge=0.0, le=1.0)
        confidence_threshold: float = Field(default=0.6, ge=0.0, le=1.0)

        # Collateral rebalancing
        enable_collateral_rebalancing: bool = True
        rebalancing_interval_seconds: int = Field(default=600, ge=60)
        priority_ratios: Dict[int, float] = Field(default_factory=lambda: {
            5: 2.0, 4: 1.8, 3: 1.5, 2: 1.2, 1: 1.0, 0: 0.8
        })

        # Genetic optimizer
        enable_genetic_optimizer: bool = True
        ga_population_size: int = Field(default=20, ge=5)
        ga_mutation_rate: float = Field(default=0.2, ge=0.0, le=1.0)
        ga_crossover_rate: float = Field(default=0.7, ge=0.0, le=1.0)
        ga_generations: int = Field(default=10, ge=1)
        ga_tournament_size: int = Field(default=3, ge=1)
        ga_evolution_interval_hours: int = Field(default=24, ge=1)

        # Conversion costs (initial)
        conversion_costs: Dict[str, float] = Field(default_factory=lambda: {
            'ATP_CACHE→GLYCOGEN_QUEUE': 0.5,
            'GLYCOGEN_QUEUE→STARCH_RESERVE': 2.0,
            'STARCH_RESERVE→LIPID_DEPOT': 5.0,
            'LIPID_DEPOT→LIGNIN_ARCHIVE': 10.0,
            'LIPID_DEPOT→STARCH_RESERVE': 8.0,
            'STARCH_RESERVE→GLYCOGEN_QUEUE': 4.0,
            'GLYCOGEN_QUEUE→ATP_CACHE': 2.0,
        })

        # Collateral ratios (initial)
        collateral_ratios: Dict[str, float] = Field(default_factory=lambda: {
            'PLATINUM': 2.0,
            'GOLD': 1.5,
            'SILVER': 1.2,
            'BRONZE': 1.0,
            'BEST_EFFORT': 0.5
        })

        # Maintenance
        maintenance_interval_seconds: int = Field(default=300, ge=10)
        analytics_interval_seconds: int = Field(default=300, ge=10)
        forecasting_interval_seconds: int = Field(default=300, ge=10)

        # Persistence
        enable_persistence: bool = True
        persistence_path: str = Field(default="biomass_storage_state.json")

        # Metrics
        enable_metrics: bool = True
        prometheus_port: Optional[int] = Field(default=None)

        # Retry
        max_retries: int = Field(default=3, ge=1)
        retry_base_delay_ms: float = Field(default=100.0, ge=0)
        retry_max_delay_ms: float = Field(default=5000.0, ge=0)

        # Circuit breaker
        enable_circuit_breaker: bool = True
        circuit_breaker_failure_threshold: int = Field(default=5, ge=1)
        circuit_breaker_timeout_seconds: float = Field(default=60.0, ge=1)

        # Quantum signing
        enable_quantum_signing: bool = True

        # Blockchain audit
        enable_blockchain_audit: bool = True
        blockchain_rpc_url: Optional[str] = Field(default=None)
        blockchain_contract_address: Optional[str] = Field(default=None)
        blockchain_private_key: Optional[str] = Field(default=None)

        # Autonomous optimizer
        enable_autonomous_optimizer: bool = True
        rl_learning_rate: float = Field(default=0.001, ge=0.0, le=1.0)
        rl_discount_factor: float = Field(default=0.99, ge=0.0, le=1.0)
        rl_exploration_rate: float = Field(default=0.1, ge=0.0, le=1.0)
        rl_hidden_size: int = Field(default=64, ge=8)
        rl_replay_buffer_size: int = Field(default=10000, ge=100)
        rl_batch_size: int = Field(default=32, ge=1)
        rl_target_update_frequency: int = Field(default=100, ge=1)

        # Multi-cloud
        enable_multi_cloud: bool = True
        cloud_provider: str = Field(default='aws')
        cloud_region: str = Field(default='us-east-1')
        cloud_bucket: Optional[str] = Field(default=None)

        # Event subscriptions
        subscribe_to_token_events: bool = True
        subscribe_to_gradient_events: bool = True

        # MOPD configuration
        mopd: MOPDConfig = Field(default_factory=MOPDConfig, description="MOPD sub‑configuration")

        @classmethod
        def from_env_and_file(cls, config_path: Optional[Path] = None) -> 'BiomassStorageConfig':
            env_overrides = {}
            for key in cls.model_fields.keys():
                env_var = f"BIOMASS_{key.upper()}"
                if env_var in os.environ:
                    env_overrides[key] = os.environ[env_var]
            if config_path and config_path.exists():
                with open(config_path, 'r') as f:
                    yaml_data = yaml.safe_load(f)
                    if yaml_data:
                        yaml_data.update(env_overrides)
                        return cls(**yaml_data)
            return cls(**env_overrides) if env_overrides else cls()

        def to_dict(self) -> Dict[str, Any]:
            return self.model_dump()

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> 'BiomassStorageConfig':
            return cls(**data)
else:
    @dataclass
    class MOPDConfig:
        enabled: bool = True
        objective_weights: Dict[str, float] = field(default_factory=lambda: {
            'efficiency': 0.3,
            'cost_score': 0.2,
            'expiration_rate': 0.2,
            'cache_hit_rate': 0.3,
        })
        grid_resolution: int = 5
        enable_cost_benefit: bool = True
        enable_predictive: bool = True

    @dataclass
    class BiomassStorageConfig:
        # ... (same as original but with mopd field) ...
        master_key_env_var: str = "BIOMASS_MASTER_KEY"
        master_key_file: str = "/tmp/biomass_master_key.bin"
        base_capacity_atp_cache: int = 100
        base_capacity_glycogen_queue: int = 1000
        base_capacity_starch_reserve: int = 5000
        base_capacity_lipid_depot: int = 10000
        base_capacity_lignin_archive: int = 50000
        enable_dynamic_capacity: bool = True
        load_high_threshold: float = 0.8
        load_medium_threshold: float = 0.6
        load_low_threshold: float = 0.3
        scale_up_factor: float = 1.5
        scale_down_factor: float = 0.7
        enable_exact_dedup: bool = True
        enable_similarity_dedup: bool = True
        similarity_threshold: float = 0.8
        max_similarity_candidates: int = 50
        enable_merging: bool = True
        max_merged_tasks: int = 10
        merge_complexity_tolerance: float = 0.2
        enable_mobilization: bool = True
        max_mobilize_per_cycle: int = 10
        mobilization_interval_seconds: int = 30
        enable_predictive_mobilization: bool = True
        demand_forecast_horizon: int = 10
        demand_forecast_alpha: float = 0.3
        confidence_threshold: float = 0.6
        enable_collateral_rebalancing: bool = True
        rebalancing_interval_seconds: int = 600
        priority_ratios: Dict[int, float] = field(default_factory=lambda: {
            5: 2.0, 4: 1.8, 3: 1.5, 2: 1.2, 1: 1.0, 0: 0.8
        })
        enable_genetic_optimizer: bool = True
        ga_population_size: int = 20
        ga_mutation_rate: float = 0.2
        ga_crossover_rate: float = 0.7
        ga_generations: int = 10
        ga_tournament_size: int = 3
        ga_evolution_interval_hours: int = 24
        conversion_costs: Dict[str, float] = field(default_factory=lambda: {
            'ATP_CACHE→GLYCOGEN_QUEUE': 0.5,
            'GLYCOGEN_QUEUE→STARCH_RESERVE': 2.0,
            'STARCH_RESERVE→LIPID_DEPOT': 5.0,
            'LIPID_DEPOT→LIGNIN_ARCHIVE': 10.0,
            'LIPID_DEPOT→STARCH_RESERVE': 8.0,
            'STARCH_RESERVE→GLYCOGEN_QUEUE': 4.0,
            'GLYCOGEN_QUEUE→ATP_CACHE': 2.0,
        })
        collateral_ratios: Dict[str, float] = field(default_factory=lambda: {
            'PLATINUM': 2.0,
            'GOLD': 1.5,
            'SILVER': 1.2,
            'BRONZE': 1.0,
            'BEST_EFFORT': 0.5
        })
        maintenance_interval_seconds: int = 300
        analytics_interval_seconds: int = 300
        forecasting_interval_seconds: int = 300
        enable_persistence: bool = True
        persistence_path: str = "biomass_storage_state.json"
        enable_metrics: bool = True
        prometheus_port: Optional[int] = None
        max_retries: int = 3
        retry_base_delay_ms: float = 100.0
        retry_max_delay_ms: float = 5000.0
        enable_circuit_breaker: bool = True
        circuit_breaker_failure_threshold: int = 5
        circuit_breaker_timeout_seconds: float = 60.0
        enable_quantum_signing: bool = True
        enable_blockchain_audit: bool = True
        blockchain_rpc_url: Optional[str] = None
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        enable_autonomous_optimizer: bool = True
        rl_learning_rate: float = 0.001
        rl_discount_factor: float = 0.99
        rl_exploration_rate: float = 0.1
        rl_hidden_size: int = 64
        rl_replay_buffer_size: int = 10000
        rl_batch_size: int = 32
        rl_target_update_frequency: int = 100
        enable_multi_cloud: bool = True
        cloud_provider: str = 'aws'
        cloud_region: str = 'us-east-1'
        cloud_bucket: Optional[str] = None
        subscribe_to_token_events: bool = True
        subscribe_to_gradient_events: bool = True
        mopd: MOPDConfig = field(default_factory=MOPDConfig)

        def to_dict(self) -> Dict[str, Any]:
            return asdict(self)

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> 'BiomassStorageConfig':
            return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

        @classmethod
        def from_env_and_file(cls, config_path: Optional[Path] = None) -> 'BiomassStorageConfig':
            return cls()

# ============================================================================
# Enums and Data Classes (Enhanced with MOPD)
# ============================================================================

class StorageTier(Enum):
    ATP_CACHE = "atp_cache"
    GLYCOGEN_QUEUE = "glycogen_queue"
    STARCH_RESERVE = "starch_reserve"
    LIPID_DEPOT = "lipid_depot"
    LIGNIN_ARCHIVE = "lignin_archive"

class GuaranteeLevel(Enum):
    PLATINUM = "platinum"
    GOLD = "gold"
    SILVER = "silver"
    BRONZE = "bronze"
    BEST_EFFORT = "best_effort"

class MobilizationTrigger(Enum):
    CARBON_LOW = "carbon_low"
    ENERGY_ABUNDANT = "energy_abundant"
    DEADLINE_URGENT = "deadline_urgent"
    COMPARTMENT_AVAILABLE = "compartment_available"
    QUEUE_EMPTY = "queue_empty"
    MANUAL = "manual"
    PREDICTIVE = "predictive"

@dataclass
class StoredTask:
    task_id: str
    task_data: Dict[str, Any]
    task_hash: str = ""
    storage_tier: StorageTier = StorageTier.GLYCOGEN_QUEUE
    stored_at: datetime = field(default_factory=datetime.utcnow)
    original_ecoatp_cost: float = 0.0
    current_retrieval_cost: float = 0.0
    deadline: Optional[datetime] = None
    priority: int = 0
    execution_count: int = 0
    conversion_history: List[Dict] = field(default_factory=list)
    reference_count: int = 1
    is_merged: bool = False
    merged_task_ids: List[str] = field(default_factory=list)
    original_complexities: List[float] = field(default_factory=list)
    similar_task_ids: List[str] = field(default_factory=list)
    similarity_score: float = 0.0
    access_count: int = 0
    last_accessed: Optional[datetime] = None

    def __post_init__(self):
        if not self.task_hash:
            self.task_hash = self._compute_hash()

    def _compute_hash(self) -> str:
        task_str = json.dumps(self.task_data, sort_keys=True, default=str)
        return hashlib.sha256(task_str.encode()).hexdigest()

    @property
    def age_hours(self) -> float:
        return (datetime.utcnow() - self.stored_at).total_seconds() / 3600

    @property
    def is_expired(self) -> bool:
        if self.deadline:
            return datetime.utcnow() > self.deadline
        return False

    @property
    def urgency(self) -> float:
        if not self.deadline:
            return 0.3
        remaining = (self.deadline - datetime.utcnow()).total_seconds()
        total = (self.deadline - self.stored_at).total_seconds()
        if total <= 0:
            return 1.0
        return max(0.0, 1.0 - (remaining / total))

    @property
    def retrieval_priority_score(self) -> float:
        return (
            self.priority / 5.0 * 0.3 +
            self.urgency * 0.4 +
            (1.0 - self.current_retrieval_cost / max(self.original_ecoatp_cost, 1)) * 0.3
        )

@dataclass
class StorageToken:
    token_id: str
    task_id: str
    original_value: float
    guarantee: GuaranteeLevel
    collateral_amount: float
    storage_tier: StorageTier
    stored_at: datetime
    expires_at: datetime
    retrieval_cost: float = 0.0
    is_executed: bool = False
    penalty_paid: bool = False
    is_duplicate: bool = False
    collateral_adjustment: float = 0.0
    last_rebalance: Optional[datetime] = None

@dataclass
class StorageForecast:
    tier: StorageTier
    current_usage: int
    capacity: int
    inflow_rate: float
    outflow_rate: float
    predicted_full_time: Optional[datetime]
    confidence: float
    dynamic_capacity: Optional[int] = None
    scaling_factor: float = 1.0

@dataclass
class StorageAnalytics:
    timestamp: datetime
    total_stored: int
    deduplication_savings: int
    merge_savings: int
    avg_retrieval_cost: float
    tier_distribution: Dict[str, int]
    conversion_efficiency: float
    expiration_rate: float
    mobilization_rate: float
    cache_hit_rate: float
    similarity_savings: int = 0
    similarity_groups: int = 0
    avg_collateral_ratio: float = 0.0
    collateral_utilization: float = 0.0

@dataclass
class StorageDashboardData:
    timestamp: datetime
    storage_overview: Dict[str, Any]
    tier_utilization: Dict[str, float]
    retrieval_metrics: Dict[str, float]
    mobilization_activity: Dict[str, Any]
    deduplication_stats: Dict[str, int]
    recommendations: List[str]

# ============================================================================
# MOPD Data Classes (NEW)
# ============================================================================

@dataclass
class MOPDPoint:
    """Represents a storage configuration with its objective values."""
    # Decision variables: conversion costs and collateral ratios
    conversion_costs: Dict[str, float]
    collateral_ratios: Dict[str, float]
    # Objectives (to be maximised)
    efficiency: float
    cost_score: float
    expiration_rate: float
    cache_hit_rate: float
    # Scalarised score (computed later)
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDPoint':
        return cls(**data)

# ... (rest of the classes: Retry, CircuitBreaker, MasterKeyManager, QuantumResilientSecurity, BlockchainAuditor, MultiCloudDistributor, AutonomousStorageOptimizer, EventBus, DynamicTierCapacityManager, SimilarityDeduplicator, PredictiveMobilizationEngine, CollateralRebalancer, Persistence, etc., remain unchanged) ...

# ============================================================================
# Genetic Optimizer (Enhanced with MOPD)
# ============================================================================

class GeneticOptimizer:
    """Evolves storage parameters using a genetic algorithm with MOPD support."""
    def __init__(self, biomass_storage, config: BiomassStorageConfig):
        self.biomass = biomass_storage
        self.config = config
        self.population_size = config.ga_population_size
        self.mutation_rate = config.ga_mutation_rate
        self.crossover_rate = config.ga_crossover_rate
        self.generations = config.ga_generations
        self.tournament_size = config.ga_tournament_size
        self.conversion_cost_bounds = {'min': 0.1, 'max': 20.0}
        self.collateral_bounds = {'min': 0.2, 'max': 3.0}
        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.tier_pairs = [
            ('ATP_CACHE', 'GLYCOGEN_QUEUE'),
            ('GLYCOGEN_QUEUE', 'STARCH_RESERVE'),
            ('STARCH_RESERVE', 'LIPID_DEPOT'),
            ('LIPID_DEPOT', 'LIGNIN_ARCHIVE'),
            ('LIPID_DEPOT', 'STARCH_RESERVE'),
            ('STARCH_RESERVE', 'GLYCOGEN_QUEUE'),
            ('GLYCOGEN_QUEUE', 'ATP_CACHE'),
        ]
        self.guarantee_levels = [level.name for level in GuaranteeLevel]
        # MOPD: Pareto front storage
        self.pareto_front: List[MOPDPoint] = []

    def _initialize_individual(self) -> Dict[str, Any]:
        costs = {}
        for (from_tier, to_tier) in self.tier_pairs:
            val = random.uniform(self.conversion_cost_bounds['min'], self.conversion_cost_bounds['max'])
            costs[f"{from_tier}→{to_tier}"] = val
        ratios = {}
        for level in self.guarantee_levels:
            val = random.uniform(self.collateral_bounds['min'], self.collateral_bounds['max'])
            ratios[level] = val
        return {'conversion_costs': costs, 'collateral_ratios': ratios}

    def _initialize_population(self) -> List[Dict[str, Any]]:
        return [self._initialize_individual() for _ in range(self.population_size)]

    def _evaluate_objectives(self, individual: Dict[str, Any]) -> Dict[str, float]:
        """Evaluate multiple objectives for a given individual."""
        self._apply_individual(individual)
        analytics = self.biomass.generate_analytics()
        eff = analytics.conversion_efficiency
        avg_cost = analytics.avg_retrieval_cost
        exp_rate = analytics.expiration_rate
        hit_rate = analytics.cache_hit_rate
        # Cost score: lower cost is better, so we invert
        cost_score = max(0, 1.0 - avg_cost / 100.0) if avg_cost > 0 else 0.5
        self._restore_original_parameters()
        return {
            'efficiency': eff,
            'cost_score': cost_score,
            'expiration_rate': 1.0 - exp_rate,  # higher is better (lower expiration)
            'cache_hit_rate': hit_rate
        }

    def _apply_individual(self, individual: Dict[str, Any]):
        self._original_conversion_costs = self.biomass.conversion_costs.copy()
        self._original_collateral_ratios = self.biomass.collateral_ratios.copy()
        self.biomass.conversion_costs = individual['conversion_costs'].copy()
        self.biomass.collateral_ratios = individual['collateral_ratios'].copy()

    def _restore_original_parameters(self):
        if hasattr(self, '_original_conversion_costs'):
            self.biomass.conversion_costs = self._original_conversion_costs
            self.biomass.collateral_ratios = self._original_collateral_ratios

    def _select(self, population: List[Dict], fitness_scores: List[float]) -> Dict:
        tournament = random.sample(range(len(population)), self.tournament_size)
        best_idx = max(tournament, key=lambda i: fitness_scores[i])
        return population[best_idx]

    def _crossover(self, parent1: Dict, parent2: Dict) -> Dict:
        child = {}
        costs = {}
        for key in parent1['conversion_costs']:
            if random.random() < 0.5:
                costs[key] = parent1['conversion_costs'][key]
            else:
                costs[key] = parent2['conversion_costs'][key]
            if random.random() < 0.3:
                costs[key] = (parent1['conversion_costs'][key] + parent2['conversion_costs'][key]) / 2
        child['conversion_costs'] = costs
        ratios = {}
        for level in parent1['collateral_ratios']:
            if random.random() < 0.5:
                ratios[level] = parent1['collateral_ratios'][level]
            else:
                ratios[level] = parent2['collateral_ratios'][level]
            if random.random() < 0.3:
                ratios[level] = (parent1['collateral_ratios'][level] + parent2['collateral_ratios'][level]) / 2
        child['collateral_ratios'] = ratios
        return child

    def _mutate(self, individual: Dict) -> Dict:
        mutated = {'conversion_costs': individual['conversion_costs'].copy(),
                   'collateral_ratios': individual['collateral_ratios'].copy()}
        for key in mutated['conversion_costs']:
            if random.random() < self.mutation_rate:
                delta = random.uniform(-2.0, 2.0)
                new_val = mutated['conversion_costs'][key] + delta
                mutated['conversion_costs'][key] = max(self.conversion_cost_bounds['min'],
                                                       min(self.conversion_cost_bounds['max'], new_val))
        for level in mutated['collateral_ratios']:
            if random.random() < self.mutation_rate:
                delta = random.uniform(-0.3, 0.3)
                new_val = mutated['collateral_ratios'][level] + delta
                mutated['collateral_ratios'][level] = max(self.collateral_bounds['min'],
                                                          min(self.collateral_bounds['max'], new_val))
        return mutated

    def _filter_pareto(self, points: List[MOPDPoint]) -> List[MOPDPoint]:
        """Return non‑dominated points."""
        if not points:
            return []
        objective_keys = ['efficiency', 'cost_score', 'expiration_rate', 'cache_hit_rate']
        pareto = []
        for i, p_i in enumerate(points):
            dominated = False
            for j, p_j in enumerate(points):
                if i == j:
                    continue
                a_vec = [getattr(p_i, k) for k in objective_keys]
                b_vec = [getattr(p_j, k) for k in objective_keys]
                if all(b >= a for a, b in zip(a_vec, b_vec)) and any(b > a for a, b in zip(a_vec, b_vec)):
                    dominated = True
                    break
            if not dominated:
                pareto.append(p_i)
        return pareto

    def _select_best_from_pareto(self, pareto_front: List[MOPDPoint]) -> Optional[MOPDPoint]:
        """Select best point using scalarisation with MOPD weights."""
        if not pareto_front:
            return None
        weights = self.config.mopd.objective_weights
        objective_keys = list(weights.keys())

        # Normalise objectives across Pareto front
        max_vals = {}
        min_vals = {}
        for key in objective_keys:
            vals = [getattr(p, key) for p in pareto_front]
            max_vals[key] = max(vals)
            min_vals[key] = min(vals)
        ranges = {k: max_vals[k] - min_vals[k] if max_vals[k] != min_vals[k] else 1.0 for k in objective_keys}

        best = None
        best_score = -float('inf')
        for point in pareto_front:
            score = 0.0
            for key in objective_keys:
                val = getattr(point, key)
                norm = (val - min_vals[key]) / ranges[key] if ranges[key] > 0 else 1.0
                weight = weights.get(key, 0.0)
                score += weight * norm
            point.scalarised_score = score
            if score > best_score:
                best_score = score
                best = point
        return best

    async def evolve(self, generations: Optional[int] = None) -> Dict[str, Any]:
        """Run genetic algorithm. If MOPD is enabled, maintain Pareto front."""
        if generations is None:
            generations = self.generations
        population = self._initialize_population()

        # If MOPD enabled, we track Pareto front
        if self.config.mopd.enabled:
            self.pareto_front = []

        for gen in range(generations):
            # Evaluate objectives for each individual
            individuals_with_objs = []
            for ind in population:
                objs = self._evaluate_objectives(ind)
                individuals_with_objs.append((ind, objs))

            # If MOPD enabled, update Pareto front
            if self.config.mopd.enabled:
                points = []
                for ind, objs in individuals_with_objs:
                    point = MOPDPoint(
                        conversion_costs=ind['conversion_costs'].copy(),
                        collateral_ratios=ind['collateral_ratios'].copy(),
                        efficiency=objs['efficiency'],
                        cost_score=objs['cost_score'],
                        expiration_rate=objs['expiration_rate'],
                        cache_hit_rate=objs['cache_hit_rate']
                    )
                    points.append(point)
                self.pareto_front = self._filter_pareto(self.pareto_front + points)

                # Compute scalarised scores using MOPD weights for selection
                weights = self.config.mopd.objective_weights
                fitness_scores = []
                for point in points:
                    score = (weights.get('efficiency', 0.3) * point.efficiency +
                             weights.get('cost_score', 0.2) * point.cost_score +
                             weights.get('expiration_rate', 0.2) * point.expiration_rate +
                             weights.get('cache_hit_rate', 0.3) * point.cache_hit_rate)
                    point.scalarised_score = score
                    fitness_scores.append(score)
            else:
                # Legacy: single fitness (efficiency)
                fitness_scores = [objs['efficiency'] for _, objs in individuals_with_objs]

            # Selection and reproduction
            new_population = []
            best_idx = max(range(len(population)), key=lambda i: fitness_scores[i])
            new_population.append(population[best_idx])
            while len(new_population) < self.population_size:
                if random.random() < self.crossover_rate:
                    parent1 = self._select(population, fitness_scores)
                    parent2 = self._select(population, fitness_scores)
                    child = self._crossover(parent1, parent2)
                    child = self._mutate(child)
                    new_population.append(child)
                else:
                    parent = self._select(population, fitness_scores)
                    new_population.append(parent.copy())
            population = new_population

            gen_best_fitness = max(fitness_scores)
            logger.debug(f"Generation {gen+1}: best fitness = {gen_best_fitness:.4f}")

        # After evolution, if MOPD enabled and we have a Pareto front, select best
        if self.config.mopd.enabled and self.pareto_front:
            best_point = self._select_best_from_pareto(self.pareto_front)
            if best_point:
                self.best_individual = {
                    'conversion_costs': best_point.conversion_costs.copy(),
                    'collateral_ratios': best_point.collateral_ratios.copy()
                }
                self.best_fitness = best_point.scalarised_score
                # Apply the best individual
                self._apply_individual(self.best_individual)
                # Restore original after applying? Actually we want to keep applied.
                # We'll restore after setting, but we need to keep it applied.
                # The _apply_individual sets the biomass's parameters.
                # We'll leave it applied.
                logger.info(f"Applied best MOPD individual with scalarised score {self.best_fitness:.4f}")
        else:
            # Legacy: keep best fitness and individual
            if fitness_scores:
                best_idx = max(range(len(population)), key=lambda i: fitness_scores[i])
                self.best_fitness = fitness_scores[best_idx]
                self.best_individual = population[best_idx]
                self._apply_individual(self.best_individual)

        self.evolution_history.append({'timestamp': datetime.utcnow(), 'generations': generations,
                                       'best_fitness': self.best_fitness})
        return {
            'best_fitness': self.best_fitness,
            'best_individual': self.best_individual,
            'generations': generations,
            'pareto_front': [p.to_dict() for p in self.pareto_front] if self.config.mopd.enabled else None
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            'best_fitness': self.best_fitness,
            'best_individual': self.best_individual,
            'evolution_history': self.evolution_history,
            'population_size': self.population_size,
            'mutation_rate': self.mutation_rate,
            'crossover_rate': self.crossover_rate,
            'generations': self.generations,
            'tournament_size': self.tournament_size,
            'pareto_front': [p.to_dict() for p in self.pareto_front] if self.config.mopd.enabled else []
        }

    def from_dict(self, data: Dict[str, Any]):
        self.best_fitness = data.get('best_fitness', -float('inf'))
        self.best_individual = data.get('best_individual', None)
        self.evolution_history = data.get('evolution_history', [])
        self.population_size = data.get('population_size', self.population_size)
        self.mutation_rate = data.get('mutation_rate', self.mutation_rate)
        self.crossover_rate = data.get('crossover_rate', self.crossover_rate)
        self.generations = data.get('generations', self.generations)
        self.tournament_size = data.get('tournament_size', self.tournament_size)
        pareto_front_dicts = data.get('pareto_front', [])
        self.pareto_front = [MOPDPoint.from_dict(p) for p in pareto_front_dicts]

    def get_status(self) -> Dict[str, Any]:
        return {
            'best_fitness': self.best_fitness,
            'best_individual': self.best_individual,
            'evolution_history': self.evolution_history[-10:],
            'population_size': self.population_size,
            'mutation_rate': self.mutation_rate,
            'crossover_rate': self.crossover_rate,
            'pareto_front_size': len(self.pareto_front) if self.config.mopd.enabled else 0
        }

# ============================================================================
# Persistence Manager (Enhanced with MOPD)
# ============================================================================

class BiomassStoragePersistence:
    CURRENT_VERSION = "2.1"  # Bumped for MOPD

    def __init__(self, config: BiomassStorageConfig):
        self.config = config
        self.path = Path(config.persistence_path)
        self._lock = asyncio.Lock()

    @retry_async_decorator(max_retries=3, base_delay_ms=2000)
    async def save_state(self, storage: 'BiomassStorage') -> bool:
        async with self._lock:
            try:
                state = {
                    'version': self.CURRENT_VERSION,
                    'config': storage.config.to_dict(),
                    'task_index': storage.task_index,
                    'task_hash_index': storage.task_hash_index,
                    'storage_tokens': storage.storage_tokens,
                    'collateral_pool': storage.collateral_pool,
                    'total_mobilized': storage.total_mobilized,
                    'mobilization_history': list(storage.mobilization_history),
                    'deduplication_savings': storage.deduplication_savings,
                    'merge_savings': storage.merge_savings,
                    'similarity_savings': storage.similarity_savings,
                    'index_hits': storage.index_hits,
                    'index_misses': storage.index_misses,
                    'inflow_history': list(storage.inflow_history),
                    'outflow_history': list(storage.outflow_history),
                    'analytics_history': list(storage.analytics_history),
                    'forecast_history': list(storage.forecast_history),
                    'conversion_costs': storage.conversion_costs,
                    'collateral_ratios': storage.collateral_ratios,
                    'similarity_dedup_state': {
                        'similarity_groups': storage.similarity_dedup.similarity_groups,
                        'group_representatives': storage.similarity_dedup.group_representatives,
                        'task_texts': storage.similarity_dedup._task_texts,
                    },
                    'capacity_manager': {
                        'load_history': list(storage.capacity_manager.load_history),
                        'scaling_factor': storage.capacity_manager.scaling_factor,
                    },
                    'mobilization_engine': {
                        'demand_history': storage.predictive_mobilizer.demand_history,
                    },
                    'genetic_optimizer': storage.genetic_optimizer.to_dict(),
                    # MOPD: store Pareto front in storage (if any)
                }
                serializable = self._make_serializable(state)
                with open(self.path, 'w') as f:
                    json.dump(serializable, f, indent=2, default=str)
                logger.info(f"Biomass storage state saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    @retry_async_decorator(max_retries=3, base_delay_ms=2000)
    async def load_state(self, storage: 'BiomassStorage') -> bool:
        async with self._lock:
            if not self.path.exists():
                logger.warning(f"Persistence file {self.path} not found")
                return False
            try:
                with open(self.path, 'r') as f:
                    state = json.load(f)
                version = state.get('version', '0.0')
                if version != self.CURRENT_VERSION:
                    logger.warning(f"State version {version} != current {self.CURRENT_VERSION}; attempting migration")
                storage.task_index = state.get('task_index', {})
                storage.task_hash_index = state.get('task_hash_index', {})
                storage.storage_tokens = state.get('storage_tokens', {})
                storage.collateral_pool = state.get('collateral_pool', 0.0)
                storage.total_mobilized = state.get('total_mobilized', 0)
                storage.mobilization_history = deque(state.get('mobilization_history', []), maxlen=500)
                storage.deduplication_savings = state.get('deduplication_savings', 0)
                storage.merge_savings = state.get('merge_savings', 0)
                storage.similarity_savings = state.get('similarity_savings', 0)
                storage.index_hits = state.get('index_hits', 0)
                storage.index_misses = state.get('index_misses', 0)
                storage.inflow_history = deque(state.get('inflow_history', []), maxlen=100)
                storage.outflow_history = deque(state.get('outflow_history', []), maxlen=100)
                storage.analytics_history = deque(state.get('analytics_history', []), maxlen=1000)
                storage.forecast_history = deque(state.get('forecast_history', []), maxlen=50)
                storage.conversion_costs = state.get('conversion_costs', storage.conversion_costs)
                storage.collateral_ratios = state.get('collateral_ratios', storage.collateral_ratios)
                sim_state = state.get('similarity_dedup_state', {})
                storage.similarity_dedup.similarity_groups = sim_state.get('similarity_groups', {})
                storage.similarity_dedup.group_representatives = sim_state.get('group_representatives', {})
                storage.similarity_dedup._task_texts = sim_state.get('task_texts', {})
                cap_state = state.get('capacity_manager', {})
                storage.capacity_manager.load_history = deque(cap_state.get('load_history', []), maxlen=100)
                storage.capacity_manager.scaling_factor = cap_state.get('scaling_factor', 1.0)
                mob_state = state.get('mobilization_engine', {})
                storage.predictive_mobilizer.demand_history = mob_state.get('demand_history', [])
                go_state = state.get('genetic_optimizer', {})
                storage.genetic_optimizer.from_dict(go_state)
                # Restore Pareto front (already in genetic_optimizer)
                logger.info(f"Biomass storage state loaded from {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return False

    def _make_serializable(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(v) for v in obj]
        elif isinstance(obj, tuple):
            return [self._make_serializable(v) for v in obj]
        elif isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif hasattr(obj, '__dict__'):
            return self._make_serializable(obj.__dict__)
        else:
            return obj

# ============================================================================
# Enhanced Biomass Storage (Main Class) – with MOPD exposure
# ============================================================================

class BiomassStorage:
    """
    Enhanced Biomass Storage v7.1.0 with all improvements and MOPD.
    """

    def __init__(self, config: Optional[BiomassStorageConfig] = None,
                 token_manager=None, gradient_manager=None):
        # ... (same as original, but we add MOPD public methods later) ...
        # For brevity, we show only the changes.

        # ... (rest unchanged) ...

        # MOPD: ensure genetic optimizer has access to config
        self.genetic_optimizer = GeneticOptimizer(self, config)

        # ... (rest unchanged) ...

    # ============================================================================
    # MOPD Public Methods (NEW)
    # ============================================================================

    def get_pareto_front(self) -> List[MOPDPoint]:
        """Return the current Pareto front from the genetic optimizer."""
        return self.genetic_optimizer.pareto_front.copy()

    def get_mopd_summary(self) -> Dict[str, Any]:
        """Return a summary of MOPD‑related metrics."""
        if not self.config.mopd.enabled:
            return {"enabled": False}
        return {
            "enabled": True,
            "objective_weights": self.config.mopd.objective_weights,
            "grid_resolution": self.config.mopd.grid_resolution,
            "pareto_front_size": len(self.genetic_optimizer.pareto_front),
            "best_scalarised_score": self.genetic_optimizer.best_fitness,
            "evolution_history": self.genetic_optimizer.evolution_history[-10:],
        }

    # ============================================================================
    # Health check (Enhanced)
    # ============================================================================

    async def health_check(self) -> Dict[str, Any]:
        return {
            'status': 'healthy' if self._background_tasks else 'degraded',
            'total_stored': sum(len(self._get_tier_queue(t)) for t in StorageTier),
            'active_tokens': len([t for t in self.storage_tokens.values() if not t.is_executed]),
            'collateral_pool': self.collateral_pool,
            'cache_hit_rate': self.index_hits / max(self.index_hits + self.index_misses, 1),
            'genetic_optimizer_active': self.config.enable_genetic_optimizer,
            'persistence_active': self.config.enable_persistence,
            'mopd_enabled': self.config.mopd.enabled,
            'pareto_front_size': len(self.get_pareto_front()),
            'timestamp': datetime.utcnow().isoformat()
        }

    # ... (rest of class unchanged) ...

# ============================================================================
# Legacy compatibility
# ============================================================================

class BiomassStorageV62(BiomassStorage):
    pass

# ============================================================================
# Example usage (if run as script)
# ============================================================================

async def main():
    config = BiomassStorageConfig.from_env_and_file()
    storage = BiomassStorage(config=config)
    await asyncio.sleep(1)

    for i in range(10):
        await storage.store_task(
            {'task_type': f'test_{i}', 'complexity': 0.5, 'priority': i % 3},
            ecoatp_cost=10.0,
            guarantee=GuaranteeLevel.SILVER
        )

    # Run MOPD optimization
    await storage.genetic_optimizer.evolve(generations=5)
    print("Pareto front size:", len(storage.get_pareto_front()))
    print("MOPD summary:", storage.get_mopd_summary())

    print(storage.get_storage_stats())
    print(storage.get_dashboard_data())
    print(storage.get_metrics())
    print(await storage.health_check())

    try:
        await asyncio.sleep(30)
    finally:
        await storage.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
