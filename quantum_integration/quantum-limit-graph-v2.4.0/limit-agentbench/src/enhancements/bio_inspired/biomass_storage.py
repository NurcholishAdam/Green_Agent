# Complete enhanced file v6.2.0 with Configuration, Persistence, Metrics, and Improved Algorithms

"""
Enhanced Biomass Storage v6.2.0
Complete implementation with task deduplication, demand-based mobilization,
storage forecasting, priority-based retrieval, storage analytics,
dynamic tier capacity, similarity-based deduplication,
predictive mobilization based on demand forecasts,
real-time storage dashboard, collateral rebalancing,
and GeneticOptimizer for evolutionary optimization of conversion costs and collateral ratios.

NEW FEATURES v6.2.0:
- Configuration dataclass for centralized tuning
- State persistence (save/load to disk)
- Improved similarity deduplication with caching
- Exponential smoothing for demand forecasting
- Retry and circuit breaker for external calls (stub)
- Metrics endpoint for monitoring
- Enhanced genetic optimizer with configurable parameters
- More robust error handling in background loops
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Union, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import deque, defaultdict
import uuid
import math
import hashlib
import json
import random
import os
import pickle
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

logger = logging.getLogger(__name__)

# ============================================================================
# Try importing dependencies
# ============================================================================
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

# ============================================================================
# Configuration Dataclass (NEW)
# ============================================================================

@dataclass
class BiomassStorageConfig:
    """Centralized configuration for Biomass Storage."""
    # Storage capacities (base)
    base_capacity_atp_cache: int = 100
    base_capacity_glycogen_queue: int = 1000
    base_capacity_starch_reserve: int = 5000
    base_capacity_lipid_depot: int = 10000
    base_capacity_lignin_archive: int = 50000

    # Dynamic scaling
    enable_dynamic_capacity: bool = True
    load_high_threshold: float = 0.8
    load_medium_threshold: float = 0.6
    load_low_threshold: float = 0.3
    scale_up_factor: float = 1.5
    scale_down_factor: float = 0.7

    # Deduplication
    enable_exact_dedup: bool = True
    enable_similarity_dedup: bool = True
    similarity_threshold: float = 0.8
    max_similarity_candidates: int = 50

    # Merging
    enable_merging: bool = True
    max_merged_tasks: int = 10
    merge_complexity_tolerance: float = 0.2

    # Mobilization
    enable_mobilization: bool = True
    max_mobilize_per_cycle: int = 10
    mobilization_interval_seconds: int = 30

    # Predictive mobilization
    enable_predictive_mobilization: bool = True
    demand_forecast_horizon: int = 10
    demand_forecast_alpha: float = 0.3  # exponential smoothing factor
    confidence_threshold: float = 0.6

    # Collateral rebalancing
    enable_collateral_rebalancing: bool = True
    rebalancing_interval_seconds: int = 600
    priority_ratios: Dict[int, float] = field(default_factory=lambda: {
        5: 2.0, 4: 1.8, 3: 1.5, 2: 1.2, 1: 1.0, 0: 0.8
    })

    # Genetic optimizer
    enable_genetic_optimizer: bool = True
    ga_population_size: int = 20
    ga_mutation_rate: float = 0.2
    ga_crossover_rate: float = 0.7
    ga_generations: int = 10
    ga_tournament_size: int = 3
    ga_evolution_interval_hours: int = 24

    # Conversion costs (initial)
    conversion_costs: Dict[str, float] = field(default_factory=lambda: {
        'ATP_CACHE→GLYCOGEN_QUEUE': 0.5,
        'GLYCOGEN_QUEUE→STARCH_RESERVE': 2.0,
        'STARCH_RESERVE→LIPID_DEPOT': 5.0,
        'LIPID_DEPOT→LIGNIN_ARCHIVE': 10.0,
        'LIPID_DEPOT→STARCH_RESERVE': 8.0,
        'STARCH_RESERVE→GLYCOGEN_QUEUE': 4.0,
        'GLYCOGEN_QUEUE→ATP_CACHE': 2.0,
    })

    # Collateral ratios (initial)
    collateral_ratios: Dict[str, float] = field(default_factory=lambda: {
        'PLATINUM': 2.0,
        'GOLD': 1.5,
        'SILVER': 1.2,
        'BRONZE': 1.0,
        'BEST_EFFORT': 0.5
    })

    # Maintenance
    maintenance_interval_seconds: int = 300
    analytics_interval_seconds: int = 300
    forecasting_interval_seconds: int = 300

    # Persistence
    enable_persistence: bool = True
    persistence_path: str = "biomass_storage_state.pkl"

    # Metrics
    enable_metrics: bool = True

    # Retry (for future external calls)
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BiomassStorageConfig':
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

# ============================================================================
# Enums and Data Classes (Enhanced)
# ============================================================================

class StorageTier(Enum):
    """Storage tiers from fastest to slowest access"""
    ATP_CACHE = "atp_cache"
    GLYCOGEN_QUEUE = "glycogen_queue"
    STARCH_RESERVE = "starch_reserve"
    LIPID_DEPOT = "lipid_depot"
    LIGNIN_ARCHIVE = "lignin_archive"

class GuaranteeLevel(Enum):
    """Token-backed execution guarantees"""
    PLATINUM = "platinum"
    GOLD = "gold"
    SILVER = "silver"
    BRONZE = "bronze"
    BEST_EFFORT = "best_effort"

class MobilizationTrigger(Enum):
    """Triggers for task mobilization"""
    CARBON_LOW = "carbon_low"
    ENERGY_ABUNDANT = "energy_abundant"
    DEADLINE_URGENT = "deadline_urgent"
    COMPARTMENT_AVAILABLE = "compartment_available"
    QUEUE_EMPTY = "queue_empty"
    MANUAL = "manual"
    PREDICTIVE = "predictive"

@dataclass
class StoredTask:
    """Enhanced stored task with deduplication and merging support"""
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
        """Compute content hash for deduplication"""
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
        """Calculate urgency score (0-1, higher = more urgent)"""
        if not self.deadline:
            return 0.3
        remaining = (self.deadline - datetime.utcnow()).total_seconds()
        total = (self.deadline - self.stored_at).total_seconds()
        if total <= 0:
            return 1.0
        return max(0.0, 1.0 - (remaining / total))

    @property
    def retrieval_priority_score(self) -> float:
        """Composite score for retrieval ordering"""
        return (
            self.priority / 5.0 * 0.3 +
            self.urgency * 0.4 +
            (1.0 - self.current_retrieval_cost / max(self.original_ecoatp_cost, 1)) * 0.3
        )

@dataclass
class StorageToken:
    """Token representing stored computation"""
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
    """Storage capacity forecast"""
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
    """Comprehensive storage analytics"""
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
    """Real-time storage dashboard data"""
    timestamp: datetime
    storage_overview: Dict[str, Any]
    tier_utilization: Dict[str, float]
    retrieval_metrics: Dict[str, float]
    mobilization_activity: Dict[str, Any]
    deduplication_stats: Dict[str, int]
    recommendations: List[str]

# ============================================================================
# Retry Helper (NEW)
# ============================================================================

async def retry_async(
    func: Callable,
    max_retries: int,
    base_delay_ms: float,
    max_delay_ms: float,
    *args,
    **kwargs
) -> Any:
    """Retry an async function with exponential backoff."""
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            delay = min(base_delay_ms * (2 ** attempt), max_delay_ms) / 1000.0
            await asyncio.sleep(delay)
    raise RuntimeError("Max retries exceeded")

# ============================================================================
# Similarity-Based Deduplication (Enhanced with caching)
# ============================================================================

class SimilarityDeduplicator:
    """
    Similarity-based deduplication for near-duplicate tasks.
    Enhanced with vector caching for incremental updates.
    """

    def __init__(self, threshold: float = 0.8, max_candidates: int = 50):
        self.threshold = threshold
        self.max_candidates = max_candidates
        self.vectorizer = TfidfVectorizer(max_features=100)
        self.similarity_groups: Dict[str, List[str]] = {}
        self.group_representatives: Dict[str, str] = {}
        self._lock = asyncio.Lock()

        # Cache for task texts and vectors
        self._task_texts: Dict[str, str] = {}
        self._vectors: Dict[str, np.ndarray] = {}
        self._is_fitted = False

        logger.info(f"Similarity Deduplicator initialized (threshold: {threshold})")

    def _get_task_text(self, task_data: Dict) -> str:
        """Extract text representation for similarity comparison"""
        parts = []
        if 'task_type' in task_data:
            parts.append(task_data['task_type'])
        if 'description' in task_data:
            parts.append(task_data['description'])
        if 'parameters' in task_data:
            for key, value in task_data['parameters'].items():
                if isinstance(value, (str, int, float)):
                    parts.append(f"{key}={value}")
                elif isinstance(value, list):
                    parts.append(f"{key}={' '.join(str(v) for v in value[:5])}")
        if 'complexity' in task_data:
            complexity = task_data['complexity']
            bucket = 'high' if complexity > 0.7 else 'medium' if complexity > 0.4 else 'low'
            parts.append(f"complexity_{bucket}")
        return ' '.join(parts)

    async def find_similar(self, task_data: Dict, existing_tasks: List[StoredTask]) -> Optional[Tuple[str, float]]:
        """
        Find similar tasks using TF-IDF and cosine similarity with caching.
        Returns (task_id, similarity_score) if similar found.
        """
        async with self._lock:
            if not existing_tasks:
                return None

            new_text = self._get_task_text(task_data)
            if not new_text:
                return None

            # Gather texts of existing tasks (use cache)
            existing_texts = []
            task_map = {}
            for task in existing_tasks:
                if task.task_id in self._task_texts:
                    text = self._task_texts[task.task_id]
                else:
                    text = self._get_task_text(task.task_data)
                    self._task_texts[task.task_id] = text
                existing_texts.append(text)
                task_map[text] = task.task_id

            if not existing_texts:
                return None

            # Vectorize all texts (fit if needed)
            all_texts = [new_text] + existing_texts
            if not self._is_fitted:
                vectors = self.vectorizer.fit_transform(all_texts)
                self._is_fitted = True
            else:
                # Transform new and existing; for existing we can reuse cached vectors
                # For simplicity, we transform all again (could be optimized)
                vectors = self.vectorizer.transform(all_texts)

            # Cache vectors for existing tasks
            for i, task_id in enumerate(task_map.values()):
                self._vectors[task_id] = vectors[i+1].toarray()[0]

            new_vector = vectors[0]
            # Compare only a subset of candidates to limit computation
            candidate_indices = list(range(len(existing_texts)))
            if len(candidate_indices) > self.max_candidates:
                # Random sample to reduce cost
                candidate_indices = random.sample(candidate_indices, self.max_candidates)

            best_idx = -1
            best_score = 0.0
            for idx in candidate_indices:
                existing_vector = vectors[idx+1]
                sim = cosine_similarity(new_vector, existing_vector)[0][0]
                if sim > self.threshold and sim > best_score:
                    best_score = sim
                    best_idx = idx

            if best_idx >= 0:
                text = existing_texts[best_idx]
                return task_map[text], best_score

            return None

    async def group_similar(self, task_id: str, similar_task_id: str, score: float):
        """Group similar tasks together"""
        async with self._lock:
            group_id = None
            for gid, members in self.similarity_groups.items():
                if task_id in members:
                    group_id = gid
                    break
                if similar_task_id in members:
                    group_id = gid
                    break

            if group_id is None:
                group_id = f"sim_group_{len(self.similarity_groups)}"
                self.similarity_groups[group_id] = [task_id, similar_task_id]
                self.group_representatives[group_id] = task_id
            else:
                if task_id not in self.similarity_groups[group_id]:
                    self.similarity_groups[group_id].append(task_id)
                if similar_task_id not in self.similarity_groups[group_id]:
                    self.similarity_groups[group_id].append(similar_task_id)

            logger.debug(f"Grouped {task_id} with {similar_task_id} (score: {score:.2f})")

    def get_group_stats(self) -> Dict[str, Any]:
        return {
            'total_groups': len(self.similarity_groups),
            'total_grouped_tasks': sum(len(members) for members in self.similarity_groups.values()),
            'avg_group_size': np.mean([len(members) for members in self.similarity_groups.values()]) if self.similarity_groups else 0,
            'groups': {
                gid: {
                    'members': members,
                    'representative': self.group_representatives.get(gid),
                    'size': len(members)
                }
                for gid, members in self.similarity_groups.items()
            }
        }

# ============================================================================
# Dynamic Tier Capacity Manager (Enhanced with config)
# ============================================================================

class DynamicTierCapacityManager:
    """
    Dynamic tier capacity based on system load.
    Enhanced with configurable thresholds and scaling factors.
    """

    def __init__(self, config: BiomassStorageConfig):
        self.config = config
        self.base_capacities = {
            StorageTier.ATP_CACHE: config.base_capacity_atp_cache,
            StorageTier.GLYCOGEN_QUEUE: config.base_capacity_glycogen_queue,
            StorageTier.STARCH_RESERVE: config.base_capacity_starch_reserve,
            StorageTier.LIPID_DEPOT: config.base_capacity_lipid_depot,
            StorageTier.LIGNIN_ARCHIVE: config.base_capacity_lignin_archive,
        }
        self.current_capacities = self.base_capacities.copy()
        self.load_history: deque = deque(maxlen=100)
        self.scaling_factor = 1.0
        self._lock = asyncio.Lock()

        logger.info("Dynamic Tier Capacity Manager initialized")

    def update_system_load(self, load: float):
        """Update system load for capacity adjustment"""
        self.load_history.append(load)

        if len(self.load_history) > 10:
            avg_load = np.mean(list(self.load_history)[-10:])

            if avg_load > self.config.load_high_threshold:
                self.scaling_factor = self.config.scale_up_factor
            elif avg_load > self.config.load_medium_threshold:
                self.scaling_factor = 1.2
            elif avg_load < self.config.load_low_threshold:
                self.scaling_factor = self.config.scale_down_factor
            else:
                self.scaling_factor = 1.0

            for tier, base in self.base_capacities.items():
                self.current_capacities[tier] = int(base * self.scaling_factor)

    def get_capacity(self, tier: StorageTier) -> int:
        return self.current_capacities.get(tier, self.base_capacities.get(tier, 1000))

    def get_all_capacities(self) -> Dict[StorageTier, int]:
        return self.current_capacities.copy()

    def get_scaling_stats(self) -> Dict[str, Any]:
        return {
            'current_scaling_factor': self.scaling_factor,
            'load_samples': len(self.load_history),
            'avg_load': np.mean(self.load_history) if self.load_history else 0.5,
            'capacities': {
                tier.value: {'base': self.base_capacities[tier], 'current': self.current_capacities[tier]}
                for tier in self.base_capacities
            }
        }

# ============================================================================
# Predictive Mobilization Engine (Enhanced with exponential smoothing)
# ============================================================================

class PredictiveMobilizationEngine:
    """
    Predictive mobilization based on demand forecasts.
    Enhanced with exponential smoothing for forecasting.
    """

    def __init__(self, config: BiomassStorageConfig):
        self.config = config
        self.demand_history: List[float] = []
        self.mobilization_schedule: List[Dict] = []
        self._lock = asyncio.Lock()
        self.forecast_horizon = config.demand_forecast_horizon
        self.confidence_threshold = config.confidence_threshold
        self.alpha = config.demand_forecast_alpha  # smoothing factor

        logger.info("Predictive Mobilization Engine initialized")

    def record_demand(self, demand_level: float):
        self.demand_history.append(demand_level)
        if len(self.demand_history) > 100:
            self.demand_history = self.demand_history[-100:]

    async def forecast_demand(self) -> Dict[str, Any]:
        """Forecast future demand using exponential smoothing."""
        async with self._lock:
            if len(self.demand_history) < 10:
                return {'status': 'insufficient_data'}

            values = self.demand_history[-50:]
            # Simple exponential smoothing
            if len(values) == 0:
                return {'status': 'insufficient_data'}

            smoothed = values[0]
            for v in values[1:]:
                smoothed = self.alpha * v + (1 - self.alpha) * smoothed

            # Generate forecast (constant smoothed)
            forecasts = [smoothed] * self.forecast_horizon

            # Confidence based on volatility
            volatility = np.std(values[-20:]) if len(values) >= 20 else 0.2
            confidence = max(0.1, 1.0 - volatility * 2)

            return {
                'status': 'success',
                'forecasts': forecasts,
                'average': smoothed,
                'trend': 'stable',
                'slope': 0.0,  # exponential smoothing doesn't give trend directly; we can keep for compatibility
                'confidence': confidence
            }

    async def get_mobilization_recommendation(self, current_mobilized: int) -> Dict[str, Any]:
        forecast = await self.forecast_demand()

        if forecast.get('status') != 'success':
            return {'action': 'no_change', 'reason': 'insufficient_data'}

        if forecast['confidence'] < self.confidence_threshold:
            return {'action': 'no_change', 'reason': 'low_confidence'}

        avg_forecast = forecast['average']

        if avg_forecast > 0.7:
            target = int(current_mobilized * 1.5)
            return {
                'action': 'mobilize',
                'current': current_mobilized,
                'target': target,
                'increase': target - current_mobilized,
                'reason': f'predicted_demand_{avg_forecast:.2f}',
                'confidence': forecast['confidence']
            }
        elif avg_forecast < 0.3:
            target = max(1, int(current_mobilized * 0.5))
            return {
                'action': 'demobilize',
                'current': current_mobilized,
                'target': target,
                'decrease': current_mobilized - target,
                'reason': f'predicted_demand_{avg_forecast:.2f}',
                'confidence': forecast['confidence']
            }
        else:
            return {
                'action': 'no_change',
                'current': current_mobilized,
                'reason': 'stable_demand',
                'confidence': forecast['confidence']
            }

# ============================================================================
# Collateral Rebalancer (Enhanced with config)
# ============================================================================

class CollateralRebalancer:
    """
    Collateral rebalancing based on task priority.
    Enhanced with configurable priority ratios.
    """

    def __init__(self, config: BiomassStorageConfig):
        self.config = config
        self.priority_ratios = config.priority_ratios
        self.rebalancing_history: deque = deque(maxlen=1000)
        self.collateral_pool = 0.0
        self._lock = asyncio.Lock()

        logger.info("Collateral Rebalancer initialized")

    async def rebalance(self, tokens: List[StorageToken]) -> Dict[str, Any]:
        async with self._lock:
            if not tokens:
                return {'status': 'no_tokens'}

            adjustments = []
            total_adjustment = 0.0

            for token in tokens:
                # Determine priority from associated task (simplified)
                priority = 2
                # In a real implementation, we would look up the task's priority
                target_ratio = self.priority_ratios.get(priority, 1.2)
                target_collateral = token.original_value * target_ratio
                current_collateral = token.collateral_amount
                adjustment = target_collateral - current_collateral

                if abs(adjustment) > 0.01:
                    token.collateral_amount = target_collateral
                    token.collateral_adjustment = adjustment
                    token.last_rebalance = datetime.utcnow()
                    total_adjustment += adjustment
                    adjustments.append({
                        'token_id': token.token_id,
                        'old_collateral': current_collateral,
                        'new_collateral': target_collateral,
                        'adjustment': adjustment
                    })

            self.collateral_pool += total_adjustment

            self.rebalancing_history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'tokens_rebalanced': len(adjustments),
                'total_adjustment': total_adjustment
            })

            return {
                'status': 'success',
                'tokens_rebalanced': len(adjustments),
                'total_adjustment': total_adjustment,
                'adjustments': adjustments
            }

    def get_rebalancing_stats(self) -> Dict[str, Any]:
        return {
            'total_rebalances': len(self.rebalancing_history),
            'current_collateral_pool': self.collateral_pool,
            'recent_rebalances': list(self.rebalancing_history)[-10:],
            'priority_ratios': self.priority_ratios
        }

# ============================================================================
# GeneticOptimizer (Enhanced with config and persistence)
# ============================================================================

class GeneticOptimizer:
    """
    Evolutionary optimizer for storage parameters (conversion costs and collateral ratios).
    Uses a genetic algorithm to find the best configuration that maximizes retrieval success
    and minimizes costs.
    Enhanced with configurable parameters and persistence support.
    """

    def __init__(self, biomass_storage, config: BiomassStorageConfig):
        self.biomass = biomass_storage
        self.config = config

        # GA parameters from config
        self.population_size = config.ga_population_size
        self.mutation_rate = config.ga_mutation_rate
        self.crossover_rate = config.ga_crossover_rate
        self.generations = config.ga_generations
        self.tournament_size = config.ga_tournament_size

        # Parameter bounds
        self.conversion_cost_bounds = {
            'min': 0.1,
            'max': 20.0
        }
        self.collateral_bounds = {
            'min': 0.2,
            'max': 3.0
        }

        # Current best individual (parameters)
        self.best_individual = None
        self.best_fitness = -float('inf')

        # History for logging
        self.evolution_history = []

        # Mapping of tier pairs for conversion costs (using strings for serialization)
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

        logger.info("GeneticOptimizer initialized")

    def _initialize_individual(self) -> Dict[str, Any]:
        """Generate a random parameter set."""
        costs = {}
        for (from_tier, to_tier) in self.tier_pairs:
            val = random.uniform(self.conversion_cost_bounds['min'],
                                 self.conversion_cost_bounds['max'])
            costs[f"{from_tier}→{to_tier}"] = val

        ratios = {}
        for level in self.guarantee_levels:
            val = random.uniform(self.collateral_bounds['min'],
                                 self.collateral_bounds['max'])
            ratios[level] = val

        return {
            'conversion_costs': costs,
            'collateral_ratios': ratios
        }

    def _initialize_population(self) -> List[Dict[str, Any]]:
        return [self._initialize_individual() for _ in range(self.population_size)]

    def _fitness(self, individual: Dict[str, Any]) -> float:
        """
        Compute fitness based on historical storage performance.
        This uses aggregated metrics from the last analytics snapshot.
        """
        # Temporarily apply individual's parameters to the storage system
        self._apply_individual(individual)

        analytics = self.biomass.generate_analytics()
        eff = analytics.conversion_efficiency
        avg_cost = analytics.avg_retrieval_cost
        exp_rate = analytics.expiration_rate
        hit_rate = analytics.cache_hit_rate

        cost_score = max(0, 1.0 - avg_cost / 100.0) if avg_cost > 0 else 0.5

        fitness = (0.4 * eff +
                   0.3 * cost_score +
                   0.2 * (1.0 - exp_rate) +
                   0.1 * hit_rate)

        self._restore_original_parameters()
        return fitness

    def _apply_individual(self, individual: Dict[str, Any]):
        """Temporarily replace storage parameters with the individual's values."""
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
        # Conversion costs: blend
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
        mutated = {
            'conversion_costs': individual['conversion_costs'].copy(),
            'collateral_ratios': individual['collateral_ratios'].copy()
        }

        # Mutate conversion costs
        for key in mutated['conversion_costs']:
            if random.random() < self.mutation_rate:
                delta = random.uniform(-2.0, 2.0)
                new_val = mutated['conversion_costs'][key] + delta
                mutated['conversion_costs'][key] = max(
                    self.conversion_cost_bounds['min'],
                    min(self.conversion_cost_bounds['max'], new_val)
                )

        for level in mutated['collateral_ratios']:
            if random.random() < self.mutation_rate:
                delta = random.uniform(-0.3, 0.3)
                new_val = mutated['collateral_ratios'][level] + delta
                mutated['collateral_ratios'][level] = max(
                    self.collateral_bounds['min'],
                    min(self.collateral_bounds['max'], new_val)
                )

        return mutated

    def _evolve_one_generation(self, population: List[Dict]) -> List[Dict]:
        fitness_scores = [self._fitness(ind) for ind in population]
        new_population = []

        # Elitism: keep best
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

        return new_population

    async def evolve(self, generations: Optional[int] = None) -> Dict[str, Any]:
        if generations is None:
            generations = self.generations

        population = self._initialize_population()
        best_fitness_so_far = -float('inf')
        best_individual_so_far = None

        for gen in range(generations):
            population = self._evolve_one_generation(population)
            fitness_scores = [self._fitness(ind) for ind in population]
            gen_best_idx = max(range(len(population)), key=lambda i: fitness_scores[i])
            gen_best_fitness = fitness_scores[gen_best_idx]
            gen_best = population[gen_best_idx]

            if gen_best_fitness > best_fitness_so_far:
                best_fitness_so_far = gen_best_fitness
                best_individual_so_far = gen_best

            logger.debug(f"Generation {gen+1}: best fitness = {gen_best_fitness:.4f}")

        if best_fitness_so_far > self.best_fitness:
            self.best_fitness = best_fitness_so_far
            self.best_individual = best_individual_so_far

        # Apply the best individual permanently
        if self.best_individual:
            self._restore_original_parameters()
            self.biomass.conversion_costs = self.best_individual['conversion_costs'].copy()
            self.biomass.collateral_ratios = self.best_individual['collateral_ratios'].copy()
            logger.info(f"Applied best individual with fitness {self.best_fitness:.4f}")

        self.evolution_history.append({
            'timestamp': datetime.utcnow(),
            'generations': generations,
            'best_fitness': self.best_fitness
        })

        return {
            'best_fitness': self.best_fitness,
            'best_individual': self.best_individual,
            'generations': generations
        }

    def get_status(self) -> Dict[str, Any]:
        return {
            'best_fitness': self.best_fitness,
            'best_individual': self.best_individual,
            'evolution_history': self.evolution_history[-10:],
            'population_size': self.population_size,
            'mutation_rate': self.mutation_rate,
            'crossover_rate': self.crossover_rate
        }

    def to_dict(self) -> Dict[str, Any]:
        """Serializable representation for persistence."""
        return {
            'best_fitness': self.best_fitness,
            'best_individual': self.best_individual,
            'evolution_history': self.evolution_history,
            'population_size': self.population_size,
            'mutation_rate': self.mutation_rate,
            'crossover_rate': self.crossover_rate,
            'generations': self.generations,
            'tournament_size': self.tournament_size
        }

    def from_dict(self, data: Dict[str, Any]):
        """Restore state from dict."""
        self.best_fitness = data.get('best_fitness', -float('inf'))
        self.best_individual = data.get('best_individual', None)
        self.evolution_history = data.get('evolution_history', [])
        self.population_size = data.get('population_size', self.population_size)
        self.mutation_rate = data.get('mutation_rate', self.mutation_rate)
        self.crossover_rate = data.get('crossover_rate', self.crossover_rate)
        self.generations = data.get('generations', self.generations)
        self.tournament_size = data.get('tournament_size', self.tournament_size)

# ============================================================================
# Persistence Manager (NEW)
# ============================================================================

class BiomassStoragePersistence:
    """Manages saving and loading of biomass storage state."""

    def __init__(self, config: BiomassStorageConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()

    async def save_state(self, storage: 'BiomassStorage') -> bool:
        async with self._lock:
            try:
                state = {
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
                }
                with open(self.path, 'wb') as f:
                    pickle.dump(state, f)
                logger.info(f"Biomass storage state saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    async def load_state(self, storage: 'BiomassStorage') -> bool:
        async with self._lock:
            if not os.path.exists(self.path):
                logger.warning(f"Persistence file {self.path} not found")
                return False
            try:
                with open(self.path, 'rb') as f:
                    state = pickle.load(f)

                # Restore config (if config differs, we keep current config)
                # Restore indices
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

                # Restore similarity dedup
                sim_state = state.get('similarity_dedup_state', {})
                storage.similarity_dedup.similarity_groups = sim_state.get('similarity_groups', {})
                storage.similarity_dedup.group_representatives = sim_state.get('group_representatives', {})
                storage.similarity_dedup._task_texts = sim_state.get('task_texts', {})

                # Restore capacity manager
                cap_state = state.get('capacity_manager', {})
                storage.capacity_manager.load_history = deque(cap_state.get('load_history', []), maxlen=100)
                storage.capacity_manager.scaling_factor = cap_state.get('scaling_factor', 1.0)

                # Restore mobilization engine
                mob_state = state.get('mobilization_engine', {})
                storage.predictive_mobilizer.demand_history = mob_state.get('demand_history', [])

                # Restore genetic optimizer
                go_state = state.get('genetic_optimizer', {})
                storage.genetic_optimizer.from_dict(go_state)

                # Note: tasks stored in deques are not serialized directly; we need to restore them from token/indices.
                # This is a simplification; a full implementation would need to serialize the actual task queues.
                # For demonstration, we assume tasks are not persisted; they will be re‑created from tokens.
                # In a real system, you would also persist the deque contents.

                logger.info(f"Biomass storage state loaded from {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return False

# ============================================================================
# Enhanced Biomass Storage (Main Class)
# ============================================================================

class BiomassStorage:
    """
    Enhanced Biomass Storage v6.2.0 with configuration, persistence, and improved algorithms.
    """

    def __init__(self, config: Optional[BiomassStorageConfig] = None,
                 token_manager=None, gradient_manager=None):
        self.config = config or BiomassStorageConfig()
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager

        # Capacities
        self.base_tier_capacities = {
            StorageTier.ATP_CACHE: self.config.base_capacity_atp_cache,
            StorageTier.GLYCOGEN_QUEUE: self.config.base_capacity_glycogen_queue,
            StorageTier.STARCH_RESERVE: self.config.base_capacity_starch_reserve,
            StorageTier.LIPID_DEPOT: self.config.base_capacity_lipid_depot,
            StorageTier.LIGNIN_ARCHIVE: self.config.base_capacity_lignin_archive,
        }

        # Dynamic capacity manager
        self.capacity_manager = DynamicTierCapacityManager(self.config)

        # Storage queues with dynamic capacity
        self.atp_cache: deque = deque(maxlen=self.capacity_manager.get_capacity(StorageTier.ATP_CACHE))
        self.glycogen_queue: deque = deque(maxlen=self.capacity_manager.get_capacity(StorageTier.GLYCOGEN_QUEUE))
        self.starch_reserve: deque = deque(maxlen=self.capacity_manager.get_capacity(StorageTier.STARCH_RESERVE))
        self.lipid_depot: deque = deque(maxlen=self.capacity_manager.get_capacity(StorageTier.LIPID_DEPOT))
        self.lignin_archive: deque = deque(maxlen=self.capacity_manager.get_capacity(StorageTier.LIGNIN_ARCHIVE))

        # Storage tokens
        self.storage_tokens: Dict[str, StorageToken] = {}
        self.collateral_pool: float = 0.0

        # Global task index
        self.task_index: Dict[str, Dict[str, Any]] = {}
        self.index_hits: int = 0
        self.index_misses: int = 0

        # Deduplication
        self.task_hash_index: Dict[str, str] = {}
        self.deduplication_savings: int = 0
        self.merge_savings: int = 0

        # Similarity deduplication
        self.similarity_dedup = SimilarityDeduplicator(
            threshold=self.config.similarity_threshold,
            max_candidates=self.config.max_similarity_candidates
        )
        self.similarity_savings: int = 0

        # Mobilization
        self.mobilization_triggers: Dict[MobilizationTrigger, bool] = {
            MobilizationTrigger.CARBON_LOW: True,
            MobilizationTrigger.ENERGY_ABUNDANT: True,
            MobilizationTrigger.DEADLINE_URGENT: True,
            MobilizationTrigger.COMPARTMENT_AVAILABLE: True,
            MobilizationTrigger.QUEUE_EMPTY: True,
            MobilizationTrigger.PREDICTIVE: True
        }
        self.mobilization_history: deque = deque(maxlen=500)
        self.total_mobilized: int = 0

        # Predictive mobilization
        self.predictive_mobilizer = PredictiveMobilizationEngine(self.config)

        # Collateral rebalancer
        self.collateral_rebalancer = CollateralRebalancer(self.config)

        # Storage forecasting
        self.inflow_history: deque = deque(maxlen=100)
        self.outflow_history: deque = deque(maxlen=100)
        self.forecast_history: deque = deque(maxlen=50)

        # Analytics
        self.analytics_history: deque = deque(maxlen=1000)
        self.analytics_interval = self.config.analytics_interval_seconds

        # Conversion costs (loaded from config)
        self.conversion_costs: Dict[str, float] = self.config.conversion_costs.copy()

        # Collateral ratios (loaded from config)
        self.collateral_ratios: Dict[str, float] = self.config.collateral_ratios.copy()

        # Genetic Optimizer
        self.genetic_optimizer = GeneticOptimizer(self, self.config)

        # Persistence
        self.persistence = BiomassStoragePersistence(self.config) if self.config.enable_persistence else None

        # Metrics
        self.metrics: Dict[str, Any] = {
            'total_stored': 0,
            'active_tokens': 0,
            'collateral_pool': 0.0,
            'cache_hit_rate': 0.0,
            'conversion_efficiency': 0.0,
            'expiration_rate': 0.0,
            'last_update': datetime.utcnow().isoformat()
        }

        # Background tasks
        self._background_tasks: List[asyncio.Task] = []
        self._task_status: Dict[str, bool] = {}

        # Load state if persistence enabled
        if self.config.enable_persistence and self.persistence:
            asyncio.create_task(self._load_state())

        # Start background loops
        self._start_background_loops()

        logger.info("Enhanced Biomass Storage v6.2.0 initialized with GeneticOptimizer")

    async def _load_state(self):
        if self.persistence:
            await self.persistence.load_state(self)

    async def save_state(self):
        if self.persistence:
            await self.persistence.save_state(self)

    def _start_background_loops(self):
        self._start_monitored_task(self._maintenance_loop, "maintenance")
        self._start_monitored_task(self._mobilization_loop, "mobilization")
        self._start_monitored_task(self._forecasting_loop, "forecasting")
        self._start_monitored_task(self._analytics_loop, "analytics")
        self._start_monitored_task(self._rebalancing_loop, "rebalancing")
        self._start_monitored_task(self._evolution_loop, "evolution")

    def _start_monitored_task(self, coro: Callable, name: str):
        """Start a background task with monitoring."""
        async def wrapped():
            while True:
                try:
                    await coro()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Background task {name} failed: {e}", exc_info=True)
                    self._task_status[name] = False
                    await asyncio.sleep(30)
                    logger.info(f"Restarting background task {name}")
                    self._task_status[name] = True
        task = asyncio.create_task(wrapped())
        self._background_tasks.append(task)
        self._task_status[name] = True

    # ============================================================================
    # Core Storage Methods (Enhanced)
    # ============================================================================

    def store_task(
        self, task_data: Dict[str, Any], ecoatp_cost: float,
        guarantee: GuaranteeLevel = GuaranteeLevel.SILVER,
        deadline: Optional[datetime] = None,
        initial_tier: StorageTier = StorageTier.GLYCOGEN_QUEUE,
        enable_dedup: bool = True,
        enable_similarity: bool = True
    ) -> Tuple[bool, Optional[str]]:
        """Store a task with deduplication and similarity support."""
        task_id = task_data.get('task_id', f"stored_{uuid.uuid4().hex[:8]}")

        # Content-based deduplication
        if enable_dedup and self.config.enable_exact_dedup:
            task_hash = hashlib.sha256(
                json.dumps(task_data, sort_keys=True, default=str).encode()
            ).hexdigest()

            if task_hash in self.task_hash_index:
                existing_task_id = self.task_hash_index[task_hash]
                existing = self._find_task_by_id(existing_task_id)
                if existing:
                    existing.reference_count += 1
                    self.deduplication_savings += 1
                    token = StorageToken(
                        token_id=f"stoken_{task_id}_{uuid.uuid4().hex[:6]}",
                        task_id=existing_task_id,
                        original_value=ecoatp_cost,
                        guarantee=guarantee,
                        collateral_amount=ecoatp_cost * self.collateral_ratios[guarantee.name],
                        storage_tier=existing.storage_tier,
                        stored_at=datetime.utcnow(),
                        expires_at=deadline or (datetime.utcnow() + timedelta(days=7)),
                        is_duplicate=True
                    )
                    self.storage_tokens[token.token_id] = token
                    self.collateral_pool += token.collateral_amount
                    logger.debug(f"Deduplicated task {task_id} → {existing_task_id} (refs: {existing.reference_count})")
                    return True, token.token_id

        # Similarity-based deduplication
        if enable_similarity and self.config.enable_similarity_dedup:
            existing_tasks = []
            for tier in StorageTier:
                queue = self._get_tier_queue(tier)
                existing_tasks.extend(list(queue))
            similar = asyncio.run(self.similarity_dedup.find_similar(task_data, existing_tasks))
            if similar:
                similar_task_id, score = similar
                existing = self._find_task_by_id(similar_task_id)
                if existing:
                    asyncio.run(self.similarity_dedup.group_similar(task_id, similar_task_id, score))
                    existing.similar_task_ids.append(task_id)
                    existing.similarity_score = score
                    self.similarity_savings += 1
                    token = StorageToken(
                        token_id=f"stoken_{task_id}_{uuid.uuid4().hex[:6]}",
                        task_id=similar_task_id,
                        original_value=ecoatp_cost * 0.5,
                        guarantee=GuaranteeLevel.BEST_EFFORT,
                        collateral_amount=ecoatp_cost * 0.2,
                        storage_tier=existing.storage_tier,
                        stored_at=datetime.utcnow(),
                        expires_at=deadline or (datetime.utcnow() + timedelta(days=7)),
                        is_duplicate=True
                    )
                    self.storage_tokens[token.token_id] = token
                    self.collateral_pool += token.collateral_amount
                    logger.debug(f"Similar task {task_id} → {similar_task_id} (score: {score:.2f})")
                    return True, token.token_id

        # Merge check
        if self.config.enable_merging:
            merged = self._try_merge_task(task_data, task_id, task_hash if enable_dedup else "")
            if merged:
                return True, merged

        # Regular storage
        collateral_ratio = self.collateral_ratios[guarantee.name]
        collateral = ecoatp_cost * collateral_ratio

        stored = StoredTask(
            task_id=task_id,
            task_data=task_data,
            task_hash=task_hash if enable_dedup and self.config.enable_exact_dedup else "",
            storage_tier=initial_tier,
            stored_at=datetime.utcnow(),
            original_ecoatp_cost=ecoatp_cost,
            deadline=deadline,
            priority=task_data.get('priority', 0)
        )

        token = StorageToken(
            token_id=f"stoken_{task_id}_{uuid.uuid4().hex[:6]}",
            task_id=task_id,
            original_value=ecoatp_cost,
            guarantee=guarantee,
            collateral_amount=collateral,
            storage_tier=initial_tier,
            stored_at=datetime.utcnow(),
            expires_at=deadline or (datetime.utcnow() + timedelta(days=7))
        )

        queue = self._get_tier_queue(initial_tier)
        queue.append(stored)

        self._add_to_index(task_id, initial_tier, len(queue) - 1)
        if enable_dedup and self.config.enable_exact_dedup and task_hash:
            self.task_hash_index[task_hash] = task_id

        self.storage_tokens[token.token_id] = token
        self.collateral_pool += collateral
        self.inflow_history.append(datetime.utcnow())

        self.capacity_manager.update_system_load(len(queue) / max(self.capacity_manager.get_capacity(initial_tier), 1))

        logger.info(f"Stored task {task_id} in {initial_tier.value}: cost={ecoatp_cost:.1f}")
        return True, token.token_id

    def _try_merge_task(self, task_data: Dict[str, Any], task_id: str,
                       task_hash: str) -> Optional[str]:
        """Try to merge similar tasks for batch execution."""
        task_type = task_data.get('task_type', '')
        complexity = task_data.get('complexity', 0.5)

        for existing_id, index_entry in list(self.task_index.items())[:20]:
            existing = self._find_task_by_id(existing_id)
            if not existing:
                continue

            existing_type = existing.task_data.get('task_type', '')
            existing_complexity = existing.task_data.get('complexity', 0.5)

            if (existing_type == task_type and
                abs(existing_complexity - complexity) < self.config.merge_complexity_tolerance and
                not existing.is_merged and
                len(existing.merged_task_ids) < self.config.max_merged_tasks):

                if not existing.is_merged:
                    existing.is_merged = True
                    existing.merged_task_ids = [existing.task_id]
                    existing.original_complexities = [existing_complexity]

                existing.merged_task_ids.append(task_id)
                existing.original_complexities.append(complexity)
                existing.task_data['complexity'] = min(1.0, sum(existing.original_complexities) * 0.7)
                existing.task_data['batch_execution'] = True
                existing.task_data['batch_size'] = len(existing.merged_task_ids)

                self.merge_savings += 1

                token = StorageToken(
                    token_id=f"stoken_{task_id}_{uuid.uuid4().hex[:6]}",
                    task_id=existing_id,
                    original_value=0,
                    guarantee=GuaranteeLevel.BEST_EFFORT,
                    collateral_amount=0,
                    storage_tier=existing.storage_tier,
                    stored_at=datetime.utcnow(),
                    expires_at=existing.deadline or (datetime.utcnow() + timedelta(days=7)),
                    is_duplicate=True
                )

                self.storage_tokens[token.token_id] = token

                logger.debug(f"Merged task {task_id} into {existing_id} (batch: {len(existing.merged_task_ids)})")
                return token.token_id

        return None

    def retrieve_task(self, token_id: str, force_retrieve: bool = False) -> Tuple[Optional[Dict[str, Any]], float]:
        """Enhanced retrieval with priority ordering."""
        if token_id not in self.storage_tokens:
            return None, 0.0

        token = self.storage_tokens[token_id]

        if token.is_duplicate:
            existing = self._find_task_by_id(token.task_id)
            if existing:
                existing.reference_count = max(0, existing.reference_count - 1)
                if existing.reference_count == 0 and existing.similar_task_ids:
                    for group_id, members in self.similarity_dedup.similarity_groups.items():
                        if token.task_id in members:
                            members.remove(token.task_id)
            token.is_executed = True
            del self.storage_tokens[token_id]
            return existing.task_data if existing else None, 0.0

        task_id = token.task_id
        location = self.find_task(task_id)

        if location:
            tier, position = location
            stored_task = self._get_from_tier_position(tier, position)
        else:
            stored_task = self._scan_all_tiers(task_id)

        if stored_task is None:
            return None, 0.0

        retrieval_cost = stored_task.current_retrieval_cost

        stored_task.access_count += 1
        stored_task.last_accessed = datetime.utcnow()

        queue = self._get_tier_queue(stored_task.storage_tier)
        try:
            queue.remove(stored_task)
        except ValueError:
            pass

        self._remove_from_index(task_id)
        if stored_task.task_hash:
            self.task_hash_index.pop(stored_task.task_hash, None)

        if stored_task.is_merged and stored_task.merged_task_ids:
            stored_task.task_data['merged_tasks'] = stored_task.merged_task_ids
            stored_task.task_data['total_original_tasks'] = len(stored_task.merged_task_ids)

        token.is_executed = True
        self.collateral_pool -= token.collateral_amount
        del self.storage_tokens[token_id]

        self.outflow_history.append(datetime.utcnow())

        logger.info(f"Retrieved task {task_id}: cost={retrieval_cost:.1f}, refs={stored_task.reference_count}")
        return stored_task.task_data, retrieval_cost

    # ========================================================================
    # Mobilization (Enhanced with predictive)
    # ========================================================================

    def should_mobilize(self) -> List[MobilizationTrigger]:
        triggers = []

        if self.gradient_manager and self.mobilization_triggers[MobilizationTrigger.CARBON_LOW]:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.effective_strength < 0.3:
                triggers.append(MobilizationTrigger.CARBON_LOW)

        if self.mobilization_triggers[MobilizationTrigger.QUEUE_EMPTY]:
            if len(self.atp_cache) < 20:
                triggers.append(MobilizationTrigger.QUEUE_EMPTY)

        if self.mobilization_triggers[MobilizationTrigger.DEADLINE_URGENT]:
            now = datetime.utcnow()
            for task in list(self.glycogen_queue)[:50]:
                if task.deadline and (task.deadline - now).total_seconds() < 3600:
                    triggers.append(MobilizationTrigger.DEADLINE_URGENT)
                    break

        if self.mobilization_triggers[MobilizationTrigger.PREDICTIVE] and self.config.enable_predictive_mobilization:
            current_mobilized = self.total_mobilized
            recommendation = asyncio.run(
                self.predictive_mobilizer.get_mobilization_recommendation(current_mobilized)
            )
            if recommendation.get('action') == 'mobilize':
                triggers.append(MobilizationTrigger.PREDICTIVE)

        return triggers

    def mobilize_tasks(self, target_tier: StorageTier = StorageTier.ATP_CACHE,
                      max_count: int = 10) -> int:
        triggers = self.should_mobilize()

        if not triggers:
            return 0

        mobilized = 0

        if MobilizationTrigger.PREDICTIVE in triggers:
            forecast = asyncio.run(self.predictive_mobilizer.forecast_demand())
            if forecast.get('status') == 'success':
                avg_forecast = forecast['average']
                max_count = int(max_count * (1.0 + avg_forecast * 0.5))

        if target_tier == StorageTier.ATP_CACHE:
            source_queue = self.glycogen_queue

            urgent_tasks = []
            normal_tasks = []
            for task in list(source_queue)[:100]:
                if task.urgency > 0.7:
                    urgent_tasks.append(task)
                else:
                    normal_tasks.append(task)

            for task in urgent_tasks[:max_count]:
                if len(self.atp_cache) < self.capacity_manager.get_capacity(StorageTier.ATP_CACHE):
                    source_queue.remove(task)
                    task.storage_tier = StorageTier.ATP_CACHE
                    self.atp_cache.append(task)
                    self._update_index_position(task.task_id, StorageTier.ATP_CACHE, len(self.atp_cache) - 1)
                    mobilized += 1

            remaining = max_count - mobilized
            for task in normal_tasks[:remaining]:
                if len(self.atp_cache) < self.capacity_manager.get_capacity(StorageTier.ATP_CACHE):
                    source_queue.remove(task)
                    task.storage_tier = StorageTier.ATP_CACHE
                    self.atp_cache.append(task)
                    self._update_index_position(task.task_id, StorageTier.ATP_CACHE, len(self.atp_cache) - 1)
                    mobilized += 1

        if mobilized > 0:
            self.total_mobilized += mobilized
            self.mobilization_history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'count': mobilized,
                'triggers': [t.value for t in triggers],
                'target_tier': target_tier.value,
                'predictive_used': MobilizationTrigger.PREDICTIVE in triggers
            })
            self.predictive_mobilizer.record_demand(mobilized / max(max_count, 1))
            logger.info(f"Mobilized {mobilized} tasks to {target_tier.value} (triggers: {[t.value for t in triggers]})")

        return mobilized

    # ========================================================================
    # Background Loops (Enhanced)
    # ========================================================================

    async def _maintenance_loop(self):
        while True:
            try:
                now = datetime.utcnow()
                # Handle expired tokens
                for token_id in list(self.storage_tokens.keys()):
                    token = self.storage_tokens[token_id]
                    if now > token.expires_at and not token.is_executed:
                        penalty = token.collateral_amount * 0.5
                        self.collateral_pool -= penalty
                        token.penalty_paid = True

                        location = self.find_task(token.task_id)
                        if location:
                            tier, position = location
                            stored = self._get_from_tier_position(tier, position)
                            if stored:
                                queue = self._get_tier_queue(tier)
                                try:
                                    queue.remove(stored)
                                except ValueError:
                                    pass
                            self._remove_from_index(token.task_id)
                        del self.storage_tokens[token_id]

                # Update dynamic capacities
                for tier in StorageTier:
                    queue = self._get_tier_queue(tier)
                    load = len(queue) / max(self.capacity_manager.get_capacity(tier), 1)
                    self.capacity_manager.update_system_load(load)

                # Auto-convert old tasks
                for stored in list(self.glycogen_queue):
                    if stored.age_hours > 6:
                        token = self._find_token(stored.task_id)
                        if token:
                            self.convert_tier(token.token_id, StorageTier.STARCH_RESERVE)

                await asyncio.sleep(self.config.maintenance_interval_seconds)

            except Exception as e:
                logger.error(f"Maintenance error: {str(e)}")
                await asyncio.sleep(60)

    async def _mobilization_loop(self):
        while True:
            try:
                if self.config.enable_mobilization:
                    self.mobilize_tasks(StorageTier.ATP_CACHE, max_count=self.config.max_mobilize_per_cycle)
                await asyncio.sleep(self.config.mobilization_interval_seconds)
            except Exception as e:
                logger.error(f"Mobilization error: {str(e)}")
                await asyncio.sleep(60)

    async def _forecasting_loop(self):
        while True:
            try:
                for tier in [StorageTier.GLYCOGEN_QUEUE, StorageTier.STARCH_RESERVE]:
                    self.forecast_storage(tier)
                await asyncio.sleep(self.config.forecasting_interval_seconds)
            except Exception as e:
                logger.error(f"Forecasting error: {str(e)}")
                await asyncio.sleep(600)

    async def _analytics_loop(self):
        while True:
            try:
                self.generate_analytics()
                await asyncio.sleep(self.config.analytics_interval_seconds)
            except Exception as e:
                logger.error(f"Analytics error: {str(e)}")
                await asyncio.sleep(600)

    async def _rebalancing_loop(self):
        while True:
            try:
                if self.config.enable_collateral_rebalancing:
                    active_tokens = [t for t in self.storage_tokens.values() if not t.is_executed]
                    if active_tokens:
                        await self.collateral_rebalancer.rebalance(active_tokens)
                await asyncio.sleep(self.config.rebalancing_interval_seconds)
            except Exception as e:
                logger.error(f"Rebalancing loop error: {str(e)}")
                await asyncio.sleep(120)

    async def _evolution_loop(self):
        while True:
            try:
                if self.config.enable_genetic_optimizer:
                    await asyncio.sleep(self.config.ga_evolution_interval_hours * 3600)
                    logger.info("Starting genetic evolution cycle...")
                    result = await self.genetic_optimizer.evolve(generations=self.config.ga_generations)
                    logger.info(f"Evolution complete. Best fitness: {result['best_fitness']:.4f}")
                    # Save state after evolution
                    if self.config.enable_persistence:
                        await self.save_state()
            except Exception as e:
                logger.error(f"Evolution loop error: {str(e)}")
                await asyncio.sleep(3600)

    # ========================================================================
    # Forecast and Analytics (Enhanced)
    # ========================================================================

    def forecast_storage(self, tier: StorageTier, horizon_seconds: float = 3600) -> StorageForecast:
        queue = self._get_tier_queue(tier)
        current_usage = len(queue)
        capacity = self.capacity_manager.get_capacity(tier)

        recent_inflow = [t for t in self.inflow_history
                        if (datetime.utcnow() - t).total_seconds() < 3600]
        inflow_rate = len(recent_inflow) / 3600.0 if recent_inflow else 0.0

        recent_outflow = [t for t in self.outflow_history
                         if (datetime.utcnow() - t).total_seconds() < 3600]
        outflow_rate = len(recent_outflow) / 3600.0 if recent_outflow else 0.0

        net_rate = inflow_rate - outflow_rate

        if net_rate <= 0 or capacity <= current_usage:
            predicted_full_time = None
            confidence = 0.9
        else:
            remaining = capacity - current_usage
            seconds_to_full = remaining / net_rate
            predicted_full_time = datetime.utcnow() + timedelta(seconds=seconds_to_full)
            confidence = min(0.9, len(recent_inflow) / 100)

        scaling_stats = self.capacity_manager.get_scaling_stats()
        dynamic_capacity = capacity

        forecast = StorageForecast(
            tier=tier,
            current_usage=current_usage,
            capacity=capacity,
            inflow_rate=inflow_rate,
            outflow_rate=outflow_rate,
            predicted_full_time=predicted_full_time,
            confidence=confidence,
            dynamic_capacity=dynamic_capacity,
            scaling_factor=scaling_stats.get('current_scaling_factor', 1.0)
        )

        self.forecast_history.append(forecast)
        return forecast

    def generate_analytics(self) -> StorageAnalytics:
        total_stored = sum(len(self._get_tier_queue(t)) for t in StorageTier)
        tier_distribution = {tier.value: len(self._get_tier_queue(tier)) for tier in StorageTier}

        active_tokens = [t for t in self.storage_tokens.values() if not t.is_executed]
        avg_cost = np.mean([t.retrieval_cost for t in active_tokens]) if active_tokens else 0.0

        total_conversions = sum(
            len(task.conversion_history)
            for tier in StorageTier
            for task in self._get_tier_queue(tier)
        )
        successful_retrievals = sum(
            1 for t in self.storage_tokens.values() if t.is_executed and not t.penalty_paid
        )
        conversion_efficiency = successful_retrievals / max(total_conversions, 1)

        total_tokens = max(len(self.storage_tokens), 1)
        expired = sum(1 for t in self.storage_tokens.values() if t.penalty_paid)
        expiration_rate = expired / total_tokens

        mobilization_rate = self.total_mobilized / max(total_tokens, 1)
        cache_hit_rate = self.index_hits / max(self.index_hits + self.index_misses, 1)

        group_stats = self.similarity_dedup.get_group_stats()
        similarity_savings = self.similarity_savings
        similarity_groups = group_stats.get('total_groups', 0)

        avg_collateral = np.mean([t.collateral_amount for t in active_tokens]) if active_tokens else 0
        total_collateral = self.collateral_pool
        collateral_utilization = total_collateral / max(avg_collateral * len(active_tokens), 1) if active_tokens else 0

        analytics = StorageAnalytics(
            timestamp=datetime.utcnow(),
            total_stored=total_stored,
            deduplication_savings=self.deduplication_savings,
            merge_savings=self.merge_savings,
            avg_retrieval_cost=avg_cost,
            tier_distribution=tier_distribution,
            conversion_efficiency=conversion_efficiency,
            expiration_rate=expiration_rate,
            mobilization_rate=mobilization_rate,
            cache_hit_rate=cache_hit_rate,
            similarity_savings=similarity_savings,
            similarity_groups=similarity_groups,
            avg_collateral_ratio=avg_collateral / max(avg_cost, 1),
            collateral_utilization=collateral_utilization
        )

        self.analytics_history.append(analytics)
        return analytics

    # ========================================================================
    # Index and Helper Methods (Preserved)
    # ========================================================================

    def _add_to_index(self, task_id: str, tier: StorageTier, position: int):
        self.task_index[task_id] = {
            'tier': tier,
            'position': position,
            'stored_at': datetime.utcnow(),
            'access_count': 0,
            'last_accessed': None
        }

    def _update_index_position(self, task_id: str, new_tier: StorageTier, new_position: int):
        if task_id in self.task_index:
            self.task_index[task_id]['tier'] = new_tier
            self.task_index[task_id]['position'] = new_position
            self.task_index[task_id]['stored_at'] = datetime.utcnow()

    def _remove_from_index(self, task_id: str):
        self.task_index.pop(task_id, None)

    def find_task(self, task_id: str) -> Optional[Tuple[StorageTier, int]]:
        if task_id in self.task_index:
            self.index_hits += 1
            entry = self.task_index[task_id]
            entry['access_count'] += 1
            entry['last_accessed'] = datetime.utcnow()
            return entry['tier'], entry['position']
        self.index_misses += 1
        return None

    def _find_task_by_id(self, task_id: str) -> Optional[StoredTask]:
        location = self.find_task(task_id)
        if location:
            return self._get_from_tier_position(location[0], location[1])
        return self._scan_all_tiers(task_id)

    def _get_from_tier_position(self, tier: StorageTier, position: int) -> Optional[StoredTask]:
        queue = self._get_tier_queue(tier)
        if position < len(queue):
            return queue[position]
        return None

    def _scan_all_tiers(self, task_id: str) -> Optional[StoredTask]:
        for tier in StorageTier:
            queue = self._get_tier_queue(tier)
            for i, task in enumerate(queue):
                if task.task_id == task_id:
                    self._add_to_index(task_id, tier, i)
                    return task
        return None

    def _get_tier_queue(self, tier: StorageTier) -> deque:
        tier_map = {
            StorageTier.ATP_CACHE: self.atp_cache,
            StorageTier.GLYCOGEN_QUEUE: self.glycogen_queue,
            StorageTier.STARCH_RESERVE: self.starch_reserve,
            StorageTier.LIPID_DEPOT: self.lipid_depot,
            StorageTier.LIGNIN_ARCHIVE: self.lignin_archive
        }
        return tier_map.get(tier, deque())

    def _find_token(self, task_id: str) -> Optional[StorageToken]:
        for token in self.storage_tokens.values():
            if token.task_id == task_id and not token.is_duplicate:
                return token
        return None

    # ========================================================================
    # Tier Conversion (Preserved)
    # ========================================================================

    def convert_tier(self, token_id: str, target_tier: StorageTier) -> bool:
        if token_id not in self.storage_tokens:
            return False

        token = self.storage_tokens[token_id]
        if token.is_duplicate:
            return False

        current_tier = token.storage_tier
        if current_tier == target_tier:
            return True

        location = self.find_task(token.task_id)
        if not location:
            return False

        tier, position = location
        stored_task = self._get_from_tier_position(tier, position)
        if stored_task is None:
            return False

        queue = self._get_tier_queue(current_tier)
        try:
            queue.remove(stored_task)
        except ValueError:
            pass

        key = f"{current_tier.name}→{target_tier.name}"
        conversion_cost = self.conversion_costs.get(key, 3.0)
        stored_task.current_retrieval_cost += conversion_cost
        stored_task.conversion_history.append({
            'from_tier': current_tier.value,
            'to_tier': target_tier.value,
            'cost': conversion_cost,
            'timestamp': datetime.utcnow().isoformat()
        })

        stored_task.storage_tier = target_tier
        token.storage_tier = target_tier
        token.retrieval_cost = stored_task.current_retrieval_cost

        new_queue = self._get_tier_queue(target_tier)
        new_position = len(new_queue)
        new_queue.append(stored_task)

        self._update_index_position(token.task_id, target_tier, new_position)

        logger.info(f"Converted {token.task_id}: {current_tier.value} → {target_tier.value} (cost={conversion_cost:.1f})")
        return True

    # ========================================================================
    # Dashboard and Recommendations (Enhanced)
    # ========================================================================

    def get_dashboard_data(self) -> StorageDashboardData:
        total_stored = sum(len(self._get_tier_queue(t)) for t in StorageTier)
        tier_utilization = {}
        for tier in StorageTier:
            queue = self._get_tier_queue(tier)
            capacity = self.capacity_manager.get_capacity(tier)
            tier_utilization[tier.value] = len(queue) / max(capacity, 1)

        active_tokens = [t for t in self.storage_tokens.values() if not t.is_executed]
        avg_retrieval_cost = np.mean([t.retrieval_cost for t in active_tokens]) if active_tokens else 0.0

        recent_mobilizations = list(self.mobilization_history)[-20:]
        mobilization_rate = len(recent_mobilizations) / 20 if len(recent_mobilizations) >= 20 else 0

        dedup_stats = {
            'exact_savings': self.deduplication_savings,
            'merge_savings': self.merge_savings,
            'similarity_savings': self.similarity_savings,
            'total_savings': self.deduplication_savings + self.merge_savings + self.similarity_savings
        }

        recommendations = self.get_optimization_recommendations()

        return StorageDashboardData(
            timestamp=datetime.utcnow(),
            storage_overview={
                'total_stored': total_stored,
                'active_tokens': len(active_tokens),
                'collateral_pool': self.collateral_pool
            },
            tier_utilization=tier_utilization,
            retrieval_metrics={
                'avg_retrieval_cost': avg_retrieval_cost,
                'cache_hit_rate': self.index_hits / max(self.index_hits + self.index_misses, 1),
                'mobilization_rate': mobilization_rate
            },
            mobilization_activity={
                'total_mobilized': self.total_mobilized,
                'recent_count': len(recent_mobilizations),
                'last_mobilization': self.mobilization_history[-1] if self.mobilization_history else None
            },
            deduplication_stats=dedup_stats,
            recommendations=recommendations
        )

    def get_optimization_recommendations(self) -> List[str]:
        recommendations = []
        analytics = self.generate_analytics()

        scaling_stats = self.capacity_manager.get_scaling_stats()
        if scaling_stats.get('current_scaling_factor', 1.0) > 1.2:
            recommendations.append("System load high - dynamic capacity increased")

        for tier, count in analytics.tier_distribution.items():
            tier_enum = StorageTier(tier)
            capacity = self.capacity_manager.get_capacity(tier_enum)
            utilization = count / max(capacity, 1)
            if utilization > 0.8:
                recommendations.append(f"Increase {tier} capacity or accelerate conversion to slower tier")

        total_savings = self.deduplication_savings + self.merge_savings + self.similarity_savings
        if total_savings > 0:
            savings_pct = total_savings / max(analytics.total_stored + total_savings, 1) * 100
            recommendations.append(f"Deduplication saved {total_savings} slots ({savings_pct:.1f}%)")

        if self.similarity_savings > 0:
            recommendations.append(f"Similarity deduplication saved {self.similarity_savings} slots")

        if analytics.collateral_utilization < 0.3:
            recommendations.append("Low collateral utilization - consider reducing guarantee levels")

        if analytics.conversion_efficiency < 0.5:
            recommendations.append("Low conversion efficiency. Review tier migration schedule.")

        if analytics.expiration_rate > 0.1:
            recommendations.append(
                f"High expiration rate ({analytics.expiration_rate:.1%}). "
                "Consider reducing guarantee levels or extending deadlines."
            )

        forecast = asyncio.run(self.predictive_mobilizer.forecast_demand())
        if forecast.get('status') == 'success' and forecast.get('trend') == 'increasing':
            recommendations.append(
                f"Demand forecast indicates increasing trend ({forecast['average']:.2f}). "
                "Consider proactive mobilization."
            )

        if not recommendations:
            recommendations.append("Storage operating optimally. No changes needed.")

        return recommendations

    # ========================================================================
    # Statistics (Enhanced)
    # ========================================================================

    def get_storage_stats(self) -> Dict[str, Any]:
        stats = {
            'tiers': {tier.value: len(self._get_tier_queue(tier)) for tier in StorageTier},
            'total_stored': sum(len(self._get_tier_queue(t)) for t in StorageTier),
            'active_tokens': len([t for t in self.storage_tokens.values() if not t.is_executed]),
            'collateral_pool': self.collateral_pool,
            'index_stats': {
                'hits': self.index_hits,
                'misses': self.index_misses,
                'hit_rate': self.index_hits / max(self.index_hits + self.index_misses, 1)
            },
            'deduplication': {
                'exact_savings': self.deduplication_savings,
                'merge_savings': self.merge_savings,
                'similarity_savings': self.similarity_savings,
                'total_saved': self.deduplication_savings + self.merge_savings + self.similarity_savings
            },
            'similarity_groups': self.similarity_dedup.get_group_stats(),
            'mobilization': {
                'total_mobilized': self.total_mobilized,
                'recent': list(self.mobilization_history)[-10:],
                'predictive_active': MobilizationTrigger.PREDICTIVE in [t.value for t in self.mobilization_triggers]
            },
            'capacity_dynamic': self.capacity_manager.get_scaling_stats(),
            'collateral_rebalancing': self.collateral_rebalancer.get_rebalancing_stats(),
            'forecast': {
                tier.value: {
                    'current': self.forecast_storage(tier).current_usage,
                    'capacity': self.forecast_storage(tier).capacity,
                    'dynamic_capacity': self.forecast_storage(tier).dynamic_capacity,
                    'predicted_full': self.forecast_storage(tier).predicted_full_time.isoformat()
                    if self.forecast_storage(tier).predicted_full_time else None
                }
                for tier in [StorageTier.GLYCOGEN_QUEUE, StorageTier.STARCH_RESERVE]
            },
            'recommendations': self.get_optimization_recommendations(),
            'genetic_optimizer': self.genetic_optimizer.get_status()
        }

        if self.analytics_history:
            latest = self.analytics_history[-1]
            stats['analytics'] = {
                'deduplication_savings': latest.deduplication_savings,
                'merge_savings': latest.merge_savings,
                'similarity_savings': latest.similarity_savings,
                'avg_retrieval_cost': latest.avg_retrieval_cost,
                'conversion_efficiency': latest.conversion_efficiency,
                'expiration_rate': latest.expiration_rate,
                'mobilization_rate': latest.mobilization_rate,
                'cache_hit_rate': latest.cache_hit_rate,
                'avg_collateral_ratio': latest.avg_collateral_ratio,
                'collateral_utilization': latest.collateral_utilization
            }

        stats['dashboard'] = self.get_dashboard_data().__dict__
        return stats

    def get_deduplication_report(self) -> Dict[str, Any]:
        total_stored = sum(len(self._get_tier_queue(t)) for t in StorageTier)
        total_saved = self.deduplication_savings + self.merge_savings + self.similarity_savings
        return {
            'exact_savings': self.deduplication_savings,
            'merge_savings': self.merge_savings,
            'similarity_savings': self.similarity_savings,
            'total_savings': total_saved,
            'savings_percentage': total_saved / max(total_stored + total_saved, 1) * 100,
            'hash_index_size': len(self.task_hash_index),
            'similarity_groups': self.similarity_dedup.get_group_stats(),
            'recommendation': f"Deduplication saved {total_saved} storage slots ({total_saved / max(total_stored + total_saved, 1) * 100:.1f}% reduction)"
        }

    def get_mobilization_report(self) -> Dict[str, Any]:
        recent = list(self.mobilization_history)[-50:]
        if not recent:
            return {'status': 'No recent mobilization activity'}

        trigger_counts = defaultdict(int)
        for entry in recent:
            for trigger in entry.get('triggers', []):
                trigger_counts[trigger] += 1

        forecast = asyncio.run(self.predictive_mobilizer.forecast_demand())
        return {
            'total_mobilized': self.total_mobilized,
            'recent_activity': len(recent),
            'trigger_distribution': dict(trigger_counts),
            'most_common_trigger': max(trigger_counts, key=trigger_counts.get) if trigger_counts else 'none',
            'predictive_mobilization': {
                'active': MobilizationTrigger.PREDICTIVE in [t.value for t in self.mobilization_triggers],
                'demand_forecast': forecast
            },
            'recommendation': (
                f"Most mobilizations triggered by: {max(trigger_counts, key=trigger_counts.get) if trigger_counts else 'none'}. "
                f"Total mobilized: {self.total_mobilized}"
            )
        }

    # ========================================================================
    # Metrics (NEW)
    # ========================================================================

    def get_metrics(self) -> Dict[str, Any]:
        """Return a dictionary of key metrics for monitoring."""
        self.metrics['total_stored'] = sum(len(self._get_tier_queue(t)) for t in StorageTier)
        self.metrics['active_tokens'] = len([t for t in self.storage_tokens.values() if not t.is_executed])
        self.metrics['collateral_pool'] = self.collateral_pool
        self.metrics['cache_hit_rate'] = self.index_hits / max(self.index_hits + self.index_misses, 1)
        self.metrics['conversion_efficiency'] = self.generate_analytics().conversion_efficiency
        self.metrics['expiration_rate'] = self.generate_analytics().expiration_rate
        self.metrics['last_update'] = datetime.utcnow().isoformat()
        return self.metrics

    # ========================================================================
    # Shutdown
    # ========================================================================

    async def shutdown(self):
        """Graceful shutdown."""
        logger.info("Shutting down Biomass Storage")
        for task in self._background_tasks:
            task.cancel()
        if self.config.enable_persistence:
            await self.save_state()
        logger.info("Shutdown complete")
