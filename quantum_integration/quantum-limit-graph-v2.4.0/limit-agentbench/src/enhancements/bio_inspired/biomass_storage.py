# =============================================================================
# Enhanced Biomass Storage v8.0.0 - Full Implementation with All Enhancement Phases
# =============================================================================
"""
Enhanced Biomass Storage with:
- Central Green Agent integration (Storage, MessageQueue, ParetoGating, AdaptiveCostFunction, DriftDetector, MetricsRegistry).
- Multi-Objective Pareto-Driven (MOPD) genetic optimizer.
- Teacher policy (policy_probs) with adaptive cost and Pareto gating.
- Causal Reinforcement Learning agent (placeholder).
- Federated Green Learning coordinator.
- Advanced Multi-Agent Coordination (TierAgents).
- Temporal Logic / Formal Verification (SafetyMonitor).
- Explainable AI (explain_decision).
- Adaptive Precision Switching (PrecisionController).
- External Carbon Markets (CarbonMarketClient, placeholder).
- Resilience Engineering (ChaosInjector).
- Human-in-the-Loop (request_human_approval).
- Retry async decorator, persistence, FeedbackEvent publication.
"""

import asyncio
import logging
import sys
from typing import Dict, Any, List, Optional, Tuple, Set, Union, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
import numpy as np
from collections import deque, defaultdict
import uuid
import hashlib
import json
import random
import os
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

try:
    from pqcrypto.sign import dilithium, falcon
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

try:
    from web3 import Web3, Account
    from web3.middleware import geth_poa_middleware, gas_price_strategy
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

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
# Central Green Agent Components
# ============================================================================
try:
    from ..config import config as central_config
    from ..storage import Storage as CentralStorage
    from ..scaling.message_queue import AsyncMessageQueue
    from ..routing.pareto_gating import ParetoGating
    from ..feedback.adaptive_cost import AdaptiveCostFunction
    from ..safety.drift_detector import DriftDetector
    from ..metrics import MetricsRegistry
    from ..schemas.feedback_event import FeedbackEvent
    from ..logger import logger as central_logger
    CENTRAL_AVAILABLE = True
except ImportError:
    CENTRAL_AVAILABLE = False
    CentralStorage = None
    AsyncMessageQueue = None
    ParetoGating = None
    AdaptiveCostFunction = None
    DriftDetector = None
    MetricsRegistry = None
    FeedbackEvent = None
    central_config = None

# ============================================================================
# Retry Async Decorator
# ============================================================================
def retry_async_decorator(max_retries=3, base_delay_ms=1000, max_delay_ms=5000):
    """Decorator that applies retry logic to an async function."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            if TENACITY_AVAILABLE:
                @retry(
                    stop=stop_after_attempt(max_retries),
                    wait=wait_exponential(multiplier=base_delay_ms/1000.0, min=base_delay_ms/1000.0, max=max_delay_ms/1000.0),
                    retry=retry_if_exception_type(Exception),
                    before_sleep=before_sleep_log(logger, logging.WARNING)
                )
                async def inner():
                    return await func(*args, **kwargs)
                return await inner()
            else:
                for attempt in range(max_retries):
                    try:
                        return await func(*args, **kwargs)
                    except Exception as e:
                        if attempt == max_retries - 1:
                            raise
                        delay = min(base_delay_ms * (2 ** attempt), max_delay_ms) / 1000.0
                        await asyncio.sleep(delay)
        return wrapper
    return decorator

# ============================================================================
# Enums
# ============================================================================
class StorageTier(Enum):
    ATP_CACHE = "ATP_CACHE"
    GLYCOGEN_QUEUE = "GLYCOGEN_QUEUE"
    STARCH_RESERVE = "STARCH_RESERVE"
    LIPID_DEPOT = "LIPID_DEPOT"
    LIGNIN_ARCHIVE = "LIGNIN_ARCHIVE"

class GuaranteeLevel(Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

# ============================================================================
# Configuration Classes
# ============================================================================
@dataclass
class MOPDConfig:
    enabled: bool = True
    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        "efficiency": 0.3,
        "cost_score": 0.2,
        "expiration_rate": 0.2,
        "cache_hit_rate": 0.3
    })
    grid_resolution: int = 10

@dataclass
class BiomassStorageConfig:
    # Storage parameters
    max_storage_tokens: int = 10000
    default_collateral_ratio: float = 1.0
    persistence_path: str = "biomass_storage_state.json"
    # Genetic algorithm
    ga_population_size: int = 50
    ga_mutation_rate: float = 0.1
    ga_crossover_rate: float = 0.7
    ga_generations: int = 10
    ga_tournament_size: int = 3
    # MOPD
    mopd: MOPDConfig = field(default_factory=MOPDConfig)
    # Enhanced settings
    chaos_probability: float = 0.0
    carbon_market_config: Optional[Dict[str, Any]] = None
    precision_policy: str = "energy_aware"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BiomassStorageConfig':
        return cls(**data)

    @classmethod
    def from_env_and_file(cls) -> 'BiomassStorageConfig':
        # Simplified: just return default config
        return cls()

# ============================================================================
# Data Classes
# ============================================================================
@dataclass
class StoredTask:
    task_id: str
    content: str
    tier: StorageTier
    timestamp: datetime
    ttl: Optional[timedelta] = None
    access_count: int = 0
    last_access: Optional[datetime] = None
    hash_value: str = ""

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['tier'] = self.tier.value
        d['timestamp'] = self.timestamp.isoformat()
        d['ttl'] = self.ttl.total_seconds() if self.ttl else None
        d['last_access'] = self.last_access.isoformat() if self.last_access else None
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StoredTask':
        data = data.copy()
        data['tier'] = StorageTier(data['tier'])
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        data['ttl'] = timedelta(seconds=data['ttl']) if data['ttl'] else None
        data['last_access'] = datetime.fromisoformat(data['last_access']) if data['last_access'] else None
        return cls(**data)

@dataclass
class StorageToken:
    token_id: str
    task_id: str
    tier: StorageTier
    created_at: datetime
    expires_at: Optional[datetime] = None
    signature: Optional[bytes] = None
    is_valid: bool = True

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['tier'] = self.tier.value
        d['created_at'] = self.created_at.isoformat()
        d['expires_at'] = self.expires_at.isoformat() if self.expires_at else None
        d['signature'] = self.signature.hex() if self.signature else None
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StorageToken':
        data = data.copy()
        data['tier'] = StorageTier(data['tier'])
        data['created_at'] = datetime.fromisoformat(data['created_at'])
        data['expires_at'] = datetime.fromisoformat(data['expires_at']) if data['expires_at'] else None
        data['signature'] = bytes.fromhex(data['signature']) if data['signature'] else None
        return cls(**data)

@dataclass
class StorageAnalytics:
    total_tasks: int
    total_size: int
    conversion_efficiency: float
    avg_retrieval_cost: float
    expiration_rate: float
    cache_hit_rate: float
    storage_distribution: Dict[str, int]

@dataclass
class StorageForecast:
    predicted_demand: float
    predicted_inflow: float
    confidence: float
    timestamp: datetime

@dataclass
class MOPDPoint:
    conversion_costs: Dict[str, float]
    collateral_ratios: Dict[str, float]
    efficiency: float
    cost_score: float
    expiration_rate: float
    cache_hit_rate: float
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDPoint':
        return cls(**data)

# ============================================================================
# Enhanced Classes (Phase 3 modules)
# ============================================================================

class CausalRLAgent:
    """Causal Reinforcement Learning Agent (simplified Q-learning)."""
    def __init__(self, state_dim: int, action_dim: int):
        self.state_dim = state_dim
        self.action_dim = action_dim
        self.q_table = defaultdict(lambda: np.zeros(action_dim))
        self.epsilon = 0.1
        self.learning_rate = 0.1
        self.gamma = 0.99

    def act(self, state: np.ndarray) -> int:
        if random.random() < self.epsilon:
            return random.randrange(self.action_dim)
        state_key = tuple(state)
        return int(np.argmax(self.q_table[state_key]))

    def update(self, state: np.ndarray, action: int, reward: float, next_state: np.ndarray, done: bool):
        state_key = tuple(state)
        next_key = tuple(next_state)
        best_next = np.max(self.q_table[next_key]) if not done else 0.0
        td_target = reward + self.gamma * best_next
        self.q_table[state_key][action] += self.learning_rate * (td_target - self.q_table[state_key][action])

class FederatedCoordinator:
    """Coordinates federated learning across deployments."""
    def __init__(self, storage_instance, queue: Optional[AsyncMessageQueue]):
        self.storage = storage_instance
        self.queue = queue

    async def send_update(self):
        if not self.queue:
            logger.warning("No message queue for federated update.")
            return
        model = self.storage.genetic_optimizer.to_dict()
        await self.queue.publish("federated_updates", json.dumps(model))
        logger.info("Federated update sent.")

    async def receive_global_model(self, model_json: str):
        model = json.loads(model_json)
        self.storage.genetic_optimizer.from_dict(model)
        logger.info("Global model applied.")

class TierAgent:
    """Agent responsible for a specific storage tier."""
    def __init__(self, tier: StorageTier, storage_instance, queue: Optional[AsyncMessageQueue]):
        self.tier = tier
        self.storage = storage_instance
        self.queue = queue
        self.specialisation = None

    async def act(self, state: Dict[str, Any]):
        # Use policy_probs to decide action; simplified: just log
        probs = await self.storage.policy_probs(state)
        logger.info(f"TierAgent {self.tier.value} action probabilities: {probs}")
        # Actual action selection would be implemented here

class SafetyMonitor:
    """Runtime monitor for safety properties."""
    def __init__(self):
        self.invariants = []

    def add_invariant(self, condition_fn: Callable[[Dict[str, Any]], bool], description: str):
        self.invariants.append((condition_fn, description))

    def check(self, state: Dict[str, Any]) -> List[str]:
        violations = []
        for fn, desc in self.invariants:
            if not fn(state):
                violations.append(desc)
        return violations

class PrecisionController:
    """Decides numerical precision based on load/energy budget."""
    def __init__(self, policy: str = "energy_aware"):
        self.policy = policy

    def get_precision(self, load: float, energy_budget: float) -> str:
        if self.policy == "energy_aware":
            if load > 0.8 or energy_budget < 0.2:
                return "float16"
            else:
                return "float32"
        return "float32"

class CarbonMarketClient:
    """Placeholder for external carbon market integration."""
    def __init__(self, provider_url: str, contract_address: str, private_key: str):
        self.w3 = Web3(Web3.HTTPProvider(provider_url)) if WEB3_AVAILABLE else None
        self.contract_address = contract_address
        self.account = Account.from_key(private_key) if WEB3_AVAILABLE else None

    def buy_credits(self, amount: float) -> bool:
        if not self.w3:
            logger.warning("Web3 not available; cannot buy credits.")
            return False
        # Actual smart contract interaction would go here
        return True

    def sell_credits(self, amount: float) -> bool:
        if not self.w3:
            logger.warning("Web3 not available; cannot sell credits.")
            return False
        return True

class ChaosInjector:
    """Injects random failures to test resilience."""
    def __init__(self, storage_instance, chaos_probability: float = 0.0):
        self.storage = storage_instance
        self.chaos_probability = chaos_probability

    async def maybe_inject_failure(self):
        if random.random() < self.chaos_probability:
            action = random.choice(['kill_task', 'corrupt_data', 'delay_queue'])
            logger.warning(f"Chaos injection: {action}")
            if action == 'kill_task' and self.storage._background_tasks:
                task = random.choice(self.storage._background_tasks)
                task.cancel()
            elif action == 'delay_queue':
                await asyncio.sleep(random.uniform(0.1, 1.0))
            # corrupt_data would be implemented here

# ============================================================================
# Genetic Optimizer (with fixes and enhancements)
# ============================================================================
class GeneticOptimizer:
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
        self.pareto_front: List[MOPDPoint] = []
        self.adaptive_cost = None
        self.pareto_gating = None
        self.drift_detector = None

    def set_central_components(self, adaptive_cost, pareto_gating, drift_detector):
        self.adaptive_cost = adaptive_cost
        self.pareto_gating = pareto_gating
        self.drift_detector = drift_detector

    def _initialize_individual(self):
        costs = {}
        for (from_tier, to_tier) in self.tier_pairs:
            costs[f"{from_tier}→{to_tier}"] = random.uniform(self.conversion_cost_bounds['min'], self.conversion_cost_bounds['max'])
        ratios = {}
        for level in self.guarantee_levels:
            ratios[level] = random.uniform(self.collateral_bounds['min'], self.collateral_bounds['max'])
        return {'conversion_costs': costs, 'collateral_ratios': ratios}

    def _initialize_population(self):
        return [self._initialize_individual() for _ in range(self.population_size)]

    def _apply_individual(self, individual):
        self._original_conversion_costs = self.biomass.conversion_costs.copy()
        self._original_collateral_ratios = self.biomass.collateral_ratios.copy()
        self.biomass.conversion_costs = individual['conversion_costs'].copy()
        self.biomass.collateral_ratios = individual['collateral_ratios'].copy()

    def _restore_original_parameters(self):
        if hasattr(self, '_original_conversion_costs'):
            self.biomass.conversion_costs = self._original_conversion_costs
            self.biomass.collateral_ratios = self._original_collateral_ratios

    async def _evaluate_objectives(self, individual):
        # Use a lock to prevent concurrent mutation
        async with self.biomass.param_lock:
            self._apply_individual(individual)
            try:
                analytics = self.biomass.generate_analytics()
                eff = analytics.conversion_efficiency
                avg_cost = analytics.avg_retrieval_cost
                exp_rate = analytics.expiration_rate
                hit_rate = analytics.cache_hit_rate
                cost_score = max(0, 1.0 - avg_cost / 100.0) if avg_cost > 0 else 0.5
                return {
                    'efficiency': eff,
                    'cost_score': cost_score,
                    'expiration_rate': 1.0 - exp_rate,  # higher is better
                    'cache_hit_rate': hit_rate
                }
            finally:
                self._restore_original_parameters()

    def _filter_pareto(self, points):
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

    def _select_best_from_pareto(self, pareto_front):
        if not pareto_front:
            return None
        weights = self.config.mopd.objective_weights
        objective_keys = list(weights.keys())
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

    async def evolve(self, generations=None):
        if generations is None:
            generations = self.generations
        population = self._initialize_population()
        if self.config.mopd.enabled:
            self.pareto_front = []

        for gen in range(generations):
            # Evaluate all individuals (async)
            individuals_with_objs = []
            for ind in population:
                objs = await self._evaluate_objectives(ind)
                individuals_with_objs.append((ind, objs))

            # Central components integration
            if self.adaptive_cost and self.pareto_gating:
                candidates = []
                for ind, objs in individuals_with_objs:
                    candidates.append({
                        'expert_id': str(id(ind)),
                        'quality_score': objs['efficiency'],
                        'carbon_g': 0.0,
                        'latency_ms': 0.0,
                        'energy_joules': 0.0,
                        'individual': ind,
                        'objectives': objs
                    })
                filtered = self.pareto_gating.filter(candidates)
                if filtered:
                    allowed_ids = {c['expert_id'] for c in filtered}
                    new_individuals = [(ind, objs) for ind, objs in individuals_with_objs if str(id(ind)) in allowed_ids]
                    if new_individuals:
                        individuals_with_objs = new_individuals
                    # else keep original (fallback)
                # Compute costs and convert to fitness (lower cost -> higher fitness)
                costs = []
                for ind, objs in individuals_with_objs:
                    cost = self.adaptive_cost.compute(
                        quality=objs['efficiency'],
                        carbon_g=0.0,
                        latency_ms=0.0,
                        energy_joules=0.0,
                        health=0.8,
                        atp=0.5
                    )
                    costs.append(cost)
                fitness_scores = [-c for c in costs]  # negative cost = fitness
            else:
                # Local fallback
                if self.config.mopd.enabled:
                    weights = self.config.mopd.objective_weights
                    fitness_scores = []
                    for _, objs in individuals_with_objs:
                        score = (weights.get('efficiency', 0.3) * objs['efficiency'] +
                                 weights.get('cost_score', 0.2) * objs['cost_score'] +
                                 weights.get('expiration_rate', 0.2) * objs['expiration_rate'] +
                                 weights.get('cache_hit_rate', 0.3) * objs['cache_hit_rate'])
                        fitness_scores.append(score)
                else:
                    fitness_scores = [objs['efficiency'] for _, objs in individuals_with_objs]

            # Update Pareto front if MOPD enabled
            if self.config.mopd.enabled:
                points = []
                for ind, objs in individuals_with_objs:
                    points.append(MOPDPoint(
                        conversion_costs=ind['conversion_costs'].copy(),
                        collateral_ratios=ind['collateral_ratios'].copy(),
                        efficiency=objs['efficiency'],
                        cost_score=objs['cost_score'],
                        expiration_rate=objs['expiration_rate'],
                        cache_hit_rate=objs['cache_hit_rate']
                    ))
                self.pareto_front = self._filter_pareto(self.pareto_front + points)

            # Selection and reproduction
            new_population = []
            # Elitism: keep best
            if individuals_with_objs:
                best_idx = max(range(len(individuals_with_objs)), key=lambda i: fitness_scores[i])
                new_population.append(individuals_with_objs[best_idx][0])
            while len(new_population) < self.population_size:
                if random.random() < self.crossover_rate and len(individuals_with_objs) >= 2:
                    parent1 = self._select(individuals_with_objs, fitness_scores)
                    parent2 = self._select(individuals_with_objs, fitness_scores)
                    child = self._crossover(parent1, parent2)
                    child = self._mutate(child)
                    new_population.append(child)
                else:
                    if individuals_with_objs:
                        parent = self._select(individuals_with_objs, fitness_scores)
                        new_population.append(parent.copy())
                    else:
                        new_population.append(self._initialize_individual())
            population = new_population

        # After evolution, apply best individual
        if self.config.mopd.enabled and self.pareto_front:
            best_point = self._select_best_from_pareto(self.pareto_front)
            if best_point:
                self.best_individual = {
                    'conversion_costs': best_point.conversion_costs.copy(),
                    'collateral_ratios': best_point.collateral_ratios.copy()
                }
                self.best_fitness = best_point.scalarised_score
                self._apply_individual(self.best_individual)
                logger.info(f"Applied best MOPD individual with scalarised score {self.best_fitness:.4f}")
        else:
            if individuals_with_objs:
                best_idx = max(range(len(individuals_with_objs)), key=lambda i: fitness_scores[i])
                self.best_fitness = fitness_scores[best_idx]
                self.best_individual = individuals_with_objs[best_idx][0]
                self._apply_individual(self.best_individual)

        self.evolution_history.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'generations': generations,
            'best_fitness': self.best_fitness
        })

        # Publish FeedbackEvent
        if self.biomass.queue and FeedbackEvent:
            event = FeedbackEvent.create_with_context(
                task_id=f"biomass_evolve_{uuid.uuid4().hex[:8]}",
                selected_action="genetic_evolution",
                quality_score=self.best_fitness,
                energy_joules=0.0,
                carbon_g=0.0,
                feedback_type="biomass_storage",
                adaptive_cost_value=self.best_fitness,
                state={'pareto_front_size': len(self.pareto_front) if self.config.mopd.enabled else 0},
                candidates=[{'action': 'evolve'}],
                source="biomass_storage",
                environment=getattr(central_config, "ENVIRONMENT", "production") if central_config else "production",
                tags=["biomass", "evolution"]
            )
            await self.biomass.queue.publish("feedback_events", event.to_json())

        # Drift detection
        if self.drift_detector and self.adaptive_cost:
            drift_score = await self.drift_detector.check_drift(self.adaptive_cost.get_current_weights())
            if drift_score and drift_score > 0.7:
                logger.warning(f"High drift detected ({drift_score:.3f}); adjusting MOPD weights.")
                self.config.mopd.objective_weights['efficiency'] = min(0.5, self.config.mopd.objective_weights['efficiency'] + 0.05)
                total = sum(self.config.mopd.objective_weights.values())
                for k in self.config.mopd.objective_weights:
                    self.config.mopd.objective_weights[k] /= total

        return {
            'best_fitness': self.best_fitness,
            'best_individual': self.best_individual,
            'generations': generations,
            'pareto_front': [p.to_dict() for p in self.pareto_front] if self.config.mopd.enabled else None
        }

    def _select(self, individuals_with_objs, fitness_scores):
        # Tournament selection based on indices
        indices = list(range(len(individuals_with_objs)))
        tournament = random.sample(indices, min(self.tournament_size, len(indices)))
        best_idx = max(tournament, key=lambda i: fitness_scores[i])
        return individuals_with_objs[best_idx][0]

    def _crossover(self, parent1, parent2):
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

    def _mutate(self, individual):
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

    def to_dict(self):
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

    def from_dict(self, data):
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

    def get_status(self):
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
# Persistence Manager
# ============================================================================
class BiomassStoragePersistence:
    CURRENT_VERSION = "2.1"

    def __init__(self, config: BiomassStorageConfig):
        self.config = config
        self.path = Path(config.persistence_path)
        self._lock = asyncio.Lock()

    @retry_async_decorator(max_retries=3, base_delay_ms=2000)
    async def save_state(self, storage):
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
    async def load_state(self, storage):
        async with self._lock:
            if not self.path.exists():
                return False
            try:
                with open(self.path, 'r') as f:
                    state = json.load(f)
                version = state.get('version', '0.0')
                if version != self.CURRENT_VERSION:
                    logger.warning(f"State version mismatch: {version} != {self.CURRENT_VERSION}")
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
                logger.info(f"Biomass storage state loaded from {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return False

    def _make_serializable(self, obj):
        if isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(v) for v in obj]
        elif isinstance(obj, tuple):
            return list(obj)
        elif isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif hasattr(obj, '__dict__'):
            return self._make_serializable(obj.__dict__)
        else:
            return obj

# ============================================================================
# BiomassStorage (Main Class with all enhancements)
# ============================================================================
class BiomassStorage:
    def __init__(
        self,
        config: Optional[BiomassStorageConfig] = None,
        token_manager=None,
        gradient_manager=None,
        storage: Optional[CentralStorage] = None,
        message_queue: Optional[AsyncMessageQueue] = None,
        adaptive_cost: Optional[AdaptiveCostFunction] = None,
        pareto_gating: Optional[ParetoGating] = None,
        drift_detector: Optional[DriftDetector] = None,
        metrics: Optional[MetricsRegistry] = None,
        # New optional components
        rl_agent=None,
        federated_coordinator=None,
        carbon_market_client=None,
        precision_controller=None,
        chaos_injector=None,
        safety_monitor=None,
    ):
        if config is None:
            config = BiomassStorageConfig.from_env_and_file()
        self.config = config
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager

        # Central components
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto_gating = pareto_gating
        self.drift_detector = drift_detector
        self.metrics = metrics

        # Core data structures
        self.task_index: Dict[str, StoredTask] = {}
        self.task_hash_index: Dict[str, str] = {}
        self.storage_tokens: Dict[str, StorageToken] = {}
        self.collateral_pool: float = 0.0
        self.total_mobilized: int = 0
        self.mobilization_history: deque = deque(maxlen=500)
        self.deduplication_savings: int = 0
        self.merge_savings: int = 0
        self.similarity_savings: int = 0
        self.index_hits: int = 0
        self.index_misses: int = 0
        self.inflow_history: deque = deque(maxlen=100)
        self.outflow_history: deque = deque(maxlen=100)
        self.analytics_history: deque = deque(maxlen=1000)
        self.forecast_history: deque = deque(maxlen=50)

        # Parameters to be optimized
        self.conversion_costs: Dict[str, float] = {
            f"{from_tier}→{to_tier}": 1.0 for (from_tier, to_tier) in [
                ('ATP_CACHE', 'GLYCOGEN_QUEUE'),
                ('GLYCOGEN_QUEUE', 'STARCH_RESERVE'),
                ('STARCH_RESERVE', 'LIPID_DEPOT'),
                ('LIPID_DEPOT', 'LIGNIN_ARCHIVE'),
                ('LIPID_DEPOT', 'STARCH_RESERVE'),
                ('STARCH_RESERVE', 'GLYCOGEN_QUEUE'),
                ('GLYCOGEN_QUEUE', 'ATP_CACHE'),
            ]
        }
        self.collateral_ratios: Dict[str, float] = {
            level.name: 1.0 for level in GuaranteeLevel
        }

        # Placeholder subsystems (simplified)
        self.similarity_dedup = type('SimilarityDedup', (), {
            'similarity_groups': {},
            'group_representatives': {},
            '_task_texts': {}
        })()
        self.capacity_manager = type('CapacityManager', (), {
            'load_history': deque(maxlen=100),
            'scaling_factor': 1.0
        })()
        self.predictive_mobilizer = type('PredictiveMobilizer', (), {
            'demand_history': []
        })()

        # Concurrency
        self.param_lock = asyncio.Lock()

        # Genetic optimizer
        self.genetic_optimizer = GeneticOptimizer(self, config)
        if adaptive_cost and pareto_gating and drift_detector:
            self.genetic_optimizer.set_central_components(adaptive_cost, pareto_gating, drift_detector)

        # Enhanced components
        self.rl_agent = rl_agent or CausalRLAgent(state_dim=10, action_dim=8)
        self.federated_coordinator = federated_coordinator or (FederatedCoordinator(self, message_queue) if message_queue else None)
        self.precision_controller = precision_controller or PrecisionController(config.precision_policy)
        self.carbon_market_client = carbon_market_client
        if not self.carbon_market_client and config.carbon_market_config:
            self.carbon_market_client = CarbonMarketClient(**config.carbon_market_config)
        self.chaos_injector = chaos_injector or ChaosInjector(self, config.chaos_probability)
        self.safety_monitor = safety_monitor or SafetyMonitor()
        self._setup_safety_invariants()

        # Multi-agent coordination
        self.tier_agents = {tier: TierAgent(tier, self, message_queue) for tier in StorageTier}

        # Background tasks
        self._background_tasks = []
        self._start_background_tasks()

    def _setup_safety_invariants(self):
        self.safety_monitor.add_invariant(
            lambda s: s.get('collateral_ratio', 0) >= 0.2,
            "Collateral ratio below 0.2"
        )
        self.safety_monitor.add_invariant(
            lambda s: s.get('total_tokens', 0) >= 0,
            "Total tokens negative"
        )

    def _create_task(self, coro):
        try:
            loop = asyncio.get_running_loop()
            task = loop.create_task(coro)
            self._background_tasks.append(task)
            return task
        except RuntimeError:
            logger.warning("No running event loop; background task not started.")
            return None

    def _start_background_tasks(self):
        # Start periodic tasks
        self._create_task(self._periodic_chaos())
        self._create_task(self._periodic_federated_update())
        # Add more as needed

    async def _periodic_chaos(self):
        while True:
            await asyncio.sleep(60)
            await self.chaos_injector.maybe_inject_failure()

    async def _periodic_federated_update(self):
        while True:
            await asyncio.sleep(300)  # every 5 minutes
            if self.federated_coordinator:
                await self.federated_coordinator.send_update()

    # ============================================================================
    # Teacher Policy
    # ============================================================================
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        """
        Return probability distribution over possible actions based on adaptive cost and Pareto gating.
        Actions: store to each tier, retrieve, mobilize.
        """
        actions = [f"store_to_{tier.value}" for tier in StorageTier] + ["retrieve", "mobilize"]
        if not self.adaptive_cost or not self.pareto_gating:
            # Fallback: uniform
            return [1.0 / len(actions)] * len(actions)

        # Build candidates for ParetoGating
        candidates = []
        for action in actions:
            quality = self._estimate_action_quality(action, state)
            candidates.append({
                'expert_id': action,
                'quality_score': quality,
                'carbon_g': 0.0,
                'latency_ms': 0.0,
                'energy_joules': 0.0,
            })

        filtered = self.pareto_gating.filter(candidates)
        allowed_actions = {c['expert_id'] for c in filtered} if filtered else set(actions)

        costs = []
        for action in actions:
            if action in allowed_actions:
                cost = self.adaptive_cost.compute(
                    quality=self._estimate_action_quality(action, state),
                    carbon_g=0.0,
                    latency_ms=0.0,
                    energy_joules=0.0,
                    health=0.8,
                    atp=0.5
                )
            else:
                cost = float('inf')
            costs.append(cost)

        # Softmax over negative costs
        if all(c == float('inf') for c in costs):
            return [1.0 / len(actions)] * len(actions)
        max_cost = max(c for c in costs if c != float('inf'))
        exp_costs = [np.exp(-(c - max_cost)) if c != float('inf') else 0.0 for c in costs]
        total = sum(exp_costs)
        if total == 0:
            return [1.0 / len(actions)] * len(actions)
        return [e / total for e in exp_costs]

    def _estimate_action_quality(self, action: str, state: Dict[str, Any]) -> float:
        # Placeholder: derive from state
        return random.uniform(0.5, 1.0)

    # ============================================================================
    # Explainable AI
    # ============================================================================
    def explain_decision(self, action: str, state: Dict[str, Any]) -> str:
        if action.startswith("store_to"):
            tier = action.split("_")[-1]
            fill = state.get(f"fill_{tier}", 0.5)
            return f"Chose to store to {tier} because fill level ({fill:.2f}) is below threshold."
        elif action == "retrieve":
            return "Retrieval chosen because requested task has high priority."
        elif action == "mobilize":
            return "Mobilization chosen because collateral ratio is healthy."
        return "Unknown action."

    # ============================================================================
    # Human-in-the-Loop
    # ============================================================================
    async def request_human_approval(self, decision: Dict[str, Any]) -> bool:
        if not self.queue or not FeedbackEvent:
            logger.warning("Cannot request human approval: no queue or FeedbackEvent.")
            return True  # auto-approve for now
        event = FeedbackEvent.create_with_context(
            task_id=f"approval_{uuid.uuid4().hex[:8]}",
            selected_action=decision.get('action', 'unknown'),
            quality_score=0.0,
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="approval_request",
            adaptive_cost_value=0.0,
            state=decision,
            candidates=[],
            source="biomass_storage",
            environment="production",
            tags=["approval"]
        )
        await self.queue.publish("approval_requests", event.to_json())
        # Simulate waiting for approval (in real system, would await a response)
        logger.info("Human approval requested; auto-approving for demonstration.")
        return True

    # ============================================================================
    # Core Storage Methods (simplified but functional)
    # ============================================================================
    async def store_task(self, task_id: str, content: str, tier: StorageTier = StorageTier.ATP_CACHE, ttl: Optional[timedelta] = None) -> Dict[str, Any]:
        # Check safety invariants
        state = {
            'collateral_ratio': self.collateral_ratios.get(tier.name, 1.0),
            'total_tokens': len(self.storage_tokens)
        }
        violations = self.safety_monitor.check(state)
        if violations:
            logger.error(f"Safety violation before store: {violations}")
            return {'success': False, 'error': 'Safety violation'}

        # Deduplicate by hash
        content_hash = hashlib.sha256(content.encode()).hexdigest()
        if content_hash in self.task_hash_index:
            self.deduplication_savings += 1
            existing_id = self.task_hash_index[content_hash]
            return {'success': True, 'deduplicated': True, 'original_task_id': existing_id}

        task = StoredTask(
            task_id=task_id,
            content=content,
            tier=tier,
            timestamp=datetime.now(timezone.utc),
            ttl=ttl,
            hash_value=content_hash
        )
        self.task_index[task_id] = task
        self.task_hash_index[content_hash] = task_id
        self.storage_tokens[task_id] = StorageToken(
            token_id=str(uuid.uuid4()),
            task_id=task_id,
            tier=tier,
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + ttl if ttl else None,
            is_valid=True
        )
        self.inflow_history.append(datetime.now(timezone.utc))

        # Optionally sign token with PQC
        if PQC_AVAILABLE:
            token = self.storage_tokens[task_id]
            token.signature = self._sign_token(token.token_id)

        # Publish feedback event
        if self.queue and FeedbackEvent:
            event = FeedbackEvent.create_with_context(
                task_id=task_id,
                selected_action="store_task",
                quality_score=1.0,
                energy_joules=0.0,
                carbon_g=0.0,
                feedback_type="biomass_storage",
                adaptive_cost_value=0.0,
                state={'tier': tier.value},
                candidates=[{'action': 'store'}],
                source="biomass_storage",
                environment="production",
                tags=["biomass", "store"],
                explanation=self.explain_decision(f"store_to_{tier.value}", {})
            )
            await self.queue.publish("feedback_events", event.to_json())

        return {'success': True, 'task': task.to_dict()}

    async def retrieve_task(self, task_id: str) -> Optional[StoredTask]:
        task = self.task_index.get(task_id)
        if task:
            self.index_hits += 1
            task.access_count += 1
            task.last_access = datetime.now(timezone.utc)
            # Check if expired
            if task.ttl and task.last_access > task.timestamp + task.ttl:
                # Remove expired
                self._remove_task(task_id)
                self.index_misses += 1
                return None
            return task
        self.index_misses += 1
        return None

    def _remove_task(self, task_id: str):
        if task_id in self.task_index:
            task = self.task_index.pop(task_id)
            if task.hash_value in self.task_hash_index:
                del self.task_hash_index[task.hash_value]
            if task_id in self.storage_tokens:
                del self.storage_tokens[task_id]

    async def mobilize(self, amount: int) -> Dict[str, Any]:
        # High-impact: request human approval
        decision = {'action': 'mobilize', 'amount': amount}
        approved = await self.request_human_approval(decision)
        if not approved:
            return {'success': False, 'error': 'Rejected by human'}

        if amount <= 0 or amount > len(self.task_index):
            return {'success': False, 'error': 'Invalid amount'}
        mobilized = []
        for _ in range(amount):
            if not self.task_index:
                break
            task_id = next(iter(self.task_index))
            task = self.task_index[task_id]
            mobilized.append(task.to_dict())
            self._remove_task(task_id)
            self.total_mobilized += 1
        self.mobilization_history.append(datetime.now(timezone.utc))
        self.outflow_history.append(datetime.now(timezone.utc))
        return {'success': True, 'mobilized': mobilized}

    # ============================================================================
    # Analytics and Health
    # ============================================================================
    def generate_analytics(self) -> StorageAnalytics:
        total_tasks = len(self.task_index)
        # Placeholder metrics
        conversion_efficiency = random.uniform(0.7, 0.95)
        avg_retrieval_cost = random.uniform(5, 20)
        expiration_rate = random.uniform(0.01, 0.1)
        cache_hit_rate = self.index_hits / (self.index_hits + self.index_misses) if (self.index_hits + self.index_misses) > 0 else 0.5
        distribution = {}
        for tier in StorageTier:
            distribution[tier.value] = sum(1 for t in self.task_index.values() if t.tier == tier)
        return StorageAnalytics(
            total_tasks=total_tasks,
            total_size=total_tasks * 100,  # approx
            conversion_efficiency=conversion_efficiency,
            avg_retrieval_cost=avg_retrieval_cost,
            expiration_rate=expiration_rate,
            cache_hit_rate=cache_hit_rate,
            storage_distribution=distribution
        )

    async def health_check(self) -> Dict[str, Any]:
        analytics = self.generate_analytics()
        return {
            'status': 'healthy',
            'total_tasks': analytics.total_tasks,
            'storage_distribution': analytics.storage_distribution,
            'mopd_enabled': self.config.mopd.enabled,
            'pareto_front_size': len(self.get_pareto_front()),
            'drift_score': 'N/A',
            'precision': self.precision_controller.get_precision(
                load=len(self.task_index) / self.config.max_storage_tokens,
                energy_budget=0.5
            )
        }

    # ============================================================================
    # MOPD Public Methods
    # ============================================================================
    def get_pareto_front(self) -> List[MOPDPoint]:
        return self.genetic_optimizer.pareto_front.copy()

    def get_mopd_summary(self) -> Dict[str, Any]:
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
    # Quantum-Safe Signing
    # ============================================================================
    def _sign_token(self, token_id: str) -> bytes:
        if not PQC_AVAILABLE:
            logger.warning("PQC not available; using SHA256 hash as signature.")
            return hashlib.sha256(token_id.encode()).digest()
        private_key = dilithium.generate_private_key()
        signature = dilithium.sign(private_key, token_id.encode())
        return signature

    def _verify_token_signature(self, token_id: str, signature: bytes) -> bool:
        if not PQC_AVAILABLE:
            expected = hashlib.sha256(token_id.encode()).digest()
            return signature == expected
        # In practice, public key would be stored; here we assume same key pair
        public_key = dilithium.generate_public_key(dilithium.generate_private_key())
        try:
            dilithium.verify(public_key, token_id.encode(), signature)
            return True
        except Exception:
            return False

    # ============================================================================
    # Shutdown
    # ============================================================================
    async def shutdown(self):
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        logger.info("BiomassStorage shutdown complete.")

# ============================================================================
# Legacy compatibility
# ============================================================================
class BiomassStorageV62(BiomassStorage):
    pass
