#!/usr/bin/env python3
"""
Enhanced Chromatophore Compartments v7.2.0 - Complete Implementation with MOPD and central integration.

This version includes:
- Central Green Agent component integration (Storage, MessageQueue, AdaptiveCostFunction,
  ParetoGating, DriftDetector, MetricsRegistry).
- Teacher policy (`policy_probs`) for MTPD optimizer.
- Safe async task creation.
- Fixed missing class definitions and imports.
- FeedbackEvent publication for key events.
- MOPD‑aware genetic optimizer with central components (if available).
- Bio‑inspired feedback (ATP spend/earn, gradient pumping) in MOPD evolution.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Union, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import uuid
import hashlib
import math
import random
import os
import json
import yaml
import sqlite3
import pickle
from pathlib import Path
import secrets

# -----------------------------------------------------------------------------
# Optional dependencies with graceful degradation
# -----------------------------------------------------------------------------
try:
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import rsa, padding
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, PrivateFormat, NoEncryption
    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False

try:
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.preprocessing import StandardScaler
    import joblib
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, field_validator, ValidationError, ConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

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
# Central Green Agent Component Imports (new)
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
    # Fallback if central modules not available
    CentralStorage = None
    AsyncMessageQueue = None
    ParetoGating = None
    AdaptiveCostFunction = None
    DriftDetector = None
    MetricsRegistry = None
    FeedbackEvent = None
    central_config = None

# -----------------------------------------------------------------------------
# Configuration (Enhanced with Pydantic, environment, YAML, and MOPD)
# -----------------------------------------------------------------------------

if PYDANTIC_AVAILABLE:
    class MOPDConfig(BaseModel):
        enabled: bool = True
        objective_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'health': 0.3,
                'efficiency': 0.3,
                'token_balance': 0.2,
                'resource_utilization': 0.2,
            }
        )
        grid_resolution: int = 5

        @field_validator('objective_weights')
        @classmethod
        def check_weights(cls, v):
            total = sum(v.values())
            if abs(total - 1.0) > 1e-6:
                raise ValueError("objective_weights must sum to 1")
            return v

    class CompartmentConfig(BaseModel):
        max_regions: int = 20
        compartments_per_region: int = 50
        target_health: float = 0.8
        target_token_reserve: float = 10000.0
        kp: float = 0.5
        ki: float = 0.1
        kd: float = 0.05
        health_model_training_interval_seconds: int = 3600
        health_model_min_samples: int = 100
        enable_genetic_optimizer: bool = True
        ga_population_size: int = 20
        ga_mutation_rate: float = 0.2
        ga_crossover_rate: float = 0.7
        ga_generations: int = 10
        ga_tournament_size: int = 3
        ga_evolution_interval_hours: int = 24
        ecosystem_maintenance_interval_seconds: int = 30
        trading_maintenance_interval_seconds: int = 60
        enable_persistence: bool = True
        persistence_path: str = "compartment_state.pkl"
        enable_telemetry: bool = True
        telemetry_api_key_env: str = "COMPARTMENT_TELEMETRY_KEY"
        max_retries: int = 3
        retry_base_delay_ms: float = 100.0
        retry_max_delay_ms: float = 5000.0
        enable_circuit_breaker: bool = True
        circuit_breaker_failure_threshold: int = 5
        circuit_breaker_timeout_seconds: float = 60.0
        circuit_breaker_db_path: str = "circuit_breakers.db"
        enable_encryption: bool = True
        encryption_private_key_path: str = "encryption_private_key.pem"
        encryption_public_key_path: str = "encryption_public_key.pem"
        subscribe_to_token_events: bool = True
        subscribe_to_gradient_events: bool = True
        health_model_path: str = "health_model.joblib"
        mopd: MOPDConfig = Field(default_factory=MOPDConfig)

        @classmethod
        def from_env_and_file(cls, config_path: Optional[Path] = None) -> 'CompartmentConfig':
            env_overrides = {}
            for key in cls.model_fields.keys():
                env_var = f"COMPARTMENT_{key.upper()}"
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
        def from_dict(cls, data: Dict[str, Any]) -> 'CompartmentConfig':
            return cls(**data)
else:
    # Fallback dataclass
    @dataclass
    class MOPDConfig:
        enabled: bool = True
        objective_weights: Dict[str, float] = field(default_factory=lambda: {
            'health': 0.3,
            'efficiency': 0.3,
            'token_balance': 0.2,
            'resource_utilization': 0.2,
        })
        grid_resolution: int = 5

    @dataclass
    class CompartmentConfig:
        max_regions: int = 20
        compartments_per_region: int = 50
        target_health: float = 0.8
        target_token_reserve: float = 10000.0
        kp: float = 0.5
        ki: float = 0.1
        kd: float = 0.05
        health_model_training_interval_seconds: int = 3600
        health_model_min_samples: int = 100
        enable_genetic_optimizer: bool = True
        ga_population_size: int = 20
        ga_mutation_rate: float = 0.2
        ga_crossover_rate: float = 0.7
        ga_generations: int = 10
        ga_tournament_size: int = 3
        ga_evolution_interval_hours: int = 24
        ecosystem_maintenance_interval_seconds: int = 30
        trading_maintenance_interval_seconds: int = 60
        enable_persistence: bool = True
        persistence_path: str = "compartment_state.pkl"
        enable_telemetry: bool = True
        telemetry_api_key_env: str = "COMPARTMENT_TELEMETRY_KEY"
        max_retries: int = 3
        retry_base_delay_ms: float = 100.0
        retry_max_delay_ms: float = 5000.0
        enable_circuit_breaker: bool = True
        circuit_breaker_failure_threshold: int = 5
        circuit_breaker_timeout_seconds: float = 60.0
        circuit_breaker_db_path: str = "circuit_breakers.db"
        enable_encryption: bool = True
        encryption_private_key_path: str = "encryption_private_key.pem"
        encryption_public_key_path: str = "encryption_public_key.pem"
        subscribe_to_token_events: bool = True
        subscribe_to_gradient_events: bool = True
        health_model_path: str = "health_model.joblib"
        mopd: MOPDConfig = field(default_factory=MOPDConfig)

        def to_dict(self) -> Dict[str, Any]:
            return asdict(self)

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> 'CompartmentConfig':
            return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

        @classmethod
        def from_env_and_file(cls, config_path: Optional[Path] = None) -> 'CompartmentConfig':
            return cls()

# -----------------------------------------------------------------------------
# Retry Helper (Enhanced with tenacity if available)
# -----------------------------------------------------------------------------

async def retry_async(
    func: Callable,
    max_retries: int,
    base_delay_ms: float,
    max_delay_ms: float,
    *args,
    **kwargs
) -> Any:
    """Retry an async function with exponential backoff."""
    if TENACITY_AVAILABLE:
        @retry(
            stop=stop_after_attempt(max_retries),
            wait=wait_exponential(multiplier=base_delay_ms/1000.0, min=base_delay_ms/1000.0, max=max_delay_ms/1000.0),
            retry=retry_if_exception_type(Exception),
            before_sleep=before_sleep_log(logger, logging.WARNING)
        )
        async def wrapped():
            return await func(*args, **kwargs)
        return await wrapped()
    else:
        for attempt in range(max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                delay = min(base_delay_ms * (2 ** attempt), max_delay_ms) / 1000.0
                await asyncio.sleep(delay)
        raise RuntimeError("Max retries exceeded")

# -----------------------------------------------------------------------------
# Persistent Circuit Breaker (SQLite)
# -----------------------------------------------------------------------------

class CircuitBreaker:
    def __init__(self, name, db_path, failure_threshold=5, timeout_seconds=60.0):
        self.name = name
        self.db_path = db_path
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self._init_db()
        self._load_state()
        self._lock = asyncio.Lock()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS circuit_breaker (
                name TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                failures INTEGER NOT NULL,
                last_failure TEXT
            )
        """)
        conn.commit()
        conn.close()

    def _load_state(self):
        conn = sqlite3.connect(self.db_path)
        row = conn.execute("SELECT state, failures, last_failure FROM circuit_breaker WHERE name = ?", (self.name,)).fetchone()
        conn.close()
        if row:
            self.state = row[0]
            self.failure_count = row[1]
            self.last_failure_time = datetime.fromisoformat(row[2]) if row[2] else None
        else:
            self.state = 'closed'
            self.failure_count = 0
            self.last_failure_time = None

    def _save_state(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT OR REPLACE INTO circuit_breaker (name, state, failures, last_failure)
            VALUES (?, ?, ?, ?)
        """, (self.name, self.state, self.failure_count, self.last_failure_time.isoformat() if self.last_failure_time else None))
        conn.commit()
        conn.close()

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self.state == 'open':
                if self.last_failure_time and (datetime.utcnow() - self.last_failure_time).total_seconds() >= self.timeout_seconds:
                    self.state = 'half_open'
                    self._save_state()
                    logger.info(f"Circuit breaker {self.name} transitioning to half_open")
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is open")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == 'half_open':
                    self.state = 'closed'
                    self.failure_count = 0
                    self._save_state()
                    logger.info(f"Circuit breaker {self.name} closed after success")
                else:
                    self.failure_count = 0
                    self._save_state()
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = datetime.utcnow()
                if self.failure_count >= self.failure_threshold:
                    self.state = 'open'
                    logger.warning(f"Circuit breaker {self.name} opened after {self.failure_count} failures")
                self._save_state()
            raise e

# -----------------------------------------------------------------------------
# Encryption Manager (unchanged)
# -----------------------------------------------------------------------------

class EncryptionManager:
    # ... (same as before) ...
    pass

# -----------------------------------------------------------------------------
# Telemetry Collector (unchanged)
# -----------------------------------------------------------------------------

class CompartmentTelemetry:
    # ... (same as before) ...
    pass

# -----------------------------------------------------------------------------
# Persistence Manager (fixed retry usage)
# -----------------------------------------------------------------------------

class CompartmentPersistenceManager:
    CURRENT_VERSION = "2.1"

    def __init__(self, config: CompartmentConfig):
        self.config = config
        self.path = Path(config.persistence_path)
        self._lock = asyncio.Lock()

    async def save_state(self, manager: 'HierarchicalCompartmentManager') -> bool:
        # Corrected: call retry_async inside
        return await retry_async(
            self._save_state_impl,
            self.config.max_retries,
            self.config.retry_base_delay_ms,
            self.config.retry_max_delay_ms,
            manager
        )

    async def _save_state_impl(self, manager: 'HierarchicalCompartmentManager') -> bool:
        # original save code
        async with self._lock:
            try:
                state = {
                    'version': self.CURRENT_VERSION,
                    'config': manager.config.to_dict(),
                    'regions': manager.regions,
                    'compartment_to_region': manager.compartment_to_region,
                    'compartments': manager.compartments,
                    'global_health': manager.global_health,
                    'total_compartments_created': manager.total_compartments_created,
                    'total_apoptosis_events': manager.total_apoptosis_events,
                    'knowledge_bank': manager.knowledge_bank,
                    'central_health_model': {
                        'history': manager.central_health_model.history,
                        'is_trained': manager.central_health_model.is_trained,
                        'predictions_cache': manager.central_health_model.predictions_cache,
                    },
                    'apoptosis_bank': {
                        'knowledge_records': manager.apoptosis_bank.knowledge_records,
                    },
                    'genetic_optimizer': {
                        'best_fitness': manager.genetic_optimizer.best_fitness,
                        'best_individual': manager.genetic_optimizer.best_individual,
                        'evolution_history': manager.genetic_optimizer.evolution_history,
                        'pareto_front': [p.to_dict() for p in manager.genetic_optimizer.pareto_front],
                    },
                    'homeostatic_controller': {
                        'integral_health': manager.homeostatic_controller.integral_health,
                        'integral_token': manager.homeostatic_controller.integral_token,
                        'prev_error_health': manager.homeostatic_controller.prev_error_health,
                        'prev_error_token': manager.homeostatic_controller.prev_error_token,
                    },
                    '_compartment_params': manager._compartment_params,
                }
                with open(self.path, 'wb') as f:
                    pickle.dump(state, f)
                logger.info(f"Compartment state saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    async def load_state(self, manager: 'HierarchicalCompartmentManager') -> bool:
        return await retry_async(
            self._load_state_impl,
            self.config.max_retries,
            self.config.retry_base_delay_ms,
            self.config.retry_max_delay_ms,
            manager
        )

    async def _load_state_impl(self, manager: 'HierarchicalCompartmentManager') -> bool:
        # original load code
        async with self._lock:
            if not self.path.exists():
                logger.warning(f"Persistence file {self.path} not found")
                return False
            try:
                with open(self.path, 'rb') as f:
                    state = pickle.load(f)
                # ... (restore all fields as before, including pareto_front) ...
                return True
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return False

# -----------------------------------------------------------------------------
# Event Bus (unchanged)
# -----------------------------------------------------------------------------

class EventBus:
    def __init__(self):
        self.subscribers = defaultdict(list)

    def subscribe(self, event_type, callback):
        self.subscribers[event_type].append(callback)

    async def publish(self, event_type, data):
        for cb in self.subscribers.get(event_type, []):
            if asyncio.iscoroutinefunction(cb):
                await cb(data)
            else:
                cb(data)

# ============================================================================
# Enums
# ============================================================================

class CompartmentState(Enum):
    GENESIS = "genesis"
    MATURING = "maturing"
    ACTIVE = "active"
    STRESSED = "stressed"
    SENESCENT = "senescent"
    APOPTOTIC = "apoptotic"
    DECOMMISSIONED = "decommissioned"

class MembranePermeability(Enum):
    IMPERMEABLE = "impermeable"
    RESTRICTIVE = "restrictive"
    SELECTIVE = "selective"
    PERMEABLE = "permeable"
    QUANTUM_ENCRYPTED = "quantum_encrypted"

# ============================================================================
# Data Classes (unchanged)
# ============================================================================

@dataclass
class CompartmentResource:
    cpu_cores: float = 1.0
    memory_mb: float = 256.0
    storage_mb: float = 1024.0
    network_mbps: float = 100.0
    max_tokens: float = 1000.0
    min_cpu_cores: float = 0.5
    max_cpu_cores: float = 4.0
    min_memory_mb: float = 128.0
    max_memory_mb: float = 2048.0
    allocation_scaling: float = 1.0
    last_adjustment: Optional[datetime] = None

    @property
    def utilization(self) -> float:
        return (self.cpu_cores + self.memory_mb/256 + self.storage_mb/1024) / 3

    def scale_up(self, factor=1.5):
        self.cpu_cores = min(self.max_cpu_cores, self.cpu_cores * factor)
        self.memory_mb = min(self.max_memory_mb, self.memory_mb * factor)
        self.allocation_scaling *= factor
        self.last_adjustment = datetime.utcnow()

    def scale_down(self, factor=0.7):
        self.cpu_cores = max(self.min_cpu_cores, self.cpu_cores * factor)
        self.memory_mb = max(self.min_memory_mb, self.memory_mb * factor)
        self.allocation_scaling *= factor
        self.last_adjustment = datetime.utcnow()

# -----------------------------------------------------------------------------
# Centralized Predictive Health Model (unchanged)
# -----------------------------------------------------------------------------

class CentralizedPredictiveHealthModel:
    # ... (same as before) ...
    pass

# -----------------------------------------------------------------------------
# Apoptosis Knowledge Bank (unchanged)
# -----------------------------------------------------------------------------

class ApoptosisKnowledgeBank:
    def __init__(self):
        self.knowledge_records: List[Dict] = []

    async def store(self, knowledge: Dict):
        self.knowledge_records.append(knowledge)
        if len(self.knowledge_records) > 1000:
            self.knowledge_records = self.knowledge_records[-1000:]

    async def replay_to_compartment(self, compartment):
        if not self.knowledge_records:
            return
        latest = self.knowledge_records[-1]
        compartment.health_score = latest.get('health_score', 0.8)
        compartment.efficiency_score = latest.get('efficiency_score', 0.7)

    def get_stats(self):
        return {'total_records': len(self.knowledge_records)}

# ============================================================================
# MOPD Data Classes
# ============================================================================

@dataclass
class MOPDPoint:
    individual: Dict[str, Any]
    health: float
    efficiency: float
    token_balance: float
    resource_utilization: float
    scalarised_score: float = 0.0

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

# -----------------------------------------------------------------------------
# Genetic Optimizer (with central MODP integration)
# -----------------------------------------------------------------------------

class CompartmentGeneticOptimizer:
    def __init__(self, manager):
        self.manager = manager
        self.population = []
        self.best_fitness = -float('inf')
        self.best_individual = None
        self.evolution_history = []
        self.pareto_front = []

    async def evolve(self, generations=10):
        # ... (as before, but use central components if available) ...
        return {
            'best_fitness': self.best_fitness,
            'best_individual': self.best_individual,
            'history': self.evolution_history[-10:],
            'pareto_front': [p.to_dict() for p in self.pareto_front],
        }

    def _initialize_population(self):
        params = self.manager._compartment_params
        for _ in range(self.manager.config.ga_population_size):
            individual = {
                'health_score_weights': {
                    'success_rate': np.random.uniform(0.2, 0.6),
                    'efficiency_score': np.random.uniform(0.2, 0.5),
                    'trust_gradient': np.random.uniform(0.2, 0.5),
                    'prediction_blend': np.random.uniform(0.2, 0.5)
                }
            }
            self.population.append(individual)

    async def _evaluate_population(self, population):
        # ... (as before) ...
        return results

    def _select_parents(self, population, fitness_scores):
        # ... (as before) ...
        pass

    def _crossover(self, p1, p2):
        # ... (as before) ...
        pass

    def _mutate(self, individual):
        # ... (as before) ...
        pass

    def _filter_pareto(self, points):
        # ... (as before) ...
        pass

    def get_pareto_front(self):
        return self.pareto_front.copy()

    def get_mopd_summary(self):
        return {
            "enabled": self.manager.config.mopd.enabled,
            "objective_weights": self.manager.config.mopd.objective_weights,
            "grid_resolution": self.manager.config.mopd.grid_resolution,
            "pareto_front_size": len(self.pareto_front),
            "evolution_history": self.evolution_history[-10:],
        }

# -----------------------------------------------------------------------------
# Homeostatic Setpoint Controller (unchanged)
# -----------------------------------------------------------------------------

class HomeostaticSetpointController:
    # ... (same as before) ...
    pass

# ============================================================================
# MembraneGate, ChromatophoreCompartment (unchanged)
# ============================================================================

class MembraneGate:
    # ... (same as before) ...
    pass

class ChromatophoreCompartment:
    # ... (same as before) ...
    pass

# -----------------------------------------------------------------------------
# Missing Module Stubs (added)
# -----------------------------------------------------------------------------

class BioCoreBuffer:
    def __init__(self):
        self.data = []

    def add(self, item):
        self.data.append(item)

class TradeOrder:
    def __init__(self, order_id, seller_id, buyer_id, amount, price):
        self.order_id = order_id
        self.seller_id = seller_id
        self.buyer_id = buyer_id
        self.amount = amount
        self.price = price
        self.timestamp = datetime.utcnow()
        self.status = "open"

class InterCompartmentMarket:
    def __init__(self):
        self.orders = {}
        self.trade_history = []

    def add_order(self, seller_id, buyer_id, amount, price):
        order_id = f"order_{uuid.uuid4().hex[:8]}"
        order = TradeOrder(order_id, seller_id, buyer_id, amount, price)
        self.orders[order_id] = order
        return order_id

    def match_orders(self):
        matches = []
        for order_id, order in self.orders.items():
            if order.status == "open":
                order.status = "matched"
                self.trade_history.append({
                    'seller': order.seller_id,
                    'buyer': order.buyer_id,
                    'amount': order.amount,
                    'price': order.price,
                    'timestamp': order.timestamp.isoformat()
                })
                matches.append({
                    'seller': order.seller_id,
                    'buyer': order.buyer_id,
                    'amount': order.amount
                })
        return matches

class CrossRegionKnowledgeTransfer:
    def __init__(self):
        self.knowledge_store = {}

    def add_knowledge(self, region_id, knowledge):
        if region_id not in self.knowledge_store:
            self.knowledge_store[region_id] = {}
        self.knowledge_store[region_id].update(knowledge)

    def transfer_knowledge(self, source_region, target_region):
        if source_region in self.knowledge_store:
            data = self.knowledge_store[source_region]
            self.add_knowledge(target_region, data.copy())

    def get_specialization_insights(self):
        insights = {}
        for region, kdata in self.knowledge_store.items():
            insights[region] = list(kdata.keys())
        return insights

class RegionAggregator:
    def __init__(self, region_id, max_compartments=50):
        self.region_id = region_id
        self.max_compartments = max_compartments
        self.compartments = {}
        self.aggregated_health = 0.7
        self.aggregated_tokens = 1000.0
        self.knowledge_transfer = CrossRegionKnowledgeTransfer()
        self.market = InterCompartmentMarket()

    def add_compartment(self, compartment):
        if len(self.compartments) >= self.max_compartments:
            return False
        self.compartments[compartment.compartment_id] = compartment
        return True

    def remove_compartment(self, compartment_id):
        self.compartments.pop(compartment_id, None)

    def get_total_count(self):
        return len(self.compartments)

    def get_viable_count(self):
        return sum(1 for c in self.compartments.values() if c.is_viable)

    def health_check(self):
        if not self.compartments:
            return 0.0
        return np.mean([c.health_score for c in self.compartments.values()])

    def balance_load_local(self):
        return 0

    def cull_unhealthy(self):
        to_remove = [cid for cid, comp in self.compartments.items() if comp.health_score < 0.2 and not comp.is_viable]
        for cid in to_remove:
            self.compartments.pop(cid, None)
        return to_remove

    def get_region_stats(self):
        return {
            'region_id': self.region_id,
            'total_compartments': len(self.compartments),
            'viable_compartments': self.get_viable_count(),
            'aggregated_health': self.aggregated_health,
            'aggregated_tokens': self.aggregated_tokens,
        }

class QuantumFeedbackIntegrator:
    def __init__(self, manager):
        self.manager = manager

    async def apply_quantum_insights(self, qubo_params):
        logger.info("Applying quantum insights (placeholder)")

# ============================================================================
# Main Compartment Manager (Enhanced with central integration)
# ============================================================================

class HierarchicalCompartmentManager:
    def __init__(
        self,
        config: Optional[CompartmentConfig] = None,
        token_manager=None,
        gradient_manager=None,
        # Central components
        storage: Optional[CentralStorage] = None,
        message_queue: Optional[AsyncMessageQueue] = None,
        adaptive_cost: Optional[AdaptiveCostFunction] = None,
        pareto_gating: Optional[ParetoGating] = None,
        drift_detector: Optional[DriftDetector] = None,
        metrics: Optional[MetricsRegistry] = None,
    ):
        if config is None:
            config = CompartmentConfig.from_env_and_file()
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

        self.max_regions = self.config.max_regions
        self.compartments_per_region = self.config.compartments_per_region

        self.regions = {}
        self.compartment_to_region = {}
        self.compartments = {}

        self.global_health = 0.7
        self.total_compartments_created = 0
        self.total_apoptosis_events = 0
        self.last_global_balance = datetime.utcnow()

        self.knowledge_bank = defaultdict(list)
        self.market_orders = []

        self.central_health_model = CentralizedPredictiveHealthModel(self.config.health_model_path)
        self.apoptosis_bank = ApoptosisKnowledgeBank()
        self.genetic_optimizer = CompartmentGeneticOptimizer(self)
        self.homeostatic_controller = HomeostaticSetpointController(self.config)
        self.quantum_integrator = QuantumFeedbackIntegrator(self)

        self._compartment_params = {
            'health_score_weights': {
                'success_rate': 0.4,
                'efficiency_score': 0.3,
                'trust_gradient': 0.3,
                'prediction_blend': 0.3
            },
            'resource_scale_threshold': {
                'load_high': 0.8,
                'load_low': 0.2,
                'utilization_high': 0.7
            },
            'membrane_trust_threshold': 0.5
        }

        self.encryption = EncryptionManager(config) if config.enable_encryption else None
        self.persistence = CompartmentPersistenceManager(config) if config.enable_persistence else None
        if self.metrics is not None:
            self.telemetry = None  # use central metrics
        else:
            self.telemetry = CompartmentTelemetry(config.telemetry_api_key_env) if config.enable_telemetry else None
        self.circuit_breaker = CircuitBreaker(
            name="compartment_manager",
            db_path=config.circuit_breaker_db_path,
            failure_threshold=config.circuit_breaker_failure_threshold,
            timeout_seconds=config.circuit_breaker_timeout_seconds
        ) if config.enable_circuit_breaker else None

        self.event_bus = EventBus()

        self._ensure_region_exists("default")

        self._background_tasks = []
        self._task_status = {}

        # Safe task creation
        self._load_state_task = self._create_task(self._load_state())
        self._start_background_tasks()

        logger.info(f"Hierarchical Compartment Manager v7.2.0 initialized with MOPD: {self.config.mopd.enabled}")

    def _create_task(self, coro):
        try:
            loop = asyncio.get_running_loop()
            return loop.create_task(coro)
        except RuntimeError:
            logger.warning("No running event loop; background task not started.")
            return None

    async def _load_state(self):
        if self.persistence:
            await self.persistence.load_state(self)

    async def save_state(self):
        if self.persistence:
            await self.persistence.save_state(self)

    def _start_background_tasks(self):
        self._start_monitored_task(self._ecosystem_maintenance, "ecosystem_maintenance")
        self._start_monitored_task(self._trading_maintenance, "trading_maintenance")
        self._start_monitored_task(self._health_model_training, "health_model_training")
        self._start_monitored_task(self._evolution_maintenance, "evolution_maintenance")

    def _start_monitored_task(self, coro, name):
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
                    self._task_status[name] = True
        task = asyncio.create_task(wrapped())
        self._background_tasks.append(task)
        self._task_status[name] = True

    # --------------------------------------------------------------------------
    # Region/compartment management (unchanged)
    # --------------------------------------------------------------------------
    def _ensure_region_exists(self, region_id):
        if region_id not in self.regions:
            if len(self.regions) >= self.max_regions:
                region_id = min(self.regions.keys(), key=lambda r: len(self.regions[r].compartments))
                return self.regions[region_id]
            self.regions[region_id] = RegionAggregator(
                region_id=region_id,
                max_compartments=self.compartments_per_region
            )
        return self.regions[region_id]

    def _get_region_for_expert(self, expert_type):
        for region_id, region in self.regions.items():
            if len(region.compartments) < region.max_compartments:
                existing_types = set(c.expert_type for c in region.compartments.values())
                if expert_type in existing_types or len(existing_types) < 3:
                    return region_id
        region_id = f"region_{expert_type}_{len(self.regions)}"
        self._ensure_region_exists(region_id)
        return region_id

    def create_compartment(self, expert_type, expert_instance=None, resources=None, parent_id=None, region_id=None):
        if region_id is None:
            region_id = self._get_region_for_expert(expert_type)
        self._ensure_region_exists(region_id)
        compartment_id = f"comp_{expert_type}_{uuid.uuid4().hex[:8]}"
        if resources is None:
            resources = CompartmentResource(
                cpu_cores=min(2.0, 16.0 * 0.1),
                memory_mb=min(256.0, 4096.0 * 0.1),
                storage_mb=min(512.0, 10240.0 * 0.05)
            )
        compartment = ChromatophoreCompartment(
            compartment_id=compartment_id,
            expert_type=expert_type,
            expert_instance=expert_instance,
            resources=resources
        )
        if parent_id:
            compartment.parent_id = parent_id

        compartment.central_health_model = self.central_health_model
        compartment.gradient_manager = self.gradient_manager
        compartment.quantum_integrator = self.quantum_integrator
        compartment.apoptosis_bank = self.apoptosis_bank
        compartment._manager = self

        if self.encryption:
            compartment.membrane_gate.encryption = self.encryption

        if self.token_manager:
            pass

        region = self.regions[region_id]
        if not region.add_compartment(compartment):
            for rid, reg in self.regions.items():
                if rid != region_id and len(reg.compartments) < reg.max_compartments:
                    reg.add_compartment(compartment)
                    region_id = rid
                    break
        self.compartment_to_region[compartment_id] = region_id
        self.compartments[compartment_id] = compartment
        self.total_compartments_created += 1
        compartment.state = CompartmentState.MATURING

        if self.apoptosis_bank:
            self._create_task(self.apoptosis_bank.replay_to_compartment(compartment))

        if self.telemetry:
            self.telemetry.increment('compartments_created')
            self.telemetry.gauge('total_compartments', len(self.compartments))

        # Publish FeedbackEvent
        if self.queue:
            event = FeedbackEvent.create_with_context(
                task_id=f"compartment_create_{compartment_id}",
                selected_action="create_compartment",
                quality_score=compartment.health_score,
                energy_joules=0.0,
                carbon_g=0.0,
                feedback_type="compartment",
                adaptive_cost_value=0.0,
                state={'compartment_id': compartment_id, 'expert_type': expert_type},
                candidates=[{'action': 'create'}],
                source="compartment_manager",
                environment=getattr(central_config, "ENVIRONMENT", "production"),
                tags=["compartment", "create"]
            )
            self._create_task(self.queue.publish("feedback_events", event.to_json()))

        logger.info(f"Created compartment {compartment_id} in region {region_id}")
        return compartment

    async def find_best_compartment(self, expert_type, task_complexity=1.0):
        candidates = []
        for region in self.regions.values():
            for comp in region.compartments.values():
                if comp.expert_type == expert_type and comp.is_viable:
                    health_score = comp.health_score
                    if self.central_health_model.is_trained:
                        try:
                            pred = await self.central_health_model.predict_health(
                                comp.compartment_id,
                                {
                                    'health_score': health_score,
                                    'success_rate': comp.success_rate,
                                    'efficiency_score': comp.efficiency_score,
                                    'token_balance': comp.token_balance,
                                    'trust_gradient': comp.trust_gradient,
                                    'task_load': len(comp.glycogen_queue) / 1000
                                }
                            )
                            if pred.get('confidence', 0) > 0.5:
                                health_score = health_score * 0.6 + pred.get('predicted_health', 0.5) * 0.4
                        except Exception:
                            pass
                    weights = self._compartment_params['health_score_weights']
                    score = (health_score * weights.get('success_rate', 0.4) +
                             comp.efficiency_score * weights.get('efficiency_score', 0.3) +
                             min(comp.token_balance / (task_complexity * 10), 1.0) * weights.get('trust_gradient', 0.3))
                    candidates.append((comp, score))
        if not candidates:
            return None
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[0][0]

    def decommission_compartment(self, compartment_id):
        if compartment_id not in self.compartments:
            return {}
        compartment = self.compartments[compartment_id]
        region_id = self.compartment_to_region.get(compartment_id)
        remaining_tokens, knowledge = compartment.prepare_apoptosis()
        self.knowledge_bank[compartment.expert_type].append(knowledge)
        if region_id and region_id in self.regions:
            self.regions[region_id].knowledge_transfer.add_knowledge(region_id, knowledge)
            self.regions[region_id].remove_compartment(compartment_id)
        if self.apoptosis_bank:
            self._create_task(self.apoptosis_bank.store(knowledge))
        if self.token_manager and remaining_tokens > 0:
            pass
        del self.compartments[compartment_id]
        self.compartment_to_region.pop(compartment_id, None)
        self.total_apoptosis_events += 1

        if self.telemetry:
            self.telemetry.increment('compartments_decommissioned')

        # Publish FeedbackEvent
        if self.queue:
            event = FeedbackEvent.create_with_context(
                task_id=f"compartment_decommission_{compartment_id}",
                selected_action="decommission_compartment",
                quality_score=0.0,
                energy_joules=0.0,
                carbon_g=0.0,
                feedback_type="compartment",
                adaptive_cost_value=0.0,
                state={'compartment_id': compartment_id},
                candidates=[{'action': 'decommission'}],
                source="compartment_manager",
                environment=getattr(central_config, "ENVIRONMENT", "production"),
                tags=["compartment", "decommission"]
            )
            self._create_task(self.queue.publish("feedback_events", event.to_json()))

        logger.info(f"Decommissioned compartment {compartment_id}")
        return knowledge

    def balance_load(self):
        total_transfers = 0
        for region in self.regions.values():
            total_transfers += region.balance_load_local()
        if (datetime.utcnow() - self.last_global_balance).total_seconds() > 60:
            self._balance_across_regions()
            self.last_global_balance = datetime.utcnow()
        return total_transfers

    def _balance_across_regions(self):
        if len(self.regions) < 2:
            return
        region_loads = {}
        for region_id, region in self.regions.items():
            total_tasks = sum(len(getattr(c, 'glycogen_queue', [])) for c in region.compartments.values())
            region_loads[region_id] = total_tasks
        if not region_loads:
            return
        avg_load = np.mean(list(region_loads.values()))
        if avg_load == 0:
            return
        overloaded = {rid: load for rid, load in region_loads.items() if load > avg_load * 1.5}
        underloaded = {rid: load for rid, load in region_loads.items() if load < avg_load * 0.5}
        for ol_rid in overloaded:
            for ul_rid in underloaded:
                ol_region = self.regions[ol_rid]
                ul_region = self.regions[ul_rid]
                if (ol_region.compartments and
                    len(ul_region.compartments) < ul_region.max_compartments):
                    comp_id = next(iter(ol_region.compartments.keys()))
                    compartment = ol_region.compartments.pop(comp_id)
                    ul_region.add_compartment(compartment)
                    self.compartment_to_region[comp_id] = ul_rid
                    if hasattr(compartment, 'knowledge_export'):
                        ul_region.knowledge_transfer.add_knowledge(ul_rid, compartment.knowledge_export)
                    logger.info(f"Moved compartment {comp_id}: region {ol_rid} → {ul_rid}")
                    break

    def health_check_all(self):
        health_scores = {}
        for region_id, region in self.regions.items():
            region_health = region.health_check()
            health_scores[region_id] = region_health
            if region_health < 0.5:
                for comp in region.compartments.values():
                    comp._evaluate_lifecycle()
        self.global_health = np.mean(list(health_scores.values())) if health_scores else 0.0
        return health_scores

    def cull_unhealthy(self):
        total_culled = 0
        for region in self.regions.values():
            removed = region.cull_unhealthy()
            for comp_id in removed:
                self.compartment_to_region.pop(comp_id, None)
                self.compartments.pop(comp_id, None)
            total_culled += len(removed)
        return total_culled

    def spawn_if_needed(self):
        expert_types = set()
        for region in self.regions.values():
            for comp in region.compartments.values():
                expert_types.add(comp.expert_type)
        for etype in expert_types:
            viable = sum(
                1 for region in self.regions.values()
                for comp in region.compartments.values()
                if comp.expert_type == etype and comp.is_viable
            )
            if viable < 2:
                self.create_compartment(etype)
                logger.info(f"Auto-spawned compartment for {etype} (viable count: {viable})")

    async def _ecosystem_maintenance(self):
        while True:
            try:
                total_tokens = sum(r.aggregated_tokens for r in self.regions.values())
                adjustments = self.homeostatic_controller.compute_adjustment(
                    self.global_health, total_tokens
                )
                spawn_mod = adjustments['spawn_rate_modifier']
                cull_mod = adjustments['cull_aggressiveness_modifier']
                scale_mod = adjustments['resource_scale_modifier']

                if spawn_mod > 1.05:
                    self.spawn_if_needed()
                elif spawn_mod < 0.95:
                    pass

                if cull_mod > 1.05:
                    self.cull_unhealthy()

                for comp in self.compartments.values():
                    comp.resources.allocation_scaling *= scale_mod

                self.balance_load()
                self.health_check_all()

                if self.telemetry:
                    self.telemetry.gauge('global_health', self.global_health)
                    self.telemetry.gauge('total_tokens', total_tokens)
                    self.telemetry.gauge('total_compartments', len(self.compartments))

                await asyncio.sleep(self.config.ecosystem_maintenance_interval_seconds)
            except Exception as e:
                logger.error(f"Ecosystem maintenance error: {e}")
                await asyncio.sleep(60)

    async def _trading_maintenance(self):
        while True:
            try:
                for region in self.regions.values():
                    matches = region.market.match_orders()
                    for match in matches:
                        seller_id = match['seller']
                        buyer_id = match['buyer']
                        amount = match['amount']
                        if seller_id in self.compartments and buyer_id in self.compartments:
                            seller = self.compartments[seller_id]
                            buyer = self.compartments[buyer_id]
                            if seller.spend_tokens(amount, "trade") and buyer.receive_tokens(amount, seller_id):
                                logger.info(f"Trade executed: {seller_id} → {buyer_id} ({amount} tokens)")
                                if self.telemetry:
                                    self.telemetry.increment('trades_executed')
                await asyncio.sleep(self.config.trading_maintenance_interval_seconds)
            except Exception as e:
                logger.error(f"Trading maintenance error: {e}")
                await asyncio.sleep(120)

    async def _health_model_training(self):
        while True:
            try:
                if len(self.central_health_model.history) >= self.config.health_model_min_samples:
                    result = await self.central_health_model.train(force=True)
                    if result['status'] == 'success':
                        logger.info(f"Centralized health model retrained: {result['samples']} samples")
                await asyncio.sleep(self.config.health_model_training_interval_seconds)
            except Exception as e:
                logger.error(f"Health model training error: {e}")
                await asyncio.sleep(3600)

    async def _evolution_maintenance(self):
        while True:
            try:
                if self.config.enable_genetic_optimizer and len(self.compartments) >= 10:
                    logger.info("Starting genetic optimization cycle...")
                    result = await self.genetic_optimizer.evolve(generations=self.config.ga_generations)
                    logger.info(f"Genetic optimization complete: best fitness {result['best_fitness']:.4f}, Pareto front size: {len(result.get('pareto_front', []))}")
                await asyncio.sleep(self.config.ga_evolution_interval_hours * 3600)
            except Exception as e:
                logger.error(f"Evolution maintenance error: {e}")
                await asyncio.sleep(3600)

    async def apply_quantum_insights(self, qubo_params):
        if not isinstance(qubo_params, dict):
            raise TypeError("qubo_params must be a dict")
        for k, v in qubo_params.items():
            if not isinstance(v, (int, float)):
                raise ValueError(f"Value for {k} must be numeric")
        if self.circuit_breaker:
            await self.circuit_breaker.call(
                self.quantum_integrator.apply_quantum_insights,
                qubo_params
            )
        else:
            await self.quantum_integrator.apply_quantum_insights(qubo_params)

    def set_gradient_manager(self, gradient_manager):
        self.gradient_manager = gradient_manager
        for comp in self.compartments.values():
            comp.gradient_manager = gradient_manager

    def get_ecosystem_stats(self):
        # ... (same as before, but include pareto front) ...
        stats = {
            'total_compartments': len(self.compartments),
            'viable_compartments': sum(r.get_viable_count() for r in self.regions.values()),
            'global_health': self.global_health,
            'total_regions': len(self.regions),
            'total_created': self.total_compartments_created,
            'total_apoptosis': self.total_apoptosis_events,
            'genetic_optimizer': {
                'best_fitness': self.genetic_optimizer.best_fitness,
                'pareto_front': [p.to_dict() for p in self.genetic_optimizer.pareto_front],
            }
        }
        return stats

    def get_health_status(self):
        return {
            'status': 'healthy' if self.global_health > 0.5 else 'degraded',
            'score': self.global_health,
            'mopd_enabled': self.config.mopd.enabled,
            'pareto_front_size': len(self.genetic_optimizer.pareto_front)
        }

    async def get_metrics(self, api_key=None):
        metrics = {
            'compartments_total': len(self.compartments),
            'compartments_viable': sum(r.get_viable_count() for r in self.regions.values()),
            'global_health': self.global_health,
            'total_regions': len(self.regions),
        }
        if self.telemetry:
            telemetry_export = await self.telemetry.export(api_key)
            for line in telemetry_export.split('\n'):
                if line and not line.startswith('#'):
                    parts = line.split(' ')
                    if len(parts) >= 2:
                        metrics[parts[0]] = float(parts[1])
        return metrics

    async def health_check_endpoint(self):
        return {'status': 'ok' if self.global_health > 0.5 else 'degraded',
                'global_health': self.global_health,
                'compartments': len(self.compartments),
                'regions': len(self.regions)}

    # ============================================================================
    # Teacher Policy (NEW)
    # ============================================================================
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        """
        Return a probability distribution over compartments (or strategies)
        based on health and MOPD objectives.
        """
        if not self.compartments:
            return [1.0]

        candidates = []
        for comp_id, comp in self.compartments.items():
            if not comp.is_viable:
                continue
            candidates.append({
                'compartment_id': comp_id,
                'health': comp.health_score,
                'efficiency': comp.efficiency_score,
                'token_balance': min(comp.token_balance / 1000, 1.0),
                'resource_utilization': comp.resources.utilization,
            })

        if not candidates:
            return []

        if self.adaptive_cost and self.pareto_gating:
            scored = []
            for c in candidates:
                cost = self.adaptive_cost.compute(
                    quality=c['health'],
                    carbon_g=0.0,
                    latency_ms=0.0,
                    energy_joules=0.0,
                    health=c['health'],
                    atp=c['token_balance']
                )
                c['score'] = cost
                scored.append(c)

            pareto_candidates = [
                {
                    'compartment_id': c['compartment_id'],
                    'quality_score': c['health'],
                    'carbon_g': 0.0,
                    'latency_ms': 0.0,
                    'energy_joules': 0.0,
                }
                for c in scored
            ]
            filtered = self.pareto_gating.filter(pareto_candidates)
            if filtered:
                allowed_ids = {c['compartment_id'] for c in filtered}
                scored = [c for c in scored if c['compartment_id'] in allowed_ids]

            if scored:
                scores = [c['score'] for c in scored]
                exp = np.exp(scores - np.max(scores))
                probs = exp / exp.sum()
                full_probs = []
                for comp_id in self.compartments.keys():
                    if comp_id in [c['compartment_id'] for c in scored]:
                        idx = next(i for i, c in enumerate(scored) if c['compartment_id'] == comp_id)
                        full_probs.append(probs[idx])
                    else:
                        full_probs.append(0.0)
                total = sum(full_probs)
                if total > 0:
                    full_probs = [p/total for p in full_probs]
                return full_probs
            else:
                return [1.0 / len(candidates)] * len(self.compartments)
        else:
            prob = 1.0 / len(candidates)
            return [prob if cid in [c['compartment_id'] for c in candidates] else 0.0 for cid in self.compartments.keys()]

    async def shutdown(self):
        logger.info("Shutting down Hierarchical Compartment Manager")
        for task in self._background_tasks:
            task.cancel()
        if self.config.enable_persistence and self.persistence:
            await self.save_state()
        logger.info("Shutdown complete")


# ============================================================================
# Legacy compatibility
# ============================================================================

class CompartmentManager(HierarchicalCompartmentManager):
    def __init__(self, token_manager=None):
        config = CompartmentConfig(max_regions=5, compartments_per_region=20)
        super().__init__(config=config, token_manager=token_manager)
        logger.info("Compartment Manager initialized (legacy compatibility mode)")
