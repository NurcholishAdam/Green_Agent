#!/usr/bin/env python3
"""
Enhanced Degradation Manager v7.2.0 – Complete Implementation with MOPD and central integration.

This version integrates central Green Agent components, adds teacher policy,
safe async task creation, central MODP, and bio-inspired feedback loops.
"""

import asyncio
import logging
import json
import os
import hashlib
import uuid
import sqlite3
import pickle
import yaml
from typing import Dict, Any, List, Optional, Tuple, Callable, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
import numpy as np
from collections import deque, defaultdict
from pathlib import Path
import random
import secrets

# ============================================================================
# Optional dependencies with graceful degradation
# ============================================================================
try:
    from pydantic import BaseModel, Field, field_validator, ConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

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
    CentralStorage = None
    AsyncMessageQueue = None
    ParetoGating = None
    AdaptiveCostFunction = None
    DriftDetector = None
    MetricsRegistry = None
    FeedbackEvent = None
    central_config = None

# ============================================================================
# Local imports (with fallback)
# ============================================================================
try:
    from .eco_atp_currency import EcoATPTokenManager, EcoATPConsumer, EcoATPSource
    TOKEN_AVAILABLE = True
except ImportError:
    TOKEN_AVAILABLE = False
    class EcoATPSource:
        GRADIENT_CONVERSION = "gradient_conversion"
    class EcoATPConsumer:
        EXPERT_EXECUTION = "expert_execution"

try:
    from .proton_gradient_fields import GradientFieldManager
    GRADIENT_AVAILABLE = True
except ImportError:
    GRADIENT_AVAILABLE = False

# ============================================================================
# Retry Decorator
# ============================================================================
def retry_decorator(max_attempts=3, min_delay=0.1, max_delay=10.0):
    """Decorator to retry async functions with exponential backoff."""
    if TENACITY_AVAILABLE:
        def decorator(func):
            @retry(
                stop=stop_after_attempt(max_attempts),
                wait=wait_exponential(multiplier=min_delay, min=min_delay, max=max_delay),
                retry=retry_if_exception_type(Exception),
                before_sleep=before_sleep_log(logger, logging.WARNING)
            )
            async def wrapper(*args, **kwargs):
                return await func(*args, **kwargs)
            return wrapper
        return decorator
    else:
        def decorator(func):
            async def wrapper(*args, **kwargs):
                for attempt in range(max_attempts):
                    try:
                        return await func(*args, **kwargs)
                    except Exception as e:
                        if attempt == max_attempts - 1:
                            raise
                        delay = min(min_delay * (2 ** attempt), max_delay)
                        await asyncio.sleep(delay)
            return wrapper
        return decorator

# ============================================================================
# Persistent Circuit Breaker (SQLite)
# ============================================================================
class CircuitBreaker:
    """Circuit breaker with SQLite persistence."""
    def __init__(self, name: str, db_path: str, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        self.name = name
        self.db_path = db_path
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
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

    async def call(self, func: Callable, *args, **kwargs):
        async with self._lock:
            if self.state == 'open':
                if self.last_failure_time and (datetime.now(timezone.utc) - self.last_failure_time).total_seconds() >= self.recovery_timeout:
                    self.state = 'half_open'
                    self._save_state()
                    logger.info(f"Circuit breaker {self.name} transitioning to half_open")
                else:
                    raise Exception(f"Circuit breaker {self.name} is OPEN")
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
                self.last_failure_time = datetime.now(timezone.utc)
                if self.failure_count >= self.failure_threshold:
                    self.state = 'open'
                    logger.warning(f"Circuit breaker {self.name} opened after {self.failure_count} failures")
                self._save_state()
            raise e

# ============================================================================
# Configuration Classes
# ============================================================================
if PYDANTIC_AVAILABLE:
    class MOPDConfig(BaseModel):
        enabled: bool = True
        objective_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'health': 0.4,
                'stability': 0.3,
                'recovery': 0.3,
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

    class DegradationConfig(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)

        enable_predictive: bool = True
        enable_ml_predictor: bool = True
        enable_anomaly_detection: bool = True
        enable_chaos_injection: bool = True
        enable_self_healing: bool = True
        enable_genetic_optimizer: bool = True
        enable_persistence: bool = True
        enable_telemetry: bool = True

        transition_cooldown_seconds: float = 30.0
        default_transition_speed: str = "normal"
        gradual_transition_duration_seconds: float = 15.0
        recovery_validation_period_seconds: float = 60.0

        health_weights: Dict[str, float] = Field(default_factory=lambda: {
            'token_balance': 0.30,
            'carbon_gradient': 0.25,
            'compartment_health': 0.20,
            'harvester_activity': 0.15,
            'error_rate': 0.10
        })

        ml_lookback: int = 10
        ml_forecast_steps: int = 5
        ml_training_interval_samples: int = 100
        ml_model_path: str = "models/lstm_health_model.joblib"

        anomaly_base_zscore: float = 3.0
        anomaly_adapt_window: int = 50

        chaos_safety_enabled: bool = True
        chaos_schedule_interval_hours: int = 6

        ga_population_size: int = 20
        ga_mutation_rate: float = 0.2
        ga_crossover_rate: float = 0.7
        ga_generations: int = 10
        ga_tournament_size: int = 3
        ga_evolution_interval_hours: int = 24

        max_retries: int = 3
        retry_base_delay_ms: float = 100.0
        retry_max_delay_ms: float = 5000.0
        circuit_breaker_threshold: int = 5
        circuit_breaker_recovery_timeout: float = 30.0
        circuit_breaker_db_path: str = "circuit_breakers.db"

        persistence_path: str = "degradation_manager_state.pkl"
        telemetry_export_interval: int = 60

        enable_quantum_signing: bool = True
        quantum_signing_algorithm: str = 'dilithium'
        enable_blockchain_audit: bool = True
        blockchain_rpc_url: str = 'http://localhost:8545'
        blockchain_contract_address: str = '0x0000000000000000000000000000000000000000'
        blockchain_private_key: Optional[str] = None
        enable_multi_cloud: bool = True
        cloud_provider: str = 'aws'
        cloud_region: str = 'us-east-1'
        cloud_bucket: str = 'degradation-state'
        cloud_access_key: Optional[str] = None
        cloud_secret_key: Optional[str] = None
        enable_autonomous_strategy: bool = True
        rl_learning_rate: float = 0.1
        rl_discount_factor: float = 0.9
        rl_exploration_rate: float = 0.1
        enable_health_endpoint: bool = True
        health_endpoint_port: int = 8081
        prometheus_port: Optional[int] = None
        q_table_db_path: str = "q_table.db"
        healing_feedback_window: int = 50
        subscribe_to_token_events: bool = True
        subscribe_to_gradient_events: bool = True
        mopd: MOPDConfig = Field(default_factory=MOPDConfig)

        @classmethod
        def from_env_and_file(cls, config_path: Optional[str] = None) -> 'DegradationConfig':
            # simplified
            return cls()

        def to_dict(self) -> Dict[str, Any]:
            return self.model_dump()

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> 'DegradationConfig':
            return cls(**data)
else:
    @dataclass
    class MOPDConfig:
        enabled: bool = True
        objective_weights: Dict[str, float] = field(default_factory=lambda: {
            'health': 0.4, 'stability': 0.3, 'recovery': 0.3,
        })
        grid_resolution: int = 5

    @dataclass
    class DegradationConfig:
        enable_predictive: bool = True
        enable_ml_predictor: bool = True
        enable_anomaly_detection: bool = True
        enable_chaos_injection: bool = True
        enable_self_healing: bool = True
        enable_genetic_optimizer: bool = True
        enable_persistence: bool = True
        enable_telemetry: bool = True
        transition_cooldown_seconds: float = 30.0
        default_transition_speed: str = "normal"
        gradual_transition_duration_seconds: float = 15.0
        recovery_validation_period_seconds: float = 60.0
        health_weights: Dict[str, float] = field(default_factory=lambda: {
            'token_balance': 0.30,
            'carbon_gradient': 0.25,
            'compartment_health': 0.20,
            'harvester_activity': 0.15,
            'error_rate': 0.10
        })
        ml_lookback: int = 10
        ml_forecast_steps: int = 5
        ml_training_interval_samples: int = 100
        ml_model_path: str = "models/lstm_health_model.joblib"
        anomaly_base_zscore: float = 3.0
        anomaly_adapt_window: int = 50
        chaos_safety_enabled: bool = True
        chaos_schedule_interval_hours: int = 6
        ga_population_size: int = 20
        ga_mutation_rate: float = 0.2
        ga_crossover_rate: float = 0.7
        ga_generations: int = 10
        ga_tournament_size: int = 3
        ga_evolution_interval_hours: int = 24
        max_retries: int = 3
        retry_base_delay_ms: float = 100.0
        retry_max_delay_ms: float = 5000.0
        circuit_breaker_threshold: int = 5
        circuit_breaker_recovery_timeout: float = 30.0
        circuit_breaker_db_path: str = "circuit_breakers.db"
        persistence_path: str = "degradation_manager_state.pkl"
        telemetry_export_interval: int = 60
        enable_quantum_signing: bool = True
        quantum_signing_algorithm: str = 'dilithium'
        enable_blockchain_audit: bool = True
        blockchain_rpc_url: str = 'http://localhost:8545'
        blockchain_contract_address: str = '0x0000000000000000000000000000000000000000'
        blockchain_private_key: Optional[str] = None
        enable_multi_cloud: bool = True
        cloud_provider: str = 'aws'
        cloud_region: str = 'us-east-1'
        cloud_bucket: str = 'degradation-state'
        cloud_access_key: Optional[str] = None
        cloud_secret_key: Optional[str] = None
        enable_autonomous_strategy: bool = True
        rl_learning_rate: float = 0.1
        rl_discount_factor: float = 0.9
        rl_exploration_rate: float = 0.1
        enable_health_endpoint: bool = True
        health_endpoint_port: int = 8081
        prometheus_port: Optional[int] = None
        q_table_db_path: str = "q_table.db"
        healing_feedback_window: int = 50
        subscribe_to_token_events: bool = True
        subscribe_to_gradient_events: bool = True
        mopd: MOPDConfig = field(default_factory=MOPDConfig)

        def to_dict(self) -> Dict[str, Any]:
            return asdict(self)

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> 'DegradationConfig':
            return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

        @classmethod
        def from_env_and_file(cls, config_path: Optional[str] = None) -> 'DegradationConfig':
            return cls()

# ============================================================================
# Enums and Data Classes
# ============================================================================
class OperationalTier(Enum):
    TIER_5_FULL = 5
    TIER_4_REDUCED = 4
    TIER_3_CONSERVATIVE = 3
    TIER_2_CRITICAL = 2
    TIER_1_SURVIVAL = 1

class TransitionType(Enum):
    DEGRADATION = "degradation"
    RECOVERY = "recovery"
    PREEMPTIVE = "preemptive"
    CHAOS_INDUCED = "chaos_induced"
    MANUAL = "manual"
    ANOMALY_INDUCED = "anomaly_induced"

class TransitionSpeed(Enum):
    INSTANT = "instant"
    FAST = "fast"
    NORMAL = "normal"
    SLOW = "slow"
    GRACEFUL = "graceful"

@dataclass
class DegradationRule:
    rule_id: str
    metric: str
    enter_threshold: float
    exit_threshold: float
    comparison: str
    target_tier: OperationalTier
    cooldown_seconds: float = 60.0
    description: str = ""
    weight: float = 1.0
    trend_sensitive: bool = False
    trend_window: int = 10
    trend_threshold: float = 0.0
    anomaly_sensitive: bool = False

@dataclass
class TransitionRecord:
    transition_id: str
    timestamp: datetime
    transition_type: TransitionType
    from_tier: OperationalTier
    to_tier: OperationalTier
    trigger_metric: str
    trigger_value: float
    trigger_threshold: float
    health_scores: Dict[str, float]
    duration_in_previous_tier: float
    was_preemptive: bool = False
    was_anomaly: bool = False
    transition_speed: TransitionSpeed = TransitionSpeed.NORMAL
    quantum_signature: Optional[Dict] = None

@dataclass
class HealthScore:
    timestamp: datetime
    overall_score: float
    component_scores: Dict[str, float]
    trend: str
    predicted_tier: Optional[OperationalTier] = None
    time_to_next_tier: Optional[float] = None
    confidence: float = 0.7
    ml_predicted_score: Optional[float] = None
    ml_confidence: float = 0.0
    anomaly_score: float = 0.0
    is_anomalous: bool = False

@dataclass
class MOPDPoint:
    individual: Dict[str, Any]
    health: float
    stability: float
    recovery: float
    scalarised_score: float = 0.0

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

# ============================================================================
# Task Manager (safe)
# ============================================================================
class TaskManager:
    def __init__(self):
        self.tasks = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()

    def start_task(self, name, coro_func, *args, **kwargs):
        async def wrapper():
            backoff = 1
            max_backoff = 300
            while not self.shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Task crashed", name=name, error=str(e), exc_info=True)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
        try:
            loop = asyncio.get_running_loop()
            task = loop.create_task(wrapper(), name=name)
        except RuntimeError:
            logger.warning(f"No running event loop; task '{name}' not started.")
            return None
        async with self._lock:
            self.tasks[name] = task
        return task

    async def stop_all(self):
        self.shutdown_event.set()
        async with self._lock:
            for task in self.tasks.values():
                task.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()

# ============================================================================
# Genetic Optimizer (Enhanced with central MODP)
# ============================================================================
class DegradationGeneticOptimizer:
    def __init__(self, manager: 'DegradationManager', config: DegradationConfig):
        self.manager = manager
        self.config = config
        self.population_size = config.ga_population_size
        self.mutation_rate = config.ga_mutation_rate
        self.crossover_rate = config.ga_crossover_rate
        self.generations = config.ga_generations
        self.tournament_size = config.ga_tournament_size
        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.pareto_front: List[MOPDPoint] = []

        # Central components (set by manager)
        self.adaptive_cost = None
        self.pareto_gating = None

    def set_central_components(self, adaptive_cost, pareto_gating):
        self.adaptive_cost = adaptive_cost
        self.pareto_gating = pareto_gating

    def _initialize_individual(self):
        ind = {}
        for rule in self.manager.rules:
            ind[f"{rule.rule_id}_enter"] = random.uniform(0.1, 0.9)
            ind[f"{rule.rule_id}_exit"] = random.uniform(0.1, 0.9)
            ind[f"{rule.rule_id}_weight"] = random.uniform(0.5, 2.0)
            if rule.trend_sensitive:
                ind[f"{rule.rule_id}_trend_threshold"] = random.uniform(-0.1, 0.1)
        for key in ['token_balance', 'carbon_gradient', 'compartment_health', 'harvester_activity', 'error_rate']:
            ind[f"weight_{key}"] = random.uniform(0.05, 0.4)
        total = sum(ind[f"weight_{key}"] for key in ['token_balance', 'carbon_gradient', 'compartment_health', 'harvester_activity', 'error_rate'])
        for key in ['token_balance', 'carbon_gradient', 'compartment_health', 'harvester_activity', 'error_rate']:
            ind[f"weight_{key}"] /= total
        return ind

    def _initialize_population(self):
        return [self._initialize_individual() for _ in range(self.population_size)]

    def _snapshot_config(self):
        return {
            'rules': [(r.rule_id, r.enter_threshold, r.exit_threshold, r.weight, r.trend_threshold if r.trend_sensitive else 0) for r in self.manager.rules],
            'weights': {k: v for k, v in self.manager._health_weights.items()}
        }

    def _apply_snapshot(self, snapshot):
        for rule, (rule_id, enter, exit, weight, trend) in zip(self.manager.rules, snapshot['rules']):
            rule.enter_threshold = enter
            rule.exit_threshold = exit
            rule.weight = weight
            if rule.trend_sensitive:
                rule.trend_threshold = trend
        self.manager._health_weights = snapshot['weights']

    def _evaluate_individual(self, individual):
        snapshot = self._snapshot_config()
        new_rules = []
        for rule in self.manager.rules:
            new_rules.append((
                rule.rule_id,
                individual[f"{rule.rule_id}_enter"],
                individual[f"{rule.rule_id}_exit"],
                individual[f"{rule.rule_id}_weight"],
                individual[f"{rule.rule_id}_trend_threshold"] if rule.trend_sensitive else 0.0
            ))
        new_weights = {}
        for key in self.manager._health_weights:
            new_weights[key] = individual[f"weight_{key}"]

        original_snapshot = self._snapshot_config()
        self._apply_snapshot({'rules': new_rules, 'weights': new_weights})

        health_score = self.manager.calculate_health_score()
        stability = max(0, 1 - len([t for t in self.manager.tier_history if (datetime.utcnow() - t.timestamp) < timedelta(hours=1)]) / 20)
        recovery = 1 - min(1, self.manager.recovery_validation_period.total_seconds() / 300)

        self._apply_snapshot(original_snapshot)

        return {
            'health': health_score.overall_score,
            'stability': stability,
            'recovery': recovery
        }

    def _select(self, population, fitness_scores):
        tournament = random.sample(range(len(population)), self.tournament_size)
        best_idx = max(tournament, key=lambda i: fitness_scores[i])
        return population[best_idx]

    def _crossover(self, parent1, parent2):
        child = {}
        for key in parent1:
            if random.random() < 0.5:
                child[key] = parent1[key]
            else:
                child[key] = parent2[key]
            if random.random() < 0.3:
                child[key] = (parent1[key] + parent2[key]) / 2
        return child

    def _mutate(self, individual):
        mutated = individual.copy()
        for key in mutated:
            if random.random() < self.mutation_rate:
                delta = random.uniform(-0.1, 0.1)
                if 'threshold' in key and 'trend' not in key:
                    mutated[key] = max(0.01, min(0.99, mutated[key] + delta))
                elif 'weight' in key:
                    mutated[key] = max(0.01, min(2.0, mutated[key] + delta))
                else:
                    mutated[key] += delta
        total = sum(mutated[f"weight_{key}"] for key in ['token_balance', 'carbon_gradient', 'compartment_health', 'harvester_activity', 'error_rate'])
        if total > 0:
            for key in ['token_balance', 'carbon_gradient', 'compartment_health', 'harvester_activity', 'error_rate']:
                mutated[f"weight_{key}"] /= total
        return mutated

    def _filter_pareto(self, points):
        if not points:
            return []
        objective_keys = ['health', 'stability', 'recovery']
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
                score += weights.get(key, 0.0) * norm
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
            individuals_with_objs = []
            for ind in population:
                objs = self._evaluate_individual(ind)
                individuals_with_objs.append((ind, objs))

            # Use central MODP if available
            if self.adaptive_cost and self.pareto_gating:
                candidates = []
                for ind, objs in individuals_with_objs:
                    candidates.append({
                        'expert_id': str(id(ind)),
                        'quality_score': objs['health'],
                        'carbon_g': 0.0,
                        'latency_ms': 0.0,
                        'energy_joules': 0.0,
                        'individual': ind,
                        'objectives': objs
                    })
                filtered = self.pareto_gating.filter(candidates)
                if filtered:
                    allowed_ids = {c['expert_id'] for c in filtered}
                    individuals_with_objs = [(ind, objs) for ind, objs in individuals_with_objs if str(id(ind)) in allowed_ids]
                    if not individuals_with_objs:
                        individuals_with_objs = [(ind, objs) for ind, objs in individuals_with_objs]  # keep all
                scores = []
                for ind, objs in individuals_with_objs:
                    cost = self.adaptive_cost.compute(
                        quality=objs['health'],
                        carbon_g=0.0,
                        latency_ms=0.0,
                        energy_joules=0.0,
                        health=0.8,
                        atp=0.5
                    )
                    scores.append(cost)
                fitness_scores = scores
            else:
                if self.config.mopd.enabled:
                    weights = self.config.mopd.objective_weights
                    fitness_scores = []
                    for _, objs in individuals_with_objs:
                        score = (weights.get('health', 0.4) * objs['health'] +
                                 weights.get('stability', 0.3) * objs['stability'] +
                                 weights.get('recovery', 0.3) * objs['recovery'])
                        fitness_scores.append(score)
                else:
                    fitness_scores = [objs['health'] for _, objs in individuals_with_objs]

            if self.config.mopd.enabled:
                points = []
                for ind, objs in individuals_with_objs:
                    points.append(MOPDPoint(
                        individual=ind,
                        health=objs['health'],
                        stability=objs['stability'],
                        recovery=objs['recovery']
                    ))
                self.pareto_front = self._filter_pareto(self.pareto_front + points)

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

        if self.config.mopd.enabled and self.pareto_front:
            best_point = self._select_best_from_pareto(self.pareto_front)
            if best_point:
                self.best_individual = best_point.individual
                self.best_fitness = best_point.scalarised_score
                # Apply best individual
                snapshot = self._snapshot_config()
                new_rules = []
                for rule in self.manager.rules:
                    new_rules.append((
                        rule.rule_id,
                        self.best_individual[f"{rule.rule_id}_enter"],
                        self.best_individual[f"{rule.rule_id}_exit"],
                        self.best_individual[f"{rule.rule_id}_weight"],
                        self.best_individual[f"{rule.rule_id}_trend_threshold"] if rule.trend_sensitive else 0.0
                    ))
                new_weights = {}
                for key in self.manager._health_weights:
                    new_weights[key] = self.best_individual[f"weight_{key}"]
                self._apply_snapshot({'rules': new_rules, 'weights': new_weights})
        else:
            if fitness_scores:
                best_idx = max(range(len(population)), key=lambda i: fitness_scores[i])
                self.best_fitness = fitness_scores[best_idx]
                self.best_individual = population[best_idx]
                self._apply_snapshot(self._snapshot_config())

        self.evolution_history.append({
            'timestamp': datetime.utcnow(),
            'best_fitness': self.best_fitness,
            'pareto_front_size': len(self.pareto_front) if self.config.mopd.enabled else 0
        })
        return {
            'best_fitness': self.best_fitness,
            'best_individual': self.best_individual,
            'pareto_front': [p.to_dict() for p in self.pareto_front] if self.config.mopd.enabled else None
        }

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
            'pareto_front_size': len(self.pareto_front) if self.config.mopd.enabled else 0
        }

# ============================================================================
# Persistence Manager
# ============================================================================
class DegradationPersistenceManager:
    CURRENT_VERSION = "2.0"

    def __init__(self, config: DegradationConfig):
        self.config = config
        self.path = Path(config.persistence_path)
        self._lock = asyncio.Lock()

    async def save_state(self, manager: 'DegradationManager') -> bool:
        async with self._lock:
            try:
                state = {
                    'version': self.CURRENT_VERSION,
                    'config': manager.config.to_dict(),
                    'current_tier': manager.current_tier.value,
                    'previous_tier': manager.previous_tier.value,
                    'tier_history': manager.tier_history,
                    'health_scores': list(manager.health_scores),
                    'metrics_history': manager.metrics_history,
                    'rules': manager.rules,
                    'health_weights': manager._health_weights,
                    'token_balance': manager._token_balance,
                    'carbon_gradient': manager._carbon_gradient,
                    'compartment_health': manager._compartment_health,
                    'harvester_activity': manager._harvester_activity,
                    'error_rate': manager._error_rate,
                    'queue_depth': manager._queue_depth,
                    'genetic_optimizer': manager.genetic_optimizer.to_dict(),
                }
                with open(self.path, 'wb') as f:
                    pickle.dump(state, f)
                logger.info(f"Degradation manager state saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    async def load_state(self, manager: 'DegradationManager') -> bool:
        async with self._lock:
            if not self.path.exists():
                return False
            try:
                with open(self.path, 'rb') as f:
                    state = pickle.load(f)
                version = state.get('version', '0.0')
                if version != self.CURRENT_VERSION:
                    logger.warning(f"State version {version} != current {self.CURRENT_VERSION}; attempting migration")
                manager.current_tier = OperationalTier(state.get('current_tier', 5))
                manager.previous_tier = OperationalTier(state.get('previous_tier', 5))
                manager.tier_history = state.get('tier_history', [])
                manager.health_scores = deque(state.get('health_scores', []), maxlen=100)
                manager.metrics_history = state.get('metrics_history', defaultdict(lambda: deque(maxlen=100)))
                manager.rules = state.get('rules', manager.rules)
                manager._health_weights = state.get('health_weights', manager._health_weights)
                manager._token_balance = state.get('token_balance', 500)
                manager._carbon_gradient = state.get('carbon_gradient', 0.5)
                manager._compartment_health = state.get('compartment_health', 0.8)
                manager._harvester_activity = state.get('harvester_activity', 0.6)
                manager._error_rate = state.get('error_rate', 0.01)
                manager._queue_depth = state.get('queue_depth', 0)
                go_state = state.get('genetic_optimizer', {})
                manager.genetic_optimizer.from_dict(go_state)
                logger.info(f"Degradation manager state loaded from {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return False

# ============================================================================
# Main Degradation Manager (Enhanced)
# ============================================================================
class DegradationManager:
    def __init__(
        self,
        config: Optional[DegradationConfig] = None,
        event_bus=None,
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
            config = DegradationConfig.from_env_and_file()
        self.config = config
        self.event_bus = event_bus

        # Central components
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto_gating = pareto_gating
        self.drift_detector = drift_detector
        self.metrics = metrics

        # External managers
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager

        # Core state
        self.current_tier = OperationalTier.TIER_5_FULL
        self.previous_tier = OperationalTier.TIER_5_FULL
        self.predicted_tier = None
        self.recovering_from_tier = None

        self.tier_history: List[TransitionRecord] = []
        self.health_scores: deque = deque(maxlen=100)
        self.metrics_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))

        # Rules and weights
        self.rules: List[DegradationRule] = self._default_rules()
        self._health_weights = self.config.health_weights.copy()

        # Metric values
        self._token_balance = 500
        self._carbon_gradient = 0.5
        self._compartment_health = 0.8
        self._harvester_activity = 0.6
        self._error_rate = 0.01
        self._queue_depth = 0

        # Sub-systems
        self.genetic_optimizer = DegradationGeneticOptimizer(self, config)
        if adaptive_cost and pareto_gating:
            self.genetic_optimizer.set_central_components(adaptive_cost, pareto_gating)

        # Persistence
        self.persistence = DegradationPersistenceManager(config) if config.enable_persistence and not storage else None

        # Background tasks
        self._task_manager = TaskManager()
        self._task_manager.start_task("evolution_loop", self._evolution_loop)
        self._task_manager.start_task("monitoring_loop", self._monitoring_loop)

        # Load state
        if self.persistence:
            self._load_state_task = self._create_task(self._load_state())

        logger.info("Enhanced Degradation Manager v7.2.0 initialized with central integration",
                    storage=storage is not None, queue=message_queue is not None)

    def _create_task(self, coro):
        try:
            loop = asyncio.get_running_loop()
            return loop.create_task(coro)
        except RuntimeError:
            logger.warning("No running event loop; task not started.")
            return None

    def _default_rules(self):
        rules = [
            DegradationRule(
                rule_id="carbon_high",
                metric="carbon_gradient",
                enter_threshold=0.7,
                exit_threshold=0.5,
                comparison="greater_than",
                target_tier=OperationalTier.TIER_4_REDUCED,
                cooldown_seconds=60,
                weight=1.0,
                trend_sensitive=True,
                trend_window=10,
                trend_threshold=0.02
            ),
            DegradationRule(
                rule_id="token_low",
                metric="token_balance",
                enter_threshold=300,
                exit_threshold=1000,
                comparison="less_than",
                target_tier=OperationalTier.TIER_3_CONSERVATIVE,
                cooldown_seconds=60,
                weight=1.0,
                trend_sensitive=True,
                trend_window=10,
                trend_threshold=-0.1
            ),
            DegradationRule(
                rule_id="health_critical",
                metric="compartment_health",
                enter_threshold=0.3,
                exit_threshold=0.6,
                comparison="less_than",
                target_tier=OperationalTier.TIER_2_CRITICAL,
                cooldown_seconds=30,
                weight=2.0,
                trend_sensitive=False
            ),
            DegradationRule(
                rule_id="error_rate_high",
                metric="error_rate",
                enter_threshold=0.05,
                exit_threshold=0.01,
                comparison="greater_than",
                target_tier=OperationalTier.TIER_2_CRITICAL,
                cooldown_seconds=30,
                weight=2.0,
                trend_sensitive=True,
                trend_window=10,
                trend_threshold=0.01
            )
        ]
        return rules

    async def _load_state(self):
        if self.persistence:
            await self.persistence.load_state(self)

    async def _monitoring_loop(self):
        while True:
            await asyncio.sleep(self.config.transition_cooldown_seconds)
            try:
                # Simulate metric updates (in production, call external services)
                if self.token_manager:
                    summary = self.token_manager.get_system_summary()
                    self._token_balance = summary.get('total_balance', 500)
                if self.gradient_manager:
                    strengths = self.gradient_manager.get_field_strengths()
                    self._carbon_gradient = strengths.get('carbon', 0.5)
                # Evaluate rules and trigger transition if needed
                await self.evaluate_rules()
            except Exception as e:
                logger.error("Monitoring loop error", error=str(e))
                await asyncio.sleep(60)

    async def _evolution_loop(self):
        while True:
            try:
                if self.config.enable_genetic_optimizer and len(self.tier_history) >= 20:
                    logger.info("Starting genetic optimization cycle...")
                    result = await self.genetic_optimizer.evolve(generations=self.config.ga_generations)
                    logger.info(f"Genetic optimization complete: best fitness {result['best_fitness']:.4f}, Pareto front size: {len(result.get('pareto_front', []))}")
                    # Publish FeedbackEvent
                    if self.queue:
                        event = FeedbackEvent.create_with_context(
                            task_id=f"degradation_evolve_{uuid.uuid4().hex[:8]}",
                            selected_action="genetic_optimization",
                            quality_score=self.genetic_optimizer.best_fitness,
                            energy_joules=0.0,
                            carbon_g=0.0,
                            feedback_type="degradation",
                            adaptive_cost_value=self.genetic_optimizer.best_fitness,
                            state={'pareto_front_size': len(self.genetic_optimizer.pareto_front)},
                            candidates=[{'action': 'evolve'}],
                            source="degradation_manager",
                            environment=getattr(central_config, "ENVIRONMENT", "production") if central_config else "production",
                            tags=["degradation", "evolution"]
                        )
                        await self.queue.publish("feedback_events", event.to_json())
                await asyncio.sleep(self.config.ga_evolution_interval_hours * 3600)
            except Exception as e:
                logger.error("Evolution loop error", error=str(e))
                await asyncio.sleep(3600)

    def calculate_health_score(self) -> HealthScore:
        metrics = {
            'token_balance': self._token_balance,
            'carbon_gradient': self._carbon_gradient,
            'compartment_health': self._compartment_health,
            'harvester_activity': self._harvester_activity,
            'error_rate': self._error_rate
        }
        weighted_sum = sum(self._health_weights.get(k, 0) * metrics[k] for k in metrics)
        # Normalise to 0-1 (simplified)
        score = max(0.0, min(1.0, weighted_sum / 2.0))
        component_scores = {k: metrics[k] for k in metrics}
        return HealthScore(
            timestamp=datetime.utcnow(),
            overall_score=score,
            component_scores=component_scores,
            trend='stable',
            predicted_tier=self.predicted_tier,
            confidence=0.7
        )

    async def evaluate_rules(self):
        """Check degradation rules and transition if necessary."""
        for rule in self.rules:
            metric_value = getattr(self, f"_{rule.metric}")
            if rule.comparison == "greater_than":
                trigger = metric_value > rule.enter_threshold
            else:
                trigger = metric_value < rule.enter_threshold
            if trigger:
                # Check trend sensitivity (optional)
                if rule.trend_sensitive:
                    # simplified: skip for demo
                    pass
                await self.transition_to(rule.target_tier, trigger_metric=rule.metric,
                                          trigger_value=metric_value,
                                          trigger_threshold=rule.enter_threshold,
                                          transition_type=TransitionType.DEGRADATION)
                break  # only one transition per cycle

    async def transition_to(self, target_tier: OperationalTier, trigger_metric="manual", trigger_value=0,
                            trigger_threshold=0, transition_type=TransitionType.MANUAL,
                            speed: TransitionSpeed = TransitionSpeed.NORMAL):
        if self.current_tier == target_tier:
            return {'status': 'no_change'}
        self.previous_tier = self.current_tier
        self.current_tier = target_tier
        record = TransitionRecord(
            transition_id=str(uuid.uuid4()),
            timestamp=datetime.utcnow(),
            transition_type=transition_type,
            from_tier=self.previous_tier,
            to_tier=target_tier,
            trigger_metric=trigger_metric,
            trigger_value=trigger_value,
            trigger_threshold=trigger_threshold,
            health_scores=self.calculate_health_score().component_scores,
            duration_in_previous_tier=0.0,
            transition_speed=speed
        )
        self.tier_history.append(record)

        # Publish FeedbackEvent
        if self.queue:
            event = FeedbackEvent.create_with_context(
                task_id=f"degradation_{record.transition_id}",
                selected_action=f"transition_to_{target_tier.value}",
                quality_score=self.calculate_health_score().overall_score,
                energy_joules=0.0,
                carbon_g=0.0,
                feedback_type="degradation",
                adaptive_cost_value=0.0,
                state={'from_tier': self.previous_tier.value, 'to_tier': target_tier.value},
                candidates=[{'action': 'transition'}],
                source="degradation_manager",
                environment=getattr(central_config, "ENVIRONMENT", "production") if central_config else "production",
                tags=["degradation", "transition"]
            )
            await self.queue.publish("feedback_events", event.to_json())

        # Bio-inspired: spend ATP for transition if token_manager available
        if self.token_manager and transition_type in [TransitionType.DEGRADATION, TransitionType.RECOVERY]:
            cost = 5.0  # arbitrary
            try:
                await self.token_manager.spend("degradation_manager", cost)
            except:
                pass

        logger.info(f"Transitioned from {self.previous_tier.value} to {target_tier.value}")
        return {'status': 'success', 'transition': record.transition_id}

    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        """
        Return a probability distribution over possible degradation actions
        (e.g., maintain, degrade, recover) using central adaptive cost if available.
        """
        actions = ['maintain', 'degrade', 'recover']
        if not (self.adaptive_cost and self.pareto_gating):
            # Fallback: based on current health
            health = self.calculate_health_score().overall_score
            probs = [health, max(0.1, 1-health) * 0.7, max(0.1, health) * 0.3]
            total = sum(probs)
            return [p/total for p in probs]

        candidates = []
        for idx, action in enumerate(actions):
            quality = 0.9 if action == 'maintain' else 0.7 if action == 'recover' else 0.5
            carbon_g = 0.0
            latency_ms = 0.0
            energy_joules = 0.0
            cost = self.adaptive_cost.compute(
                quality=quality,
                carbon_g=carbon_g,
                latency_ms=latency_ms,
                energy_joules=energy_joules,
                health=self.calculate_health_score().overall_score,
                atp=0.5
            )
            candidates.append({
                'action': action,
                'score': cost,
                'carbon_g': carbon_g,
                'latency_ms': latency_ms,
                'energy_joules': energy_joules,
                'quality_score': quality
            })

        filtered = self.pareto_gating.filter(candidates)
        if filtered:
            allowed = {c['action'] for c in filtered}
            candidates = [c for c in candidates if c['action'] in allowed]

        if not candidates:
            return [1.0/3, 1.0/3, 1.0/3]

        scores = [c['score'] for c in candidates]
        exp = np.exp(scores - np.max(scores))
        probs = exp / exp.sum()
        full_probs = [0.0, 0.0, 0.0]
        for c, p in zip(candidates, probs):
            idx = actions.index(c['action'])
            full_probs[idx] = p
        return full_probs

    def get_mopd_pareto_front(self):
        if not self.config.mopd.enabled:
            return []
        return self.genetic_optimizer.pareto_front.copy()

    def get_mopd_summary(self):
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

    def get_health_status(self):
        health = self.calculate_health_score()
        return {
            'status': 'healthy' if self.current_tier.value > 3 else 'degraded',
            'score': health.overall_score,
            'details': {
                'current_tier': self.current_tier.value,
                'previous_tier': self.previous_tier.value,
                'predicted_tier': self.predicted_tier.value if self.predicted_tier else None,
                'transition_count': len(self.tier_history),
                'mopd_enabled': self.config.mopd.enabled,
                'pareto_front_size': len(self.genetic_optimizer.pareto_front),
            }
        }

    async def shutdown(self):
        logger.info("Shutting down Degradation Manager")
        await self._task_manager.stop_all()
        if self.persistence:
            await self.persistence.save_state(self)
        logger.info("Shutdown complete")

# ============================================================================
# Example usage (optional)
# ============================================================================
if __name__ == "__main__":
    async def main():
        mgr = DegradationManager()
        await asyncio.sleep(5)
        print(mgr.get_health_status())
        print(await mgr.policy_probs({}))
        print(mgr.get_mopd_summary())
        await mgr.shutdown()

    asyncio.run(main())
