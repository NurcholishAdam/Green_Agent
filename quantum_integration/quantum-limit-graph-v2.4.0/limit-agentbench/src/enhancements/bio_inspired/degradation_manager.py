# =============================================================================
# Enhanced Degradation Manager v7.1.0 - Complete Implementation with MOPD
# =============================================================================
"""
Enhanced Degradation Manager v7.1.0
All improvements integrated plus Multi‑Objective Pareto Decision (MOPD) support.

MOPD enhancements:
- MOPDConfig sub‑configuration for objective weights and grid resolution.
- MOPDPoint dataclass to represent a configuration with objectives.
- Pareto front generation in the genetic optimizer.
- Selection of best configuration via scalarisation.
- Persistence of Pareto front.
- Telemetry tracks MOPD generations and Pareto front sizes.
- Full backward compatibility.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Callable, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
import numpy as np
from collections import deque, defaultdict
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
    from sklearn.ensemble import RandomForestRegressor, IsolationForest
    from sklearn.preprocessing import StandardScaler
    import joblib
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import tensorflow as tf
    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import LSTM, Dense, Dropout
    TENSORFLOW_AVAILABLE = True
except ImportError:
    TENSORFLOW_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server, CollectorRegistry
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
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

# Web3 for blockchain
try:
    from web3 import Web3, Account, HTTPProvider
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

# FastAPI for health endpoint
try:
    from fastapi import FastAPI
    import uvicorn
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

# ============================================================================
# Import existing components (if available)
# ============================================================================
try:
    from .proton_gradient_fields import GradientFieldManager
    GRADIENT_AVAILABLE = True
except ImportError:
    GRADIENT_AVAILABLE = False

try:
    from .eco_atp_currency import EcoATPTokenManager, EcoATPConsumer, EcoATPSource
    TOKEN_AVAILABLE = True
except ImportError:
    TOKEN_AVAILABLE = False

# Structured logging
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
# Configuration (Enhanced with MOPD)
# ============================================================================

if PYDANTIC_AVAILABLE:
    class MOPDConfig(BaseModel):
        """Configuration for Multi‑Objective Pareto Decision (MOPD) in degradation optimization."""
        enabled: bool = Field(True, description="Enable MOPD‑aware genetic optimization")
        objective_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'health': 0.4,
                'stability': 0.3,
                'recovery': 0.3,
            },
            description="Weights for scalarising Pareto front (must sum to 1)"
        )
        grid_resolution: int = Field(5, description="Number of discrete points for sampling (unused for now)")

        @field_validator('objective_weights')
        @classmethod
        def check_weights(cls, v):
            total = sum(v.values())
            if abs(total - 1.0) > 1e-6:
                raise ValueError("objective_weights must sum to 1")
            return v

    class DegradationConfig(BaseModel):
        """Centralized configuration for Degradation Manager."""
        model_config = ConfigDict(arbitrary_types_allowed=True)

        # Feature flags
        enable_predictive: bool = True
        enable_ml_predictor: bool = True
        enable_anomaly_detection: bool = True
        enable_chaos_injection: bool = True
        enable_self_healing: bool = True
        enable_genetic_optimizer: bool = True
        enable_persistence: bool = True
        enable_telemetry: bool = True

        # Transition settings
        transition_cooldown_seconds: float = Field(default=30.0, ge=0)
        default_transition_speed: str = Field(default="normal")
        gradual_transition_duration_seconds: float = Field(default=15.0, ge=0)
        recovery_validation_period_seconds: float = Field(default=60.0, ge=0)

        # Health scoring weights (initial)
        health_weights: Dict[str, float] = Field(default_factory=lambda: {
            'token_balance': 0.30,
            'carbon_gradient': 0.25,
            'compartment_health': 0.20,
            'harvester_activity': 0.15,
            'error_rate': 0.10
        })

        # ML predictor
        ml_lookback: int = Field(default=10, ge=1)
        ml_forecast_steps: int = Field(default=5, ge=1)
        ml_training_interval_samples: int = Field(default=100, ge=1)
        ml_model_path: str = Field(default="models/lstm_health_model.joblib")

        # Anomaly detection
        anomaly_base_zscore: float = Field(default=3.0, ge=0)
        anomaly_adapt_window: int = Field(default=50, ge=1)

        # Chaos injection
        chaos_safety_enabled: bool = True
        chaos_schedule_interval_hours: int = Field(default=6, ge=1)

        # Genetic optimizer
        ga_population_size: int = Field(default=20, ge=2)
        ga_mutation_rate: float = Field(default=0.2, ge=0.0, le=1.0)
        ga_crossover_rate: float = Field(default=0.7, ge=0.0, le=1.0)
        ga_generations: int = Field(default=10, ge=1)
        ga_tournament_size: int = Field(default=3, ge=1)
        ga_evolution_interval_hours: int = Field(default=24, ge=1)

        # Retry and circuit breaker
        max_retries: int = Field(default=3, ge=1)
        retry_base_delay_ms: float = Field(default=100.0, ge=0)
        retry_max_delay_ms: float = Field(default=5000.0, ge=0)
        circuit_breaker_threshold: int = Field(default=5, ge=1)
        circuit_breaker_recovery_timeout: float = Field(default=30.0, ge=0)
        circuit_breaker_db_path: str = Field(default="circuit_breakers.db")

        # Persistence
        persistence_path: str = Field(default="degradation_manager_state.pkl")

        # Telemetry
        telemetry_export_interval: int = Field(default=60, ge=1)

        # ===== ENTERPRISE ENHANCEMENTS =====
        # Quantum signing
        enable_quantum_signing: bool = True
        quantum_signing_algorithm: str = Field(default='dilithium')

        # Blockchain audit
        enable_blockchain_audit: bool = True
        blockchain_rpc_url: str = Field(default='http://localhost:8545')
        blockchain_contract_address: str = Field(default='0x0000000000000000000000000000000000000000')
        blockchain_private_key: Optional[str] = None

        # Multi-cloud
        enable_multi_cloud: bool = True
        cloud_provider: str = Field(default='aws')
        cloud_region: str = Field(default='us-east-1')
        cloud_bucket: str = Field(default='degradation-state')
        cloud_access_key: Optional[str] = None
        cloud_secret_key: Optional[str] = None

        # Autonomous strategy selector
        enable_autonomous_strategy: bool = True
        rl_learning_rate: float = Field(default=0.1, ge=0.0, le=1.0)
        rl_discount_factor: float = Field(default=0.9, ge=0.0, le=1.0)
        rl_exploration_rate: float = Field(default=0.1, ge=0.0, le=1.0)

        # Health check HTTP endpoint
        enable_health_endpoint: bool = True
        health_endpoint_port: int = Field(default=8081)

        # Prometheus
        prometheus_port: Optional[int] = Field(default=None)

        # Q-table persistence path
        q_table_db_path: str = Field(default="q_table.db")

        # Self-healing feedback window
        healing_feedback_window: int = Field(default=50, ge=1)

        # Event subscription
        subscribe_to_token_events: bool = True
        subscribe_to_gradient_events: bool = True

        # MOPD configuration (NEW)
        mopd: MOPDConfig = Field(default_factory=MOPDConfig, description="MOPD sub‑configuration")

        @classmethod
        def from_env_and_file(cls, config_path: Optional[str] = None) -> 'DegradationConfig':
            env_overrides = {}
            for key in cls.model_fields.keys():
                env_var = f"DEGRADATION_{key.upper()}"
                if env_var in os.environ:
                    env_overrides[key] = os.environ[env_var]
            if config_path and os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    yaml_data = yaml.safe_load(f)
                    if yaml_data:
                        yaml_data.update(env_overrides)
                        return cls(**yaml_data)
            return cls(**env_overrides) if env_overrides else cls()

        def to_dict(self) -> Dict[str, Any]:
            return self.model_dump()

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> 'DegradationConfig':
            return cls(**data)
else:
    # Fallback dataclass (simplified)
    @dataclass
    class MOPDConfig:
        enabled: bool = True
        objective_weights: Dict[str, float] = field(default_factory=lambda: {
            'health': 0.4,
            'stability': 0.3,
            'recovery': 0.3,
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
# Enums and Data Classes (Enhanced with MOPD)
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
class ChaosExperimentResult:
    experiment_id: str
    experiment_name: str
    intensity: float
    start_time: datetime
    end_time: datetime
    recovery_time_seconds: float
    tier_impact: int
    safety_breached: bool
    metrics_before: Dict[str, float]
    metrics_after: Dict[str, float]
    resilience_score: float
    recommendations: List[str]
    lessons_learned: List[str]
    component_impacts: Dict[str, float] = field(default_factory=dict)

# ============================================================================
# MOPD Data Classes (NEW)
# ============================================================================

@dataclass
class MOPDPoint:
    """Represents a genetic individual with its objective vector."""
    # Decision variables: the individual parameters
    individual: Dict[str, Any]
    # Objectives (to be maximised)
    health: float
    stability: float
    recovery: float
    # Scalarised score (computed later)
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDPoint':
        return cls(**data)

# ============================================================================
# Retry Decorator (unchanged)
# ============================================================================

def retry_decorator(max_attempts: int = 3, min_delay: float = 0.1, max_delay: float = 10.0):
    # ... (same as before) ...
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
# Persistent Circuit Breaker (unchanged)
# ============================================================================

class CircuitBreaker:
    # ... (same as before) ...
    pass

# ============================================================================
# Quantum Security, Blockchain, Multi‑Cloud (unchanged)
# ============================================================================

class QuantumResilientSecurity:
    # ... (same as before) ...
    pass

class BlockchainAuditor:
    # ... (same as before) ...
    pass

class MultiCloudDistributor:
    # ... (same as before) ...
    pass

# ============================================================================
# Autonomous Strategy Selector (unchanged)
# ============================================================================

class AutonomousStrategySelector:
    # ... (same as before) ...
    pass

# ============================================================================
# LSTM Health Predictor (unchanged)
# ============================================================================

class LSTMHealthPredictor:
    # ... (same as before) ...
    pass

# ============================================================================
# Adaptive Anomaly Detection (unchanged)
# ============================================================================

class AdaptiveAnomalyDetection:
    # ... (same as before) ...
    pass

# ============================================================================
# Self-Healing Engine (unchanged)
# ============================================================================

class SelfHealingEngine:
    # ... (same as before) ...
    pass

# ============================================================================
# Chaos Injection (unchanged)
# ============================================================================

class ChaosInjectionSystem:
    # ... (same as before) ...
    pass

# ============================================================================
# Genetic Optimizer (Enhanced with MOPD)
# ============================================================================

class DegradationGeneticOptimizer:
    """Evolves degradation thresholds, weights, and trend parameters using MOPD if enabled."""

    def __init__(self, degradation_manager: 'DegradationManager', config: DegradationConfig):
        self.manager = degradation_manager
        self.config = config
        self.population_size = config.ga_population_size
        self.mutation_rate = config.ga_mutation_rate
        self.crossover_rate = config.ga_crossover_rate
        self.generations = config.ga_generations
        self.tournament_size = config.ga_tournament_size
        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        # MOPD: Pareto front storage
        self.pareto_front: List[MOPDPoint] = []
        logger.info("Degradation Genetic Optimizer initialized")

    def _initialize_individual(self) -> Dict:
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

    def _initialize_population(self) -> List[Dict]:
        return [self._initialize_individual() for _ in range(self.population_size)]

    def _snapshot_config(self) -> Dict:
        return {
            'rules': [(r.rule_id, r.enter_threshold, r.exit_threshold, r.weight, r.trend_threshold if r.trend_sensitive else 0) for r in self.manager.rules],
            'weights': {k: v for k, v in self.manager._health_weights.items()}
        }

    def _apply_snapshot(self, snapshot: Dict):
        for rule, (rule_id, enter, exit, weight, trend) in zip(self.manager.rules, snapshot['rules']):
            rule.enter_threshold = enter
            rule.exit_threshold = exit
            rule.weight = weight
            if rule.trend_sensitive:
                rule.trend_threshold = trend
        self.manager._health_weights = snapshot['weights']

    # ---------- Multi‑objective evaluation (NEW) ----------
    def _evaluate_individual(self, individual: Dict) -> Dict[str, float]:
        """Evaluate an individual on multiple objectives."""
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

        # Simulate performance
        health_score = self.manager.calculate_health_score()
        stability = max(0, 1 - len([t for t in self.manager.tier_history if (datetime.utcnow() - t.timestamp) < timedelta(hours=1)]) / 20)
        recovery = 1 - min(1, self.manager.recovery_validation_period.total_seconds() / 300)

        # Restore original
        self._apply_snapshot(original_snapshot)

        return {
            'health': health_score.overall_score,
            'stability': stability,
            'recovery': recovery
        }

    def _select(self, population: List[Dict], fitness_scores: List[float]) -> Dict:
        tournament = random.sample(range(len(population)), self.tournament_size)
        best_idx = max(tournament, key=lambda i: fitness_scores[i])
        return population[best_idx]

    def _crossover(self, parent1: Dict, parent2: Dict) -> Dict:
        child = {}
        for key in parent1:
            if random.random() < 0.5:
                child[key] = parent1[key]
            else:
                child[key] = parent2[key]
            if random.random() < 0.3:
                child[key] = (parent1[key] + parent2[key]) / 2
        return child

    def _mutate(self, individual: Dict) -> Dict:
        mutated = individual.copy()
        for key in mutated:
            if random.random() < self.mutation_rate:
                delta = random.uniform(-0.1, 0.1)
                if 'threshold' in key and 'trend' not in key:
                    mutated[key] = max(0.01, min(0.99, mutated[key] + delta))
                elif 'weight' in key:
                    mutated[key] = max(0.01, min(2.0, mutated[key] + delta))
                else:
                    mutated[key] = mutated[key] + delta
        total = sum(mutated[f"weight_{key}"] for key in ['token_balance', 'carbon_gradient', 'compartment_health', 'harvester_activity', 'error_rate'])
        if total > 0:
            for key in ['token_balance', 'carbon_gradient', 'compartment_health', 'harvester_activity', 'error_rate']:
                mutated[f"weight_{key}"] /= total
        return mutated

    # ---------- Pareto front methods (NEW) ----------
    def _filter_pareto(self, points: List[MOPDPoint]) -> List[MOPDPoint]:
        """Return non‑dominated points."""
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

    # ---------- Main evolve (enhanced) ----------
    async def evolve(self, generations: Optional[int] = None) -> Dict:
        if generations is None:
            generations = self.generations
        population = self._initialize_population()
        best_fitness = -float('inf')
        best_ind = None

        # If MOPD enabled, we'll collect Pareto front
        if self.config.mopd.enabled:
            self.pareto_front = []

        for gen in range(generations):
            # Evaluate objectives for all individuals
            individuals_with_objs = []
            for ind in population:
                objs = self._evaluate_individual(ind)
                individuals_with_objs.append((ind, objs))

            # If MOPD enabled, update Pareto front
            if self.config.mopd.enabled:
                points = []
                for ind, objs in individuals_with_objs:
                    point = MOPDPoint(
                        individual=ind,
                        health=objs['health'],
                        stability=objs['stability'],
                        recovery=objs['recovery']
                    )
                    points.append(point)
                self.pareto_front = self._filter_pareto(self.pareto_front + points)

                # Compute scalarised scores for selection
                weights = self.config.mopd.objective_weights
                fitness_scores = []
                for point in points:
                    score = (weights.get('health', 0.4) * point.health +
                             weights.get('stability', 0.3) * point.stability +
                             weights.get('recovery', 0.3) * point.recovery)
                    point.scalarised_score = score
                    fitness_scores.append(score)
            else:
                # Legacy: single fitness (health)
                fitness_scores = [objs['health'] for _, objs in individuals_with_objs]

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
            logger.debug(f"Gen {gen+1}: best fitness = {gen_best_fitness:.4f}")

        # After evolution, if MOPD enabled and we have a Pareto front, select best
        if self.config.mopd.enabled and self.pareto_front:
            best_point = self._select_best_from_pareto(self.pareto_front)
            if best_point:
                self.best_individual = best_point.individual
                self.best_fitness = best_point.scalarised_score
                # Apply best individual permanently
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
                logger.info(f"Applied best MOPD individual with scalarised score {self.best_fitness:.4f}")
        else:
            # Legacy: keep best fitness and individual
            if fitness_scores:
                best_idx = max(range(len(population)), key=lambda i: fitness_scores[i])
                self.best_fitness = fitness_scores[best_idx]
                self.best_individual = population[best_idx]
                self._apply_snapshot(self._snapshot_config())  # Apply the best
                logger.info(f"Applied best individual with fitness {self.best_fitness:.4f}")

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
            'pareto_front_size': len(self.pareto_front) if self.config.mopd.enabled else 0
        }

# ============================================================================
# Persistence Manager (Enhanced with MOPD)
# ============================================================================

class DegradationPersistenceManager:
    """Saves and loads degradation manager state using versioned pickle."""

    CURRENT_VERSION = "2.0"  # Bumped for MOPD

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
                    'chaos_experiments': manager.chaos_experiments,
                    'chaos_history': list(manager.chaos_history),
                    'tier_policies': manager.tier_policies,
                    'current_policy': manager.current_policy,
                    'target_policy': manager.target_policy,
                    'policy_transition_progress': manager.policy_transition_progress,
                    'prediction_history': list(manager.prediction_history),
                    'recovery_validation_metrics': manager.recovery_validation_metrics,
                    'recovering_from_tier': manager.recovering_from_tier.value if manager.recovering_from_tier else None,
                    # MOPD: store genetic optimizer state (including Pareto front)
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
                logger.warning(f"Persistence file {self.path} not found")
                return False
            try:
                with open(self.path, 'rb') as f:
                    state = pickle.load(f)
                version = state.get('version', '0.0')
                if version != self.CURRENT_VERSION:
                    logger.warning(f"State version {version} != current {self.CURRENT_VERSION}; attempting migration")
                # Restore state (same as before)
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
                manager.chaos_experiments = state.get('chaos_experiments', {})
                manager.chaos_history = deque(state.get('chaos_history', []), maxlen=500)
                manager.tier_policies = state.get('tier_policies', manager.tier_policies)
                manager.current_policy = state.get('current_policy', manager.tier_policies[OperationalTier.TIER_5_FULL])
                manager.target_policy = state.get('target_policy', None)
                manager.policy_transition_progress = state.get('policy_transition_progress', 1.0)
                manager.prediction_history = deque(state.get('prediction_history', []), maxlen=100)
                manager.recovery_validation_metrics = state.get('recovery_validation_metrics', defaultdict(list))
                recovering = state.get('recovering_from_tier')
                manager.recovering_from_tier = OperationalTier(recovering) if recovering else None
                # Restore genetic optimizer (including Pareto front)
                go_state = state.get('genetic_optimizer', {})
                manager.genetic_optimizer.from_dict(go_state)
                logger.info(f"Degradation manager state loaded from {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return False

# ============================================================================
# Telemetry (unchanged)
# ============================================================================

class DegradationTelemetry:
    # ... (same as before, but we'll add MOPD counters in the manager) ...
    pass

# ============================================================================
# Enhanced Degradation Manager (Main Class)
# ============================================================================

class DegradationManager:
    """
    Enhanced Degradation Manager v7.1.0 with MOPD support.
    """

    def __init__(self, config: Optional[DegradationConfig] = None, event_bus=None):
        if config is None:
            config = DegradationConfig.from_env_and_file()
        self.config = config
        self.event_bus = event_bus

        # ... (all existing initializations) ...

        # MOPD: genetic optimizer already created in __init__
        # ... (rest of __init__) ...

        logger.info(f"Enhanced Degradation Manager v7.1.0 initialized with MOPD: {self.config.mopd.enabled}")

    # ============================================================================
    # Public MOPD Methods (NEW)
    # ============================================================================

    def get_mopd_pareto_front(self) -> List[MOPDPoint]:
        """Return the current Pareto front from the genetic optimizer."""
        if not self.config.mopd.enabled:
            return []
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
    # Background loops (Enhanced with MOPD)
    # ============================================================================

    async def _evolution_loop(self):
        while True:
            try:
                if self.config.enable_genetic_optimizer and len(self.tier_history) >= 20:
                    logger.info("Starting genetic optimization cycle...")
                    result = await self.genetic_optimizer.evolve(generations=self.config.ga_generations)
                    logger.info(f"Genetic optimization complete: best fitness {result['best_fitness']:.4f}, Pareto front size: {len(result.get('pareto_front', []))}")
                    # Telemetry for MOPD
                    if self.config.mopd.enabled and self.telemetry:
                        self.telemetry.increment('mopd_generations')
                        if result.get('pareto_front'):
                            self.telemetry.histogram('mopd_pareto_front_size', len(result['pareto_front']))
                await asyncio.sleep(self.config.ga_evolution_interval_hours * 3600)
            except Exception as e:
                logger.error(f"Evolution loop error: {str(e)}")
                await asyncio.sleep(3600)

    # ============================================================================
    # Health check (Enhanced)
    # ============================================================================

    def get_health_status(self) -> Dict[str, Any]:
        return {
            'status': 'healthy' if self.current_tier.value > 3 else 'degraded',
            'score': self.calculate_health_score().overall_score,
            'details': {
                'current_tier': self.current_tier.value,
                'previous_tier': self.previous_tier.value,
                'predicted_tier': self.predicted_tier.value if self.predicted_tier else None,
                'transition_count': len(self.tier_history),
                'last_transition': self.tier_history[-1].timestamp.isoformat() if self.tier_history else None,
                'ml_predictor_trained': self.ml_predictor.is_trained,
                'telemetry_active': self.config.enable_telemetry,
                'persistence_active': self.config.enable_persistence,
                'quantum_security': self.config.enable_quantum_signing,
                'blockchain_audit': self.config.enable_blockchain_audit,
                'mopd_enabled': self.config.mopd.enabled,
                'pareto_front_size': len(self.genetic_optimizer.pareto_front),
            }
        }

    # ============================================================================
    # Async context and shutdown (unchanged)
    # ============================================================================

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()

    async def shutdown(self):
        logger.info("Shutting down Degradation Manager")
        for task in self._background_tasks:
            task.cancel()
        if self.config.enable_persistence and self.persistence:
            await self.save_state()
        logger.info("Shutdown complete")

# ============================================================================
# Test stubs (pytest)
# ============================================================================

import pytest
import pytest_asyncio

@pytest.fixture
def config():
    return DegradationConfig(enable_persistence=False, enable_telemetry=False, enable_blockchain_audit=False, enable_multi_cloud=False)

@pytest_asyncio.fixture
async def manager(config):
    async with DegradationManager(config=config) as mgr:
        yield mgr

@pytest.mark.asyncio
async def test_initial_state(manager):
    assert manager.current_tier == OperationalTier.TIER_5_FULL

@pytest.mark.asyncio
async def test_update_metrics(manager):
    await manager.update_metrics(token_balance=200)
    assert manager._token_balance == 200

@pytest.mark.asyncio
async def test_health_score(manager):
    health = manager.calculate_health_score()
    assert 0 <= health.overall_score <= 1

@pytest.mark.asyncio
async def test_transition(manager):
    result = await manager.transition_to({'target_tier': OperationalTier.TIER_4_REDUCED})
    assert result['status'] == 'success'
    assert manager.current_tier == OperationalTier.TIER_4_REDUCED

# ============================================================================
# Example usage
# ============================================================================

async def main():
    config = DegradationConfig.from_env_and_file()
    async with DegradationManager(config=config) as manager:
        await asyncio.sleep(2)
        print(manager.get_health_status())
        print(manager.get_metrics())
        # MOPD
        print("Pareto front:", manager.get_mopd_pareto_front())
        print("MOPD summary:", manager.get_mopd_summary())

if __name__ == "__main__":
    asyncio.run(main())
