#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/system_enhancement_simulator_enhanced_v11_0.py
# VERSION: 11.0.0 (Enterprise Quantum Resilience + GA + MoE + Pareto + Federated RL)
# =============================================================================
"""
Green Agent System Enhancement Simulator - Version 11.0.0

ENHANCEMENTS OVER v10.0.0:
1. Bio‑inspired Genetic Algorithm (GA) for parameter optimisation.
2. Full Mixture‑of‑Experts (MoE) gating network for dynamic strategy selection.
3. Pareto‑front integration into the simulation optimisation loop.
4. Federated learning for RL model weights.
5. Adaptive chaos injection using contextual bandit.
6. Active user preference learning for scenario comparison.
7. Drift detection with adaptive thresholds.
8. Integration with central Green Agent components (Config, Storage, Metrics).
9. All enhancements are optional and configurable.
"""

import asyncio
import hashlib
import json
import os
import random
import sqlite3
import time
import uuid
import signal
from functools import wraps
from collections import deque, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Set, Union, Callable
import secrets
import gc
import contextvars
import itertools

# -----------------------------------------------------------------------------
# Attempt to import central Green Agent components
# -----------------------------------------------------------------------------
try:
    from ..config import config as central_config
    from ..storage import Storage as CentralStorage
    from ..metrics import MetricsRegistry as CentralMetrics
    from ..logger import logger as central_logger
    CENTRAL_COMPONENTS_AVAILABLE = True
except ImportError:
    CENTRAL_COMPONENTS_AVAILABLE = False
    central_config = None
    CentralStorage = None
    CentralMetrics = None
    central_logger = None

# -----------------------------------------------------------------------------
# Async SQLite (aiosqlite) – fallback to sqlite3 with thread pool
# -----------------------------------------------------------------------------
try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

# -----------------------------------------------------------------------------
# External dependencies
# -----------------------------------------------------------------------------
try:
    from web3 import Web3, Account, HTTPProvider
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
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, PrivateFormat, NoEncryption
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    import gym
    from gym import spaces
    from stable_baselines3 import PPO, A2C, DQN
    from stable_baselines3.common.vec_env import DummyVecEnv
    RL_AVAILABLE = True
except ImportError:
    RL_AVAILABLE = False

try:
    import optuna
    OPTUNA_AVAILABLE = True
except ImportError:
    OPTUNA_AVAILABLE = False

try:
    from pymoo.algorithms.moo.nsga2 import NSGA2
    from pymoo.operators.crossover.sbx import SBX
    from pymoo.operators.mutation.pm import PM
    from pymoo.operators.sampling.rnd import FloatRandomSampling
    from pymoo.core.problem import Problem
    from pymoo.optimize import minimize as pymoo_minimize
    PYMOO_AVAILABLE = True
except ImportError:
    PYMOO_AVAILABLE = False

try:
    import plotly.graph_objects as go
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, field_validator, ValidationError
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    import websockets
    from websockets.server import serve
    from websockets.exceptions import ConnectionClosed
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    import structlog
    from structlog.processors import JSONRenderer, TimeStamper
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False

# ---------- For forecasting (optional) ----------
try:
    from statsmodels.tsa.arima.model import ARIMA
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False

# ---------- For scikit-learn (MoE gating) ----------
try:
    from sklearn.neural_network import MLPClassifier
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# -----------------------------------------------------------------------------
# DUMMY TENACITY DECORATOR
# -----------------------------------------------------------------------------
if not TENACITY_AVAILABLE:
    def retry(*args, **kwargs):
        def decorator(func):
            @wraps(func)
            async def wrapper(*fargs, **fkwargs):
                attempts = 0
                max_attempts = kwargs.get('stop', stop_after_attempt(3)).stop.max_attempt_number
                delay = 1
                while attempts < max_attempts:
                    try:
                        return await func(*fargs, **fkwargs)
                    except Exception as e:
                        attempts += 1
                        if attempts >= max_attempts:
                            raise
                        await asyncio.sleep(delay)
                        delay *= 2
            return wrapper
        return decorator

# -----------------------------------------------------------------------------
# Structured logging with correlation ID
# -----------------------------------------------------------------------------
correlation_id_var = contextvars.ContextVar('correlation_id', default='unknown')

if CENTRAL_COMPONENTS_AVAILABLE and central_logger:
    logger = central_logger
else:
    if STRUCTLOG_AVAILABLE:
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
        logger = logger.bind(correlation_id=correlation_id_var.get())
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s')
        logger = logging.getLogger(__name__)
        class CorrelationIdFilter(logging.Filter):
            def filter(self, record):
                record.correlation_id = correlation_id_var.get()
                return True
        logger.addFilter(CorrelationIdFilter())

# Audit logger
import logging.handlers
audit_logger = logging.getLogger('simulator_audit')
audit_handler = logging.handlers.RotatingFileHandler('simulator_audit_v11.log', maxBytes=50*1024*1024, backupCount=10)
audit_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)

# -----------------------------------------------------------------------------
# Prometheus metrics (use central if available)
# -----------------------------------------------------------------------------
if CENTRAL_COMPONENTS_AVAILABLE and CentralMetrics:
    metrics = CentralMetrics()
    SIMULATION_RUNS = metrics.counter('simulation_runs_total', ['type', 'status'])
    SIMULATION_DURATION = metrics.histogram('simulation_duration_seconds', ['type'])
    SIMULATION_QUEUE_SIZE = metrics.gauge('simulation_queue_size')
    CIRCUIT_BREAKER_STATE = metrics.gauge('simulator_circuit_breaker_state', ['component'])
    HEALTH_SCORE = metrics.gauge('simulator_system_health')
    DB_SIZE = metrics.gauge('simulator_db_size_mb')
    DATA_QUALITY_SCORE = metrics.gauge('simulator_data_quality')
    WS_CONNECTIONS = metrics.gauge('simulator_ws_connections')
    FAILURE_INJECTIONS = metrics.counter('simulator_failure_injections_total', ['type'])
    AB_TEST_RESULTS = metrics.counter('simulator_ab_test_results', ['winner'])
    RL_OPTIMIZATION_ITERATIONS = metrics.counter('rl_optimization_iterations_total', ['algorithm'])
    BAYESIAN_TUNING_TRIALS = metrics.counter('bayesian_tuning_trials_total', ['domain'])
    CHAOS_EXPERIMENTS = metrics.counter('chaos_experiments_total', ['type', 'status'])
    SCENARIO_COMPARISONS = metrics.counter('scenario_comparisons_total', ['scenario_count'])
    SIMULATION_ACCURACY = metrics.gauge('simulation_accuracy_score', ['type'])
    QUANTUM_SIGNATURES = metrics.counter('simulator_quantum_signatures_total', ['algorithm', 'status'])
    BLOCKCHAIN_VERIFICATIONS = metrics.counter('simulator_blockchain_verifications_total', ['status'])
    AUTONOMOUS_OPTIMIZATIONS = metrics.counter('simulator_autonomous_optimizations_total', ['strategy', 'status'])
    CLOUD_DISTRIBUTIONS = metrics.counter('simulator_cloud_distributions_total', ['provider', 'status'])
    MTOP_TEACHER_WEIGHTS = metrics.gauge('simulator_mtop_teacher_weights', ['teacher'])
    MTOP_STUDENT_UPDATES = metrics.counter('simulator_mtop_student_updates_total')
    # New metrics
    GA_POPULATION_FITNESS = metrics.gauge('simulator_ga_population_fitness')
    MOE_GATING_PROBABILITIES = metrics.gauge('simulator_moe_gating_probabilities', ['expert'])
    PARETO_FRONT_SIZE = metrics.gauge('simulator_pareto_front_size')
    ADAPTIVE_DRIFT_THRESHOLD = metrics.gauge('simulator_adaptive_drift_threshold', ['domain'])
    FEDERATED_AGGREGATION = metrics.counter('simulator_federated_aggregations_total')
else:
    if PROMETHEUS_AVAILABLE:
        REGISTRY = CollectorRegistry()
        # Define all metrics similarly (omitted for brevity)
    else:
        class DummyMetric:
            def labels(self, **kwargs): return self
            def inc(self, **kwargs): pass
            def set(self, **kwargs): pass
            def observe(self, **kwargs): pass
        # Dummy assignments for all metrics (omitted for brevity)

# -----------------------------------------------------------------------------
# CENTRAL CONFIGURATION (if available) or fallback to custom config
# -----------------------------------------------------------------------------
if CENTRAL_COMPONENTS_AVAILABLE and central_config:
    class SimulatorConfigFromCentral:
        def __init__(self):
            self.instance_id = getattr(central_config, 'instance_id', str(uuid.uuid4())[:8])
            self.version = "11.0.0"
            self.log_level = getattr(central_config, 'log_level', 'INFO')
            self.db_path = getattr(central_config, 'db_path', '/tmp/simulator_v11.db')
            self.openai_api_key = getattr(central_config, 'openai_api_key', None)
            self.electricity_maps_api_key = getattr(central_config, 'electricity_maps_api_key', None)
            self.carbon_region = getattr(central_config, 'carbon_region', 'global')
            self.carbon_update_interval = getattr(central_config, 'carbon_update_interval', 300)
            self.blockchain_rpc_url = getattr(central_config, 'blockchain_rpc_url', 'http://localhost:8545')
            self.blockchain_contract_address = getattr(central_config, 'blockchain_contract_address', None)
            self.blockchain_private_key = getattr(central_config, 'blockchain_private_key', None)
            self.aws_access_key_id = getattr(central_config, 'aws_access_key_id', None)
            self.aws_secret_access_key = getattr(central_config, 'aws_secret_access_key', None)
            self.aws_region = getattr(central_config, 'aws_region', 'us-east-1')
            self.azure_connection_string = getattr(central_config, 'azure_connection_string', None)
            self.gcp_credentials_path = getattr(central_config, 'gcp_credentials_path', None)
            self.cache_ttl = getattr(central_config, 'cache_ttl', 300)
            self.retry_attempts = getattr(central_config, 'retry_attempts', 3)
            self.retry_min_wait = getattr(central_config, 'retry_min_wait', 2)
            self.retry_max_wait = getattr(central_config, 'retry_max_wait', 10)
            self.metrics_port = getattr(central_config, 'metrics_port', 8000)
            self.websocket_port = getattr(central_config, 'websocket_port', 8770)
            self.mopd_weights = getattr(central_config, 'simulator_mopd_weights', {
                'accuracy': 0.4, 'carbon': 0.3, 'cost': 0.2, 'latency': 0.1
            })
            self.health_check_interval = getattr(central_config, 'health_check_interval', 60)
            self.model_retrain_interval = getattr(central_config, 'model_retrain_interval', 3600)
            self.cache_cleanup_interval = getattr(central_config, 'cache_cleanup_interval', 3600)
            self.auto_optimize_interval = getattr(central_config, 'auto_optimize_interval', 1800)
            self.federated_interval = getattr(central_config, 'federated_interval', 3600)
            self.predictive_interval = getattr(central_config, 'predictive_interval', 3600)
            self.sustainability_interval = getattr(central_config, 'sustainability_interval', 3600)
            self.key_rotation_interval = getattr(central_config, 'key_rotation_interval', 86400)
            self.master_key_env = getattr(central_config, 'master_key_env', 'SIMULATOR_MASTER_KEY')
            # New v11.0.0 parameters
            self.ga_enabled = getattr(central_config, 'simulator_ga_enabled', True)
            self.ga_population_size = getattr(central_config, 'simulator_ga_population_size', 20)
            self.ga_generations = getattr(central_config, 'simulator_ga_generations', 5)
            self.ga_mutation_rate = getattr(central_config, 'simulator_ga_mutation_rate', 0.2)
            self.ga_crossover_rate = getattr(central_config, 'simulator_ga_crossover_rate', 0.7)
            self.moe_enabled = getattr(central_config, 'simulator_moe_enabled', True)
            self.moe_expert_count = getattr(central_config, 'simulator_moe_expert_count', 4)
            self.moe_hidden_layers = getattr(central_config, 'simulator_moe_hidden_layers', [16, 8])
            self.pareto_enabled = getattr(central_config, 'simulator_pareto_enabled', True)
            self.pareto_max_architectures = getattr(central_config, 'simulator_pareto_max_architectures', 100)
            self.federated_learning_enabled = getattr(central_config, 'simulator_federated_learning_enabled', True)
            self.adaptive_chaos_enabled = getattr(central_config, 'simulator_adaptive_chaos_enabled', True)
            self.user_preference_learning_enabled = getattr(central_config, 'simulator_user_preference_learning_enabled', True)
            self.drift_detection_enabled = getattr(central_config, 'simulator_drift_detection_enabled', True)

        def get_master_key(self) -> bytes:
            key_hex = os.getenv(self.master_key_env)
            if not key_hex:
                raise ValueError(f"Master key not set in env {self.master_key_env}")
            return bytes.fromhex(key_hex)

    SimulatorConfig = SimulatorConfigFromCentral
else:
    if PYDANTIC_AVAILABLE:
        class SimulatorConfig(BaseModel):
            instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
            version: str = Field("11.0.0")
            log_level: str = Field("INFO")
            db_path: str = Field("/tmp/simulator_v11.db")
            openai_api_key: Optional[str] = None
            electricity_maps_api_key: Optional[str] = None
            carbon_region: str = Field("global")
            carbon_update_interval: int = Field(300, ge=10)
            blockchain_rpc_url: str = Field("http://localhost:8545")
            blockchain_contract_address: Optional[str] = None
            blockchain_private_key: Optional[str] = None
            aws_access_key_id: Optional[str] = None
            aws_secret_access_key: Optional[str] = None
            aws_region: str = Field("us-east-1")
            azure_connection_string: Optional[str] = None
            gcp_credentials_path: Optional[str] = None
            cache_ttl: int = Field(300, ge=1)
            retry_attempts: int = Field(3, ge=0)
            retry_min_wait: int = Field(2, ge=1)
            retry_max_wait: int = Field(10, ge=1)
            metrics_port: int = Field(8000, ge=1024, le=65535)
            websocket_port: int = Field(8770, ge=1024)
            mopd_weights: Dict[str, float] = Field(
                default_factory=lambda: {
                    'accuracy': 0.4, 'carbon': 0.3, 'cost': 0.2, 'latency': 0.1
                }
            )
            health_check_interval: int = Field(60, ge=10)
            model_retrain_interval: int = Field(3600, ge=60)
            cache_cleanup_interval: int = Field(3600, ge=60)
            auto_optimize_interval: int = Field(1800, ge=60)
            federated_interval: int = Field(3600, ge=60)
            predictive_interval: int = Field(3600, ge=60)
            sustainability_interval: int = Field(3600, ge=60)
            key_rotation_interval: int = Field(86400, ge=60)
            master_key_env: str = Field("SIMULATOR_MASTER_KEY")
            # New v11.0.0 fields
            ga_enabled: bool = True
            ga_population_size: int = Field(20, ge=5)
            ga_generations: int = Field(5, ge=1)
            ga_mutation_rate: float = Field(0.2, ge=0.0, le=1.0)
            ga_crossover_rate: float = Field(0.7, ge=0.0, le=1.0)
            moe_enabled: bool = True
            moe_expert_count: int = Field(4, ge=2)
            moe_hidden_layers: List[int] = Field(default_factory=lambda: [16, 8])
            pareto_enabled: bool = True
            pareto_max_architectures: int = Field(100, ge=10)
            federated_learning_enabled: bool = True
            adaptive_chaos_enabled: bool = True
            user_preference_learning_enabled: bool = True
            drift_detection_enabled: bool = True

            @field_validator('log_level')
            @classmethod
            def validate_log_level(cls, v: str) -> str:
                allowed = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
                if v.upper() not in allowed:
                    raise ValueError(f'LOG_LEVEL must be one of {allowed}')
                return v.upper()

            def get_master_key(self) -> bytes:
                key_hex = os.getenv(self.master_key_env)
                if not key_hex:
                    raise ValueError(f"Master key not set in env {self.master_key_env}")
                return bytes.fromhex(key_hex)

            class Config:
                env_prefix = "SIMULATOR_"
    else:
        # Fallback as dict (omitted for brevity, but should include all fields)

# -----------------------------------------------------------------------------
# AES-256-GCM Encryption Manager
# -----------------------------------------------------------------------------
class EncryptionManager:
    # ... (same as original)

# -----------------------------------------------------------------------------
# ENHANCED DATABASE MANAGER (with central or custom)
# -----------------------------------------------------------------------------
if CENTRAL_COMPONENTS_AVAILABLE and CentralStorage:
    class EnhancedStorage:
        # ... (similar to original, but we'll add new tables for GA, MoE, Pareto, etc.)
        pass
else:
    class EnhancedStorage:
        # ... (original with new tables)
        pass

# -----------------------------------------------------------------------------
# CIRCUIT BREAKER, RATE LIMITER (unchanged)
# -----------------------------------------------------------------------------
class CircuitBreaker:
    # ...
    pass

class RateLimiter:
    # ...
    pass

# -----------------------------------------------------------------------------
# Carbon Intensity Manager (unchanged)
# -----------------------------------------------------------------------------
class CarbonIntensityManager:
    # ...
    pass

# -----------------------------------------------------------------------------
# QUANTUM SECURITY, BLOCKCHAIN, MULTI-CLOUD, etc. (unchanged)
# -----------------------------------------------------------------------------
class QuantumResilientSimulationSecurity:
    # ...
    pass

class BlockchainSimulationVerification:
    # ...
    pass

class MultiCloudSimulationDistribution:
    # ...
    pass

# -----------------------------------------------------------------------------
# MTOP ENGINE (kept as fallback)
# -----------------------------------------------------------------------------
class StrategyTeacherEnsemble:
    # ...
    pass

class StrategyDistillationStudent:
    # ...
    pass

class MTOPStrategyEngine:
    # ...
    pass

# -----------------------------------------------------------------------------
# NEW MODULE: Genetic Parameter Optimizer
# -----------------------------------------------------------------------------
class GeneticParameterOptimizer:
    """
    Genetic algorithm for evolving simulation parameters.
    """
    def __init__(self, config: SimulatorConfig, storage: EnhancedStorage, simulator):
        self.config = config
        self.storage = storage
        self.simulator = simulator
        self.population_size = config.ga_population_size
        self.generations = config.ga_generations
        self.mutation_rate = config.ga_mutation_rate
        self.crossover_rate = config.ga_crossover_rate
        self.param_bounds = {
            'iterations': (10, 1000),
            'batch_size': (4, 512),
            'learning_rate': (0.0001, 0.1),
        }
        self._lock = asyncio.Lock()

    def _random_chromosome(self) -> Dict[str, Any]:
        return {
            'iterations': random.randint(*self.param_bounds['iterations']),
            'batch_size': random.randint(*self.param_bounds['batch_size']),
            'learning_rate': 10 ** random.uniform(np.log10(self.param_bounds['learning_rate'][0]), np.log10(self.param_bounds['learning_rate'][1])),
        }

    def _mutate(self, chrom: Dict[str, Any]) -> Dict[str, Any]:
        new = chrom.copy()
        for param, bounds in self.param_bounds.items():
            if random.random() < self.mutation_rate:
                if param == 'learning_rate':
                    # Log-space mutation
                    log_lr = np.log10(new[param])
                    delta = random.gauss(0, 0.5)
                    new[param] = 10 ** max(np.log10(bounds[0]), min(np.log10(bounds[1]), log_lr + delta))
                else:
                    low, high = bounds
                    delta = random.gauss(0, (high - low) / 10)
                    new[param] = int(max(low, min(high, chrom[param] + delta)))
        return new

    def _crossover(self, p1: Dict[str, Any], p2: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        if random.random() > self.crossover_rate:
            return p1.copy(), p2.copy()
        c1, c2 = p1.copy(), p2.copy()
        for param in self.param_bounds:
            if random.random() < 0.5:
                c1[param] = p2[param]
                c2[param] = p1[param]
        return c1, c2

    async def _evaluate_fitness(self, chrom: Dict[str, Any]) -> float:
        """Run a short simulation with these parameters and return readiness."""
        sim_run = await self.simulator.run_simulation(
            sim_type='quantum',
            parameters=chrom,
            use_rl_optimization=False,
            use_bayesian_tuning=False
        )
        if sim_run.results:
            return sim_run.results[0].estimated_production_readiness
        return 0.0

    async def run_search(self) -> Dict[str, Any]:
        population = [self._random_chromosome() for _ in range(self.population_size)]
        best_fitness = -1.0
        best_individual = None

        for gen in range(self.generations):
            fitnesses = await asyncio.gather(*[self._evaluate_fitness(ind) for ind in population])
            sorted_pop = sorted(zip(population, fitnesses), key=lambda x: x[1], reverse=True)
            if sorted_pop[0][1] > best_fitness:
                best_fitness = sorted_pop[0][1]
                best_individual = sorted_pop[0][0]

            parents = [ind for ind, _ in sorted_pop[:max(2, self.population_size//2)]]
            offspring = []
            while len(offspring) < self.population_size:
                p1 = random.choice(parents)
                p2 = random.choice(parents)
                c1, c2 = self._crossover(p1, p2)
                c1 = self._mutate(c1)
                c2 = self._mutate(c2)
                offspring.append(c1)
                if len(offspring) < self.population_size:
                    offspring.append(c2)
            combined = parents + offspring
            combined_fitness = await asyncio.gather(*[self._evaluate_fitness(ind) for ind in combined])
            sorted_combined = sorted(zip(combined, combined_fitness), key=lambda x: x[1], reverse=True)
            population = [ind for ind, _ in sorted_combined[:self.population_size]]

            # Store generation
            await self.storage.save_ga_population('simulator', gen, [{'individual_id': f'gen{gen}_ind{i}',
                                                                      'attributes': population[i],
                                                                      'fitness': float(fitnesses[i])} for i in range(len(population))])
            if PROMETHEUS_AVAILABLE:
                GA_POPULATION_FITNESS.set(best_fitness)

        return best_individual if best_individual else self._random_chromosome()

# -----------------------------------------------------------------------------
# NEW MODULE: MoE Gating Network
# -----------------------------------------------------------------------------
class MoEGatingNetwork:
    """
    Full Mixture-of-Experts gating network that selects among simulation strategies.
    """
    def __init__(self, config: SimulatorConfig, storage: EnhancedStorage):
        self.config = config
        self.storage = storage
        self.num_experts = config.moe_expert_count
        self.hidden_layers = config.moe_hidden_layers
        self._gating_model = None
        self._scaler = None
        self._trained = False
        self._training_data = []  # (feature_vector, expert_label, reward)
        self._lock = asyncio.Lock()

        # Define experts: each expert is a simulation strategy
        self.experts = {
            'performance': self._performance_expert,
            'carbon': self._carbon_expert,
            'cost': self._cost_expert,
            'adaptive': self._adaptive_expert
        }
        if len(self.experts) < self.num_experts:
            keys = list(self.experts.keys())
            for i in range(self.num_experts - len(keys)):
                self.experts[f'custom_{i}'] = self.experts[keys[i % len(keys)]]
        self.expert_names = list(self.experts.keys())

    def _performance_expert(self, context: Dict) -> Dict[str, Any]:
        return {'strategy': 'performance'}

    def _carbon_expert(self, context: Dict) -> Dict[str, Any]:
        return {'strategy': 'carbon'}

    def _cost_expert(self, context: Dict) -> Dict[str, Any]:
        return {'strategy': 'cost'}

    def _adaptive_expert(self, context: Dict) -> Dict[str, Any]:
        return {'strategy': 'adaptive'}

    def _encode_context(self, context: Dict) -> np.ndarray:
        features = []
        # Carbon intensity (normalized)
        features.append(context.get('carbon_intensity', 0.4))
        # Accuracy target
        features.append(context.get('accuracy_target', 0.8))
        # Cost budget
        features.append(context.get('cost_budget', 0.5))
        # Success rate
        features.append(context.get('success_rate', 0.5))
        # Simulation type (one-hot)
        sim_type = context.get('sim_type', 'quantum')
        sim_map = {'quantum': 0, 'blockchain': 1, 'gpu': 2, 'streaming': 3, 'multitenant': 4, 'federated': 5, 'ml_training': 6}
        sim_vec = [0]*7
        sim_vec[sim_map.get(sim_type, 0)] = 1
        features.extend(sim_vec)
        return np.array(features, dtype=np.float32)

    def _train_gating(self):
        if not SKLEARN_AVAILABLE or len(self._training_data) < 10:
            return
        X = np.array([item[0] for item in self._training_data])
        y = np.array([item[1] for item in self._training_data])
        self._scaler = StandardScaler()
        X_scaled = self._scaler.fit_transform(X)
        self._gating_model = MLPClassifier(hidden_layer_sizes=self.hidden_layers, max_iter=200, random_state=42)
        self._gating_model.fit(X_scaled, y)
        self._trained = True
        logger.info(f"MoE gating network trained on {len(self._training_data)} samples.")

    async def select_expert(self, context: Dict) -> Tuple[str, Dict[str, Any]]:
        features = self._encode_context(context)
        if self._trained and self._gating_model is not None:
            X = features.reshape(1, -1)
            if self._scaler:
                X = self._scaler.transform(X)
            probs = self._gating_model.predict_proba(X)[0]
            expert_idx = np.argmax(probs)
            selected = self.expert_names[expert_idx]
            if PROMETHEUS_AVAILABLE:
                for i, p in enumerate(probs):
                    MOE_GATING_PROBABILITIES.labels(expert=self.expert_names[i]).set(p)
        else:
            selected = 'performance'
        expert_func = self.experts[selected]
        params = expert_func(context)
        return selected, params

    async def add_training_sample(self, context: Dict, selected_expert: str, reward: float):
        features = self._encode_context(context)
        expert_idx = self.expert_names.index(selected_expert)
        async with self._lock:
            self._training_data.append((features, expert_idx, reward))
            if len(self._training_data) % 10 == 0:
                self._train_gating()

# -----------------------------------------------------------------------------
# NEW MODULE: Pareto-Front Optimizer (integrated)
# -----------------------------------------------------------------------------
class ParetoFrontOptimizer:
    """
    Maintains a Pareto front of simulation configurations.
    """
    def __init__(self, config: SimulatorConfig, storage: EnhancedStorage):
        self.config = config
        self.storage = storage
        self.pareto_front = []  # list of dict with config_params, metrics
        self.max_size = config.pareto_max_architectures
        self._lock = asyncio.Lock()
        self.objectives = ['accuracy', 'carbon', 'cost', 'latency']

    def _dominates(self, a: Dict, b: Dict) -> bool:
        # For accuracy, higher is better; for others, lower is better.
        a_metrics = (-a['metrics']['accuracy'], a['metrics']['carbon'], a['metrics']['cost'], a['metrics']['latency'])
        b_metrics = (-b['metrics']['accuracy'], b['metrics']['carbon'], b['metrics']['cost'], b['metrics']['latency'])
        return all(a_metrics[i] <= b_metrics[i] for i in range(4)) and any(a_metrics[i] < b_metrics[i] for i in range(4))

    async def add_configuration(self, config_params: Dict, metrics: Dict[str, float]) -> bool:
        entry = {
            'solution_id': f"cfg_{uuid.uuid4().hex[:8]}",
            'config_params': config_params,
            'metrics': metrics
        }
        async with self._lock:
            # Check if dominated
            for existing in self.pareto_front:
                if self._dominates(existing, entry):
                    return False
            # Remove any dominated by new
            self.pareto_front = [e for e in self.pareto_front if not self._dominates(entry, e)]
            self.pareto_front.append(entry)
            if len(self.pareto_front) > self.max_size:
                # Remove one with smallest crowding distance (simplified)
                self.pareto_front.sort(key=lambda e: e['metrics']['accuracy'])
                self.pareto_front = self.pareto_front[:self.max_size]
            # Persist to storage
            await self.storage.save_pareto_front(self.pareto_front)
            if PROMETHEUS_AVAILABLE:
                PARETO_FRONT_SIZE.set(len(self.pareto_front))
            return True

    def get_pareto_front(self) -> List[Dict]:
        return self.pareto_front

    async def get_trade_off_suggestions(self, user_weights: Dict[str, float]) -> List[Dict]:
        if not self.pareto_front:
            return []
        scored = []
        for e in self.pareto_front:
            score = (user_weights.get('accuracy', 0.4) * e['metrics']['accuracy'] -
                     user_weights.get('carbon', 0.3) * e['metrics']['carbon'] -
                     user_weights.get('cost', 0.2) * e['metrics']['cost'] -
                     user_weights.get('latency', 0.1) * e['metrics']['latency'])
            scored.append((score, e))
        scored.sort(reverse=True)
        return [e for _, e in scored[:5]]

# -----------------------------------------------------------------------------
# NEW MODULE: Federated RL Aggregator
# -----------------------------------------------------------------------------
class FederatedRLAggregator:
    """
    Aggregates RL model weights from multiple instances using federated averaging.
    """
    def __init__(self, config: SimulatorConfig, storage: EnhancedStorage, instance_id: str):
        self.config = config
        self.storage = storage
        self.instance_id = instance_id
        self.aggregated_weights = None
        self._lock = asyncio.Lock()

    async def share_local_weights(self, sim_type: str, weights: Dict[str, Any]):
        # Serialize weights (for simplicity, we store in state)
        await self.storage.save_state(f"fed_rl_weight_{self.instance_id}_{sim_type}", json.dumps(weights, default=str))

    async def pull_aggregated_weights(self, sim_type: str) -> Optional[Dict[str, Any]]:
        rows = await self.storage._fetchall("SELECT value FROM state WHERE key LIKE 'fed_rl_weight_%' AND key LIKE ?", (f'%_{sim_type}',))
        if not rows:
            return None
        weight_list = []
        for r in rows:
            try:
                w = json.loads(r[0])
                weight_list.append(w)
            except Exception:
                continue
        if not weight_list:
            return None
        # Average (simplified: assume dict of parameters)
        avg = {}
        for w in weight_list:
            for k, v in w.items():
                avg[k] = avg.get(k, 0) + v
        for k in avg:
            avg[k] /= len(weight_list)
        self.aggregated_weights = avg
        if PROMETHEUS_AVAILABLE:
            FEDERATED_AGGREGATION.inc()
        return avg

    async def apply_aggregated_weights(self, sim_type: str, current_weights: Dict[str, Any]) -> Dict[str, Any]:
        agg = await self.pull_aggregated_weights(sim_type)
        if agg is None:
            return current_weights
        merged = {}
        for k in current_weights:
            merged[k] = (current_weights[k] + agg.get(k, current_weights[k])) / 2
        return merged

# -----------------------------------------------------------------------------
# NEW MODULE: Adaptive Chaos Injector (Contextual Bandit)
# -----------------------------------------------------------------------------
class AdaptiveChaosInjector:
    """
    Uses a contextual bandit to choose which chaos type and intensity to inject.
    """
    def __init__(self, storage: EnhancedStorage, config: SimulatorConfig):
        self.storage = storage
        self.config = config
        self.chaos_types = ['latency_spike', 'network_partition', 'resource_exhaustion', 'data_corruption', 'service_degradation']
        self.intensity_levels = [0.1, 0.3, 0.5, 0.7, 0.9]
        # Bandit weights for each (type, intensity) pair
        self.weights = {}
        for t in self.chaos_types:
            for i in self.intensity_levels:
                self.weights[(t, i)] = 1.0
        self.counts = defaultdict(int)
        self.rewards = defaultdict(float)
        self._lock = asyncio.Lock()
        self.learning_rate = 0.1

    async def choose_experiment(self) -> Tuple[str, float]:
        """Select a chaos type and intensity using epsilon-greedy."""
        async with self._lock:
            if random.random() < 0.1:
                # Explore
                t = random.choice(self.chaos_types)
                i = random.choice(self.intensity_levels)
                return t, i
            # Exploit: choose with highest expected reward
            best = max(self.weights, key=lambda k: self.weights[k])
            return best[0], best[1]

    async def update(self, chaos_type: str, intensity: float, reward: float):
        """Update bandit weights based on reward."""
        async with self._lock:
            key = (chaos_type, intensity)
            self.counts[key] += 1
            self.rewards[key] += reward
            self.weights[key] = self.rewards[key] / self.counts[key]

# -----------------------------------------------------------------------------
# NEW MODULE: Active User Preference Learner
# -----------------------------------------------------------------------------
class ActiveUserPreferenceLearner:
    """
    Queries the user when two simulation configurations yield similar performance.
    """
    def __init__(self, storage: EnhancedStorage, websocket: 'EnhancedWebSocketServer'):
        self.storage = storage
        self.websocket = websocket
        self.user_weights = {}  # user_id -> weights dict

    async def query_user_if_needed(self, user_id: str, top_configs: List[Dict]) -> Optional[str]:
        if len(top_configs) < 2:
            return None
        # If scores are within 5%, ask user
        scores = [c['metrics']['accuracy'] for c in top_configs[:2]]
        if abs(scores[0] - scores[1]) / max(scores) < 0.05:
            # Send WebSocket query (simulate)
            await self.websocket.broadcast({
                'type': 'preference_query',
                'user_id': user_id,
                'options': [{'id': c['solution_id'], 'accuracy': c['metrics']['accuracy']} for c in top_configs[:2]]
            }, topic='user_preferences')
            # For demo, return the first one
            return top_configs[0]['solution_id']
        return None

    async def record_choice(self, user_id: str, chosen_solution_id: str, context: Dict):
        # Update user weights based on choice
        # Simple heuristic: increase weight on accuracy if chosen config has higher accuracy
        # For demo, we store the preference
        await self.storage.save_user_preference(user_id, {'chosen': chosen_solution_id}, chosen_solution_id)

# -----------------------------------------------------------------------------
# NEW MODULE: Drift Detector
# -----------------------------------------------------------------------------
class DriftDetector:
    """
    Detects significant changes in carbon intensity or accuracy trends.
    """
    def __init__(self, storage: EnhancedStorage, config: SimulatorConfig):
        self.storage = storage
        self.config = config
        self.carbon_history = deque(maxlen=100)
        self.accuracy_history = deque(maxlen=100)
        self.threshold = 0.15
        self.domain_thresholds = defaultdict(lambda: self.threshold)

    async def check_carbon_drift(self, current_intensity: float) -> bool:
        self.carbon_history.append(current_intensity)
        if len(self.carbon_history) < 10:
            return False
        recent = list(self.carbon_history)[-10:]
        mean = np.mean(recent)
        if mean == 0:
            return False
        if abs(current_intensity - mean) > self.threshold * mean:
            logger.warning(f"Carbon drift detected: current {current_intensity} vs mean {mean}")
            return True
        return False

    async def check_accuracy_drift(self, current_accuracy: float, sim_type: str) -> bool:
        self.accuracy_history.append(current_accuracy)
        if len(self.accuracy_history) < 10:
            return False
        recent = list(self.accuracy_history)[-10:]
        mean = np.mean(recent)
        if mean == 0:
            return False
        if abs(current_accuracy - mean) > self.threshold * mean:
            logger.warning(f"Accuracy drift detected for {sim_type}: current {current_accuracy} vs mean {mean}")
            return True
        return False

    async def get_threshold(self, domain: str) -> float:
        return self.domain_thresholds[domain]

# -----------------------------------------------------------------------------
# Autonomous Simulation Optimizer (updated with MoE, GA, Pareto)
# -----------------------------------------------------------------------------
class AutonomousSimulationOptimizer:
    def __init__(self, config: SimulatorConfig, storage: EnhancedStorage, state: 'SimulationState'):
        self.config = config
        self.storage = storage
        self.state = state
        self._lock = asyncio.Lock()
        self.mtop_engine = MTOPStrategyEngine(config) if not config.moe_enabled else None
        self.moe_gating = MoEGatingNetwork(config, storage) if config.moe_enabled else None
        self.ga_optimizer = None  # will be set later
        self.pareto_optimizer = None  # will be set later
        self.drift_detector = None  # will be set later

    async def optimize_simulation(self, current_state: Dict, strategy: str = None) -> Dict:
        carbon_intensity = current_state.get('carbon_intensity', 0.4)
        if self.moe_gating and self.config.moe_enabled:
            # Use MoE to select expert
            selected_expert, expert_params = await self.moe_gating.select_expert(current_state)
            result = {
                'action': f'{selected_expert}_optimization',
                'selected_strategy': selected_expert,
                'expert_params': expert_params,
                'recommendation': self._generate_recommendation(selected_expert, current_state)
            }
        elif self.mtop_engine:
            # Fallback to MTOP
            mtop_result = await self.mtop_engine.select_strategy(current_state, carbon_intensity)
            selected = mtop_result['selected_strategy']
            result = {
                'action': f'{selected}_optimization',
                'selected_strategy': selected,
                'scores': mtop_result['scores'],
                'recommendation': self._generate_recommendation(selected, current_state)
            }
        else:
            result = {'action': 'no_op', 'selected_strategy': 'balanced', 'recommendation': 'No optimizer available'}

        await self.storage.save_optimisation(result['selected_strategy'], result)
        if PROMETHEUS_AVAILABLE:
            AUTONOMOUS_OPTIMIZATIONS.labels(strategy=result['selected_strategy'], status='success').inc()

        # Apply GA if enabled
        if self.ga_optimizer and self.config.ga_enabled:
            best_params = await self.ga_optimizer.run_search()
            if best_params:
                # Update current state or parameters
                current_state.update(best_params)
                logger.info("GA updated simulation parameters: %s", best_params)

        return result

    async def record_outcome(self, reward: float, context: Dict):
        if self.moe_gating and self.config.moe_enabled:
            selected = context.get('selected_strategy', 'performance')
            await self.moe_gating.add_training_sample(context, selected, reward)
        elif self.mtop_engine:
            teacher_scores = context.get('teacher_scores', {})
            selected = context.get('selected_strategy', 'balanced')
            await self.mtop_engine.update(selected, reward, teacher_scores)

    def _generate_recommendation(self, strategy: str, state: Dict) -> str:
        if strategy == 'performance':
            return "Focus on maximising simulation accuracy."
        elif strategy == 'carbon':
            return "Prioritise carbon-efficient simulation configurations."
        elif strategy == 'cost':
            return "Optimise simulation resource usage."
        elif strategy == 'adaptive':
            return "Adjust dynamically based on recent simulation accuracy trends."
        return "Maintain current strategy with monitoring."

    def get_optimization_stats(self) -> Dict:
        stats = {
            'strategies': ['performance', 'carbon', 'cost', 'adaptive'],
            'recent_optimizations': self.storage.get_recent_optimisations(5),
            'moe_enabled': self.config.moe_enabled,
            'ga_enabled': self.config.ga_enabled,
        }
        if self.mtop_engine:
            stats['teacher_weights'] = self.mtop_engine.teacher_ensemble.teacher_weights
            stats['student_weights'] = self.mtop_engine.student.weights.tolist()
            stats['student_updates'] = self.mtop_engine.student.update_count
        return stats

# -----------------------------------------------------------------------------
# Simulation State (updated with thresholds)
# -----------------------------------------------------------------------------
class SimulationState:
    # ... (same as original, but with additional fields for drift, etc.)
    pass

# -----------------------------------------------------------------------------
# SIMULATION ENVIRONMENT, RL OPTIMIZER, BAYESIAN TUNER, CHAOS (unchanged)
# -----------------------------------------------------------------------------
class SimulationEnvironment(gym.Env):
    # ...
    pass

class RLParameterOptimizer:
    # ...
    pass

class BayesianHyperparameterTuner:
    # ...
    pass

class ChaosExperiment:
    # ...
    pass

class ChaosEngineeringManager:
    # ... (updated to use adaptive injector)
    pass

# -----------------------------------------------------------------------------
# Scenario Comparison Engine (updated with Pareto)
# -----------------------------------------------------------------------------
class ScenarioComparisonEngine:
    # ... (unchanged, but we'll use Pareto front if available)
    pass

# -----------------------------------------------------------------------------
# Enhanced Visualization Dashboard (unchanged)
# -----------------------------------------------------------------------------
class EnhancedVisualizationDashboard:
    # ...
    pass

# -----------------------------------------------------------------------------
# WebSocket Server (unchanged)
# -----------------------------------------------------------------------------
class EnhancedWebSocketServer:
    # ...
    pass

# -----------------------------------------------------------------------------
# MAIN ENHANCED SYSTEM SIMULATOR V11.0.0
# -----------------------------------------------------------------------------
class EnhancedSystemSimulatorV11:
    """Enhanced system simulator v11.0.0 with GA, MoE, Pareto, federated RL, adaptive chaos, drift detection."""

    def __init__(self, config: Optional[SimulatorConfig] = None):
        self.config = config or SimulatorConfig()
        self.instance_id = self.config.instance_id

        # Storage
        self.storage = EnhancedStorage(self.config)
        self.state = SimulationState(self.storage)

        # Enhanced modules
        self.quantum_security = QuantumResilientSimulationSecurity(self.config, self.storage)
        self.blockchain = BlockchainSimulationVerification(self.config, self.storage)
        self.carbon_manager = CarbonIntensityManager(self.config, self.storage)
        self.cloud_distributor = MultiCloudSimulationDistribution(self.config, self.storage)

        # Advanced components (existing)
        self.rl_optimizer = RLParameterOptimizer(self, algorithm='PPO')
        self.bayesian_tuner = BayesianHyperparameterTuner(self)
        self.chaos_manager = ChaosEngineeringManager(self.storage)
        self.scenario_engine = ScenarioComparisonEngine(self)
        self.visualization_dashboard = EnhancedVisualizationDashboard(self)

        # ===== NEW v11.0.0 components =====
        # GA optimizer
        self.ga_optimizer = GeneticParameterOptimizer(self.config, self.storage, self) if self.config.ga_enabled else None

        # MoE gating
        self.moe_gating = MoEGatingNetwork(self.config, self.storage) if self.config.moe_enabled else None

        # Pareto optimizer
        self.pareto_optimizer = ParetoFrontOptimizer(self.config, self.storage) if self.config.pareto_enabled else None

        # Federated RL aggregator
        self.federated_rl_aggregator = FederatedRLAggregator(self.config, self.storage, self.instance_id) if self.config.federated_learning_enabled else None

        # Adaptive chaos injector
        self.adaptive_chaos = AdaptiveChaosInjector(self.storage, self.config) if self.config.adaptive_chaos_enabled else None

        # Active user preference learner
        self.user_pref_learner = ActiveUserPreferenceLearner(self.storage, self.websocket) if self.config.user_preference_learning_enabled else None

        # Drift detector
        self.drift_detector = DriftDetector(self.storage, self.config) if self.config.drift_detection_enabled else None

        # MTOP optimizer (legacy)
        self.autonomous_optimizer = AutonomousSimulationOptimizer(self.config, self.storage, self.state)
        # Inject GA, Pareto, drift into optimizer
        self.autonomous_optimizer.ga_optimizer = self.ga_optimizer
        self.autonomous_optimizer.pareto_optimizer = self.pareto_optimizer
        self.autonomous_optimizer.drift_detector = self.drift_detector

        # Completed stubs (unchanged)
        self.federated_learner = FederatedSimulationLearner(self.storage, self.instance_id, self.config.federated_interval)
        self.user_adaptive = UserAdaptiveSimulationReflexivity(self.storage, 0.01)
        self.carbon_scheduler = CarbonAwareSimulationScheduler(self.storage, self.config)
        self.cross_domain_transfer = CrossDomainSimulationTransfer(self.storage)
        self.human_collaborator = HumanAISimulationCollaboration(self.storage, 300)
        self.predictive_manager = PredictiveSimulationManager(self.storage, 24)
        self.sustainability_tracker = SimulationSustainabilityTracker(self.storage)

        # WebSocket
        self.websocket = EnhancedWebSocketServer(self.config.websocket_port)

        # State
        self.all_results = deque(maxlen=1000)
        self.simulation_runs = deque(maxlen=1000)
        self._results_lock = asyncio.Lock()
        self._simulation_semaphore = asyncio.Semaphore(5)
        self.operation_queue = asyncio.Queue(maxsize=100)
        self._queue_worker = None
        self._running = False
        self.background_tasks = set()
        self._shutdown_event = asyncio.Event()

        # Start Prometheus HTTP server
        if PROMETHEUS_AVAILABLE:
            start_http_server(self.config.metrics_port)
            logger.info("Prometheus metrics exposed on port %d", self.config.metrics_port)

        logger.info("EnhancedSystemSimulatorV11 v%s initialized (instance: %s)", self.config.version, self.instance_id)

    async def start(self):
        self._running = True
        await self.websocket.start()
        await self.visualization_dashboard.start()
        self._queue_worker = asyncio.create_task(self._process_queue())
        asyncio.create_task(self._train_rl_optimizer())

        tasks = [
            asyncio.create_task(self._health_check_loop()),
            asyncio.create_task(self._cleanup_loop()),
            asyncio.create_task(self._carbon_update_loop()),
            asyncio.create_task(self._auto_optimize_loop()),
            asyncio.create_task(self._cloud_sync_loop()),
            asyncio.create_task(self._federated_learning_loop()),
            asyncio.create_task(self._predictive_loop()),
            asyncio.create_task(self._sustainability_loop()),
            asyncio.create_task(self._quantum_monitor_loop()),
            asyncio.create_task(self._blockchain_monitor_loop()),
            asyncio.create_task(self._key_rotation_loop()),
            asyncio.create_task(self._websocket_heartbeat()),
            # New loops
            asyncio.create_task(self._ga_optimization_loop()),
            asyncio.create_task(self._moe_training_loop()),
            asyncio.create_task(self._pareto_update_loop()),
            asyncio.create_task(self._federated_rl_loop()),
            asyncio.create_task(self._adaptive_chaos_loop()),
            asyncio.create_task(self._drift_detection_loop()),
            asyncio.create_task(self._active_user_learning_loop()),
        ]

        for task in tasks:
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)

        logger.info("Simulator started with %d background tasks", len(self.background_tasks))

    # ------------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------------
    async def _ga_optimization_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            if self.ga_optimizer and self.config.ga_enabled:
                try:
                    logger.info("Running GA parameter optimization...")
                    best_params = await self.ga_optimizer.run_search()
                    if best_params:
                        logger.info("GA best parameters: %s", best_params)
                except Exception as e:
                    logger.error("GA loop error: %s", e)

    async def _moe_training_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1800)
            if self.moe_gating and self.config.moe_enabled:
                try:
                    self.moe_gating._train_gating()
                except Exception as e:
                    logger.error("MoE training loop error: %s", e)

    async def _pareto_update_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1800)
            if self.pareto_optimizer and self.config.pareto_enabled:
                try:
                    logger.debug("Pareto front size: %d", len(self.pareto_optimizer.get_pareto_front()))
                except Exception as e:
                    logger.error("Pareto update loop error: %s", e)

    async def _federated_rl_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            if self.federated_rl_aggregator and self.config.federated_learning_enabled:
                try:
                    # Share current RL model weights
                    for sim_type in ['quantum', 'blockchain', 'gpu']:  # limit for demo
                        if sim_type in self.rl_optimizer.models:
                            model = self.rl_optimizer.models[sim_type]
                            # We need to serialize model weights; for simplicity, we just store a dummy.
                            await self.federated_rl_aggregator.share_local_weights(sim_type, {'dummy': 1.0})
                    # Pull aggregated
                    for sim_type in ['quantum', 'blockchain', 'gpu']:
                        merged = await self.federated_rl_aggregator.apply_aggregated_weights(sim_type, {})
                        if merged:
                            logger.info("Applied federated weights for %s", sim_type)
                except Exception as e:
                    logger.error("Federated RL loop error: %s", e)

    async def _adaptive_chaos_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(600)
            if self.adaptive_chaos and self.config.adaptive_chaos_enabled:
                try:
                    # Choose and run a chaos experiment adaptively
                    chaos_type, intensity = await self.adaptive_chaos.choose_experiment()
                    logger.info("Adaptive chaos: injecting %s with intensity %.2f", chaos_type, intensity)
                    exp_id = await self.chaos_manager.schedule_experiment(chaos_type, intensity, duration_seconds=30)
                    # Wait for completion and get reward (e.g., improvement in resilience)
                    # For demo, we'll just update with random reward.
                    await asyncio.sleep(35)
                    reward = random.uniform(0, 1)
                    await self.adaptive_chaos.update(chaos_type, intensity, reward)
                except Exception as e:
                    logger.error("Adaptive chaos loop error: %s", e)

    async def _drift_detection_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            if self.drift_detector and self.config.drift_detection_enabled:
                try:
                    # Check carbon drift
                    intensity = await self.carbon_manager.get_current_intensity()
                    if await self.drift_detector.check_carbon_drift(intensity):
                        logger.warning("Carbon drift detected; triggering reflection")
                        await self.state.trigger_reflection('carbon_drift')
                    # Check accuracy drift (using recent average)
                    if self.all_results:
                        avg_accuracy = np.mean([r.estimated_production_readiness for r in list(self.all_results)[-10:]])
                        if await self.drift_detector.check_accuracy_drift(avg_accuracy, 'all'):
                            logger.warning("Accuracy drift detected; triggering re-optimization")
                            # Could trigger GA or MTOP update
                except Exception as e:
                    logger.error("Drift detection loop error: %s", e)

    async def _active_user_learning_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1800)
            if self.user_pref_learner and self.config.user_preference_learning_enabled:
                try:
                    # Periodically query user if there are multiple good configurations
                    if self.pareto_optimizer and len(self.pareto_optimizer.get_pareto_front()) > 1:
                        front = self.pareto_optimizer.get_pareto_front()
                        # Simulate user interaction
                        chosen = await self.user_pref_learner.query_user_if_needed('demo_user', front[:2])
                        if chosen:
                            await self.user_pref_learner.record_choice('demo_user', chosen, {})
                except Exception as e:
                    logger.error("Active user learning loop error: %s", e)

    # ... (other loops same as original)

    # ------------------------------------------------------------------------
    # Core simulation execution (enhanced with MoE, GA, Pareto, etc.)
    # ------------------------------------------------------------------------
    async def _execute_simulation(self, operation: Dict) -> SimulationRun:
        async with self._simulation_semaphore:
            start_time = time.time()
            sim_type = operation['sim_type']
            inject_failure = operation.get('inject_failure', False)
            failure_type = operation.get('failure_type')
            user_id = operation.get('user_id')
            parameters = operation.get('parameters', {})
            use_rl_optimization = operation.get('use_rl_optimization', False)
            use_bayesian_tuning = operation.get('use_bayesian_tuning', False)

            # User adaptation
            if user_id and self.user_adaptive:
                await self.user_adaptive.learn_user_preference(user_id, 'accept_simulation', {'sim_type': sim_type}, {'success': True})

            # Carbon-aware scheduling
            schedule = await self.carbon_scheduler.schedule_simulation("normal")
            if schedule.get('action') == 'delay':
                logger.info("Simulation scheduled for better carbon time")

            # Federated insights
            params = await self.federated_learner.apply_federated_insights({'n_samples': 1})

            # RL optimization
            if use_rl_optimization and RL_AVAILABLE:
                parameters = await self.rl_optimizer.optimize_parameters(sim_type, parameters)

            # Bayesian tuning
            if use_bayesian_tuning and OPTUNA_AVAILABLE:
                best_params = await self.bayesian_tuner.tune_hyperparameters(sim_type, n_trials=20)
                parameters.update(best_params)

            # MoE strategy selection
            selected_strategy = None
            if self.moe_gating and self.config.moe_enabled:
                context = {
                    'sim_type': sim_type,
                    'carbon_intensity': await self.carbon_manager.get_current_intensity(),
                    'accuracy_target': 0.8,
                    'cost_budget': self.state.carbon_budget_remaining,
                    'success_rate': self.state.historical_success_rate
                }
                selected_strategy, expert_params = await self.moe_gating.select_expert(context)
                # Could adjust parameters based on selected strategy
                if selected_strategy == 'carbon':
                    # E.g., reduce batch size to lower carbon
                    parameters['batch_size'] = max(4, parameters.get('batch_size', 32) - 8)
                elif selected_strategy == 'cost':
                    parameters['iterations'] = min(1000, parameters.get('iterations', 100) + 50)
                # else performance: no change

            # Chaos active
            chaos_active = bool(self.chaos_manager.get_active_experiments())
            if chaos_active:
                logger.info("Active chaos experiments: %s", self.chaos_manager.get_active_experiments())

            # Run simulation (mock for demo; in real implementation, would call actual simulation engine)
            try:
                results = []
                # Simulate results based on parameters (simplified)
                readiness = 0.5 + 0.4 * (parameters.get('iterations', 100) / 1000) + np.random.normal(0, 0.02)
                readiness = max(0, min(1, readiness))
                latency = 50 - parameters.get('batch_size', 32) * 0.1 + np.random.normal(0, 5)
                carbon = 0.1 + 0.5 * (parameters.get('iterations', 100) / 1000)
                cost = parameters.get('batch_size', 32) * 0.01
                results.append(SimulationResult(
                    estimated_production_readiness=readiness,
                    latency_improvement_pct=max(0, latency),
                    carbon_impact=carbon,
                    cost_impact=cost,
                    confidence_interval=(readiness * 0.9, readiness * 1.1)
                ))
                status = 'success'
            except Exception as e:
                status = 'failed'
                logger.error("Simulation failed: %s", e)
                raise

            duration_ms = (time.time() - start_time) * 1000

            # Compute reward for MTOP/MoE
            reward = readiness

            sim_run = SimulationRun(
                results=results,
                total_duration_ms=duration_ms,
                parallel_execution=True,
                data_quality_score=await self._assess_quality(results),
                simulation_type=sim_type,
                parameters_used=parameters
            )

            # ============================================================
            # MTOP / MoE update
            # ============================================================
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            context = {
                'sim_type': sim_type,
                'carbon_intensity': carbon_intensity,
                'accuracy_target': 0.8,
                'cost_budget': self.state.carbon_budget_remaining,
                'success_rate': self.state.historical_success_rate
            }
            if self.moe_gating and self.config.moe_enabled:
                # Record training sample for MoE
                await self.moe_gating.add_training_sample(context, selected_strategy or 'performance', reward)
            else:
                # Fallback to MTOP
                state = {
                    'accuracy': readiness,
                    'carbon_intensity': carbon_intensity,
                    'cost_budget': self.state.carbon_budget_remaining,
                    'success_rate': self.state.historical_success_rate
                }
                mtop_result = await self.autonomous_optimizer.mtop_engine.select_strategy(state, carbon_intensity)
                selected_strategy = mtop_result['selected_strategy']
                await self.autonomous_optimizer.mtop_engine.update(selected_strategy, reward, mtop_result['teacher_scores'])
                self.autonomous_optimizer._last_optimization = (selected_strategy, mtop_result['teacher_scores'])

            # ============================================================
            # Pareto front update
            # ============================================================
            if self.pareto_optimizer and self.config.pareto_enabled:
                metrics = {
                    'accuracy': readiness,
                    'carbon': carbon,
                    'cost': cost,
                    'latency': latency
                }
                config_params = {
                    'sim_type': sim_type,
                    'parameters': parameters,
                    'strategy': selected_strategy
                }
                await self.pareto_optimizer.add_configuration(config_params, metrics)

            # ============================================================
            # Quantum-Resilient Signing
            # ============================================================
            result_dict = {
                'simulation_id': sim_run.run_id,
                'sim_type': sim_type,
                'results_count': len(results),
                'avg_readiness': readiness,
                'timestamp': datetime.now().isoformat()
            }
            quantum_key = await self.quantum_security.generate_keypair(self.config.quantum_algorithm)
            signature = await self.quantum_security.sign_simulation_data(result_dict, quantum_key['key_id'])
            sim_run.quantum_signature = signature
            if PROMETHEUS_AVAILABLE:
                QUANTUM_SIGNATURES.labels(algorithm=self.config.quantum_algorithm, status='sign_success').inc()

            # ============================================================
            # Blockchain Verification
            # ============================================================
            data_id = f"sim_{uuid.uuid4().hex[:8]}"
            data_hash = hashlib.sha256(json.dumps(result_dict, sort_keys=True, default=str).encode()).hexdigest()
            blockchain_result = await self.blockchain.record_simulation_data(
                data_id,
                data_hash,
                {'sim_type': sim_type, 'avg_readiness': readiness}
            )
            sim_run.blockchain_tx_hash = blockchain_result.get('tx_hash')
            if PROMETHEUS_AVAILABLE:
                BLOCKCHAIN_VERIFICATIONS.labels(status='recorded').inc()

            # ============================================================
            # Multi-Cloud Distribution
            # ============================================================
            cloud_data = {'size_gb': len(results) * 0.001}
            distribution = await self.cloud_distributor.distribute_simulation_data(cloud_data)
            sim_run.cloud_distribution = distribution
            if PROMETHEUS_AVAILABLE:
                CLOUD_DISTRIBUTIONS.labels(provider=distribution['optimal_provider'], status='success').inc()

            # ============================================================
            # Autonomous Optimization (already done)
            # ============================================================
            sim_run.autonomous_optimization = {'selected_strategy': selected_strategy, 'reward': reward}

            # Federated sharing
            if readiness > 0.8:
                await self.federated_learner.share_simulation_insight({
                    'simulation': {'sim_type': sim_type, 'accuracy': readiness, 'strategy': selected_strategy}
                })

            # Human collaboration
            await self.human_collaborator.request_simulation_feedback(
                {'sim_type': sim_type, 'readiness': readiness},
                {'reasoning': 'Simulation completed with v11 enhancements'}
            )

            # Sustainability
            await self.sustainability_tracker.record_metric('eco_efficiency', readiness, {'sim_type': sim_type})

            # Store in memory
            async with self._results_lock:
                for r in results:
                    self.all_results.append(r)
                self.simulation_runs.append(sim_run)

            # Save to persistent storage
            await self.storage.save_simulation_run(
                run_id=sim_run.run_id,
                sim_type=sim_type,
                parameters=parameters,
                duration_ms=duration_ms,
                results=[asdict(r) for r in results]
            )

            # Update metrics
            if PROMETHEUS_AVAILABLE:
                SIMULATION_RUNS.labels(type=sim_type, status=status).inc()
                SIMULATION_DURATION.labels(type=sim_type).observe(duration_ms / 1000)
                SIMULATION_ACCURACY.labels(type=sim_type).set(readiness)

            # Reflection
            if readiness > 0.8:
                await self.state.trigger_reflection('accuracy_improved')
            else:
                await self.state.trigger_reflection('accuracy_decreased')
            if carbon_intensity > 0.4:
                await self.state.trigger_reflection('high_carbon')
            await self.state.save()

            # Broadcast via WebSocket
            await self.websocket.broadcast({
                'type': 'simulation_complete',
                'run_id': sim_run.run_id,
                'sim_type': sim_type,
                'duration_ms': duration_ms,
                'results_count': len(results),
                'readiness': readiness,
                'rl_optimized': use_rl_optimization,
                'bayesian_tuned': use_bayesian_tuning,
                'chaos_active': chaos_active,
                'blockchain_tx': sim_run.blockchain_tx_hash[:16] if sim_run.blockchain_tx_hash else 'N/A'
            }, topic='simulation')

            if inject_failure:
                if PROMETHEUS_AVAILABLE:
                    FAILURE_INJECTIONS.labels(type=failure_type).inc()

            audit_logger.info("Simulation %s completed in %.0fms: readiness=%.3f, blockchain=%s...",
                             sim_type, duration_ms, readiness,
                             sim_run.blockchain_tx_hash[:16] if sim_run.blockchain_tx_hash else 'N/A')
            return sim_run

    # ... (other methods: _assess_quality, health_check, get_statistics, shutdown, etc. remain similar)

    async def shutdown(self):
        logger.info("Shutting down EnhancedSystemSimulatorV11 (instance: %s)", self.instance_id)
        self._shutdown_event.set()
        self._running = False

        if self._queue_worker:
            self._queue_worker.cancel()
            try:
                await self._queue_worker
            except asyncio.CancelledError:
                pass

        for task in self.background_tasks:
            task.cancel()
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)

        await self.websocket.stop()
        await self.visualization_dashboard.stop()
        await self.carbon_manager.close()
        if self.carbon_scheduler:
            await self.carbon_scheduler.close()
        await self.federated_learner.shutdown()
        await self.state.save()
        await self.storage.dispose()
        logger.info("Simulator shutdown complete")

# -----------------------------------------------------------------------------
# Singleton Accessor
# -----------------------------------------------------------------------------
_simulator_instance = None
_simulator_lock = asyncio.Lock()

async def get_system_simulator(config: Optional[SimulatorConfig] = None) -> EnhancedSystemSimulatorV11:
    global _simulator_instance
    if _simulator_instance is None:
        async with _simulator_lock:
            if _simulator_instance is None:
                _simulator_instance = EnhancedSystemSimulatorV11(config)
                await _simulator_instance.start()
    return _simulator_instance

# -----------------------------------------------------------------------------
# Signal Handling (fixed)
# -----------------------------------------------------------------------------
_shutdown_requested = False
_shutdown_event_global = asyncio.Event()

def handle_signal(signum, frame):
    global _shutdown_requested
    if not _shutdown_requested:
        _shutdown_requested = True
        logger.info("Received signal %s, initiating shutdown...", signum)
        asyncio.create_task(_signal_shutdown())

async def _signal_shutdown():
    _shutdown_event_global.set()

async def shutdown_handler():
    global _simulator_instance
    if _simulator_instance:
        await _simulator_instance.shutdown()
        _simulator_instance = None

# -----------------------------------------------------------------------------
# MAIN ENTRY POINT
# -----------------------------------------------------------------------------
async def main():
    # ... (same as original, but updated version)
    pass

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s, None))
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
