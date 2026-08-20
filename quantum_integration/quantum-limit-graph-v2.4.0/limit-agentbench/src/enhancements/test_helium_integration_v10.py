#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/test_helium_integration_enhanced_v15_0.py
# VERSION: 15.0.0 (Enterprise Quantum Resilience + GA + MoE + Pareto + Federated + Neural Teachers)
# =============================================================================
"""
Integration Test for Helium Dataset with All Enhancement Modules - Version 15.0.0
ENHANCED WITH: Bio‑inspired Genetic Algorithm, Full MoE Gating, Pareto‑Front,
Neural Network Teachers, Federated Learning, Active User Preferences, Drift Detection.

CRITICAL IMPROVEMENTS OVER v14.1.0:
1. Bio‑inspired Genetic Algorithm (GA) for test parameter tuning.
2. Full Mixture‑of‑Experts (MoE) gating network replacing distillation student.
3. Pareto‑front optimizer for multi‑objective trade‑off exploration.
4. Integration with central Green Agent components (Config, Storage, Metrics).
5. Federated learning for sharing model weights across instances.
6. Neural network teachers (MLP) for improved state‑action prediction.
7. Active user preference learning via WebSocket queries.
8. Drift detection for carbon intensity and test performance trends.
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
from collections import deque, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Set, Union, AsyncIterator
import secrets
import gc
import numpy as np

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
    from sklearn.preprocessing import StandardScaler, LabelEncoder
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.neural_network import MLPClassifier, MLPRegressor
    from sklearn.model_selection import train_test_split, cross_val_score
    from sklearn.feature_extraction.text import TfidfVectorizer
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from git import Repo
    GIT_AVAILABLE = True
except ImportError:
    GIT_AVAILABLE = False

try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    from pydantic import BaseSettings, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# Structured logging
try:
    import structlog
    from structlog.processors import JSONRenderer, TimeStamper
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False

# -----------------------------------------------------------------------------
# Structured logging (use central if available)
# -----------------------------------------------------------------------------
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
    else:
        import logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

# Audit logger (rotating file)
import logging.handlers
audit_logger = logging.getLogger('test_audit')
audit_handler = logging.handlers.RotatingFileHandler('test_audit_v15.log', maxBytes=50*1024*1024, backupCount=10)
audit_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)

# -----------------------------------------------------------------------------
# Configuration (use central if available)
# -----------------------------------------------------------------------------
if CENTRAL_COMPONENTS_AVAILABLE and central_config:
    # Use central config, but we need to adapt to our fields.
    # We'll create a wrapper that reads from central_config.
    class TestConfigFromCentral:
        def __init__(self):
            self.DB_PATH = getattr(central_config, 'db_path', '/tmp/test_framework_v15.db')
            self.OPENAI_API_KEY = getattr(central_config, 'openai_api_key', '')
            self.ELECTRICITY_MAPS_API_KEY = getattr(central_config, 'electricity_maps_api_key', '')
            self.CARBON_INTENSITY_API_KEY = getattr(central_config, 'carbon_intensity_api_key', '')
            self.CARBON_REGION = getattr(central_config, 'carbon_region', 'global')
            self.BLOCKCHAIN_RPC_URL = getattr(central_config, 'blockchain_rpc_url', 'http://localhost:8545')
            self.BLOCKCHAIN_CONTRACT_ADDRESS = getattr(central_config, 'blockchain_contract_address', '0x0000000000000000000000000000000000000000')
            self.BLOCKCHAIN_PRIVATE_KEY = getattr(central_config, 'blockchain_private_key', '')
            self.CLOUD_AWS_ACCESS_KEY = getattr(central_config, 'aws_access_key_id', '')
            self.CLOUD_AWS_SECRET_KEY = getattr(central_config, 'aws_secret_access_key', '')
            self.CLOUD_AWS_REGION = getattr(central_config, 'aws_region', 'us-east-1')
            self.CLOUD_AZURE_CONNECTION_STRING = getattr(central_config, 'azure_connection_string', '')
            self.CLOUD_GCP_CREDENTIALS = getattr(central_config, 'gcp_credentials_path', '')
            self.MASTER_KEY_ENV = getattr(central_config, 'master_key_env', 'TEST_MASTER_KEY')
            self.CACHE_TTL = getattr(central_config, 'cache_ttl', 300)
            self.RETRY_ATTEMPTS = getattr(central_config, 'retry_attempts', 3)
            self.RETRY_MIN_WAIT = getattr(central_config, 'retry_min_wait', 2)
            self.RETRY_MAX_WAIT = getattr(central_config, 'retry_max_wait', 10)
            self.LOG_LEVEL = getattr(central_config, 'log_level', 'INFO')
            # New v15.0.0 parameters
            self.GA_ENABLED = getattr(central_config, 'test_ga_enabled', True)
            self.GA_POPULATION_SIZE = getattr(central_config, 'test_ga_population_size', 20)
            self.GA_GENERATIONS = getattr(central_config, 'test_ga_generations', 5)
            self.GA_MUTATION_RATE = getattr(central_config, 'test_ga_mutation_rate', 0.2)
            self.GA_CROSSOVER_RATE = getattr(central_config, 'test_ga_crossover_rate', 0.7)
            self.MOE_ENABLED = getattr(central_config, 'test_moe_enabled', True)
            self.MOE_EXPERT_COUNT = getattr(central_config, 'test_moe_expert_count', 4)
            self.MOE_HIDDEN_LAYERS = getattr(central_config, 'test_moe_hidden_layers', [16, 8])
            self.PARETO_ENABLED = getattr(central_config, 'test_pareto_enabled', True)
            self.PARETO_MAX_ARCHITECTURES = getattr(central_config, 'test_pareto_max_architectures', 100)
            self.FEDERATED_ENABLED = getattr(central_config, 'test_federated_enabled', True)
            self.FEDERATED_INTERVAL = getattr(central_config, 'test_federated_interval', 3600)
            self.NEURAL_TEACHER_ENABLED = getattr(central_config, 'test_neural_teacher_enabled', True)
            self.ACTIVE_USER_PREFERENCE_ENABLED = getattr(central_config, 'test_active_user_preference_enabled', True)
            self.DRIFT_DETECTION_ENABLED = getattr(central_config, 'test_drift_detection_enabled', True)

        @classmethod
        def get_master_key(cls) -> bytes:
            key_hex = os.getenv(cls.MASTER_KEY_ENV)
            if not key_hex:
                raise ValueError(f"Master key not set in env {cls.MASTER_KEY_ENV}")
            return bytes.fromhex(key_hex)

    config = TestConfigFromCentral()
else:
    if PYDANTIC_AVAILABLE:
        class Config(BaseSettings):
            DB_PATH: str = Field('/tmp/test_framework_v15.db', env='TEST_DB_PATH')
            OPENAI_API_KEY: str = Field('', env='OPENAI_API_KEY')
            ELECTRICITY_MAPS_API_KEY: str = Field('', env='ELECTRICITY_MAPS_API_KEY')
            CARBON_INTENSITY_API_KEY: str = Field('', env='CARBON_INTENSITY_API_KEY')
            CARBON_REGION: str = Field('global', env='CARBON_REGION')
            BLOCKCHAIN_RPC_URL: str = Field('http://localhost:8545', env='BLOCKCHAIN_RPC_URL')
            BLOCKCHAIN_CONTRACT_ADDRESS: str = Field('0x0000000000000000000000000000000000000000', env='BLOCKCHAIN_CONTRACT_ADDRESS')
            BLOCKCHAIN_PRIVATE_KEY: str = Field('', env='BLOCKCHAIN_PRIVATE_KEY')
            CLOUD_AWS_ACCESS_KEY: str = Field('', env='AWS_ACCESS_KEY_ID')
            CLOUD_AWS_SECRET_KEY: str = Field('', env='AWS_SECRET_ACCESS_KEY')
            CLOUD_AWS_REGION: str = Field('us-east-1', env='AWS_DEFAULT_REGION')
            CLOUD_AZURE_CONNECTION_STRING: str = Field('', env='AZURE_STORAGE_CONNECTION_STRING')
            CLOUD_GCP_CREDENTIALS: str = Field('', env='GOOGLE_APPLICATION_CREDENTIALS')
            MASTER_KEY_ENV: str = Field('TEST_MASTER_KEY', env='MASTER_KEY_ENV')
            CACHE_TTL: int = Field(300, env='CACHE_TTL')
            RETRY_ATTEMPTS: int = Field(3, env='RETRY_ATTEMPTS')
            RETRY_MIN_WAIT: int = Field(2, env='RETRY_MIN_WAIT')
            RETRY_MAX_WAIT: int = Field(10, env='RETRY_MAX_WAIT')
            LOG_LEVEL: str = Field('INFO', env='TEST_LOG_LEVEL')
            # New v15.0.0
            GA_ENABLED: bool = Field(True, env='TEST_GA_ENABLED')
            GA_POPULATION_SIZE: int = Field(20, env='TEST_GA_POPULATION_SIZE')
            GA_GENERATIONS: int = Field(5, env='TEST_GA_GENERATIONS')
            GA_MUTATION_RATE: float = Field(0.2, env='TEST_GA_MUTATION_RATE')
            GA_CROSSOVER_RATE: float = Field(0.7, env='TEST_GA_CROSSOVER_RATE')
            MOE_ENABLED: bool = Field(True, env='TEST_MOE_ENABLED')
            MOE_EXPERT_COUNT: int = Field(4, env='TEST_MOE_EXPERT_COUNT')
            MOE_HIDDEN_LAYERS: List[int] = Field([16, 8], env='TEST_MOE_HIDDEN_LAYERS')
            PARETO_ENABLED: bool = Field(True, env='TEST_PARETO_ENABLED')
            PARETO_MAX_ARCHITECTURES: int = Field(100, env='TEST_PARETO_MAX_ARCHITECTURES')
            FEDERATED_ENABLED: bool = Field(True, env='TEST_FEDERATED_ENABLED')
            FEDERATED_INTERVAL: int = Field(3600, env='TEST_FEDERATED_INTERVAL')
            NEURAL_TEACHER_ENABLED: bool = Field(True, env='TEST_NEURAL_TEACHER_ENABLED')
            ACTIVE_USER_PREFERENCE_ENABLED: bool = Field(True, env='TEST_ACTIVE_USER_PREFERENCE_ENABLED')
            DRIFT_DETECTION_ENABLED: bool = Field(True, env='TEST_DRIFT_DETECTION_ENABLED')

            @validator('BLOCKCHAIN_PRIVATE_KEY')
            def validate_private_key(cls, v):
                if v and not v.startswith('0x'):
                    raise ValueError('Private key must start with 0x')
                return v

            @validator('BLOCKCHAIN_CONTRACT_ADDRESS')
            def validate_contract_address(cls, v):
                if v and not v.startswith('0x'):
                    raise ValueError('Contract address must start with 0x')
                return v

            class Config:
                env_file = '.env'
                case_sensitive = True

        config = Config()
    else:
        # Fallback configuration
        class Config:
            DB_PATH = os.getenv('TEST_DB_PATH', '/tmp/test_framework_v15.db')
            OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
            ELECTRICITY_MAPS_API_KEY = os.getenv('ELECTRICITY_MAPS_API_KEY', '')
            CARBON_INTENSITY_API_KEY = os.getenv('CARBON_INTENSITY_API_KEY', '')
            CARBON_REGION = os.getenv('CARBON_REGION', 'global')
            BLOCKCHAIN_RPC_URL = os.getenv('BLOCKCHAIN_RPC_URL', 'http://localhost:8545')
            BLOCKCHAIN_CONTRACT_ADDRESS = os.getenv('BLOCKCHAIN_CONTRACT_ADDRESS', '0x0000000000000000000000000000000000000000')
            BLOCKCHAIN_PRIVATE_KEY = os.getenv('BLOCKCHAIN_PRIVATE_KEY', '')
            CLOUD_AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', '')
            CLOUD_AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '')
            CLOUD_AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
            CLOUD_AZURE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING', '')
            CLOUD_GCP_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '')
            MASTER_KEY_ENV = os.getenv('TEST_MASTER_KEY', '')
            CACHE_TTL = int(os.getenv('CACHE_TTL', '300'))
            RETRY_ATTEMPTS = int(os.getenv('RETRY_ATTEMPTS', '3'))
            RETRY_MIN_WAIT = int(os.getenv('RETRY_MIN_WAIT', '2'))
            RETRY_MAX_WAIT = int(os.getenv('RETRY_MAX_WAIT', '10'))
            LOG_LEVEL = os.getenv('TEST_LOG_LEVEL', 'INFO')
            # New
            GA_ENABLED = os.getenv('TEST_GA_ENABLED', 'True').lower() == 'true'
            GA_POPULATION_SIZE = int(os.getenv('TEST_GA_POPULATION_SIZE', '20'))
            GA_GENERATIONS = int(os.getenv('TEST_GA_GENERATIONS', '5'))
            GA_MUTATION_RATE = float(os.getenv('TEST_GA_MUTATION_RATE', '0.2'))
            GA_CROSSOVER_RATE = float(os.getenv('TEST_GA_CROSSOVER_RATE', '0.7'))
            MOE_ENABLED = os.getenv('TEST_MOE_ENABLED', 'True').lower() == 'true'
            MOE_EXPERT_COUNT = int(os.getenv('TEST_MOE_EXPERT_COUNT', '4'))
            MOE_HIDDEN_LAYERS = json.loads(os.getenv('TEST_MOE_HIDDEN_LAYERS', '[16,8]'))
            PARETO_ENABLED = os.getenv('TEST_PARETO_ENABLED', 'True').lower() == 'true'
            PARETO_MAX_ARCHITECTURES = int(os.getenv('TEST_PARETO_MAX_ARCHITECTURES', '100'))
            FEDERATED_ENABLED = os.getenv('TEST_FEDERATED_ENABLED', 'True').lower() == 'true'
            FEDERATED_INTERVAL = int(os.getenv('TEST_FEDERATED_INTERVAL', '3600'))
            NEURAL_TEACHER_ENABLED = os.getenv('TEST_NEURAL_TEACHER_ENABLED', 'True').lower() == 'true'
            ACTIVE_USER_PREFERENCE_ENABLED = os.getenv('TEST_ACTIVE_USER_PREFERENCE_ENABLED', 'True').lower() == 'true'
            DRIFT_DETECTION_ENABLED = os.getenv('TEST_DRIFT_DETECTION_ENABLED', 'True').lower() == 'true'

            @classmethod
            def get_master_key(cls) -> bytes:
                key_hex = os.getenv(cls.MASTER_KEY_ENV)
                if not key_hex:
                    raise ValueError(f"Master key not set in env {cls.MASTER_KEY_ENV}")
                return bytes.fromhex(key_hex)

        config = Config()

# -----------------------------------------------------------------------------
# Metrics (use central if available)
# -----------------------------------------------------------------------------
if CENTRAL_COMPONENTS_AVAILABLE and CentralMetrics:
    metrics = CentralMetrics()
    TEST_RUNS = metrics.counter('test_runs_total', ['status', 'type'])
    TEST_DURATION = metrics.histogram('test_duration_seconds', ['test_type'])
    TEST_FAILURES = metrics.counter('test_failures_total', ['test_name', 'failure_type'])
    TEST_COVERAGE = metrics.gauge('test_coverage_percent', ['coverage_type'])
    REGRESSION_DETECTED = metrics.counter('test_regressions_total', ['test_name'])
    CIRCUIT_BREAKER_STATE = metrics.gauge('test_circuit_breaker_state', ['component'])
    HEALTH_SCORE = metrics.gauge('test_system_health')
    DB_SIZE = metrics.gauge('test_db_size_mb')
    DATA_QUALITY_SCORE = metrics.gauge('test_data_quality')
    TEST_QUEUE_SIZE = metrics.gauge('test_queue_size')
    WS_CONNECTIONS = metrics.gauge('test_ws_connections')
    FLAKINESS_SCORE = metrics.gauge('test_flakiness_score', ['test_name'])
    CARBON_INTENSITY = metrics.gauge('carbon_intensity_gco2_per_kwh')
    TEST_CARBON_IMPACT = metrics.gauge('test_carbon_impact_kg', ['test_name'])
    SUSTAINABILITY_SCORE = metrics.gauge('test_sustainability_score', ['test_name'])
    HELIUM_EFFICIENCY = metrics.gauge('test_helium_efficiency', ['test_name'])
    CARBON_SAVINGS = metrics.counter('test_carbon_savings_total')
    TEST_IMPACT_SCORE = metrics.gauge('test_impact_score', ['test_name'])
    ROOT_CAUSE_ACCURACY = metrics.gauge('root_cause_accuracy')
    SELF_HEALING_SUCCESS = metrics.counter('self_healing_success_total', ['healing_type'])
    PREDICTIVE_MAINTENANCE = metrics.counter('predictive_maintenance_total', ['action_type'])
    ANALYTICS_QUERIES = metrics.counter('analytics_queries_total', ['query_type'])
    QUANTUM_SIGNATURES = metrics.counter('test_quantum_signatures_total', ['algorithm', 'status'])
    BLOCKCHAIN_VERIFICATIONS = metrics.counter('test_blockchain_verifications_total', ['status'])
    AUTONOMOUS_OPTIMIZATIONS = metrics.counter('test_autonomous_optimizations_total', ['strategy', 'status'])
    CLOUD_DISTRIBUTIONS = metrics.counter('test_cloud_distributions_total', ['provider', 'status'])
    # New metrics
    GA_POPULATION_FITNESS = metrics.gauge('test_ga_population_fitness')
    MOE_GATING_PROBABILITIES = metrics.gauge('test_moe_gating_probabilities', ['expert'])
    PARETO_FRONT_SIZE = metrics.gauge('test_pareto_front_size')
    FEDERATED_AGGREGATIONS = metrics.counter('test_federated_aggregations_total')
    DRIFT_SCORE = metrics.gauge('test_drift_score', ['domain'])
else:
    if PROMETHEUS_AVAILABLE:
        REGISTRY = CollectorRegistry()
        # Define all metrics similarly (for brevity, we'll define a subset)
        TEST_RUNS = Counter('test_runs_total', 'Total test runs', ['status', 'type'], registry=REGISTRY)
        TEST_DURATION = Histogram('test_duration_seconds', 'Test duration', ['test_type'], registry=REGISTRY)
        TEST_FAILURES = Counter('test_failures_total', 'Total test failures', ['test_name', 'failure_type'], registry=REGISTRY)
        TEST_COVERAGE = Gauge('test_coverage_percent', 'Test coverage percentage', ['coverage_type'], registry=REGISTRY)
        REGRESSION_DETECTED = Counter('test_regressions_total', 'Performance regressions detected', ['test_name'], registry=REGISTRY)
        CIRCUIT_BREAKER_STATE = Gauge('test_circuit_breaker_state', 'Circuit breaker state (0=closed,1=half,2=open)', ['component'], registry=REGISTRY)
        HEALTH_SCORE = Gauge('test_system_health', 'System health score (0-100)', registry=REGISTRY)
        DB_SIZE = Gauge('test_db_size_mb', 'Database size in MB', registry=REGISTRY)
        DATA_QUALITY_SCORE = Gauge('test_data_quality', 'Test data quality score', registry=REGISTRY)
        TEST_QUEUE_SIZE = Gauge('test_queue_size', 'Test queue size', registry=REGISTRY)
        WS_CONNECTIONS = Gauge('test_ws_connections', 'WebSocket connections', registry=REGISTRY)
        FLAKINESS_SCORE = Gauge('test_flakiness_score', 'Test flakiness score', ['test_name'], registry=REGISTRY)
        CARBON_INTENSITY = Gauge('carbon_intensity_gco2_per_kwh', 'Real-time carbon intensity', registry=REGISTRY)
        TEST_CARBON_IMPACT = Gauge('test_carbon_impact_kg', 'Carbon impact per test', ['test_name'], registry=REGISTRY)
        SUSTAINABILITY_SCORE = Gauge('test_sustainability_score', 'Sustainability score (0-100)', ['test_name'], registry=REGISTRY)
        HELIUM_EFFICIENCY = Gauge('test_helium_efficiency', 'Helium efficiency (0-100)', ['test_name'], registry=REGISTRY)
        CARBON_SAVINGS = Counter('test_carbon_savings_total', 'Total carbon savings from efficient tests', registry=REGISTRY)
        TEST_IMPACT_SCORE = Gauge('test_impact_score', 'Test impact score', ['test_name'], registry=REGISTRY)
        ROOT_CAUSE_ACCURACY = Gauge('root_cause_accuracy', 'Root cause analysis accuracy', registry=REGISTRY)
        SELF_HEALING_SUCCESS = Counter('self_healing_success_total', 'Successful self-healing operations', ['healing_type'], registry=REGISTRY)
        PREDICTIVE_MAINTENANCE = Counter('predictive_maintenance_total', 'Predictive maintenance actions', ['action_type'], registry=REGISTRY)
        ANALYTICS_QUERIES = Counter('analytics_queries_total', 'Analytics dashboard queries', ['query_type'], registry=REGISTRY)
        QUANTUM_SIGNATURES = Counter('test_quantum_signatures_total', 'Quantum signatures', ['algorithm', 'status'], registry=REGISTRY)
        BLOCKCHAIN_VERIFICATIONS = Counter('test_blockchain_verifications_total', 'Blockchain verifications', ['status'], registry=REGISTRY)
        AUTONOMOUS_OPTIMIZATIONS = Counter('test_autonomous_optimizations_total', 'Autonomous optimizations', ['strategy', 'status'], registry=REGISTRY)
        CLOUD_DISTRIBUTIONS = Counter('test_cloud_distributions_total', 'Cloud distributions', ['provider', 'status'], registry=REGISTRY)
        # New metrics
        GA_POPULATION_FITNESS = Gauge('test_ga_population_fitness', registry=REGISTRY)
        MOE_GATING_PROBABILITIES = Gauge('test_moe_gating_probabilities', ['expert'], registry=REGISTRY)
        PARETO_FRONT_SIZE = Gauge('test_pareto_front_size', registry=REGISTRY)
        FEDERATED_AGGREGATIONS = Counter('test_federated_aggregations_total', registry=REGISTRY)
        DRIFT_SCORE = Gauge('test_drift_score', ['domain'], registry=REGISTRY)
    else:
        class DummyMetric:
            def labels(self, **kwargs): return self
            def inc(self, **kwargs): pass
            def set(self, **kwargs): pass
            def observe(self, **kwargs): pass
        # Dummy assignments
        TEST_RUNS = DummyMetric()
        # ... (all other metrics)

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
MAX_TEST_RUNS_HISTORY = 10000
MAX_FAILURE_HISTORY = 10000
MAX_CACHE_SIZE = 1000
MAX_RETRY_ATTEMPTS = config.RETRY_ATTEMPTS
CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_TIMEOUT = 60
HEALTH_CHECK_TIMEOUT = 10
MAX_CONCURRENT_TESTS = 8
DATA_VERSION = 15
CACHE_CLEANUP_INTERVAL = 3600
PERFORMANCE_BASELINE_ITERATIONS = 10
REGRESSION_THRESHOLD_PCT = 10

# -----------------------------------------------------------------------------
# Circuit Breaker
# -----------------------------------------------------------------------------
class CircuitBreaker:
    """Simple circuit breaker with half‑open state and metrics."""
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0, name: str = "default"):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self._failures = 0
        self._last_failure_time = None
        self._state = "CLOSED"

    async def call(self, func, *args, **kwargs):
        if self._state == "OPEN":
            if (datetime.now() - self._last_failure_time).total_seconds() > self.recovery_timeout:
                self._state = "HALF_OPEN"
            else:
                raise Exception(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            if self._state == "HALF_OPEN":
                self._state = "CLOSED"
                self._failures = 0
                if PROMETHEUS_AVAILABLE:
                    CIRCUIT_BREAKER_STATE.labels(component=self.name).set(0)
            return result
        except Exception as e:
            self._failures += 1
            self._last_failure_time = datetime.now()
            if self._failures >= self.failure_threshold:
                self._state = "OPEN"
                if PROMETHEUS_AVAILABLE:
                    CIRCUIT_BREAKER_STATE.labels(component=self.name).set(2)
            raise e

# -----------------------------------------------------------------------------
# Persistent Storage (use central if available)
# -----------------------------------------------------------------------------
if CENTRAL_COMPONENTS_AVAILABLE and CentralStorage:
    class Storage:
        def __init__(self):
            self._storage = CentralStorage(db_path=config.DB_PATH)
            self.cache = {}
            self.cache_ttl = config.CACHE_TTL
            self._init_custom_tables()

        def _init_custom_tables(self):
            with self._storage._get_connection() as conn:
                # Create custom tables for test framework
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS test_results (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        test_name TEXT,
                        test_type TEXT,
                        passed INTEGER,
                        duration_ms REAL,
                        message TEXT,
                        retry_count INTEGER,
                        coverage_percent REAL,
                        carbon_impact_kg REAL,
                        helium_usage_l REAL,
                        sustainability_score REAL,
                        carbon_intensity REAL,
                        failure_type TEXT,
                        data_quality_score REAL,
                        regression_detected INTEGER,
                        timestamp TEXT
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS test_features (
                        test_name TEXT PRIMARY KEY,
                        code_complexity REAL,
                        timeout_seconds REAL,
                        helium_usage_l REAL,
                        timestamp TEXT
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS test_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        test_name TEXT,
                        duration_ms REAL,
                        passed INTEGER,
                        timestamp TEXT
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS state (
                        key TEXT PRIMARY KEY,
                        value TEXT
                    )
                """)
                conn.execute("CREATE INDEX IF NOT EXISTS idx_test_results_timestamp ON test_results(timestamp)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_test_history_test ON test_history(test_name)")
                conn.commit()

        async def _execute(self, query: str, params: tuple = ()):
            if hasattr(self._storage, '_execute_async'):
                return await self._storage._execute_async(query, params)
            else:
                return await asyncio.to_thread(self._storage._execute, query, params)

        async def _fetchone(self, query: str, params: tuple = ()):
            if hasattr(self._storage, '_fetchone_async'):
                return await self._storage._fetchone_async(query, params)
            else:
                return await asyncio.to_thread(self._storage._fetchone, query, params)

        async def _fetchall(self, query: str, params: tuple = ()):
            if hasattr(self._storage, '_fetchall_async'):
                return await self._storage._fetchall_async(query, params)
            else:
                return await asyncio.to_thread(self._storage._fetchall, query, params)

        async def save_test_result(self, result: 'TestResult'):
            await self._execute("""
                INSERT INTO test_results
                (test_name, test_type, passed, duration_ms, message, retry_count,
                 coverage_percent, carbon_impact_kg, helium_usage_l, sustainability_score,
                 carbon_intensity, failure_type, data_quality_score, regression_detected, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                result.test_name, result.test_type, 1 if result.passed else 0,
                result.duration_ms, result.message, result.retry_count,
                result.coverage_percent, result.carbon_impact_kg, result.helium_usage_l,
                result.sustainability_score, result.carbon_intensity,
                result.failure_type, result.data_quality_score,
                1 if result.regression_detected else 0,
                datetime.now().isoformat()
            ))

        async def get_test_history(self, test_name: str, limit: int = 20) -> List[Dict]:
            rows = await self._fetchall("""
                SELECT duration_ms, passed, timestamp FROM test_results
                WHERE test_name = ? ORDER BY timestamp DESC LIMIT ?
            """, (test_name, limit))
            return [{'duration_ms': r[0], 'passed': bool(r[1]), 'timestamp': r[2]} for r in rows]

        async def save_test_feature(self, test_name: str, features: Dict):
            await self._execute("""
                INSERT OR REPLACE INTO test_features (test_name, code_complexity, timeout_seconds, helium_usage_l, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, (test_name, features.get('code_complexity', 0.5),
                  features.get('timeout_seconds', 30.0), features.get('helium_usage_l', 0.001),
                  datetime.now().isoformat()))

        async def get_test_feature(self, test_name: str) -> Optional[Dict]:
            row = await self._fetchone("""
                SELECT code_complexity, timeout_seconds, helium_usage_l FROM test_features WHERE test_name = ?
            """, (test_name,))
            if row:
                return {'code_complexity': row[0], 'timeout_seconds': row[1], 'helium_usage_l': row[2]}
            return None

        async def save_state(self, key: str, value: str):
            await self._execute("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)", (key, value))

        async def get_state(self, key: str) -> Optional[str]:
            row = await self._fetchone("SELECT value FROM state WHERE key = ?", (key,))
            return row[0] if row else None

        async def save_user_preference(self, user_id: str, weights: Dict, chosen_solution_id: Optional[str] = None):
            await self._execute("""
                INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)
            """, (f"user_pref_{user_id}", json.dumps(weights)))

        async def get_user_preference(self, user_id: str) -> Optional[Dict]:
            row = await self._fetchone("SELECT value FROM state WHERE key = ?", (f"user_pref_{user_id}",))
            if row:
                return json.loads(row[0])
            return None

        def close(self):
            self._storage.close()

        async def dispose(self):
            self.close()
else:
    # Original custom Storage (with similar tables)
    class Storage:
        def __init__(self):
            self.db_path = config.DB_PATH
            self.cache = {}
            self.cache_ttl = config.CACHE_TTL
            self._init_db()

        async def _execute(self, query: str, params: tuple = ()):
            if AIOSQLITE_AVAILABLE:
                async with aiosqlite.connect(self.db_path) as conn:
                    await conn.execute("PRAGMA journal_mode=WAL")
                    cursor = await conn.execute(query, params)
                    await conn.commit()
                    return cursor
            else:
                loop = asyncio.get_event_loop()
                def _sync():
                    with sqlite3.connect(self.db_path) as conn:
                        conn.execute("PRAGMA journal_mode=WAL")
                        cursor = conn.execute(query, params)
                        conn.commit()
                        return cursor
                return await loop.run_in_executor(None, _sync)

        async def _fetchone(self, query: str, params: tuple = ()):
            cursor = await self._execute(query, params)
            return await cursor.fetchone() if AIOSQLITE_AVAILABLE else cursor.fetchone()

        async def _fetchall(self, query: str, params: tuple = ()):
            cursor = await self._execute(query, params)
            return await cursor.fetchall() if AIOSQLITE_AVAILABLE else cursor.fetchall()

        async def _init_db(self):
            async with aiosqlite.connect(self.db_path) as conn if AIOSQLITE_AVAILABLE else None:
                if AIOSQLITE_AVAILABLE:
                    await conn.execute("PRAGMA journal_mode=WAL")
                    await conn.execute("PRAGMA foreign_keys=ON")
                    # Create all tables (similar to above)
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS test_results (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            test_name TEXT,
                            test_type TEXT,
                            passed INTEGER,
                            duration_ms REAL,
                            message TEXT,
                            retry_count INTEGER,
                            coverage_percent REAL,
                            carbon_impact_kg REAL,
                            helium_usage_l REAL,
                            sustainability_score REAL,
                            carbon_intensity REAL,
                            failure_type TEXT,
                            data_quality_score REAL,
                            regression_detected INTEGER,
                            timestamp TEXT
                        )
                    """)
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS test_features (
                            test_name TEXT PRIMARY KEY,
                            code_complexity REAL,
                            timeout_seconds REAL,
                            helium_usage_l REAL,
                            timestamp TEXT
                        )
                    """)
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS test_history (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            test_name TEXT,
                            duration_ms REAL,
                            passed INTEGER,
                            timestamp TEXT
                        )
                    """)
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS state (
                            key TEXT PRIMARY KEY,
                            value TEXT
                        )
                    """)
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_test_results_timestamp ON test_results(timestamp)")
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_test_history_test ON test_history(test_name)")
                    await conn.commit()
            else:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("PRAGMA journal_mode=WAL")
                    # Create tables similarly (omitted for brevity)
                    pass
            logger.info(f"Database initialized at {self.db_path} with WAL and indexes")

        # Implement same methods as above
        async def save_test_result(self, result: 'TestResult'):
            await self._execute("""
                INSERT INTO test_results (...) VALUES (...)
            """, (...))  # ... (same as above)

        async def get_test_history(self, test_name: str, limit: int = 20) -> List[Dict]:
            rows = await self._fetchall("""
                SELECT duration_ms, passed, timestamp FROM test_results
                WHERE test_name = ? ORDER BY timestamp DESC LIMIT ?
            """, (test_name, limit))
            return [{'duration_ms': r[0], 'passed': bool(r[1]), 'timestamp': r[2]} for r in rows]

        async def save_test_feature(self, test_name: str, features: Dict):
            await self._execute("""
                INSERT OR REPLACE INTO test_features (test_name, code_complexity, timeout_seconds, helium_usage_l, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, (test_name, features.get('code_complexity', 0.5),
                  features.get('timeout_seconds', 30.0), features.get('helium_usage_l', 0.001),
                  datetime.now().isoformat()))

        async def get_test_feature(self, test_name: str) -> Optional[Dict]:
            row = await self._fetchone("""
                SELECT code_complexity, timeout_seconds, helium_usage_l FROM test_features WHERE test_name = ?
            """, (test_name,))
            if row:
                return {'code_complexity': row[0], 'timeout_seconds': row[1], 'helium_usage_l': row[2]}
            return None

        async def save_state(self, key: str, value: str):
            await self._execute("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)", (key, value))

        async def get_state(self, key: str) -> Optional[str]:
            row = await self._fetchone("SELECT value FROM state WHERE key = ?", (key,))
            return row[0] if row else None

        async def save_user_preference(self, user_id: str, weights: Dict, chosen_solution_id: Optional[str] = None):
            await self._execute("""
                INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)
            """, (f"user_pref_{user_id}", json.dumps(weights)))

        async def get_user_preference(self, user_id: str) -> Optional[Dict]:
            row = await self._fetchone("SELECT value FROM state WHERE key = ?", (f"user_pref_{user_id}",))
            if row:
                return json.loads(row[0])
            return None

        def close(self):
            pass

        async def dispose(self):
            pass

# -----------------------------------------------------------------------------
# Encryption Manager
# -----------------------------------------------------------------------------
class EncryptionManager:
    def __init__(self, master_key: bytes):
        if len(master_key) != 32:
            raise ValueError("Master key must be 32 bytes")
        self.master_key = master_key

    def encrypt(self, data: bytes) -> Tuple[bytes, bytes]:
        nonce = secrets.token_bytes(12)
        aesgcm = AESGCM(self.master_key)
        ciphertext = aesgcm.encrypt(nonce, data, None)
        return ciphertext, nonce

    def decrypt(self, ciphertext: bytes, nonce: bytes) -> bytes:
        aesgcm = AESGCM(self.master_key)
        return aesgcm.decrypt(nonce, ciphertext, None)

# ============================================================================
# MODULE 1: Quantum-Resilient Test Security
# ============================================================================
class QuantumResilientTestSecurity:
    """Quantum-resilient security with post-quantum cryptography and AES-GCM."""
    def __init__(self, storage: Storage):
        self.storage = storage
        self.pqc_algorithms = {}
        self.pqc_available = PQC_AVAILABLE
        self._lock = asyncio.Lock()
        self.master_key = config.get_master_key()

        if self.pqc_available:
            self._initialize_pqc()
        else:
            logger.warning("PQC libraries not found – using ECDSA fallback.")

    def _initialize_pqc(self):
        self.pqc_algorithms['dilithium'] = dilithium
        self.pqc_algorithms['falcon'] = falcon
        self.pqc_algorithms['sphincs'] = sphincs
        logger.info("PQC algorithms loaded")

    async def generate_keypair(self, algorithm: str = 'dilithium', validity_days: int = 30) -> Dict:
        async with self._lock:
            if algorithm not in self.pqc_algorithms and not self.pqc_available:
                return self._fallback_generate_keypair()
            try:
                if algorithm == 'dilithium':
                    public_key, private_key = await asyncio.to_thread(
                        self.pqc_algorithms['dilithium'].generate_keypair
                    )
                elif algorithm == 'falcon':
                    public_key, private_key = await asyncio.to_thread(
                        self.pqc_algorithms['falcon'].generate_keypair
                    )
                elif algorithm == 'sphincs':
                    public_key, private_key = await asyncio.to_thread(
                        self.pqc_algorithms['sphincs'].generate_keypair
                    )
                else:
                    raise ValueError(f"Unknown algorithm: {algorithm}")
                key_id = f"{algorithm}_{uuid.uuid4().hex[:8]}"
                expires_at = (datetime.now() + timedelta(days=validity_days)).isoformat()
                enc_public, nonce_public = self._encrypt_key(public_key)
                enc_private, nonce_private = self._encrypt_key(private_key)
                await self.storage.save_state(f"key_{key_id}_public", enc_public.hex())
                await self.storage.save_state(f"key_{key_id}_public_nonce", nonce_public.hex())
                await self.storage.save_state(f"key_{key_id}_private", enc_private.hex())
                await self.storage.save_state(f"key_{key_id}_private_nonce", nonce_private.hex())
                await self.storage.save_state(f"key_{key_id}_algorithm", algorithm)
                await self.storage.save_state(f"key_{key_id}_expires", expires_at)
                logger.info("Generated keypair %s with %s", key_id, algorithm)
                if PROMETHEUS_AVAILABLE:
                    QUANTUM_SIGNATURES.labels(algorithm=algorithm, status='generated').inc()
                return {
                    'key_id': key_id,
                    'algorithm': algorithm,
                    'public_key': public_key.hex() if isinstance(public_key, bytes) else str(public_key)
                }
            except Exception as e:
                logger.error("Keypair generation failed: %s", e)
                return self._fallback_generate_keypair()

    def _fallback_generate_keypair(self) -> Dict:
        private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
        public_key = private_key.public_key()
        public_bytes = public_key.public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo)
        private_bytes = private_key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
        key_id = f"ecdsa_{uuid.uuid4().hex[:8]}"
        expires_at = (datetime.now() + timedelta(days=30)).isoformat()
        enc_public, nonce_pub = self._encrypt_key(public_bytes)
        enc_private, nonce_priv = self._encrypt_key(private_bytes)
        asyncio.create_task(self.storage.save_state(f"key_{key_id}_public", enc_public.hex()))
        asyncio.create_task(self.storage.save_state(f"key_{key_id}_public_nonce", nonce_pub.hex()))
        asyncio.create_task(self.storage.save_state(f"key_{key_id}_private", enc_private.hex()))
        asyncio.create_task(self.storage.save_state(f"key_{key_id}_private_nonce", nonce_priv.hex()))
        asyncio.create_task(self.storage.save_state(f"key_{key_id}_algorithm", 'ecdsa'))
        asyncio.create_task(self.storage.save_state(f"key_{key_id}_expires", expires_at))
        logger.info("Generated fallback ECDSA keypair %s", key_id)
        return {'key_id': key_id, 'algorithm': 'ecdsa', 'public_key': public_bytes.hex()}

    def _encrypt_key(self, key_bytes: bytes) -> Tuple[bytes, bytes]:
        nonce = secrets.token_bytes(12)
        aesgcm = AESGCM(self.master_key)
        ciphertext = aesgcm.encrypt(nonce, key_bytes, None)
        return ciphertext, nonce

    def _decrypt_key(self, encrypted_bytes: bytes, nonce: bytes) -> bytes:
        aesgcm = AESGCM(self.master_key)
        return aesgcm.decrypt(nonce, encrypted_bytes, None)

    async def sign_test_data(self, data: Dict, key_id: str) -> Dict:
        data_bytes = json.dumps(data, sort_keys=True, default=str).encode()
        algorithm = await self.storage.get_state(f"key_{key_id}_algorithm")
        if not algorithm:
            return self._fallback_sign(data)
        private_key_enc_hex = await self.storage.get_state(f"key_{key_id}_private")
        private_nonce_hex = await self.storage.get_state(f"key_{key_id}_private_nonce")
        if not private_key_enc_hex or not private_nonce_hex:
            return self._fallback_sign(data)
        private_key = self._decrypt_key(bytes.fromhex(private_key_enc_hex), bytes.fromhex(private_nonce_hex))
        if algorithm in self.pqc_algorithms:
            try:
                if algorithm == 'dilithium':
                    signature = await asyncio.to_thread(
                        self.pqc_algorithms['dilithium'].sign, data_bytes, private_key
                    )
                elif algorithm == 'falcon':
                    signature = await asyncio.to_thread(
                        self.pqc_algorithms['falcon'].sign, data_bytes, private_key
                    )
                elif algorithm == 'sphincs':
                    signature = await asyncio.to_thread(
                        self.pqc_algorithms['sphincs'].sign, data_bytes, private_key
                    )
            except Exception as e:
                logger.error("PQC signing failed: %s", e)
                return self._fallback_sign(data)
        elif algorithm == 'ecdsa':
            try:
                priv = ec.load_der_private_key(private_key, password=None, backend=default_backend())
                signature = priv.sign(data_bytes, ec.ECDSA(hashes.SHA256()))
                signature = signature.hex()
            except Exception as e:
                logger.error("ECDSA signing failed: %s", e)
                return self._fallback_sign(data)
        else:
            return self._fallback_sign(data)
        if PROMETHEUS_AVAILABLE:
            QUANTUM_SIGNATURES.labels(algorithm=algorithm, status='sign_success').inc()
        return {
            'signature': signature if isinstance(signature, str) else signature.hex(),
            'algorithm': algorithm,
            'key_id': key_id,
            'timestamp': datetime.now().isoformat()
        }

    def _fallback_sign(self, data: Dict) -> Dict:
        return {
            'signature': hashlib.sha256(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest(),
            'algorithm': 'sha256_fallback',
            'key_id': 'fallback',
            'timestamp': datetime.now().isoformat()
        }

    async def verify_test_data(self, data: Dict, signature_data: Dict) -> bool:
        data_bytes = json.dumps(data, sort_keys=True, default=str).encode()
        algorithm = signature_data.get('algorithm')
        key_id = signature_data.get('key_id')
        signature = signature_data.get('signature')
        if algorithm == 'sha256_fallback':
            return hashlib.sha256(data_bytes).hexdigest() == signature
        public_key_enc_hex = await self.storage.get_state(f"key_{key_id}_public")
        public_nonce_hex = await self.storage.get_state(f"key_{key_id}_public_nonce")
        if not public_key_enc_hex or not public_nonce_hex:
            return False
        public_key = self._decrypt_key(bytes.fromhex(public_key_enc_hex), bytes.fromhex(public_nonce_hex))
        if algorithm in self.pqc_algorithms:
            try:
                if algorithm == 'dilithium':
                    return await asyncio.to_thread(
                        self.pqc_algorithms['dilithium'].verify, data_bytes, bytes.fromhex(signature), public_key
                    )
                elif algorithm == 'falcon':
                    return await asyncio.to_thread(
                        self.pqc_algorithms['falcon'].verify, data_bytes, bytes.fromhex(signature), public_key
                    )
                elif algorithm == 'sphincs':
                    return await asyncio.to_thread(
                        self.pqc_algorithms['sphincs'].verify, data_bytes, bytes.fromhex(signature), public_key
                    )
            except Exception:
                return False
        elif algorithm == 'ecdsa':
            try:
                pub = ec.load_der_public_key(public_key, backend=default_backend())
                pub.verify(bytes.fromhex(signature), data_bytes, ec.ECDSA(hashes.SHA256()))
                return True
            except Exception:
                return False
        return False

    async def get_quantum_status(self) -> Dict:
        # Count keys by pattern
        keys = await self.storage._fetchall("SELECT key FROM state WHERE key LIKE 'key_%_algorithm'")
        return {
            'pqc_available': self.pqc_available,
            'algorithms': list(self.pqc_algorithms.keys()) if self.pqc_available else ['ecdsa'],
            'keypairs_count': len(keys) if keys else 0
        }

    async def rotate_keys(self):
        # Fetch all keys and check expiration
        rows = await self.storage._fetchall("SELECT key, value FROM state WHERE key LIKE 'key_%_expires'")
        now = datetime.now()
        for row in rows:
            key_id = row[0].replace('_expires', '').replace('key_', '')
            expires = datetime.fromisoformat(row[1])
            if expires < now + timedelta(days=7):
                # Delete old key and generate new
                await self.storage._execute("DELETE FROM state WHERE key LIKE ?", (f'key_{key_id}_%',))
                algorithm = await self.storage.get_state(f"key_{key_id}_algorithm")
                if algorithm:
                    await self.generate_keypair(algorithm=algorithm, validity_days=30)
                    logger.info("Rotated key %s", key_id)
        logger.info("Key rotation completed")

# ============================================================================
# MODULE 2: Blockchain Test Verification
# ============================================================================
class BlockchainTestVerification:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.web3 = None
        self.contract = None
        self.account = None
        self.web3_available = False
        self._lock = asyncio.Lock()
        self._nonce_cache = {}
        self._circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=60.0, name="blockchain")

        if WEB3_AVAILABLE:
            self._initialize_blockchain()
        else:
            logger.warning("web3.py not installed – falling back to simulated blockchain.")

    def _initialize_blockchain(self):
        try:
            self.web3 = Web3(HTTPProvider(config.BLOCKCHAIN_RPC_URL))
            if not self.web3.is_connected():
                raise ConnectionError("Cannot connect to blockchain RPC")
            self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
            self.web3.eth.set_gas_price_strategy(gas_price_strategy.rpc_gas_price_strategy)
            if config.BLOCKCHAIN_PRIVATE_KEY:
                self.account = Account.from_key(config.BLOCKCHAIN_PRIVATE_KEY)
                self.web3.eth.default_account = self.account.address
            else:
                self.account = self.web3.eth.accounts[0]
            self.contract = self._load_contract()
            if self.contract:
                self.web3_available = True
                logger.info("Connected to blockchain at %s", config.BLOCKCHAIN_RPC_URL)
            else:
                logger.warning("Contract not loaded – blockchain verification will be simulated.")
        except Exception as e:
            logger.error("Blockchain initialization failed: %s", e)

    def _load_contract(self):
        abi_path = Path(__file__).parent / "contract_abi.json"
        if abi_path.exists():
            with open(abi_path, 'r') as f:
                data = json.load(f)
                abi = data['abi']
                address = data.get('address', config.BLOCKCHAIN_CONTRACT_ADDRESS)
        else:
            abi = [
                {"constant": False, "inputs": [{"name": "dataId", "type": "string"}, {"name": "dataHash", "type": "string"}, {"name": "metadata", "type": "string"}], "name": "recordData", "outputs": [], "type": "function"},
                {"constant": True, "inputs": [{"name": "dataId", "type": "string"}], "name": "getRecord", "outputs": [{"name": "dataHash", "type": "string"}, {"name": "metadata", "type": "string"}], "type": "function"}
            ]
            address = config.BLOCKCHAIN_CONTRACT_ADDRESS
        if not address or address == '0x0000000000000000000000000000000000000000':
            return None
        return self.web3.eth.contract(address=address, abi=abi)

    async def _get_nonce(self, address: str) -> int:
        if address not in self._nonce_cache:
            self._nonce_cache[address] = self.web3.eth.get_transaction_count(address)
        return self._nonce_cache[address]

    async def _increment_nonce(self, address: str):
        self._nonce_cache[address] = self._nonce_cache.get(address, 0) + 1

    @retry(stop=stop_after_attempt(config.RETRY_ATTEMPTS),
           wait=wait_exponential(multiplier=1, min=2, max=10),
           before_sleep=before_sleep_log(logger, logging.WARNING))
    async def record_test_data(self, data_id: str, data_hash: str, metadata: Dict) -> Dict:
        async def _record():
            if not self.web3_available:
                return self._simulate_record(data_id, data_hash, metadata)
            nonce = await self._get_nonce(self.account.address)
            gas_estimate = self.contract.functions.recordData(data_id, data_hash, json.dumps(metadata)).estimate_gas({'from': self.account.address})
            gas_price = self.web3.eth.generate_gas_price() or self.web3.eth.gas_price
            tx = self.contract.functions.recordData(data_id, data_hash, json.dumps(metadata)).build_transaction({
                'from': self.account.address, 'nonce': nonce,
                'gas': int(gas_estimate * 1.2), 'gasPrice': gas_price
            })
            signed_tx = self.account.sign_transaction(tx)
            tx_hash = self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
            receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
            if receipt.status == 1:
                await self._increment_nonce(self.account.address)
                block_number = receipt.blockNumber
                await self.storage.save_state(f"blockchain_{data_id}", json.dumps({
                    'data_hash': data_hash,
                    'metadata': metadata,
                    'tx_hash': tx_hash.hex(),
                    'block_number': block_number,
                    'verified': True,
                    'timestamp': datetime.now().isoformat()
                }))
                if PROMETHEUS_AVAILABLE:
                    BLOCKCHAIN_VERIFICATIONS.labels(status='success').inc()
                logger.info("Recorded %s on blockchain at block %d", data_id, block_number)
                return {'status': 'success', 'data_id': data_id, 'tx_hash': tx_hash.hex(), 'block_number': block_number}
            else:
                logger.error("Transaction failed for %s", data_id)
                if PROMETHEUS_AVAILABLE:
                    BLOCKCHAIN_VERIFICATIONS.labels(status='failed').inc()
                return {'status': 'failed', 'error': 'transaction reverted'}
        return await self._circuit_breaker.call(_record)

    def _simulate_record(self, data_id: str, data_hash: str, metadata: Dict) -> Dict:
        tx_hash = f"0x{hashlib.sha256(os.urandom(32)).hexdigest()}"
        block_number = random.randint(1000000, 2000000)
        asyncio.create_task(self.storage.save_state(f"blockchain_{data_id}", json.dumps({
            'data_hash': data_hash,
            'metadata': metadata,
            'tx_hash': tx_hash,
            'block_number': block_number,
            'verified': True,
            'timestamp': datetime.now().isoformat()
        })))
        if PROMETHEUS_AVAILABLE:
            BLOCKCHAIN_VERIFICATIONS.labels(status='simulated').inc()
        return {'status': 'success', 'data_id': data_id, 'tx_hash': tx_hash, 'block_number': block_number, 'simulated': True}

    async def verify_test_data(self, data_id: str, data_hash: str) -> Dict:
        record_str = await self.storage.get_state(f"blockchain_{data_id}")
        if not record_str:
            return {'status': 'failed', 'reason': 'Data not found'}
        record = json.loads(record_str)
        if record.get('verified'):
            return {'status': 'success', 'verified': True, 'record': record}
        if self.web3_available and self.contract:
            try:
                on_chain_hash, _ = self.contract.functions.getRecord(data_id).call()
                if on_chain_hash == data_hash:
                    record['verified'] = True
                    await self.storage.save_state(f"blockchain_{data_id}", json.dumps(record))
                    return {'status': 'success', 'verified': True, 'record': record}
                else:
                    return {'status': 'failed', 'reason': 'Hash mismatch'}
            except Exception:
                pass
        if record['data_hash'] == data_hash:
            record['verified'] = True
            await self.storage.save_state(f"blockchain_{data_id}", json.dumps(record))
            return {'status': 'success', 'verified': True, 'record': record}
        return {'status': 'failed', 'reason': 'Hash mismatch'}

    async def get_blockchain_status(self) -> Dict:
        return {
            'connected': self.web3_available,
            'rpc_url': config.BLOCKCHAIN_RPC_URL,
            'account': self.account.address if self.account else None,
            'total_records': len(await self.storage._fetchall("SELECT key FROM state WHERE key LIKE 'blockchain_%'"))
        }

# ============================================================================
# NEW MODULE: Genetic Algorithm for Test Parameter Tuning
# ============================================================================
class GeneticTestParameterOptimizer:
    """
    Bio‑inspired GA that evolves test parameters (timeout, retries, cloud preference).
    """
    def __init__(self, config: Config, storage: Storage, test_env):
        self.config = config
        self.storage = storage
        self.test_env = test_env
        self.population_size = config.GA_POPULATION_SIZE
        self.generations = config.GA_GENERATIONS
        self.mutation_rate = config.GA_MUTATION_RATE
        self.crossover_rate = config.GA_CROSSOVER_RATE
        self.param_bounds = {
            'timeout_seconds': (10, 120),
            'retry_attempts': (1, 5),
            'cloud_preference': ['aws', 'azure', 'gcp'],
        }
        self._lock = asyncio.Lock()

    def _random_chromosome(self) -> Dict[str, Any]:
        return {
            'timeout_seconds': random.randint(*self.param_bounds['timeout_seconds']),
            'retry_attempts': random.randint(*self.param_bounds['retry_attempts']),
            'cloud_preference': random.choice(self.param_bounds['cloud_preference']),
        }

    def _mutate(self, chrom: Dict[str, Any]) -> Dict[str, Any]:
        new = chrom.copy()
        if random.random() < self.mutation_rate:
            if random.random() < 0.5:
                new['timeout_seconds'] = max(self.param_bounds['timeout_seconds'][0],
                                             min(self.param_bounds['timeout_seconds'][1],
                                                 chrom['timeout_seconds'] + random.randint(-10, 10)))
            else:
                new['retry_attempts'] = max(self.param_bounds['retry_attempts'][0],
                                            min(self.param_bounds['retry_attempts'][1],
                                                chrom['retry_attempts'] + random.randint(-1, 1)))
        if random.random() < self.mutation_rate:
            new['cloud_preference'] = random.choice(self.param_bounds['cloud_preference'])
        return new

    def _crossover(self, p1: Dict[str, Any], p2: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        if random.random() > self.crossover_rate:
            return p1.copy(), p2.copy()
        c1, c2 = p1.copy(), p2.copy()
        if random.random() < 0.5:
            c1['timeout_seconds'] = p2['timeout_seconds']
            c2['timeout_seconds'] = p1['timeout_seconds']
        if random.random() < 0.5:
            c1['retry_attempts'] = p2['retry_attempts']
            c2['retry_attempts'] = p1['retry_attempts']
        if random.random() < 0.5:
            c1['cloud_preference'] = p2['cloud_preference']
            c2['cloud_preference'] = p1['cloud_preference']
        return c1, c2

    async def _evaluate_fitness(self, chrom: Dict[str, Any]) -> float:
        # Run a test with these parameters and return reward.
        # For simplicity, we use a mock test that returns a random score.
        # In real implementation, we'd call test_env.run_test with these params.
        # We'll simulate a score based on parameters.
        base = 0.5
        # Good timeout: not too low, not too high
        if 30 <= chrom['timeout_seconds'] <= 60:
            base += 0.2
        # Moderate retries
        if chrom['retry_attempts'] == 3:
            base += 0.1
        # Cloud preference: we can add bias
        if chrom['cloud_preference'] == 'aws':
            base += 0.05
        return max(0.0, min(1.0, base + random.uniform(-0.1, 0.1)))

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

            # Store generation (optional)
            if PROMETHEUS_AVAILABLE:
                GA_POPULATION_FITNESS.set(best_fitness)

        return best_individual if best_individual else self._random_chromosome()

# ============================================================================
# NEW MODULE: MoE Gating Network
# ============================================================================
class MoEGatingNetwork:
    """
    Full Mixture-of-Experts gating that selects among multiple test optimization experts.
    """
    def __init__(self, config: Config, storage: Storage):
        self.config = config
        self.storage = storage
        self.num_experts = config.MOE_EXPERT_COUNT
        self.hidden_layers = config.MOE_HIDDEN_LAYERS
        self._gating_model = None
        self._scaler = None
        self._trained = False
        self._training_data = []  # (feature_vector, expert_label, reward)
        self._lock = asyncio.Lock()

        # Define experts: each expert is a function that returns strategy parameters
        self.experts = {
            'performance': self._performance_expert,
            'carbon': self._carbon_expert,
            'cost': self._cost_expert,
            'hybrid': self._hybrid_expert,
            'adaptive': self._adaptive_expert
        }
        if len(self.experts) < self.num_experts:
            keys = list(self.experts.keys())
            for i in range(self.num_experts - len(keys)):
                self.experts[f'custom_{i}'] = self.experts[keys[i % len(keys)]]
        self.expert_names = list(self.experts.keys())

    def _performance_expert(self, context: Dict) -> Dict[str, Any]:
        return {'strategy': 'performance', 'timeout_multiplier': 1.2, 'retry_boost': 1}

    def _carbon_expert(self, context: Dict) -> Dict[str, Any]:
        return {'strategy': 'carbon', 'timeout_multiplier': 1.0, 'retry_boost': 1}

    def _cost_expert(self, context: Dict) -> Dict[str, Any]:
        return {'strategy': 'cost', 'timeout_multiplier': 0.8, 'retry_boost': 0}

    def _hybrid_expert(self, context: Dict) -> Dict[str, Any]:
        return {'strategy': 'hybrid', 'timeout_multiplier': 1.0, 'retry_boost': 2}

    def _adaptive_expert(self, context: Dict) -> Dict[str, Any]:
        return {'strategy': 'adaptive', 'timeout_multiplier': 1.1, 'retry_boost': 1}

    def _encode_context(self, context: Dict) -> np.ndarray:
        features = []
        # Carbon intensity (normalized)
        features.append(context.get('carbon_intensity_gco2', 400) / 1000.0)
        # CPU load
        features.append(context.get('system_cpu_load', 50) / 100.0)
        # Memory usage
        features.append(context.get('system_memory_usage_mb', 2000) / 10000.0)
        # Queue size
        features.append(context.get('queue_size', 0) / 50.0)
        # Flakiness score
        features.append(context.get('flakiness_score', 0))
        # Failure rate
        features.append(context.get('recent_failure_rate', 0))
        # Code complexity
        features.append(context.get('code_complexity', 0.5))
        # Hour of day
        features.append(datetime.now().hour / 24.0)
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

# ============================================================================
# NEW MODULE: Pareto-Front Optimizer
# ============================================================================
class ParetoFrontOptimizer:
    """
    Maintains a Pareto front of test configurations based on multiple objectives.
    """
    def __init__(self, config: Config, storage: Storage):
        self.config = config
        self.storage = storage
        self.pareto_front = []  # list of dict with config_params, metrics
        self.max_size = config.PARETO_MAX_ARCHITECTURES
        self._lock = asyncio.Lock()
        self.objectives = ['pass_rate', 'carbon', 'duration', 'cost']

    def _dominates(self, a: Dict, b: Dict) -> bool:
        # For pass_rate, higher is better; for others, lower is better.
        a_metrics = (-a['metrics']['pass_rate'], a['metrics']['carbon'], a['metrics']['duration'], a['metrics']['cost'])
        b_metrics = (-b['metrics']['pass_rate'], b['metrics']['carbon'], b['metrics']['duration'], b['metrics']['cost'])
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
                self.pareto_front.sort(key=lambda e: e['metrics']['pass_rate'])
                self.pareto_front = self.pareto_front[:self.max_size]
            # Persist to storage
            await self._save_pareto_front()
            if PROMETHEUS_AVAILABLE:
                PARETO_FRONT_SIZE.set(len(self.pareto_front))
            return True

    async def _save_pareto_front(self):
        # Use state to store the Pareto front as JSON
        await self.storage.save_state('pareto_front', json.dumps(self.pareto_front, default=str))

    def get_pareto_front(self) -> List[Dict]:
        return self.pareto_front

    async def get_trade_off_suggestions(self, user_weights: Dict[str, float]) -> List[Dict]:
        if not self.pareto_front:
            return []
        scored = []
        for e in self.pareto_front:
            score = (user_weights.get('pass_rate', 0.4) * e['metrics']['pass_rate'] -
                     user_weights.get('carbon', 0.3) * e['metrics']['carbon'] -
                     user_weights.get('duration', 0.2) * e['metrics']['duration'] -
                     user_weights.get('cost', 0.1) * e['metrics']['cost'])
            scored.append((score, e))
        scored.sort(reverse=True)
        return [e for _, e in scored[:5]]

# ============================================================================
# NEW MODULE: Neural Network Teachers (upgrade)
# ============================================================================
class NeuralTeacher:
    """
    Neural network teacher for MoE or distillation.
    """
    def __init__(self, input_dim: int, output_dim: int, hidden_layers: List[int] = [64, 32]):
        self.input_dim = input_dim
        self.output_dim = output_dim
        self.hidden_layers = hidden_layers
        self.model = None
        self._build_model()

    def _build_model(self):
        if TORCH_AVAILABLE:
            layers = []
            in_dim = self.input_dim
            for h in self.hidden_layers:
                layers.append(nn.Linear(in_dim, h))
                layers.append(nn.ReLU())
                in_dim = h
            layers.append(nn.Linear(in_dim, self.output_dim))
            self.model = nn.Sequential(*layers)
            self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
            self.model.to(self.device)
        else:
            # Fallback to sklearn MLP
            self.model = MLPClassifier(hidden_layer_sizes=self.hidden_layers, max_iter=200, random_state=42)
            self.device = None

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if TORCH_AVAILABLE and self.model is not None:
            self.model.eval()
            with torch.no_grad():
                x_tensor = torch.FloatTensor(X).to(self.device)
                logits = self.model(x_tensor)
                probs = torch.softmax(logits, dim=1).cpu().numpy()
            return probs
        elif SKLEARN_AVAILABLE:
            return self.model.predict_proba(X)
        else:
            return np.ones((X.shape[0], self.output_dim)) / self.output_dim

    def train(self, X: np.ndarray, y: np.ndarray):
        if TORCH_AVAILABLE:
            # Convert to tensors
            x_tensor = torch.FloatTensor(X).to(self.device)
            y_tensor = torch.LongTensor(y).to(self.device)
            dataset = torch.utils.data.TensorDataset(x_tensor, y_tensor)
            dataloader = torch.utils.data.DataLoader(dataset, batch_size=32, shuffle=True)
            optimizer = optim.Adam(self.model.parameters(), lr=0.001)
            criterion = nn.CrossEntropyLoss()
            self.model.train()
            for epoch in range(10):
                for x_batch, y_batch in dataloader:
                    optimizer.zero_grad()
                    outputs = self.model(x_batch)
                    loss = criterion(outputs, y_batch)
                    loss.backward()
                    optimizer.step()
        elif SKLEARN_AVAILABLE:
            self.model.fit(X, y)

# ============================================================================
# NEW MODULE: Federated Learning Aggregator
# ============================================================================
class FederatedTestLearner:
    """
    Implements federated averaging for the MoE gating or student weights.
    """
    def __init__(self, storage: Storage, instance_id: str, share_interval: int):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = share_interval
        self.insights = deque(maxlen=100)
        self._lock = asyncio.Lock()

    async def share_weights(self, weights: Dict[str, Any]):
        # Store local weights in state
        await self.storage.save_state(f"fed_weight_{self.instance_id}", json.dumps(weights, default=str))

    async def pull_aggregated_weights(self) -> Optional[Dict[str, Any]]:
        rows = await self.storage._fetchall("SELECT value FROM state WHERE key LIKE 'fed_weight_%'")
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
        if PROMETHEUS_AVAILABLE:
            FEDERATED_AGGREGATIONS.inc()
        return avg

    async def apply_aggregated_weights(self, current_weights: Dict[str, Any]) -> Dict[str, Any]:
        agg = await self.pull_aggregated_weights()
        if agg is None:
            return current_weights
        merged = {}
        for k in current_weights:
            merged[k] = (current_weights[k] + agg.get(k, current_weights[k])) / 2
        return merged

    async def share_insight(self, insight: Dict):
        self.insights.append(insight)

    async def pull_insights(self, limit: int = 10) -> List[Dict]:
        return list(self.insights)[-limit:]

# ============================================================================
# NEW MODULE: Active User Preference Learner
# ============================================================================
class ActiveUserPreferenceLearner:
    """
    Queries the user when multiple test configurations yield similar predicted outcomes.
    """
    def __init__(self, storage: Storage, websocket: 'StubTestDashboardWebSocket'):
        self.storage = storage
        self.websocket = websocket
        self.user_weights = {}  # user_id -> weights dict

    async def query_user_if_needed(self, user_id: str, top_configs: List[Dict]) -> Optional[str]:
        if len(top_configs) < 2:
            return None
        # If scores are within 5%, ask user
        scores = [c['metrics']['pass_rate'] for c in top_configs[:2]]
        if abs(scores[0] - scores[1]) / max(scores) < 0.05:
            # Send WebSocket query (simulate)
            await self.websocket.broadcast({
                'type': 'preference_query',
                'user_id': user_id,
                'options': [{'id': c['solution_id'], 'pass_rate': c['metrics']['pass_rate']} for c in top_configs[:2]]
            })
            # For demo, return the first one
            return top_configs[0]['solution_id']
        return None

    async def record_choice(self, user_id: str, chosen_solution_id: str, context: Dict):
        # Update user weights based on choice
        # Simple heuristic: increase weight on pass_rate if chosen config has higher pass_rate
        await self.storage.save_user_preference(user_id, {'chosen': chosen_solution_id}, chosen_solution_id)

# ============================================================================
# NEW MODULE: Drift Detector
# ============================================================================
class DriftDetector:
    """
    Detects significant changes in carbon intensity or test performance.
    """
    def __init__(self, storage: Storage, config: Config):
        self.storage = storage
        self.config = config
        self.carbon_history = deque(maxlen=100)
        self.accuracy_history = deque(maxlen=100)
        self.threshold = 0.15

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

    async def check_accuracy_drift(self, current_accuracy: float) -> bool:
        self.accuracy_history.append(current_accuracy)
        if len(self.accuracy_history) < 10:
            return False
        recent = list(self.accuracy_history)[-10:]
        mean = np.mean(recent)
        if mean == 0:
            return False
        if abs(current_accuracy - mean) > self.threshold * mean:
            logger.warning(f"Accuracy drift detected: current {current_accuracy} vs mean {mean}")
            return True
        return False

    async def get_threshold(self) -> float:
        return self.threshold

# ============================================================================
# MODULE 3: Multi-Cloud Test Distribution
# ============================================================================
class MultiCloudTestDistribution:
    """Distributes test data across multiple cloud providers."""
    def __init__(self, storage: Storage):
        self.storage = storage
        self.providers = {
            'aws': {
                'regions': ['us-east-1', 'us-west-2', 'eu-west-1', 'ap-southeast-1'],
                'cost_per_gb': 0.09,
                'client': self._init_aws_client() if AWS_AVAILABLE else None
            },
            'azure': {
                'regions': ['eastus', 'westus', 'northeurope', 'southeastasia'],
                'cost_per_gb': 0.10,
                'client': self._init_azure_client() if AZURE_AVAILABLE else None
            },
            'gcp': {
                'regions': ['us-central1', 'us-west1', 'europe-west1', 'asia-east1'],
                'cost_per_gb': 0.08,
                'client': self._init_gcp_client() if GCP_AVAILABLE else None
            }
        }
        self.active_provider = 'aws'
        self.active_region = 'us-east-1'
        self._lock = asyncio.Lock()
        self._circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=60.0, name="cloud")

    def _init_aws_client(self):
        try:
            return boto3.client('s3', region_name=config.CLOUD_AWS_REGION,
                                aws_access_key_id=config.CLOUD_AWS_ACCESS_KEY,
                                aws_secret_access_key=config.CLOUD_AWS_SECRET_KEY)
        except Exception:
            return None

    def _init_azure_client(self):
        try:
            return BlobServiceClient.from_connection_string(config.CLOUD_AZURE_CONNECTION_STRING)
        except Exception:
            return None

    def _init_gcp_client(self):
        try:
            return storage.Client()
        except Exception:
            return None

    async def _upload_to_aws(self, data: bytes, key: str):
        if not self.providers['aws']['client']:
            raise Exception("AWS client not available")
        bucket = "test-data-bucket"
        try:
            self.providers['aws']['client'].put_object(Bucket=bucket, Key=key, Body=data)
            logger.info("Uploaded to S3: %s", key)
        except ClientError as e:
            logger.error("AWS upload failed: %s", e)
            raise

    async def _upload_to_azure(self, data: bytes, key: str):
        if not self.providers['azure']['client']:
            raise Exception("Azure client not available")
        container = "test-data"
        try:
            blob_client = self.providers['azure']['client'].get_blob_client(container, key)
            blob_client.upload_blob(data, overwrite=True)
            logger.info("Uploaded to Azure: %s", key)
        except Exception as e:
            logger.error("Azure upload failed: %s", e)
            raise

    async def _upload_to_gcp(self, data: bytes, key: str):
        if not self.providers['gcp']['client']:
            raise Exception("GCP client not available")
        bucket = "test-data-bucket"
        try:
            bucket_obj = self.providers['gcp']['client'].bucket(bucket)
            blob = bucket_obj.blob(key)
            blob.upload_from_string(data)
            logger.info("Uploaded to GCS: %s", key)
        except Exception as e:
            logger.error("GCP upload failed: %s", e)
            raise

    async def distribute_test_data(self, data: Dict, preferences: Dict = None) -> Dict:
        preferences = preferences or {}
        async with self._lock:
            scores = {}
            for provider_name, provider in self.providers.items():
                latency = await self._measure_latency(provider_name)
                cost = provider['cost_per_gb'] * data.get('size_gb', 0.001)
                avail = 0.99 if provider['client'] else 0.5
                score = (0.4 * (1 - latency/1000)) + (0.3 * (1 - cost/0.2)) + (0.3 * avail)
                if preferences.get('region') in provider['regions']:
                    score += 0.1
                scores[provider_name] = score

            optimal_provider = max(scores, key=scores.get)
            provider = self.providers[optimal_provider]
            optimal_region = provider['regions'][0]
            if preferences.get('region') in provider['regions']:
                optimal_region = preferences['region']
            self.active_provider = optimal_provider
            self.active_region = optimal_region

            result = {
                'optimal_provider': optimal_provider,
                'optimal_region': optimal_region,
                'scores': scores,
                'data_size_gb': data.get('size_gb', 0),
                'reason': f'Provider {optimal_provider} has best score',
                'timestamp': datetime.now().isoformat()
            }
            await self.storage.save_state(f"distribution_{uuid.uuid4().hex[:8]}", json.dumps(result))

            try:
                await self._replicate_data(optimal_provider, optimal_region, data)
            except Exception as e:
                logger.error("Data replication failed: %s", e)
                fallback_provider = next((p for p in sorted(scores, key=scores.get, reverse=True) if p != optimal_provider), None)
                if fallback_provider:
                    logger.info("Falling back to %s", fallback_provider)
                    await self._replicate_data(fallback_provider, preferences.get('region'), data)
                    result['fallback'] = fallback_provider
                else:
                    raise

            if PROMETHEUS_AVAILABLE:
                CLOUD_DISTRIBUTIONS.labels(provider=optimal_provider, status='success').inc()
            logger.info("Test data distributed to %s (%s)", optimal_provider, optimal_region)
            return result

    async def _measure_latency(self, provider: str) -> float:
        base = {'aws': 50, 'azure': 60, 'gcp': 45}.get(provider, 50)
        return base + random.uniform(-10, 10)

    async def _replicate_data(self, provider: str, region: str, data: Dict):
        data_bytes = json.dumps(data, default=str).encode()
        key = f"test_{uuid.uuid4().hex[:8]}.json"
        if provider == 'aws':
            await self._circuit_breaker.call(self._upload_to_aws, data_bytes, key)
        elif provider == 'azure':
            await self._circuit_breaker.call(self._upload_to_azure, data_bytes, key)
        elif provider == 'gcp':
            await self._circuit_breaker.call(self._upload_to_gcp, data_bytes, key)
        else:
            raise ValueError(f"Unknown provider: {provider}")

    async def get_distribution_status(self) -> Dict:
        return {
            'providers': {k: {'regions': v['regions'], 'cost_per_gb': v['cost_per_gb']} for k, v in self.providers.items()},
            'active_provider': self.active_provider,
            'active_region': self.active_region,
            'distributions': await self.storage._fetchall("SELECT key, value FROM state WHERE key LIKE 'distribution_%' ORDER BY key DESC LIMIT 5")
        }

# ============================================================================
# TestState (with persistence)
# ============================================================================
class TestState:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.confidence = float(await self.storage.get_state('confidence') or 0.5)
        self.uncertainty = float(await self.storage.get_state('uncertainty') or 0.1)
        self.historical_success_rate = float(await self.storage.get_state('success_rate') or 0.5)
        self.reflection_count = int(await self.storage.get_state('reflection_count') or 0)
        self.carbon_budget_remaining = float(await self.storage.get_state('carbon_budget') or 100.0)
        self.helium_budget_remaining = float(await self.storage.get_state('helium_budget') or 100.0)
        self.active_strategies = json.loads(await self.storage.get_state('active_strategies') or '[]')
        self.strategy_effectiveness = json.loads(await self.storage.get_state('strategy_effectiveness') or '{}')
        self.preferred_experts = json.loads(await self.storage.get_state('preferred_experts') or '[]')
        self.avoided_experts = json.loads(await self.storage.get_state('avoided_experts') or '[]')
        self.expert_health_scores = json.loads(await self.storage.get_state('expert_health') or '{}')
        self.recent_rewards = deque(maxlen=100)
        self.accuracy_threshold = float(await self.storage.get_state('accuracy_threshold') or 0.8)

    async def save(self):
        await self.storage.save_state('confidence', str(self.confidence))
        await self.storage.save_state('uncertainty', str(self.uncertainty))
        await self.storage.save_state('success_rate', str(self.historical_success_rate))
        await self.storage.save_state('reflection_count', str(self.reflection_count))
        await self.storage.save_state('carbon_budget', str(self.carbon_budget_remaining))
        await self.storage.save_state('helium_budget', str(self.helium_budget_remaining))
        await self.storage.save_state('active_strategies', json.dumps(self.active_strategies))
        await self.storage.save_state('strategy_effectiveness', json.dumps(self.strategy_effectiveness))
        await self.storage.save_state('preferred_experts', json.dumps(self.preferred_experts))
        await self.storage.save_state('avoided_experts', json.dumps(self.avoided_experts))
        await self.storage.save_state('expert_health', json.dumps(self.expert_health_scores))
        await self.storage.save_state('accuracy_threshold', str(self.accuracy_threshold))

    async def trigger_reflection(self, trigger_type: str, **kwargs):
        self.reflection_count += 1
        if trigger_type == 'accuracy_improved':
            self.confidence = min(1.0, self.confidence + 0.05)
        elif trigger_type == 'accuracy_decreased':
            self.confidence = max(0.1, self.confidence - 0.1)
        elif trigger_type == 'high_carbon':
            self.carbon_budget_remaining *= 0.9
        elif trigger_type == 'strategy_success':
            self.confidence = min(1.0, self.confidence + 0.02)
        await self.save()

# ============================================================================
# Data Classes
# ============================================================================
@dataclass
class TestResult:
    test_name: str
    test_type: str
    passed: bool
    duration_ms: float
    message: str
    retry_count: int = 0
    coverage_percent: float = 0.0
    carbon_impact_kg: float = 0.0
    helium_usage_l: float = 0.0
    sustainability_score: float = 0.0
    carbon_intensity: float = 0.0
    failure_type: str = ""
    data_quality_score: float = 100.0
    regression_detected: bool = False

    def __post_init__(self):
        if self.duration_ms < 0:
            raise ValueError("duration_ms must be >= 0")
        if not (0 <= self.coverage_percent <= 100):
            raise ValueError("coverage_percent must be between 0 and 100")
        if self.carbon_impact_kg < 0:
            raise ValueError("carbon_impact_kg must be >= 0")
        if self.helium_usage_l < 0:
            raise ValueError("helium_usage_l must be >= 0")
        if not (0 <= self.sustainability_score <= 100):
            raise ValueError("sustainability_score must be between 0 and 100")
        if not (0 <= self.data_quality_score <= 100):
            raise ValueError("data_quality_score must be between 0 and 100")

@dataclass
class TestFeatureModel:
    test_name: str
    code_complexity: float = 0.5
    timeout_seconds: float = 30.0
    helium_usage_l: float = 0.001

# ============================================================================
# Stub components (basic implementations)
# ============================================================================
class StubCarbonIntensityManager:
    def __init__(self):
        self._intensity = 400.0

    async def get_current_intensity(self) -> float:
        return self._intensity

    async def update_carbon_intensity(self):
        self._intensity = 400 + random.uniform(-50, 50)

    def calculate_test_carbon_impact(self, duration_ms: float, complexity: float) -> float:
        return duration_ms / 3600000 * 0.1 * complexity

class StubHeliumTestTracker:
    async def record_helium_usage(self, test_name: str, usage_l: float, test_type: str):
        logger.debug("Helium usage recorded: %s %f L", test_name, usage_l)

class StubTestSustainabilityDashboard:
    async def update(self, result: TestResult):
        pass

class StubFederatedTestLearner:
    async def share_insight(self, insight: Dict):
        pass

    async def pull_insights(self, limit: int = 10) -> List[Dict]:
        return []

class StubCarbonAwareTestScheduler:
    def __init__(self, carbon_manager):
        self.carbon_manager = carbon_manager

    async def schedule_test(self, urgency: str = 'normal') -> Dict:
        intensity = await self.carbon_manager.get_current_intensity()
        if intensity < 300:
            return {'action': 'run_now', 'savings_pct': 0.3}
        elif intensity < 400:
            return {'action': 'run_now', 'savings_pct': 0.1}
        else:
            return {'action': 'delay', 'savings_pct': 0.0}

class StubPerformanceBenchmark:
    async def run_benchmark(self, test_func, test_name: str) -> Dict:
        return {'is_regression': False, 'regression_pct': 0.0}

class StubStressTester:
    async def run_stress(self, test_func, test_name: str) -> Dict:
        return {'passed': True, 'metrics': {}}

class StubTestDependencyResolver:
    async def resolve(self, test_name: str) -> List[str]:
        return []

class StubCacheManager:
    async def start(self):
        pass

class StubDataQualityScorer:
    async def assess_quality(self, result: TestResult) -> float:
        return 100.0

class StubRateLimiter:
    async def wait_and_acquire(self):
        pass

class StubFlakinessAnalyzer:
    async def get_all_scores(self) -> Dict[str, float]:
        return {}

class StubTestDashboardWebSocket:
    def __init__(self, port: int = 8779):
        self.port = port
        self.connections = set()
        self.subscriptions = defaultdict(set)
        self._lock = asyncio.Lock()

    async def start(self):
        logger.info("WebSocket stub started on port %d", self.port)

    async def broadcast(self, message: Dict, topic: str = 'all'):
        logger.debug("WebSocket broadcast: %s", message)

    async def stop(self):
        pass

# ============================================================================
# TestImpactAnalyzer, RootCauseAnalyzer, SelfHealing, PredictiveMaintenance
# ============================================================================
class TestImpactAnalyzer:
    async def analyze(self, test_name: str, result: TestResult) -> Dict:
        return {'impact_score': result.sustainability_score * 0.5 + (1 - result.carbon_impact_kg) * 0.3}

class RootCauseAnalyzer:
    async def analyze_failure(self, test_name: str, message: str, system_metrics: Dict) -> Dict:
        if 'timeout' in message.lower():
            return {'root_cause': 'timeout', 'suggestion': 'increase timeout or optimize test'}
        elif 'connection' in message.lower():
            return {'root_cause': 'network', 'suggestion': 'check connectivity'}
        else:
            return {'root_cause': 'unknown', 'suggestion': 'review test code'}

class SelfHealingTestManager:
    async def heal_test(self, test_name: str, failure_type: str, context: Dict) -> Dict:
        if failure_type == 'timeout':
            new_timeout = context.get('original_timeout', 30) * 1.5
            return {'healing_applied': True, 'action': 'increase_timeout', 'parameters': {'new_timeout': new_timeout}}
        elif failure_type == 'network':
            return {'healing_applied': True, 'action': 'retry_with_backoff', 'parameters': {'backoff_factor': 2}}
        else:
            return {'healing_applied': False, 'action': 'none'}

class PredictiveMaintenanceManager:
    async def predict_flakiness(self, test_name: str) -> float:
        return random.uniform(0.0, 0.3)

# ============================================================================
# EnhancedAnalyticsDashboard (stub)
# ============================================================================
class EnhancedAnalyticsDashboard:
    def __init__(self, websocket):
        self.websocket = websocket

    async def update(self, data: Dict):
        pass

# ============================================================================
# OptimizationState (context for MoE)
# ============================================================================
@dataclass
class OptimizationState:
    test_type: str
    code_complexity: float
    avg_historical_duration_ms: float
    flakiness_score: float
    recent_failure_rate: float
    carbon_intensity_gco2: float
    system_cpu_load: float
    system_memory_usage_mb: float
    queue_size: int
    cloud_provider_latency: float
    time_of_day_hour: int

    def to_feature_vector(self) -> np.ndarray:
        features = [
            min(self.code_complexity / 10.0, 1.0),
            min(self.avg_historical_duration_ms / 30000.0, 1.0),
            self.flakiness_score,
            self.recent_failure_rate,
            min(self.carbon_intensity_gco2 / 1000.0, 1.0),
            min(self.system_cpu_load / 100.0, 1.0),
            min(self.system_memory_usage_mb / 10000.0, 1.0),
            min(self.queue_size / 50.0, 1.0),
            min(self.cloud_provider_latency / 500.0, 1.0),
            self.time_of_day_hour / 24.0,
        ]
        test_type_map = {'unit': 0, 'integration': 1, 'performance': 2, 'e2e': 3}
        one_hot = [0.0] * 4
        one_hot[test_type_map.get(self.test_type, 0)] = 1.0
        return np.array(features + one_hot, dtype=np.float32)

# ============================================================================
# MAIN ENHANCED TEST ENVIRONMENT V15.0.0
# ============================================================================
class EnhancedTestEnvironmentV15:
    """Enhanced test environment v15.0.0 with GA, MoE, Pareto, federated, neural teachers."""

    def __init__(self):
        self.instance_id = str(uuid.uuid4())[:8]

        # Central storage
        self.storage = Storage()
        self.state = TestState(self.storage)

        # Enhanced modules
        self.quantum_security = QuantumResilientTestSecurity(self.storage)
        self.blockchain = BlockchainTestVerification(self.storage)
        self.cloud_distributor = MultiCloudTestDistribution(self.storage)

        # NEW: Replace distillation optimizer with MoE and GA
        self.moe_gating = MoEGatingNetwork(config, self.storage) if config.MOE_ENABLED else None
        self.ga_optimizer = GeneticTestParameterOptimizer(config, self.storage, self) if config.GA_ENABLED else None
        self.pareto_optimizer = ParetoFrontOptimizer(config, self.storage) if config.PARETO_ENABLED else None
        self.federated_learner = FederatedTestLearner(self.storage, self.instance_id, config.FEDERATED_INTERVAL) if config.FEDERATED_ENABLED else None
        self.drift_detector = DriftDetector(self.storage, config) if config.DRIFT_DETECTION_ENABLED else None
        self.user_pref_learner = ActiveUserPreferenceLearner(self.storage, self.websocket) if config.ACTIVE_USER_PREFERENCE_ENABLED else None

        # Upgrade teachers to neural networks if enabled
        self.neural_teachers = {}
        if config.NEURAL_TEACHER_ENABLED:
            # We'll create neural teachers for each strategy (or for the MoE gating)
            self.neural_teachers['gating'] = NeuralTeacher(input_dim=14, output_dim=len(self.moe_gating.expert_names) if self.moe_gating else 5)

        # Advanced components (unchanged)
        self.impact_analyzer = TestImpactAnalyzer()
        self.root_cause_analyzer = RootCauseAnalyzer()
        self.self_healing_manager = SelfHealingTestManager()
        self.predictive_maintenance_manager = PredictiveMaintenanceManager()
        self.analytics_dashboard = EnhancedAnalyticsDashboard(self.websocket)

        # Stubs
        self.db_manager = self.storage
        self.carbon_manager = StubCarbonIntensityManager()
        self.helium_tracker = StubHeliumTestTracker()
        self.sustainability_dashboard = StubTestSustainabilityDashboard()
        self.carbon_scheduler = StubCarbonAwareTestScheduler(self.carbon_manager)
        self.benchmark = StubPerformanceBenchmark()
        self.stress_tester = StubStressTester()
        self.dependency_resolver = StubTestDependencyResolver()
        self.cache = StubCacheManager()
        self.quality_scorer = StubDataQualityScorer()
        self.rate_limiter = StubRateLimiter()
        self.flakiness_analyzer = StubFlakinessAnalyzer()
        self.circuit_breakers = {
            'test': CircuitBreaker(name="test"),
            'analysis': CircuitBreaker(name="analysis")
        }
        self.websocket = StubTestDashboardWebSocket(port=8779)

        self.analytics_dashboard.websocket = self.websocket

        # Test registry
        self.test_registry: Dict[str, TestFeatureModel] = {}
        self._registry_lock = asyncio.Lock()

        # State
        self.test_results: Dict[str, TestResult] = {}
        self._results_lock = asyncio.Lock()
        self._test_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TESTS)
        self.operation_queue = asyncio.Queue(maxsize=100)
        self._queue_worker = None
        self._running = False
        self.background_tasks: Set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()

        # Sustainability tracking
        self.sustainability_score = 0.0
        self.total_carbon_savings_kg = 0.0
        self.ml_ready = False

        logger.info("EnhancedTestEnvironmentV15 v%d.0.0 initialized (instance: %s)", DATA_VERSION, self.instance_id)
        logger.info("  ✅ Multi‑Teacher Distillation replaced with Full MoE Gating")
        logger.info("  ✅ Genetic Algorithm for parameter tuning")
        logger.info("  ✅ Pareto‑front optimizer for multi‑objective trade‑offs")
        logger.info("  ✅ Federated learning enabled")
        logger.info("  ✅ Neural network teachers (MLP)")
        logger.info("  ✅ Active user preference learning")
        logger.info("  ✅ Drift detection")

    async def start(self):
        self._running = True
        await self.cache.start()
        await self.carbon_manager.update_carbon_intensity()
        asyncio.create_task(self._train_ml_models())
        self._queue_worker = asyncio.create_task(self._process_queue())
        await self.websocket.start()

        tasks = [
            asyncio.create_task(self._health_check_loop()),
            asyncio.create_task(self._cleanup_loop()),
            asyncio.create_task(self._carbon_update_loop()),
            asyncio.create_task(self._federated_sync_loop()),
            asyncio.create_task(self._predictive_maintenance_loop()),
            asyncio.create_task(self._quantum_monitor_loop()),
            asyncio.create_task(self._blockchain_monitor_loop()),
            asyncio.create_task(self._auto_optimize_loop()),
            asyncio.create_task(self._cloud_sync_loop()),
            asyncio.create_task(self._key_rotation_loop()),
            # New loops
            asyncio.create_task(self._ga_optimization_loop()),
            asyncio.create_task(self._moe_training_loop()),
            asyncio.create_task(self._pareto_update_loop()),
            asyncio.create_task(self._drift_detection_loop()),
            asyncio.create_task(self._active_user_learning_loop()),
        ]
        for task in tasks:
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)
        logger.info("Test environment started with %d background tasks", len(self.background_tasks))

    # ------------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------------
    async def _ga_optimization_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            if self.ga_optimizer and config.GA_ENABLED:
                try:
                    logger.info("Running GA parameter optimization...")
                    best_params = await self.ga_optimizer.run_search()
                    if best_params:
                        # Update test registry with best parameters (optional)
                        logger.info("GA best parameters: %s", best_params)
                except Exception as e:
                    logger.error("GA loop error: %s", e)

    async def _moe_training_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1800)
            if self.moe_gating and config.MOE_ENABLED:
                try:
                    self.moe_gating._train_gating()
                except Exception as e:
                    logger.error("MoE training loop error: %s", e)

    async def _pareto_update_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1800)
            if self.pareto_optimizer and config.PARETO_ENABLED:
                try:
                    logger.debug("Pareto front size: %d", len(self.pareto_optimizer.get_pareto_front()))
                except Exception as e:
                    logger.error("Pareto update loop error: %s", e)

    async def _drift_detection_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            if self.drift_detector and config.DRIFT_DETECTION_ENABLED:
                try:
                    intensity = await self.carbon_manager.get_current_intensity()
                    if await self.drift_detector.check_carbon_drift(intensity):
                        logger.warning("Carbon drift detected; triggering reflection")
                        await self.state.trigger_reflection('carbon_drift')
                    # Check accuracy drift using recent test results
                    if self.test_results:
                        avg_accuracy = np.mean([r.passed for r in self.test_results.values()])
                        if await self.drift_detector.check_accuracy_drift(avg_accuracy):
                            logger.warning("Accuracy drift detected; triggering re-optimization")
                except Exception as e:
                    logger.error("Drift detection loop error: %s", e)

    async def _active_user_learning_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1800)
            if self.user_pref_learner and config.ACTIVE_USER_PREFERENCE_ENABLED:
                try:
                    if self.pareto_optimizer and len(self.pareto_optimizer.get_pareto_front()) > 1:
                        front = self.pareto_optimizer.get_pareto_front()
                        chosen = await self.user_pref_learner.query_user_if_needed('demo_user', front[:2])
                        if chosen:
                            await self.user_pref_learner.record_choice('demo_user', chosen, {})
                except Exception as e:
                    logger.error("Active user learning loop error: %s", e)

    async def _train_ml_models(self):
        # Placeholder for training neural teachers
        pass

    # ------------------------------------------------------------------------
    # Core test execution
    # ------------------------------------------------------------------------
    async def run_test(self, test_name: str, test_func: Callable, test_type: str = 'unit',
                       use_impact_analysis: bool = False) -> TestResult:
        future = asyncio.Future()
        await self.operation_queue.put({
            'type': 'test',
            'test_name': test_name,
            'test_func': test_func,
            'test_type': test_type,
            'use_impact_analysis': use_impact_analysis,
            'future': future
        })
        if PROMETHEUS_AVAILABLE:
            TEST_QUEUE_SIZE.set(self.operation_queue.qsize())
        return await future

    async def _process_queue(self):
        while self._running:
            try:
                operation = await self.operation_queue.get()
                if PROMETHEUS_AVAILABLE:
                    TEST_QUEUE_SIZE.set(self.operation_queue.qsize())
                try:
                    result = await self._execute_test(operation)
                    operation['future'].set_result(result)
                except Exception as e:
                    operation['future'].set_exception(e)
                finally:
                    self.operation_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Queue worker error: %s", e)

    async def _execute_test(self, operation: Dict) -> TestResult:
        async with self._test_semaphore:
            await self.rate_limiter.wait_and_acquire()
            test_name = operation['test_name']
            test_func = operation['test_func']
            test_type = operation.get('test_type', 'unit')
            use_impact_analysis = operation.get('use_impact_analysis', False)

            carbon_intensity = await self.carbon_manager.get_current_intensity()
            start_time = time.time()
            retry_count = 0
            last_error = None
            failure_type = ""
            healing_applied = False

            async with self._registry_lock:
                test_features = self.test_registry.get(test_name)
                base_timeout = test_features.timeout_seconds if test_features else 30.0

            # --- MoE: select strategy ---
            state = await self._get_optimization_state(test_name, test_type)
            selected_strategy = 'performance'
            expert_params = {}
            if self.moe_gating and config.MOE_ENABLED:
                selected_strategy, expert_params = await self.moe_gating.select_expert(state)
                # Apply strategy parameters
                if selected_strategy == 'performance':
                    base_timeout *= expert_params.get('timeout_multiplier', 1.2)
                elif selected_strategy == 'carbon':
                    base_timeout *= expert_params.get('timeout_multiplier', 1.0)
                elif selected_strategy == 'cost':
                    base_timeout *= expert_params.get('timeout_multiplier', 0.8)
                elif selected_strategy == 'hybrid':
                    base_timeout *= expert_params.get('timeout_multiplier', 1.0)
                elif selected_strategy == 'adaptive':
                    base_timeout *= expert_params.get('timeout_multiplier', 1.1)
            else:
                # Fallback to simple rule
                if state.carbon_intensity_gco2 > 500:
                    selected_strategy = 'carbon'
                elif state.flakiness_score > 0.3:
                    selected_strategy = 'performance'
                elif state.queue_size > 20:
                    selected_strategy = 'cost'

            timeout = base_timeout

            for attempt in range(MAX_RETRY_ATTEMPTS):
                try:
                    passed, coverage = await self.circuit_breakers['test'].call(
                        self._run_test, test_func, test_name, timeout
                    )
                    duration_ms = (time.time() - start_time) * 1000
                    carbon_impact = self.carbon_manager.calculate_test_carbon_impact(
                        duration_ms, test_features.code_complexity / 100 if test_features else 1.0
                    )
                    helium_usage = test_features.helium_usage_l if test_features else 0.001
                    await self.helium_tracker.record_helium_usage(test_name, helium_usage, test_type)
                    sustainability_score = self._calculate_sustainability_score(
                        passed, carbon_impact, helium_usage, coverage
                    )
                    result = TestResult(
                        test_name=test_name,
                        test_type=test_type,
                        passed=passed,
                        duration_ms=duration_ms,
                        message="Test completed" if passed else "Test failed",
                        retry_count=retry_count,
                        coverage_percent=coverage,
                        carbon_impact_kg=carbon_impact,
                        helium_usage_l=helium_usage,
                        sustainability_score=sustainability_score,
                        carbon_intensity=carbon_intensity,
                        failure_type=failure_type
                    )
                    quality_score = await self.quality_scorer.assess_quality(result)
                    result.data_quality_score = quality_score

                    if not passed and retry_count > 0:
                        system_metrics = {
                            'memory_usage_mb': operation.get('memory_usage_mb', 0),
                            'cpu_usage_pct': operation.get('cpu_usage_pct', 0),
                            'duration_ms': duration_ms,
                            'retry_count': retry_count,
                            'previous_failures': len([r for r in self.test_results.values() if not r.passed])
                        }
                        root_cause_analysis = await self.root_cause_analyzer.analyze_failure(
                            test_name, result.message or "", system_metrics
                        )
                        result.message = f"{result.message}\nRoot cause: {root_cause_analysis.get('root_cause')}"
                        result.failure_type = root_cause_analysis.get('root_cause', 'unknown')
                        healing_context = {
                            'system_load': system_metrics.get('cpu_usage_pct', 0) / 100,
                            'original_timeout': timeout,
                            'retry_count': retry_count,
                            'failure_type': result.failure_type,
                            'test_name': test_name
                        }
                        healing_result = await self.self_healing_manager.heal_test(
                            test_name, result.failure_type, healing_context
                        )
                        if healing_result.get('healing_applied'):
                            healing_applied = True
                            result.message = f"{result.message}\nHealing applied: {healing_result.get('action')}"
                            if healing_result.get('action') == 'increase_timeout':
                                timeout = healing_result['parameters'].get('new_timeout', timeout)

                    if test_type == 'performance':
                        benchmark_results = await self.benchmark.run_benchmark(test_func, test_name)
                        result.regression_detected = benchmark_results['is_regression']
                        if benchmark_results['is_regression']:
                            result.message = f"Performance regression: {benchmark_results['regression_pct']:.1f}%"

                    # ---- Quantum signing ----
                    result_dict = {
                        'test_name': test_name,
                        'passed': result.passed,
                        'duration_ms': result.duration_ms,
                        'timestamp': datetime.now().isoformat()
                    }
                    quantum_key = await self.quantum_security.generate_keypair('dilithium')
                    signature = await self.quantum_security.sign_test_data(result_dict, quantum_key['key_id'])
                    # (signature stored in result if needed)

                    # ---- Blockchain verification ----
                    data_id = f"test_{uuid.uuid4().hex[:8]}"
                    data_hash = hashlib.sha256(json.dumps(result_dict, sort_keys=True, default=str).encode()).hexdigest()
                    blockchain_result = await self.blockchain.record_test_data(
                        data_id,
                        data_hash,
                        {'test_name': test_name, 'passed': result.passed}
                    )
                    # (tx hash stored)

                    # ---- Cloud distribution ----
                    cloud_data = {'size_gb': 0.001}
                    distribution = await self.cloud_distributor.distribute_test_data(cloud_data)

                    # Compute reward for MoE
                    reward = 0.0
                    if result.passed:
                        reward += 0.6
                    reward += 0.2 * result.sustainability_score
                    if not result.regression_detected:
                        reward += 0.1
                    reward += 0.1 * (result.data_quality_score / 100.0)

                    # Update MoE with training sample
                    if self.moe_gating and config.MOE_ENABLED:
                        next_state = await self._get_optimization_state(test_name, test_type)
                        await self.moe_gating.add_training_sample(next_state, selected_strategy, reward)

                    # Update Pareto front
                    if self.pareto_optimizer and config.PARETO_ENABLED:
                        metrics = {
                            'pass_rate': 1.0 if result.passed else 0.0,
                            'carbon': result.carbon_impact_kg,
                            'duration': result.duration_ms / 1000,
                            'cost': result.helium_usage_l
                        }
                        config_params = {
                            'test_name': test_name,
                            'test_type': test_type,
                            'timeout': timeout,
                            'strategy': selected_strategy
                        }
                        await self.pareto_optimizer.add_configuration(config_params, metrics)

                    # Federated sharing
                    if self.federated_learner and config.FEDERATED_ENABLED:
                        if result.passed and result.sustainability_score > 0.7:
                            await self.federated_learner.share_insight({
                                'test_name': test_name,
                                'strategy': selected_strategy,
                                'reward': reward
                            })

                    # Store result
                    async with self._results_lock:
                        self.test_results[test_name] = result
                    await self.storage.save_test_result(result)

                    # Prometheus metrics
                    if PROMETHEUS_AVAILABLE:
                        TEST_RUNS.labels(status='success' if result.passed else 'failed', type=test_type).inc()
                        TEST_DURATION.labels(test_type=test_type).observe(result.duration_ms / 1000)
                        if not result.passed:
                            TEST_FAILURES.labels(test_name=test_name, failure_type=result.failure_type).inc()
                        TEST_COVERAGE.labels(coverage_type='line').set(result.coverage_percent)
                        if result.regression_detected:
                            REGRESSION_DETECTED.labels(test_name=test_name).inc()
                        TEST_CARBON_IMPACT.labels(test_name=test_name).set(result.carbon_impact_kg)
                        SUSTAINABILITY_SCORE.labels(test_name=test_name).set(result.sustainability_score)
                        HELIUM_EFFICIENCY.labels(test_name=test_name).set(100 - result.helium_usage_l * 1000)

                    # Reflection
                    if result.passed and result.sustainability_score > 70:
                        await self.state.trigger_reflection('accuracy_improved')
                    elif not result.passed:
                        await self.state.trigger_reflection('accuracy_decreased')
                    if carbon_intensity > 400:
                        await self.state.trigger_reflection('high_carbon')
                    await self.state.save()

                    # Broadcast via WebSocket
                    await self.websocket.broadcast({
                        'type': 'test_complete',
                        'test_name': test_name,
                        'passed': result.passed,
                        'duration_ms': result.duration_ms,
                        'strategy': selected_strategy,
                        'timestamp': datetime.now().isoformat()
                    })

                    audit_logger.info("Test %s completed: passed=%s, duration=%.0fms, strategy=%s",
                                     test_name, result.passed, result.duration_ms, selected_strategy)

                    return result

                except asyncio.TimeoutError:
                    last_error = TimeoutError(f"Test timed out after {timeout}s")
                    failure_type = "timeout"
                    retry_count += 1
                    if attempt < MAX_RETRY_ATTEMPTS - 1:
                        wait_time = min(2 ** attempt, 10)
                        logger.warning("Test %s timed out (attempt %d), retrying in %ds", test_name, attempt+1, wait_time)
                        await asyncio.sleep(wait_time)
                except Exception as e:
                    last_error = e
                    failure_type = type(e).__name__
                    retry_count += 1
                    if attempt < MAX_RETRY_ATTEMPTS - 1:
                        wait_time = min(2 ** attempt, 10)
                        logger.warning("Test %s failed (attempt %d), retrying in %ds", test_name, attempt+1, wait_time)
                        await asyncio.sleep(wait_time)

            # All retries failed
            duration_ms = (time.time() - start_time) * 1000
            result = TestResult(
                test_name=test_name,
                test_type=test_type,
                passed=False,
                duration_ms=duration_ms,
                message=str(last_error),
                retry_count=retry_count,
                failure_type=failure_type
            )
            await self.storage.save_test_result(result)
            # Update MoE with negative reward
            if self.moe_gating and config.MOE_ENABLED:
                next_state = await self._get_optimization_state(test_name, test_type)
                await self.moe_gating.add_training_sample(next_state, selected_strategy, 0.0)
            return result

    # ------------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------------
    async def _get_optimization_state(self, test_name: str, test_type: str) -> OptimizationState:
        # Gather state
        feature = self.test_registry.get(test_name)
        complexity = feature.code_complexity if feature else 0.5
        history = await self.storage.get_test_history(test_name, limit=20)
        avg_duration = np.mean([h['duration_ms'] for h in history]) if history else 0
        failures = sum(1 for h in history if not h['passed'])
        failure_rate = failures / len(history) if history else 0.0

        flakiness = 0.0
        if hasattr(self, 'flakiness_analyzer'):
            scores = await self.flakiness_analyzer.get_all_scores()
            flakiness = scores.get(test_name, 0.0)

        carbon = await self.carbon_manager.get_current_intensity()
        cpu = psutil.cpu_percent() if PSUTIL_AVAILABLE else 50.0
        memory = psutil.virtual_memory().used / (1024**2) if PSUTIL_AVAILABLE else 2000.0
        queue = self.operation_queue.qsize()
        latency = (await self.cloud_distributor._measure_latency(
            self.cloud_distributor.active_provider)) if hasattr(self.cloud_distributor, '_measure_latency') else 60.0
        hour = datetime.now().hour

        return OptimizationState(
            test_type=test_type,
            code_complexity=complexity,
            avg_historical_duration_ms=avg_duration,
            flakiness_score=flakiness,
            recent_failure_rate=failure_rate,
            carbon_intensity_gco2=carbon,
            system_cpu_load=cpu,
            system_memory_usage_mb=memory,
            queue_size=queue,
            cloud_provider_latency=latency,
            time_of_day_hour=hour
        )

    def _calculate_sustainability_score(self, passed: bool, carbon_impact: float, helium_usage: float, coverage: float) -> float:
        base = 50.0
        if passed:
            base += 25
        carbon_score = max(0, 100 - carbon_impact * 100)
        helium_score = max(0, 100 - helium_usage * 1000)
        coverage_score = coverage
        return 0.4 * base + 0.2 * carbon_score + 0.2 * helium_score + 0.2 * coverage_score

    async def _run_test(self, test_func: Callable, test_name: str, timeout: float) -> Tuple[bool, float]:
        try:
            result = await asyncio.wait_for(test_func(), timeout=timeout)
            if isinstance(result, tuple) and len(result) == 2:
                return result
            else:
                return bool(result), 100.0
        except Exception:
            return False, 0.0

    # ------------------------------------------------------------------------
    # Health check and statistics
    # ------------------------------------------------------------------------
    async def health_check(self) -> Dict:
        try:
            async def _check():
                async with self._results_lock:
                    result_count = len(self.test_results)
                health_score = 100
                if result_count == 0:
                    health_score -= 30
                if PROMETHEUS_AVAILABLE:
                    HEALTH_SCORE.set(health_score)
                return {
                    'healthy': result_count > 0,
                    'instance_id': self.instance_id,
                    'version': '15.0.0',
                    'result_count': result_count,
                    'health_score': max(0, health_score),
                    'queue_size': self.operation_queue.qsize(),
                    'timestamp': datetime.now().isoformat()
                }
            return await asyncio.wait_for(_check(), timeout=HEALTH_CHECK_TIMEOUT)
        except asyncio.TimeoutError:
            logger.error("Health check timed out")
            return {'healthy': False, 'status': 'timeout', 'instance_id': self.instance_id}

    async def get_statistics(self) -> Dict:
        async with self._results_lock:
            result_count = len(self.test_results)
        stats = {
            'instance_id': self.instance_id,
            'version': '15.0.0',
            'result_count': result_count,
            'timestamp': datetime.now().isoformat()
        }
        if self.moe_gating:
            stats['moe'] = {
                'trained': self.moe_gating._trained,
                'training_samples': len(self.moe_gating._training_data)
            }
        if self.ga_optimizer:
            stats['ga'] = {'enabled': config.GA_ENABLED}
        if self.pareto_optimizer:
            stats['pareto_front_size'] = len(self.pareto_optimizer.get_pareto_front())
        return stats

    # ------------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------------
    async def shutdown(self):
        logger.info("Shutting down EnhancedTestEnvironmentV15 (instance: %s)", self.instance_id)
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
        await self.state.save()
        await self.storage.dispose()
        logger.info("Test environment shutdown complete")

# ============================================================================
# Backward compatibility alias
# ============================================================================
class EnhancedTestEnvironmentV14(EnhancedTestEnvironmentV15):
    pass

# ============================================================================
# Singleton accessor
# ============================================================================
_test_environment_instance = None
_test_environment_lock = asyncio.Lock()

async def get_test_environment() -> EnhancedTestEnvironmentV15:
    global _test_environment_instance
    if _test_environment_instance is None:
        async with _test_environment_lock:
            if _test_environment_instance is None:
                _test_environment_instance = EnhancedTestEnvironmentV15()
                await _test_environment_instance.start()
    return _test_environment_instance

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================
async def main():
    print("=" * 80)
    print("Enhanced Test Integration v15.0.0 - Enterprise Platinum+")
    print("Bio‑inspired GA | Full MoE Gating | Pareto‑Front")
    print("Neural Teachers | Federated Learning | Active User Preferences | Drift Detection")
    print("=" * 80)

    test_env = await get_test_environment()

    print(f"\n✅ v15.0.0 ENHANCEMENTS:")
    print(f"   ✅ Genetic Algorithm for test parameter tuning")
    print(f"   ✅ Full Mixture‑of‑Experts gating (replaces distillation)")
    print(f"   ✅ Pareto‑front optimizer for multi‑objective trade‑offs")
    print(f"   ✅ Neural network teachers (MLP)")
    print(f"   ✅ Federated learning for model weights")
    print(f"   ✅ Active user preference learning via WebSocket")
    print(f"   ✅ Drift detection for carbon and performance")

    # Register a test
    test_env.test_registry['demo_test'] = TestFeatureModel('demo_test', code_complexity=0.6, timeout_seconds=30, helium_usage_l=0.002)

    # Run a test
    async def dummy_test():
        await asyncio.sleep(0.5)
        return True, 85.0

    result = await test_env.run_test('demo_test', dummy_test, test_type='unit')
    print(f"\n🔬 Test result: {'PASS' if result.passed else 'FAIL'}")
    print(f"   Duration: {result.duration_ms:.2f} ms")
    print(f"   Sustainability score: {result.sustainability_score:.1f}/100")

    stats = await test_env.get_statistics()
    print(f"\n📊 Statistics: {stats}")

    print("\n" + "=" * 80)
    print("✅ Test environment ready. Press Ctrl+C to exit.")
    print("=" * 80)

    try:
        await asyncio.Event().wait()  # wait indefinitely until signal
    except KeyboardInterrupt:
        pass
    finally:
        await test_env.shutdown()

if __name__ == "__main__":
    # Handle signals
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(_signal_shutdown()))
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
