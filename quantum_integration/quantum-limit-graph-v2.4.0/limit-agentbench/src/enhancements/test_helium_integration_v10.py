#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/test_helium_integration_enhanced_v15_0.py
# VERSION: 15.0.0 (Enterprise Quantum Resilience + GA + MoE + Pareto + Federated + Neural Teachers + LIMIT Graph + MODP + RLHF + Distillation)
# =============================================================================
"""
Integration Test for Helium Dataset with All Enhancement Modules - Version 15.0.0
ENHANCED WITH: Bio‑inspired Genetic Algorithm, Full MoE Gating, Pareto‑Front,
Neural Network Teachers, Federated Learning, Active User Preferences, Drift Detection,
LIMIT Graph, MODP, RLHF, and Multi‑Teacher Policy Distillation.
All enhancements are optional and configurable.
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
            # ===== NEW: LIMIT Graph, MODP, RLHF, Distillation configs =====
            self.LIMIT_GRAPH_ENABLED = getattr(central_config, 'test_limit_graph_enabled', True)
            self.LIMIT_GRAPH_UPDATE_INTERVAL = getattr(central_config, 'test_limit_graph_update_interval', 300)
            self.MODP_ENABLED = getattr(central_config, 'test_modp_enabled', True)
            self.MODP_WEIGHTS = getattr(central_config, 'test_modp_weights', [0.25, 0.25, 0.25, 0.25])
            self.RLHF_ENABLED = getattr(central_config, 'test_rlhf_enabled', True)
            self.RLHF_REWARD_MODEL = getattr(central_config, 'test_rlhf_reward_model', 'linear')
            self.RLHF_TRAINING_INTERVAL = getattr(central_config, 'test_rlhf_training_interval', 600)
            self.DISTILLATION_ENABLED = getattr(central_config, 'test_distillation_enabled', True)
            self.DISTILLATION_TEMPERATURE = getattr(central_config, 'test_distillation_temperature', 2.0)
            self.DISTILLATION_ALPHA = getattr(central_config, 'test_distillation_alpha', 0.5)
            self.DISTILLATION_INTERVAL = getattr(central_config, 'test_distillation_interval', 300)

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
            # ===== NEW: LIMIT Graph, MODP, RLHF, Distillation configs =====
            LIMIT_GRAPH_ENABLED: bool = Field(True, env='TEST_LIMIT_GRAPH_ENABLED')
            LIMIT_GRAPH_UPDATE_INTERVAL: int = Field(300, env='TEST_LIMIT_GRAPH_UPDATE_INTERVAL')
            MODP_ENABLED: bool = Field(True, env='TEST_MODP_ENABLED')
            MODP_WEIGHTS: List[float] = Field([0.25, 0.25, 0.25, 0.25], env='TEST_MODP_WEIGHTS')
            RLHF_ENABLED: bool = Field(True, env='TEST_RLHF_ENABLED')
            RLHF_REWARD_MODEL: str = Field("linear", env='TEST_RLHF_REWARD_MODEL')
            RLHF_TRAINING_INTERVAL: int = Field(600, env='TEST_RLHF_TRAINING_INTERVAL')
            DISTILLATION_ENABLED: bool = Field(True, env='TEST_DISTILLATION_ENABLED')
            DISTILLATION_TEMPERATURE: float = Field(2.0, env='TEST_DISTILLATION_TEMPERATURE')
            DISTILLATION_ALPHA: float = Field(0.5, env='TEST_DISTILLATION_ALPHA')
            DISTILLATION_INTERVAL: int = Field(300, env='TEST_DISTILLATION_INTERVAL')

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
            # ===== NEW =====
            LIMIT_GRAPH_ENABLED = os.getenv('TEST_LIMIT_GRAPH_ENABLED', 'True').lower() == 'true'
            LIMIT_GRAPH_UPDATE_INTERVAL = int(os.getenv('TEST_LIMIT_GRAPH_UPDATE_INTERVAL', '300'))
            MODP_ENABLED = os.getenv('TEST_MODP_ENABLED', 'True').lower() == 'true'
            MODP_WEIGHTS = json.loads(os.getenv('TEST_MODP_WEIGHTS', '[0.25,0.25,0.25,0.25]'))
            RLHF_ENABLED = os.getenv('TEST_RLHF_ENABLED', 'True').lower() == 'true'
            RLHF_REWARD_MODEL = os.getenv('TEST_RLHF_REWARD_MODEL', 'linear')
            RLHF_TRAINING_INTERVAL = int(os.getenv('TEST_RLHF_TRAINING_INTERVAL', '600'))
            DISTILLATION_ENABLED = os.getenv('TEST_DISTILLATION_ENABLED', 'True').lower() == 'true'
            DISTILLATION_TEMPERATURE = float(os.getenv('TEST_DISTILLATION_TEMPERATURE', '2.0'))
            DISTILLATION_ALPHA = float(os.getenv('TEST_DISTILLATION_ALPHA', '0.5'))
            DISTILLATION_INTERVAL = int(os.getenv('TEST_DISTILLATION_INTERVAL', '300'))

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
    GA_POPULATION_FITNESS = metrics.gauge('test_ga_population_fitness')
    MOE_GATING_PROBABILITIES = metrics.gauge('test_moe_gating_probabilities', ['expert'])
    PARETO_FRONT_SIZE = metrics.gauge('test_pareto_front_size')
    FEDERATED_AGGREGATIONS = metrics.counter('test_federated_aggregations_total')
    DRIFT_SCORE = metrics.gauge('test_drift_score', ['domain'])
else:
    if PROMETHEUS_AVAILABLE:
        REGISTRY = CollectorRegistry()
        TEST_RUNS = Counter('test_runs_total', 'Total test runs', ['status', 'type'], registry=REGISTRY)
        TEST_DURATION = Histogram('test_duration_seconds', 'Test duration', ['test_type'], registry=REGISTRY)
        TEST_FAILURES = Counter('test_failures_total', 'Total test failures', ['test_name', 'failure_type'], registry=REGISTRY)
        TEST_COVERAGE = Gauge('test_coverage_percent', 'Test coverage percentage', ['coverage_type'], registry=REGISTRY)
        REGRESSION_DETECTED = Counter('test_regressions_total', 'Performance regressions detected', ['test_name'], registry=REGISTRY)
        CIRCUIT_BREAKER_STATE = Gauge('test_circuit_breaker_state', 'Circuit breaker state', ['component'], registry=REGISTRY)
        HEALTH_SCORE = Gauge('test_system_health', 'System health score', registry=REGISTRY)
        DB_SIZE = Gauge('test_db_size_mb', 'Database size', registry=REGISTRY)
        DATA_QUALITY_SCORE = Gauge('test_data_quality', 'Test data quality score', registry=REGISTRY)
        TEST_QUEUE_SIZE = Gauge('test_queue_size', 'Test queue size', registry=REGISTRY)
        WS_CONNECTIONS = Gauge('test_ws_connections', 'WebSocket connections', registry=REGISTRY)
        FLAKINESS_SCORE = Gauge('test_flakiness_score', 'Test flakiness score', ['test_name'], registry=REGISTRY)
        CARBON_INTENSITY = Gauge('carbon_intensity_gco2_per_kwh', 'Real-time carbon intensity', registry=REGISTRY)
        TEST_CARBON_IMPACT = Gauge('test_carbon_impact_kg', 'Carbon impact per test', ['test_name'], registry=REGISTRY)
        SUSTAINABILITY_SCORE = Gauge('test_sustainability_score', 'Sustainability score', ['test_name'], registry=REGISTRY)
        HELIUM_EFFICIENCY = Gauge('test_helium_efficiency', 'Helium efficiency', ['test_name'], registry=REGISTRY)
        CARBON_SAVINGS = Counter('test_carbon_savings_total', 'Total carbon savings', registry=REGISTRY)
        TEST_IMPACT_SCORE = Gauge('test_impact_score', 'Test impact score', ['test_name'], registry=REGISTRY)
        ROOT_CAUSE_ACCURACY = Gauge('root_cause_accuracy', 'Root cause analysis accuracy', registry=REGISTRY)
        SELF_HEALING_SUCCESS = Counter('self_healing_success_total', 'Successful self-healing', ['healing_type'], registry=REGISTRY)
        PREDICTIVE_MAINTENANCE = Counter('predictive_maintenance_total', 'Predictive maintenance actions', ['action_type'], registry=REGISTRY)
        ANALYTICS_QUERIES = Counter('analytics_queries_total', 'Analytics dashboard queries', ['query_type'], registry=REGISTRY)
        QUANTUM_SIGNATURES = Counter('test_quantum_signatures_total', 'Quantum signatures', ['algorithm', 'status'], registry=REGISTRY)
        BLOCKCHAIN_VERIFICATIONS = Counter('test_blockchain_verifications_total', 'Blockchain verifications', ['status'], registry=REGISTRY)
        AUTONOMOUS_OPTIMIZATIONS = Counter('test_autonomous_optimizations_total', 'Autonomous optimizations', ['strategy', 'status'], registry=REGISTRY)
        CLOUD_DISTRIBUTIONS = Counter('test_cloud_distributions_total', 'Cloud distributions', ['provider', 'status'], registry=REGISTRY)
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
        TEST_RUNS = DummyMetric()
        TEST_DURATION = DummyMetric()
        TEST_FAILURES = DummyMetric()
        TEST_COVERAGE = DummyMetric()
        REGRESSION_DETECTED = DummyMetric()
        CIRCUIT_BREAKER_STATE = DummyMetric()
        HEALTH_SCORE = DummyMetric()
        DB_SIZE = DummyMetric()
        DATA_QUALITY_SCORE = DummyMetric()
        TEST_QUEUE_SIZE = DummyMetric()
        WS_CONNECTIONS = DummyMetric()
        FLAKINESS_SCORE = DummyMetric()
        CARBON_INTENSITY = DummyMetric()
        TEST_CARBON_IMPACT = DummyMetric()
        SUSTAINABILITY_SCORE = DummyMetric()
        HELIUM_EFFICIENCY = DummyMetric()
        CARBON_SAVINGS = DummyMetric()
        TEST_IMPACT_SCORE = DummyMetric()
        ROOT_CAUSE_ACCURACY = DummyMetric()
        SELF_HEALING_SUCCESS = DummyMetric()
        PREDICTIVE_MAINTENANCE = DummyMetric()
        ANALYTICS_QUERIES = DummyMetric()
        QUANTUM_SIGNATURES = DummyMetric()
        BLOCKCHAIN_VERIFICATIONS = DummyMetric()
        AUTONOMOUS_OPTIMIZATIONS = DummyMetric()
        CLOUD_DISTRIBUTIONS = DummyMetric()
        GA_POPULATION_FITNESS = DummyMetric()
        MOE_GATING_PROBABILITIES = DummyMetric()
        PARETO_FRONT_SIZE = DummyMetric()
        FEDERATED_AGGREGATIONS = DummyMetric()
        DRIFT_SCORE = DummyMetric()

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
        async def _execute(self, query, params=()):
            if hasattr(self._storage, '_execute_async'):
                return await self._storage._execute_async(query, params)
            return await asyncio.to_thread(self._storage._execute, query, params)
        async def _fetchone(self, query, params=()):
            if hasattr(self._storage, '_fetchone_async'):
                return await self._storage._fetchone_async(query, params)
            return await asyncio.to_thread(self._storage._fetchone, query, params)
        async def _fetchall(self, query, params=()):
            if hasattr(self._storage, '_fetchall_async'):
                return await self._storage._fetchall_async(query, params)
            return await asyncio.to_thread(self._storage._fetchall, query, params)
        # Implement all storage methods...
        async def save_test_result(self, result):
            await self._execute("""
                INSERT INTO test_results
                (test_name, test_type, passed, duration_ms, message, retry_count,
                 coverage_percent, carbon_impact_kg, helium_usage_l, sustainability_score,
                 carbon_intensity, failure_type, data_quality_score, regression_detected, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (result.test_name, result.test_type, 1 if result.passed else 0,
                  result.duration_ms, result.message, result.retry_count,
                  result.coverage_percent, result.carbon_impact_kg, result.helium_usage_l,
                  result.sustainability_score, result.carbon_intensity,
                  result.failure_type, result.data_quality_score,
                  1 if result.regression_detected else 0,
                  datetime.now().isoformat()))
        async def get_test_history(self, test_name, limit=20):
            rows = await self._fetchall("""
                SELECT duration_ms, passed, timestamp FROM test_results
                WHERE test_name = ? ORDER BY timestamp DESC LIMIT ?
            """, (test_name, limit))
            return [{'duration_ms': r[0], 'passed': bool(r[1]), 'timestamp': r[2]} for r in rows]
        async def save_test_feature(self, test_name, features):
            await self._execute("""
                INSERT OR REPLACE INTO test_features (test_name, code_complexity, timeout_seconds, helium_usage_l, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, (test_name, features.get('code_complexity', 0.5),
                  features.get('timeout_seconds', 30.0), features.get('helium_usage_l', 0.001),
                  datetime.now().isoformat()))
        async def get_test_feature(self, test_name):
            row = await self._fetchone("""
                SELECT code_complexity, timeout_seconds, helium_usage_l FROM test_features WHERE test_name = ?
            """, (test_name,))
            if row:
                return {'code_complexity': row[0], 'timeout_seconds': row[1], 'helium_usage_l': row[2]}
            return None
        async def save_state(self, key, value):
            await self._execute("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)", (key, value))
        async def get_state(self, key):
            row = await self._fetchone("SELECT value FROM state WHERE key = ?", (key,))
            return row[0] if row else None
        async def save_user_preference(self, user_id, weights, chosen_solution_id=None):
            await self._execute("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)",
                                (f"user_pref_{user_id}", json.dumps(weights)))
        async def get_user_preference(self, user_id):
            row = await self._fetchone("SELECT value FROM state WHERE key = ?", (f"user_pref_{user_id}",))
            if row:
                return json.loads(row[0])
            return None
        def close(self):
            self._storage.close()
        async def dispose(self):
            self.close()
else:
    # Original custom Storage (with similar tables) – omitted for brevity; copy from original file.
    class Storage:
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
# Quantum-Resilient Test Security
# ============================================================================
class QuantumResilientTestSecurity:
    # ... (implementation as in original, abbreviated)
    pass

# ============================================================================
# Blockchain Test Verification
# ============================================================================
class BlockchainTestVerification:
    # ... (implementation as in original, abbreviated)
    pass

# ============================================================================
# Multi-Cloud Test Distribution
# ============================================================================
class MultiCloudTestDistribution:
    # ... (implementation as in original, abbreviated)
    pass

# ============================================================================
# NEW MODULE: Genetic Algorithm for Test Parameter Tuning
# ============================================================================
class GeneticTestParameterOptimizer:
    def __init__(self, config, storage, test_env):
        # ... (abbreviated)
        pass
    async def run_search(self):
        # ... (abbreviated)
        pass

# ============================================================================
# NEW MODULE: MoE Gating Network
# ============================================================================
class MoEGatingNetwork:
    # ... (implementation as in original)
    pass

# ============================================================================
# NEW MODULE: Pareto-Front Optimizer
# ============================================================================
class ParetoFrontOptimizer:
    # ... (implementation as in original)
    pass

# ============================================================================
# NEW MODULE: Neural Network Teacher
# ============================================================================
class NeuralTeacher:
    # ... (implementation as in original)
    pass

# ============================================================================
# NEW MODULE: Federated Learning Aggregator
# ============================================================================
class FederatedTestLearner:
    # ... (implementation as in original)
    pass

# ============================================================================
# NEW MODULE: Active User Preference Learner
# ============================================================================
class ActiveUserPreferenceLearner:
    # ... (implementation as in original)
    pass

# ============================================================================
# NEW MODULE: Drift Detector
# ============================================================================
class DriftDetector:
    # ... (implementation as in original)
    pass

# =============================================================================
# NEW MODULE: LIMIT Graph Manager
# =============================================================================
class LimitGraphManager:
    def __init__(self, config):
        self.config = config
        self.graph = {}
        self.constraints = {}
        self._lock = asyncio.Lock()
        self._initialize_graph()

    def _initialize_graph(self):
        nodes = ['carbon', 'cost', 'latency', 'quality']
        for n in nodes:
            self.graph[n] = {}
        self.graph['carbon']['cost'] = 0.8
        self.graph['cost']['latency'] = 0.2
        self.graph['latency']['quality'] = -0.3
        self.graph['quality']['cost'] = -0.1

    async def update_constraint(self, name, value):
        async with self._lock:
            self.constraints[name] = value

    async def get_constraint(self, name):
        return self.constraints.get(name, 0.0)

    async def evaluate_path(self, start, end):
        if start not in self.graph or end not in self.graph:
            return 0.0
        visited = set()
        queue = [(start, 1.0)]
        while queue:
            node, weight = queue.pop(0)
            if node == end:
                return weight
            visited.add(node)
            for neighbor, w in self.graph[node].items():
                if neighbor not in visited:
                    queue.append((neighbor, weight * w))
        return 0.0

    async def get_graph_summary(self):
        return {
            'nodes': list(self.graph.keys()),
            'constraints': self.constraints,
            'edge_count': sum(len(v) for v in self.graph.values())
        }

# =============================================================================
# NEW MODULE: MODP Strategy Optimizer
# =============================================================================
class MODPStrategyOptimizer:
    def __init__(self, config):
        self.config = config
        self.weights = config.MODP_WEIGHTS[:]
        self.candidates = [
            {'name': 'performance', 'quality': 0.9, 'carbon': 0.6, 'cost': 0.5, 'latency': 0.3},
            {'name': 'carbon', 'quality': 0.7, 'carbon': 0.2, 'cost': 0.3, 'latency': 0.4},
            {'name': 'cost', 'quality': 0.6, 'carbon': 0.4, 'cost': 0.1, 'latency': 0.5},
            {'name': 'balanced', 'quality': 0.8, 'carbon': 0.4, 'cost': 0.3, 'latency': 0.35},
        ]
        self.criteria = ['quality', 'carbon', 'cost', 'latency']

    async def select_strategy(self, state):
        candidates = []
        for cand in self.candidates:
            cand_dict = {
                'quality': cand['quality'],
                'carbon': 1.0 - cand['carbon'],
                'cost': 1.0 - cand['cost'],
                'latency': 1.0 - cand['latency'],
            }
            candidates.append(cand_dict)
        scores = await asyncio.to_thread(self._topsis, candidates, self.weights, self.criteria)
        best_idx = np.argmax(scores)
        best = self.candidates[best_idx]
        return {
            'strategy': best['name'],
            'scores': scores.tolist(),
            'recommendation': f"Selected {best['name']} based on MODP"
        }

    def _topsis(self, candidates, weights, criteria):
        matrix = np.array([[c[crit] for crit in criteria] for c in candidates])
        norm_matrix = matrix / np.sqrt((matrix**2).sum(axis=0))
        weighted = norm_matrix * weights
        ideal = weighted.max(axis=0)
        neg_ideal = weighted.min(axis=0)
        d_plus = np.sqrt(((weighted - ideal)**2).sum(axis=1))
        d_minus = np.sqrt(((weighted - neg_ideal)**2).sum(axis=1))
        return d_minus / (d_plus + d_minus + 1e-9)

# =============================================================================
# NEW MODULE: RLHF Manager
# =============================================================================
class RLHFManager:
    def __init__(self, config):
        self.config = config
        self.feedback_buffer = []
        self.reward_model = None
        self.policy = {'weights': np.array([0.25, 0.25, 0.25, 0.25])}
        self._lock = asyncio.Lock()
        if SKLEARN_AVAILABLE:
            self.reward_model = MLPRegressor(hidden_layer_sizes=(16,), max_iter=200, random_state=42)

    def _state_to_features(self, state):
        return [
            state.get('carbon_intensity', 0.4),
            state.get('quality_score', 0.5),
            state.get('cost', 0.5),
            state.get('latency', 0.5),
        ]

    def _action_to_index(self, action):
        actions = ['performance', 'carbon', 'cost', 'balanced']
        return actions.index(action) if action in actions else 0

    async def record_feedback(self, state, action, reward):
        async with self._lock:
            self.feedback_buffer.append({
                'state': self._state_to_features(state),
                'action': self._action_to_index(action),
                'reward': reward
            })

    async def train_reward_model(self):
        if not self.reward_model or len(self.feedback_buffer) < 10:
            return
        X = [f['state'] for f in self.feedback_buffer]
        y = [f['reward'] for f in self.feedback_buffer]
        self.reward_model.fit(X, y)
        logger.info(f"RLHF reward model trained on {len(self.feedback_buffer)} samples")
        self.feedback_buffer.clear()

    async def get_policy_probs(self, state):
        if self.reward_model:
            return self.policy['weights'].tolist()
        return self.policy['weights'].tolist()

# =============================================================================
# NEW MODULE: Multi‑Teacher Policy Distillation
# =============================================================================
class MultiTeacherPolicyDistillation:
    def __init__(self, config, moe_engine=None):
        self.config = config
        self.moe_engine = moe_engine
        self.student_policy = np.array([0.25, 0.25, 0.25, 0.25])
        self.temperature = config.DISTILLATION_TEMPERATURE
        self.alpha = config.DISTILLATION_ALPHA
        self.history = deque(maxlen=500)
        self._lock = asyncio.Lock()

    async def distill(self, state):
        if not self.moe_engine:
            return
        context = self._state_to_context(state)
        selected, params = await self.moe_engine.select_expert(context)
        expert_names = list(self.moe_engine.expert_names)
        probs = np.ones(len(expert_names)) / len(expert_names)
        if self.moe_engine._trained:
            features = self.moe_engine._encode_context(context)
            X = features.reshape(1, -1)
            if self.moe_engine._scaler:
                X = self.moe_engine._scaler.transform(X)
            probs = self.moe_engine._gating_model.predict_proba(X)[0]
        teacher_dist = np.array(probs)
        teacher_dist /= teacher_dist.sum()

        soft_teacher = np.exp(np.log(teacher_dist + 1e-8) / self.temperature)
        soft_teacher /= soft_teacher.sum()

        loss = -np.sum(soft_teacher * np.log(self.student_policy + 1e-8))
        grad = -soft_teacher / (self.student_policy + 1e-8)
        lr = 0.01
        self.student_policy -= lr * grad
        self.student_policy = np.clip(self.student_policy, 0.01, None)
        self.student_policy /= self.student_policy.sum()

        async with self._lock:
            self.history.append({
                'teacher_dist': teacher_dist,
                'student_dist': self.student_policy.copy(),
                'loss': loss
            })

    def _state_to_context(self, state):
        return {
            'carbon_intensity': state.get('carbon_intensity', 0.4),
            'quality_score': state.get('quality_score', 0.5),
            'cost': state.get('cost', 0.5),
            'latency': state.get('latency', 0.5),
        }

    def get_student_probs(self):
        return self.student_policy.tolist()

# -----------------------------------------------------------------------------
# MAIN ENHANCED TEST ENVIRONMENT V15.0.0
# -----------------------------------------------------------------------------
class EnhancedTestEnvironmentV15:
    def __init__(self):
        self.instance_id = str(uuid.uuid4())[:8]
        self.storage = Storage()
        self.state = TestState(self.storage)
        self.quantum_security = QuantumResilientTestSecurity(self.storage)
        self.blockchain = BlockchainTestVerification(self.storage)
        self.cloud_distributor = MultiCloudTestDistribution(self.storage)
        self.moe_gating = MoEGatingNetwork(config, self.storage) if config.MOE_ENABLED else None
        self.ga_optimizer = GeneticTestParameterOptimizer(config, self.storage, self) if config.GA_ENABLED else None
        self.pareto_optimizer = ParetoFrontOptimizer(config, self.storage) if config.PARETO_ENABLED else None
        self.federated_learner = FederatedTestLearner(self.storage, self.instance_id, config.FEDERATED_INTERVAL) if config.FEDERATED_ENABLED else None
        self.drift_detector = DriftDetector(self.storage, config) if config.DRIFT_DETECTION_ENABLED else None
        self.user_pref_learner = ActiveUserPreferenceLearner(self.storage, self.websocket) if config.ACTIVE_USER_PREFERENCE_ENABLED else None
        # ===== NEW =====
        self.limit_graph = LimitGraphManager(config) if config.LIMIT_GRAPH_ENABLED else None
        self.modp_optimizer = MODPStrategyOptimizer(config) if config.MODP_ENABLED else None
        self.rlhf = RLHFManager(config) if config.RLHF_ENABLED else None
        self.distillation = MultiTeacherPolicyDistillation(config, self.moe_gating) if config.DISTILLATION_ENABLED and self.moe_gating else None

        # Advanced components (unchanged)
        self.impact_analyzer = TestImpactAnalyzer()
        self.root_cause_analyzer = RootCauseAnalyzer()
        self.self_healing_manager = SelfHealingTestManager()
        self.predictive_maintenance_manager = PredictiveMaintenanceManager()
        self.analytics_dashboard = EnhancedAnalyticsDashboard(self.websocket)
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
        self.circuit_breakers = {'test': CircuitBreaker(name="test"), 'analysis': CircuitBreaker(name="analysis")}
        self.websocket = StubTestDashboardWebSocket(port=8779)
        self.analytics_dashboard.websocket = self.websocket
        self.test_registry: Dict[str, TestFeatureModel] = {}
        self._registry_lock = asyncio.Lock()
        self.test_results: Dict[str, TestResult] = {}
        self._results_lock = asyncio.Lock()
        self._test_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TESTS)
        self.operation_queue = asyncio.Queue(maxsize=100)
        self._queue_worker = None
        self._running = False
        self.background_tasks: Set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()
        self.sustainability_score = 0.0
        self.total_carbon_savings_kg = 0.0
        self.ml_ready = False
        logger.info("EnhancedTestEnvironmentV15 v%d.0.0 initialized (instance: %s)", DATA_VERSION, self.instance_id)

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
            asyncio.create_task(self._ga_optimization_loop()),
            asyncio.create_task(self._moe_training_loop()),
            asyncio.create_task(self._pareto_update_loop()),
            asyncio.create_task(self._drift_detection_loop()),
            asyncio.create_task(self._active_user_learning_loop()),
        ]
        # ===== NEW: background tasks =====
        if self.limit_graph:
            tasks.append(asyncio.create_task(self._limit_graph_loop()))
        if self.rlhf:
            tasks.append(asyncio.create_task(self._rlhf_loop()))
        if self.distillation:
            tasks.append(asyncio.create_task(self._distillation_loop()))

        for task in tasks:
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)
        logger.info("Test environment started with %d background tasks", len(self.background_tasks))

    # Background loops (existing)
    async def _ga_optimization_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            if self.ga_optimizer and config.GA_ENABLED:
                try:
                    best_params = await self.ga_optimizer.run_search()
                    if best_params:
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

    # ===== NEW: Background loop methods =====
    async def _limit_graph_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(config.LIMIT_GRAPH_UPDATE_INTERVAL)
            try:
                carbon = await self.carbon_manager.get_current_intensity()
                await self.limit_graph.update_constraint('carbon', carbon)
                influence = await self.limit_graph.evaluate_path('carbon', 'cost')
                logger.debug(f"LIMIT Graph carbon->cost influence: {influence:.3f}")
            except Exception as e:
                logger.error(f"Limit graph loop error: {e}")

    async def _rlhf_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(config.RLHF_TRAINING_INTERVAL)
            try:
                if self.rlhf:
                    await self.rlhf.train_reward_model()
            except Exception as e:
                logger.error(f"RLHF loop error: {e}")

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(config.DISTILLATION_INTERVAL)
            try:
                if self.distillation:
                    state = {'carbon_intensity': await self.carbon_manager.get_current_intensity(),
                             'quality_score': 0.7, 'cost': 0.3, 'latency': 0.4}
                    await self.distillation.distill(state)
            except Exception as e:
                logger.error(f"Distillation loop error: {e}")

    # Core test execution (modified)
    async def _execute_test(self, operation):
        # ... (long method)
        # In strategy selection:
        # Priority: MODP > RLHF > Distillation > MoE > fallback
        # After quality assessment:
        # Update LIMIT graph and record RLHF feedback
        pass

    # Helper methods
    async def _get_optimization_state(self, test_name, test_type):
        # ... (same as original)
        pass

    # Health check, statistics, shutdown (abbreviated)
    async def health_check(self):
        # ...
        pass

    async def get_statistics(self):
        # ...
        pass

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

# -----------------------------------------------------------------------------
# Singleton accessor
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# Main entry point (abbreviated)
# -----------------------------------------------------------------------------
async def main():
    # ... (same as original)
    pass

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(_signal_shutdown()))
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
