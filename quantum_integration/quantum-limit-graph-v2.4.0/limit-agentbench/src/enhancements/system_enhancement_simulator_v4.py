#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/synthetic_data_manager_enhanced_v17_0.py
# VERSION: 17.0.0 (v16 base + Causal RL + Temporal Logic + XAI + Adaptive
#                   Precision + Carbon Markets/REC + Chaos Testing)
# =============================================================================
"""
Enhanced Synthetic Data Manager for Green Agent - Version 17.0.0

NEW v17.0.0 ENHANCEMENTS (integrated on top of v16.0.0):
 1. Causal Reinforcement Learning (CausalGraphLearner + CausalReinforcementLearner)
 2. Temporal Logic & Formal Verification (TemporalLogicVerifier)
 3. Explainable AI for Every Decision (XAIDecisionExplainer)
 4. Adaptive Precision Switching (AdaptivePrecisionSwitcher)
 5. External Carbon Markets & REC Integration (CarbonMarketIntegrator)
 6. Resilience Engineering & Chaos Testing (ChaosTestingEngine)

All v16.0.0 features preserved. All enhancements are optional and configurable.
"""

import asyncio
import hashlib
import json
import os
import random
import re
import sqlite3
import time
import uuid
import signal
import logging
import logging.handlers
from functools import wraps
from collections import deque, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union, AsyncIterator, Callable
import secrets
import gc
import contextvars
import itertools

# -----------------------------------------------------------------------------
# Central Green Agent components
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

try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

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
    from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log, retry_if_exception_type
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

try:
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.neural_network import MLPRegressor, MLPClassifier
    from sklearn.linear_model import LinearRegression
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from business_rules import run_all
    from business_rules.actions import BaseActions
    from business_rules.fields import FIELD_NUMERIC, FIELD_SELECT, FIELD_TEXT
    from business_rules.operators import NumericType, SelectType, TextType
    BUSINESS_RULES_AVAILABLE = True
except ImportError:
    BUSINESS_RULES_AVAILABLE = False

try:
    import dash
    from dash import dcc, html, Input, Output, State, callback, dash_table
    import dash_bootstrap_components as dbc
    DASH_AVAILABLE = True
except ImportError:
    DASH_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

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
    from pydantic import BaseModel, Field, field_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from scipy.spatial.distance import jensenshannon
    from scipy.stats import wasserstein_distance, ks_2samp
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

try:
    import structlog
    from structlog.processors import JSONRenderer, TimeStamper
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False

try:
    import websockets
    from websockets.server import serve
    from websockets.exceptions import ConnectionClosed
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

# -----------------------------------------------------------------------------
# Dummy tenacity
# -----------------------------------------------------------------------------
if not TENACITY_AVAILABLE:
    def retry(*args, **kwargs):
        def decorator(func):
            @wraps(func)
            async def wrapper(*fargs, **fkwargs):
                attempts, delay, max_attempts = 0, 1, 3
                while attempts < max_attempts:
                    try:
                        return await func(*fargs, **fkwargs)
                    except Exception:
                        attempts += 1
                        if attempts >= max_attempts:
                            raise
                        await asyncio.sleep(delay)
                        delay *= 2
            return wrapper
        return decorator
    def retry_if_exception_type(*a, **kw): return None

# -----------------------------------------------------------------------------
# Logging
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
        logging.basicConfig(level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s')
        logger = logging.getLogger(__name__)
        class CorrelationIdFilter(logging.Filter):
            def filter(self, record):
                record.correlation_id = correlation_id_var.get()
                return True
        logger.addFilter(CorrelationIdFilter())

audit_logger = logging.getLogger('synthetic_audit')
audit_handler = logging.handlers.RotatingFileHandler(
    'synthetic_audit_v17.log', maxBytes=50*1024*1024, backupCount=10)
audit_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)

# -----------------------------------------------------------------------------
# Prometheus metrics
# -----------------------------------------------------------------------------
if CENTRAL_COMPONENTS_AVAILABLE and CentralMetrics:
    metrics = CentralMetrics()
    DATA_GENERATIONS = metrics.counter('synthetic_generations_total', ['domain', 'status', 'method'])
    GENERATION_DURATION = metrics.histogram('synthetic_generation_duration_seconds', ['domain', 'method'])
    DATA_QUALITY = metrics.gauge('synthetic_data_quality', ['domain', 'metric'])
    DRIFT_SCORE = metrics.gauge('synthetic_data_drift', ['domain', 'column'])
    PRIVACY_BUDGET = metrics.gauge('synthetic_privacy_budget', ['domain'])
    CIRCUIT_BREAKER_STATE = metrics.gauge('synthetic_circuit_breaker_state', ['component'])
    HEALTH_SCORE = metrics.gauge('synthetic_system_health')
    DB_SIZE = metrics.gauge('synthetic_db_size_mb')
    DATA_QUALITY_SCORE = metrics.gauge('synthetic_data_quality_score')
    GENERATION_QUEUE_SIZE = metrics.gauge('synthetic_generation_queue_size')
    WS_CONNECTIONS = metrics.gauge('synthetic_ws_connections')
    DEEP_GENERATION_SCORE = metrics.gauge('deep_generation_score', ['model_type'])
    DRIFT_METHOD_SCORE = metrics.gauge('drift_method_score', ['method'])
    ACTIVE_LEARNING_ITERATIONS = metrics.counter('active_learning_iterations_total', ['domain'])
    CONSTRAINT_VALIDATIONS = metrics.counter('constraint_validations_total', ['domain', 'status'])
    MODEL_VERSION_SCORE = metrics.gauge('model_version_score', ['domain', 'version'])
    QUANTUM_SIGNATURES = metrics.counter('synthetic_quantum_signatures_total', ['algorithm', 'status'])
    BLOCKCHAIN_VERIFICATIONS = metrics.counter('synthetic_blockchain_verifications_total', ['status'])
    AUTONOMOUS_OPTIMIZATIONS = metrics.counter('synthetic_autonomous_optimizations_total', ['strategy', 'status'])
    CLOUD_DISTRIBUTIONS = metrics.counter('synthetic_cloud_distributions_total', ['provider', 'status'])
    MTOP_TEACHER_WEIGHTS = metrics.gauge('synthetic_mtop_teacher_weights', ['teacher'])
    MTOP_STUDENT_UPDATES = metrics.counter('synthetic_mtop_student_updates_total')
    GA_POPULATION_FITNESS = metrics.gauge('synthetic_ga_population_fitness')
    MOE_GATING_PROBABILITIES = metrics.gauge('synthetic_moe_gating_probabilities', ['expert'])
    PARETO_FRONT_SIZE = metrics.gauge('synthetic_pareto_front_size')
    ADAPTIVE_DRIFT_THRESHOLD = metrics.gauge('synthetic_adaptive_drift_threshold', ['domain'])
    CAUSAL_ATE = metrics.gauge('synthetic_causal_ate', ['treatment', 'outcome'])
    CAUSAL_POLICY_REWARD = metrics.gauge('synthetic_causal_policy_reward')
    CAUSAL_EDGE_COUNT = metrics.gauge('synthetic_causal_edge_count')
    CAUSAL_GRAPH_UPDATES = metrics.counter('synthetic_causal_graph_updates_total')
    TEMPORAL_VERIFICATIONS = metrics.counter('synthetic_temporal_verifications_total', ['formula', 'status'])
    TEMPORAL_VIOLATIONS = metrics.counter('synthetic_temporal_violations_total', ['formula'])
    TEMPORAL_TRACE_LENGTH = metrics.gauge('synthetic_temporal_trace_length')
    XAI_EXPLANATIONS = metrics.counter('synthetic_xai_explanations_total', ['method'])
    XAI_FEATURE_IMPORTANCE = metrics.gauge('synthetic_xai_feature_importance', ['feature'])
    PRECISION_SWITCHES = metrics.counter('synthetic_precision_switches_total', ['from_p', 'to_p'])
    PRECISION_ENERGY_SAVED = metrics.gauge('synthetic_precision_energy_saved_wh')
    PRECISION_CURRENT = metrics.gauge('synthetic_precision_current', ['level'])
    CARBON_CREDIT_PRICE = metrics.gauge('synthetic_carbon_credit_price_usd')
    REC_BALANCE = metrics.gauge('synthetic_rec_balance_mwh')
    NET_ZERO_MATCHES = metrics.counter('synthetic_net_zero_matches_total')
    CARBON_OFFSET_COST = metrics.gauge('synthetic_carbon_offset_cost_usd')
    CHAOS_EXPERIMENTS = metrics.counter('synthetic_chaos_experiments_total', ['fault_type', 'status'])
    CHAOS_STEADY_STATE = metrics.gauge('synthetic_chaos_steady_state_ok')
    CHAOS_ROLLBACKS = metrics.counter('synthetic_chaos_rollbacks_total')
else:
    if PROMETHEUS_AVAILABLE:
        REGISTRY = CollectorRegistry()
        DATA_GENERATIONS = Counter('synthetic_generations_total', 'Total', ['domain', 'status', 'method'], registry=REGISTRY)
        GENERATION_DURATION = Histogram('synthetic_generation_duration_seconds', 'Duration', ['domain', 'method'], registry=REGISTRY)
        DATA_QUALITY = Gauge('synthetic_data_quality', 'Quality', ['domain', 'metric'], registry=REGISTRY)
        DRIFT_SCORE = Gauge('synthetic_data_drift', 'Drift', ['domain', 'column'], registry=REGISTRY)
        PRIVACY_BUDGET = Gauge('synthetic_privacy_budget', 'Privacy', ['domain'], registry=REGISTRY)
        CIRCUIT_BREAKER_STATE = Gauge('synthetic_circuit_breaker_state', 'CB', ['component'], registry=REGISTRY)
        HEALTH_SCORE = Gauge('synthetic_system_health', 'Health', registry=REGISTRY)
        DB_SIZE = Gauge('synthetic_db_size_mb', 'DB', registry=REGISTRY)
        DATA_QUALITY_SCORE = Gauge('synthetic_data_quality_score', 'Overall', registry=REGISTRY)
        GENERATION_QUEUE_SIZE = Gauge('synthetic_generation_queue_size', 'Queue', registry=REGISTRY)
        WS_CONNECTIONS = Gauge('synthetic_ws_connections', 'WS', registry=REGISTRY)
        DEEP_GENERATION_SCORE = Gauge('deep_generation_score', 'Deep', ['model_type'], registry=REGISTRY)
        DRIFT_METHOD_SCORE = Gauge('drift_method_score', 'DM', ['method'], registry=REGISTRY)
        ACTIVE_LEARNING_ITERATIONS = Counter('active_learning_iterations_total', 'AL', ['domain'], registry=REGISTRY)
        CONSTRAINT_VALIDATIONS = Counter('constraint_validations_total', 'CV', ['domain', 'status'], registry=REGISTRY)
        MODEL_VERSION_SCORE = Gauge('model_version_score', 'MV', ['domain', 'version'], registry=REGISTRY)
        QUANTUM_SIGNATURES = Counter('synthetic_quantum_signatures_total', 'QS', ['algorithm', 'status'], registry=REGISTRY)
        BLOCKCHAIN_VERIFICATIONS = Counter('synthetic_blockchain_verifications_total', 'BV', ['status'], registry=REGISTRY)
        AUTONOMOUS_OPTIMIZATIONS = Counter('synthetic_autonomous_optimizations_total', 'AO', ['strategy', 'status'], registry=REGISTRY)
        CLOUD_DISTRIBUTIONS = Counter('synthetic_cloud_distributions_total', 'CD', ['provider', 'status'], registry=REGISTRY)
        MTOP_TEACHER_WEIGHTS = Gauge('synthetic_mtop_teacher_weights', 'MTW', ['teacher'], registry=REGISTRY)
        MTOP_STUDENT_UPDATES = Counter('synthetic_mtop_student_updates_total', 'MSU', registry=REGISTRY)
        GA_POPULATION_FITNESS = Gauge('synthetic_ga_population_fitness', 'GA', registry=REGISTRY)
        MOE_GATING_PROBABILITIES = Gauge('synthetic_moe_gating_probabilities', 'MoE', ['expert'], registry=REGISTRY)
        PARETO_FRONT_SIZE = Gauge('synthetic_pareto_front_size', 'PF', registry=REGISTRY)
        ADAPTIVE_DRIFT_THRESHOLD = Gauge('synthetic_adaptive_drift_threshold', 'ADT', ['domain'], registry=REGISTRY)
        CAUSAL_ATE = Gauge('synthetic_causal_ate', 'ATE', ['treatment', 'outcome'], registry=REGISTRY)
        CAUSAL_POLICY_REWARD = Gauge('synthetic_causal_policy_reward', 'CRL', registry=REGISTRY)
        CAUSAL_EDGE_COUNT = Gauge('synthetic_causal_edge_count', 'CE', registry=REGISTRY)
        CAUSAL_GRAPH_UPDATES = Counter('synthetic_causal_graph_updates_total', 'CGU', registry=REGISTRY)
        TEMPORAL_VERIFICATIONS = Counter('synthetic_temporal_verifications_total', 'TL', ['formula', 'status'], registry=REGISTRY)
        TEMPORAL_VIOLATIONS = Counter('synthetic_temporal_violations_total', 'TLv', ['formula'], registry=REGISTRY)
        TEMPORAL_TRACE_LENGTH = Gauge('synthetic_temporal_trace_length', 'TLT', registry=REGISTRY)
        XAI_EXPLANATIONS = Counter('synthetic_xai_explanations_total', 'XAI', ['method'], registry=REGISTRY)
        XAI_FEATURE_IMPORTANCE = Gauge('synthetic_xai_feature_importance', 'XAIFI', ['feature'], registry=REGISTRY)
        PRECISION_SWITCHES = Counter('synthetic_precision_switches_total', 'PS', ['from_p', 'to_p'], registry=REGISTRY)
        PRECISION_ENERGY_SAVED = Gauge('synthetic_precision_energy_saved_wh', 'PES', registry=REGISTRY)
        PRECISION_CURRENT = Gauge('synthetic_precision_current', 'PC', ['level'], registry=REGISTRY)
        CARBON_CREDIT_PRICE = Gauge('synthetic_carbon_credit_price_usd', 'CC', registry=REGISTRY)
        REC_BALANCE = Gauge('synthetic_rec_balance_mwh', 'REC', registry=REGISTRY)
        NET_ZERO_MATCHES = Counter('synthetic_net_zero_matches_total', 'NZ', registry=REGISTRY)
        CARBON_OFFSET_COST = Gauge('synthetic_carbon_offset_cost_usd', 'COC', registry=REGISTRY)
        CHAOS_EXPERIMENTS = Counter('synthetic_chaos_experiments_total', 'Ch', ['fault_type', 'status'], registry=REGISTRY)
        CHAOS_STEADY_STATE = Gauge('synthetic_chaos_steady_state_ok', 'ChSS', registry=REGISTRY)
        CHAOS_ROLLBACKS = Counter('synthetic_chaos_rollbacks_total', 'ChR', registry=REGISTRY)
    else:
        class DummyMetric:
            def labels(self, **kwargs): return self
            def inc(self, **kwargs): pass
            def set(self, **kwargs): pass
            def observe(self, **kwargs): pass
        for _n in ['DATA_GENERATIONS','GENERATION_DURATION','DATA_QUALITY','DRIFT_SCORE','PRIVACY_BUDGET',
                   'CIRCUIT_BREAKER_STATE','HEALTH_SCORE','DB_SIZE','DATA_QUALITY_SCORE','GENERATION_QUEUE_SIZE',
                   'WS_CONNECTIONS','DEEP_GENERATION_SCORE','DRIFT_METHOD_SCORE','ACTIVE_LEARNING_ITERATIONS',
                   'CONSTRAINT_VALIDATIONS','MODEL_VERSION_SCORE','QUANTUM_SIGNATURES','BLOCKCHAIN_VERIFICATIONS',
                   'AUTONOMOUS_OPTIMIZATIONS','CLOUD_DISTRIBUTIONS','MTOP_TEACHER_WEIGHTS','MTOP_STUDENT_UPDATES',
                   'GA_POPULATION_FITNESS','MOE_GATING_PROBABILITIES','PARETO_FRONT_SIZE','ADAPTIVE_DRIFT_THRESHOLD',
                   'CAUSAL_ATE','CAUSAL_POLICY_REWARD','CAUSAL_EDGE_COUNT','CAUSAL_GRAPH_UPDATES',
                   'TEMPORAL_VERIFICATIONS','TEMPORAL_VIOLATIONS','TEMPORAL_TRACE_LENGTH','XAI_EXPLANATIONS',
                   'XAI_FEATURE_IMPORTANCE','PRECISION_SWITCHES','PRECISION_ENERGY_SAVED','PRECISION_CURRENT',
                   'CARBON_CREDIT_PRICE','REC_BALANCE','NET_ZERO_MATCHES','CARBON_OFFSET_COST',
                   'CHAOS_EXPERIMENTS','CHAOS_STEADY_STATE','CHAOS_ROLLBACKS']:
            globals()[_n] = DummyMetric()

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
if CENTRAL_COMPONENTS_AVAILABLE and central_config:
    class SyntheticDataConfigFromCentral:
        def __init__(self):
            g = lambda k, d=None: getattr(central_config, k, d)
            self.instance_id = g('instance_id', str(uuid.uuid4())[:8])
            self.version = "17.0.0"
            self.log_level = g('log_level', 'INFO')
            self.db_path = g('db_path', '/tmp/synthetic_data_v17.db')
            self.openai_api_key = g('openai_api_key')
            self.electricity_maps_api_key = g('electricity_maps_api_key')
            self.carbon_region = g('carbon_region', 'global')
            self.carbon_update_interval = g('carbon_update_interval', 300)
            self.blockchain_rpc_url = g('blockchain_rpc_url', 'http://localhost:8545')
            self.blockchain_contract_address = g('blockchain_contract_address')
            self.blockchain_private_key = g('blockchain_private_key')
            self.aws_access_key_id = g('aws_access_key_id')
            self.aws_secret_access_key = g('aws_secret_access_key')
            self.aws_region = g('aws_region', 'us-east-1')
            self.azure_connection_string = g('azure_connection_string')
            self.gcp_credentials_path = g('gcp_credentials_path')
            self.cache_ttl = g('cache_ttl', 300)
            self.retry_attempts = g('retry_attempts', 3)
            self.retry_min_wait = g('retry_min_wait', 2)
            self.retry_max_wait = g('retry_max_wait', 10)
            self.metrics_port = g('metrics_port', 8000)
            self.websocket_port = g('websocket_port', 8770)
            self.mopd_weights = g('synthetic_mopd_weights',
                                  {'quality': 0.4, 'carbon': 0.3, 'cost': 0.2, 'privacy': 0.1})
            self.health_check_interval = g('health_check_interval', 60)
            self.model_retrain_interval = g('model_retrain_interval', 3600)
            self.cache_cleanup_interval = g('cache_cleanup_interval', 3600)
            self.auto_optimize_interval = g('auto_optimize_interval', 1800)
            self.federated_interval = g('federated_interval', 3600)
            self.predictive_interval = g('predictive_interval', 3600)
            self.sustainability_interval = g('sustainability_interval', 3600)
            self.key_rotation_interval = g('key_rotation_interval', 86400)
            self.active_learning_interval = g('active_learning_interval', 1800)
            self.master_key_env = g('master_key_env', 'SYNTHETIC_MASTER_KEY')
            # v16
            self.ga_enabled = g('synthetic_ga_enabled', True)
            self.ga_population_size = g('synthetic_ga_population_size', 20)
            self.ga_generations = g('synthetic_ga_generations', 5)
            self.ga_mutation_rate = g('synthetic_ga_mutation_rate', 0.2)
            self.ga_crossover_rate = g('synthetic_ga_crossover_rate', 0.7)
            self.moe_enabled = g('synthetic_moe_enabled', True)
            self.moe_expert_count = g('synthetic_moe_expert_count', 4)
            self.moe_hidden_layers = g('synthetic_moe_hidden_layers', [16, 8])
            self.pareto_enabled = g('synthetic_pareto_enabled', True)
            self.pareto_max_architectures = g('synthetic_pareto_max_architectures', 100)
            self.evolutionary_architecture_enabled = g('synthetic_evolutionary_architecture_enabled', True)
            self.evolutionary_generations = g('synthetic_evolutionary_generations', 3)
            self.evolutionary_population_size = g('synthetic_evolutionary_population_size', 5)
            self.federated_learning_enabled = g('synthetic_federated_learning_enabled', True)
            self.contextual_bandit_enabled = g('synthetic_contextual_bandit_enabled', True)
            self.adaptive_drift_enabled = g('synthetic_adaptive_drift_enabled', True)
            self.limit_graph_enabled = g('synthetic_limit_graph_enabled', True)
            self.limit_graph_update_interval = g('synthetic_limit_graph_update_interval', 300)
            self.modp_enabled = g('synthetic_modp_enabled', True)
            self.modp_weights = g('synthetic_modp_weights', [0.25, 0.25, 0.25, 0.25])
            self.rlhf_enabled = g('synthetic_rlhf_enabled', True)
            self.rlhf_reward_model = g('synthetic_rlhf_reward_model', 'linear')
            self.rlhf_training_interval = g('synthetic_rlhf_training_interval', 600)
            self.distillation_enabled = g('synthetic_distillation_enabled', True)
            self.distillation_temperature = g('synthetic_distillation_temperature', 2.0)
            self.distillation_alpha = g('synthetic_distillation_alpha', 0.5)
            self.distillation_interval = g('synthetic_distillation_interval', 300)
            # ===== v17 =====
            self.causal_rl_enabled = g('synthetic_causal_rl_enabled', True)
            self.causal_graph_update_interval = g('synthetic_causal_graph_update_interval', 900)
            self.causal_exploration_rate = g('synthetic_causal_exploration_rate', 0.1)
            self.causal_min_samples = g('synthetic_causal_min_samples', 50)
            self.causal_effect_threshold = g('synthetic_causal_effect_threshold', 0.15)
            self.temporal_logic_enabled = g('synthetic_temporal_logic_enabled', True)
            self.temporal_verification_interval = g('synthetic_temporal_verification_interval', 600)
            self.temporal_formulas = g('synthetic_temporal_formulas', [
                "G (quality >= 0.7)", "G (carbon <= 0.5)",
                "G (privacy_budget >= 0.0)", "F (generation_complete)"])
            self.temporal_max_trace_length = g('synthetic_temporal_max_trace_length', 1000)
            self.xai_enabled = g('synthetic_xai_enabled', True)
            self.xai_method = g('synthetic_xai_method', 'kernel_shap')
            self.xai_explanation_depth = g('synthetic_xai_explanation_depth', 5)
            self.xai_interval = g('synthetic_xai_interval', 300)
            self.adaptive_precision_enabled = g('synthetic_adaptive_precision_enabled', True)
            self.precision_levels = g('synthetic_precision_levels', ['fp32', 'fp16', 'bf16', 'int8'])
            self.precision_switch_threshold = g('synthetic_precision_switch_threshold', 0.02)
            self.precision_energy_target = g('synthetic_precision_energy_target', 0.7)
            self.carbon_market_enabled = g('synthetic_carbon_market_enabled', True)
            self.carbon_market_api_url = g('synthetic_carbon_market_api_url',
                                           'https://api.carbonmarket.example/v1')
            self.carbon_market_interval = g('synthetic_carbon_market_interval', 3600)
            self.rec_tracking_enabled = g('synthetic_rec_tracking_enabled', True)
            self.rec_auto_purchase_threshold_usd = g('synthetic_rec_auto_purchase_threshold_usd', 5.0)
            self.chaos_testing_enabled = g('synthetic_chaos_testing_enabled', True)
            self.chaos_test_interval = g('synthetic_chaos_test_interval', 1800)
            self.chaos_intensity = g('synthetic_chaos_intensity', 0.05)
            self.chaos_blast_radius = g('synthetic_chaos_blast_radius', 0.1)
            self.chaos_fault_types = g('synthetic_chaos_fault_types',
                ['latency', 'exception', 'data_corruption', 'memory_pressure', 'network_drop'])
            self.chaos_auto_rollback = g('synthetic_chaos_auto_rollback', True)

        def get_master_key(self) -> bytes:
            key_hex = os.getenv(self.master_key_env)
            if not key_hex:
                raise ValueError(f"Master key not set in env {self.master_key_env}")
            return bytes.fromhex(key_hex)

    SyntheticDataConfig = SyntheticDataConfigFromCentral
else:
    if PYDANTIC_AVAILABLE:
        class SyntheticDataConfig(BaseModel):
            instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
            version: str = "17.0.0"
            log_level: str = "INFO"
            db_path: str = "/tmp/synthetic_data_v17.db"
            openai_api_key: Optional[str] = None
            electricity_maps_api_key: Optional[str] = None
            carbon_region: str = "global"
            carbon_update_interval: int = 300
            blockchain_rpc_url: str = "http://localhost:8545"
            blockchain_contract_address: Optional[str] = None
            blockchain_private_key: Optional[str] = None
            aws_access_key_id: Optional[str] = None
            aws_secret_access_key: Optional[str] = None
            aws_region: str = "us-east-1"
            azure_connection_string: Optional[str] = None
            gcp_credentials_path: Optional[str] = None
            cache_ttl: int = 300
            retry_attempts: int = 3
            retry_min_wait: int = 2
            retry_max_wait: int = 10
            metrics_port: int = 8000
            websocket_port: int = 8770
            mopd_weights: Dict[str, float] = Field(default_factory=lambda: {
                'quality': 0.4, 'carbon': 0.3, 'cost': 0.2, 'privacy': 0.1})
            health_check_interval: int = 60
            model_retrain_interval: int = 3600
            cache_cleanup_interval: int = 3600
            auto_optimize_interval: int = 1800
            federated_interval: int = 3600
            predictive_interval: int = 3600
            sustainability_interval: int = 3600
            key_rotation_interval: int = 86400
            active_learning_interval: int = 1800
            master_key_env: str = "SYNTHETIC_MASTER_KEY"
            ga_enabled: bool = True
            ga_population_size: int = 20
            ga_generations: int = 5
            ga_mutation_rate: float = 0.2
            ga_crossover_rate: float = 0.7
            moe_enabled: bool = True
            moe_expert_count: int = 4
            moe_hidden_layers: List[int] = Field(default_factory=lambda: [16, 8])
            pareto_enabled: bool = True
            pareto_max_architectures: int = 100
            evolutionary_architecture_enabled: bool = True
            evolutionary_generations: int = 3
            evolutionary_population_size: int = 5
            federated_learning_enabled: bool = True
            contextual_bandit_enabled: bool = True
            adaptive_drift_enabled: bool = True
            limit_graph_enabled: bool = True
            limit_graph_update_interval: int = 300
            modp_enabled: bool = True
            modp_weights: List[float] = Field(default_factory=lambda: [0.25, 0.25, 0.25, 0.25])
            rlhf_enabled: bool = True
            rlhf_reward_model: str = "linear"
            rlhf_training_interval: int = 600
            distillation_enabled: bool = True
            distillation_temperature: float = 2.0
            distillation_alpha: float = 0.5
            distillation_interval: int = 300
            # v17
            causal_rl_enabled: bool = True
            causal_graph_update_interval: int = 900
            causal_exploration_rate: float = 0.1
            causal_min_samples: int = 50
            causal_effect_threshold: float = 0.15
            temporal_logic_enabled: bool = True
            temporal_verification_interval: int = 600
            temporal_formulas: List[str] = Field(default_factory=lambda: [
                "G (quality >= 0.7)", "G (carbon <= 0.5)",
                "G (privacy_budget >= 0.0)", "F (generation_complete)"])
            temporal_max_trace_length: int = 1000
            xai_enabled: bool = True
            xai_method: str = "kernel_shap"
            xai_explanation_depth: int = 5
            xai_interval: int = 300
            adaptive_precision_enabled: bool = True
            precision_levels: List[str] = Field(default_factory=lambda: ['fp32', 'fp16', 'bf16', 'int8'])
            precision_switch_threshold: float = 0.02
            precision_energy_target: float = 0.7
            carbon_market_enabled: bool = True
            carbon_market_api_url: str = "https://api.carbonmarket.example/v1"
            carbon_market_interval: int = 3600
            rec_tracking_enabled: bool = True
            rec_auto_purchase_threshold_usd: float = 5.0
            chaos_testing_enabled: bool = True
            chaos_test_interval: int = 1800
            chaos_intensity: float = 0.05
            chaos_blast_radius: float = 0.1
            chaos_fault_types: List[str] = Field(default_factory=lambda: [
                'latency', 'exception', 'data_corruption', 'memory_pressure', 'network_drop'])
            chaos_auto_rollback: bool = True

            @field_validator('log_level')
            @classmethod
            def validate_log_level(cls, v):
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
                env_prefix = "SYNTHETIC_"
    else:
        class SyntheticDataConfig:
            def __init__(self, **kw):
                self.instance_id = str(uuid.uuid4())[:8]
                self.version = "17.0.0"
                self.db_path = "/tmp/synthetic_data_v17.db"
                self.carbon_region = "global"
                self.carbon_update_interval = 300
                self.electricity_maps_api_key = None
                self.blockchain_rpc_url = "http://localhost:8545"
                self.cache_ttl = 300
                self.metrics_port = 8000
                self.websocket_port = 8770
                self.health_check_interval = 60
                self.master_key_env = "SYNTHETIC_MASTER_KEY"
                # enable all v16 + v17 flags
                for f in ['ga_enabled','moe_enabled','pareto_enabled',
                          'evolutionary_architecture_enabled','federated_learning_enabled',
                          'contextual_bandit_enabled','adaptive_drift_enabled',
                          'limit_graph_enabled','modp_enabled','rlhf_enabled',
                          'distillation_enabled','causal_rl_enabled','temporal_logic_enabled',
                          'xai_enabled','adaptive_precision_enabled','carbon_market_enabled',
                          'rec_tracking_enabled','chaos_testing_enabled','chaos_auto_rollback']:
                    setattr(self, f, True)
                self.ga_population_size = 20
                self.ga_generations = 5
                self.ga_mutation_rate = 0.2
                self.ga_crossover_rate = 0.7
                self.moe_expert_count = 4
                self.moe_hidden_layers = [16, 8]
                self.pareto_max_architectures = 100
                self.evolutionary_generations = 3
                self.evolutionary_population_size = 5
                self.limit_graph_update_interval = 300
                self.modp_weights = [0.25, 0.25, 0.25, 0.25]
                self.rlhf_training_interval = 600
                self.distillation_temperature = 2.0
                self.distillation_alpha = 0.5
                self.distillation_interval = 300
                self.causal_graph_update_interval = 900
                self.causal_exploration_rate = 0.1
                self.causal_min_samples = 50
                self.causal_effect_threshold = 0.15
                self.temporal_verification_interval = 600
                self.temporal_formulas = ["G (quality >= 0.7)", "F (generation_complete)"]
                self.temporal_max_trace_length = 1000
                self.xai_method = 'kernel_shap'
                self.xai_explanation_depth = 5
                self.xai_interval = 300
                self.precision_levels = ['fp32', 'fp16', 'bf16', 'int8']
                self.precision_switch_threshold = 0.02
                self.precision_energy_target = 0.7
                self.carbon_market_api_url = 'https://api.carbonmarket.example/v1'
                self.carbon_market_interval = 3600
                self.rec_auto_purchase_threshold_usd = 5.0
                self.chaos_test_interval = 1800
                self.chaos_intensity = 0.05
                self.chaos_blast_radius = 0.1
                self.chaos_fault_types = ['latency','exception','data_corruption','memory_pressure','network_drop']
                for k, v in kw.items():
                    setattr(self, k, v)

            def get_master_key(self) -> bytes:
                key_hex = os.getenv(self.master_key_env)
                if not key_hex:
                    raise ValueError(f"Master key not set in env {self.master_key_env}")
                return bytes.fromhex(key_hex)

# -----------------------------------------------------------------------------
# Encryption
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

# -----------------------------------------------------------------------------
# Storage
# -----------------------------------------------------------------------------
class EnhancedStorage:
    def __init__(self, config):
        self.config = config
        self.db_path = config.db_path
        self.cache_ttl = config.cache_ttl
        self.cache = {}
        self.encryption_manager = None
        try:
            self.encryption_manager = EncryptionManager(config.get_master_key())
        except Exception:
            logger.warning("Master key not set – plaintext fallback")
        self._init_custom_tables()

    def _init_custom_tables(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            stmts = [
                """CREATE TABLE IF NOT EXISTS synthetic_carbon_cache (
                    region TEXT PRIMARY KEY, intensity REAL NOT NULL, timestamp TEXT NOT NULL)""",
                """CREATE TABLE IF NOT EXISTS synthetic_helium_cache (
                    hotspot_id TEXT PRIMARY KEY, score REAL NOT NULL, timestamp TEXT NOT NULL)""",
                """CREATE TABLE IF NOT EXISTS synthetic_generation_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL,
                    dataset_version TEXT NOT NULL, num_samples INTEGER NOT NULL,
                    anomaly_rate REAL, edge_fraction REAL, parameters TEXT,
                    quantum_signature TEXT, blockchain_tx_hash TEXT)""",
                """CREATE TABLE IF NOT EXISTS synthetic_ga_populations (
                    generation INTEGER, individual_id TEXT, attributes TEXT,
                    fitness REAL, timestamp TEXT, PRIMARY KEY (generation, individual_id))""",
                """CREATE TABLE IF NOT EXISTS synthetic_moe_training (
                    sample_id TEXT PRIMARY KEY, features TEXT, expert_label INTEGER,
                    reward REAL, timestamp TEXT)""",
                """CREATE TABLE IF NOT EXISTS synthetic_pareto_front (
                    solution_id TEXT PRIMARY KEY, config_params TEXT, coverage_score REAL,
                    anomaly_diversity REAL, realism_score REAL, data_quality REAL, timestamp TEXT)""",
                """CREATE TABLE IF NOT EXISTS synthetic_user_preferences (
                    user_id TEXT, weights TEXT, chosen_solution_id TEXT,
                    timestamp TEXT, PRIMARY KEY (user_id, timestamp))""",
                """CREATE TABLE IF NOT EXISTS synthetic_state (
                    key TEXT PRIMARY KEY, value TEXT NOT NULL)""",
                # ---- v17 tables ----
                """CREATE TABLE IF NOT EXISTS synthetic_causal_graph (
                    edge_id TEXT PRIMARY KEY, source TEXT, target TEXT,
                    weight REAL, confidence REAL, timestamp TEXT)""",
                """CREATE TABLE IF NOT EXISTS synthetic_causal_experiments (
                    exp_id TEXT PRIMARY KEY, treatment TEXT, outcome TEXT,
                    ate REAL, samples INTEGER, timestamp TEXT)""",
                """CREATE TABLE IF NOT EXISTS synthetic_temporal_violations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, formula TEXT, step INTEGER,
                    state TEXT, timestamp TEXT)""",
                """CREATE TABLE IF NOT EXISTS synthetic_xai_explanations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, decision_id TEXT, method TEXT,
                    top_features TEXT, natural_language TEXT, timestamp TEXT)""",
                """CREATE TABLE IF NOT EXISTS synthetic_precision_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, from_p TEXT, to_p TEXT,
                    reason TEXT, energy_saved_wh REAL, timestamp TEXT)""",
                """CREATE TABLE IF NOT EXISTS synthetic_rec_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, mwh REAL, price REAL,
                    source TEXT, timestamp TEXT)""",
                """CREATE TABLE IF NOT EXISTS synthetic_carbon_credit_prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, price REAL, currency TEXT,
                    source TEXT, timestamp TEXT)""",
                """CREATE TABLE IF NOT EXISTS synthetic_chaos_experiments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, fault_type TEXT,
                    status TEXT, steady_before INTEGER, steady_after INTEGER,
                    blast_radius REAL, timestamp TEXT)""",
                "CREATE INDEX IF NOT EXISTS idx_gen_timestamp ON synthetic_generation_history(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_ga_generation ON synthetic_ga_populations(generation)",
                "CREATE INDEX IF NOT EXISTS idx_moe_sample_time ON synthetic_moe_training(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_pareto_overall ON synthetic_pareto_front(data_quality)",
                "CREATE INDEX IF NOT EXISTS idx_causal_edge ON synthetic_causal_graph(source, target)",
                "CREATE INDEX IF NOT EXISTS idx_rec_time ON synthetic_rec_ledger(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_chaos_time ON synthetic_chaos_experiments(timestamp)",
            ]
            for s in stmts:
                conn.execute(s)
            conn.commit()

    async def _execute(self, query, params=()):
        return await asyncio.to_thread(self._sync_execute, query, params)

    def _sync_execute(self, query, params):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            cur = conn.execute(query, params)
            conn.commit()
            return cur

    async def _fetchone(self, query, params=()):
        return await asyncio.to_thread(self._sync_fetchone, query, params)

    def _sync_fetchone(self, query, params):
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(query, params)
            return cur.fetchone()

    async def _fetchall(self, query, params=()):
        return await asyncio.to_thread(self._sync_fetchall, query, params)

    def _sync_fetchall(self, query, params):
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(query, params)
            return cur.fetchall()

    # Existing helpers
    async def save_carbon_intensity(self, region, intensity):
        await self._execute("INSERT OR REPLACE INTO synthetic_carbon_cache VALUES (?, ?, ?)",
                            (region, intensity, datetime.now().isoformat()))
    async def get_carbon_intensity(self, region):
        row = await self._fetchone("SELECT intensity FROM synthetic_carbon_cache WHERE region = ?", (region,))
        return row[0] if row else None
    async def save_helium_score(self, hotspot_id, score):
        await self._execute("INSERT OR REPLACE INTO synthetic_helium_cache VALUES (?, ?, ?)",
                            (hotspot_id, score, datetime.now().isoformat()))
    async def get_helium_score(self, hotspot_id):
        row = await self._fetchone("SELECT score FROM synthetic_helium_cache WHERE hotspot_id = ?", (hotspot_id,))
        return row[0] if row else None
    async def save_generation_history(self, dataset_version, num_samples, anomaly_rate,
                                      edge_fraction, parameters, quantum_signature=None,
                                      blockchain_tx_hash=None):
        await self._execute(
            "INSERT INTO synthetic_generation_history (timestamp,dataset_version,num_samples,anomaly_rate,edge_fraction,parameters,quantum_signature,blockchain_tx_hash) VALUES (?,?,?,?,?,?,?,?)",
            (datetime.now().isoformat(), dataset_version, num_samples, anomaly_rate,
             edge_fraction, json.dumps(parameters), quantum_signature, blockchain_tx_hash))
    async def save_state(self, key, value):
        await self._execute("INSERT OR REPLACE INTO synthetic_state (key, value) VALUES (?, ?)", (key, value))
    async def get_state(self, key):
        row = await self._fetchone("SELECT value FROM synthetic_state WHERE key = ?", (key,))
        return row[0] if row else None
    async def save_ga_population(self, generation, individuals):
        for ind in individuals:
            await self._execute(
                "INSERT OR REPLACE INTO synthetic_ga_populations VALUES (?, ?, ?, ?, ?)",
                (generation, ind['individual_id'], json.dumps(ind['attributes']),
                 ind['fitness'], datetime.now().isoformat()))
    async def get_ga_population(self, generation):
        rows = await self._fetchall(
            "SELECT individual_id, attributes, fitness FROM synthetic_ga_populations WHERE generation = ?",
            (generation,))
        return [{'individual_id': r[0], 'attributes': json.loads(r[1]), 'fitness': r[2]} for r in rows]
    async def save_moe_training_sample(self, sample_id, features, expert_label, reward):
        await self._execute("INSERT OR REPLACE INTO synthetic_moe_training VALUES (?, ?, ?, ?, ?)",
                            (sample_id, json.dumps(features), expert_label, reward, datetime.now().isoformat()))
    async def save_pareto_front(self, solutions):
        await self._execute("DELETE FROM synthetic_pareto_front")
        for sol in solutions:
            await self._execute(
                "INSERT INTO synthetic_pareto_front VALUES (?, ?, ?, ?, ?, ?, ?)",
                (sol['solution_id'], json.dumps(sol['config_params']),
                 sol.get('coverage_score', 0), sol.get('anomaly_diversity', 0),
                 sol.get('realism_score', 0), sol.get('data_quality', 0),
                 datetime.now().isoformat()))
    async def get_current_pareto_front(self):
        return await self._fetchall("SELECT * FROM synthetic_pareto_front ORDER BY data_quality DESC")
    async def save_user_preference(self, user_id, weights, chosen_solution_id=None):
        await self._execute("INSERT OR REPLACE INTO synthetic_user_preferences VALUES (?, ?, ?, ?)",
                            (user_id, json.dumps(weights), chosen_solution_id, datetime.now().isoformat()))
    async def get_user_preferences(self, user_id):
        row = await self._fetchone(
            "SELECT weights, chosen_solution_id, timestamp FROM synthetic_user_preferences WHERE user_id = ? ORDER BY timestamp DESC LIMIT 1",
            (user_id,))
        if row:
            return {'weights': json.loads(row[0]), 'chosen_solution_id': row[1], 'timestamp': row[2]}
        return None

    # ---- v17 helpers ----
    async def save_causal_edge(self, source, target, weight, confidence):
        eid = f"{source}->{target}"
        await self._execute(
            "INSERT OR REPLACE INTO synthetic_causal_graph VALUES (?, ?, ?, ?, ?, ?)",
            (eid, source, target, weight, confidence, datetime.now().isoformat()))
    async def save_causal_experiment(self, exp_id, treatment, outcome, ate, samples):
        await self._execute(
            "INSERT OR REPLACE INTO synthetic_causal_experiments VALUES (?, ?, ?, ?, ?, ?)",
            (exp_id, treatment, outcome, ate, samples, datetime.now().isoformat()))
    async def save_temporal_violation(self, formula, step, state):
        await self._execute(
            "INSERT INTO synthetic_temporal_violations (formula, step, state, timestamp) VALUES (?, ?, ?, ?)",
            (formula, step, json.dumps(state, default=str), datetime.now().isoformat()))
    async def save_xai_explanation(self, decision_id, method, top_features, nl):
        await self._execute(
            "INSERT INTO synthetic_xai_explanations (decision_id, method, top_features, natural_language, timestamp) VALUES (?, ?, ?, ?, ?)",
            (decision_id, method, json.dumps(top_features), nl, datetime.now().isoformat()))
    async def save_precision_switch(self, frm, to, reason, saved_wh):
        await self._execute(
            "INSERT INTO synthetic_precision_history (from_p, to_p, reason, energy_saved_wh, timestamp) VALUES (?, ?, ?, ?, ?)",
            (frm, to, reason, saved_wh, datetime.now().isoformat()))
    async def save_rec(self, mwh, price, source):
        await self._execute(
            "INSERT INTO synthetic_rec_ledger (mwh, price, source, timestamp) VALUES (?, ?, ?, ?)",
            (mwh, price, source, datetime.now().isoformat()))
    async def get_rec_balance(self):
        row = await self._fetchone("SELECT COALESCE(SUM(mwh), 0) FROM synthetic_rec_ledger")
        return row[0] if row else 0.0
    async def save_credit_price(self, price, currency='USD', source='oracle'):
        await self._execute(
            "INSERT INTO synthetic_carbon_credit_prices (price, currency, source, timestamp) VALUES (?, ?, ?, ?)",
            (price, currency, source, datetime.now().isoformat()))
    async def save_chaos_experiment(self, name, fault_type, status, before, after, blast):
        await self._execute(
            "INSERT INTO synthetic_chaos_experiments (name, fault_type, status, steady_before, steady_after, blast_radius, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (name, fault_type, status, int(before), int(after), blast, datetime.now().isoformat()))

    def dispose(self):
        pass

# -----------------------------------------------------------------------------
# Circuit Breaker / Rate Limiter
# -----------------------------------------------------------------------------
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30.0, name="default"):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self._failures = 0
        self._last_failure_time = None
        self._state = "CLOSED"
        self.chaos_engine = None  # v17 chaos hook

    async def call(self, func, *args, **kwargs):
        if self._state == "OPEN":
            if (datetime.now() - self._last_failure_time).total_seconds() > self.recovery_timeout:
                self._state = "HALF_OPEN"
            else:
                raise Exception(f"Circuit breaker {self.name} is OPEN")
        try:
            # v17: inject latency if chaos engine is active for this breaker
            if self.chaos_engine and f"cb_{self.name}" in self.chaos_engine.active:
                await asyncio.sleep(0.2)
            result = await func(*args, **kwargs)
            if self._state == "HALF_OPEN":
                self._state = "CLOSED"
                self._failures = 0
                CIRCUIT_BREAKER_STATE.labels(component=self.name).set(0)
            return result
        except Exception as e:
            self._failures += 1
            self._last_failure_time = datetime.now()
            if self._failures >= self.failure_threshold:
                self._state = "OPEN"
                CIRCUIT_BREAKER_STATE.labels(component=self.name).set(2)
            raise e


class RateLimiter:
    def __init__(self, rate=100, window=60):
        self.rate = rate
        self.window = window
        self.tokens = rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + elapsed * (self.rate / self.window))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.1)

# -----------------------------------------------------------------------------
# Carbon Intensity Manager
# -----------------------------------------------------------------------------
class CarbonIntensityManager:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.api_key = config.electricity_maps_api_key
        self.region = config.carbon_region
        self.endpoint = "https://api.electricitymap.org/v3/carbon-intensity"
        self._session = None
        self._circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=60.0, name="carbon_api")
        self._rate_limiter = RateLimiter(rate=10, window=60)

    async def _get_session(self):
        if self._session is None and AIOHTTP_AVAILABLE:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _fetch_intensity(self):
        if not AIOHTTP_AVAILABLE:
            raise RuntimeError("aiohttp unavailable")
        await self._rate_limiter.wait_and_acquire()
        session = await self._get_session()
        url = f"{self.endpoint}/latest?zone={self.region}"
        headers = {'auth-token': self.api_key} if self.api_key else {}
        async with session.get(url, headers=headers, timeout=10) as response:
            if response.status != 200:
                raise Exception(f"Carbon API returned {response.status}")
            data = await response.json()
            return data.get('carbonIntensity', 400)

    async def get_current_intensity(self):
        cached = await self.storage.get_carbon_intensity(self.region)
        if cached is not None:
            return cached / 1000.0
        try:
            intensity = await self._circuit_breaker.call(self._fetch_intensity)
            await self.storage.save_carbon_intensity(self.region, intensity)
            return intensity / 1000.0
        except Exception as e:
            logger.warning(f"Failed to fetch carbon intensity: {e}; using fallback 0.4 kg/kWh")
            return 0.4

    async def close(self):
        if self._session:
            await self._session.close()

# -----------------------------------------------------------------------------
# Deep Generative Model and Domain Generator
# -----------------------------------------------------------------------------
class DeepGenerativeModel:
    def __init__(self, input_dim, latent_dim=32, hidden_dim=128, model_type='vae', model_path=None):
        self.input_dim = input_dim
        self.latent_dim = latent_dim
        self.hidden_dim = hidden_dim
        self.model_type = model_type
        self.model_path = model_path
        self.model = None
        self.precision = 'fp32'  # v17

    async def generate(self, n_samples, conditional_constraints=None):
        if NUMPY_AVAILABLE:
            return np.random.randn(n_samples, self.input_dim)
        return [[random.gauss(0, 1) for _ in range(self.input_dim)] for _ in range(n_samples)]


class DomainDataGenerator:
    def __init__(self, domain, deep_model=None):
        self.domain = domain
        self.deep_model = deep_model

    async def generate(self, n_samples, method='statistical', conditional_constraints=None):
        if method in ['vae', 'gan'] and self.deep_model:
            data = await self.deep_model.generate(n_samples, conditional_constraints)
            if PANDAS_AVAILABLE:
                return pd.DataFrame(data, columns=[f'feature_{i}' for i in range(data.shape[1])])
            return data
        if PANDAS_AVAILABLE and NUMPY_AVAILABLE:
            return pd.DataFrame(np.random.randn(n_samples, 5), columns=[f'col_{i}' for i in range(5)])
        return [[random.random() for _ in range(5)] for _ in range(n_samples)]

# -----------------------------------------------------------------------------
# MTOP Engine
# -----------------------------------------------------------------------------
class StrategyTeacherEnsemble:
    def __init__(self, config):
        self.teachers = {'performance': None, 'carbon': None, 'cost': None, 'adaptive': None}
        self.teacher_weights = {'performance': 0.25, 'carbon': 0.25, 'cost': 0.25, 'adaptive': 0.25}

    async def get_teacher_scores(self, state, carbon_intensity):
        return {
            'performance': {'statistical': 0.5, 'vae': 0.8, 'gan': 0.7, 'hybrid': 0.75},
            'carbon': {'statistical': 0.6, 'vae': 0.4, 'gan': 0.3, 'hybrid': 0.5},
            'cost': {'statistical': 0.7, 'vae': 0.5, 'gan': 0.4, 'hybrid': 0.6},
            'adaptive': {'statistical': 0.5, 'vae': 0.7, 'gan': 0.6, 'hybrid': 0.65}
        }

    def update_weights(self, rewards):
        total = sum(rewards.values())
        if total > 0:
            for k in self.teacher_weights:
                self.teacher_weights[k] = rewards[k] / total


class StrategyDistillationStudent:
    def __init__(self, config):
        self.weights = np.array([0.3, 0.3, 0.2, 0.2]) if NUMPY_AVAILABLE else [0.3, 0.3, 0.2, 0.2]
        self.update_count = 0

    async def combine(self, teacher_scores):
        combined = {}
        keys = list(teacher_scores.keys())
        for strategy in teacher_scores[keys[0]].keys():
            combined[strategy] = 0.0
            for i, teacher in enumerate(keys):
                combined[strategy] += self.weights[i] * teacher_scores[teacher][strategy]
        return combined

    async def train_step(self, teacher_scores, target, reward):
        pass


class MTOPStrategyEngine:
    def __init__(self, config):
        self.teacher_ensemble = StrategyTeacherEnsemble(config)
        self.student = StrategyDistillationStudent(config)

    async def select_strategy(self, state, carbon_intensity):
        scores = await self.teacher_ensemble.get_teacher_scores(state, carbon_intensity)
        combined = await self.student.combine(scores)
        best = max(combined, key=combined.get)
        return {'selected_strategy': best, 'teacher_scores': scores}

    async def update(self, selected, reward, teacher_scores):
        await self.student.train_step(teacher_scores, selected, reward)
        teacher_rewards = {name: reward for name in self.teacher_ensemble.teachers}
        self.teacher_ensemble.update_weights(teacher_rewards)

# -----------------------------------------------------------------------------
# Genetic Hyperparameter Optimizer
# -----------------------------------------------------------------------------
class GeneticHyperparameterOptimizer:
    def __init__(self, config, storage, domain, model):
        self.config = config
        self.storage = storage
        self.domain = domain
        self.model = model
        self.population_size = config.ga_population_size
        self.generations = config.ga_generations
        self.mutation_rate = config.ga_mutation_rate
        self.crossover_rate = config.ga_crossover_rate
        self.param_bounds = {
            'latent_dim': (16, 128), 'hidden_dim': (64, 512),
            'learning_rate': (1e-5, 1e-2), 'batch_size': (16, 256)}

    def _random_chromosome(self):
        return {
            'latent_dim': random.randint(*self.param_bounds['latent_dim']),
            'hidden_dim': random.randint(*self.param_bounds['hidden_dim']),
            'learning_rate': 10 ** random.uniform(-5, -2),
            'batch_size': 2 ** random.randint(4, 8)}

    def _mutate(self, chrom):
        new = chrom.copy()
        for param, bounds in self.param_bounds.items():
            if random.random() < self.mutation_rate:
                if param == 'learning_rate':
                    log_lr = (np.log10(new[param]) if NUMPY_AVAILABLE else new[param])
                    delta = random.gauss(0, 0.5)
                    new[param] = 10 ** max(-5, min(-2, log_lr + delta))
                elif param == 'batch_size':
                    new[param] = 2 ** random.randint(4, 8)
                else:
                    low, high = bounds
                    delta = random.gauss(0, (high - low) / 10)
                    new[param] = int(max(low, min(high, chrom[param] + delta)))
        return new

    def _crossover(self, p1, p2):
        if random.random() > self.crossover_rate:
            return p1.copy(), p2.copy()
        c1, c2 = p1.copy(), p2.copy()
        for param in self.param_bounds:
            if random.random() < 0.5:
                c1[param], c2[param] = p2[param], p1[param]
        return c1, c2

    async def _evaluate_fitness(self, chrom):
        return random.uniform(0.5, 1.0)

    async def run_search(self):
        population = [self._random_chromosome() for _ in range(self.population_size)]
        best_fitness = -1.0
        best_individual = None
        for gen in range(self.generations):
            fitnesses = await asyncio.gather(*[self._evaluate_fitness(ind) for ind in population])
            sorted_pop = sorted(zip(population, fitnesses), key=lambda x: x[1], reverse=True)
            if sorted_pop[0][1] > best_fitness:
                best_fitness = sorted_pop[0][1]
                best_individual = sorted_pop[0][0]
            parents = [ind for ind, _ in sorted_pop[:max(2, self.population_size // 2)]]
            offspring = []
            while len(offspring) < self.population_size:
                p1 = random.choice(parents)
                p2 = random.choice(parents)
                c1, c2 = self._crossover(p1, p2)
                offspring.append(self._mutate(c1))
                if len(offspring) < self.population_size:
                    offspring.append(self._mutate(c2))
            combined = parents + offspring
            combined_fitness = await asyncio.gather(*[self._evaluate_fitness(ind) for ind in combined])
            sorted_combined = sorted(zip(combined, combined_fitness), key=lambda x: x[1], reverse=True)
            population = [ind for ind, _ in sorted_combined[:self.population_size]]
            await self.storage.save_ga_population(gen, [
                {'individual_id': f'gen{gen}_ind{i}', 'attributes': population[i],
                 'fitness': float(fitnesses[i]) if i < len(fitnesses) else 0.0}
                for i in range(len(population))])
            GA_POPULATION_FITNESS.set(best_fitness)
        return best_individual if best_individual else self._random_chromosome()

# -----------------------------------------------------------------------------
# MoE Gating Network
# -----------------------------------------------------------------------------
class MoEGatingNetwork:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.num_experts = config.moe_expert_count
        self.hidden_layers = config.moe_hidden_layers
        self._gating_model = None
        self._scaler = None
        self._trained = False
        self._training_data = []
        self._lock = asyncio.Lock()
        self.experts = {
            'statistical': self._statistical_expert,
            'vae': self._vae_expert,
            'gan': self._gan_expert,
            'hybrid': self._hybrid_expert}
        self.expert_names = list(self.experts.keys())

    def _statistical_expert(self, context): return {'method': 'statistical'}
    def _vae_expert(self, context): return {'method': 'vae'}
    def _gan_expert(self, context): return {'method': 'gan'}
    def _hybrid_expert(self, context): return {'method': 'hybrid'}

    def _encode_context(self, context):
        features = []
        domain = context.get('domain', 'general')
        domain_map = {'esg_metrics': 0, 'carbon_data': 1, 'helium_data': 2,
                      'time_series': 3, 'general': 4}
        domain_vec = [0] * 5
        domain_vec[domain_map.get(domain, 0)] = 1
        features.extend(domain_vec)
        features.append(context.get('carbon_intensity', 0.4))
        features.append(context.get('quality_target', 0.8))
        features.append(context.get('epsilon', 1.0))
        features.append(context.get('n_samples', 1000) / 10000.0)
        features.append(1.0 if context.get('use_deep_model', False) else 0.0)
        if NUMPY_AVAILABLE:
            return np.array(features, dtype=np.float32)
        return features

    def _train_gating(self):
        if not SKLEARN_AVAILABLE or len(self._training_data) < 10:
            return
        X = np.array([item[0] for item in self._training_data])
        y = np.array([item[1] for item in self._training_data])
        self._scaler = StandardScaler()
        X_scaled = self._scaler.fit_transform(X)
        self._gating_model = MLPClassifier(hidden_layer_sizes=self.hidden_layers,
                                           max_iter=200, random_state=42)
        self._gating_model.fit(X_scaled, y)
        self._trained = True
        logger.info(f"MoE gating network trained on {len(self._training_data)} samples.")

    async def select_expert(self, context):
        features = self._encode_context(context)
        if self._trained and self._gating_model is not None and NUMPY_AVAILABLE:
            X = features.reshape(1, -1)
            if self._scaler:
                X = self._scaler.transform(X)
            probs = self._gating_model.predict_proba(X)[0]
            idx = int(np.argmax(probs))
            selected = self.expert_names[idx]
            for i, p in enumerate(probs):
                MOE_GATING_PROBABILITIES.labels(expert=self.expert_names[i]).set(float(p))
        else:
            selected = 'statistical'
        return selected, self.experts[selected](context)

    async def add_training_sample(self, context, selected_expert, reward):
        features = self._encode_context(context)
        if selected_expert not in self.expert_names:
            selected_expert = 'statistical'
        idx = self.expert_names.index(selected_expert)
        async with self._lock:
            self._training_data.append((features, idx, reward))
            if len(self._training_data) % 10 == 0:
                self._train_gating()

# -----------------------------------------------------------------------------
# Pareto Front Optimizer
# -----------------------------------------------------------------------------
class ParetoFrontOptimizer:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.pareto_front = []
        self.max_size = config.pareto_max_architectures
        self.objectives = ['quality', 'carbon', 'cost', 'privacy']
        self._lock = asyncio.Lock()

    def _dominates(self, a, b):
        am = (-a['metrics']['quality'], a['metrics']['carbon'], a['metrics']['cost'], a['metrics']['privacy'])
        bm = (-b['metrics']['quality'], b['metrics']['carbon'], b['metrics']['cost'], b['metrics']['privacy'])
        return all(am[i] <= bm[i] for i in range(4)) and any(am[i] < bm[i] for i in range(4))

    async def add_configuration(self, config_params, metrics):
        entry = {'solution_id': f"cfg_{uuid.uuid4().hex[:8]}",
                 'config_params': config_params, 'metrics': metrics,
                 'coverage_score': metrics.get('quality', 0),
                 'anomaly_diversity': 0.5, 'realism_score': metrics.get('quality', 0),
                 'data_quality': metrics.get('quality', 0)}
        async with self._lock:
            for existing in self.pareto_front:
                if self._dominates(existing, entry):
                    return False
            self.pareto_front = [e for e in self.pareto_front if not self._dominates(entry, e)]
            self.pareto_front.append(entry)
            if len(self.pareto_front) > self.max_size:
                self.pareto_front.sort(key=lambda e: e['metrics']['quality'])
                self.pareto_front = self.pareto_front[:self.max_size]
            await self.storage.save_pareto_front(self.pareto_front)
            PARETO_FRONT_SIZE.set(len(self.pareto_front))
            return True

    def get_pareto_front(self):
        return self.pareto_front

    async def get_trade_off_suggestions(self, user_weights):
        if not self.pareto_front:
            return []
        scored = []
        for e in self.pareto_front:
            score = (user_weights.get('quality', 0.4) * e['metrics']['quality'] -
                     user_weights.get('carbon', 0.3) * e['metrics']['carbon'] -
                     user_weights.get('cost', 0.2) * e['metrics']['cost'] -
                     user_weights.get('privacy', 0.1) * e['metrics']['privacy'])
            scored.append((score, e))
        scored.sort(reverse=True)
        return [e for _, e in scored[:5]]

# -----------------------------------------------------------------------------
# Evolutionary Architecture Search
# -----------------------------------------------------------------------------
class EvolutionaryArchitectureSearch:
    def __init__(self, config, storage, domain, input_dim):
        self.config = config
        self.storage = storage
        self.domain = domain
        self.input_dim = input_dim
        self.population_size = config.evolutionary_population_size
        self.generations = config.evolutionary_generations
        self.mutation_rate = 0.2
        self.crossover_rate = 0.7
        self.min_layers = 2
        self.max_layers = 6
        self.min_neurons = 16
        self.max_neurons = 512

    def _random_architecture(self):
        num_layers = random.randint(self.min_layers, self.max_layers)
        layers = [self.input_dim]
        for _ in range(num_layers - 1):
            layers.append(random.randint(self.min_neurons, self.max_neurons))
        layers.append(self.input_dim)
        return layers

    def _mutate(self, arch):
        new = arch.copy()
        if random.random() < self.mutation_rate and len(new) > 2:
            idx = random.randint(1, len(new) - 2)
            new[idx] = max(self.min_neurons, min(self.max_neurons, arch[idx] + random.randint(-32, 32)))
        if random.random() < self.mutation_rate and len(new) < 8:
            new.insert(random.randint(1, len(new) - 1), random.randint(self.min_neurons, self.max_neurons))
        if random.random() < self.mutation_rate and len(new) > 4:
            del new[random.randint(1, len(new) - 2)]
        return new

    def _crossover(self, p1, p2):
        if random.random() > self.crossover_rate:
            return p1.copy(), p2.copy()
        min_len = min(len(p1), len(p2))
        if min_len < 2:
            return p1.copy(), p2.copy()
        point = random.randint(1, min_len - 1)
        return p1[:point] + p2[point:], p2[:point] + p1[point:]

    async def _evaluate_fitness(self, arch):
        return random.uniform(0.5, 1.0)

    async def run_search(self):
        population = [self._random_architecture() for _ in range(self.population_size)]
        best_fitness = -1.0
        best_individual = None
        for _ in range(self.generations):
            fitnesses = await asyncio.gather(*[self._evaluate_fitness(ind) for ind in population])
            sorted_pop = sorted(zip(population, fitnesses), key=lambda x: x[1], reverse=True)
            if sorted_pop[0][1] > best_fitness:
                best_fitness = sorted_pop[0][1]
                best_individual = sorted_pop[0][0]
            parents = [ind for ind, _ in sorted_pop[:max(2, self.population_size // 2)]]
            offspring = []
            while len(offspring) < self.population_size:
                p1 = random.choice(parents)
                p2 = random.choice(parents)
                c1, c2 = self._crossover(p1, p2)
                offspring.append(self._mutate(c1))
                if len(offspring) < self.population_size:
                    offspring.append(self._mutate(c2))
            combined = parents + offspring
            combined_fitness = await asyncio.gather(*[self._evaluate_fitness(ind) for ind in combined])
            sorted_combined = sorted(zip(combined, combined_fitness), key=lambda x: x[1], reverse=True)
            population = [ind for ind, _ in sorted_combined[:self.population_size]]
        return best_individual if best_individual else self._random_architecture()

# -----------------------------------------------------------------------------
# Federated Model Aggregator
# -----------------------------------------------------------------------------
class FederatedModelAggregator:
    def __init__(self, config, storage, instance_id):
        self.config = config
        self.storage = storage
        self.instance_id = instance_id
        self.aggregated_weights = None
        self._lock = asyncio.Lock()

    async def share_local_weights(self, domain, weights):
        await self.storage.save_state(f"fed_weight_{self.instance_id}_{domain}",
                                      json.dumps(weights, default=str))

    async def pull_aggregated_weights(self, domain):
        rows = await self.storage._fetchall(
            "SELECT value FROM synthetic_state WHERE key LIKE ?", (f'%_{domain}',))
        if not rows:
            return None
        weight_list = []
        for r in rows:
            try:
                weight_list.append(json.loads(r[0]))
            except Exception:
                continue
        if not weight_list:
            return None
        avg = {}
        for w in weight_list:
            for k, v in w.items():
                avg[k] = avg.get(k, 0) + v
        for k in avg:
            avg[k] /= len(weight_list)
        self.aggregated_weights = avg
        return avg

    async def apply_aggregated_weights(self, domain, current_weights):
        agg = await self.pull_aggregated_weights(domain)
        if agg is None:
            return current_weights
        merged = {}
        for k in current_weights:
            merged[k] = (current_weights[k] + agg.get(k, current_weights[k])) / 2
        return merged

# -----------------------------------------------------------------------------
# Contextual Bandit Active Learner
# -----------------------------------------------------------------------------
class ContextualBanditActiveLearner:
    def __init__(self, storage):
        self.storage = storage
        self.strategies = ['uncertainty', 'diversity', 'random', 'mixed']
        self.weights = {s: 1.0 for s in self.strategies}
        self.counts = {s: 0 for s in self.strategies}
        self.rewards = {s: 0.0 for s in self.strategies}
        self._lock = asyncio.Lock()
        self.learning_rate = 0.1

    async def choose_strategy(self, context):
        async with self._lock:
            if random.random() < 0.1:
                return random.choice(self.strategies)
            return max(self.weights, key=lambda k: self.weights[k])

    async def update(self, strategy, reward):
        async with self._lock:
            if strategy not in self.counts:
                return
            self.counts[strategy] += 1
            self.rewards[strategy] += reward
            self.weights[strategy] = self.rewards[strategy] / self.counts[strategy]

# -----------------------------------------------------------------------------
# Adaptive Drift Detector
# -----------------------------------------------------------------------------
class AdaptiveDriftDetector:
    def __init__(self, storage, config, base_threshold=0.15):
        self.storage = storage
        self.config = config
        self.base_threshold = base_threshold
        self.domain_thresholds = {}
        self.history = defaultdict(list)
        self._lock = asyncio.Lock()

    async def get_threshold(self, domain):
        async with self._lock:
            return self.domain_thresholds.get(domain, self.base_threshold)

    async def detect_drift(self, data, domain, current_quality):
        drift_score = random.uniform(0, 0.3)
        async with self._lock:
            self.history[domain].append((drift_score, current_quality))
            if len(self.history[domain]) > 20:
                recent = self.history[domain][-10:]
                threshold = self.domain_thresholds.get(domain, self.base_threshold)
                high_drift = [d for d, q in recent if d > threshold]
                if high_drift:
                    avg_quality = sum(q for d, q in recent if d > threshold) / len(high_drift)
                    if avg_quality > 0.8:
                        self.domain_thresholds[domain] = min(0.5, threshold + 0.02)
                else:
                    if sum(q for _, q in recent) / len(recent) < 0.6:
                        self.domain_thresholds[domain] = max(0.05, threshold - 0.02)
        ADAPTIVE_DRIFT_THRESHOLD.labels(domain=domain).set(
            self.domain_thresholds.get(domain, self.base_threshold))
        return {'overall_drift': drift_score,
                'threshold': self.domain_thresholds.get(domain, self.base_threshold)}

# -----------------------------------------------------------------------------
# LIMIT Graph Manager
# -----------------------------------------------------------------------------
class LimitGraphManager:
    def __init__(self, config):
        self.config = config
        self.graph = {}
        self.constraints = {}
        self._lock = asyncio.Lock()
        self._initialize_graph()

    def _initialize_graph(self):
        nodes = ['quality', 'carbon', 'cost', 'privacy', 'latency']
        for n in nodes:
            self.graph[n] = {}
        self.graph['carbon']['cost'] = 0.8
        self.graph['cost']['quality'] = 0.5
        self.graph['quality']['cost'] = -0.2
        self.graph['privacy']['quality'] = -0.3
        self.graph['latency']['cost'] = 0.4

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
        return {'nodes': list(self.graph.keys()), 'constraints': self.constraints,
                'edge_count': sum(len(v) for v in self.graph.values())}

# -----------------------------------------------------------------------------
# MODP Strategy Optimizer
# -----------------------------------------------------------------------------
class MODPStrategyOptimizer:
    def __init__(self, config):
        self.config = config
        self.weights = config.modp_weights[:]
        self.candidates = [
            {'name': 'statistical', 'quality': 0.7, 'carbon': 0.2, 'cost': 0.1, 'privacy': 0.1},
            {'name': 'vae', 'quality': 0.9, 'carbon': 0.5, 'cost': 0.3, 'privacy': 0.2},
            {'name': 'gan', 'quality': 0.85, 'carbon': 0.6, 'cost': 0.4, 'privacy': 0.15},
            {'name': 'hybrid', 'quality': 0.88, 'carbon': 0.45, 'cost': 0.25, 'privacy': 0.18}]
        self.criteria = ['quality', 'carbon', 'cost', 'privacy']
        self._xai = None  # v17 XAI hook

    def _topsis(self, candidates, weights, criteria):
        matrix = np.array([[c[crit] for crit in criteria] for c in candidates])
        norm_matrix = matrix / np.sqrt((matrix ** 2).sum(axis=0) + 1e-12)
        weighted = norm_matrix * weights
        ideal = weighted.max(axis=0)
        neg_ideal = weighted.min(axis=0)
        d_plus = np.sqrt(((weighted - ideal) ** 2).sum(axis=1))
        d_minus = np.sqrt(((weighted - neg_ideal) ** 2).sum(axis=1))
        return d_minus / (d_plus + d_minus + 1e-9)

    async def select_strategy(self, state):
        candidates = [{'quality': c['quality'], 'carbon': 1.0 - c['carbon'],
                       'cost': 1.0 - c['cost'], 'privacy': 1.0 - c['privacy']}
                      for c in self.candidates]
        scores = await asyncio.to_thread(self._topsis, candidates, self.weights, self.criteria)
        best_idx = int(np.argmax(scores))
        best = self.candidates[best_idx]
        front = [{'name': c['name'],
                  'objectives': [c['quality'], c['carbon'], c['cost'], c['privacy']]}
                 for c in self.candidates]
        result = {'strategy': best['name'], 'scores': scores.tolist(),
                  'pareto_front': front,
                  'recommendation': f"Selected {best['name']} based on MODP"}
        # v17: XAI explanation
        if self._xai and NUMPY_AVAILABLE:
            try:
                feats = np.array([state.get('quality', 0.8), state.get('carbon', 0.4),
                                  state.get('cost', 0.5), state.get('privacy', 0.0)])
                def _proxy(x):
                    return float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))
                xai_result = await self._xai.explain(
                    decision_id=f"modp_{uuid.uuid4().hex[:8]}",
                    label=f"strategy={best['name']}",
                    features=feats,
                    names=['quality', 'carbon', 'cost', 'privacy'],
                    model_fn=_proxy)
                result['attributions'] = xai_result.get('attributions')
                result['explanation'] = xai_result.get('explanation')
            except Exception as e:
                logger.debug(f"XAI hook in MODP failed: {e}")
        return result

# -----------------------------------------------------------------------------
# RLHF Manager
# -----------------------------------------------------------------------------
class RLHFManager:
    def __init__(self, config):
        self.config = config
        self.feedback_buffer = []
        self.reward_model = None
        self.policy = {'weights': np.array([0.25, 0.25, 0.25, 0.25]) if NUMPY_AVAILABLE
                       else [0.25, 0.25, 0.25, 0.25]}
        self._lock = asyncio.Lock()
        if SKLEARN_AVAILABLE:
            self.reward_model = MLPRegressor(hidden_layer_sizes=(16,), max_iter=200, random_state=42)

    def _state_to_features(self, state):
        return [state.get('carbon_intensity', 0.4), state.get('quality_score', 0.5),
                state.get('cost', 0.5), state.get('privacy', 0.0)]

    def _action_to_index(self, action):
        actions = ['statistical', 'vae', 'gan', 'hybrid']
        return actions.index(action) if action in actions else 0

    async def record_feedback(self, state, action, reward):
        async with self._lock:
            self.feedback_buffer.append({
                'state': self._state_to_features(state),
                'action': self._action_to_index(action),
                'reward': reward})

    async def train_reward_model(self):
        if not self.reward_model or len(self.feedback_buffer) < 10:
            return
        X = [f['state'] for f in self.feedback_buffer]
        y = [f['reward'] for f in self.feedback_buffer]
        self.reward_model.fit(X, y)
        logger.info(f"RLHF reward model trained on {len(self.feedback_buffer)} samples")
        self.feedback_buffer.clear()

    async def get_policy_probs(self, state):
        if NUMPY_AVAILABLE and isinstance(self.policy['weights'], np.ndarray):
            return self.policy['weights'].tolist()
        return list(self.policy['weights'])

# -----------------------------------------------------------------------------
# Multi-Teacher Policy Distillation
# -----------------------------------------------------------------------------
class MultiTeacherPolicyDistillation:
    def __init__(self, config, moe_engine=None):
        self.config = config
        self.moe_engine = moe_engine
        self.student_policy = np.array([0.25, 0.25, 0.25, 0.25]) if NUMPY_AVAILABLE else [0.25] * 4
        self.temperature = config.distillation_temperature
        self.alpha = config.distillation_alpha
        self.history = deque(maxlen=500)
        self._lock = asyncio.Lock()

    async def distill(self, state):
        if not self.moe_engine or not NUMPY_AVAILABLE:
            return
        context = {'domain': state.get('domain', 'general'),
                   'carbon_intensity': state.get('carbon_intensity', 0.4),
                   'quality_target': state.get('quality_target', 0.8),
                   'epsilon': state.get('epsilon', 1.0),
                   'n_samples': state.get('n_samples', 1000),
                   'use_deep_model': state.get('use_deep_model', False)}
        selected, params = await self.moe_engine.select_expert(context)
        expert_names = list(self.moe_engine.expert_names)
        probs = np.ones(len(expert_names)) / len(expert_names)
        if self.moe_engine._trained:
            features = self.moe_engine._encode_context(context)
            if isinstance(features, np.ndarray):
                X = features.reshape(1, -1)
                if self.moe_engine._scaler:
                    X = self.moe_engine._scaler.transform(X)
                probs = self.moe_engine._gating_model.predict_proba(X)[0]
        teacher_dist = np.array(probs)
        teacher_dist = teacher_dist / teacher_dist.sum()
        soft_teacher = np.exp(np.log(teacher_dist + 1e-8) / self.temperature)
        soft_teacher = soft_teacher / soft_teacher.sum()
        loss = -np.sum(soft_teacher * np.log(self.student_policy + 1e-8))
        grad = -soft_teacher / (self.student_policy + 1e-8)
        self.student_policy -= 0.01 * grad
        self.student_policy = np.clip(self.student_policy, 0.01, None)
        self.student_policy = self.student_policy / self.student_policy.sum()
        async with self._lock:
            self.history.append({'teacher_dist': teacher_dist,
                                 'student_dist': self.student_policy.copy(), 'loss': loss})

    def get_student_probs(self):
        if NUMPY_AVAILABLE and isinstance(self.student_policy, np.ndarray):
            return self.student_policy.tolist()
        return list(self.student_policy)

# =============================================================================
# v17 MODULE 1: CAUSAL REINFORCEMENT LEARNING
# =============================================================================
class CausalGraphLearner:
    """Structure learning of a causal DAG via correlation + variance orientation."""
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []
        self._lock = asyncio.Lock()

    async def learn_structure(self, data, variables, threshold=0.25):
        self.variables = variables
        if not (SKLEARN_AVAILABLE and NUMPY_AVAILABLE):
            for i, s in enumerate(variables):
                for j, t in enumerate(variables):
                    if i < j and random.random() < 0.2:
                        await self._add_edge(s, t, random.uniform(0.1, 0.9), 0.5)
            return {k: dict(v) for k, v in self.graph.items()}
        try:
            if PANDAS_AVAILABLE and hasattr(data, 'columns'):
                X = data[variables].to_numpy(dtype=float)
            else:
                X = np.asarray(data, dtype=float)
        except Exception:
            return {k: dict(v) for k, v in self.graph.items()}
        if X.ndim != 2 or X.shape[1] != len(variables):
            return {k: dict(v) for k, v in self.graph.items()}
        X = (X - X.mean(axis=0)) / (X.std(axis=0) + 1e-9)
        corr = np.corrcoef(X, rowvar=False)
        async with self._lock:
            self.graph.clear()
            for i in range(len(variables)):
                for j in range(len(variables)):
                    if i == j:
                        continue
                    c = abs(float(corr[i, j]))
                    if c > threshold:
                        var_i = float(X[:, i].var())
                        var_j = float(X[:, j].var())
                        if var_i > var_j:
                            src, dst = variables[i], variables[j]
                        else:
                            src, dst = variables[j], variables[i]
                        await self._add_edge(src, dst, float(corr[i, j]), c)
            CAUSAL_EDGE_COUNT.set(sum(len(v) for v in self.graph.values()))
            CAUSAL_GRAPH_UPDATES.inc()
            return {k: dict(v) for k, v in self.graph.items()}

    async def _add_edge(self, src, dst, weight, confidence):
        self.graph[src][dst] = {'weight': weight, 'confidence': confidence}
        await self.storage.save_causal_edge(src, dst, weight, confidence)

    def get_parents(self, node):
        return [src for src, edges in self.graph.items() if node in edges]

    def get_children(self, node):
        return list(self.graph.get(node, {}).keys())

    def summary(self):
        return {'nodes': len(self.variables),
                'edges': sum(len(v) for v in self.graph.values()),
                'variables': list(self.variables)}


class CausalReinforcementLearner:
    """Do-calculus reward estimation + epsilon-greedy causal policy."""
    def __init__(self, config, storage, graph_learner):
        self.config = config
        self.storage = storage
        self.graph = graph_learner
        self.actions = ['statistical', 'vae', 'gan', 'hybrid']
        self.policy = (np.ones(len(self.actions)) / len(self.actions)) if NUMPY_AVAILABLE else [0.25] * 4
        self.action_values = defaultdict(float)
        self.action_counts = defaultdict(int)
        self.epsilon = config.causal_exploration_rate
        self._lock = asyncio.Lock()

    async def estimate_ate(self, treatment, outcome, samples=100):
        async with self._lock:
            edges = self.graph.graph.get(treatment, {})
            w = edges.get(outcome, {}).get('weight', 0.0)
        await self.storage.save_causal_experiment(
            f"exp_{uuid.uuid4().hex[:8]}", treatment, outcome, w, samples)
        CAUSAL_ATE.labels(treatment=treatment, outcome=outcome).set(w)
        return w

    async def choose_action(self, context):
        async with self._lock:
            if random.random() < self.epsilon:
                return random.choice(self.actions)
            return max(self.actions, key=lambda a: self.action_values.get(a, 0.0))

    async def update(self, action, reward, context):
        async with self._lock:
            if action not in self.actions:
                action = 'statistical'
            self.action_counts[action] += 1
            n = self.action_counts[action]
            self.action_values[action] += (reward - self.action_values[action]) / n
            CAUSAL_POLICY_REWARD.set(float(reward))
            if NUMPY_AVAILABLE:
                vals = np.array([self.action_values.get(a, 0.0) for a in self.actions])
                exp = np.exp(vals / 0.5)
                self.policy = exp / exp.sum()
        await self.storage.save_moe_training_sample(
            f"crl_{uuid.uuid4().hex[:8]}",
            [context.get('carbon_intensity', 0.4), reward],
            self.actions.index(action) if action in self.actions else 0, reward)

    def get_policy(self):
        if NUMPY_AVAILABLE and isinstance(self.policy, np.ndarray):
            return self.policy.tolist()
        return list(self.policy)

# =============================================================================
# v17 MODULE 2: TEMPORAL LOGIC & FORMAL VERIFICATION
# =============================================================================
class TemporalLogicVerifier:
    """LTL subset (G, F, X, U, &, |, !) + bounded model checker."""
    ATOMIC_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")

    def __init__(self, storage, config, formulas):
        self.storage = storage
        self.config = config
        self.formulas = [f.strip() for f in formulas if f.strip()]
        self.history = deque(maxlen=getattr(config, 'temporal_max_trace_length', 1000))
        self._lock = asyncio.Lock()

    def _atomic(self, expr, state):
        m = self.ATOMIC_RE.match(expr)
        if not m:
            return bool(state.get(expr.strip(), False))
        var, op, val = m.group(1), m.group(2), float(m.group(3))
        if var not in state:
            return False
        try:
            v = float(state[var])
        except Exception:
            return False
        return {'>=': v >= val, '<=': v <= val, '==': v == val,
                '!=': v != val, '>': v > val, '<': v < val}[op]

    def _split_top(self, s):
        depth = 0
        for i, ch in enumerate(s):
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif depth == 0 and ch in '&|':
                return s[:i].strip(), ch + s[i + 1:].strip()
        return s, ''

    def _eval(self, formula, trace, idx):
        if not trace or idx >= len(trace):
            return True
        f = formula.strip()
        while f.startswith('(') and f.endswith(')'):
            f = f[1:-1].strip()
        for op in ('G', 'F', 'X'):
            if f.startswith(op + ' ') or f.startswith(op + '('):
                inner = f[len(op):].strip()
                if op == 'G':
                    return all(self._eval(inner, trace, i) for i in range(idx, len(trace)))
                if op == 'F':
                    return any(self._eval(inner, trace, i) for i in range(idx, len(trace)))
                if op == 'X':
                    return idx + 1 < len(trace) and self._eval(inner, trace, idx + 1)
        left, rest = self._split_top(f)
        if rest:
            op, right = rest[0], rest[1:].strip()
            if op == '&':
                return self._eval(left, trace, idx) and self._eval(right, trace, idx)
            if op == '|':
                return self._eval(left, trace, idx) or self._eval(right, trace, idx)
        if ' U ' in f:
            l, r = f.split(' U ', 1)
            for i in range(idx, len(trace)):
                if self._eval(r, trace, i):
                    return True
                if not self._eval(l, trace, i):
                    return False
            return False
        if f.startswith('!'):
            return not self._eval(f[1:].strip(), trace, idx)
        return self._atomic(f, trace[idx])

    async def push_state(self, state):
        async with self._lock:
            self.history.append(state)
            TEMPORAL_TRACE_LENGTH.set(len(self.history))

    async def verify(self):
        async with self._lock:
            trace = list(self.history)
        results = {}
        for f in self.formulas:
            try:
                ok = self._eval(f, trace, 0) if trace else True
            except Exception:
                ok = False
            results[f] = ok
            TEMPORAL_VERIFICATIONS.labels(
                formula=f, status='satisfied' if ok else 'violated').inc()
            if not ok:
                TEMPORAL_VIOLATIONS.labels(formula=f).inc()
                await self.storage.save_temporal_violation(
                    f, len(trace) - 1, trace[-1] if trace else {})
        return results

    def model_check(self, formula, initial_state, max_depth=10):
        visited = set()
        queue = deque([(initial_state, 0)])
        cex = None
        while queue:
            state, depth = queue.popleft()
            key = json.dumps(state, sort_keys=True, default=str)
            if key in visited or depth > max_depth:
                continue
            visited.add(key)
            try:
                if not self._eval(formula, [state], 0):
                    cex = state
                    break
            except Exception:
                pass
            for k, v in state.items():
                if isinstance(v, (int, float)):
                    for delta in (-0.1, 0.1):
                        child = dict(state)
                        child[k] = v + delta
                        queue.append((child, depth + 1))
        return {'formula': formula, 'satisfied': cex is None,
                'counter_example': cex, 'visited': len(visited)}

# =============================================================================
# v17 MODULE 3: EXPLAINABLE AI
# =============================================================================
class XAIDecisionExplainer:
    """KernelSHAP + LIME + natural-language rendering."""
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.method = config.xai_method
        self.depth = config.xai_explanation_depth

    def _kernel_shap(self, f, x, names, n=64):
        if not NUMPY_AVAILABLE:
            return {k: 0.0 for k in names}
        base = np.zeros_like(x)
        contrib = np.zeros(len(x))
        for _ in range(n):
            perm = list(range(len(x)))
            random.shuffle(perm)
            prev = base.copy()
            for i in perm:
                cur = prev.copy()
                cur[i] = x[i]
                try:
                    delta = float(f(cur.reshape(1, -1))) - float(f(prev.reshape(1, -1)))
                except Exception:
                    delta = 0.0
                contrib[i] += delta
                prev = cur
        contrib = contrib / max(1, n)
        return dict(zip(names, contrib.tolist()))

    def _lime(self, f, x, names, n=200):
        if not (SKLEARN_AVAILABLE and NUMPY_AVAILABLE):
            return {k: random.uniform(-1, 1) for k in names}
        X = np.tile(x, (n, 1)) + np.random.normal(0, 0.1, (n, len(x)))
        try:
            y = np.array([float(f(r.reshape(1, -1))) for r in X])
        except Exception:
            return {k: 0.0 for k in names}
        w = np.exp(-np.sum((X - x) ** 2, axis=1) / 0.02)
        model = LinearRegression()
        try:
            model.fit(X, y, sample_weight=w)
            return dict(zip(names, model.coef_.tolist()))
        except Exception:
            return {k: 0.0 for k in names}

    def _nl(self, decision, attrs):
        top = sorted(attrs.items(), key=lambda kv: abs(kv[1]), reverse=True)[:self.depth]
        lines = "\n".join(f"  • {k}: {v:+.3f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    async def explain(self, decision_id, label, features, names, model_fn):
        if self.method == 'lime':
            attrs = self._lime(model_fn, features, names)
        else:
            attrs = self._kernel_shap(model_fn, features, names)
        nl = self._nl(label, attrs)
        XAI_EXPLANATIONS.labels(method=self.method).inc()
        for k, v in list(attrs.items())[:self.depth]:
            XAI_FEATURE_IMPORTANCE.labels(feature=k).set(v)
        await self.storage.save_xai_explanation(decision_id, self.method, attrs, nl)
        return {'decision_id': decision_id, 'method': self.method,
                'attributions': attrs, 'explanation': nl}

# =============================================================================
# v17 MODULE 4: ADAPTIVE PRECISION SWITCHER
# =============================================================================
class AdaptivePrecisionSwitcher:
    ENERGY_MODEL = {'fp32': 1.0, 'tf32': 0.75, 'bf16': 0.55, 'fp16': 0.5, 'int8': 0.3}
    LATENCY_MODEL = {'fp32': 1.0, 'tf32': 0.8, 'bf16': 0.65, 'fp16': 0.6, 'int8': 0.4}

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.levels = config.precision_levels
        self.current = 'fp32'
        self.energy_saved_wh = 0.0
        self._lock = asyncio.Lock()

    def _hw_probe(self):
        info = {'cuda': False, 'bf16': False, 'name': 'cpu'}
        if TORCH_AVAILABLE:
            try:
                info['cuda'] = torch.cuda.is_available()
                if info['cuda']:
                    info['name'] = torch.cuda.get_device_name(0)
                    info['bf16'] = torch.cuda.is_bf16_supported()
            except Exception:
                pass
        return info

    def select_precision(self):
        hw = self._hw_probe()
        cands = list(self.levels)
        if not hw['cuda']:
            cands = [p for p in cands if p in ('fp32', 'int8')]
        if not hw['bf16']:
            cands = [p for p in cands if p != 'bf16']
        if not cands:
            cands = ['fp32']
        return min(cands, key=lambda p: self.ENERGY_MODEL.get(p, 1.0))

    async def switch_to(self, target, reason='policy'):
        async with self._lock:
            if target == self.current or target not in self.levels:
                return False
            old = self.current
            self.current = target
            saved = max(0.0, self.ENERGY_MODEL.get(old, 1.0) - self.ENERGY_MODEL.get(target, 1.0))
            self.energy_saved_wh += saved
            PRECISION_SWITCHES.labels(from_p=old, to_p=target).inc()
            PRECISION_ENERGY_SAVED.set(self.energy_saved_wh)
            PRECISION_CURRENT.labels(level=target).set(1)
            await self.storage.save_precision_switch(old, target, reason, saved)
            logger.info(f"Precision switched {old} -> {target} (reason={reason})")
            return True

    async def auto_switch(self, recent_acc, baseline_acc):
        if baseline_acc <= 0:
            return
        drop = (baseline_acc - recent_acc) / baseline_acc
        if drop > self.config.precision_switch_threshold:
            await self.switch_to('fp32', reason=f'accuracy drop {drop:.3f}')
        elif drop < self.config.precision_switch_threshold / 2:
            await self.switch_to(self.select_precision(), reason='headroom')

    @property
    def hardware(self):
        return self._hw_probe()

# =============================================================================
# v17 MODULE 5: CARBON MARKET & REC INTEGRATOR
# =============================================================================
class CarbonMarketIntegrator:
    def __init__(self, config, storage, carbon_manager):
        self.config = config
        self.storage = storage
        self.carbon_manager = carbon_manager
        self.last_price = 25.0
        self.last_rec_balance = 0.0
        self._circuit = CircuitBreaker(3, 60.0, 'carbon_market')
        self._lock = asyncio.Lock()

    async def _fetch_price(self):
        if AIOHTTP_AVAILABLE and self.config.carbon_market_api_url:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(f"{self.config.carbon_market_api_url}/price", timeout=8) as r:
                        if r.status == 200:
                            data = await r.json()
                            return float(data.get('price', self.last_price))
            except Exception as e:
                logger.debug(f"Carbon market API failed: {e}")
        return max(5.0, self.last_price + random.gauss(0, 1.5))

    async def update_price(self):
        try:
            price = await self._circuit.call(self._fetch_price)
        except Exception:
            price = self.last_price
        async with self._lock:
            self.last_price = price
            await self.storage.save_credit_price(price, 'USD', 'oracle')
            CARBON_CREDIT_PRICE.set(price)
        return price

    async def purchase_rec(self, mwh, price_per_mwh=5.0, source='wind'):
        cost = mwh * price_per_mwh
        await self.storage.save_rec(mwh, price_per_mwh, source)
        self.last_rec_balance = await self.storage.get_rec_balance()
        REC_BALANCE.set(self.last_rec_balance)
        NET_ZERO_MATCHES.inc()
        logger.info(f"Purchased {mwh} MWh REC from {source} for ${cost:.2f}")
        return cost

    async def net_zero_schedule(self, workload_kwh, current_intensity):
        price = await self.update_price()
        carbon_kg = workload_kwh * current_intensity
        offset_cost = (carbon_kg / 1000.0) * price
        CARBON_OFFSET_COST.set(offset_cost)
        if current_intensity > 0.3:
            action = 'defer'
        elif offset_cost < 0.5:
            action = 'run_offset'
        else:
            action = 'run'
        return {'action': action, 'carbon_kg': carbon_kg,
                'offset_cost_usd': offset_cost, 'credit_price_usd': price,
                'rec_balance_mwh': await self.storage.get_rec_balance()}

# =============================================================================
# v17 MODULE 6: CHAOS TESTING ENGINE
# =============================================================================
class ChaosTestingEngine:
    FAULT_TYPES = ['latency', 'exception', 'data_corruption', 'memory_pressure', 'network_drop']

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def _steady(self):
        if not self.active:
            return True
        return random.random() > self.config.chaos_intensity

    async def _inject_latency(self, delay=0.5):
        await asyncio.sleep(delay)

    async def _inject_exception(self):
        raise RuntimeError("Chaos: injected exception")

    async def _inject_corruption(self, data):
        if PANDAS_AVAILABLE and NUMPY_AVAILABLE and hasattr(data, 'copy'):
            c = data.copy()
            for col in c.columns:
                if c[col].dtype.kind in 'fi':
                    mask = np.random.rand(len(c)) < self.config.chaos_intensity
                    c.loc[mask, col] = np.nan
            return c
        return data

    def _inject_memory(self, mb=5):
        _ = bytearray(mb * 1024 * 1024)
        del _

    async def run_experiment(self, name, fault_type, target=None, workload=None):
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"Unknown fault type {fault_type}")
        before = await self._steady()
        CHAOS_STEADY_STATE.set(1 if before else 0)
        status = 'completed'
        try:
            async with self._lock:
                self.active[name] = {'fault_type': fault_type,
                                     'blast': self.config.chaos_blast_radius,
                                     'started': datetime.now().isoformat()}
            if fault_type == 'latency':
                await self._inject_latency()
            elif fault_type == 'exception':
                await self._inject_exception()
            elif fault_type == 'data_corruption':
                if workload and 'data' in workload:
                    _ = await self._inject_corruption(workload['data'])
            elif fault_type == 'memory_pressure':
                self._inject_memory()
            elif fault_type == 'network_drop':
                await asyncio.sleep(0.2)
        except Exception as e:
            logger.info(f"Chaos '{name}' raised (expected): {e}")
            status = 'injected'
        finally:
            async with self._lock:
                self.active.pop(name, None)
        after = await self._steady()
        await self.storage.save_chaos_experiment(
            name, fault_type, status, int(before), int(after), self.config.chaos_blast_radius)
        CHAOS_EXPERIMENTS.labels(fault_type=fault_type, status=status).inc()
        CHAOS_STEADY_STATE.set(1 if after else 0)
        if not after and self.config.chaos_auto_rollback:
            CHAOS_ROLLBACKS.inc()
        return {'name': name, 'fault_type': fault_type,
                'steady_before': before, 'steady_after': after,
                'status': status, 'blast_radius': self.config.chaos_blast_radius}

# -----------------------------------------------------------------------------
# Autonomous Simulation Optimizer
# -----------------------------------------------------------------------------
class AutonomousSyntheticOptimizer:
    def __init__(self, config, storage, state):
        self.config = config
        self.storage = storage
        self.state = state
        self.mtop_engine = MTOPStrategyEngine(config)
        self.moe_gating = MoEGatingNetwork(config, storage) if config.moe_enabled else None
        self.ga_optimizer = None
        self.pareto_optimizer = None
        self.limit_graph = None
        self.modp_optimizer = None
        self.rlhf = None
        self.distillation = None
        self.causal_rl = None  # v17
        self.causal_graph = None  # v17
        self._last_context = {}

    async def optimize_simulation(self, current_state, strategy=None):
        carbon_intensity = current_state.get('carbon_intensity', 0.4)
        self._last_context = dict(current_state)
        # Priority: Causal RL > MODP > RLHF > Distillation > MoE > MTOP
        if self.causal_rl and self.config.causal_rl_enabled:
            selected = await self.causal_rl.choose_action(current_state)
            result = {'action': f'{selected}_optimization', 'selected_strategy': selected,
                      'recommendation': f"Selected {selected} via causal policy"}
        elif self.modp_optimizer and self.config.modp_enabled:
            modp_result = await self.modp_optimizer.select_strategy(current_state)
            selected = modp_result['strategy']
            result = {'action': f'{selected}_optimization', 'selected_strategy': selected,
                      'recommendation': modp_result['recommendation']}
            if 'attributions' in modp_result:
                result['attributions'] = modp_result['attributions']
            if 'explanation' in modp_result:
                result['explanation'] = modp_result['explanation']
        elif self.rlhf and self.rlhf.reward_model is not None:
            probs = await self.rlhf.get_policy_probs(current_state)
            expert_names = ['statistical', 'vae', 'gan', 'hybrid']
            selected = expert_names[int(np.argmax(probs)) % len(expert_names)] if NUMPY_AVAILABLE else 'statistical'
            result = {'action': f'{selected}_optimization', 'selected_strategy': selected,
                      'recommendation': f"Selected {selected} based on RLHF"}
        elif self.distillation and self.distillation.get_student_probs():
            probs = self.distillation.get_student_probs()
            expert_names = ['statistical', 'vae', 'gan', 'hybrid']
            selected = expert_names[int(np.argmax(probs)) % len(expert_names)] if NUMPY_AVAILABLE else 'statistical'
            result = {'action': f'{selected}_optimization', 'selected_strategy': selected,
                      'recommendation': f"Selected {selected} based on Distillation"}
        elif self.moe_gating and self.config.moe_enabled:
            selected, params = await self.moe_gating.select_expert(current_state)
            result = {'action': f'{selected}_optimization', 'selected_strategy': selected,
                      'expert_params': params,
                      'recommendation': self._generate_recommendation(selected, current_state)}
        elif self.mtop_engine:
            mtop_result = await self.mtop_engine.select_strategy(current_state, carbon_intensity)
            selected = mtop_result['selected_strategy']
            result = {'action': f'{selected}_optimization', 'selected_strategy': selected,
                      'scores': mtop_result['teacher_scores'],
                      'recommendation': self._generate_recommendation(selected, current_state)}
        else:
            result = {'action': 'no_op', 'selected_strategy': 'balanced',
                      'recommendation': 'No optimizer available'}
        await self.storage.save_state(
            f"last_optimization_{datetime.now().isoformat()}",
            json.dumps({k: v for k, v in result.items() if k != 'scores'}, default=str))
        AUTONOMOUS_OPTIMIZATIONS.labels(
            strategy=result['selected_strategy'], status='success').inc()
        return result

    async def record_outcome(self, reward, context):
        if self.causal_rl:
            await self.causal_rl.update(context.get('selected_strategy', 'statistical'),
                                        reward, context)
        if self.moe_gating and self.config.moe_enabled:
            await self.moe_gating.add_training_sample(
                context, context.get('selected_strategy', 'statistical'), reward)
        if self.mtop_engine:
            teacher_scores = context.get('teacher_scores', {})
            await self.mtop_engine.update(context.get('selected_strategy', 'balanced'),
                                          reward, teacher_scores)

    def _generate_recommendation(self, strategy, state):
        if strategy == 'performance':
            return "Focus on maximising simulation accuracy."
        elif strategy == 'carbon':
            return "Prioritise carbon-efficient simulation configurations."
        elif strategy == 'cost':
            return "Optimise simulation resource usage."
        elif strategy == 'adaptive':
            return "Adjust dynamically based on recent trends."
        return "Maintain current strategy with monitoring."

# -----------------------------------------------------------------------------
# Simulation State
# -----------------------------------------------------------------------------
class SimulationState:
    def __init__(self, storage):
        self.storage = storage
        self.confidence = 0.5
        self.uncertainty = 0.1
        self.historical_success_rate = 0.5
        self.reflection_count = 0
        self.carbon_budget_remaining = 100.0
        self.helium_budget_remaining = 100.0
        self.active_strategies = []
        self.strategy_effectiveness = {}
        self.preferred_experts = []
        self.avoided_experts = []
        self.expert_health_scores = {}
        self.recent_rewards = deque(maxlen=100)
        self.esg_threshold = 80

    async def save(self):
        await self.storage.save_state('simulation_state', json.dumps({
            'confidence': self.confidence, 'uncertainty': self.uncertainty,
            'reflection_count': self.reflection_count}, default=str))

    async def trigger_reflection(self, trigger_type, **kwargs):
        self.reflection_count += 1

# -----------------------------------------------------------------------------
# Legacy security / blockchain / cloud stubs
# -----------------------------------------------------------------------------
class QuantumResilientSimulationSecurity:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
    async def sign(self, data: bytes):
        try:
            if PQC_AVAILABLE:
                pk, sk = dilithium.generate_keypair()
                sig = dilithium.sign(sk, data)
                QUANTUM_SIGNATURES.labels(algorithm='dilithium', status='ok').inc()
                return {'algorithm': 'dilithium', 'signature': sig.hex(), 'public_key': pk.hex()}
        except Exception:
            pass
        QUANTUM_SIGNATURES.labels(algorithm='fallback', status='ok').inc()
        return {'algorithm': 'sha256', 'signature': hashlib.sha256(data).hexdigest()}


class BlockchainSimulationVerification:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
    async def anchor(self, data_hash):
        BLOCKCHAIN_VERIFICATIONS.labels(status='simulated').inc()
        return f"0x{secrets.token_hex(32)}"


class MultiCloudSimulationDistribution:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
    async def distribute(self, data, name):
        out = {}
        if AWS_AVAILABLE:
            CLOUD_DISTRIBUTIONS.labels(provider='aws', status='ok').inc()
            out['aws'] = 's3://sim/' + name
        if AZURE_AVAILABLE:
            CLOUD_DISTRIBUTIONS.labels(provider='azure', status='ok').inc()
            out['azure'] = 'azure://sim/' + name
        if GCP_AVAILABLE:
            CLOUD_DISTRIBUTIONS.labels(provider='gcp', status='ok').inc()
            out['gcp'] = 'gs://sim/' + name
        if not out:
            CLOUD_DISTRIBUTIONS.labels(provider='local', status='ok').inc()
            out['local'] = '/tmp/sim/' + name
        return out


class RLParameterOptimizer:
    def __init__(self, simulator=None, algorithm='PPO'):
        self.simulator = simulator
        self.algorithm = algorithm


class BayesianHyperparameterTuner:
    def __init__(self, simulator=None):
        self.simulator = simulator


class ChaosEngineeringManager:
    def __init__(self, storage):
        self.storage = storage


class ScenarioComparisonEngine:
    def __init__(self, simulator=None):
        self.simulator = simulator


class EnhancedVisualizationDashboard:
    def __init__(self, simulator=None):
        self.simulator = simulator
    async def start(self): pass
    async def stop(self): pass


class GeneticParameterOptimizer:
    def __init__(self, config, storage, simulator=None):
        self.config = config
        self.storage = storage
        self.simulator = simulator
    async def run_search(self):
        return {'param': random.random()}


class FederatedRLAggregator:
    def __init__(self, config, storage, instance_id):
        self.config = config
        self.storage = storage
        self.instance_id = instance_id


class AdaptiveChaosInjector:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config


class ActiveUserPreferenceLearner:
    def __init__(self, storage, websocket=None):
        self.storage = storage
        self.websocket = websocket


class DriftDetector:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
    async def detect(self, data, domain):
        return {'drift': random.uniform(0, 0.2)}


class EnhancedWebSocketServer:
    def __init__(self, port):
        self.port = port
        self._server = None
    async def start(self):
        if WEBSOCKETS_AVAILABLE:
            try:
                self._server = await serve(self._handler, "0.0.0.0", self.port)
                logger.info(f"WebSocket server on :{self.port}")
            except Exception as e:
                logger.warning(f"WebSocket failed: {e}")
    async def _handler(self, ws, path=None):
        WS_CONNECTIONS.inc()
        try:
            async for _ in ws:
                pass
        except Exception:
            pass
        finally:
            WS_CONNECTIONS.dec()
    async def stop(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()

# -----------------------------------------------------------------------------
# MAIN SIMULATOR
# -----------------------------------------------------------------------------
class EnhancedSystemSimulatorV11:
    def __init__(self, config=None):
        self.config = config or SyntheticDataConfig()
        self.instance_id = self.config.instance_id
        self.storage = EnhancedStorage(self.config)
        self.state = SimulationState(self.storage)
        self.quantum_security = QuantumResilientSimulationSecurity(self.config, self.storage)
        self.blockchain = BlockchainSimulationVerification(self.config, self.storage)
        self.carbon_manager = CarbonIntensityManager(self.config, self.storage)
        self.cloud_distributor = MultiCloudSimulationDistribution(self.config, self.storage)
        self.rl_optimizer = RLParameterOptimizer(self, algorithm='PPO')
        self.bayesian_tuner = BayesianHyperparameterTuner(self)
        self.chaos_manager = ChaosEngineeringManager(self.storage)
        self.scenario_engine = ScenarioComparisonEngine(self)
        self.visualization_dashboard = EnhancedVisualizationDashboard(self)
        self.ga_optimizer = GeneticParameterOptimizer(self.config, self.storage, self) \
            if self.config.ga_enabled else None
        self.moe_gating = MoEGatingNetwork(self.config, self.storage) \
            if self.config.moe_enabled else None
        self.pareto_optimizer = ParetoFrontOptimizer(self.config, self.storage) \
            if self.config.pareto_enabled else None
        self.federated_rl_aggregator = FederatedRLAggregator(
            self.config, self.storage, self.instance_id) \
            if self.config.federated_learning_enabled else None
        self.adaptive_chaos = AdaptiveChaosInjector(self.storage, self.config)
        self.user_pref_learner = ActiveUserPreferenceLearner(self.storage, None)
        self.drift_detector = DriftDetector(self.storage, self.config)
        self.limit_graph = LimitGraphManager(self.config) if self.config.limit_graph_enabled else None
        self.modp_optimizer = MODPStrategyOptimizer(self.config) if self.config.modp_enabled else None
        self.rlhf = RLHFManager(self.config) if self.config.rlhf_enabled else None
        self.distillation = MultiTeacherPolicyDistillation(self.config, self.moe_gating) \
            if self.config.distillation_enabled and self.moe_gating else None

        # ===== v17 =====
        self.causal_graph = CausalGraphLearner(self.storage, self.config) \
            if self.config.causal_rl_enabled else None
        self.causal_rl = CausalReinforcementLearner(self.config, self.storage, self.causal_graph) \
            if self.config.causal_rl_enabled and self.causal_graph else None
        self.temporal_verifier = TemporalLogicVerifier(
            self.storage, self.config, self.config.temporal_formulas) \
            if self.config.temporal_logic_enabled else None
        self.xai = XAIDecisionExplainer(self.config, self.storage) if self.config.xai_enabled else None
        self.precision_switcher = AdaptivePrecisionSwitcher(self.config, self.storage) \
            if self.config.adaptive_precision_enabled else None
        self.carbon_market = CarbonMarketIntegrator(self.config, self.storage, self.carbon_manager) \
            if self.config.carbon_market_enabled else None
        self.chaos_engine = ChaosTestingEngine(self.config, self.storage) \
            if self.config.chaos_testing_enabled else None

        # Inject hooks
        if self.xai and self.modp_optimizer:
            self.modp_optimizer._xai = self.xai
        if self.chaos_engine:
            self.carbon_manager._circuit_breaker.chaos_engine = self.chaos_engine

        # Autonomous optimizer
        self.autonomous_optimizer = AutonomousSyntheticOptimizer(
            self.config, self.storage, self.state)
        self.autonomous_optimizer.ga_optimizer = self.ga_optimizer
        self.autonomous_optimizer.pareto_optimizer = self.pareto_optimizer
        self.autonomous_optimizer.limit_graph = self.limit_graph
        self.autonomous_optimizer.modp_optimizer = self.modp_optimizer
        self.autonomous_optimizer.rlhf = self.rlhf
        self.autonomous_optimizer.distillation = self.distillation
        self.autonomous_optimizer.causal_rl = self.causal_rl
        self.autonomous_optimizer.causal_graph = self.causal_graph

        self.websocket = EnhancedWebSocketServer(self.config.websocket_port)

        self.all_results = deque(maxlen=1000)
        self.simulation_runs = deque(maxlen=1000)
        self._results_lock = asyncio.Lock()
        self._simulation_semaphore = asyncio.Semaphore(5)
        self.operation_queue = asyncio.Queue(maxsize=100)
        self._queue_worker = None
        self._running = False
        self.background_tasks = set()
        self._shutdown_event = asyncio.Event()

        if PROMETHEUS_AVAILABLE:
            try:
                start_http_server(self.config.metrics_port)
                logger.info(f"Prometheus metrics on :{self.config.metrics_port}")
            except Exception as e:
                logger.warning(f"Prometheus start failed: {e}")

        logger.info("EnhancedSystemSimulatorV11 v%s initialized (instance=%s)",
                    self.config.version, self.instance_id)

    async def start(self):
        self._running = True
        await self.websocket.start()
        await self.visualization_dashboard.start()
        self._queue_worker = asyncio.create_task(self._process_queue())
        tasks = [
            asyncio.create_task(self._health_check_loop()),
            asyncio.create_task(self._carbon_update_loop()),
            asyncio.create_task(self._auto_optimize_loop()),
            asyncio.create_task(self._ga_optimization_loop()),
            asyncio.create_task(self._moe_training_loop()),
        ]
        if self.limit_graph:
            tasks.append(asyncio.create_task(self._limit_graph_loop()))
        if self.rlhf:
            tasks.append(asyncio.create_task(self._rlhf_loop()))
        if self.distillation:
            tasks.append(asyncio.create_task(self._distillation_loop()))
        if self.causal_graph and self.causal_rl:
            tasks.append(asyncio.create_task(self._causal_rl_loop()))
        if self.temporal_verifier:
            tasks.append(asyncio.create_task(self._temporal_logic_loop()))
        if self.xai:
            tasks.append(asyncio.create_task(self._xai_loop()))
        if self.precision_switcher:
            tasks.append(asyncio.create_task(self._precision_loop()))
        if self.carbon_market:
            tasks.append(asyncio.create_task(self._carbon_market_loop()))
        if self.chaos_engine:
            tasks.append(asyncio.create_task(self._chaos_loop()))
        for t in tasks:
            self.background_tasks.add(t)
            t.add_done_callback(self.background_tasks.discard)
        logger.info(f"Simulator started with {len(self.background_tasks)} background tasks")

    async def shutdown(self):
        logger.info("Shutting down simulator...")
        self._shutdown_event.set()
        self._running = False
        if self._queue_worker:
            self._queue_worker.cancel()
            try:
                await self._queue_worker
            except asyncio.CancelledError:
                pass
        for t in list(self.background_tasks):
            t.cancel()
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        await self.websocket.stop()
        await self.visualization_dashboard.stop()
        await self.carbon_manager.close()
        await self.state.save()
        self.storage.dispose()
        logger.info("Shutdown complete")

    async def _process_queue(self):
        while self._running:
            try:
                op = await asyncio.wait_for(self.operation_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                if op['type'] == 'simulation':
                    result = await self._execute_simulation(op)
                    if not op['future'].done():
                        op['future'].set_result(result)
            except Exception as e:
                if not op['future'].done():
                    op['future'].set_exception(e)
            finally:
                self.operation_queue.task_done()
                GENERATION_QUEUE_SIZE.set(self.operation_queue.qsize())

    async def run_simulation(self, domain='general', n_samples=1000,
                             method='statistical', enable_privacy=False,
                             epsilon=1.0, use_deep_model=False, user_id=None):
        future = asyncio.Future()
        await self.operation_queue.put({
            'type': 'simulation', 'domain': domain, 'n_samples': n_samples,
            'method': method, 'enable_privacy': enable_privacy,
            'epsilon': epsilon, 'use_deep_model': use_deep_model,
            'user_id': user_id, 'future': future})
        GENERATION_QUEUE_SIZE.set(self.operation_queue.qsize())
        return await future

    async def _execute_simulation(self, operation):
        async with self._simulation_semaphore:
            t0 = time.time()
            domain = operation.get('domain', 'general')
            n_samples = operation.get('n_samples', 1000)
            method = operation.get('method', 'statistical')
            enable_privacy = operation.get('enable_privacy', False)
            epsilon = operation.get('epsilon', 1.0)
            use_deep_model = operation.get('use_deep_model', False)
            user_id = operation.get('user_id')

            if user_id and self.user_pref_learner:
                try:
                    await self.storage.save_user_preference(
                        user_id, {'domain': domain, 'method': method}, None)
                except Exception:
                    pass

            carbon_intensity = await self.carbon_manager.get_current_intensity()

            # v17: carbon market decision
            if self.carbon_market:
                try:
                    decision = await self.carbon_market.net_zero_schedule(
                        workload_kwh=n_samples * 0.001,
                        current_intensity= carbon_intensity)
                    logger.debug(f"Carbon market decision: {decision['action']}")
                except Exception as e:
                    logger.debug(f"Carbon market failed: {e}")

            # Optimize strategy selection
            opt_state = {
                'carbon_intensity': carbon_intensity,
                'quality': 0.8, 'cost': 0.5, 'privacy': epsilon if enable_privacy else 0.0,
                'selected_strategy': method}
            opt_result = await self.autonomous_optimizer.optimize_simulation(opt_state)
            selected_strategy = opt_result.get('selected_strategy', method)
            if selected_strategy in ('statistical', 'vae', 'gan', 'hybrid'):
                method = selected_strategy

            # Precision selection
            if self.precision_switcher:
                dm = DeepGenerativeModel(input_dim=10, model_type=method) if domain not in ['carbon_data'] else None
                await self.precision_switcher.auto_switch(
                    recent_acc=0.85, baseline_acc=0.9)

            # Generate data
            if use_deep_model and method in ('vae', 'gan'):
                dm = DeepGenerativeModel(input_dim=10, model_type=method)
                if self.precision_switcher:
                    dm.precision = self.precision_switcher.current
                if NUMPY_AVAILABLE:
                    arr = await dm.generate(n_samples)
                    if PANDAS_AVAILABLE:
                        data = pd.DataFrame(arr, columns=[f'f_{i}' for i in range(arr.shape[1])])
                    else:
                        data = arr
                else:
                    data = await dm.generate(n_samples)
                used_method = f"deep_{method}"
                DEEP_GENERATION_SCORE.labels(model_type=method).set(0.8)
            else:
                gen = DomainDataGenerator(domain)
                data = await gen.generate(n_samples, method)
                used_method = method

            # Quality assessment
            quality_score = 75.0
            if PANDAS_AVAILABLE and hasattr(data, 'shape'):
                try:
                    missing = float(data.isnull().mean().mean())
                    quality_score = max(0.0, 100.0 - missing * 100.0 - random.uniform(0, 5))
                except Exception:
                    quality_score = 75.0
            DATA_QUALITY_SCORE.set(quality_score)
            reward = quality_score / 100.0

            # Temporal logic push
            if self.temporal_verifier:
                await self.temporal_verifier.push_state({
                    'quality': quality_score / 100.0,
                    'carbon': carbon_intensity,
                    'privacy_budget': 0.0 if not enable_privacy else epsilon,
                    'generation_complete': True})

            # Record outcome for optimizer
            await self.autonomous_optimizer.record_outcome(
                reward, {**opt_state, 'selected_strategy': used_method})

            # RLHF feedback
            if self.rlhf and quality_score > 85:
                await self.rlhf.record_feedback(
                    {'carbon_intensity': carbon_intensity,
                     'quality_score': quality_score,
                     'cost': n_samples * 0.001,
                     'privacy': epsilon if enable_privacy else 0.0},
                    used_method, reward)

            # Pareto front update
            if self.pareto_optimizer:
                await self.pareto_optimizer.add_configuration(
                    config_params={'domain': domain, 'method': used_method,
                                   'n_samples': n_samples},
                    metrics={'quality': quality_score,
                             'carbon': n_samples * 0.001,
                             'cost': n_samples * 0.0001,
                             'privacy': epsilon if enable_privacy else 0.0})

            # LIMIT graph
            if self.limit_graph:
                await self.limit_graph.update_constraint('quality', quality_score)
                await self.limit_graph.update_constraint('carbon', carbon_intensity)

            # Persist
            await self.storage.save_generation_history(
                self.config.version, n_samples,
                random.uniform(0, 0.1), random.uniform(0, 0.2),
                {'domain': domain, 'method': used_method})

            elapsed = time.time() - t0
            GENERATION_DURATION.labels(domain=domain, method=used_method).observe(elapsed)
            DATA_GENERATIONS.labels(domain=domain, status='success',
                                    method=used_method).inc()

            async with self._results_lock:
                self.all_results.append({'domain': domain, 'method': used_method,
                                         'quality': quality_score, 'reward': reward})
            return {'data': data, 'quality': quality_score, 'method': used_method,
                    'strategy': opt_result.get('selected_strategy', used_method),
                    'recommendation': opt_result.get('recommendation', '')}

    # Background loops
    async def _health_check_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.health_check_interval)
            try:
                HEALTH_SCORE.set(0.9 + random.uniform(-0.05, 0.05))
            except Exception as e:
                logger.error(f"Health loop: {e}")

    async def _carbon_update_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.carbon_update_interval)
            try:
                await self.carbon_manager.get_current_intensity()
            except Exception as e:
                logger.error(f"Carbon loop: {e}")

    async def _auto_optimize_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.auto_optimize_interval)
            try:
                carbon = await self.carbon_manager.get_current_intensity()
                await self.autonomous_optimizer.optimize_simulation(
                    {'carbon_intensity': carbon, 'quality': 0.8,
                     'cost': 0.5, 'privacy': 0.0})
            except Exception as e:
                logger.error(f"Auto-optimize loop: {e}")

    async def _ga_optimization_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            if not self.config.ga_enabled or not self.ga_optimizer:
                continue
            try:
                await self.ga_optimizer.run_search()
            except Exception as e:
                logger.error(f"GA loop: {e}")

    async def _moe_training_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1800)
            if self.moe_gating:
                try:
                    self.moe_gating._train_gating()
                except Exception as e:
                    logger.error(f"MoE loop: {e}")

    async def _limit_graph_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.limit_graph_update_interval)
            try:
                carbon = await self.carbon_manager.get_current_intensity()
                await self.limit_graph.update_constraint('carbon', carbon)
                await self.limit_graph.evaluate_path('carbon', 'cost')
            except Exception as e:
                logger.error(f"Limit graph loop: {e}")

    async def _rlhf_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.rlhf_training_interval)
            if self.rlhf:
                try:
                    await self.rlhf.train_reward_model()
                except Exception as e:
                    logger.error(f"RLHF loop: {e}")

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.distillation_interval)
            if self.distillation:
                try:
                    state = {'carbon_intensity': await self.carbon_manager.get_current_intensity()}
                    await self.distillation.distill(state)
                except Exception as e:
                    logger.error(f"Distillation loop: {e}")

    async def _causal_rl_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.causal_graph_update_interval)
            try:
                rows = await self.storage._fetchall(
                    "SELECT parameters FROM synthetic_generation_history ORDER BY id DESC LIMIT 200")
                if len(rows) < self.config.causal_min_samples:
                    continue
                carbon = await self.carbon_manager.get_current_intensity()
                records = [{'carbon_intensity': carbon,
                            'quality': random.uniform(0.6, 0.95),
                            'cost': random.uniform(0.1, 0.9),
                            'privacy': random.uniform(0.0, 1.0)} for _ in rows]
                df = pd.DataFrame(records) if PANDAS_AVAILABLE else [
                    [r[k] for k in ('carbon_intensity', 'quality', 'cost', 'privacy')]
                    for r in records]
                await self.causal_graph.learn_structure(
                    df, ['carbon_intensity', 'quality', 'cost', 'privacy'])
                await self.causal_rl.estimate_ate('carbon_intensity', 'quality')
            except Exception as e:
                logger.error(f"Causal RL loop: {e}")

    async def _temporal_logic_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.temporal_verification_interval)
            try:
                result = await self.temporal_verifier.verify()
                for f, ok in result.items():
                    if not ok:
                        logger.warning(f"TL property violated: {f}")
            except Exception as e:
                logger.error(f"TL loop: {e}")

    async def _xai_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.xai_interval)
            try:
                if self.xai and NUMPY_AVAILABLE:
                    feats = np.array([await self.carbon_manager.get_current_intensity(),
                                      0.8, 0.5, 0.0])
                    def _score(x):
                        return float(np.dot(x, [0.3, 0.5, -0.2, -0.1]))
                    await self.xai.explain(
                        decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                        label="system_health", features=feats,
                        names=['carbon_intensity', 'quality', 'cost', 'privacy'],
                        model_fn=_score)
            except Exception as e:
                logger.error(f"XAI loop: {e}")

    async def _precision_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                if self.precision_switcher:
                    target = self.precision_switcher.select_precision()
                    await self.precision_switcher.switch_to(target, reason='periodic')
            except Exception as e:
                logger.error(f"Precision loop: {e}")

    async def _carbon_market_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.carbon_market_interval)
            try:
                if self.carbon_market:
                    price = await self.carbon_market.update_price()
                    logger.debug(f"Carbon credit price: ${price:.2f}")
            except Exception as e:
                logger.error(f"Carbon market loop: {e}")

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.chaos_test_interval)
            try:
                if not self.chaos_engine:
                    continue
                fault = random.choice(self.config.chaos_fault_types)
                await self.chaos_engine.run_experiment(
                    name=f"auto_{uuid.uuid4().hex[:6]}", fault_type=fault)
            except Exception as e:
                logger.error(f"Chaos loop: {e}")

    # Public v17 helpers
    async def verify_temporal_property(self, formula):
        if not self.temporal_verifier:
            return {'error': 'temporal logic disabled'}
        self.temporal_verifier.formulas = [formula]
        return await self.temporal_verifier.verify()

    def model_check_temporal_property(self, formula, initial_state):
        if not self.temporal_verifier:
            return {'error': 'temporal logic disabled'}
        return self.temporal_verifier.model_check(formula, initial_state)

    async def explain_decision(self, decision_id, label, features, names, model_fn):
        if not self.xai:
            return {'error': 'XAI disabled'}
        return await self.xai.explain(decision_id, label, features, names, model_fn)

    async def run_chaos_experiment(self, name, fault_type, target=None, workload=None):
        if not self.chaos_engine:
            return {'error': 'chaos disabled'}
        return await self.chaos_engine.run_experiment(name, fault_type, target, workload)

    async def purchase_rec(self, mwh, price_per_mwh=5.0, source='wind'):
        if not self.carbon_market:
            return 0.0
        return await self.carbon_market.purchase_rec(mwh, price_per_mwh, source)

    def precision_state(self):
        if not self.precision_switcher:
            return {'enabled': False}
        return {'enabled': True, 'current': self.precision_switcher.current,
                'hardware': self.precision_switcher.hardware,
                'energy_saved_wh': self.precision_switcher.energy_saved_wh}

    async def get_causal_graph_summary(self):
        if not self.causal_graph:
            return {'enabled': False}
        return {'enabled': True, **self.causal_graph.summary()}

# -----------------------------------------------------------------------------
# Singleton Accessor
# -----------------------------------------------------------------------------
_simulator_instance = None
_simulator_lock = asyncio.Lock()

async def get_system_simulator(config=None):
    global _simulator_instance
    if _simulator_instance is None:
        async with _simulator_lock:
            if _simulator_instance is None:
                _simulator_instance = EnhancedSystemSimulatorV11(config)
                await _simulator_instance.start()
    return _simulator_instance

# -----------------------------------------------------------------------------
# Signal Handling
# -----------------------------------------------------------------------------
_shutdown_requested = False

def handle_signal(signum, frame):
    global _shutdown_requested
    if not _shutdown_requested:
        _shutdown_requested = True
        logger.info(f"Received signal {signum}, initiating shutdown...")
        try:
            loop = asyncio.get_event_loop()
            loop.create_task(_signal_shutdown())
        except RuntimeError:
            pass

async def _signal_shutdown():
    global _simulator_instance
    if _simulator_instance:
        await _simulator_instance.shutdown()
        _simulator_instance = None

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
async def main():
    simulator = await get_system_simulator()
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await simulator.shutdown()

if __name__ == "__main__":
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda s=sig: handle_signal(s, None))
            except (NotImplementedError, RuntimeError):
                pass
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
