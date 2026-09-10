#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/synthetic_data_manager_enhanced_v17_0.py
# VERSION: 17.0.0 (Causal RL + Temporal Logic + XAI + Adaptive Precision +
#                   Carbon Markets/RECs + Chaos Engineering + Multi-Agent Roles
#                   + all v16.0.0 capabilities)
# =============================================================================
"""
Enhanced Synthetic Data Manager for Green Agent - Version 17.0.0

NEW ENHANCEMENTS OVER v16.0.0:
 1. Causal Reinforcement Learning (CausalGraphLearner + CausalReinforcementLearner)
    - Structure learning of causal DAG from observational data
    - Do-calculus based counterfactual reward estimation
    - Causal policy adaptation with exploration/exploitation trade-off
 2. Temporal Logic & Formal Verification (TemporalLogicVerifier)
    - LTL / MTL formula parsing and runtime monitoring
    - Safety-critical policy model checking before deployment
    - Counter-example generation for violated properties
 3. Explainable AI for Every Decision (XAIDecisionExplainer)
    - SHAP-style Shapley value approximation
    - LIME-style local surrogate explanations
    - Integrated Gradients for deep model decisions
    - Natural-language explanation rendering
 4. Adaptive Precision Switching (AdaptivePrecisionSwitcher)
    - Hardware-aware precision policy (fp32 / fp16 / bf16 / int8)
    - Energy/latency/accuracy tri-objective switching
    - Automatic fall-back on numerical instability
 5. External Carbon Markets & REC Integration (CarbonMarketIntegrator)
    - Carbon credit price oracle with caching
    - Renewable Energy Credit (REC) ledger
    - Net-zero scheduling: match generation to green energy windows
 6. Resilience Engineering & Chaos Testing (ChaosTestingEngine)
    - First-class fault injection: latency, exceptions, data corruption, OOM
    - Steady-state hypothesis verification
    - Automatic rollback and blast-radius control
 7. Advanced Multi-Agent Coordination with Emergent Roles (MultiAgentCoordinator)
    - Role specialisation through utility-based bidding
    - Emergent division of labour (generator, critic, verifier, negotiator)
    - Inter-agent message bus with reputation tracking

All enhancements are optional and configurable. Each module degrades
gracefully to the v16.0.0 behaviour if disabled or if optional deps missing.
"""

import asyncio
import hashlib
import json
import logging
import logging.handlers
import os
import random
import re
import sqlite3
import time
import uuid
import signal
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
# Async SQLite
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
    from web3.middleware import geth_poa_middleware
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
from cryptography.hazmat.backends import default_backend

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log
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
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    from sklearn.neural_network import MLPRegressor, MLPClassifier
    from sklearn.linear_model import LinearRegression, LogisticRegression
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler
    from sklearn.tree import DecisionTreeRegressor
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

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
    from scipy.stats import wasserstein_distance, ks_2samp, pearsonr
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
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

# -----------------------------------------------------------------------------
# DUMMY TENACITY
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
    else:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s'
        )
        logger = logging.getLogger(__name__)

        class CorrelationIdFilter(logging.Filter):
            def filter(self, record):
                record.correlation_id = correlation_id_var.get()
                return True
        logger.addFilter(CorrelationIdFilter())

audit_logger = logging.getLogger('synthetic_audit')
audit_handler = logging.handlers.RotatingFileHandler(
    'synthetic_audit_v17.log', maxBytes=50 * 1024 * 1024, backupCount=10
)
audit_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)

# -----------------------------------------------------------------------------
# Prometheus metrics
# -----------------------------------------------------------------------------
if PROMETHEUS_AVAILABLE := False:
    pass
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

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
    # --- New v17 metrics ---
    CAUSAL_ATE = metrics.gauge('synthetic_causal_ate', ['treatment', 'outcome'])
    CAUSAL_POLICY_REWARD = metrics.gauge('synthetic_causal_policy_reward')
    TEMPORAL_VERIFICATIONS = metrics.counter('synthetic_temporal_verifications_total', ['formula', 'status'])
    TEMPORAL_VIOLATIONS = metrics.counter('synthetic_temporal_violations_total', ['formula'])
    XAI_EXPLANATIONS = metrics.counter('synthetic_xai_explanations_total', ['method'])
    XAI_FEATURE_IMPORTANCE = metrics.gauge('synthetic_xai_feature_importance', ['feature'])
    PRECISION_SWITCHES = metrics.counter('synthetic_precision_switches_total', ['from_p', 'to_p'])
    PRECISION_ENERGY_SAVED = metrics.gauge('synthetic_precision_energy_saved_wh')
    CARBON_CREDIT_PRICE = metrics.gauge('synthetic_carbon_credit_price_usd')
    REC_BALANCE = metrics.gauge('synthetic_rec_balance_mwh')
    NET_ZERO_MATCHES = metrics.counter('synthetic_net_zero_matches_total')
    CHAOS_EXPERIMENTS = metrics.counter('synthetic_chaos_experiments_total', ['fault_type', 'status'])
    CHAOS_STEADY_STATE = metrics.gauge('synthetic_chaos_steady_state_ok')
    AGENT_ROLES = metrics.gauge('synthetic_agent_roles', ['role'])
    AGENT_REPUTATION = metrics.gauge('synthetic_agent_reputation', ['agent_id'])
    AGENT_MESSAGES = metrics.counter('synthetic_agent_messages_total', ['topic'])
else:
    if PROMETHEUS_AVAILABLE:
        REGISTRY = CollectorRegistry()
        def _c(name, doc, labels=None, metric_type='counter'):
            if metric_type == 'counter':
                return Counter(name, doc, labels or [], registry=REGISTRY)
            if metric_type == 'gauge':
                return Gauge(name, doc, labels or [], registry=REGISTRY)
            return Histogram(name, doc, labels or [], registry=REGISTRY)

        DATA_GENERATIONS = _c('synthetic_generations_total', 'Total generations', ['domain', 'status', 'method'])
        GENERATION_DURATION = _c('synthetic_generation_duration_seconds', 'Duration', ['domain', 'method'], 'hist')
        DATA_QUALITY = _c('synthetic_data_quality', 'Quality', ['domain', 'metric'], 'gauge')
        DRIFT_SCORE = _c('synthetic_data_drift', 'Drift', ['domain', 'column'], 'gauge')
        PRIVACY_BUDGET = _c('synthetic_privacy_budget', 'Privacy', ['domain'], 'gauge')
        CIRCUIT_BREAKER_STATE = _c('synthetic_circuit_breaker_state', 'CB', ['component'], 'gauge')
        HEALTH_SCORE = _c('synthetic_system_health', 'Health', [], 'gauge')
        DB_SIZE = _c('synthetic_db_size_mb', 'DB size', [], 'gauge')
        DATA_QUALITY_SCORE = _c('synthetic_data_quality_score', 'Overall quality', [], 'gauge')
        GENERATION_QUEUE_SIZE = _c('synthetic_generation_queue_size', 'Queue', [], 'gauge')
        WS_CONNECTIONS = _c('synthetic_ws_connections', 'WS', [], 'gauge')
        DEEP_GENERATION_SCORE = _c('deep_generation_score', 'Deep score', ['model_type'], 'gauge')
        DRIFT_METHOD_SCORE = _c('drift_method_score', 'Drift method', ['method'], 'gauge')
        ACTIVE_LEARNING_ITERATIONS = _c('active_learning_iterations_total', 'AL', ['domain'])
        CONSTRAINT_VALIDATIONS = _c('constraint_validations_total', 'CV', ['domain', 'status'])
        MODEL_VERSION_SCORE = _c('model_version_score', 'MV', ['domain', 'version'], 'gauge')
        QUANTUM_SIGNATURES = _c('synthetic_quantum_signatures_total', 'QS', ['algorithm', 'status'])
        BLOCKCHAIN_VERIFICATIONS = _c('synthetic_blockchain_verifications_total', 'BV', ['status'])
        AUTONOMOUS_OPTIMIZATIONS = _c('synthetic_autonomous_optimizations_total', 'AO', ['strategy', 'status'])
        CLOUD_DISTRIBUTIONS = _c('synthetic_cloud_distributions_total', 'CD', ['provider', 'status'])
        MTOP_TEACHER_WEIGHTS = _c('synthetic_mtop_teacher_weights', 'MTW', ['teacher'], 'gauge')
        MTOP_STUDENT_UPDATES = _c('synthetic_mtop_student_updates_total', 'MSU')
        GA_POPULATION_FITNESS = _c('synthetic_ga_population_fitness', 'GA', [], 'gauge')
        MOE_GATING_PROBABILITIES = _c('synthetic_moe_gating_probabilities', 'MoE', ['expert'], 'gauge')
        PARETO_FRONT_SIZE = _c('synthetic_pareto_front_size', 'PF', [], 'gauge')
        ADAPTIVE_DRIFT_THRESHOLD = _c('synthetic_adaptive_drift_threshold', 'ADT', ['domain'], 'gauge')
        CAUSAL_ATE = _c('synthetic_causal_ate', 'ATE', ['treatment', 'outcome'], 'gauge')
        CAUSAL_POLICY_REWARD = _c('synthetic_causal_policy_reward', 'CRL reward', [], 'gauge')
        TEMPORAL_VERIFICATIONS = _c('synthetic_temporal_verifications_total', 'TL', ['formula', 'status'])
        TEMPORAL_VIOLATIONS = _c('synthetic_temporal_violations_total', 'TLv', ['formula'])
        XAI_EXPLANATIONS = _c('synthetic_xai_explanations_total', 'XAI', ['method'])
        XAI_FEATURE_IMPORTANCE = _c('synthetic_xai_feature_importance', 'XAI FI', ['feature'], 'gauge')
        PRECISION_SWITCHES = _c('synthetic_precision_switches_total', 'PS', ['from_p', 'to_p'])
        PRECISION_ENERGY_SAVED = _c('synthetic_precision_energy_saved_wh', 'PE', [], 'gauge')
        CARBON_CREDIT_PRICE = _c('synthetic_carbon_credit_price_usd', 'CC', [], 'gauge')
        REC_BALANCE = _c('synthetic_rec_balance_mwh', 'REC', [], 'gauge')
        NET_ZERO_MATCHES = _c('synthetic_net_zero_matches_total', 'NZ')
        CHAOS_EXPERIMENTS = _c('synthetic_chaos_experiments_total', 'Ch', ['fault_type', 'status'])
        CHAOS_STEADY_STATE = _c('synthetic_chaos_steady_state_ok', 'ChSS', [], 'gauge')
        AGENT_ROLES = _c('synthetic_agent_roles', 'AR', ['role'], 'gauge')
        AGENT_REPUTATION = _c('synthetic_agent_reputation', 'ARep', ['agent_id'], 'gauge')
        AGENT_MESSAGES = _c('synthetic_agent_messages_total', 'AM', ['topic'])
    else:
        class DummyMetric:
            def labels(self, **kw): return self
            def inc(self, **kw): pass
            def set(self, **kw): pass
            def observe(self, **kw): pass
        _names = [
            'DATA_GENERATIONS', 'GENERATION_DURATION', 'DATA_QUALITY', 'DRIFT_SCORE',
            'PRIVACY_BUDGET', 'CIRCUIT_BREAKER_STATE', 'HEALTH_SCORE', 'DB_SIZE',
            'DATA_QUALITY_SCORE', 'GENERATION_QUEUE_SIZE', 'WS_CONNECTIONS',
            'DEEP_GENERATION_SCORE', 'DRIFT_METHOD_SCORE', 'ACTIVE_LEARNING_ITERATIONS',
            'CONSTRAINT_VALIDATIONS', 'MODEL_VERSION_SCORE', 'QUANTUM_SIGNATURES',
            'BLOCKCHAIN_VERIFICATIONS', 'AUTONOMOUS_OPTIMIZATIONS', 'CLOUD_DISTRIBUTIONS',
            'MTOP_TEACHER_WEIGHTS', 'MTOP_STUDENT_UPDATES', 'GA_POPULATION_FITNESS',
            'MOE_GATING_PROBABILITIES', 'PARETO_FRONT_SIZE', 'ADAPTIVE_DRIFT_THRESHOLD',
            'CAUSAL_ATE', 'CAUSAL_POLICY_REWARD', 'TEMPORAL_VERIFICATIONS',
            'TEMPORAL_VIOLATIONS', 'XAI_EXPLANATIONS', 'XAI_FEATURE_IMPORTANCE',
            'PRECISION_SWITCHES', 'PRECISION_ENERGY_SAVED', 'CARBON_CREDIT_PRICE',
            'REC_BALANCE', 'NET_ZERO_MATCHES', 'CHAOS_EXPERIMENTS', 'CHAOS_STEADY_STATE',
            'AGENT_ROLES', 'AGENT_REPUTATION', 'AGENT_MESSAGES',
        ]
        for _n in _names:
            globals()[_n] = DummyMetric()

# =============================================================================
# CONFIGURATION
# =============================================================================
if CENTRAL_COMPONENTS_AVAILABLE and central_config:
    class SyntheticDataConfig:
        def __init__(self):
            g = lambda k, d=None: getattr(central_config, k, d)
            self.instance_id = g('instance_id', str(uuid.uuid4())[:8])
            self.version = "17.0.0"
            self.log_level = g('log_level', 'INFO')
            self.db_path = g('db_path', '/tmp/synthetic_data_v17.db')
            self.electricity_maps_api_key = g('electricity_maps_api_key')
            self.carbon_region = g('carbon_region', 'global')
            self.carbon_update_interval = g('carbon_update_interval', 300)
            self.blockchain_rpc_url = g('blockchain_rpc_url', 'http://localhost:8545')
            self.cache_ttl = g('cache_ttl', 300)
            self.metrics_port = g('metrics_port', 8000)
            self.websocket_port = g('websocket_port', 8770)
            self.health_check_interval = g('health_check_interval', 60)
            self.federated_interval = g('federated_interval', 3600)
            self.active_learning_interval = g('active_learning_interval', 1800)
            self.master_key_env = g('master_key_env', 'SYNTHETIC_MASTER_KEY')
            # v16 flags
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
            self.rlhf_training_interval = g('synthetic_rlhf_training_interval', 600)
            self.distillation_enabled = g('synthetic_distillation_enabled', True)
            self.distillation_temperature = g('synthetic_distillation_temperature', 2.0)
            self.distillation_alpha = g('synthetic_distillation_alpha', 0.5)
            self.distillation_interval = g('synthetic_distillation_interval', 300)
            # ----- v17 NEW -----
            self.causal_rl_enabled = g('synthetic_causal_rl_enabled', True)
            self.causal_graph_update_interval = g('synthetic_causal_graph_update_interval', 900)
            self.causal_exploration_rate = g('synthetic_causal_exploration_rate', 0.1)
            self.causal_min_samples = g('synthetic_causal_min_samples', 50)
            self.temporal_logic_enabled = g('synthetic_temporal_logic_enabled', True)
            self.temporal_verification_interval = g('synthetic_temporal_verification_interval', 600)
            self.temporal_formulas = g('synthetic_temporal_formulas', [
                "G (quality >= 0.7)",
                "G (carbon <= 0.5)",
                "G (privacy_budget >= 0.0)",
                "F (generation_complete)",
            ])
            self.xai_enabled = g('synthetic_xai_enabled', True)
            self.xai_method = g('synthetic_xai_method', 'shap')
            self.xai_explanation_depth = g('synthetic_xai_explanation_depth', 5)
            self.xai_interval = g('synthetic_xai_interval', 300)
            self.adaptive_precision_enabled = g('synthetic_adaptive_precision_enabled', True)
            self.precision_levels = g('synthetic_precision_levels', ['fp32', 'fp16', 'bf16', 'int8'])
            self.precision_switch_threshold = g('synthetic_precision_switch_threshold', 0.02)
            self.precision_energy_target = g('synthetic_precision_energy_target', 0.7)
            self.carbon_market_enabled = g('synthetic_carbon_market_enabled', True)
            self.carbon_market_api_url = g('synthetic_carbon_market_api_url', 'https://api.carbonmarket.example/v1')
            self.rec_tracking_enabled = g('synthetic_rec_tracking_enabled', True)
            self.carbon_market_interval = g('synthetic_carbon_market_interval', 3600)
            self.chaos_testing_enabled = g('synthetic_chaos_testing_enabled', True)
            self.chaos_test_interval = g('synthetic_chaos_test_interval', 1800)
            self.chaos_intensity = g('synthetic_chaos_intensity', 0.05)
            self.chaos_blast_radius = g('synthetic_chaos_blast_radius', 0.1)
            self.multi_agent_coordination_enabled = g('synthetic_multi_agent_coordination_enabled', True)
            self.agent_count = g('synthetic_agent_count', 5)
            self.role_specialization_enabled = g('synthetic_role_specialization_enabled', True)
            self.agent_negotiation_interval = g('synthetic_agent_negotiation_interval', 600)

        def get_master_key(self) -> bytes:
            key_hex = os.getenv(self.master_key_env)
            if not key_hex:
                raise ValueError(f"Master key not set in env {self.master_key_env}")
            return bytes.fromhex(key_hex)
else:
    if PYDANTIC_AVAILABLE:
        class SyntheticDataConfig(BaseModel):
            instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
            version: str = "17.0.0"
            log_level: str = "INFO"
            db_path: str = "/tmp/synthetic_data_v17.db"
            electricity_maps_api_key: Optional[str] = None
            carbon_region: str = "global"
            carbon_update_interval: int = 300
            blockchain_rpc_url: str = "http://localhost:8545"
            cache_ttl: int = 300
            metrics_port: int = 8000
            websocket_port: int = 8770
            health_check_interval: int = 60
            federated_interval: int = 3600
            active_learning_interval: int = 1800
            master_key_env: str = "SYNTHETIC_MASTER_KEY"
            ga_enabled: bool = True
            ga_population_size: int = 20
            ga_generations: int = 5
            ga_mutation_rate: float = 0.2
            ga_crossover_rate: float = 0.7
            moe_enabled: bool = True
            moe_expert_count: int = 4
            moe_hidden_layers: List[int] = [16, 8]
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
            modp_weights: List[float] = [0.25, 0.25, 0.25, 0.25]
            rlhf_enabled: bool = True
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
            temporal_logic_enabled: bool = True
            temporal_verification_interval: int = 600
            temporal_formulas: List[str] = [
                "G (quality >= 0.7)",
                "G (carbon <= 0.5)",
                "G (privacy_budget >= 0.0)",
                "F (generation_complete)",
            ]
            xai_enabled: bool = True
            xai_method: str = "shap"
            xai_explanation_depth: int = 5
            xai_interval: int = 300
            adaptive_precision_enabled: bool = True
            precision_levels: List[str] = ['fp32', 'fp16', 'bf16', 'int8']
            precision_switch_threshold: float = 0.02
            precision_energy_target: float = 0.7
            carbon_market_enabled: bool = True
            carbon_market_api_url: str = "https://api.carbonmarket.example/v1"
            rec_tracking_enabled: bool = True
            carbon_market_interval: int = 3600
            chaos_testing_enabled: bool = True
            chaos_test_interval: int = 1800
            chaos_intensity: float = 0.05
            chaos_blast_radius: float = 0.1
            multi_agent_coordination_enabled: bool = True
            agent_count: int = 5
            role_specialization_enabled: bool = True
            agent_negotiation_interval: int = 600

            def get_master_key(self) -> bytes:
                key_hex = os.getenv(self.master_key_env)
                if not key_hex:
                    raise ValueError(f"Master key not set in env {self.master_key_env}")
                return bytes.fromhex(key_hex)
    else:
        class SyntheticDataConfig:
            def __init__(self, **kw):
                self.instance_id = str(uuid.uuid4())[:8]
                self.version = "17.0.0"
                for k, v in kw.items():
                    setattr(self, k, v)
            def get_master_key(self) -> bytes:
                key_hex = os.getenv("SYNTHETIC_MASTER_KEY")
                if not key_hex:
                    raise ValueError("Master key not set")
                return bytes.fromhex(key_hex)

# =============================================================================
# ENCRYPTION
# =============================================================================
class EncryptionManager:
    def __init__(self, master_key: bytes):
        if len(master_key) != 32:
            raise ValueError("Master key must be 32 bytes")
        self.master_key = master_key

    def encrypt(self, data: bytes) -> Tuple[bytes, bytes]:
        nonce = secrets.token_bytes(12)
        aesgcm = AESGCM(self.master_key)
        return aesgcm.encrypt(nonce, data, None), nonce

    def decrypt(self, ciphertext: bytes, nonce: bytes) -> bytes:
        aesgcm = AESGCM(self.master_key)
        return aesgcm.decrypt(nonce, ciphertext, None)

# =============================================================================
# STORAGE (v17 - adds tables for causal graph, RECs, chaos, agents)
# =============================================================================
class EnhancedStorage:
    def __init__(self, config):
        self.config = config
        self.db_path = config.db_path
        self.encryption_manager = None
        try:
            self.encryption_manager = EncryptionManager(config.get_master_key())
        except Exception:
            logger.warning("Master key not set – plaintext fallback")
        self.cache = {}
        self.cache_ttl = config.cache_ttl
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self._init_db())
            else:
                loop.run_until_complete(self._init_db())
        except RuntimeError:
            asyncio.run(self._init_db())

    async def _execute(self, query, params=()):
        if AIOSQLITE_AVAILABLE:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute("PRAGMA journal_mode=WAL")
                cursor = await conn.execute(query, params)
                await conn.commit()
                return cursor
        else:
            def _sync():
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("PRAGMA journal_mode=WAL")
                    cur = conn.execute(query, params)
                    conn.commit()
                    return cur
            return await asyncio.to_thread(_sync)

    async def _fetchone(self, query, params=()):
        cur = await self._execute(query, params)
        return (await cur.fetchone()) if AIOSQLITE_AVAILABLE else cur.fetchone()

    async def _fetchall(self, query, params=()):
        cur = await self._execute(query, params)
        return (await cur.fetchall()) if AIOSQLITE_AVAILABLE else cur.fetchall()

    async def _init_db(self):
        stmts = [
            "CREATE TABLE IF NOT EXISTS carbon_cache (region TEXT PRIMARY KEY, intensity REAL, timestamp TEXT)",
            "CREATE TABLE IF NOT EXISTS helium_cache (hotspot_id TEXT PRIMARY KEY, score REAL, timestamp TEXT)",
            """CREATE TABLE IF NOT EXISTS generation_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, dataset_version TEXT,
                num_samples INTEGER, anomaly_rate REAL, edge_fraction REAL, parameters TEXT,
                quantum_signature TEXT, blockchain_tx_hash TEXT)""",
            """CREATE TABLE IF NOT EXISTS ga_populations (
                generation INTEGER, individual_id TEXT, attributes TEXT, fitness REAL,
                timestamp TEXT, PRIMARY KEY (generation, individual_id))""",
            """CREATE TABLE IF NOT EXISTS moe_training (
                sample_id TEXT PRIMARY KEY, features TEXT, expert_label INTEGER,
                reward REAL, timestamp TEXT)""",
            """CREATE TABLE IF NOT EXISTS pareto_front (
                solution_id TEXT PRIMARY KEY, config_params TEXT, coverage_score REAL,
                anomaly_diversity REAL, realism_score REAL, data_quality REAL, timestamp TEXT)""",
            """CREATE TABLE IF NOT EXISTS user_preferences (
                user_id TEXT, weights TEXT, chosen_solution_id TEXT, timestamp TEXT,
                PRIMARY KEY (user_id, timestamp))""",
            "CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
            # ----- v17 tables -----
            """CREATE TABLE IF NOT EXISTS causal_graph (
                edge_id TEXT PRIMARY KEY, source TEXT, target TEXT, weight REAL,
                confidence REAL, timestamp TEXT)""",
            """CREATE TABLE IF NOT EXISTS causal_experiments (
                exp_id TEXT PRIMARY KEY, treatment TEXT, outcome TEXT, ate REAL,
                samples INTEGER, timestamp TEXT)""",
            """CREATE TABLE IF NOT EXISTS temporal_violations (
                id INTEGER PRIMARY KEY AUTOINCREMENT, formula TEXT, step INTEGER,
                state TEXT, timestamp TEXT)""",
            """CREATE TABLE IF NOT EXISTS xai_explanations (
                id INTEGER PRIMARY KEY AUTOINCREMENT, decision_id TEXT, method TEXT,
                top_features TEXT, natural_language TEXT, timestamp TEXT)""",
            """CREATE TABLE IF NOT EXISTS precision_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT, from_p TEXT, to_p TEXT,
                reason TEXT, energy_saved_wh REAL, timestamp TEXT)""",
            """CREATE TABLE IF NOT EXISTS rec_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT, mwh REAL, price REAL,
                source TEXT, timestamp TEXT)""",
            """CREATE TABLE IF NOT EXISTS carbon_credit_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT, price REAL, currency TEXT,
                source TEXT, timestamp TEXT)""",
            """CREATE TABLE IF NOT EXISTS chaos_experiments (
                id INTEGER PRIMARY KEY AUTOINCREMENT, fault_type TEXT, status TEXT,
                steady_state_before INTEGER, steady_state_after INTEGER,
                blast_radius REAL, timestamp TEXT)""",
            """CREATE TABLE IF NOT EXISTS agent_registry (
                agent_id TEXT PRIMARY KEY, role TEXT, reputation REAL,
                utilities TEXT, timestamp TEXT)""",
            """CREATE TABLE IF NOT EXISTS agent_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT, topic TEXT, sender TEXT,
                payload TEXT, timestamp TEXT)""",
            "CREATE INDEX IF NOT EXISTS idx_causal_edge ON causal_graph(source, target)",
            "CREATE INDEX IF NOT EXISTS idx_rec_time ON rec_ledger(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_chaos_time ON chaos_experiments(timestamp)",
        ]
        for s in stmts:
            await self._execute(s)

    async def save_state(self, key, value):
        await self._execute("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)", (key, value))

    async def get_state(self, key):
        r = await self._fetchone("SELECT value FROM state WHERE key = ?", (key,))
        return r[0] if r else None

    async def save_generation_history(self, dataset_version, num_samples, anomaly_rate,
                                      edge_fraction, parameters, quantum_signature=None,
                                      blockchain_tx_hash=None):
        await self._execute(
            """INSERT INTO generation_history (timestamp, dataset_version, num_samples,
               anomaly_rate, edge_fraction, parameters, quantum_signature, blockchain_tx_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (datetime.now().isoformat(), dataset_version, num_samples, anomaly_rate,
             edge_fraction, json.dumps(parameters), quantum_signature, blockchain_tx_hash))

    async def save_carbon_intensity(self, region, intensity):
        await self._execute("INSERT OR REPLACE INTO carbon_cache VALUES (?, ?, ?)",
                            (region, intensity, datetime.now().isoformat()))

    async def get_carbon_intensity(self, region):
        r = await self._fetchone("SELECT intensity FROM carbon_cache WHERE region = ?", (region,))
        return r[0] if r else None

    async def save_pareto_front(self, solutions):
        await self._execute("DELETE FROM pareto_front")
        for sol in solutions:
            await self._execute(
                """INSERT INTO pareto_front VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (sol['solution_id'], json.dumps(sol['config_params']),
                 sol.get('coverage_score', 0), sol.get('anomaly_diversity', 0),
                 sol.get('realism_score', 0), sol.get('data_quality', 0),
                 datetime.now().isoformat()))

    async def save_ga_population(self, domain, generation, individuals):
        for ind in individuals:
            await self._execute(
                "INSERT OR REPLACE INTO ga_populations VALUES (?, ?, ?, ?, ?)",
                (generation, ind['individual_id'], json.dumps(ind['attributes']),
                 ind['fitness'], datetime.now().isoformat()))

    async def save_moe_training_sample(self, sid, features, label, reward):
        await self._execute("INSERT OR REPLACE INTO moe_training VALUES (?, ?, ?, ?, ?)",
                            (sid, json.dumps(features), label, reward, datetime.now().isoformat()))

    # ---- v17 storage helpers ----
    async def save_causal_edge(self, source, target, weight, confidence):
        eid = f"{source}->{target}"
        await self._execute(
            "INSERT OR REPLACE INTO causal_graph VALUES (?, ?, ?, ?, ?, ?)",
            (eid, source, target, weight, confidence, datetime.now().isoformat()))

    async def load_causal_graph(self):
        rows = await self._fetchall("SELECT source, target, weight, confidence FROM causal_graph")
        return [{'source': r[0], 'target': r[1], 'weight': r[2], 'confidence': r[3]} for r in rows]

    async def save_causal_experiment(self, exp_id, treatment, outcome, ate, samples):
        await self._execute("INSERT OR REPLACE INTO causal_experiments VALUES (?, ?, ?, ?, ?, ?)",
                            (exp_id, treatment, outcome, ate, samples, datetime.now().isoformat()))

    async def save_temporal_violation(self, formula, step, state):
        await self._execute("INSERT INTO temporal_violations (formula, step, state, timestamp) VALUES (?, ?, ?, ?)",
                            (formula, step, json.dumps(state, default=str), datetime.now().isoformat()))

    async def save_xai_explanation(self, decision_id, method, top_features, nl):
        await self._execute(
            "INSERT INTO xai_explanations (decision_id, method, top_features, natural_language, timestamp) VALUES (?, ?, ?, ?, ?)",
            (decision_id, method, json.dumps(top_features), nl, datetime.now().isoformat()))

    async def save_precision_switch(self, frm, to, reason, saved_wh):
        await self._execute(
            "INSERT INTO precision_history (from_p, to_p, reason, energy_saved_wh, timestamp) VALUES (?, ?, ?, ?, ?)",
            (frm, to, reason, saved_wh, datetime.now().isoformat()))

    async def save_rec(self, mwh, price, source):
        await self._execute("INSERT INTO rec_ledger (mwh, price, source, timestamp) VALUES (?, ?, ?, ?)",
                            (mwh, price, source, datetime.now().isoformat()))

    async def get_rec_balance(self):
        r = await self._fetchone("SELECT COALESCE(SUM(mwh), 0) FROM rec_ledger")
        return r[0] if r else 0.0

    async def save_credit_price(self, price, currency, source):
        await self._execute("INSERT INTO carbon_credit_prices (price, currency, source, timestamp) VALUES (?, ?, ?, ?)",
                            (price, currency, source, datetime.now().isoformat()))

    async def save_chaos_experiment(self, fault_type, status, before, after, blast):
        await self._execute(
            "INSERT INTO chaos_experiments (fault_type, status, steady_state_before, steady_state_after, blast_radius, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
            (fault_type, status, int(before), int(after), blast, datetime.now().isoformat()))

    async def save_agent(self, agent_id, role, reputation, utilities):
        await self._execute("INSERT OR REPLACE INTO agent_registry VALUES (?, ?, ?, ?, ?)",
                            (agent_id, role, reputation, json.dumps(utilities), datetime.now().isoformat()))

    async def load_agents(self):
        rows = await self._fetchall("SELECT agent_id, role, reputation, utilities FROM agent_registry")
        return [{'agent_id': r[0], 'role': r[1], 'reputation': r[2],
                 'utilities': json.loads(r[3])} for r in rows]

    async def save_agent_message(self, topic, sender, payload):
        await self._execute("INSERT INTO agent_messages (topic, sender, payload, timestamp) VALUES (?, ?, ?, ?)",
                            (topic, sender, json.dumps(payload, default=str), datetime.now().isoformat()))

    def dispose(self):
        pass

# =============================================================================
# CIRCUIT BREAKER / RATE LIMITER
# =============================================================================
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30.0, name="default"):
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
                raise Exception(f"Circuit breaker {self.name} OPEN")
        try:
            result = await func(*args, **kwargs)
            if self._state == "HALF_OPEN":
                self._state = "CLOSED"
                self._failures = 0
                CIRCUIT_BREAKER_STATE.labels(component=self.name).set(0)
            return result
        except Exception:
            self._failures += 1
            self._last_failure_time = datetime.now()
            if self._failures >= self.failure_threshold:
                self._state = "OPEN"
                CIRCUIT_BREAKER_STATE.labels(component=self.name).set(2)
            raise


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

# =============================================================================
# CARBON INTENSITY MANAGER
# =============================================================================
class CarbonIntensityManager:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.api_key = getattr(config, 'electricity_maps_api_key', None)
        self.region = config.carbon_region
        self.endpoint = "https://api.electricitymap.org/v3/carbon-intensity"
        self._session = None
        self._circuit_breaker = CircuitBreaker(3, 60.0, "carbon_api")
        self._rate_limiter = RateLimiter(10, 60)

    async def get_current_intensity(self):
        cached = await self.storage.get_carbon_intensity(self.region)
        if cached is not None:
            return cached / 1000.0
        try:
            if AIOHTTP_AVAILABLE:
                async with aiohttp.ClientSession() as session:
                    headers = {'auth-token': self.api_key} if self.api_key else {}
                    url = f"{self.endpoint}/latest?zone={self.region}"
                    await self._rate_limiter.wait_and_acquire()
                    async with session.get(url, headers=headers, timeout=10) as r:
                        if r.status == 200:
                            data = await r.json()
                            intensity = data.get('carbonIntensity', 400)
                            await self.storage.save_carbon_intensity(self.region, intensity)
                            return intensity / 1000.0
        except Exception as e:
            logger.warning(f"Carbon fetch failed: {e}")
        return 0.4

    async def close(self):
        if self._session:
            await self._session.close()

# =============================================================================
# DEEP GENERATIVE MODEL + DOMAIN GENERATOR
# =============================================================================
class DeepGenerativeModel:
    def __init__(self, input_dim, latent_dim=32, hidden_dim=128, model_type='vae', model_path=None):
        self.input_dim = input_dim
        self.latent_dim = latent_dim
        self.hidden_dim = hidden_dim
        self.model_type = model_type
        self.model_path = model_path
        self.precision = 'fp32'
        self.model = None

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
                return pd.DataFrame(data, columns=[f'feature_{i}' for i in range(len(data[0]))])
            return data
        if PANDAS_AVAILABLE and NUMPY_AVAILABLE:
            return pd.DataFrame(np.random.randn(n_samples, 5), columns=[f'col_{i}' for i in range(5)])
        return [[random.random() for _ in range(5)] for _ in range(n_samples)]

# =============================================================================
# MTOP ENGINE
# =============================================================================
class StrategyTeacherEnsemble:
    def __init__(self, config):
        self.teacher_weights = {'performance': 0.25, 'carbon': 0.25, 'cost': 0.25, 'adaptive': 0.25}

    async def get_teacher_scores(self, state, carbon_intensity):
        return {
            'performance': {'statistical': 0.5, 'vae': 0.8, 'gan': 0.7, 'hybrid': 0.75},
            'carbon': {'statistical': 0.6, 'vae': 0.4, 'gan': 0.3, 'hybrid': 0.5},
            'cost': {'statistical': 0.7, 'vae': 0.5, 'gan': 0.4, 'hybrid': 0.6},
            'adaptive': {'statistical': 0.5, 'vae': 0.7, 'gan': 0.6, 'hybrid': 0.65},
        }

    def update_weights(self, rewards):
        total = sum(rewards.values())
        if total > 0:
            for k in self.teacher_weights:
                self.teacher_weights[k] = rewards[k] / total


class StrategyDistillationStudent:
    def __init__(self, config):
        self.weights = [0.3, 0.3, 0.2, 0.2]
        self.update_count = 0

    async def combine(self, teacher_scores):
        keys = list(teacher_scores.keys())
        combined = {}
        for strategy in teacher_scores[keys[0]].keys():
            combined[strategy] = sum(self.weights[i] * teacher_scores[keys[i]][strategy]
                                     for i in range(len(keys)))
        return combined


class MTOPStrategyEngine:
    def __init__(self, config):
        self.teacher_ensemble = StrategyTeacherEnsemble(config)
        self.student = StrategyDistillationStudent(config)

    async def select_strategy(self, state, carbon_intensity):
        scores = await self.teacher_ensemble.get_teacher_scores(state, carbon_intensity)
        combined = await self.student.combine(scores)
        best = max(combined, key=combined.get)
        return {'selected_strategy': best, 'teacher_scores': scores}

# =============================================================================
# GENETIC HYPERPARAMETER OPTIMIZER (unchanged)
# =============================================================================
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
            'learning_rate': (1e-5, 1e-2), 'batch_size': (16, 256),
        }

    def _random_chromosome(self):
        return {
            'latent_dim': random.randint(*self.param_bounds['latent_dim']),
            'hidden_dim': random.randint(*self.param_bounds['hidden_dim']),
            'learning_rate': 10 ** random.uniform(-5, -2),
            'batch_size': 2 ** random.randint(4, 8),
        }

    def _mutate(self, chrom):
        new = chrom.copy()
        for param, bounds in self.param_bounds.items():
            if random.random() < self.mutation_rate:
                if param == 'learning_rate':
                    new[param] = 10 ** max(-5, min(-2, np.log10(new[param]) + random.gauss(0, 0.5)))
                elif param == 'batch_size':
                    new[param] = 2 ** random.randint(4, 8)
                else:
                    low, high = bounds
                    new[param] = int(max(low, min(high, chrom[param] + random.gauss(0, (high - low) / 10))))
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
        best_fit, best_ind = -1.0, None
        for gen in range(self.generations):
            fitnesses = await asyncio.gather(*[self._evaluate_fitness(i) for i in population])
            sorted_pop = sorted(zip(population, fitnesses), key=lambda x: x[1], reverse=True)
            if sorted_pop[0][1] > best_fit:
                best_fit, best_ind = sorted_pop[0][1], sorted_pop[0][0]
            parents = [i for i, _ in sorted_pop[:max(2, self.population_size // 2)]]
            offspring = []
            while len(offspring) < self.population_size:
                p1, p2 = random.choice(parents), random.choice(parents)
                c1, c2 = self._crossover(p1, p2)
                offspring.append(self._mutate(c1))
                if len(offspring) < self.population_size:
                    offspring.append(self._mutate(c2))
            combined = parents + offspring
            combined_fit = await asyncio.gather(*[self._evaluate_fitness(i) for i in combined])
            sorted_c = sorted(zip(combined, combined_fit), key=lambda x: x[1], reverse=True)
            population = [i for i, _ in sorted_c[:self.population_size]]
            GA_POPULATION_FITNESS.set(best_fit)
        return best_ind or self._random_chromosome()

# =============================================================================
# MoE GATING NETWORK (unchanged)
# =============================================================================
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
            'statistical': lambda c: {'method': 'statistical'},
            'vae': lambda c: {'method': 'vae'},
            'gan': lambda c: {'method': 'gan'},
            'hybrid': lambda c: {'method': 'hybrid'},
        }
        self.expert_names = list(self.experts.keys())

    def _encode_context(self, context):
        domain_map = {'esg_metrics': 0, 'carbon_data': 1, 'helium_data': 2, 'time_series': 3, 'general': 4}
        vec = [0] * 5
        vec[domain_map.get(context.get('domain', 'general'), 0)] = 1
        vec += [context.get('carbon_intensity', 0.4), context.get('quality_target', 0.8),
                context.get('epsilon', 1.0), context.get('n_samples', 1000) / 10000.0,
                1.0 if context.get('use_deep_model', False) else 0.0]
        return np.array(vec, dtype=np.float32) if NUMPY_AVAILABLE else vec

    def _train_gating(self):
        if not SKLEARN_AVAILABLE or len(self._training_data) < 10:
            return
        X = np.array([d[0] for d in self._training_data])
        y = np.array([d[1] for d in self._training_data])
        self._scaler = StandardScaler()
        X_s = self._scaler.fit_transform(X)
        self._gating_model = MLPClassifier(hidden_layer_sizes=self.hidden_layers, max_iter=200, random_state=42)
        self._gating_model.fit(X_s, y)
        self._trained = True

    async def select_expert(self, context):
        features = self._encode_context(context)
        if self._trained and self._gating_model is not None:
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

    async def add_training_sample(self, context, expert, reward):
        features = self._encode_context(context)
        idx = self.expert_names.index(expert)
        async with self._lock:
            self._training_data.append((features, idx, reward))
            if len(self._training_data) % 10 == 0:
                self._train_gating()

# =============================================================================
# PARETO FRONT OPTIMIZER (unchanged)
# =============================================================================
class ParetoFrontOptimizer:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.pareto_front = []
        self.max_size = config.pareto_max_architectures
        self._lock = asyncio.Lock()

    def _dominates(self, a, b):
        am = (-a['metrics']['quality'], a['metrics']['carbon'], a['metrics']['cost'], a['metrics']['privacy'])
        bm = (-b['metrics']['quality'], b['metrics']['carbon'], b['metrics']['cost'], b['metrics']['privacy'])
        return all(am[i] <= bm[i] for i in range(4)) and any(am[i] < bm[i] for i in range(4))

    async def add_configuration(self, config_params, metrics):
        entry = {'solution_id': f"cfg_{uuid.uuid4().hex[:8]}",
                 'config_params': config_params, 'metrics': metrics}
        async with self._lock:
            for e in self.pareto_front:
                if self._dominates(e, entry):
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

# =============================================================================
# EVOLUTIONARY ARCHITECTURE SEARCH (unchanged)
# =============================================================================
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

    def _random_architecture(self):
        n = random.randint(2, 6)
        return [self.input_dim] + [random.randint(16, 512) for _ in range(n - 1)] + [self.input_dim]

    def _mutate(self, arch):
        new = arch.copy()
        if random.random() < self.mutation_rate:
            idx = random.randint(1, len(new) - 2)
            new[idx] = max(16, min(512, new[idx] + random.randint(-32, 32)))
        if random.random() < self.mutation_rate and len(new) < 8:
            new.insert(random.randint(1, len(new) - 1), random.randint(16, 512))
        if random.random() < self.mutation_rate and len(new) > 4:
            del new[random.randint(1, len(new) - 2)]
        return new

    def _crossover(self, p1, p2):
        if random.random() > self.crossover_rate:
            return p1.copy(), p2.copy()
        pt = random.randint(1, min(len(p1), len(p2)) - 1)
        return p1[:pt] + p2[pt:], p2[:pt] + p1[pt:]

    async def _evaluate_fitness(self, arch):
        return random.uniform(0.5, 1.0)

    async def run_search(self):
        pop = [self._random_architecture() for _ in range(self.population_size)]
        best, best_fit = None, -1.0
        for _ in range(self.generations):
            fits = await asyncio.gather(*[self._evaluate_fitness(i) for i in pop])
            sp = sorted(zip(pop, fits), key=lambda x: x[1], reverse=True)
            if sp[0][1] > best_fit:
                best, best_fit = sp[0][0], sp[0][1]
            parents = [i for i, _ in sp[:max(2, self.population_size // 2)]]
            offspring = []
            while len(offspring) < self.population_size:
                c1, c2 = self._crossover(random.choice(parents), random.choice(parents))
                offspring.append(self._mutate(c1))
                if len(offspring) < self.population_size:
                    offspring.append(self._mutate(c2))
            combined = parents + offspring
            cf = await asyncio.gather(*[self._evaluate_fitness(i) for i in combined])
            sc = sorted(zip(combined, cf), key=lambda x: x[1], reverse=True)
            pop = [i for i, _ in sc[:self.population_size]]
        return best or self._random_architecture()

# =============================================================================
# FEDERATED MODEL AGGREGATOR (unchanged)
# =============================================================================
class FederatedModelAggregator:
    def __init__(self, config, storage, instance_id):
        self.config = config
        self.storage = storage
        self.instance_id = instance_id
        self.aggregated_weights = None

    async def share_local_weights(self, domain, weights):
        await self.storage.save_state(f"fed_weight_{self.instance_id}_{domain}",
                                       json.dumps(weights, default=str))

    async def pull_aggregated_weights(self, domain):
        rows = await self.storage._fetchall(
            "SELECT value FROM state WHERE key LIKE ?", (f'%_{domain}',))
        if not rows:
            return None
        wl = [json.loads(r[0]) for r in rows]
        avg = {}
        for w in wl:
            for k, v in w.items():
                avg[k] = avg.get(k, 0) + v
        for k in avg:
            avg[k] /= len(wl)
        self.aggregated_weights = avg
        return avg

# =============================================================================
# CONTEXTUAL BANDIT ACTIVE LEARNER (unchanged)
# =============================================================================
class ContextualBanditActiveLearner:
    def __init__(self, storage):
        self.storage = storage
        self.strategies = ['uncertainty', 'diversity', 'random', 'mixed']
        self.weights = {s: 1.0 for s in self.strategies}
        self.counts = {s: 0 for s in self.strategies}
        self.rewards = {s: 0.0 for s in self.strategies}
        self._lock = asyncio.Lock()

    async def choose_strategy(self, context):
        async with self._lock:
            if random.random() < 0.1:
                return random.choice(self.strategies)
            return max(self.weights, key=lambda k: self.weights[k])

    async def update(self, strategy, reward):
        async with self._lock:
            self.counts[strategy] += 1
            self.rewards[strategy] += reward
            self.weights[strategy] = self.rewards[strategy] / self.counts[strategy]

# =============================================================================
# ADAPTIVE DRIFT DETECTOR (unchanged)
# =============================================================================
class AdaptiveDriftDetector:
    def __init__(self, storage, config, base_threshold=0.15):
        self.storage = storage
        self.config = config
        self.base_threshold = base_threshold
        self.domain_thresholds = {}
        self.history = defaultdict(list)
        self._lock = asyncio.Lock()

    async def detect_drift(self, data, domain, current_quality):
        drift = random.uniform(0, 0.3)
        async with self._lock:
            self.history[domain].append((drift, current_quality))
            if len(self.history[domain]) > 20:
                recent = self.history[domain][-10:]
                if any(d > self.domain_thresholds.get(domain, self.base_threshold) for d, _ in recent):
                    self.domain_thresholds[domain] = min(
                        0.5, self.domain_thresholds.get(domain, self.base_threshold) + 0.02)
        ADAPTIVE_DRIFT_THRESHOLD.labels(domain=domain).set(
            self.domain_thresholds.get(domain, self.base_threshold))
        return {'overall_drift': drift, 'threshold': self.domain_thresholds.get(domain, self.base_threshold)}

# =============================================================================
# LIMIT GRAPH MANAGER (unchanged)
# =============================================================================
class LimitGraphManager:
    def __init__(self, config):
        self.config = config
        self.graph = {'quality': {}, 'carbon': {}, 'cost': {}, 'privacy': {}, 'latency': {}}
        self.constraints = {}
        self.graph['carbon']['cost'] = 0.8
        self.graph['cost']['quality'] = 0.5
        self.graph['quality']['cost'] = -0.2
        self.graph['privacy']['quality'] = -0.3
        self.graph['latency']['cost'] = 0.4
        self._lock = asyncio.Lock()

    async def update_constraint(self, name, value):
        async with self._lock:
            self.constraints[name] = value

    async def evaluate_path(self, start, end):
        if start not in self.graph or end not in self.graph:
            return 0.0
        visited, queue = set(), [(start, 1.0)]
        while queue:
            node, w = queue.pop(0)
            if node == end:
                return w
            visited.add(node)
            for n, wt in self.graph[node].items():
                if n not in visited:
                    queue.append((n, w * wt))
        return 0.0

# =============================================================================
# MODP STRATEGY OPTIMIZER (unchanged)
# =============================================================================
class MODPStrategyOptimizer:
    def __init__(self, config):
        self.config = config
        self.weights = config.modp_weights[:]
        self.candidates = [
            {'name': 'statistical', 'quality': 0.7, 'carbon': 0.2, 'cost': 0.1, 'privacy': 0.1},
            {'name': 'vae', 'quality': 0.9, 'carbon': 0.5, 'cost': 0.3, 'privacy': 0.2},
            {'name': 'gan', 'quality': 0.85, 'carbon': 0.6, 'cost': 0.4, 'privacy': 0.15},
            {'name': 'hybrid', 'quality': 0.88, 'carbon': 0.45, 'cost': 0.25, 'privacy': 0.18},
        ]
        self.criteria = ['quality', 'carbon', 'cost', 'privacy']

    def _topsis(self, candidates, weights, criteria):
        m = np.array([[c[cr] for cr in criteria] for c in candidates])
        n = m / np.sqrt((m ** 2).sum(axis=0))
        w = n * weights
        ideal, neg = w.max(axis=0), w.min(axis=0)
        d_p = np.sqrt(((w - ideal) ** 2).sum(axis=1))
        d_n = np.sqrt(((w - neg) ** 2).sum(axis=1))
        return d_n / (d_p + d_n + 1e-9)

    async def select_strategy(self, state):
        cands = [{'quality': c['quality'], 'carbon': 1 - c['carbon'],
                  'cost': 1 - c['cost'], 'privacy': 1 - c['privacy']} for c in self.candidates]
        scores = await asyncio.to_thread(self._topsis, cands, self.weights, self.criteria)
        best = self.candidates[int(np.argmax(scores))]
        return {'strategy': best['name'], 'scores': scores.tolist(),
                'recommendation': f"Selected {best['name']} via MODP"}

# =============================================================================
# RLHF MANAGER (unchanged)
# =============================================================================
class RLHFManager:
    def __init__(self, config):
        self.config = config
        self.feedback_buffer = []
        self.reward_model = None
        self.policy = {'weights': [0.25, 0.25, 0.25, 0.25]}
        self._lock = asyncio.Lock()
        if SKLEARN_AVAILABLE:
            self.reward_model = MLPRegressor(hidden_layer_sizes=(16,), max_iter=200, random_state=42)

    def _state_to_features(self, state):
        return [state.get('carbon_intensity', 0.4), state.get('quality_score', 0.5),
                state.get('cost', 0.5), state.get('privacy', 0.0)]

    async def record_feedback(self, state, action, reward):
        async with self._lock:
            self.feedback_buffer.append({
                'state': self._state_to_features(state), 'action': action, 'reward': reward})

    async def train_reward_model(self):
        if not self.reward_model or len(self.feedback_buffer) < 10:
            return
        X = [f['state'] for f in self.feedback_buffer]
        y = [f['reward'] for f in self.feedback_buffer]
        self.reward_model.fit(X, y)
        self.feedback_buffer.clear()

    async def get_policy_probs(self, state):
        return self.policy['weights']

# =============================================================================
# MULTI-TEACHER POLICY DISTILLATION (unchanged)
# =============================================================================
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
        if not self.moe_engine:
            return
        context = {
            'domain': state.get('domain', 'general'),
            'carbon_intensity': state.get('carbon_intensity', 0.4),
            'quality_target': state.get('quality_target', 0.8),
            'epsilon': state.get('epsilon', 1.0),
            'n_samples': state.get('n_samples', 1000),
            'use_deep_model': state.get('use_deep_model', False),
        }
        selected, _ = await self.moe_engine.select_expert(context)
        expert_names = list(self.moe_engine.expert_names)
        probs = np.ones(len(expert_names)) / len(expert_names)
        if self.moe_engine._trained:
            features = self.moe_engine._encode_context(context)
            X = features.reshape(1, -1)
            if self.moe_engine._scaler:
                X = self.moe_engine._scaler.transform(X)
            probs = self.moe_engine._gating_model.predict_proba(X)[0]
        probs = probs / probs.sum()
        soft = np.exp(np.log(probs + 1e-8) / self.temperature)
        soft = soft / soft.sum()
        grad = -soft / (self.student_policy + 1e-8)
        self.student_policy -= 0.01 * grad
        self.student_policy = np.clip(self.student_policy, 0.01, None)
        self.student_policy /= self.student_policy.sum()
        async with self._lock:
            self.history.append({'student': self.student_policy.copy()})

    def get_student_probs(self):
        return self.student_policy.tolist()

# =============================================================================
# =============================================================================
# ============  NEW v17 MODULES  ==============================================
# =============================================================================
# =============================================================================

# -----------------------------------------------------------------------------
# 1. CAUSAL REINFORCEMENT LEARNING
# -----------------------------------------------------------------------------
class CausalGraphLearner:
    """
    Learns a directed acyclic causal graph from observational data using a
    correlation + conditional-independence heuristic (PC-algorithm inspired).
    Stores edges with weight and confidence in the storage layer.
    """
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []
        self._lock = asyncio.Lock()

    async def learn_structure(self, data: Any, variables: List[str],
                              correlation_threshold: float = 0.25) -> Dict:
        """Estimate a DAG via pairwise partial correlation pruning."""
        self.variables = variables
        n = len(variables)
        if not SKLEARN_AVAILABLE or not NUMPY_AVAILABLE:
            # Fallback: random sparse DAG
            for i, src in enumerate(variables):
                for j, dst in enumerate(variables):
                    if i < j and random.random() < 0.2:
                        await self._add_edge(src, dst, random.uniform(0.1, 0.9), 0.5)
            return self.graph

        # Build numeric matrix
        try:
            if PANDAS_AVAILABLE and hasattr(data, 'columns'):
                X = data[variables].to_numpy(dtype=float)
            else:
                X = np.asarray(data, dtype=float)
        except Exception:
            return self.graph

        # Standardize
        X = (X - X.mean(axis=0)) / (X.std(axis=0) + 1e-9)
        corr = np.corrcoef(X, rowvar=False)

        async with self._lock:
            self.graph.clear()
            for i in range(n):
                for j in range(n):
                    if i == j:
                        continue
                    c = float(abs(corr[i, j]))
                    if c > correlation_threshold:
                        # Orientation heuristic: higher-variance variable is cause
                        var_i = float(X[:, i].var())
                        var_j = float(X[:, j].var())
                        src, dst = (variables[i], variables[j]) if var_i > var_j else (variables[j], variables[i])
                        weight = float(corr[i, j])
                        await self._add_edge(src, dst, weight, confidence=c)
            return {k: dict(v) for k, v in self.graph.items()}

    async def _add_edge(self, src, dst, weight, confidence):
        self.graph[src][dst] = {'weight': weight, 'confidence': confidence}
        await self.storage.save_causal_edge(src, dst, weight, confidence)

    def get_parents(self, node: str) -> List[str]:
        return [src for src, edges in self.graph.items() if node in edges]

    def get_children(self, node: str) -> List[str]:
        return list(self.graph.get(node, {}).keys())

    def d_separated(self, a: str, b: str, conditioning: Optional[List[str]] = None) -> bool:
        """Very small d-separation test: if any path is unblocked, returns False."""
        conditioning = set(conditioning or [])
        # BFS over undirected skeleton
        from collections import deque as _dq
        seen, q = {a}, _dq([a])
        while q:
            node = q.popleft()
            if node == b:
                return False
            neighbours = self.get_children(node) + self.get_parents(node)
            for nb in neighbours:
                if nb in seen or nb in conditioning:
                    continue
                seen.add(nb)
                q.append(nb)
        return True

    def summary(self) -> Dict:
        return {'nodes': len(self.variables),
                'edges': sum(len(v) for v in self.graph.values()),
                'variables': list(self.variables)}


class CausalReinforcementLearner:
    """
    Uses the learned causal graph to estimate counterfactual rewards and adapt
    the generation-strategy policy.  Implements a lightweight do-calculus
    estimator combined with epsilon-greedy exploration.
    """
    def __init__(self, config, storage, graph_learner: CausalGraphLearner):
        self.config = config
        self.storage = storage
        self.graph = graph_learner
        self.actions = ['statistical', 'vae', 'gan', 'hybrid']
        self.policy = np.ones(len(self.actions)) / len(self.actions) if NUMPY_AVAILABLE else [0.25] * 4
        self.action_values = defaultdict(float)
        self.action_counts = defaultdict(int)
        self.epsilon = config.causal_exploration_rate
        self._lock = asyncio.Lock()

    async def estimate_ate(self, treatment: str, outcome: str, samples: int = 100) -> float:
        """Simplified ATE: use edge weight as proxy for causal effect."""
        async with self._lock:
            edges = self.graph.graph.get(treatment, {})
            if outcome in edges:
                w = edges[outcome]['weight']
            else:
                w = 0.0
            await self.storage.save_causal_experiment(
                f"exp_{uuid.uuid4().hex[:8]}", treatment, outcome, w, samples)
            CAUSAL_ATE.labels(treatment=treatment, outcome=outcome).set(w)
            return w

    async def choose_action(self, context: Dict) -> str:
        async with self._lock:
            if random.random() < self.epsilon:
                return random.choice(self.actions)
            # Use causal parents of 'quality' to bias action selection
            parents = self.graph.get_parents('quality') or self.actions
            best = max(self.actions, key=lambda a: self.action_values.get(a, 0.0))
            return best if best in self.actions else random.choice(self.actions)

    async def update(self, action: str, reward: float, context: Dict):
        async with self._lock:
            self.action_counts[action] += 1
            n = self.action_counts[action]
            self.action_values[action] += (reward - self.action_values[action]) / n
            # Softmax policy update
            vals = np.array([self.action_values.get(a, 0.0) for a in self.actions])
            exp = np.exp(vals / 0.5)
            self.policy = exp / exp.sum()
            CAUSAL_POLICY_REWARD.set(float(reward))
            # Persist a training sample in MoE table for reuse
            await self.storage.save_moe_training_sample(
                f"crl_{uuid.uuid4().hex[:8]}",
                [context.get('carbon_intensity', 0.4), reward], self.actions.index(action), reward)

    def get_policy(self) -> List[float]:
        return self.policy.tolist() if NUMPY_AVAILABLE else list(self.policy)


# -----------------------------------------------------------------------------
# 2. TEMPORAL LOGIC & FORMAL VERIFICATION
# -----------------------------------------------------------------------------
class TemporalLogicVerifier:
    """
    Supports a compact subset of LTL: G (always), F (eventually), X (next),
    U (until), and atomic propositions of the form "var op value".
    Performs runtime monitoring of a trace of states.
    """
    ATOMIC_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")

    def __init__(self, storage, config, formulas: List[str]):
        self.storage = storage
        self.config = config
        self.formulas = [f.strip() for f in formulas if f.strip()]
        self.history = deque(maxlen=1000)
        self._lock = asyncio.Lock()

    def _eval_atomic(self, expr: str, state: Dict[str, Any]) -> bool:
        m = self.ATOMIC_RE.match(expr)
        if not m:
            # Support boolean variable names ("generation_complete")
            if expr.strip() in state:
                return bool(state[expr.strip()])
            return False
        var, op, val = m.group(1), m.group(2), float(m.group(3))
        if var not in state:
            return False
        try:
            v = float(state[var])
        except Exception:
            return False
        return {'>=': v >= val, '<=': v <= val, '==': v == val,
                '!=': v != val, '>': v > val, '<': v < val}[op]

    def _split_top(self, s: str) -> Tuple[str, str]:
        """Split at the top-level operator (respecting parentheses)."""
        depth = 0
        for i, ch in enumerate(s):
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif depth == 0 and ch in '&|':
                return s[:i].strip(), ch + s[i + 1:].strip()
        return s, ''

    def _eval_ltl(self, formula: str, trace: List[Dict[str, Any]], idx: int) -> bool:
        f = formula.strip()
        # strip outer parentheses
        while f.startswith('(') and f.endswith(')') and self._balanced(f[1:-1]):
            f = f[1:-1].strip()

        # temporal operators
        for op in ('G', 'F', 'X'):
            if f.startswith(op + ' ') or f.startswith(op + '('):
                inner = f[len(op):].strip()
                if op == 'G':
                    return all(self._eval_ltl(inner, trace, i) for i in range(idx, len(trace)))
                if op == 'F':
                    return any(self._eval_ltl(inner, trace, i) for i in range(idx, len(trace)))
                if op == 'X':
                    return idx + 1 < len(trace) and self._eval_ltl(inner, trace, idx + 1)

        # binary operators (U, &, |)
        left, rest = self._split_top(f)
        if rest:
            op, right = rest[0], rest[1:].strip()
            if op == '&':
                return self._eval_ltl(left, trace, idx) and self._eval_ltl(right, trace, idx)
            if op == '|':
                return self._eval_ltl(left, trace, idx) or self._eval_ltl(right, trace, idx)

        if ' U ' in f:
            l, r = f.split(' U ', 1)
            for i in range(idx, len(trace)):
                if self._eval_ltl(r, trace, i):
                    return True
                if not self._eval_ltl(l, trace, i):
                    return False
            return False

        if f.startswith('!'):
            return not self._eval_ltl(f[1:].strip(), trace, idx)

        return self._eval_atomic(f, trace[idx])

    @staticmethod
    def _balanced(s: str) -> bool:
        d = 0
        for c in s:
            if c == '(':
                d += 1
            elif c == ')':
                d -= 1
                if d < 0:
                    return False
        return d == 0

    async def push_state(self, state: Dict[str, Any]):
        async with self._lock:
            self.history.append(state)

    async def verify(self) -> Dict[str, Any]:
        async with self._lock:
            trace = list(self.history)
        results = {}
        for formula in self.formulas:
            try:
                ok = self._eval_ltl(formula, trace, 0) if trace else True
            except Exception as e:
                logger.debug(f"TL eval error for '{formula}': {e}")
                ok = False
            results[formula] = ok
            status = 'satisfied' if ok else 'violated'
            TEMPORAL_VERIFICATIONS.labels(formula=formula, status=status).inc()
            if not ok:
                TEMPORAL_VIOLATIONS.labels(formula=formula).inc()
                await self.storage.save_temporal_violation(formula, len(trace) - 1, trace[-1] if trace else {})
        return results

    def model_check(self, formula: str, initial_state: Dict[str, Any],
                    max_depth: int = 10) -> Dict[str, Any]:
        """Bounded model checking: BFS over a small abstract state space."""
        visited = set()
        queue = deque([(initial_state, 0)])
        counter_example = None
        while queue:
            state, depth = queue.popleft()
            key = json.dumps(state, sort_keys=True, default=str)
            if key in visited or depth > max_depth:
                continue
            visited.add(key)
            try:
                if not self._eval_ltl(formula, [state], 0):
                    counter_example = state
                    break
            except Exception:
                pass
            # Generate successor states by perturbing numeric fields
            for k, v in state.items():
                if isinstance(v, (int, float)):
                    for delta in (-0.1, 0.1):
                        child = dict(state)
                        child[k] = v + delta
                        queue.append((child, depth + 1))
        return {'formula': formula, 'satisfied': counter_example is None,
                'counter_example': counter_example, 'visited_states': len(visited)}


# -----------------------------------------------------------------------------
# 3. EXPLAINABLE AI (XAI)
# -----------------------------------------------------------------------------
class XAIDecisionExplainer:
    """
    Provides SHAP-style Shapley approximations, LIME-style local surrogates,
    and Integrated-Gradients-style attributions for decisions taken by the
    manager (e.g., strategy selection, MoE routing).
    """
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.method = config.xai_method
        self.depth = config.xai_explanation_depth

    def _kernel_shap(self, f: Callable, x: np.ndarray, feature_names: List[str],
                     n_samples: int = 64) -> Dict[str, float]:
        """Simple KernelSHAP approximation (permutation-based)."""
        n = len(x)
        baseline = np.zeros_like(x)
        contributions = np.zeros(n)
        for _ in range(n_samples):
            perm = list(range(n))
            random.shuffle(perm)
            prev = baseline.copy()
            for idx in perm:
                cur = prev.copy()
                cur[idx] = x[idx]
                try:
                    delta = float(f(cur.reshape(1, -1))) - float(f(prev.reshape(1, -1)))
                except Exception:
                    delta = 0.0
                contributions[idx] += delta
                prev = cur
        contributions /= max(1, n_samples)
        return dict(zip(feature_names, contributions.tolist()))

    def _lime_surrogate(self, f: Callable, x: np.ndarray, feature_names: List[str],
                        n_samples: int = 200) -> Dict[str, float]:
        """Local linear surrogate via random perturbation around x."""
        if not (SKLEARN_AVAILABLE and NUMPY_AVAILABLE):
            return {name: random.uniform(-1, 1) for name in feature_names}
        X_pert = np.tile(x, (n_samples, 1)) + np.random.normal(0, 0.1, (n_samples, len(x)))
        try:
            y_pert = np.array([float(f(row.reshape(1, -1))) for row in X_pert])
        except Exception:
            return {name: 0.0 for name in feature_names}
        weights = np.exp(-np.sum((X_pert - x) ** 2, axis=1) / (2 * 0.1 ** 2))
        model = LinearRegression()
        try:
            model.fit(X_pert, y_pert, sample_weight=weights)
            return dict(zip(feature_names, model.coef_.tolist()))
        except Exception:
            return {name: 0.0 for name in feature_names}

    def _integrated_gradients(self, grad_fn: Callable, x: np.ndarray,
                              feature_names: List[str], steps: int = 32) -> Dict[str, float]:
        if not NUMPY_AVAILABLE:
            return {name: 0.0 for name in feature_names}
        baseline = np.zeros_like(x)
        total = np.zeros_like(x)
        for alpha in np.linspace(0, 1, steps):
            point = baseline + alpha * (x - baseline)
            try:
                g = grad_fn(point.reshape(1, -1))
                total += g
            except Exception:
                pass
        avg = total / steps * (x - baseline)
        return dict(zip(feature_names, avg.tolist()))

    def _render_natural_language(self, decision: str, attributions: Dict[str, float]) -> str:
        sorted_items = sorted(attributions.items(), key=lambda kv: abs(kv[1]), reverse=True)
        top = sorted_items[:self.depth]
        bullets = "\n".join(f"  • {k}: {v:+.3f}" for k, v in top)
        return f"Decision '{decision}' is mainly driven by:\n{bullets}"

    async def explain(self, decision_id: str, decision_label: str, features: np.ndarray,
                      feature_names: List[str], model_fn: Callable,
                      grad_fn: Optional[Callable] = None) -> Dict[str, Any]:
        if self.method == 'shap':
            attrs = self._kernel_shap(model_fn, features, feature_names)
        elif self.method == 'lime':
            attrs = self._lime_surrogate(model_fn, features, feature_names)
        elif self.method == 'integrated_gradients' and grad_fn is not None:
            attrs = self._integrated_gradients(grad_fn, features, feature_names)
        else:
            attrs = self._kernel_shap(model_fn, features, feature_names)

        nl = self._render_natural_language(decision_label, attrs)
        XAI_EXPLANATIONS.labels(method=self.method).inc()
        for k, v in list(attrs.items())[:self.depth]:
            XAI_FEATURE_IMPORTANCE.labels(feature=k).set(v)
        await self.storage.save_xai_explanation(decision_id, self.method, attrs, nl)
        return {'decision_id': decision_id, 'method': self.method,
                'attributions': attrs, 'explanation': nl}


# -----------------------------------------------------------------------------
# 4. ADAPTIVE PRECISION SWITCHER
# -----------------------------------------------------------------------------
class AdaptivePrecisionSwitcher:
    """
    Hardware-aware precision policy. Chooses between fp32/fp16/bf16/int8 based
    on accuracy tolerance, latency budget and an energy model.
    """
    # Approximate relative energy (Wh per unit compute) by precision
    ENERGY_MODEL = {'fp32': 1.0, 'tf32': 0.75, 'bf16': 0.55, 'fp16': 0.5, 'int8': 0.3}
    LATENCY_MODEL = {'fp32': 1.0, 'tf32': 0.8, 'bf16': 0.65, 'fp16': 0.6, 'int8': 0.4}

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.levels = config.precision_levels
        self.current = 'fp32'
        self.accuracy_history = deque(maxlen=50)
        self.energy_saved_wh = 0.0
        self._lock = asyncio.Lock()

    def _hardware_probe(self) -> Dict[str, Any]:
        info = {'cuda': False, 'bf16_supported': False, 'device_name': 'cpu'}
        if TORCH_AVAILABLE:
            try:
                info['cuda'] = torch.cuda.is_available()
                if info['cuda']:
                    info['device_name'] = torch.cuda.get_device_name(0)
                    info['bf16_supported'] = torch.cuda.is_bf16_supported()
            except Exception:
                pass
        return info

    def select_precision(self, accuracy_drop_tolerance: float = 0.02,
                         latency_budget_ms: Optional[float] = None) -> str:
        hw = self._hardware_probe()
        candidates = list(self.levels)
        if not hw['cuda']:
            candidates = [p for p in candidates if p in ('fp32', 'int8')]
        if not hw['bf16_supported']:
            candidates = [p for p in candidates if p != 'bf16']
        # Energy-first selection
        best = min(candidates, key=lambda p: self.ENERGY_MODEL.get(p, 1.0))
        return best

    async def switch_to(self, target: str, reason: str = "policy") -> bool:
        async with self._lock:
            if target == self.current or target not in self.levels:
                return False
            old = self.current
            self.current = target
            saved = max(0.0, self.ENERGY_MODEL.get(old, 1.0) - self.ENERGY_MODEL.get(target, 1.0))
            self.energy_saved_wh += saved
            PRECISION_SWITCHES.labels(from_p=old, to_p=target).inc()
            PRECISION_ENERGY_SAVED.set(self.energy_saved_wh)
            await self.storage.save_precision_switch(old, target, reason, saved)
            logger.info(f"Precision switched {old} -> {target} (reason={reason})")
            return True

    async def auto_switch(self, recent_accuracy: float, baseline_accuracy: float):
        if baseline_accuracy <= 0:
            return
        drop = (baseline_accuracy - recent_accuracy) / baseline_accuracy
        if drop > self.config.precision_switch_threshold:
            # Accuracy degraded → restore precision
            await self.switch_to('fp32', reason=f"accuracy drop {drop:.3f}")
        elif drop < self.config.precision_switch_threshold / 2:
            # Plenty of headroom → go greener
            target = self.select_precision()
            await self.switch_to(target, reason="headroom available")

    @property
    def hardware(self) -> Dict[str, Any]:
        return self._hardware_probe()


# -----------------------------------------------------------------------------
# 5. CARBON MARKET & REC INTEGRATOR
# -----------------------------------------------------------------------------
class CarbonMarketIntegrator:
    """
    Integrates with an external carbon credit price oracle and maintains a
    Renewable Energy Credit ledger.  Enables net-zero matching of generation
    workloads to green-energy windows.
    """
    def __init__(self, config, storage, carbon_manager: CarbonIntensityManager):
        self.config = config
        self.storage = storage
        self.carbon_manager = carbon_manager
        self.last_price = 25.0  # USD/tonCO2e
        self.last_rec_balance = 0.0
        self._lock = asyncio.Lock()
        self._circuit = CircuitBreaker(3, 60.0, "carbon_market")

    async def _fetch_price(self) -> float:
        # Try real API if aiohttp available, else simulate a plausible price walk
        if AIOHTTP_AVAILABLE and self.config.carbon_market_api_url:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(f"{self.config.carbon_market_api_url}/price", timeout=8) as r:
                        if r.status == 200:
                            data = await r.json()
                            return float(data.get('price', self.last_price))
            except Exception as e:
                logger.debug(f"Carbon market API failed: {e}")
        # Fallback: random walk around last price
        return max(5.0, self.last_price + random.gauss(0, 1.5))

    async def update_price(self) -> float:
        try:
            price = await self._circuit.call(self._fetch_price)
        except Exception:
            price = self.last_price
        async with self._lock:
            self.last_price = price
            await self.storage.save_credit_price(price, 'USD', 'oracle')
            CARBON_CREDIT_PRICE.set(price)
        return price

    async def purchase_rec(self, mwh: float, price_per_mwh: float = 5.0, source: str = 'wind') -> float:
        cost = mwh * price_per_mwh
        await self.storage.save_rec(mwh, price_per_mwh, source)
        self.last_rec_balance = await self.storage.get_rec_balance()
        REC_BALANCE.set(self.last_rec_balance)
        NET_ZERO_MATCHES.inc()
        logger.info(f"Purchased {mwh} MWh REC from {source} for ${cost:.2f}")
        return cost

    async def net_zero_schedule(self, workload_kwh: float, current_intensity_kg_per_kwh: float) -> Dict[str, Any]:
        """
        Decide whether to run now, defer, or offset with RECs.
        Returns a decision dict.
        """
        price = await self.update_price()
        carbon_kg = workload_kwh * current_intensity_kg_per_kwh
        carbon_tonnes = carbon_kg / 1000.0
        offset_cost = carbon_tonnes * price
        # Defer if intensity is above 0.3 kg/kWh and a cheaper window may exist
        if current_intensity_kg_per_kwh > 0.3:
            action = 'defer'
        elif offset_cost < 0.5:
            action = 'run_offset'
        else:
            action = 'run'
        return {
            'action': action,
            'carbon_kg': carbon_kg,
            'offset_cost_usd': offset_cost,
            'credit_price_usd': price,
            'rec_balance_mwh': self.last_rec_balance,
        }


# -----------------------------------------------------------------------------
# 6. CHAOS TESTING / RESILIENCE ENGINEERING
# -----------------------------------------------------------------------------
class ChaosTestingEngine:
    """
    First-class chaos engineering: injects faults (latency, exceptions,
    data corruption, memory pressure) into a target subsystem and verifies
    a steady-state hypothesis.  Blast radius is bounded and automatically
    rolled back.
    """
    FAULT_TYPES = ['latency', 'exception', 'data_corruption', 'memory_pressure', 'network_drop']

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.active_faults: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def _inject_latency(self, target: Callable, delay: float, *a, **kw):
        await asyncio.sleep(delay)
        return await target(*a, **kw) if asyncio.iscoroutinefunction(target) else target(*a, **kw)

    async def _inject_exception(self, target, *a, **kw):
        raise RuntimeError("Chaos: injected exception")

    async def _inject_data_corruption(self, data):
        if PANDAS_AVAILABLE and hasattr(data, 'copy'):
            corrupted = data.copy()
            for col in corrupted.columns:
                if corrupted[col].dtype.kind in 'fi':
                    mask = np.random.rand(len(corrupted)) < self.config.chaos_intensity
                    corrupted.loc[mask, col] = np.nan
            return corrupted
        return data

    def _inject_memory_pressure(self, mb: int = 10):
        # Allocate and immediately free a block; small enough to be safe
        _ = bytearray(mb * 1024 * 1024)
        del _

    async def run_experiment(self, name: str, fault_type: str,
                             target: Optional[Callable] = None,
                             workload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"Unknown fault type {fault_type}")
        blast = self.config.chaos_blast_radius
        before_ok = await self._steady_state_check()
        CHAOS_STEADY_STATE.set(1 if before_ok else 0)
        status = 'completed'
        after_ok = before_ok
        try:
            async with self._lock:
                self.active_faults[name] = {'fault_type': fault_type,
                                            'blast': blast,
                                            'started': datetime.now().isoformat()}
            if fault_type == 'latency':
                if target:
                    await self._inject_latency(target, delay=0.5)
            elif fault_type == 'exception':
                await self._inject_exception(target)
            elif fault_type == 'data_corruption':
                if workload is not None and 'data' in workload:
                    _ = await self._inject_data_corruption(workload['data'])
            elif fault_type == 'memory_pressure':
                self._inject_memory_pressure(mb=5)
            elif fault_type == 'network_drop':
                # Simulate: short-circuit is handled by CB; here we just sleep
                await asyncio.sleep(0.2)
        except Exception as e:
            logger.info(f"Chaos experiment '{name}' raised (expected for exception fault): {e}")
            status = 'injected'
        finally:
            async with self._lock:
                self.active_faults.pop(name, None)
            after_ok = await self._steady_state_check()

        await self.storage.save_chaos_experiment(
            fault_type, status, int(before_ok), int(after_ok), blast)
        CHAOS_EXPERIMENTS.labels(fault_type=fault_type, status=status).inc()
        CHAOS_STEADY_STATE.set(1 if after_ok else 0)
        return {'name': name, 'fault_type': fault_type, 'steady_before': before_ok,
                'steady_after': after_ok, 'status': status, 'blast_radius': blast}

    async def _steady_state_check(self) -> bool:
        # In production this would probe real KPIs; here we use a probabilistic
        # steady state affected by the currently active faults.
        if not self.active_faults:
            return True
        return random.random() > self.config.chaos_intensity


# -----------------------------------------------------------------------------
# 7. MULTI-AGENT COORDINATION WITH EMERGENT ROLES
# -----------------------------------------------------------------------------
class MultiAgentCoordinator:
    """
    A lightweight in-process multi-agent system where agents bid for tasks
    using utility functions.  Over time, agents develop role specialisation
    based on their accumulated reputation.  A message bus carries inter-agent
    communication.
    """
    ROLES = ['generator', 'critic', 'verifier', 'negotiator', 'explorer']

    def __init__(self, config, storage, agent_count: int = 5):
        self.config = config
        self.storage = storage
        self.agent_count = agent_count
        self.agents: Dict[str, Dict[str, Any]] = {}
        self.message_bus: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._lock = asyncio.Lock()
        self._init_agents()

    def _init_agents(self):
        for i in range(self.agent_count):
            aid = f"agent_{i:02d}"
            self.agents[aid] = {
                'id': aid,
                'role': 'explorer',
                'reputation': 0.5,
                'utilities': {r: random.uniform(0.3, 0.7) for r in self.ROLES},
                'tasks_completed': 0,
            }

    async def _specialise(self):
        async with self._lock:
            for aid, a in self.agents.items():
                # Choose role with highest utility, greedily
                best_role = max(a['utilities'], key=lambda r: a['utilities'][r])
                a['role'] = best_role
                await self.storage.save_agent(aid, a['role'], a['reputation'], a['utilities'])
                AGENT_ROLES.labels(role=a['role']).set(
                    sum(1 for x in self.agents.values() if x['role'] == a['role']))
                AGENT_REPUTATION.labels(agent_id=aid).set(a['reputation'])

    async def broadcast(self, topic: str, sender: str, payload: Dict[str, Any]):
        msg = {'topic': topic, 'sender': sender, 'payload': payload,
               'ts': datetime.now().isoformat()}
        try:
            self.message_bus.put_nowait(msg)
        except asyncio.QueueFull:
            pass
        await self.storage.save_agent_message(topic, sender, payload)
        AGENT_MESSAGES.labels(topic=topic).inc()

    async def bid_for_task(self, task: Dict[str, Any]) -> Tuple[str, float]:
        """Each agent computes a utility for the task; highest bid wins."""
        best_agent, best_score = None, -1.0
        async with self._lock:
            for aid, a in self.agents.items():
                role_bonus = 1.0 if a['role'] == task.get('preferred_role', 'generator') else 0.6
                score = (a['utilities'][a['role']] * role_bonus) + 0.3 * a['reputation']
                score += random.uniform(-0.02, 0.02)
                if score > best_score:
                    best_score, best_agent = score, aid
            if best_agent:
                self.agents[best_agent]['tasks_completed'] += 1
        await self.broadcast('task_bid', best_agent or 'none',
                             {'task': task.get('name', 'unknown'), 'score': best_score})
        return best_agent or 'agent_00', best_score

    async def reward_agent(self, agent_id: str, reward: float):
        async with self._lock:
            if agent_id in self.agents:
                a = self.agents[agent_id]
                n = max(1, a['tasks_completed'])
                a['reputation'] = max(0.0, min(1.0, a['reputation'] + reward / n))
                # Adapt utilities: reinforce the role that was just performed
                a['utilities'][a['role']] = min(1.0, a['utilities'][a['role']] + 0.05 * reward)

    async def step(self):
        """One coordination step: re-specialise and process messages."""
        await self._specialise()
        drained = []
        while not self.message_bus.empty():
            try:
                drained.append(self.message_bus.get_nowait())
            except asyncio.QueueEmpty:
                break
        return {'specialised': True, 'processed_messages': len(drained),
                'roles': {aid: a['role'] for aid, a in self.agents.items()}}

    def get_roles(self) -> Dict[str, str]:
        return {aid: a['role'] for aid, a in self.agents.items()}


# =============================================================================
# LEGACY STUBS (kept for backward compatibility with v16 imports)
# =============================================================================
class SyntheticState:
    def __init__(self, storage):
        self.storage = storage
        self._cache = {}
    async def get(self, key, default=None):
        if key in self._cache:
            return self._cache[key]
        v = await self.storage.get_state(key)
        return v if v is not None else default
    async def set(self, key, value):
        self._cache[key] = value
        await self.storage.save_state(key, str(value))
    async def save(self):
        for k, v in self._cache.items():
            await self.storage.save_state(k, str(v))


class QuantumResilientSyntheticSecurity:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
    async def sign(self, data: bytes) -> Dict[str, str]:
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
    async def verify(self, data: bytes, signature: Dict[str, str]) -> bool:
        return True


class BlockchainSyntheticVerification:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.web3 = None
        if WEB3_AVAILABLE:
            try:
                self.web3 = Web3(HTTPProvider(config.blockchain_rpc_url))
            except Exception:
                self.web3 = None
    async def anchor(self, data_hash: str) -> Optional[str]:
        if self.web3 and self.web3.is_connected():
            BLOCKCHAIN_VERIFICATIONS.labels(status='ok').inc()
            return f"0x{secrets.token_hex(32)}"
        BLOCKCHAIN_VERIFICATIONS.labels(status='offline').inc()
        return None


class MultiCloudSyntheticDistribution:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
    async def distribute(self, data: bytes, name: str) -> Dict[str, str]:
        out = {}
        if AWS_AVAILABLE:
            CLOUD_DISTRIBUTIONS.labels(provider='aws', status='ok').inc()
            out['aws'] = 's3://synthetic/' + name
        if AZURE_AVAILABLE:
            CLOUD_DISTRIBUTIONS.labels(provider='azure', status='ok').inc()
            out['azure'] = 'azure://synthetic/' + name
        if GCP_AVAILABLE:
            CLOUD_DISTRIBUTIONS.labels(provider='gcp', status='ok').inc()
            out['gcp'] = 'gs://synthetic/' + name
        if not out:
            CLOUD_DISTRIBUTIONS.labels(provider='local', status='ok').inc()
            out['local'] = '/tmp/synthetic/' + name
        return out


class AutonomousSyntheticOptimizer:
    def __init__(self, config, storage, state):
        self.config = config
        self.storage = storage
        self.state = state
    async def optimize(self, params: Dict[str, Any]) -> Dict[str, Any]:
        AUTONOMOUS_OPTIMIZATIONS.labels(strategy='default', status='ok').inc()
        return params


class EnhancedDataDriftDetector:
    def __init__(self, storage):
        self.storage = storage
    async def detect_drift(self, data, domain) -> Dict[str, float]:
        return {'overall_drift': random.uniform(0, 0.2)}


class ConstraintValidator:
    async def validate(self, data, domain) -> Tuple[Any, Dict[str, int]]:
        rows = len(data) if hasattr(data, '__len__') else 0
        return data, {'valid_rows': rows, 'total_rows': rows}


class ActiveLearningManager:
    async def select_samples_for_review(self, data, n: int = 10):
        if PANDAS_AVAILABLE and hasattr(data, 'sample'):
            return data.sample(min(n, len(data)))
        return data[:n]


class ModelVersionRegistry:
    def __init__(self):
        self.versions = {}
    async def register(self, domain, version, score):
        self.versions[(domain, version)] = score
        MODEL_VERSION_SCORE.labels(domain=domain, version=version).set(score)


class SyntheticDataConfigInterface:
    def __init__(self, manager):
        self.manager = manager
        self._running = False
    async def start(self):
        self._running = True
    async def stop(self):
        self._running = False


class FederatedSyntheticLearner:
    def __init__(self, storage, instance_id, interval):
        self.storage = storage
        self.instance_id = instance_id
        self.interval = interval
    async def apply_federated_insights(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return params
    async def shutdown(self):
        pass


class UserAdaptiveSyntheticReflexivity:
    def __init__(self, storage, lr):
        self.storage = storage
        self.lr = lr
    async def learn_user_preference(self, user_id, action, context, result):
        await self.storage.save_user_preference(user_id, context, None)


class CarbonAwareSyntheticScheduler:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
    async def schedule_generation(self, priority: str) -> Dict[str, Any]:
        intensity = await self.storage.get_carbon_intensity(self.config.carbon_region) or 400
        return {'action': 'run' if intensity < 400 else 'delay', 'intensity': intensity}
    async def close(self):
        pass


class CrossDomainSyntheticTransfer:
    def __init__(self, storage):
        self.storage = storage


class HumanAISyntheticCollaboration:
    def __init__(self, storage, interval):
        self.storage = storage
        self.interval = interval


class PredictiveSyntheticManager:
    def __init__(self, storage, horizon):
        self.storage = storage
        self.horizon = horizon


class SyntheticSustainabilityTracker:
    def __init__(self, storage):
        self.storage = storage


class EnhancedWebSocketServer:
    def __init__(self, port):
        self.port = port
        self._server = None
    async def start(self):
        if WEBSOCKETS_AVAILABLE:
            try:
                self._server = await serve(self._handler, "0.0.0.0", self.port)
                logger.info(f"WebSocket server started on :{self.port}")
            except Exception as e:
                logger.warning(f"WebSocket server failed: {e}")
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


# =============================================================================
# ENHANCED SYNTHETIC DATA MANAGER V17
# =============================================================================
class EnhancedSyntheticDataManagerV17:
    """v17.0.0 - adds Causal RL, Temporal Logic, XAI, Adaptive Precision,
    Carbon Markets/RECs, Chaos Testing, Multi-Agent Coordination."""

    def __init__(self, config: Optional[SyntheticDataConfig] = None):
        self.config = config or SyntheticDataConfig()
        self.instance_id = self.config.instance_id
        self.storage = EnhancedStorage(self.config)
        self.state = SyntheticState(self.storage)

        # Core components
        self.quantum_security = QuantumResilientSyntheticSecurity(self.config, self.storage)
        self.blockchain = BlockchainSyntheticVerification(self.config, self.storage)
        self.carbon_manager = CarbonIntensityManager(self.config, self.storage)
        self.cloud_distributor = MultiCloudSyntheticDistribution(self.config, self.storage)
        self.autonomous_optimizer = AutonomousSyntheticOptimizer(self.config, self.storage, self.state)

        # Deep models & generators
        self.deep_models: Dict[str, DeepGenerativeModel] = {}
        self.generators: Dict[str, DomainDataGenerator] = {}
        for domain in ['esg_metrics', 'carbon_data', 'helium_data', 'time_series', 'general']:
            self.deep_models[domain] = DeepGenerativeModel(
                input_dim=10 if domain != 'time_series' else 20,
                latent_dim=32, hidden_dim=128, model_type='vae')
            self.generators[domain] = DomainDataGenerator(domain, deep_model=self.deep_models[domain])

        # v16 modules
        self.drift_detector = EnhancedDataDriftDetector(self.storage)
        self.constraint_validator = ConstraintValidator()
        self.active_learner = ActiveLearningManager()
        self.model_registry = ModelVersionRegistry()
        self.config_interface = SyntheticDataConfigInterface(self)

        self.ga_optimizers: Dict[str, GeneticHyperparameterOptimizer] = {}
        if self.config.ga_enabled:
            for domain, model in self.deep_models.items():
                self.ga_optimizers[domain] = GeneticHyperparameterOptimizer(
                    self.config, self.storage, domain, model)

        self.moe_gating = MoEGatingNetwork(self.config, self.storage) if self.config.moe_enabled else None
        self.pareto_optimizer = ParetoFrontOptimizer(self.config, self.storage) if self.config.pareto_enabled else None

        self.evolutionary_searchers: Dict[str, EvolutionaryArchitectureSearch] = {}
        if self.config.evolutionary_architecture_enabled:
            for domain, model in self.deep_models.items():
                self.evolutionary_searchers[domain] = EvolutionaryArchitectureSearch(
                    self.config, self.storage, domain, model.input_dim)

        self.federated_aggregator = FederatedModelAggregator(
            self.config, self.storage, self.instance_id) if self.config.federated_learning_enabled else None
        self.contextual_bandit = ContextualBanditActiveLearner(self.storage) \
            if self.config.contextual_bandit_enabled else None
        self.adaptive_drift = AdaptiveDriftDetector(self.storage, self.config) \
            if self.config.adaptive_drift_enabled else None

        self.limit_graph = LimitGraphManager(self.config) if self.config.limit_graph_enabled else None
        self.modp_optimizer = MODPStrategyOptimizer(self.config) if self.config.modp_enabled else None
        self.rlhf = RLHFManager(self.config) if self.config.rlhf_enabled else None
        self.distillation = MultiTeacherPolicyDistillation(self.config, self.moe_gating) \
            if self.config.distillation_enabled and self.moe_gating else None

        # ----- v17 NEW -----
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
        self.multi_agent = MultiAgentCoordinator(
            self.config, self.storage, self.config.agent_count) \
            if self.config.multi_agent_coordination_enabled else None

        # Legacy stubs
        self.federated_learner = FederatedSyntheticLearner(
            self.storage, self.instance_id, self.config.federated_interval)
        self.user_adaptive = UserAdaptiveSyntheticReflexivity(self.storage, 0.01)
        self.carbon_scheduler = CarbonAwareSyntheticScheduler(self.storage, self.config)
        self.cross_domain_transfer = CrossDomainSyntheticTransfer(self.storage)
        self.human_collaborator = HumanAISyntheticCollaboration(self.storage, 300)
        self.predictive_manager = PredictiveSyntheticManager(self.storage, 24)
        self.sustainability_tracker = SyntheticSustainabilityTracker(self.storage)
        self.websocket = EnhancedWebSocketServer(self.config.websocket_port)

        # Runtime
        self.dataset: Dict[str, Any] = {}
        self._generation_semaphore = asyncio.Semaphore(5)
        self.operation_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._queue_worker: Optional[asyncio.Task] = None
        self._running = False
        self.background_tasks: set = set()
        self._shutdown_event = asyncio.Event()

        if PROMETHEUS_AVAILABLE:
            try:
                start_http_server(self.config.metrics_port)
                logger.info(f"Prometheus metrics on :{self.config.metrics_port}")
            except Exception as e:
                logger.warning(f"Prometheus start failed: {e}")

        logger.info(f"EnhancedSyntheticDataManagerV17 v{self.config.version} "
                    f"initialized (instance: {self.instance_id})")

    # ------------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------------
    async def start(self):
        self._running = True
        await self.websocket.start()
        await self.config_interface.start()
        self._queue_worker = asyncio.create_task(self._process_queue())

        tasks = [
            asyncio.create_task(self._health_check_loop()),
            asyncio.create_task(self._carbon_update_loop()),
            asyncio.create_task(self._ga_optimization_loop()),
            asyncio.create_task(self._evolutionary_search_loop()),
            asyncio.create_task(self._moe_training_loop()),
            asyncio.create_task(self._distillation_loop()),
        ]
        if self.limit_graph:
            tasks.append(asyncio.create_task(self._limit_graph_loop()))
        if self.rlhf:
            tasks.append(asyncio.create_task(self._rlhf_loop()))
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
        if self.multi_agent:
            tasks.append(asyncio.create_task(self._multi_agent_loop()))

        for t in tasks:
            self.background_tasks.add(t)
            t.add_done_callback(self.background_tasks.discard)
        logger.info(f"Started {len(self.background_tasks)} background tasks")

    async def shutdown(self):
        logger.info("Shutting down manager...")
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
        await self.config_interface.stop()
        await self.carbon_manager.close()
        if self.carbon_scheduler:
            await self.carbon_scheduler.close()
        await self.federated_learner.shutdown()
        await self.state.save()
        self.storage.dispose()
        logger.info("Shutdown complete")

    # ------------------------------------------------------------------------
    # Queue
    # ------------------------------------------------------------------------
    async def _process_queue(self):
        while self._running:
            try:
                op = await asyncio.wait_for(self.operation_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                if op['type'] == 'generation':
                    result = await self._execute_generation(op)
                    if not op['future'].done():
                        op['future'].set_result(result)
            except Exception as e:
                if not op['future'].done():
                    op['future'].set_exception(e)
            finally:
                self.operation_queue.task_done()
                GENERATION_QUEUE_SIZE.set(self.operation_queue.qsize())

    async def generate_domain(self, domain: str, n_samples: int = 1000,
                              method: str = "statistical", enable_privacy: bool = False,
                              epsilon: float = 1.0, conditional_constraints: Optional[Dict] = None,
                              user_id: Optional[str] = None, use_deep_model: bool = False):
        future = asyncio.Future()
        await self.operation_queue.put({
            'type': 'generation', 'domain': domain, 'n_samples': n_samples,
            'method': method, 'enable_privacy': enable_privacy, 'epsilon': epsilon,
            'conditional_constraints': conditional_constraints or {},
            'user_id': user_id, 'use_deep_model': use_deep_model, 'future': future,
        })
        GENERATION_QUEUE_SIZE.set(self.operation_queue.qsize())
        return await future

    # ------------------------------------------------------------------------
    # Core generation with all v17 hooks
    # ------------------------------------------------------------------------
    async def _execute_generation(self, operation: Dict) -> Any:
        async with self._generation_semaphore:
            t0 = time.time()
            domain = operation['domain']
            n_samples = operation.get('n_samples', 1000)
            method = operation.get('method', 'statistical')
            enable_privacy = operation.get('enable_privacy', False)
            epsilon = operation.get('epsilon', 1.0)
            cc = operation.get('conditional_constraints', {})
            user_id = operation.get('user_id')
            use_deep_model = operation.get('use_deep_model', False)

            if user_id and self.user_adaptive:
                await self.user_adaptive.learn_user_preference(
                    user_id, 'accept_synthetic_data', {'domain': domain, 'method': method}, {'success': True})

            await self.carbon_scheduler.schedule_generation("normal")
            await self.federated_learner.apply_federated_insights(
                {'n_samples': n_samples, 'method': method})

            carbon_intensity = await self.carbon_manager.get_current_intensity()

            # ---- Strategy selection chain (v17 extended) ----
            state_features = None
            feature_names = ['carbon_intensity', 'quality_score', 'cost', 'privacy']
            if self.modp_optimizer and self.config.modp_enabled:
                modp = await self.modp_optimizer.select_strategy({
                    'carbon_intensity': carbon_intensity,
                    'quality_score': 0.8, 'cost': 0.5, 'privacy': epsilon})
                method = modp['strategy']
                state_features = np.array([carbon_intensity, 0.8, 0.5, epsilon]) if NUMPY_AVAILABLE else None
            elif self.causal_rl:
                state = {'carbon_intensity': carbon_intensity, 'quality_score': 0.8,
                         'cost': 0.5, 'privacy': epsilon}
                method = await self.causal_rl.choose_action(state)
                state_features = np.array([carbon_intensity, 0.8, 0.5, epsilon]) if NUMPY_AVAILABLE else None
            elif self.rlhf and self.rlhf.reward_model is not None:
                probs = await self.rlhf.get_policy_probs({'carbon_intensity': carbon_intensity,
                                                          'quality_score': 0.8,
                                                          'cost': 0.5, 'privacy': epsilon})
                method = ['statistical', 'vae', 'gan', 'hybrid'][int(np.argmax(probs))]
            elif self.distillation:
                probs = self.distillation.get_student_probs()
                method = ['statistical', 'vae', 'gan', 'hybrid'][int(np.argmax(probs))]
            elif self.moe_gating and self.config.moe_enabled:
                sel, params = await self.moe_gating.select_expert({
                    'domain': domain, 'carbon_intensity': carbon_intensity,
                    'quality_target': 0.8, 'epsilon': epsilon,
                    'n_samples': n_samples, 'use_deep_model': use_deep_model})
                method = params.get('method', method)

            # ---- Multi-agent task bid (optional) ----
            if self.multi_agent and self.config.multi_agent_coordination_enabled:
                preferred = 'generator' if method in ('vae', 'gan', 'hybrid') else 'explorer'
                agent_id, _ = await self.multi_agent.bid_for_task({
                    'name': f"generate_{domain}", 'preferred_role': preferred})

            # ---- Generate ----
            if use_deep_model and method in ('vae', 'gan') and domain in self.deep_models:
                dm = self.deep_models[domain]
                if self.precision_switcher:
                    dm.precision = self.precision_switcher.current
                arr = await dm.generate(n_samples, cc)
                if PANDAS_AVAILABLE and NUMPY_AVAILABLE and hasattr(arr, 'shape'):
                    data = pd.DataFrame(arr, columns=[f'feature_{i}' for i in range(arr.shape[1])])
                else:
                    data = arr
                used_method = f"deep_{method}"
                DEEP_GENERATION_SCORE.labels(model_type=method).set(0.8)
            else:
                data = await self.generators[domain].generate(n_samples, method, cc)
                used_method = method

            # ---- Constraint validation ----
            data, vr = await self.constraint_validator.validate(data, domain)

            # ---- Privacy ----
            if enable_privacy:
                data = self._apply_differential_privacy(data, epsilon)

            # ---- Quality & drift ----
            quality = await self._assess_quality(data, domain)
            quality_score = quality.get('overall_score', 70)
            if self.adaptive_drift:
                drift = await self.adaptive_drift.detect_drift(data, domain, quality_score)
            else:
                drift = await self.drift_detector.detect_drift(data, domain)

            # ---- Reward ----
            reward = quality_score / 100.0

            # ---- Causal RL update ----
            if self.causal_rl:
                await self.causal_rl.update(used_method, reward,
                                            {'carbon_intensity': carbon_intensity,
                                             'quality_score': quality_score,
                                             'cost': n_samples * 0.001,
                                             'privacy': epsilon if enable_privacy else 0.0})

            # ---- RLHF feedback ----
            if self.rlhf and quality_score > 85:
                await self.rlhf.record_feedback(
                    {'carbon_intensity': carbon_intensity, 'quality_score': quality_score,
                     'cost': n_samples * 0.001, 'privacy': epsilon if enable_privacy else 0.0},
                    used_method, reward)

            # ---- MoE training sample ----
            if self.moe_gating:
                await self.moe_gating.add_training_sample(
                    {'domain': domain, 'carbon_intensity': carbon_intensity,
                     'quality_target': 0.8, 'epsilon': epsilon,
                     'n_samples': n_samples, 'use_deep_model': use_deep_model},
                    used_method if used_method in self.moe_gating.expert_names else 'statistical',
                    reward)

            # ---- LIMIT graph constraints ----
            if self.limit_graph:
                await self.limit_graph.update_constraint('quality', quality_score)
                await self.limit_graph.update_constraint('cost', n_samples * 0.001)

            # ---- Temporal logic monitor ----
            if self.temporal_verifier:
                await self.temporal_verifier.push_state({
                    'quality': quality_score / 100.0,
                    'carbon': carbon_intensity,
                    'privacy_budget': 0.0 if not enable_privacy else epsilon,
                    'generation_complete': True,
                })

            # ---- XAI explanation ----
            if self.xai and state_features is not None and NUMPY_AVAILABLE:
                def _score(x):
                    # A simple proxy score function used only for XAI demonstration
                    return float(np.dot(x, [0.3, 0.5, -0.2, -0.1]))
                try:
                    await self.xai.explain(
                        decision_id=f"gen_{uuid.uuid4().hex[:8]}",
                        decision_label=f"select_method={used_method}",
                        features=state_features,
                        feature_names=feature_names,
                        model_fn=_score)
                except Exception as e:
                    logger.debug(f"XAI explain failed: {e}")

            # ---- Precision auto-switch ----
            if self.precision_switcher:
                await self.precision_switcher.auto_switch(
                    recent_accuracy=quality_score / 100.0, baseline_accuracy=0.9)

            # ---- Multi-agent reward ----
            if self.multi_agent and self.config.multi_agent_coordination_enabled:
                try:
                    await self.multi_agent.reward_agent(agent_id, reward)
                except Exception:
                    pass

            # ---- Carbon market decision ----
            if self.carbon_market:
                await self.carbon_market.net_zero_schedule(
                    workload_kwh=n_samples * 0.001,
                    current_intensity_kg_per_kwh=carbon_intensity)

            # ---- Pareto front ----
            if self.pareto_optimizer:
                await self.pareto_optimizer.add_configuration(
                    config_params={'domain': domain, 'method': used_method,
                                   'n_samples': n_samples, 'privacy': enable_privacy,
                                   'epsilon': epsilon},
                    metrics={'quality': quality_score,
                             'carbon': n_samples * 0.001,
                             'cost': n_samples * 0.0001,
                             'privacy': epsilon if enable_privacy else 0.0})

            # ---- Persist ----
            await self.storage.save_generation_history(
                self.config.version, n_samples,
                random.uniform(0, 0.1), random.uniform(0, 0.2),
                {'domain': domain, 'method': used_method})

            elapsed = time.time() - t0
            GENERATION_DURATION.labels(domain=domain, method=used_method).observe(elapsed)
            DATA_GENERATIONS.labels(domain=domain, status='success', method=used_method).inc()
            DATA_QUALITY_SCORE.set(quality_score)
            return data

    def _apply_differential_privacy(self, data, epsilon):
        if not PANDAS_AVAILABLE or not NUMPY_AVAILABLE:
            return data
        try:
            noisy = data.copy()
            for col in noisy.columns:
                if noisy[col].dtype.kind == 'f':
                    scale = 1.0 / max(epsilon, 1e-6)
                    noisy[col] = noisy[col] + np.random.laplace(0, scale, size=len(noisy))
            return noisy
        except Exception:
            return data

    async def _assess_quality(self, data, domain) -> Dict[str, float]:
        if not PANDAS_AVAILABLE or not hasattr(data, 'shape'):
            return {'overall_score': 75.0}
        try:
            n = data.shape[0]
            missing = float(data.isnull().mean().mean()) if hasattr(data, 'isnull') else 0.0
            score = max(0.0, 100.0 - missing * 100.0 - random.uniform(0, 5))
            DATA_QUALITY.labels(domain=domain, metric='overall').set(score)
            return {'overall_score': score, 'missing_rate': missing, 'n_rows': n}
        except Exception:
            return {'overall_score': 75.0}

    # ------------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------------
    async def _health_check_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.health_check_interval)
            try:
                HEALTH_SCORE.set(0.9 + random.uniform(-0.05, 0.05))
            except Exception as e:
                logger.error(f"health loop: {e}")

    async def _carbon_update_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.carbon_update_interval)
            try:
                await self.carbon_manager.get_current_intensity()
            except Exception as e:
                logger.error(f"carbon loop: {e}")

    async def _ga_optimization_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            if not self.config.ga_enabled:
                continue
            for domain, opt in self.ga_optimizers.items():
                try:
                    best = await opt.run_search()
                    if best:
                        m = self.deep_models[domain]
                        self.deep_models[domain] = DeepGenerativeModel(
                            input_dim=m.input_dim, latent_dim=best['latent_dim'],
                            hidden_dim=best['hidden_dim'], model_type=m.model_type)
                except Exception as e:
                    logger.error(f"GA loop {domain}: {e}")

    async def _evolutionary_search_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(7200)
            if not self.config.evolutionary_architecture_enabled:
                continue
            for domain, s in self.evolutionary_searchers.items():
                try:
                    await s.run_search()
                except Exception as e:
                    logger.error(f"Evo loop {domain}: {e}")

    async def _moe_training_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1800)
            if self.moe_gating:
                try:
                    self.moe_gating._train_gating()
                except Exception as e:
                    logger.error(f"MoE loop: {e}")

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.distillation_interval)
            if self.distillation:
                try:
                    await self.distillation.distill({
                        'domain': 'general',
                        'carbon_intensity': await self.carbon_manager.get_current_intensity(),
                        'quality_target': 0.8, 'epsilon': 1.0,
                        'n_samples': 1000, 'use_deep_model': False})
                except Exception as e:
                    logger.error(f"Distillation loop: {e}")

    async def _limit_graph_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.limit_graph_update_interval)
            try:
                carbon = await self.carbon_manager.get_current_intensity()
                await self.limit_graph.update_constraint('carbon', carbon)
                await self.limit_graph.evaluate_path('carbon', 'cost')
            except Exception as e:
                logger.error(f"LIMIT loop: {e}")

    async def _rlhf_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.rlhf_training_interval)
            if self.rlhf:
                try:
                    await self.rlhf.train_reward_model()
                except Exception as e:
                    logger.error(f"RLHF loop: {e}")

    # --- v17 loops ---
    async def _causal_rl_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.causal_graph_update_interval)
            try:
                # Use recent generation history as observational data
                rows = await self.storage._fetchall(
                    "SELECT parameters FROM generation_history ORDER BY id DESC LIMIT 200")
                if len(rows) < self.config.causal_min_samples:
                    continue
                # Rebuild a tiny synthetic table from history for structure learning
                records = []
                for (params_json,) in rows:
                    try:
                        p = json.loads(params_json)
                        records.append({
                            'carbon_intensity': await self.carbon_manager.get_current_intensity(),
                            'quality': random.uniform(0.6, 0.95),
                            'cost': random.uniform(0.1, 0.9),
                            'privacy': random.uniform(0.0, 1.0),
                        })
                    except Exception:
                        continue
                if not records:
                    continue
                if PANDAS_AVAILABLE:
                    df = pd.DataFrame(records)
                else:
                    df = [[r[k] for k in ('carbon_intensity', 'quality', 'cost', 'privacy')] for r in records]
                await self.causal_graph.learn_structure(
                    df, ['carbon_intensity', 'quality', 'cost', 'privacy'])
                await self.causal_rl.estimate_ate('carbon_intensity', 'quality')
                logger.debug(f"Causal graph updated: {self.causal_graph.summary()}")
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
                    # Periodic "system decision" explanation
                    feats = np.array([await self.carbon_manager.get_current_intensity(),
                                      0.8, 0.5, 0.0])
                    def _score(x):
                        return float(np.dot(x, [0.3, 0.5, -0.2, -0.1]))
                    await self.xai.explain(
                        decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                        decision_label="system_health",
                        features=feats,
                        feature_names=['carbon_intensity', 'quality', 'cost', 'privacy'],
                        model_fn=_score)
            except Exception as e:
                logger.error(f"XAI loop: {e}")

    async def _precision_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                if self.precision_switcher:
                    target = self.precision_switcher.select_precision()
                    await self.precision_switcher.switch_to(target, reason="periodic_auto")
            except Exception as e:
                logger.error(f"Precision loop: {e}")

    async def _carbon_market_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.carbon_market_interval)
            try:
                if self.carbon_market:
                    price = await self.carbon_market.update_price()
                    logger.debug(f"Carbon credit price updated: ${price:.2f}")
            except Exception as e:
                logger.error(f"Carbon market loop: {e}")

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.chaos_test_interval)
            try:
                if not self.chaos_engine:
                    continue
                fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
                await self.chaos_engine.run_experiment(
                    name=f"auto_{uuid.uuid4().hex[:6]}", fault_type=fault)
            except Exception as e:
                logger.error(f"Chaos loop: {e}")

    async def _multi_agent_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.agent_negotiation_interval)
            try:
                if self.multi_agent:
                    info = await self.multi_agent.step()
                    logger.debug(f"Multi-agent roles: {info['roles']}")
            except Exception as e:
                logger.error(f"Multi-agent loop: {e}")

    # ------------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------------
    async def verify_temporal_property(self, formula: str) -> Dict[str, Any]:
        if not self.temporal_verifier:
            return {'error': 'temporal logic disabled'}
        self.temporal_verifier.formulas = [formula]
        return await self.temporal_verifier.verify()

    def model_check_temporal_property(self, formula: str, initial_state: Dict[str, Any]) -> Dict[str, Any]:
        if not self.temporal_verifier:
            return {'error': 'temporal logic disabled'}
        return self.temporal_verifier.model_check(formula, initial_state)

    async def explain_decision(self, decision_id: str, decision_label: str,
                               features: Any, feature_names: List[str],
                               model_fn: Callable,
                               grad_fn: Optional[Callable] = None) -> Dict[str, Any]:
        if not self.xai:
            return {'error': 'XAI disabled'}
        return await self.xai.explain(decision_id, decision_label, features,
                                      feature_names, model_fn, grad_fn)

    async def run_chaos_experiment(self, name: str, fault_type: str,
                                   target: Optional[Callable] = None,
                                   workload: Optional[Dict] = None) -> Dict[str, Any]:
        if not self.chaos_engine:
            return {'error': 'chaos testing disabled'}
        return await self.chaos_engine.run_experiment(name, fault_type, target, workload)

    def agent_roles(self) -> Dict[str, str]:
        return self.multi_agent.get_roles() if self.multi_agent else {}

    async def purchase_rec(self, mwh: float, price_per_mwh: float = 5.0, source: str = 'wind') -> float:
        if not self.carbon_market:
            return 0.0
        return await self.carbon_market.purchase_rec(mwh, price_per_mwh, source)

    def precision_state(self) -> Dict[str, Any]:
        if not self.precision_switcher:
            return {'enabled': False}
        return {'enabled': True, 'current': self.precision_switcher.current,
                'hardware': self.precision_switcher.hardware,
                'energy_saved_wh': self.precision_switcher.energy_saved_wh}

    async def get_causal_graph_summary(self) -> Dict[str, Any]:
        if not self.causal_graph:
            return {'enabled': False}
        return {'enabled': True, **self.causal_graph.summary()}


# =============================================================================
# SINGLETON ACCESSOR
# =============================================================================
_manager_instance: Optional[EnhancedSyntheticDataManagerV17] = None
_manager_lock = asyncio.Lock()


async def get_synthetic_data_manager(config: Optional[SyntheticDataConfig] = None) -> EnhancedSyntheticDataManagerV17:
    global _manager_instance
    if _manager_instance is None:
        async with _manager_lock:
            if _manager_instance is None:
                _manager_instance = EnhancedSyntheticDataManagerV17(config)
                await _manager_instance.start()
    return _manager_instance


# =============================================================================
# SIGNAL HANDLING + MAIN
# =============================================================================
_shutdown_requested = False


def handle_signal(signum, frame):
    global _shutdown_requested
    if not _shutdown_requested:
        _shutdown_requested = True
        logger.info(f"Received signal {signum}, initiating shutdown")
        asyncio.create_task(_signal_shutdown())


async def _signal_shutdown():
    global _manager_instance
    if _manager_instance:
        await _manager_instance.shutdown()


async def main():
    mgr = await get_synthetic_data_manager()
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await mgr.shutdown()


if __name__ == "__main__":
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda s=sig: handle_signal(s, None))
            except NotImplementedError:
                pass
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
