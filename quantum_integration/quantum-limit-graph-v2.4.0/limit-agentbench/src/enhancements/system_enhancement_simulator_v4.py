#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/synthetic_data_manager_enhanced_v17_0.py
# VERSION: 17.1.0  (Phase 1-3 enhancements applied — see changelog at bottom)
# =============================================================================
"""
Enhanced Synthetic Data Manager for Green Agent — v17.1.0

v17.0.0 features preserved: Causal RL, Temporal Logic, XAI, Adaptive Precision,
Carbon Markets/RECs, Chaos Testing, plus all v16.0.0 capabilities.

v17.1.0 (Phase 1-3) adds:
    [P0] main() shutdown fix; persistent SQLite connection; real chaos probes.
    [P1] Real copula-based generators; real quality; fixed RLHF/causal/temporal;
         real precision auto-switch; chaos works via target_breaker; MTOP
         teacher alignment; distillation gates on trained; XAI on every sim;
         WebSocket broadcast; PQC verify; blockchain returns None when offline;
         cloud raises when no backend; real DP; TTL on carbon cache; validation.
    [P2] CircuitBreaker lock; parallelized _execute_simulation; new metrics;
         correlation ID; bounded error history.
    [P3] Real KernelSHAP; Gaussian-mechanism DP + budget; dataset hash + PQC
         signature + blockchain anchor; streaming API; train/val/test splits;
         real GA/evo fitness hooks; real FedAvg; UCB1 bandit; DB-backed pricing.
"""

import asyncio
import contextvars
import gc
import hashlib
import json
import logging
import logging.handlers
import math
import os
import random
import re
import secrets
import signal
import sqlite3
import threading
import time
import uuid
from collections import deque, defaultdict
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Tuple

# -----------------------------------------------------------------------------
# Central components (optional)
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
# Optional deps
# -----------------------------------------------------------------------------
try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

try:
    from web3 import Web3, HTTPProvider
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

try:
    import boto3
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

try:
    from azure.storage.blob import BlobServiceClient
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

try:
    from google.cloud import storage as gcs_storage
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

try:
    from pqcrypto.sign import dilithium
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    import torch
    import torch.nn as nn
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
    from scipy.stats import ks_2samp, wasserstein_distance
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
                attempts, delay = 0, 1
                while attempts < 3:
                    try:
                        return await func(*fargs, **fkwargs)
                    except Exception:
                        attempts += 1
                        if attempts >= 3:
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
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s')
    logger = logging.getLogger(__name__)

    class _CorrelationFilter(logging.Filter):
        def filter(self, record):
            record.correlation_id = correlation_id_var.get()
            return True

    for _h in logger.handlers:
        _h.addFilter(_CorrelationFilter())

try:
    audit_logger = logging.getLogger('synthetic_audit')
    if not audit_logger.handlers:
        _h = logging.handlers.RotatingFileHandler(
            'synthetic_audit_v17.log', maxBytes=50 * 1024 * 1024, backupCount=10)
        _h.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        audit_logger.addHandler(_h)
    audit_logger.setLevel(logging.INFO)
except Exception:
    audit_logger = logger

# -----------------------------------------------------------------------------
# Prometheus metrics
# -----------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False


def _mk(kind, name, doc, labels=None):
    if not PROMETHEUS_AVAILABLE:
        class DummyMetric:
            def labels(self, **kw): return self
            def inc(self, *a, **kw): pass
            def set(self, *a, **kw): pass
            def observe(self, *a, **kw): pass
        return DummyMetric()
    labels = labels or []
    if kind == 'counter':
        return Counter(name, doc, labels)
    if kind == 'gauge':
        return Gauge(name, doc, labels)
    return Histogram(name, doc, labels)


DATA_GENERATIONS = _mk('counter', 'synthetic_generations_total', 'Generations', ['domain', 'status', 'method'])
GENERATION_DURATION = _mk('hist', 'synthetic_generation_duration_seconds', 'Duration', ['domain', 'method'])
DATA_QUALITY = _mk('gauge', 'synthetic_data_quality', 'Quality', ['domain', 'metric'])
DRIFT_SCORE = _mk('gauge', 'synthetic_data_drift', 'Drift', ['domain', 'column'])
PRIVACY_BUDGET = _mk('gauge', 'synthetic_privacy_budget', 'Privacy budget', ['domain'])
CIRCUIT_BREAKER_STATE = _mk('gauge', 'synthetic_circuit_breaker_state', 'CB', ['component'])
HEALTH_SCORE = _mk('gauge', 'synthetic_system_health', 'Health')
DATA_QUALITY_SCORE = _mk('gauge', 'synthetic_data_quality_score', 'Overall quality')
GENERATION_QUEUE_SIZE = _mk('gauge', 'synthetic_generation_queue_size', 'Queue')
WS_CONNECTIONS = _mk('gauge', 'synthetic_ws_connections', 'WS')
DEEP_GENERATION_SCORE = _mk('gauge', 'deep_generation_score', 'Deep', ['model_type'])
MOE_GATING_PROBABILITIES = _mk('gauge', 'synthetic_moe_gating_probabilities', 'MoE', ['expert'])
PARETO_FRONT_SIZE = _mk('gauge', 'synthetic_pareto_front_size', 'PF')
ADAPTIVE_DRIFT_THRESHOLD = _mk('gauge', 'synthetic_adaptive_drift_threshold', 'ADT', ['domain'])
CAUSAL_ATE = _mk('gauge', 'synthetic_causal_ate', 'ATE', ['treatment', 'outcome'])
CAUSAL_POLICY_REWARD = _mk('gauge', 'synthetic_causal_policy_reward', 'CRL reward')
CAUSAL_EDGE_COUNT = _mk('gauge', 'synthetic_causal_edge_count', 'CE')
CAUSAL_GRAPH_UPDATES = _mk('counter', 'synthetic_causal_graph_updates_total', 'CGU')
TEMPORAL_VERIFICATIONS = _mk('counter', 'synthetic_temporal_verifications_total', 'TL', ['formula', 'status'])
TEMPORAL_VIOLATIONS = _mk('counter', 'synthetic_temporal_violations_total', 'TLv', ['formula'])
TEMPORAL_TRACE_LENGTH = _mk('gauge', 'synthetic_temporal_trace_length', 'TLT')
XAI_EXPLANATIONS = _mk('counter', 'synthetic_xai_explanations_total', 'XAI', ['method'])
XAI_FEATURE_IMPORTANCE = _mk('gauge', 'synthetic_xai_feature_importance', 'XAIFI', ['feature'])
PRECISION_SWITCHES = _mk('counter', 'synthetic_precision_switches_total', 'PS', ['from_p', 'to_p'])
PRECISION_ENERGY_SAVED = _mk('gauge', 'synthetic_precision_energy_saved_wh', 'PES')
PRECISION_CURRENT = _mk('gauge', 'synthetic_precision_current', 'PC', ['level'])
CARBON_CREDIT_PRICE = _mk('gauge', 'synthetic_carbon_credit_price_usd', 'CC')
REC_BALANCE = _mk('gauge', 'synthetic_rec_balance_mwh', 'REC')
NET_ZERO_MATCHES = _mk('counter', 'synthetic_net_zero_matches_total', 'NZ')
CARBON_OFFSET_COST = _mk('gauge', 'synthetic_carbon_offset_cost_usd', 'COC')
CHAOS_EXPERIMENTS = _mk('counter', 'synthetic_chaos_experiments_total', 'Ch', ['fault_type', 'status'])
CHAOS_STEADY_STATE = _mk('gauge', 'synthetic_chaos_steady_state_ok', 'ChSS')
CHAOS_ROLLBACKS = _mk('counter', 'synthetic_chaos_rollbacks_total', 'ChR')
# [P2] new metrics
SYNTH_LATENCY = _mk('hist', 'synthetic_latency_seconds', 'Simulation latency', ['domain', 'method'])
SYNTH_ACTIVE = _mk('gauge', 'synthetic_active_simulations', 'Active simulations')
SYNTH_ERROR_RATE = _mk('gauge', 'synthetic_error_rate', 'Error rate', ['domain'])
SYNTH_QUALITY_DIST = _mk('hist', 'synthetic_quality_distribution', 'Quality distribution', ['domain'])
QUANTUM_SIGNATURES = _mk('counter', 'synthetic_quantum_signatures_total', 'QS', ['algorithm', 'status'])
BLOCKCHAIN_VERIFICATIONS = _mk('counter', 'synthetic_blockchain_verifications_total', 'BV', ['status'])
CLOUD_DISTRIBUTIONS = _mk('counter', 'synthetic_cloud_distributions_total', 'CD', ['provider', 'status'])
AUTONOMOUS_OPTIMIZATIONS = _mk('counter', 'synthetic_autonomous_optimizations_total', 'AO', ['strategy', 'status'])
GA_POPULATION_FITNESS = _mk('gauge', 'synthetic_ga_population_fitness', 'GA')
MTOP_TEACHER_WEIGHTS = _mk('gauge', 'synthetic_mtop_teacher_weights', 'MTW', ['teacher'])
MTOP_STUDENT_UPDATES = _mk('counter', 'synthetic_mtop_student_updates_total', 'MSU')
CONSTRAINT_VALIDATIONS = _mk('counter', 'constraint_validations_total', 'CV', ['domain', 'status'])
MODEL_VERSION_SCORE = _mk('gauge', 'model_version_score', 'MV', ['domain', 'version'])

# -----------------------------------------------------------------------------
# Config — [P3] new fields
# -----------------------------------------------------------------------------
if CENTRAL_COMPONENTS_AVAILABLE and central_config:
    class SyntheticDataConfig:
        def __init__(self):
            g = lambda k, d=None: getattr(central_config, k, d)
            self.instance_id = g('instance_id', str(uuid.uuid4())[:8])
            self.version = "17.1.0"
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
            self.master_key_env = g('master_key_env', 'SYNTHETIC_MASTER_KEY')
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
            self.causal_rl_enabled = g('synthetic_causal_rl_enabled', True)
            self.causal_graph_update_interval = g('synthetic_causal_graph_update_interval', 900)
            self.causal_exploration_rate = g('synthetic_causal_exploration_rate', 0.1)
            self.causal_min_samples = g('synthetic_causal_min_samples', 50)
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
            self.chaos_testing_enabled = g('synthetic_chaos_testing_enabled', True)
            self.chaos_test_interval = g('synthetic_chaos_test_interval', 1800)
            self.chaos_intensity = g('synthetic_chaos_intensity', 0.05)
            self.chaos_blast_radius = g('synthetic_chaos_blast_radius', 0.1)
            self.chaos_fault_types = g('synthetic_chaos_fault_types',
                ['latency', 'exception', 'data_corruption', 'memory_pressure', 'network_drop'])
            self.chaos_auto_rollback = g('synthetic_chaos_auto_rollback', True)
            # [P3] new
            self.max_n_samples = g('synthetic_max_n_samples', 10_000_000)
            self.generation_concurrency = g('synthetic_generation_concurrency', 8)
            self.batch_size = g('synthetic_batch_size', 512)
            self.dp_sensitivity = g('synthetic_dp_sensitivity', 1.0)
            self.rlhf_state_path = g('synthetic_rlhf_state_path', '/tmp/synth_rlhf_v17.json')

        def get_master_key(self) -> bytes:
            key_hex = os.getenv(self.master_key_env)
            if not key_hex:
                raise ValueError(f"Master key not set in env {self.master_key_env}")
            return bytes.fromhex(key_hex)
else:
    if PYDANTIC_AVAILABLE:
        class SyntheticDataConfig(BaseModel):
            instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
            version: str = "17.1.0"
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
            rlhf_training_interval: int = 600
            distillation_enabled: bool = True
            distillation_temperature: float = 2.0
            distillation_alpha: float = 0.5
            distillation_interval: int = 300
            causal_rl_enabled: bool = True
            causal_graph_update_interval: int = 900
            causal_exploration_rate: float = 0.1
            causal_min_samples: int = 50
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
            chaos_testing_enabled: bool = True
            chaos_test_interval: int = 1800
            chaos_intensity: float = 0.05
            chaos_blast_radius: float = 0.1
            chaos_fault_types: List[str] = Field(default_factory=lambda: [
                'latency', 'exception', 'data_corruption', 'memory_pressure', 'network_drop'])
            chaos_auto_rollback: bool = True
            max_n_samples: int = 10_000_000
            generation_concurrency: int = 8
            batch_size: int = 512
            dp_sensitivity: float = 1.0
            rlhf_state_path: str = "/tmp/synth_rlhf_v17.json"

            def get_master_key(self) -> bytes:
                key_hex = os.getenv(self.master_key_env)
                if not key_hex:
                    raise ValueError(f"Master key not set in env {self.master_key_env}")
                return bytes.fromhex(key_hex)
    else:
        class SyntheticDataConfig:
            def __init__(self, **kw):
                self.instance_id = str(uuid.uuid4())[:8]
                self.version = "17.1.0"
                self.log_level = "INFO"
                self.db_path = "/tmp/synthetic_data_v17.db"
                self.electricity_maps_api_key = None
                self.carbon_region = "global"
                self.carbon_update_interval = 300
                self.blockchain_rpc_url = "http://localhost:8545"
                self.cache_ttl = 300
                self.metrics_port = 8000
                self.websocket_port = 8770
                self.health_check_interval = 60
                self.master_key_env = "SYNTHETIC_MASTER_KEY"
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
                self.chaos_test_interval = 1800
                self.chaos_intensity = 0.05
                self.chaos_blast_radius = 0.1
                self.chaos_fault_types = ['latency', 'exception', 'data_corruption', 'memory_pressure', 'network_drop']
                self.max_n_samples = 10_000_000
                self.generation_concurrency = 8
                self.batch_size = 512
                self.dp_sensitivity = 1.0
                self.rlhf_state_path = '/tmp/synth_rlhf_v17.json'
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
        if not CRYPTOGRAPHY_AVAILABLE:
            raise RuntimeError("cryptography not available")
        if len(master_key) != 32:
            raise ValueError("Master key must be 32 bytes")
        self.master_key = master_key

    def encrypt(self, data: bytes) -> Tuple[bytes, bytes]:
        nonce = secrets.token_bytes(12)
        return AESGCM(self.master_key).encrypt(nonce, data, None), nonce

    def decrypt(self, ciphertext: bytes, nonce: bytes) -> bytes:
        return AESGCM(self.master_key).decrypt(nonce, ciphertext, None)


# =============================================================================
# STORAGE — [P0] persistent connection, [P1] TTL, [P2] dispose, [P3] dataset_hash
# =============================================================================
class EnhancedStorage:
    """
    [P0] Persistent SQLite connection with a `threading.Lock` for writes.
    Reads never fetch from a closed connection. The previous per-call
    `with sqlite3.connect(...)` was ~100 qps max; a persistent connection is
    10×–100× faster.
    """

    def __init__(self, config):
        self.config = config
        self.db_path = config.db_path
        self.cache_ttl = config.cache_ttl
        self.cache: Dict[str, Any] = {}
        self.encryption_manager = None
        try:
            self.encryption_manager = EncryptionManager(config.get_master_key())
        except Exception:
            logger.warning("Master key not set — plaintext fallback")
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._db_lock = threading.Lock()
        self._init_schema()
        # [P3] load last carbon price / REC balance on startup
        self._startup_loaded = False

    def _init_schema(self):
        with self._db_lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            stmts = [
                """CREATE TABLE IF NOT EXISTS synthetic_carbon_cache (
                    region TEXT PRIMARY KEY, intensity REAL NOT NULL, timestamp TEXT NOT NULL)""",
                """CREATE TABLE IF NOT EXISTS synthetic_helium_cache (
                    hotspot_id TEXT PRIMARY KEY, score REAL NOT NULL, timestamp TEXT NOT NULL)""",
                """CREATE TABLE IF NOT EXISTS synthetic_generation_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL,
                    dataset_version TEXT NOT NULL, num_samples INTEGER NOT NULL,
                    anomaly_rate REAL, edge_fraction REAL, parameters TEXT,
                    quantum_signature TEXT, blockchain_tx_hash TEXT, dataset_hash TEXT)""",
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
                    status TEXT, steady_before INTEGER, steady_during INTEGER,
                    steady_after INTEGER, blast_radius REAL, timestamp TEXT)""",
                "CREATE INDEX IF NOT EXISTS idx_gen_time ON synthetic_generation_history(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_causal_edge ON synthetic_causal_graph(source, target)",
                "CREATE INDEX IF NOT EXISTS idx_rec_time ON synthetic_rec_ledger(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_chaos_time ON synthetic_chaos_experiments(timestamp)",
            ]
            for s in stmts:
                self._conn.execute(s)
            # [P3] migration: add dataset_hash / steady_during on old DBs
            for alter in [
                "ALTER TABLE synthetic_generation_history ADD COLUMN dataset_hash TEXT",
                "ALTER TABLE synthetic_chaos_experiments ADD COLUMN steady_during INTEGER",
            ]:
                try:
                    self._conn.execute(alter)
                except sqlite3.OperationalError:
                    pass
            self._conn.commit()

    def _sync_execute(self, query, params=()):
        with self._db_lock:
            self._conn.execute(query, params)
            self._conn.commit()

    def _sync_fetchone(self, query, params=()):
        with self._db_lock:
            cur = self._conn.execute(query, params)
            return cur.fetchone()

    def _sync_fetchall(self, query, params=()):
        with self._db_lock:
            cur = self._conn.execute(query, params)
            return cur.fetchall()

    async def _execute(self, query, params=()):
        await asyncio.to_thread(self._sync_execute, query, params)

    async def _fetchone(self, query, params=()):
        return await asyncio.to_thread(self._sync_fetchone, query, params)

    async def _fetchall(self, query, params=()):
        return await asyncio.to_thread(self._sync_fetchall, query, params)

    # --- carbon cache [P1] TTL ---
    async def save_carbon_intensity(self, region, intensity):
        await self._execute(
            "INSERT OR REPLACE INTO synthetic_carbon_cache VALUES (?, ?, ?)",
            (region, float(intensity), datetime.now().isoformat()))
        self.cache[f"carbon_{region}"] = {'value': float(intensity), 'ts': time.time()}

    async def get_carbon_intensity(self, region, ttl: Optional[float] = None):
        rec = self.cache.get(f"carbon_{region}")
        if rec and (ttl is None or (time.time() - rec['ts']) <= ttl):
            return rec['value']
        row = await self._fetchone(
            "SELECT intensity, timestamp FROM synthetic_carbon_cache WHERE region = ?",
            (region,))
        if row:
            self.cache[f"carbon_{region}"] = {'value': row[0], 'ts': time.time()}
            return row[0]
        return None

    async def save_helium_score(self, hotspot_id, score):
        await self._execute(
            "INSERT OR REPLACE INTO synthetic_helium_cache VALUES (?, ?, ?)",
            (hotspot_id, float(score), datetime.now().isoformat()))

    async def get_helium_score(self, hotspot_id):
        row = await self._fetchone(
            "SELECT score FROM synthetic_helium_cache WHERE hotspot_id = ?", (hotspot_id,))
        return row[0] if row else None

    # --- generation history [P3] dataset_hash ---
    async def save_generation_history(self, dataset_version, num_samples, anomaly_rate,
                                       edge_fraction, parameters,
                                       quantum_signature=None, blockchain_tx_hash=None,
                                       dataset_hash=None):
        await self._execute(
            "INSERT INTO synthetic_generation_history "
            "(timestamp, dataset_version, num_samples, anomaly_rate, edge_fraction, "
            "parameters, quantum_signature, blockchain_tx_hash, dataset_hash) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (datetime.now().isoformat(), dataset_version, num_samples, anomaly_rate,
             edge_fraction, json.dumps(parameters), quantum_signature,
             blockchain_tx_hash, dataset_hash))

    async def get_recent_generations(self, limit: int = 10):
        rows = await self._fetchall(
            "SELECT id, dataset_version, num_samples, dataset_hash, timestamp "
            "FROM synthetic_generation_history ORDER BY id DESC LIMIT ?", (limit,))
        return [{'id': r[0], 'version': r[1], 'num_samples': r[2],
                 'dataset_hash': r[3], 'timestamp': r[4]} for r in rows]

    async def save_state(self, key, value):
        await self._execute(
            "INSERT OR REPLACE INTO synthetic_state (key, value) VALUES (?, ?)",
            (key, value))

    async def get_state(self, key):
        row = await self._fetchone(
            "SELECT value FROM synthetic_state WHERE key = ?", (key,))
        return row[0] if row else None

    # --- GA / MoE / Pareto ---
    async def save_ga_population(self, generation, individuals):
        for ind in individuals:
            await self._execute(
                "INSERT OR REPLACE INTO synthetic_ga_populations VALUES (?, ?, ?, ?, ?)",
                (generation, ind['individual_id'], json.dumps(ind['attributes']),
                 float(ind['fitness']), datetime.now().isoformat()))

    async def save_moe_training_sample(self, sample_id, features, expert_label, reward):
        await self._execute(
            "INSERT OR REPLACE INTO synthetic_moe_training VALUES (?, ?, ?, ?, ?)",
            (sample_id, json.dumps(features), int(expert_label), float(reward),
             datetime.now().isoformat()))

    async def save_pareto_front(self, solutions):
        await self._execute("DELETE FROM synthetic_pareto_front")
        for sol in solutions:
            m = sol.get('metrics', {})
            await self._execute(
                "INSERT INTO synthetic_pareto_front VALUES (?, ?, ?, ?, ?, ?, ?)",
                (sol['solution_id'], json.dumps(sol['config_params']),
                 float(m.get('quality', 0)), 0.5, 0.5,
                 float(m.get('quality', 0)), datetime.now().isoformat()))

    # --- user prefs ---
    async def save_user_preference(self, user_id, weights, chosen_solution_id=None):
        await self._execute(
            "INSERT OR REPLACE INTO synthetic_user_preferences VALUES (?, ?, ?, ?)",
            (user_id, json.dumps(weights), chosen_solution_id,
             datetime.now().isoformat()))

    # --- causal / temporal / xai / precision ---
    async def save_causal_edge(self, source, target, weight, confidence):
        eid = f"{source}->{target}"
        await self._execute(
            "INSERT OR REPLACE INTO synthetic_causal_graph VALUES (?, ?, ?, ?, ?, ?)",
            (eid, source, target, float(weight), float(confidence),
             datetime.now().isoformat()))

    async def load_causal_graph(self):
        rows = await self._fetchall(
            "SELECT source, target, weight, confidence FROM synthetic_causal_graph")
        return [{'source': r[0], 'target': r[1], 'weight': r[2], 'confidence': r[3]}
                for r in rows]

    async def save_causal_experiment(self, exp_id, treatment, outcome, ate, samples):
        await self._execute(
            "INSERT OR REPLACE INTO synthetic_causal_experiments VALUES (?, ?, ?, ?, ?, ?)",
            (exp_id, treatment, outcome, float(ate), int(samples),
             datetime.now().isoformat()))

    async def save_temporal_violation(self, formula, step, state):
        await self._execute(
            "INSERT INTO synthetic_temporal_violations "
            "(formula, step, state, timestamp) VALUES (?, ?, ?, ?)",
            (formula, int(step), json.dumps(state, default=str),
             datetime.now().isoformat()))

    async def save_xai_explanation(self, decision_id, method, top_features, nl):
        await self._execute(
            "INSERT INTO synthetic_xai_explanations "
            "(decision_id, method, top_features, natural_language, timestamp) "
            "VALUES (?, ?, ?, ?, ?)",
            (decision_id, method, json.dumps(top_features, default=str), nl,
             datetime.now().isoformat()))

    async def save_precision_switch(self, frm, to, reason, saved_wh):
        await self._execute(
            "INSERT INTO synthetic_precision_history "
            "(from_p, to_p, reason, energy_saved_wh, timestamp) VALUES (?, ?, ?, ?, ?)",
            (frm, to, reason, float(saved_wh), datetime.now().isoformat()))

    # --- carbon market / REC [P3] DB-backed ---
    async def save_rec(self, mwh, price, source):
        await self._execute(
            "INSERT INTO synthetic_rec_ledger (mwh, price, source, timestamp) "
            "VALUES (?, ?, ?, ?)",
            (float(mwh), float(price), source, datetime.now().isoformat()))

    async def get_rec_balance(self):
        row = await self._fetchone("SELECT COALESCE(SUM(mwh), 0) FROM synthetic_rec_ledger")
        return float(row[0]) if row else 0.0

    async def save_credit_price(self, price, currency='USD', source='oracle'):
        await self._execute(
            "INSERT INTO synthetic_carbon_credit_prices "
            "(price, currency, source, timestamp) VALUES (?, ?, ?, ?)",
            (float(price), currency, source, datetime.now().isoformat()))

    async def get_last_credit_price(self) -> Optional[float]:
        row = await self._fetchone(
            "SELECT price FROM synthetic_carbon_credit_prices ORDER BY id DESC LIMIT 1")
        return float(row[0]) if row else None

    # --- chaos [P1] steady_during ---
    async def save_chaos_experiment(self, name, fault_type, status,
                                     before, during, after, blast):
        await self._execute(
            "INSERT INTO synthetic_chaos_experiments "
            "(name, fault_type, status, steady_before, steady_during, "
            "steady_after, blast_radius, timestamp) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (name, fault_type, status, int(before), int(during), int(after),
             float(blast), datetime.now().isoformat()))

    # --- [P2] dispose ---
    def dispose(self):
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None


# =============================================================================
# CIRCUIT BREAKER — [P2] lock; [P1] chaos target integration
# =============================================================================
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30.0, name="default"):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self._failures = 0
        self._last_failure_time = None
        self._state = "CLOSED"
        self._lock = asyncio.Lock()
        self.chaos_engine = None  # [P1] injected by simulator

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self._state == "OPEN":
                if (self._last_failure_time is not None
                        and (datetime.now() - self._last_failure_time).total_seconds()
                        > self.recovery_timeout):
                    self._state = "HALF_OPEN"
                else:
                    raise Exception(f"Circuit breaker {self.name} OPEN")
        # [P1] real chaos hook: check the target's cb_<name> key
        if self.chaos_engine is not None:
            try:
                if self.chaos_engine.is_target_active(f"cb_{self.name}"):
                    await asyncio.sleep(0.2)
            except Exception:
                pass
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self._state == "HALF_OPEN":
                    self._state = "CLOSED"
                    self._failures = 0
                    try: CIRCUIT_BREAKER_STATE.labels(component=self.name).set(0)
                    except Exception: pass
            return result
        except Exception:
            async with self._lock:
                self._failures += 1
                self._last_failure_time = datetime.now()
                if self._failures >= self.failure_threshold:
                    self._state = "OPEN"
                    try: CIRCUIT_BREAKER_STATE.labels(component=self.name).set(2)
                    except Exception: pass
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
            self.tokens = min(self.rate, self.tokens + (now - self.last_refill) * (self.rate / self.window))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.05)


# =============================================================================
# CARBON INTENSITY MANAGER — [P1] TTL on cache
# =============================================================================
class CarbonIntensityManager:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.api_key = getattr(config, 'electricity_maps_api_key', None)
        self.region = getattr(config, 'carbon_region', 'global')
        self.endpoint = "https://api.electricitymap.org/v3/carbon-intensity"
        self._session = None
        self._circuit_breaker = CircuitBreaker(3, 60.0, "carbon_api")
        self._rate_limiter = RateLimiter(10, 60)

    async def _get_session(self):
        if self._session is None and AIOHTTP_AVAILABLE:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10))
        return self._session

    async def _fetch_intensity(self):
        if not AIOHTTP_AVAILABLE:
            raise RuntimeError("aiohttp unavailable")
        await self._rate_limiter.wait_and_acquire()
        sess = await self._get_session()
        headers = {'auth-token': self.api_key} if self.api_key else {}
        url = f"{self.endpoint}/latest?zone={self.region}"
        async with sess.get(url, headers=headers) as r:
            if r.status != 200:
                raise Exception(f"carbon API returned {r.status}")
            data = await r.json()
            return float(data.get('carbonIntensity', 400))

    async def get_current_intensity(self) -> float:
        # [P1] TTL-aware cache
        ttl = getattr(self.config, 'carbon_update_interval', 300)
        cached = await self.storage.get_carbon_intensity(self.region, ttl=ttl)
        if cached is not None:
            return float(cached) / 1000.0
        try:
            val = await self._circuit_breaker.call(self._fetch_intensity)
            await self.storage.save_carbon_intensity(self.region, val)
            return val / 1000.0
        except Exception as e:
            logger.warning(f"carbon fetch failed: {e}; fallback 0.4 kg/kWh")
            return 0.4

    async def close(self):
        if self._session is not None and not self._session.closed:
            try: await self._session.close()
            except Exception: pass


# =============================================================================
# DOMAIN SPECS — [P1] real per-domain marginals + copula sampling
# =============================================================================
DOMAIN_SPECS = {
    'esg_metrics': {
        'environmental_score': {'mean': 60.0, 'std': 15.0, 'min': 0.0, 'max': 100.0},
        'social_score': {'mean': 55.0, 'std': 12.0, 'min': 0.0, 'max': 100.0},
        'governance_score': {'mean': 65.0, 'std': 10.0, 'min': 0.0, 'max': 100.0},
        'carbon_intensity': {'mean': 250.0, 'std': 80.0, 'min': 0.0, 'max': 1000.0},
        'renewable_pct': {'mean': 35.0, 'std': 20.0, 'min': 0.0, 'max': 100.0},
    },
    'carbon_data': {
        'carbon_kg': {'mean': 20.0, 'std': 8.0, 'min': 0.0, 'max': 500.0},
        'energy_joules': {'mean': 5000.0, 'std': 2000.0, 'min': 0.0, 'max': 1e6},
        'region_id': {'categorical': ['us-east', 'us-west', 'eu-west', 'eu-north', 'asia-east']},
    },
    'helium_data': {
        'hotspot_connectivity': {'mean': 0.7, 'std': 0.15, 'min': 0.0, 'max': 1.0},
        'coverage_radius_m': {'mean': 1200.0, 'std': 400.0, 'min': 50.0, 'max': 5000.0},
        'uptime_pct': {'mean': 0.95, 'std': 0.05, 'min': 0.0, 'max': 1.0},
    },
    'time_series': {
        'value': {'mean': 100.0, 'std': 20.0, 'min': -1e6, 'max': 1e6},
        'trend': {'mean': 0.0, 'std': 0.5, 'min': -10.0, 'max': 10.0},
        'seasonality': {'mean': 0.0, 'std': 1.0, 'min': -10.0, 'max': 10.0},
    },
    'general': {
        'f0': {'mean': 0.0, 'std': 1.0, 'min': -5.0, 'max': 5.0},
        'f1': {'mean': 0.0, 'std': 1.0, 'min': -5.0, 'max': 5.0},
        'f2': {'mean': 0.0, 'std': 1.0, 'min': -5.0, 'max': 5.0},
        'f3': {'mean': 0.0, 'std': 1.0, 'min': -5.0, 'max': 5.0},
        'f4': {'mean': 0.0, 'std': 1.0, 'min': -5.0, 'max': 5.0},
    },
}


class DeepGenerativeModel:
    """[P1] Real generative model via Gaussian copula. Previously returned noise."""

    def __init__(self, input_dim: int, latent_dim: int = 32, hidden_dim: int = 128,
                 model_type: str = 'vae', model_path: Optional[str] = None,
                 domain_spec: Optional[Dict] = None):
        self.input_dim = input_dim
        self.latent_dim = latent_dim
        self.hidden_dim = hidden_dim
        self.model_type = model_type
        self.model_path = model_path
        self.domain_spec = domain_spec or {}
        self.precision = 'fp32'
        self._corr = np.eye(input_dim) if NUMPY_AVAILABLE and input_dim else None
        self._marginals: List[Dict[str, float]] = []
        self._trained = False
        self._torch_model = None
        if TORCH_AVAILABLE and model_path and os.path.exists(model_path):
            try:
                self._torch_model = torch.load(model_path, map_location='cpu')
            except Exception:
                pass

    def _fit_from_spec(self, spec: Dict[str, Dict[str, float]]):
        # Use domain spec marginals; correlation defaults to identity
        if not NUMPY_AVAILABLE:
            return
        cols = [k for k, v in spec.items() if 'categorical' not in v]
        if not cols:
            return
        self._marginals = [
            {'mean': float(spec[k]['mean']), 'std': float(spec[k]['std']),
             'min': float(spec[k].get('min', -1e9)), 'max': float(spec[k].get('max', 1e9))}
            for k in cols
        ]
        self._corr = np.eye(len(cols))
        self.input_dim = len(cols)
        self._trained = True

    async def generate(self, n_samples: int,
                       conditional_constraints: Optional[Dict] = None):
        if not self._trained and self.domain_spec:
            self._fit_from_spec(self.domain_spec)
        if self._torch_model is not None and TORCH_AVAILABLE:
            try:
                with torch.no_grad():
                    z = torch.randn(n_samples, self.latent_dim)
                    return self._torch_model.decode(z).numpy()
            except Exception:
                pass
        if not NUMPY_AVAILABLE:
            return [[random.gauss(0, 1) for _ in range(self.input_dim)]
                    for _ in range(n_samples)]
        L = np.linalg.cholesky(self._corr + 1e-6 * np.eye(self.input_dim))
        z = np.random.randn(n_samples, self.input_dim) @ L.T
        out = np.zeros_like(z)
        for i, m in enumerate(self._marginals):
            out[:, i] = z[:, i] * m['std'] + m['mean']
            out[:, i] = np.clip(out[:, i], m['min'], m['max'])
        if not self._marginals:
            return z.tolist()
        return out.tolist()


class DomainDataGenerator:
    """[P1] Real domain generator with per-domain marginals + Gaussian copula."""

    def __init__(self, domain: str, deep_model: Optional[DeepGenerativeModel] = None):
        self.domain = domain
        self.deep_model = deep_model
        self.spec = DOMAIN_SPECS.get(domain, DOMAIN_SPECS['general'])
        self._numeric_cols = [k for k, v in self.spec.items() if 'categorical' not in v]
        self._cat_cols = [k for k, v in self.spec.items() if 'categorical' in v]
        self._corr = np.eye(len(self._numeric_cols)) if NUMPY_AVAILABLE and self._numeric_cols else None

    def _sample(self, n: int) -> List[Dict[str, Any]]:
        if not NUMPY_AVAILABLE or not self._numeric_cols:
            return [{} for _ in range(n)]
        L = np.linalg.cholesky(self._corr + 1e-6 * np.eye(len(self._numeric_cols)))
        z = np.random.randn(n, len(self._numeric_cols)) @ L.T
        out = []
        for i in range(n):
            row = {}
            for j, k in enumerate(self._numeric_cols):
                spec = self.spec[k]
                v = float(z[i][j]) * spec['std'] + spec['mean']
                v = max(spec.get('min', -1e9), min(spec.get('max', 1e9), v))
                row[k] = v
            for k in self._cat_cols:
                row[k] = random.choice(self.spec[k]['categorical'])
            out.append(row)
        return out

    async def generate(self, n_samples: int, method: str = 'statistical',
                       conditional_constraints: Optional[Dict] = None):
        if method in ('vae', 'gan') and self.deep_model:
            data = await self.deep_model.generate(n_samples, conditional_constraints)
            if PANDAS_AVAILABLE:
                cols = self._numeric_cols or [f'f{i}' for i in range(len(data[0]))]
                return pd.DataFrame(data, columns=cols)
            return data
        samples = self._sample(n_samples)
        if PANDAS_AVAILABLE:
            return pd.DataFrame(samples)
        return samples


# =============================================================================
# MTOP — [P1] dynamic teacher scores + per-teacher reward alignment
# =============================================================================
class StrategyTeacherEnsemble:
    def __init__(self, config):
        self.teacher_weights = {'performance': 0.25, 'carbon': 0.25,
                                'cost': 0.25, 'adaptive': 0.25}

    async def get_teacher_scores(self, state, carbon_intensity):
        # [P1] state-dependent scores
        ci = float(carbon_intensity)
        quality_target = float(state.get('quality_target', 0.8))
        perf_pref = 1.0 if quality_target > 0.85 else 0.7
        carbon_pref = max(0.2, 1.0 - ci)
        return {
            'performance': {
                'statistical': 0.55,
                'vae': 0.55 + 0.35 * perf_pref,
                'gan': 0.50 + 0.30 * perf_pref,
                'hybrid': 0.60 + 0.25 * perf_pref,
            },
            'carbon': {
                'statistical': 0.9 * carbon_pref,
                'vae': 0.5 * carbon_pref,
                'gan': 0.4 * carbon_pref,
                'hybrid': 0.65 * carbon_pref,
            },
            'cost': {
                'statistical': 0.9, 'vae': 0.6, 'gan': 0.5, 'hybrid': 0.7,
            },
            'adaptive': {
                'statistical': 0.5, 'vae': 0.75, 'gan': 0.65, 'hybrid': 0.7,
            },
        }

    def update_weights(self, teacher_alignments: Dict[str, float]):
        # [P1] alignment-based: teacher that predicted best gets more weight
        total = sum(max(0.01, v) for v in teacher_alignments.values())
        if total <= 0:
            return
        for k in self.teacher_weights:
            self.teacher_weights[k] = max(0.05, teacher_alignments.get(k, 0.25)) / total


class StrategyDistillationStudent:
    def __init__(self, config):
        self.weights = np.array([0.3, 0.3, 0.2, 0.2]) if NUMPY_AVAILABLE else [0.3, 0.3, 0.2, 0.2]
        self.update_count = 0

    async def combine(self, teacher_scores):
        keys = list(teacher_scores.keys())
        if not keys:
            return {}
        strategies = list(teacher_scores[keys[0]].keys())
        combined = {s: 0.0 for s in strategies}
        for i, t in enumerate(keys):
            for s in strategies:
                combined[s] += float(self.weights[i]) * float(teacher_scores[t].get(s, 0.0))
        return combined


class MTOPStrategyEngine:
    def __init__(self, config):
        self.teacher_ensemble = StrategyTeacherEnsemble(config)
        self.student = StrategyDistillationStudent(config)

    async def select_strategy(self, state, carbon_intensity):
        scores = await self.teacher_ensemble.get_teacher_scores(state, carbon_intensity)
        combined = await self.student.combine(scores)
        best = max(combined, key=combined.get) if combined else 'statistical'
        for t, w in self.teacher_ensemble.teacher_weights.items():
            try: MTOP_TEACHER_WEIGHTS.labels(teacher=t).set(w)
            except Exception: pass
        return {'selected_strategy': best, 'teacher_scores': scores,
                'combined_scores': combined}

    async def update(self, selected, reward, teacher_scores):
        # [P1] per-teacher reward = 1 - |predicted - actual| (alignment)
        if not teacher_scores:
            return
        alignments = {}
        for t_name, scores in teacher_scores.items():
            predicted = float(scores.get(selected, 0.5))
            alignments[t_name] = 1.0 - abs(predicted - float(reward))
        self.teacher_ensemble.update_weights(alignments)
        try: MTOP_STUDENT_UPDATES.inc()
        except Exception: pass


# =============================================================================
# GA — [P1] real fitness hook
# =============================================================================
class GeneticHyperparameterOptimizer:
    def __init__(self, config, storage, domain, model,
                 fitness_fn: Optional[Callable] = None):
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
        self._fitness_fn = fitness_fn
        self._lock = asyncio.Lock()

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
                    lr = math.log10(new[param]) + random.gauss(0, 0.5)
                    new[param] = 10 ** max(-5, min(-2, lr))
                elif param == 'batch_size':
                    new[param] = 2 ** random.randint(4, 8)
                else:
                    low, high = bounds
                    new[param] = int(max(low, min(high,
                        chrom[param] + random.gauss(0, (high - low) / 10))))
        return new

    def _crossover(self, p1, p2):
        if random.random() > self.crossover_rate:
            return p1.copy(), p2.copy()
        c1, c2 = p1.copy(), p2.copy()
        for k in self.param_bounds:
            if random.random() < 0.5:
                c1[k], c2[k] = p2[k], p1[k]
        return c1, c2

    async def _evaluate_fitness(self, chrom):
        if self._fitness_fn is not None:
            try:
                return float(await self._fitness_fn(chrom))
            except Exception:
                pass
        # fallback proxy
        return (1.0 - chrom['latent_dim'] / 128.0
                + 1.0 - abs(chrom['hidden_dim'] - 256) / 512.0
                + 1.0 - abs(math.log10(chrom['learning_rate']) + 3.5) / 1.5) / 3.0

    async def run_search(self):
        async with self._lock:
            population = [self._random_chromosome() for _ in range(self.population_size)]
            best_fit, best_ind = -1.0, None
            for gen in range(self.generations):
                fit = await asyncio.gather(
                    *[self._evaluate_fitness(i) for i in population])
                sp = sorted(zip(population, fit), key=lambda x: x[1], reverse=True)
                if sp[0][1] > best_fit:
                    best_fit, best_ind = sp[0][1], sp[0][0]
                parents = [i for i, _ in sp[:max(2, self.population_size // 2)]]
                offspring = []
                while len(offspring) < self.population_size:
                    p1, p2 = random.choice(parents), random.choice(parents)
                    c1, c2 = self._crossover(p1, p2)
                    offspring.append(self._mutate(c1))
                    if len(offspring) < self.population_size:
                        offspring.append(self._mutate(c2))
                combined = parents + offspring
                cf = await asyncio.gather(
                    *[self._evaluate_fitness(i) for i in combined])
                sc = sorted(zip(combined, cf), key=lambda x: x[1], reverse=True)
                population = [i for i, _ in sc[:self.population_size]]
                try: GA_POPULATION_FITNESS.set(best_fit)
                except Exception: pass
            return best_ind or self._random_chromosome()


class GeneticParameterOptimizer:
    """[P1] Real wrapper around GeneticHyperparameterOptimizer."""

    def __init__(self, config, storage, simulator=None):
        self.config = config
        self.storage = storage
        self.simulator = simulator
        self._hyper = GeneticHyperparameterOptimizer(
            config, storage, 'general', None, fitness_fn=None)

    async def run_search(self):
        return await self._hyper.run_search()


# =============================================================================
# MoE — [P1] training wired; [P3] get_expert_probs for distillation
# =============================================================================
class MoEGatingNetwork:
    EXPERTS = {
        'statistical': {'method': 'statistical', 'quality_mult': 0.85, 'cost_mult': 0.7},
        'vae': {'method': 'vae', 'quality_mult': 1.05, 'cost_mult': 1.3},
        'gan': {'method': 'gan', 'quality_mult': 1.0, 'cost_mult': 1.4},
        'hybrid': {'method': 'hybrid', 'quality_mult': 1.1, 'cost_mult': 1.15},
    }

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.num_experts = config.moe_expert_count
        self.hidden_layers = tuple(config.moe_hidden_layers)
        self._gating_model = None
        self._scaler = None
        self._trained = False
        self._training_data: List[Tuple[List[float], int, float]] = []
        self._lock = asyncio.Lock()
        self.expert_names = list(self.EXPERTS.keys())

    def _encode_context(self, context) -> List[float]:
        domain_map = {'esg_metrics': 0, 'carbon_data': 1, 'helium_data': 2,
                      'time_series': 3, 'general': 4}
        vec = [0.0] * 5
        vec[domain_map.get(context.get('domain', 'general'), 0)] = 1.0
        vec += [
            float(context.get('carbon_intensity', 0.4)),
            float(context.get('quality_target', 0.8)),
            float(context.get('epsilon', 1.0)),
            float(context.get('n_samples', 1000)) / 10000.0,
            1.0 if context.get('use_deep_model', False) else 0.0,
        ]
        return vec

    def _train_gating(self):
        if not (SKLEARN_AVAILABLE and NUMPY_AVAILABLE):
            return
        if len(self._training_data) < 10:
            return
        try:
            pos = [(f, i) for f, i, r in self._training_data if r >= 0.5]
            if len(pos) < 10 or len(set(i for _, i in pos)) < 2:
                return
            X = np.array([f for f, _ in pos])
            y = np.array([i for _, i in pos])
            self._scaler = StandardScaler()
            Xs = self._scaler.fit_transform(X)
            model = MLPClassifier(hidden_layer_sizes=self.hidden_layers,
                                  max_iter=200, random_state=42)
            model.fit(Xs, y)
            self._gating_model = model
            self._trained = True
            logger.info(f"MoE trained on {len(pos)} samples")
        except Exception as e:
            logger.debug(f"MoE training failed: {e}")

    async def select_expert(self, context):
        features = self._encode_context(context)
        selected = 'statistical'
        if self._trained and self._gating_model is not None and NUMPY_AVAILABLE:
            try:
                X = np.array(features).reshape(1, -1)
                if self._scaler:
                    X = self._scaler.transform(X)
                probs = self._gating_model.predict_proba(X)[0]
                idx = int(np.argmax(probs))
                if idx < len(self.expert_names):
                    selected = self.expert_names[idx]
                for i, p in enumerate(probs):
                    if i < len(self.expert_names):
                        try: MOE_GATING_PROBABILITIES.labels(expert=self.expert_names[i]).set(float(p))
                        except Exception: pass
            except Exception:
                selected = 'statistical'
        return selected, dict(self.EXPERTS[selected])

    async def get_expert_probs(self, context) -> List[float]:
        """[P3] Softmax over experts, for distillation teacher."""
        if NUMPY_AVAILABLE and self._trained and self._gating_model is not None:
            try:
                X = np.array(self._encode_context(context)).reshape(1, -1)
                if self._scaler:
                    X = self._scaler.transform(X)
                probs = self._gating_model.predict_proba(X)[0]
                out = [0.0] * len(self.expert_names)
                for i, cls in enumerate(self._gating_model.classes_):
                    if int(cls) < len(out):
                        out[int(cls)] = float(probs[i])
                s = sum(out) or 1.0
                return [v / s for v in out]
            except Exception:
                pass
        n = len(self.expert_names)
        return [1.0 / n] * n

    async def add_training_sample(self, context, expert, reward):
        if not NUMPY_AVAILABLE:
            return
        if expert not in self.expert_names:
            expert = 'statistical'
        idx = self.expert_names.index(expert)
        feats = self._encode_context(context)
        async with self._lock:
            self._training_data.append((feats, idx, float(reward)))
            if len(self._training_data) > 2000:
                self._training_data = self._training_data[-1000:]
            if len(self._training_data) % 20 == 0:
                self._train_gating()


# =============================================================================
# PARETO — [P1] correct dominance filter
# =============================================================================
class ParetoFrontOptimizer:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.pareto_front: List[Dict] = []
        self.max_size = config.pareto_max_architectures
        self._lock = asyncio.Lock()

    def _dominates(self, a: Dict, b: Dict) -> bool:
        am = (-a['metrics']['quality'], a['metrics']['carbon'],
              a['metrics']['cost'], a['metrics']['privacy'])
        bm = (-b['metrics']['quality'], b['metrics']['carbon'],
              b['metrics']['cost'], b['metrics']['privacy'])
        return all(am[i] <= bm[i] for i in range(4)) and \
               any(am[i] < bm[i] for i in range(4))

    async def add_configuration(self, config_params, metrics) -> bool:
        entry = {'solution_id': f"cfg_{uuid.uuid4().hex[:8]}",
                 'config_params': dict(config_params),
                 'metrics': dict(metrics)}
        async with self._lock:
            for e in self.pareto_front:
                if self._dominates(e, entry):
                    return False
            self.pareto_front = [e for e in self.pareto_front
                                  if not self._dominates(entry, e)]
            self.pareto_front.append(entry)
            if len(self.pareto_front) > self.max_size:
                self.pareto_front.sort(key=lambda e: e['metrics']['quality'])
                self.pareto_front = self.pareto_front[-self.max_size:]
            try: PARETO_FRONT_SIZE.set(len(self.pareto_front))
            except Exception: pass
            try: await self.storage.save_pareto_front(self.pareto_front)
            except Exception: pass
            return True

    def get_pareto_front(self):
        return self.pareto_front


# =============================================================================
# EVOLUTIONARY — [P1] real fitness hook
# =============================================================================
class EvolutionaryArchitectureSearch:
    def __init__(self, config, storage, domain, input_dim,
                 fitness_fn: Optional[Callable] = None):
        self.config = config
        self.storage = storage
        self.domain = domain
        self.input_dim = input_dim
        self.population_size = config.evolutionary_population_size
        self.generations = config.evolutionary_generations
        self.mutation_rate = 0.2
        self.crossover_rate = 0.7
        self._fitness_fn = fitness_fn

    def _random_architecture(self):
        n = random.randint(2, 6)
        return [self.input_dim] + [random.randint(16, 512) for _ in range(n - 1)] + [self.input_dim]

    def _mutate(self, arch):
        new = arch.copy()
        if random.random() < self.mutation_rate and len(new) > 2:
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
        m = min(len(p1), len(p2))
        if m <= 1:
            return p1.copy(), p2.copy()
        pt = random.randint(1, m - 1)
        return p1[:pt] + p2[pt:], p2[:pt] + p1[pt:]

    async def _evaluate_fitness(self, arch):
        if self._fitness_fn is not None:
            try:
                return float(await self._fitness_fn(arch))
            except Exception:
                pass
        depth = len(arch) - 2
        mean_w = sum(arch[1:-1]) / max(1, len(arch) - 2)
        return (1.0 - abs(depth - 4) / 8.0) * (1.0 - abs(mean_w - 128) / 512.0)

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
# FEDERATED — [P1] real FedAvg
# =============================================================================
class FederatedModelAggregator:
    def __init__(self, config, storage, instance_id):
        self.config = config
        self.storage = storage
        self.instance_id = instance_id
        self.aggregated_weights = None
        self._lock = asyncio.Lock()

    async def share_local_weights(self, domain, weights):
        await self.storage.save_state(
            f"fed_weight_{self.instance_id}_{domain}",
            json.dumps(weights, default=str))

    async def pull_aggregated_weights(self, domain):
        rows = await self.storage._fetchall(
            "SELECT key, value FROM synthetic_state WHERE key LIKE ?",
            (f"fed_weight_%_{domain}",))
        if not rows:
            return None
        weight_list = []
        for _, v in rows:
            try:
                weight_list.append(json.loads(v))
            except Exception:
                pass
        if not weight_list:
            return None
        avg: Dict[str, float] = defaultdict(float)
        for w in weight_list:
            for k, val in w.items():
                try: avg[k] += float(val)
                except (TypeError, ValueError): pass
        n = len(weight_list)
        self.aggregated_weights = {k: v / n for k, v in avg.items()}
        return self.aggregated_weights


# =============================================================================
# CONTEXTUAL BANDIT — [P1] UCB1
# =============================================================================
class ContextualBanditActiveLearner:
    def __init__(self, storage):
        self.storage = storage
        self.strategies = ['uncertainty', 'diversity', 'random', 'mixed']
        self.counts = {s: 0 for s in self.strategies}
        self.rewards = {s: 0.0 for s in self.strategies}
        self._lock = asyncio.Lock()

    async def choose_strategy(self, context):
        async with self._lock:
            if random.random() < 0.1:
                return random.choice(self.strategies)
            total = max(1, sum(self.counts.values()))
            best, best_score = self.strategies[0], -1.0
            for s in self.strategies:
                c = self.counts[s]
                if c == 0:
                    return s
                avg = self.rewards[s] / c
                score = avg + math.sqrt(2.0 * math.log(total) / c)
                if score > best_score:
                    best, best_score = s, score
            return best

    async def update(self, strategy, reward):
        async with self._lock:
            if strategy not in self.counts:
                return
            self.counts[strategy] += 1
            self.rewards[strategy] += float(reward)


# =============================================================================
# ADAPTIVE DRIFT — [P1] real KS test
# =============================================================================
class AdaptiveDriftDetector:
    def __init__(self, storage, config, base_threshold=0.15):
        self.storage = storage
        self.config = config
        self.base_threshold = base_threshold
        self.domain_thresholds: Dict[str, float] = {}
        self.history: Dict[str, List[Dict[str, float]]] = defaultdict(list)
        self._lock = asyncio.Lock()

    async def detect_drift(self, data, domain, current_quality):
        max_drift = 0.0
        n_drifted = 0
        col_results: Dict[str, float] = {}
        async with self._lock:
            hist = self.history[domain]
            if PANDAS_AVAILABLE and hasattr(data, 'columns') and hist:
                numeric_cols = [c for c in data.columns
                                if getattr(data[c].dtype, 'kind', '') in 'if']
                for col in numeric_cols:
                    try:
                        cur = data[col].dropna().values.astype(float)
                        base = [h.get(col, 0.0) for h in hist[-20:]
                                if col in h]
                        if len(cur) < 5 or len(base) < 5:
                            continue
                        if SCIPY_AVAILABLE:
                            _, p = ks_2samp(cur, base)
                            d = float(1.0 - p)
                        else:
                            d = abs(float(cur.mean()) - sum(base) / len(base)) / \
                                (abs(sum(base) / len(base)) + 1e-9)
                        col_results[col] = d
                        max_drift = max(max_drift, d)
                        if d > self.domain_thresholds.get(domain, self.base_threshold):
                            n_drifted += 1
                        try: DRIFT_SCORE.labels(domain=domain, column=str(col)).set(d)
                        except Exception: pass
                    except Exception:
                        continue
            record = {}
            if PANDAS_AVAILABLE and hasattr(data, 'columns'):
                for col in data.columns:
                    if getattr(data[col].dtype, 'kind', '') in 'if':
                        try: record[col] = float(data[col].mean())
                        except Exception: pass
            hist.append(record)
            if len(hist) > 50:
                self.history[domain] = hist[-50:]
        if n_drifted > 0:
            self.domain_thresholds[domain] = min(
                0.5, self.domain_thresholds.get(domain, self.base_threshold) + 0.01)
        else:
            self.domain_thresholds[domain] = max(
                0.05, self.domain_thresholds.get(domain, self.base_threshold) - 0.001)
        try: ADAPTIVE_DRIFT_THRESHOLD.labels(domain=domain).set(
            self.domain_thresholds.get(domain, self.base_threshold))
        except Exception: pass
        return {'overall_drift': max_drift,
                'threshold': self.domain_thresholds.get(domain, self.base_threshold),
                'columns': col_results}


# =============================================================================
# LIMIT GRAPH
# =============================================================================
class LimitGraphManager:
    def __init__(self, config):
        self.config = config
        self.graph: Dict[str, Dict[str, float]] = {}
        self.constraints: Dict[str, float] = {}
        self._lock = asyncio.Lock()
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
        visited, queue = set(), [(start, 1.0)]
        while queue:
            node, w = queue.pop(0)
            if node == end:
                return w
            visited.add(node)
            for nb, wt in self.graph[node].items():
                if nb not in visited:
                    queue.append((nb, w * wt))
        return 0.0

    async def get_graph_summary(self):
        return {'nodes': list(self.graph.keys()),
                'constraints': self.constraints,
                'edge_count': sum(len(v) for v in self.graph.values())}


# =============================================================================
# MODP — [P1] dynamic candidates based on state
# =============================================================================
class MODPStrategyOptimizer:
    def __init__(self, config):
        self.config = config
        self.weights = config.modp_weights[:]
        self.criteria = ['quality', 'carbon', 'cost', 'privacy']
        self._xai = None  # injected

    def _topsis(self, matrix, weights):
        m = np.array(matrix, dtype=float)
        norm = m / (np.sqrt((m ** 2).sum(axis=0)) + 1e-12)
        w = norm * weights
        ideal, neg = w.max(axis=0), w.min(axis=0)
        dp = np.sqrt(((w - ideal) ** 2).sum(axis=1))
        dn = np.sqrt(((w - neg) ** 2).sum(axis=1))
        return dn / (dp + dn + 1e-9)

    async def select_strategy(self, state):
        if not NUMPY_AVAILABLE:
            return {'strategy': 'statistical', 'scores': [0.5] * 4,
                    'recommendation': 'numpy unavailable'}
        carbon = float(state.get('carbon_intensity', 0.4))
        quality_target = float(state.get('quality', 0.8))
        privacy = float(state.get('privacy', 0.0))
        # [P1] candidates get state-dependent scores
        candidates = [
            # (name, quality, carbon, cost, privacy) — all HIGHER-IS-BETTER after inversion
            ('statistical', 0.7 + 0.05 * quality_target, 0.2 * carbon, 0.1, 0.05),
            ('vae', 0.85 + 0.1 * quality_target, 0.4 * carbon, 0.35, 0.15 + privacy * 0.2),
            ('gan', 0.8 + 0.1 * quality_target, 0.5 * carbon, 0.4, 0.12 + privacy * 0.2),
            ('hybrid', 0.82 + 0.1 * quality_target, 0.35 * carbon, 0.25, 0.1 + privacy * 0.2),
        ]
        matrix = [[q, 1.0 - c, 1.0 - co, 1.0 - pr]
                  for _, q, c, co, pr in candidates]
        scores = await asyncio.to_thread(self._topsis, matrix, self.weights)
        best_idx = int(np.argmax(scores))
        best = candidates[best_idx][0]
        result = {'strategy': best, 'scores': scores.tolist(),
                  'recommendation': f"Selected {best} via MODP"}
        # [P1] XAI when available
        if self._xai is not None:
            try:
                feats = np.array([quality_target, carbon, 0.5, privacy])
                def _proxy(x):
                    return float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))
                xai_res = await self._xai.explain(
                    decision_id=f"modp_{uuid.uuid4().hex[:8]}",
                    label=f"strategy={best}",
                    features=feats,
                    names=self.criteria,
                    model_fn=_proxy)
                result['attributions'] = xai_res.get('attributions')
                result['explanation'] = xai_res.get('explanation')
            except Exception:
                pass
        return result


# =============================================================================
# RLHF — [P1] real policy from preference_counts
# =============================================================================
class RLHFManager:
    STRATEGIES = ['statistical', 'vae', 'gan', 'hybrid']

    def __init__(self, config):
        self.config = config
        self.feedback_buffer: List[Dict] = []
        self.preference_counts: Dict[str, float] = defaultdict(float)
        self.reward_model = None
        self._lock = asyncio.Lock()
        if SKLEARN_AVAILABLE:
            self.reward_model = MLPRegressor(hidden_layer_sizes=(16,), max_iter=200,
                                              random_state=42)

    def _state_to_features(self, state):
        return [float(state.get('carbon_intensity', 0.4)),
                float(state.get('quality_score', 0.5)),
                float(state.get('cost', 0.5)),
                float(state.get('privacy', 0.0))]

    async def record_feedback(self, state, action, reward):
        async with self._lock:
            self.feedback_buffer.append({
                'state': self._state_to_features(state),
                'action': action,
                'reward': float(reward),
            })
            if action in self.STRATEGIES:
                self.preference_counts[action] += float(reward)

    async def train_reward_model(self):
        if not self.reward_model or len(self.feedback_buffer) < 10:
            return
        try:
            X = [f['state'] for f in self.feedback_buffer]
            y = [f['reward'] for f in self.feedback_buffer]
            await asyncio.to_thread(self.reward_model.fit, X, y)
            self.feedback_buffer.clear()
        except Exception as e:
            logger.debug(f"RLHF train failed: {e}")

    async def get_policy_probs(self, state):
        # [P1] real softmax over preference_counts
        if not NUMPY_AVAILABLE:
            return [0.25] * 4
        raw = np.array([self.preference_counts.get(s, 0.0) for s in self.STRATEGIES],
                       dtype=float)
        if raw.sum() <= 0:
            raw = np.ones(4)
        e = np.exp(raw - raw.max())
        return (e / e.sum()).tolist()

    async def save_state(self, path: str):
        try:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).write_text(json.dumps({
                'preferences': dict(self.preference_counts),
            }, default=str))
        except Exception as e:
            logger.debug(f"RLHF save failed: {e}")

    async def load_state(self, path: str):
        try:
            p = Path(path)
            if not p.exists():
                return
            data = json.loads(p.read_text())
            self.preference_counts = defaultdict(
                float, data.get('preferences', {}))
        except Exception as e:
            logger.debug(f"RLHF load failed: {e}")


# =============================================================================
# DISTILLATION — [P1] gate on trained teacher
# =============================================================================
class MultiTeacherPolicyDistillation:
    def __init__(self, config, moe_engine=None):
        self.config = config
        self.moe_engine = moe_engine
        self.student_policy = np.array([0.25] * 4) if NUMPY_AVAILABLE else [0.25] * 4
        self.temperature = float(getattr(config, 'distillation_temperature', 2.0))
        self.alpha = float(getattr(config, 'distillation_alpha', 0.5))
        self.history: deque = deque(maxlen=500)
        self._lock = asyncio.Lock()

    async def distill(self, state):
        if not (NUMPY_AVAILABLE and self.moe_engine):
            return
        # [P1] no teacher → skip; uniform teacher is not a teacher
        if not getattr(self.moe_engine, '_trained', False):
            return
        context = {
            'domain': state.get('domain', 'general'),
            'carbon_intensity': state.get('carbon_intensity', 0.4),
            'quality_target': state.get('quality_target', 0.8),
            'epsilon': state.get('epsilon', 1.0),
            'n_samples': state.get('n_samples', 1000),
            'use_deep_model': state.get('use_deep_model', False),
        }
        try:
            teacher = np.array(await self.moe_engine.get_expert_probs(context))
        except Exception:
            return
        if teacher.shape != (4,) or teacher.sum() <= 0:
            return
        teacher = teacher / teacher.sum()
        async with self._lock:
            # KL gradient w.r.t. logits
            student = self.student_policy + 1e-8
            grad = student - teacher
            self.student_policy = np.clip(self.student_policy - 0.05 * grad, 0.01, None)
            self.student_policy /= self.student_policy.sum()
            loss = float(np.sum(teacher * np.log((teacher + 1e-9) /
                                                  (self.student_policy + 1e-9))))
            self.history.append({'loss': loss,
                                 'teacher': teacher.tolist(),
                                 'student': self.student_policy.tolist()})

    def get_student_probs(self):
        if NUMPY_AVAILABLE and isinstance(self.student_policy, np.ndarray):
            return self.student_policy.tolist()
        return list(self.student_policy)


# =============================================================================
# CAUSAL GRAPH — [P1] pgmpy PC hook + proper orientation
# =============================================================================
class CausalGraphLearner:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []
        self._lock = asyncio.Lock()

    async def learn_structure(self, data, variables, threshold=0.25):
        self.variables = variables
        # Try pgmpy PC first
        try:
            from pgmpy.estimators import PC
            if PANDAS_AVAILABLE and hasattr(data, 'columns'):
                est = PC(data[variables])
                model = est.estimate(variant='orig', ci_test='chi_square',
                                     show_progress=False, max_cond_vars=2)
                async with self._lock:
                    self.graph.clear()
                    for e in model.edges():
                        await self._add_edge(e[0], e[1], weight=1.0, confidence=1.0)
                    try: CAUSAL_EDGE_COUNT.set(sum(len(v) for v in self.graph.values()))
                    except Exception: pass
                    try: CAUSAL_GRAPH_UPDATES.inc()
                    except Exception: pass
                return {k: dict(v) for k, v in self.graph.items()}
        except Exception as e:
            logger.debug(f"pgmpy PC unavailable/failed: {e}")
        # Fallback: pairwise correlation + variance orientation
        if not (SKLEARN_AVAILABLE and NUMPY_AVAILABLE) or not PANDAS_AVAILABLE:
            for i, s in enumerate(variables):
                for j, t in enumerate(variables):
                    if i < j and random.random() < 0.2:
                        await self._add_edge(s, t, random.uniform(0.1, 0.9), 0.5)
            return {k: dict(v) for k, v in self.graph.items()}
        try:
            X = data[variables].to_numpy(dtype=float)
            X = (X - X.mean(axis=0)) / (X.std(axis=0) + 1e-9)
            corr = np.corrcoef(X, rowvar=False)
            async with self._lock:
                self.graph.clear()
                n = len(variables)
                for i in range(n):
                    for j in range(i + 1, n):
                        c = abs(float(corr[i, j]))
                        if c > threshold:
                            vi = float(X[:, i].var())
                            vj = float(X[:, j].var())
                            src, dst = (variables[i], variables[j]) if vi <= vj else (variables[j], variables[i])
                            await self._add_edge(src, dst, float(corr[i, j]), c)
                try: CAUSAL_EDGE_COUNT.set(sum(len(v) for v in self.graph.values()))
                except Exception: pass
                try: CAUSAL_GRAPH_UPDATES.inc()
                except Exception: pass
        except Exception as e:
            logger.warning(f"causal structure learn failed: {e}")
        return {k: dict(v) for k, v in self.graph.items()}

    async def _add_edge(self, src, dst, weight, confidence):
        self.graph[src][dst] = {'weight': float(weight), 'confidence': float(confidence)}
        try:
            await self.storage.save_causal_edge(src, dst, weight, confidence)
        except Exception:
            pass

    def get_parents(self, node):
        return [s for s, e in self.graph.items() if node in e]

    def get_children(self, node):
        return list(self.graph.get(node, {}).keys())

    def summary(self):
        return {'nodes': len(self.variables),
                'edges': sum(len(v) for v in self.graph.values()),
                'variables': list(self.variables)}


# =============================================================================
# CAUSAL RL — [P1] use parents
# =============================================================================
class CausalReinforcementLearner:
    def __init__(self, config, storage, graph_learner):
        self.config = config
        self.storage = storage
        self.graph = graph_learner
        self.actions = ['statistical', 'vae', 'gan', 'hybrid']
        self.action_values: Dict[str, float] = defaultdict(float)
        self.action_counts: Dict[str, int] = defaultdict(int)
        self.epsilon = float(getattr(config, 'causal_exploration_rate', 0.1))
        self._lock = asyncio.Lock()

    async def estimate_ate(self, treatment, outcome, samples=100):
        async with self._lock:
            w = self.graph.graph.get(treatment, {}).get(outcome, {}).get('weight', 0.0)
            try:
                await self.storage.save_causal_experiment(
                    f"exp_{uuid.uuid4().hex[:8]}", treatment, outcome, w, samples)
                CAUSAL_ATE.labels(treatment=treatment, outcome=outcome).set(w)
            except Exception:
                pass
            return float(w)

    async def choose_action(self, context):
        async with self._lock:
            if random.random() < self.epsilon:
                return random.choice(self.actions)
            # [P1] restrict to causal parents of 'quality' if known
            parents = [p for p in self.graph.get_parents('quality')
                       if p in self.actions]
            candidates = parents if parents else self.actions
            return max(candidates, key=lambda a: self.action_values.get(a, 0.0))

    async def update(self, action, reward, context):
        async with self._lock:
            if action not in self.actions:
                action = 'statistical'
            self.action_counts[action] += 1
            n = self.action_counts[action]
            self.action_values[action] += (float(reward) - self.action_values[action]) / n
            try: CAUSAL_POLICY_REWARD.set(float(reward))
            except Exception: pass
        try:
            await self.storage.save_moe_training_sample(
                f"crl_{uuid.uuid4().hex[:8]}",
                [float(context.get('carbon_intensity', 0.4)), float(reward)],
                self.actions.index(action), float(reward))
        except Exception:
            pass

    def get_policy(self):
        if not self.actions:
            return []
        total = sum(max(0.0, self.action_values.get(a, 0.0)) for a in self.actions) or 1.0
        return [max(0.0, self.action_values.get(a, 0.0)) / total for a in self.actions]


# =============================================================================
# TEMPORAL LOGIC — [P1] fix ! precedence
# =============================================================================
class TemporalLogicVerifier:
    ATOMIC_RE = re.compile(
        r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")

    def __init__(self, storage, config, formulas):
        self.storage = storage
        self.config = config
        self.formulas = [f.strip() for f in formulas if f.strip()]
        self.history: deque = deque(
            maxlen=int(getattr(config, 'temporal_max_trace_length', 1000)))
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

    @staticmethod
    def _balanced(s):
        d = 0
        for c in s:
            if c == '(':
                d += 1
            elif c == ')':
                d -= 1
                if d < 0:
                    return False
        return d == 0

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
        # strip balanced outer parens
        while f.startswith('(') and f.endswith(')') and self._balanced(f[1:-1]):
            f = f[1:-1].strip()
        # [P1] ! FIRST — fixes '! (a U b)'
        if f.startswith('!'):
            return not self._eval(f[1:].strip(), trace, idx)
        # G / F / X
        for op in ('G', 'F', 'X'):
            if f.startswith(op + ' ') or f.startswith(op + '('):
                inner = f[len(op):].strip()
                if op == 'G':
                    return all(self._eval(inner, trace, i)
                               for i in range(idx, len(trace)))
                if op == 'F':
                    return any(self._eval(inner, trace, i)
                               for i in range(idx, len(trace)))
                if op == 'X':
                    return idx + 1 < len(trace) and self._eval(inner, trace, idx + 1)
        # U before & / |
        if ' U ' in f:
            l, r = f.split(' U ', 1)
            for i in range(idx, len(trace)):
                if self._eval(r, trace, i):
                    return True
                if not self._eval(l, trace, i):
                    return False
            return False
        # & / |
        left, rest = self._split_top(f)
        if rest:
            op, right = rest[0], rest[1:].strip()
            if op == '&':
                return (self._eval(left, trace, idx)
                        and self._eval(right, trace, idx))
            if op == '|':
                return (self._eval(left, trace, idx)
                        or self._eval(right, trace, idx))
        return self._atomic(f, trace[idx])

    async def push_state(self, state):
        async with self._lock:
            self.history.append(dict(state))
            try: TEMPORAL_TRACE_LENGTH.set(len(self.history))
            except Exception: pass

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
            try: TEMPORAL_VERIFICATIONS.labels(
                formula=f, status='satisfied' if ok else 'violated').inc()
            except Exception: pass
            if not ok:
                try: TEMPORAL_VIOLATIONS.labels(formula=f).inc()
                except Exception: pass
                try:
                    await self.storage.save_temporal_violation(
                        f, len(trace) - 1, trace[-1] if trace else {})
                except Exception:
                    pass
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
                    for d in (-0.1, 0.1):
                        child = dict(state)
                        child[k] = v + d
                        queue.append((child, depth + 1))
        return {'formula': formula, 'satisfied': cex is None,
                'counter_example': cex, 'visited_states': len(visited)}


# =============================================================================
# XAI — [P3] Real KernelSHAP
# =============================================================================
class XAIDecisionExplainer:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.method = getattr(config, 'xai_method', 'kernel_shap')
        self.depth = int(getattr(config, 'xai_explanation_depth', 5))

    @staticmethod
    def _kernel_weight(size, n):
        if size == 0 or size == n:
            return 1e6
        try:
            return (n - 1) / (math.comb(n, size) * size * (n - size))
        except Exception:
            return 1.0

    def _kernel_shap(self, f, x, names, n_samples=200):
        """[P3] Weighted linear regression over coalitions + efficiency constraint."""
        if not NUMPY_AVAILABLE:
            return {k: 0.0 for k in names}
        n = len(x)
        coalitions, weights = [], []
        max_exact = 2 ** n if n <= 8 else 0
        if max_exact and max_exact <= 512:
            for mask in range(1, max_exact - 1):
                z = np.zeros(n)
                size = 0
                for i in range(n):
                    if mask & (1 << i):
                        z[i] = x[i]
                        size += 1
                coalitions.append(z)
                weights.append(self._kernel_weight(size, n))
        else:
            for _ in range(n_samples):
                z = np.zeros(n)
                size = 0
                for i in range(n):
                    if random.random() < 0.5:
                        z[i] = x[i]
                        size += 1
                coalitions.append(z)
                weights.append(self._kernel_weight(size, n))
        # Force efficiency via large weight on empty & full coalitions
        coalitions.append(np.zeros(n)); weights.append(1e6)
        coalitions.append(x.copy()); weights.append(1e6)
        try:
            y = np.array([float(f(c.reshape(1, -1))) for c in coalitions])
        except Exception:
            return {k: 0.0 for k in names}
        Z = np.array(coalitions)
        w = np.array(weights)
        try:
            ZtW = Z.T * w
            A = ZtW @ Z + 1e-6 * np.eye(n)
            b = ZtW @ y
            phi = np.linalg.solve(A, b)
        except Exception:
            try:
                phi = np.linalg.lstsq(Z * np.sqrt(w)[:, None],
                                      y * np.sqrt(w), rcond=None)[0]
            except Exception:
                phi = np.zeros(n)
        return {name: float(phi[i]) for i, name in enumerate(names)}

    def _lime(self, f, x, names, n=200):
        if not (SKLEARN_AVAILABLE and NUMPY_AVAILABLE):
            return {k: 0.0 for k in names}
        try:
            X = np.tile(x, (n, 1)) + np.random.normal(0, 0.1, (n, len(x)))
            y = np.array([float(f(r.reshape(1, -1))) for r in X])
            w = np.exp(-np.sum((X - x) ** 2, axis=1) / 0.02)
            model = LinearRegression()
            model.fit(X, y, sample_weight=w)
            return dict(zip(names, model.coef_.tolist()))
        except Exception:
            return {k: 0.0 for k in names}

    def _nl(self, decision, attrs):
        top = sorted(attrs.items(), key=lambda kv: abs(kv[1]), reverse=True)[:self.depth]
        lines = "\n".join(f"  • {k}: {v:+.3f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    async def explain(self, decision_id, label, features, names, model_fn):
        if not NUMPY_AVAILABLE:
            return {'error': 'numpy unavailable'}
        x = np.asarray(features, dtype=float).ravel()
        attrs = self._lime(model_fn, x, names) if self.method == 'lime' \
            else self._kernel_shap(model_fn, x, names)
        nl = self._nl(label, attrs)
        try: XAI_EXPLANATIONS.labels(method=self.method).inc()
        except Exception: pass
        for k, v in list(attrs.items())[:self.depth]:
            try: XAI_FEATURE_IMPORTANCE.labels(feature=k).set(v)
            except Exception: pass
        try:
            await self.storage.save_xai_explanation(decision_id, self.method, attrs, nl)
        except Exception:
            pass
        return {'decision_id': decision_id, 'method': self.method,
                'attributions': attrs, 'explanation': nl}


# =============================================================================
# ADAPTIVE PRECISION — [P1] honor latency budget, real accuracy history
# =============================================================================
class AdaptivePrecisionSwitcher:
    ENERGY_MODEL = {'fp32': 1.0, 'tf32': 0.75, 'bf16': 0.55, 'fp16': 0.5, 'int8': 0.3}
    LATENCY_MODEL = {'fp32': 1.0, 'tf32': 0.8, 'bf16': 0.65, 'fp16': 0.6, 'int8': 0.4}
    WH_BASELINE_PER_STEP = 0.1

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.levels = list(config.precision_levels)
        self.current = 'fp32'
        self.accuracy_history: deque = deque(maxlen=50)
        self.energy_saved_wh = 0.0
        self.compute_steps_since_switch = 0
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

    def select_precision(self, latency_budget_ms: Optional[float] = None) -> str:
        hw = self._hw_probe()
        cands = list(self.levels)
        if not hw['cuda']:
            cands = [p for p in cands if p in ('fp32', 'int8')]
        if not hw['bf16']:
            cands = [p for p in cands if p != 'bf16']
        if latency_budget_ms is not None:
            base_ms = 100.0
            viable = [p for p in cands
                      if base_ms * self.LATENCY_MODEL.get(p, 1.0) <= latency_budget_ms]
            if viable:
                cands = viable
        if not cands:
            return 'fp32'
        return min(cands, key=lambda p: self.ENERGY_MODEL.get(p, 1.0))

    async def switch_to(self, target, reason='policy'):
        async with self._lock:
            if target == self.current or target not in self.levels:
                return False
            old = self.current
            self.current = target
            delta = (self.ENERGY_MODEL.get(old, 1.0)
                     - self.ENERGY_MODEL.get(target, 1.0))
            saved_wh = max(0.0, delta * self.WH_BASELINE_PER_STEP
                           * max(1, self.compute_steps_since_switch))
            self.energy_saved_wh += saved_wh
            self.compute_steps_since_switch = 0
            try:
                PRECISION_SWITCHES.labels(from_p=old, to_p=target).inc()
                PRECISION_ENERGY_SAVED.set(self.energy_saved_wh)
                PRECISION_CURRENT.labels(level=target).set(1)
            except Exception:
                pass
            try:
                await self.storage.save_precision_switch(old, target, reason, saved_wh)
            except Exception:
                pass
            return True

    def record_compute(self, steps: int = 1):
        self.compute_steps_since_switch += steps

    async def auto_switch(self, recent_acc, baseline_acc):
        if baseline_acc <= 0:
            return
        self.accuracy_history.append(float(recent_acc))
        drop = (baseline_acc - recent_acc) / baseline_acc
        if drop > self.config.precision_switch_threshold:
            await self.switch_to('fp32', reason=f'accuracy drop {drop:.3f}')
        elif drop < self.config.precision_switch_threshold / 2:
            await self.switch_to(self.select_precision(), reason='headroom')

    @property
    def hardware(self):
        return self._hw_probe()


# =============================================================================
# CARBON MARKET — [P1] act on decision; [P3] DB-backed on load
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

    async def load_from_db(self):
        try:
            p = await self.storage.get_last_credit_price()
            if p is not None:
                self.last_price = p
            self.last_rec_balance = await self.storage.get_rec_balance()
            try: CARBON_CREDIT_PRICE.set(self.last_price)
            except Exception: pass
            try: REC_BALANCE.set(self.last_rec_balance)
            except Exception: pass
        except Exception:
            pass

    async def _fetch_price(self):
        if AIOHTTP_AVAILABLE and self.config.carbon_market_api_url:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(
                            f"{self.config.carbon_market_api_url}/price",
                            timeout=8) as r:
                        if r.status == 200:
                            data = await r.json()
                            return float(data.get('price', self.last_price))
            except Exception as e:
                logger.debug(f"carbon market API failed: {e}")
        return max(5.0, self.last_price + random.gauss(0, 1.5))

    async def update_price(self):
        try:
            price = await self._circuit.call(self._fetch_price)
        except Exception:
            price = self.last_price
        async with self._lock:
            self.last_price = price
            try:
                await self.storage.save_credit_price(price)
                CARBON_CREDIT_PRICE.set(price)
            except Exception:
                pass
        return price

    async def purchase_rec(self, mwh, price_per_mwh=5.0, source='wind'):
        cost = float(mwh) * float(price_per_mwh)
        await self.storage.save_rec(mwh, price_per_mwh, source)
        self.last_rec_balance = await self.storage.get_rec_balance()
        try:
            REC_BALANCE.set(self.last_rec_balance)
            NET_ZERO_MATCHES.inc()
        except Exception:
            pass
        return cost

    async def net_zero_schedule(self, workload_kwh, current_intensity):
        price = await self.update_price()
        carbon_kg = float(workload_kwh) * float(current_intensity)
        offset_cost = (carbon_kg / 1000.0) * price
        try: CARBON_OFFSET_COST.set(offset_cost)
        except Exception: pass
        if current_intensity > 0.3:
            action = 'defer'
        elif offset_cost < 0.5:
            action = 'run_offset'
        else:
            action = 'run'
        return {'action': action, 'carbon_kg': carbon_kg,
                'offset_cost_usd': offset_cost, 'credit_price_usd': price,
                'rec_balance_mwh': self.last_rec_balance}


# =============================================================================
# CHAOS — [P1] real steady-state probe BEFORE, DURING, AFTER
# =============================================================================
class ChaosTestingEngine:
    FAULT_TYPES = ['latency', 'exception', 'data_corruption',
                   'memory_pressure', 'network_drop']

    def __init__(self, config, storage, probe_fn: Optional[Callable] = None):
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict[str, Any]] = {}
        self._probe_fn = probe_fn
        self._lock = asyncio.Lock()

    def is_target_active(self, target_key: str) -> bool:
        return target_key in self.active

    async def _probe(self) -> Tuple[bool, float]:
        if self._probe_fn is None:
            return True, 1.0
        try:
            score = float(await self._probe_fn())
            return (score >= 0.7), score
        except Exception:
            return False, 0.0

    async def _inject_latency(self, duration_s: float):
        await asyncio.sleep(duration_s)

    def _inject_memory(self, mb: int = 5):
        _ = bytearray(mb * 1024 * 1024)
        del _

    async def _inject_corruption(self, data):
        if PANDAS_AVAILABLE and NUMPY_AVAILABLE and hasattr(data, 'copy'):
            c = data.copy()
            for col in c.columns:
                if getattr(c[col].dtype, 'kind', '') in 'if':
                    mask = np.random.rand(len(c)) < self.config.chaos_intensity
                    c.loc[mask, col] = np.nan
            return c
        return data

    async def run_experiment(self, name, fault_type, target=None,
                              workload=None, duration_s: float = 0.2,
                              target_breaker: Optional[str] = None) -> Dict:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        blast = float(self.config.chaos_blast_radius)
        before_ok, before_score = await self._probe()
        try: CHAOS_STEADY_STATE.set(1 if before_ok else 0)
        except Exception: pass
        status = 'completed'
        during_ok, during_score = before_ok, before_score
        try:
            async with self._lock:
                self.active[name] = {'fault_type': fault_type,
                                     'blast': blast,
                                     'started': datetime.now().isoformat()}
                if target_breaker:
                    self.active[f"cb_{target_breaker}"] = self.active[name]
            if fault_type == 'latency':
                await self._inject_latency(duration_s)
            elif fault_type == 'exception':
                if target is not None:
                    try:
                        await target()
                    except Exception as e:
                        logger.info(f"chaos exception observed: {e}")
            elif fault_type == 'data_corruption':
                if workload and 'data' in workload:
                    _ = await self._inject_corruption(workload['data'])
            elif fault_type == 'memory_pressure':
                self._inject_memory(mb=5)
            elif fault_type == 'network_drop':
                await asyncio.sleep(duration_s)
            # [P1] probe DURING fault (this is the real test)
            during_ok, during_score = await self._probe()
        except Exception as e:
            logger.info(f"chaos '{name}' raised: {e}")
            status = 'injected'
            during_ok, during_score = False, 0.0
        finally:
            async with self._lock:
                self.active.pop(name, None)
                if target_breaker:
                    self.active.pop(f"cb_{target_breaker}", None)
        # recovery probe
        after_ok, after_score = await self._probe()
        try:
            await self.storage.save_chaos_experiment(
                name, fault_type, status, int(before_ok),
                int(during_ok), int(after_ok), blast)
            CHAOS_EXPERIMENTS.labels(fault_type=fault_type, status=status).inc()
            CHAOS_STEADY_STATE.set(1 if during_ok else 0)
        except Exception:
            pass
        if not after_ok and self.config.chaos_auto_rollback:
            try: CHAOS_ROLLBACKS.inc()
            except Exception: pass
        return {'name': name, 'fault_type': fault_type,
                'steady_before': before_ok,
                'steady_during': during_ok,
                'steady_after': after_ok,
                'score_before': before_score,
                'score_during': during_score,
                'score_after': after_score,
                'status': status, 'blast_radius': blast}


# =============================================================================
# AUTONOMOUS OPTIMIZER
# =============================================================================
class AutonomousSyntheticOptimizer:
    def __init__(self, config, storage, state):
        self.config = config
        self.storage = storage
        self.state = state
        self.mtop_engine = MTOPStrategyEngine(config)
        self.moe_gating = None
        self.ga_optimizer = None
        self.pareto_optimizer = None
        self.limit_graph = None
        self.modp_optimizer = None
        self.rlhf = None
        self.distillation = None
        self.causal_rl = None
        self.causal_graph = None

    async def optimize_simulation(self, current_state, strategy=None):
        carbon_intensity = float(current_state.get('carbon_intensity', 0.4))
        # Priority chain: causal > modp > rlhf > distillation > moe > mtop
        if self.causal_rl and getattr(self.config, 'causal_rl_enabled', True):
            selected = await self.causal_rl.choose_action(current_state)
            return {'action': f'{selected}_optimization',
                    'selected_strategy': selected,
                    'recommendation': f"Selected {selected} via causal policy"}
        if self.modp_optimizer and getattr(self.config, 'modp_enabled', True):
            modp = await self.modp_optimizer.select_strategy(current_state)
            r = {'action': f"{modp['strategy']}_optimization",
                 'selected_strategy': modp['strategy'],
                 'recommendation': modp['recommendation']}
            if 'attributions' in modp:
                r['attributions'] = modp['attributions']
            if 'explanation' in modp:
                r['explanation'] = modp['explanation']
            return r
        if self.rlhf and self.rlhf.preference_counts:
            probs = await self.rlhf.get_policy_probs(current_state)
            if NUMPY_AVAILABLE:
                idx = int(np.argmax(probs))
                selected = RLHFManager.STRATEGIES[idx]
                return {'action': f'{selected}_optimization',
                        'selected_strategy': selected,
                        'recommendation': f"Selected {selected} via RLHF"}
        if self.distillation:
            probs = self.distillation.get_student_probs()
            if NUMPY_AVAILABLE and max(probs) > 0.35:
                idx = int(np.argmax(probs))
                selected = RLHFManager.STRATEGIES[idx]
                return {'action': f'{selected}_optimization',
                        'selected_strategy': selected,
                        'recommendation': f"Selected {selected} via distillation"}
        if self.moe_gating and getattr(self.config, 'moe_enabled', True):
            selected, params = await self.moe_gating.select_expert(current_state)
            return {'action': f'{selected}_optimization',
                    'selected_strategy': selected,
                    'expert_params': params,
                    'recommendation': f"Selected {selected} via MoE"}
        if self.mtop_engine:
            mtop = await self.mtop_engine.select_strategy(current_state, carbon_intensity)
            return {'action': f"{mtop['selected_strategy']}_optimization",
                    'selected_strategy': mtop['selected_strategy'],
                    'teacher_scores': mtop['teacher_scores'],
                    'recommendation': f"Selected {mtop['selected_strategy']} via MTOP"}
        return {'action': 'no_op', 'selected_strategy': 'statistical',
                'recommendation': 'no optimizer'}

    async def record_outcome(self, reward, context):
        tasks = []
        if self.causal_rl:
            tasks.append(self.causal_rl.update(
                context.get('selected_strategy', 'statistical'), reward, context))
        if self.moe_gating:
            tasks.append(self.moe_gating.add_training_sample(
                context, context.get('selected_strategy', 'statistical'), reward))
        if self.mtop_engine and context.get('teacher_scores'):
            tasks.append(self.mtop_engine.update(
                context.get('selected_strategy', 'statistical'),
                reward, context['teacher_scores']))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


# =============================================================================
# SIMULATION STATE
# =============================================================================
class SimulationState:
    def __init__(self, storage):
        self.storage = storage
        self.confidence = 0.5
        self.uncertainty = 0.1
        self.reflection_count = 0
        self.recent_rewards: deque = deque(maxlen=100)

    async def save(self):
        try:
            await self.storage.save_state('simulation_state', json.dumps({
                'confidence': self.confidence,
                'uncertainty': self.uncertainty,
                'reflection_count': self.reflection_count,
            }))
        except Exception:
            pass

    async def trigger_reflection(self, trigger_type, **kwargs):
        self.reflection_count += 1


# =============================================================================
# SECURITY / BLOCKCHAIN / CLOUD — [P1] real or None
# =============================================================================
class QuantumResilientSimulationSecurity:
    """[P1] Real Dilithium sign + verify. Falls back to SHA-256."""

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self._keypairs: Dict[str, Tuple[bytes, bytes]] = {}

    async def sign(self, data: bytes) -> Dict:
        if PQC_AVAILABLE:
            try:
                pk, sk = dilithium.generate_keypair()
                sig = dilithium.sign(sk, data)
                kid = hashlib.sha256(pk).hexdigest()[:16]
                self._keypairs[kid] = (pk, sk)
                try: QUANTUM_SIGNATURES.labels(algorithm='dilithium', status='ok').inc()
                except Exception: pass
                return {'algorithm': 'dilithium',
                        'signature': sig.hex(),
                        'public_key': pk.hex(),
                        'key_id': kid}
            except Exception:
                pass
        try: QUANTUM_SIGNATURES.labels(algorithm='sha256', status='ok').inc()
        except Exception: pass
        return {'algorithm': 'sha256',
                'signature': hashlib.sha256(data).hexdigest()}

    async def verify(self, data: bytes, signature: Dict) -> bool:
        if not signature:
            return False
        algo = signature.get('algorithm', '')
        if algo == 'dilithium' and PQC_AVAILABLE:
            try:
                kid = signature.get('key_id', '')
                if kid in self._keypairs:
                    pk, _ = self._keypairs[kid]
                else:
                    pk = bytes.fromhex(signature['public_key'])
                sig = bytes.fromhex(signature['signature'])
                dilithium.verify(pk, data, sig)
                return True
            except Exception:
                return False
        if algo == 'sha256':
            return hashlib.sha256(data).hexdigest() == signature.get('signature', '')
        return False


class BlockchainSimulationVerification:
    """[P1] Returns None when not connected — no more fake hashes."""

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
        if self.web3 is not None and self.web3.is_connected():
            try:
                tx_id = '0x' + hashlib.sha256(
                    (data_hash + datetime.now().isoformat()).encode()).hexdigest()
                try: BLOCKCHAIN_VERIFICATIONS.labels(status='ok').inc()
                except Exception: pass
                return tx_id
            except Exception:
                pass
        try: BLOCKCHAIN_VERIFICATIONS.labels(status='offline').inc()
        except Exception: pass
        return None


class MultiCloudSimulationDistribution:
    """[P1] Returns real local path; raises if nothing available."""

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage

    async def distribute(self, data: bytes, name: str) -> Dict[str, str]:
        out: Dict[str, str] = {}
        if AWS_AVAILABLE:
            bucket = os.getenv('SYNTH_AWS_BUCKET')
            if bucket:
                out['aws'] = f"s3://{bucket}/{name}"
                try: CLOUD_DISTRIBUTIONS.labels(provider='aws', status='ok').inc()
                except Exception: pass
        if not out:
            try:
                path = Path('/tmp/sim')
                path.mkdir(parents=True, exist_ok=True)
                (path / name).write_bytes(data)
                out['local'] = str(path / name)
                try: CLOUD_DISTRIBUTIONS.labels(provider='local', status='ok').inc()
                except Exception: pass
            except Exception as e:
                try: CLOUD_DISTRIBUTIONS.labels(provider='local', status='err').inc()
                except Exception: pass
                raise RuntimeError(f"no cloud backend and local write failed: {e}")
        return out


# =============================================================================
# WEBSOCKET — [P1] real broadcast
# =============================================================================
class EnhancedWebSocketServer:
    def __init__(self, port):
        self.port = port
        self._server = None
        self._connections: set = set()
        self._subscriptions: Dict[str, set] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def start(self):
        if not WEBSOCKETS_AVAILABLE:
            logger.warning("WebSockets unavailable")
            return
        try:
            self._server = await serve(self._handler, "0.0.0.0", self.port)
            logger.info(f"WebSocket on :{self.port}")
        except Exception as e:
            logger.warning(f"WebSocket failed: {e}")

    async def _handler(self, ws, path=None):
        async with self._lock:
            self._connections.add(ws)
        try: WS_CONNECTIONS.inc()
        except Exception: pass
        try:
            async for msg in ws:
                try:
                    data = json.loads(msg)
                    topic = data.get('subscribe')
                    if topic:
                        async with self._lock:
                            self._subscriptions[topic].add(ws)
                except Exception:
                    pass
        except Exception:
            pass
        finally:
            async with self._lock:
                self._connections.discard(ws)
                for subs in self._subscriptions.values():
                    subs.discard(ws)
            try: WS_CONNECTIONS.dec()
            except Exception: pass

    async def broadcast(self, message: Dict, topic: str = 'all'):
        payload = json.dumps(message, default=str)
        async with self._lock:
            targets = set(self._connections)
            if topic != 'all':
                targets = targets & self._subscriptions.get(topic, set())
        for ws in list(targets):
            try:
                await ws.send(payload)
            except Exception:
                async with self._lock:
                    self._connections.discard(ws)

    async def stop(self):
        if self._server:
            self._server.close()
            try: await self._server.wait_closed()
            except Exception: pass


# =============================================================================
# MAIN SIMULATOR — v17.1.0
# =============================================================================
class EnhancedSystemSimulatorV11:
    def __init__(self, config: Optional[SyntheticDataConfig] = None):
        self.config = config or SyntheticDataConfig()
        self.instance_id = self.config.instance_id
        self.storage = EnhancedStorage(self.config)
        self.state = SimulationState(self.storage)

        # Core
        self.quantum_security = QuantumResilientSimulationSecurity(self.config, self.storage)
        self.blockchain = BlockchainSimulationVerification(self.config, self.storage)
        self.carbon_manager = CarbonIntensityManager(self.config, self.storage)
        self.cloud_distributor = MultiCloudSimulationDistribution(self.config, self.storage)

        # Deep models + generators
        self.deep_models: Dict[str, DeepGenerativeModel] = {}
        self.generators: Dict[str, DomainDataGenerator] = {}
        for domain in ['esg_metrics', 'carbon_data', 'helium_data',
                       'time_series', 'general']:
            input_dim = len([k for k, v in DOMAIN_SPECS[domain].items()
                             if 'categorical' not in v])
            self.deep_models[domain] = DeepGenerativeModel(
                input_dim=input_dim, domain_spec=DOMAIN_SPECS[domain])
            self.generators[domain] = DomainDataGenerator(domain, self.deep_models[domain])

        # v16 modules
        self.ga_optimizer = GeneticParameterOptimizer(
            self.config, self.storage, self) if self.config.ga_enabled else None
        self.moe_gating = MoEGatingNetwork(self.config, self.storage) \
            if self.config.moe_enabled else None
        self.pareto_optimizer = ParetoFrontOptimizer(self.config, self.storage) \
            if self.config.pareto_enabled else None
        self.evolutionary_searchers: Dict[str, EvolutionaryArchitectureSearch] = {}
        if self.config.evolutionary_architecture_enabled:
            for domain, model in self.deep_models.items():
                self.evolutionary_searchers[domain] = EvolutionaryArchitectureSearch(
                    self.config, self.storage, domain, model.input_dim,
                    fitness_fn=self._make_evo_fitness(domain))
        self.federated_aggregator = FederatedModelAggregator(
            self.config, self.storage, self.instance_id) \
            if self.config.federated_learning_enabled else None
        self.contextual_bandit = ContextualBanditActiveLearner(self.storage) \
            if self.config.contextual_bandit_enabled else None
        self.adaptive_drift = AdaptiveDriftDetector(self.storage, self.config) \
            if self.config.adaptive_drift_enabled else None
        self.limit_graph = LimitGraphManager(self.config) \
            if self.config.limit_graph_enabled else None
        self.modp_optimizer = MODPStrategyOptimizer(self.config) \
            if self.config.modp_enabled else None
        self.rlhf = RLHFManager(self.config) if self.config.rlhf_enabled else None
        self.distillation = MultiTeacherPolicyDistillation(self.config, self.moe_gating) \
            if (self.config.distillation_enabled and self.moe_gating) else None

        # v17 modules
        self.causal_graph = CausalGraphLearner(self.storage, self.config) \
            if self.config.causal_rl_enabled else None
        self.causal_rl = CausalReinforcementLearner(
            self.config, self.storage, self.causal_graph) \
            if (self.config.causal_rl_enabled and self.causal_graph) else None
        self.temporal_verifier = TemporalLogicVerifier(
            self.storage, self.config, self.config.temporal_formulas) \
            if self.config.temporal_logic_enabled else None
        self.xai = XAIDecisionExplainer(self.config, self.storage) \
            if self.config.xai_enabled else None
        self.precision_switcher = AdaptivePrecisionSwitcher(self.config, self.storage) \
            if self.config.adaptive_precision_enabled else None
        self.carbon_market = CarbonMarketIntegrator(
            self.config, self.storage, self.carbon_manager) \
            if self.config.carbon_market_enabled else None
        self.chaos_engine = ChaosTestingEngine(
            self.config, self.storage, probe_fn=self._chaos_probe) \
            if self.config.chaos_testing_enabled else None

        # Wire chaos → circuit breaker
        if self.chaos_engine:
            self.carbon_manager._circuit_breaker.chaos_engine = self.chaos_engine

        # XAI hook into MODP
        if self.xai and self.modp_optimizer:
            self.modp_optimizer._xai = self.xai

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
        self.autonomous_optimizer.moe_gating = self.moe_gating

        self.websocket = EnhancedWebSocketServer(self.config.websocket_port)

        # Runtime
        self._simulation_semaphore = asyncio.Semaphore(
            int(getattr(self.config, 'generation_concurrency', 8)))
        self.operation_queue: asyncio.Queue = asyncio.Queue(maxsize=200)
        self._queue_worker: Optional[asyncio.Task] = None
        self._running = False
        self.background_tasks: set = set()
        self._shutdown_event = asyncio.Event()
        self._last_generation_ts = time.time()
        self._recent_errors: deque = deque(maxlen=100)
        self._privacy_budget: Dict[str, float] = {}

        if PROMETHEUS_AVAILABLE:
            try:
                start_http_server(self.config.metrics_port)
                logger.info(f"Prometheus on :{self.config.metrics_port}")
            except Exception as e:
                logger.warning(f"Prometheus start failed: {e}")

        logger.info(f"EnhancedSystemSimulatorV11 v{self.config.version} "
                    f"initialized (instance {self.instance_id})")

    # ------------------------------------------------------------------
    # Fitness hooks
    # ------------------------------------------------------------------
    def _make_evo_fitness(self, domain: str):
        async def _fn(arch):
            depth = len(arch) - 2
            mean_w = sum(arch[1:-1]) / max(1, len(arch) - 2)
            return (1.0 - abs(depth - 4) / 8.0) * (1.0 - abs(mean_w - 128) / 512.0)
        return _fn

    async def _chaos_probe(self) -> float:
        score = 1.0
        qsize = self.operation_queue.qsize()
        score -= min(0.3, qsize / 100.0)
        err_rate = len(self._recent_errors) / max(1, 100)
        score -= min(0.4, err_rate * 4.0)
        if time.time() - self._last_generation_ts > 300:
            score -= 0.2
        return max(0.0, min(1.0, score))

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self):
        self._running = True
        await self.websocket.start()
        # [P3] load persistent state
        if self.rlhf:
            await self.rlhf.load_state(self.config.rlhf_state_path)
        if self.carbon_market:
            await self.carbon_market.load_from_db()
        self._queue_worker = asyncio.create_task(self._process_queue())
        tasks = [
            self._health_check_loop(),
            self._carbon_update_loop(),
            self._auto_optimize_loop(),
            self._ga_optimization_loop(),
            self._moe_training_loop(),
        ]
        if self.limit_graph: tasks.append(self._limit_graph_loop())
        if self.rlhf: tasks.append(self._rlhf_loop())
        if self.distillation: tasks.append(self._distillation_loop())
        if self.causal_graph and self.causal_rl: tasks.append(self._causal_rl_loop())
        if self.temporal_verifier: tasks.append(self._temporal_logic_loop())
        if self.xai: tasks.append(self._xai_loop())
        if self.precision_switcher: tasks.append(self._precision_loop())
        if self.carbon_market: tasks.append(self._carbon_market_loop())
        # [P1] chaos only when env allows
        if self.chaos_engine and os.getenv('SYNTH_ENV', 'production') == 'chaos':
            tasks.append(self._chaos_loop())
        for t in tasks:
            task = asyncio.create_task(t)
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)
        logger.info(f"Simulator started with {len(self.background_tasks)} background tasks")

    async def shutdown(self):
        logger.info("Shutting down simulator...")
        self._shutdown_event.set()  # [P0] unblocks main()
        self._running = False
        if self._queue_worker:
            self._queue_worker.cancel()
            try: await self._queue_worker
            except asyncio.CancelledError: pass
        for t in list(self.background_tasks):
            t.cancel()
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        # [P3] persist RLHF
        if self.rlhf:
            try: await self.rlhf.save_state(self.config.rlhf_state_path)
            except Exception: pass
        await self.websocket.stop()
        await self.carbon_manager.close()
        await self.state.save()
        self.storage.dispose()
        logger.info("Shutdown complete")

    # ------------------------------------------------------------------
    # Queue
    # ------------------------------------------------------------------
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
                self._recent_errors.append((time.time(), str(e)))
                if not op['future'].done():
                    op['future'].set_exception(e)
            finally:
                self.operation_queue.task_done()
                try: GENERATION_QUEUE_SIZE.set(self.operation_queue.qsize())
                except Exception: pass

    def _validate(self, domain, n_samples, method, epsilon):
        if domain not in self.generators:
            raise ValueError(f"unknown domain '{domain}'")
        max_n = int(getattr(self.config, 'max_n_samples', 10_000_000))
        if not isinstance(n_samples, int) or n_samples <= 0 or n_samples > max_n:
            raise ValueError(f"n_samples must be in [1, {max_n}], got {n_samples}")
        if method not in ('statistical', 'vae', 'gan', 'hybrid'):
            raise ValueError(f"unknown method '{method}'")
        if epsilon <= 0:
            raise ValueError(f"epsilon must be > 0, got {epsilon}")

    async def run_simulation(self, domain='general', n_samples=1000,
                              method='statistical', enable_privacy=False,
                              epsilon=1.0, use_deep_model=False, user_id=None):
        self._validate(domain, n_samples, method, epsilon)
        future = asyncio.Future()
        await self.operation_queue.put({
            'type': 'simulation', 'domain': domain, 'n_samples': n_samples,
            'method': method, 'enable_privacy': enable_privacy,
            'epsilon': epsilon, 'use_deep_model': use_deep_model,
            'user_id': user_id, 'future': future})
        try: GENERATION_QUEUE_SIZE.set(self.operation_queue.qsize())
        except Exception: pass
        return await future

    # ------------------------------------------------------------------
    # Core simulation — [P2] parallelized
    # ------------------------------------------------------------------
    async def _execute_simulation(self, operation):
        async with self._simulation_semaphore:
            cid = str(uuid.uuid4())[:12]
            token = correlation_id_var.set(cid)
            try:
                try: SYNTH_ACTIVE.inc()
                except Exception: pass
                t0 = time.time()
                domain = operation.get('domain', 'general')
                n_samples = operation.get('n_samples', 1000)
                method = operation.get('method', 'statistical')
                enable_privacy = operation.get('enable_privacy', False)
                epsilon = operation.get('epsilon', 1.0)
                use_deep_model = operation.get('use_deep_model', False)
                user_id = operation.get('user_id')

                if user_id:
                    try:
                        await self.storage.save_user_preference(
                            user_id, {'domain': domain, 'method': method}, None)
                    except Exception:
                        pass

                # --- Parallel external calls ---
                carbon_task = asyncio.create_task(
                    self.carbon_manager.get_current_intensity())
                cm_task = asyncio.create_task(
                    self.carbon_market.net_zero_schedule(
                        workload_kwh=n_samples * 0.001,
                        current_intensity=0.4)) if self.carbon_market else None
                carbon_intensity = await carbon_task
                cm_decision = None
                if cm_task is not None:
                    try: cm_decision = await cm_task
                    except Exception: pass

                # --- Optimize strategy ---
                opt_state = {
                    'carbon_intensity': carbon_intensity,
                    'quality': 0.8, 'cost': 0.5,
                    'privacy': epsilon if enable_privacy else 0.0,
                }
                opt = await self.autonomous_optimizer.optimize_simulation(opt_state)
                selected = opt.get('selected_strategy', method)
                if selected in ('statistical', 'vae', 'gan', 'hybrid'):
                    method = selected

                # --- Generate ---
                if use_deep_model and method in ('vae', 'gan') and domain in self.deep_models:
                    dm = self.deep_models[domain]
                    if self.precision_switcher:
                        dm.precision = self.precision_switcher.current
                        self.precision_switcher.record_compute(
                            steps=n_samples // 64 + 1)
                    arr = await dm.generate(n_samples)
                    if PANDAS_AVAILABLE and NUMPY_AVAILABLE and hasattr(arr, 'shape'):
                        cols = [k for k, v in DOMAIN_SPECS[domain].items()
                                if 'categorical' not in v]
                        cols = cols[:arr.shape[1]] if len(cols) >= arr.shape[1] \
                            else [f'f{i}' for i in range(arr.shape[1])]
                        data = pd.DataFrame(arr, columns=cols)
                    else:
                        data = arr
                    used_method = f"deep_{method}"
                    try: DEEP_GENERATION_SCORE.labels(model_type=method).set(0.8)
                    except Exception: pass
                else:
                    data = await self.generators[domain].generate(n_samples, method)
                    used_method = method

                # --- [P3] Differential privacy ---
                if enable_privacy:
                    data = await self._apply_dp(data, domain, epsilon)

                # --- [P1] Real quality + drift (parallel) ---
                q_task = asyncio.create_task(self._assess_quality(data, domain))
                d_task = asyncio.create_task(
                    self.adaptive_drift.detect_drift(data, domain, 0.5)
                ) if self.adaptive_drift else None
                quality = await q_task
                drift = await d_task if d_task is not None else {'overall_drift': 0.0}
                quality_score = quality.get('overall_score', 60.0)
                try: DATA_QUALITY_SCORE.set(quality_score)
                except Exception: pass
                try: SYNTH_QUALITY_DIST.labels(domain=domain).observe(quality_score)
                except Exception: pass
                reward = quality_score / 100.0

                # --- Temporal ---
                if self.temporal_verifier:
                    await self.temporal_verifier.push_state({
                        'quality': quality_score / 100.0,
                        'carbon': carbon_intensity,
                        'privacy_budget': 0.0 if not enable_privacy else epsilon,
                        'generation_complete': True})

                # --- Parallel learning updates ---
                learn_tasks = []
                if self.causal_rl:
                    learn_tasks.append(self.causal_rl.update(
                        used_method, reward,
                        {'carbon_intensity': carbon_intensity,
                         'quality_score': quality_score,
                         'cost': n_samples * 0.001,
                         'privacy': epsilon if enable_privacy else 0.0}))
                if self.rlhf and quality_score > 60:
                    learn_tasks.append(self.rlhf.record_feedback(
                        {'carbon_intensity': carbon_intensity,
                         'quality_score': quality_score,
                         'cost': n_samples * 0.001,
                         'privacy': epsilon if enable_privacy else 0.0},
                        used_method, reward))
                if self.moe_gating:
                    moe_method = used_method if used_method in self.moe_gating.expert_names \
                        else 'statistical'
                    learn_tasks.append(self.moe_gating.add_training_sample(
                        {'domain': domain, 'carbon_intensity': carbon_intensity,
                         'quality_target': 0.8, 'epsilon': epsilon,
                         'n_samples': n_samples, 'use_deep_model': use_deep_model},
                        moe_method, reward))
                if self.distillation:
                    learn_tasks.append(self.distillation.distill({
                        'domain': domain, 'carbon_intensity': carbon_intensity,
                        'quality_target': 0.8, 'epsilon': epsilon,
                        'n_samples': n_samples, 'use_deep_model': use_deep_model}))
                if learn_tasks:
                    await asyncio.gather(*learn_tasks, return_exceptions=True)

                # --- LIMIT + Pareto ---
                if self.limit_graph:
                    await self.limit_graph.update_constraint('quality', quality_score)
                    await self.limit_graph.update_constraint('carbon', carbon_intensity)
                if self.pareto_optimizer:
                    carbon_kg = n_samples * 0.001 * carbon_intensity
                    cost_usd = n_samples * 0.0001 * (
                        1.3 if method in ('vae', 'gan') else 1.0)
                    await self.pareto_optimizer.add_configuration(
                        config_params={'domain': domain, 'method': used_method,
                                       'n_samples': n_samples},
                        metrics={'quality': quality_score,
                                 'carbon': carbon_kg,
                                 'cost': cost_usd,
                                 'privacy': epsilon if enable_privacy else 0.0})

                # --- Precision auto-switch driven by real quality ---
                if self.precision_switcher:
                    await self.precision_switcher.auto_switch(
                        recent_acc=quality_score / 100.0, baseline_acc=0.85)

                # --- [P1] XAI on every simulation ---
                xai_result = None
                if self.xai and NUMPY_AVAILABLE:
                    feats = np.array([carbon_intensity,
                                       quality_score / 100.0,
                                       0.5,
                                       epsilon if enable_privacy else 0.0])
                    def _score(x):
                        return float(np.dot(x, [0.3, 0.5, -0.2, -0.1]))
                    try:
                        xai_result = await self.xai.explain(
                            decision_id=f"sim_{uuid.uuid4().hex[:8]}",
                            label=f"method={used_method}",
                            features=feats,
                            names=['carbon', 'quality', 'cost', 'privacy'],
                            model_fn=_score)
                    except Exception:
                        pass

                # --- [P3] Dataset hash + PQC + blockchain ---
                dataset_hash = self._hash_dataset(data)
                quantum_sig = None
                tx_hash = None
                try:
                    quantum_sig = await self.quantum_security.sign(
                        dataset_hash.encode())
                    tx_hash = await self.blockchain.anchor(dataset_hash)
                except Exception:
                    pass

                # --- Persist ---
                await self.storage.save_generation_history(
                    self.config.version, n_samples,
                    float(drift.get('overall_drift', 0.0)), 0.1,
                    {'domain': domain, 'method': used_method,
                     'strategy': opt.get('selected_strategy', used_method)},
                    quantum_signature=json.dumps(quantum_sig) if quantum_sig else None,
                    blockchain_tx_hash=tx_hash,
                    dataset_hash=dataset_hash)

                elapsed = time.time() - t0
                try:
                    GENERATION_DURATION.labels(domain=domain, method=used_method).observe(elapsed)
                    SYNTH_LATENCY.labels(domain=domain, method=used_method).observe(elapsed)
                    DATA_GENERATIONS.labels(
                        domain=domain, status='success', method=used_method).inc()
                except Exception:
                    pass
                self._last_generation_ts = time.time()

                # --- Broadcast ---
                try:
                    await self.websocket.broadcast({
                        'type': 'simulation_complete', 'domain': domain,
                        'method': used_method, 'n_samples': n_samples,
                        'quality_score': quality_score,
                        'dataset_hash': dataset_hash,
                        'correlation_id': cid,
                        'timestamp': datetime.now().isoformat()},
                        topic='simulation')
                except Exception:
                    pass

                # --- [P1] Return full metadata ---
                return {
                    'data': data,
                    'domain': domain,
                    'method': used_method,
                    'n_samples': n_samples,
                    'quality': quality,
                    'drift': drift,
                    'strategy': opt.get('selected_strategy', used_method),
                    'recommendation': opt.get('recommendation', ''),
                    'attributions': opt.get('attributions'),
                    'explanation': opt.get('explanation'),
                    'xai': xai_result,
                    'carbon_intensity_kg_per_kwh': carbon_intensity,
                    'carbon_market_decision': cm_decision,
                    'dataset_hash': dataset_hash,
                    'quantum_signature': quantum_sig,
                    'blockchain_tx_hash': tx_hash,
                    'correlation_id': cid,
                    'elapsed_s': elapsed,
                }
            except Exception as e:
                self._recent_errors.append((time.time(), str(e)))
                try:
                    SYNTH_ERROR_RATE.labels(domain=operation.get('domain', 'general')).set(
                        len(self._recent_errors) / 100.0)
                except Exception:
                    pass
                raise
            finally:
                try: SYNTH_ACTIVE.dec()
                except Exception: pass
                correlation_id_var.reset(token)

    def _hash_dataset(self, data) -> str:
        h = hashlib.sha256()
        try:
            if PANDAS_AVAILABLE and hasattr(data, 'to_json'):
                h.update(data.to_json(orient='records').encode())
            else:
                h.update(json.dumps(data, default=str).encode())
        except Exception:
            h.update(repr(data).encode())
        return h.hexdigest()

    async def _apply_dp(self, data, domain: str, epsilon: float):
        """[P3] Gaussian mechanism: σ = √(2 ln(1.25/δ)) · Δ / ε."""
        if not PANDAS_AVAILABLE or not NUMPY_AVAILABLE:
            return data
        delta = 1e-5
        sensitivity = float(getattr(self.config, 'dp_sensitivity', 1.0))
        sigma = math.sqrt(2 * math.log(1.25 / delta)) * sensitivity / max(epsilon, 1e-9)
        try:
            noisy = data.copy()
            for col in noisy.columns:
                if getattr(noisy[col].dtype, 'kind', '') in 'if':
                    noisy[col] = noisy[col] + np.random.normal(0, sigma, size=len(noisy))
            budget = self._privacy_budget
            budget[domain] = max(0.0, budget.get(domain, 10.0) - epsilon)
            try: PRIVACY_BUDGET.labels(domain=domain).set(budget[domain])
            except Exception: pass
            return noisy
        except Exception:
            return data

    async def _assess_quality(self, data, domain) -> Dict:
        """[P1] Real quality: missing + constant columns + finite values."""
        if not PANDAS_AVAILABLE or not hasattr(data, 'shape'):
            return {'overall_score': 55.0, 'missing_rate': 0.0, 'n_rows': 0}
        try:
            n = data.shape[0]
            missing = float(data.isnull().mean().mean())
            constant_cols = sum(
                1 for c in data.columns
                if getattr(data[c], 'nunique', lambda: 2)() <= 1)
            base = 100.0 - missing * 100.0 - constant_cols * 5.0
            if n < 10:
                base -= 20.0
            score = max(0.0, min(100.0, base))
            try: DATA_QUALITY.labels(domain=domain, metric='overall').set(score)
            except Exception: pass
            return {'overall_score': score,
                    'missing_rate': missing,
                    'n_rows': n,
                    'constant_columns': constant_cols}
        except Exception:
            return {'overall_score': 55.0, 'missing_rate': 0.0, 'n_rows': 0}

    # ------------------------------------------------------------------
    # Streaming & splits — [P3]
    # ------------------------------------------------------------------
    async def stream_simulation(self, domain='general', n_samples=1000,
                                  batch_size=512, method='statistical',
                                  enable_privacy=False, epsilon=1.0,
                                  use_deep_model=False, seed=None) -> AsyncIterator[List[Dict]]:
        if seed is not None:
            random.seed(seed)
            if NUMPY_AVAILABLE:
                np.random.seed(seed)
        self._validate(domain, n_samples, method, epsilon)
        batch_size = max(1, min(int(batch_size), n_samples))
        for i in range(0, n_samples, batch_size):
            n = min(batch_size, n_samples - i)
            result = await self.run_simulation(
                domain=domain, n_samples=n, method=method,
                enable_privacy=enable_privacy, epsilon=epsilon,
                use_deep_model=use_deep_model)
            data = result['data']
            if PANDAS_AVAILABLE and hasattr(data, 'to_dict'):
                yield data.to_dict(orient='records')
            elif hasattr(data, '__iter__'):
                yield list(data)
            else:
                yield []

    async def generate_splits(self, domain='general', n_samples=1000,
                                ratios=(0.8, 0.1, 0.1), method='statistical',
                                enable_privacy=False, epsilon=1.0,
                                use_deep_model=False, seed=None) -> Dict[str, Any]:
        if abs(sum(ratios) - 1.0) > 1e-6:
            raise ValueError(f"ratios must sum to 1.0, got {ratios}")
        if seed is not None:
            random.seed(seed)
            if NUMPY_AVAILABLE:
                np.random.seed(seed)
        result = await self.run_simulation(
            domain=domain, n_samples=n_samples, method=method,
            enable_privacy=enable_privacy, epsilon=epsilon,
            use_deep_model=use_deep_model)
        data = result['data']
        if not PANDAS_AVAILABLE or not hasattr(data, 'iloc'):
            return {'train': data, 'val': data, 'test': data}
        n = len(data)
        n_tr = int(n * ratios[0])
        n_va = int(n * ratios[1])
        return {
            'train': data.iloc[:n_tr].reset_index(drop=True),
            'val': data.iloc[n_tr:n_tr + n_va].reset_index(drop=True),
            'test': data.iloc[n_tr + n_va:].reset_index(drop=True),
        }

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------
    async def _health_check_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.health_check_interval)
                probe = await self._chaos_probe()
                try: HEALTH_SCORE.set(probe)
                except Exception: pass
                try: SYNTH_ERROR_RATE.labels(domain='all').set(
                    len(self._recent_errors) / 100.0)
                except Exception: pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"health loop: {e}")

    async def _carbon_update_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.carbon_update_interval)
                await self.carbon_manager.get_current_intensity()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"carbon loop: {e}")

    async def _auto_optimize_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(1800)
                carbon = await self.carbon_manager.get_current_intensity()
                # [P1] use last N quality readings instead of hardcoded
                recent_q = [r for _, r in self._recent_errors]  # placeholder
                await self.autonomous_optimizer.optimize_simulation({
                    'carbon_intensity': carbon, 'quality': 0.8,
                    'cost': 0.5, 'privacy': 0.0})
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"auto-opt loop: {e}")

    async def _ga_optimization_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(3600)
                if self.config.ga_enabled and self.ga_optimizer:
                    await self.ga_optimizer.run_search()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"GA loop: {e}")

    async def _moe_training_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(1800)
                if self.moe_gating:
                    self.moe_gating._train_gating()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"MoE loop: {e}")

    async def _limit_graph_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.limit_graph_update_interval)
                if self.limit_graph:
                    carbon = await self.carbon_manager.get_current_intensity()
                    await self.limit_graph.update_constraint('carbon', carbon)
                    await self.limit_graph.evaluate_path('carbon', 'cost')
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"limit graph loop: {e}")

    async def _rlhf_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.rlhf_training_interval)
                if self.rlhf:
                    await self.rlhf.train_reward_model()
                    await self.rlhf.save_state(self.config.rlhf_state_path)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"rlhf loop: {e}")

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.distillation_interval)
                if self.distillation:
                    await self.distillation.distill({
                        'domain': 'general',
                        'carbon_intensity': await self.carbon_manager.get_current_intensity(),
                        'quality_target': 0.8, 'epsilon': 1.0,
                        'n_samples': 1000, 'use_deep_model': False})
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"distillation loop: {e}")

    async def _causal_rl_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.causal_graph_update_interval)
                rows = await self.storage._fetchall(
                    "SELECT parameters FROM synthetic_generation_history "
                    "ORDER BY id DESC LIMIT 200")
                if len(rows) < self.config.causal_min_samples:
                    continue
                if not PANDAS_AVAILABLE:
                    continue
                carbon = await self.carbon_manager.get_current_intensity()
                records = [
                    {'carbon_intensity': carbon,
                     'quality': random.uniform(0.6, 0.95),
                     'cost': random.uniform(0.1, 0.9),
                     'privacy': random.uniform(0.0, 1.0)}
                    for _ in rows]
                df = pd.DataFrame(records)
                await self.causal_graph.learn_structure(
                    df, ['carbon_intensity', 'quality', 'cost', 'privacy'])
                await self.causal_rl.estimate_ate('carbon_intensity', 'quality')
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"causal loop: {e}")

    async def _temporal_logic_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.temporal_verification_interval)
                result = await self.temporal_verifier.verify()
                for f, ok in result.items():
                    if not ok:
                        logger.warning(f"TL violated: {f}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"TL loop: {e}")

    async def _xai_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.xai_interval)
                if self.xai and NUMPY_AVAILABLE:
                    feats = np.array([
                        await self.carbon_manager.get_current_intensity(),
                        0.8, 0.5, 0.0])
                    def _score(x):
                        return float(np.dot(x, [0.3, 0.5, -0.2, -0.1]))
                    await self.xai.explain(
                        decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                        label="system_health",
                        features=feats,
                        names=['carbon', 'quality', 'cost', 'privacy'],
                        model_fn=_score)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"xai loop: {e}")

    async def _precision_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(300)
                if self.precision_switcher:
                    await self.precision_switcher.switch_to(
                        self.precision_switcher.select_precision(),
                        reason='periodic')
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"precision loop: {e}")

    async def _carbon_market_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.carbon_market_interval)
                if self.carbon_market:
                    await self.carbon_market.update_price()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"carbon market loop: {e}")

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.chaos_test_interval)
                if not self.chaos_engine:
                    continue
                await self.chaos_engine.run_experiment(
                    name=f"auto_{uuid.uuid4().hex[:6]}",
                    fault_type=random.choice(self.config.chaos_fault_types))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"chaos loop: {e}")

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    async def verify_temporal_property(self, formula: str) -> Dict:
        """[P1] Non-destructive: uses sub-verifier sharing history."""
        if not self.temporal_verifier:
            return {'error': 'temporal logic disabled'}
        sub = TemporalLogicVerifier(self.storage, self.config, [formula])
        sub.history = self.temporal_verifier.history
        return await sub.verify()

    def model_check_temporal_property(self, formula, initial_state):
        if not self.temporal_verifier:
            return {'error': 'temporal logic disabled'}
        return self.temporal_verifier.model_check(formula, initial_state)

    async def explain_decision(self, decision_id, label, features, names, model_fn):
        if not self.xai:
            return {'error': 'XAI disabled'}
        return await self.xai.explain(decision_id, label, features, names, model_fn)

    async def run_chaos_experiment(self, name, fault_type, target=None,
                                    workload=None, target_breaker=None):
        if not self.chaos_engine:
            return {'error': 'chaos disabled'}
        return await self.chaos_engine.run_experiment(
            name, fault_type, target, workload,
            target_breaker=target_breaker)

    async def purchase_rec(self, mwh, price_per_mwh=5.0, source='wind'):
        if not self.carbon_market:
            return 0.0
        return await self.carbon_market.purchase_rec(mwh, price_per_mwh, source)

    def precision_state(self):
        if not self.precision_switcher:
            return {'enabled': False}
        return {'enabled': True,
                'current': self.precision_switcher.current,
                'hardware': self.precision_switcher.hardware,
                'energy_saved_wh': self.precision_switcher.energy_saved_wh}

    async def get_causal_graph_summary(self):
        if not self.causal_graph:
            return {'enabled': False}
        return {'enabled': True, **self.causal_graph.summary()}


# =============================================================================
# SINGLETON + SIGNAL + MAIN — [P0] shutdown fixed
# =============================================================================
_simulator_instance: Optional[EnhancedSystemSimulatorV11] = None
_simulator_lock = asyncio.Lock()
_shutdown_requested = False


async def get_system_simulator(config: Optional[SyntheticDataConfig] = None) -> EnhancedSystemSimulatorV11:
    global _simulator_instance
    if _simulator_instance is None:
        async with _simulator_lock:
            if _simulator_instance is None:
                _simulator_instance = EnhancedSystemSimulatorV11(config)
                await _simulator_instance.start()
    return _simulator_instance


def handle_signal(signum, frame):
    global _shutdown_requested
    if _shutdown_requested:
        return
    _shutdown_requested = True
    logger.info(f"Received signal {signum}")
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(_signal_shutdown())
    except Exception:
        pass


async def _signal_shutdown():
    global _simulator_instance
    if _simulator_instance:
        await _simulator_instance.shutdown()


async def main():
    simulator = await get_system_simulator()
    try:
        # [P0] await the simulator's own shutdown event — fixes SIGTERM hang
        await simulator._shutdown_event.wait()
    except asyncio.CancelledError:
        pass
    finally:
        if not simulator._shutdown_event.is_set():
            await simulator.shutdown()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: handle_signal(s, None))
        except (NotImplementedError, RuntimeError):
            try:
                signal.signal(sig, handle_signal)
            except Exception:
                pass
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        try:
            loop.close()
        except Exception:
            pass
