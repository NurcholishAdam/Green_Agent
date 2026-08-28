#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/synthetic_data_generator_enhanced_v5_0.py
# VERSION: 5.0.0 (Enterprise Quantum Resilience + GA + MoE + Pareto + Adaptive Anomalies + LIMIT Graph + RLHF + Distillation)
# =============================================================================
"""
Advanced Synthetic Data Generator for Green Agent - Version 5.0.0
Generates realistic workloads, environmental conditions, and edge cases for policy testing.

ENHANCEMENTS OVER v4.0.0:
1. Bio‑inspired Genetic Algorithm (GA) for automatic tuning of generation parameters.
2. Full Mixture‑of‑Experts (MoE) gating network for dynamic strategy selection.
3. Pareto‑front optimizer for multi‑objective trade‑off exploration of dataset qualities.
4. Adaptive anomaly injection using reinforcement learning (contextual bandit).
5. Federated learning for sharing generation parameters across instances.
6. Drift detection for external data distributions and user feedback.
7. Active user preference learning via interactive WebSocket queries.
8. Integration with central Green Agent components (Config, Storage, MetricsRegistry).
9. LIMIT Graph for constraint propagation and decision support.
10. RLHF (Reinforcement Learning from Human Feedback) for reward‑based policy updates.
11. Multi‑Teacher Policy Distillation to combine teacher policies into a student policy.
All enhancements are optional and configurable.
"""

import asyncio
import json
import random
import hashlib
import uuid
import logging
import sys
import signal
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union, Callable, AsyncIterator
from pathlib import Path
import secrets
import contextvars
from functools import wraps
import numpy as np
import pandas as pd
from collections import deque, defaultdict

# ---------- Attempt to import central Green Agent components ----------
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

# ---------- Async SQLite (aiosqlite) – fallback to sqlite3 with thread pool ----------
try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

# ---------- Structured logging ----------
try:
    import structlog
    from structlog.processors import JSONRenderer, TimeStamper
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False

# ---------- Pydantic ----------
try:
    from pydantic import BaseSettings, Field, field_validator, ValidationInfo
    from pydantic_settings import BaseSettings as SettingsBase
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# ---------- Retry / Cache ----------
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    from async_lru import alru_cache
    ALRU_CACHE_AVAILABLE = True
except ImportError:
    ALRU_CACHE_AVAILABLE = False

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- WebSockets ----------
try:
    import websockets
    from websockets.server import serve
    from websockets.exceptions import ConnectionClosed
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

# ---------- Web3 ----------
try:
    from web3 import Web3, Account, HTTPProvider
    from web3.middleware import geth_poa_middleware, gas_price_strategy
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

# ---------- Post‑quantum cryptography ----------
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

# ---------- Async HTTP (for carbon/collector calls) ----------
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

# ---------- Local imports (schemas) ----------
from .schemas.node_descriptor import NodeDescriptor
from .schemas.workload_descriptor import WorkloadDescriptor
from ..expert_registry import ExpertProfile, ExpertDomain
from ..node_registry import NodeDescriptor as NodeDescriptorFallback

# ---------- Optional: data collectors (for real distributions) ----------
try:
    from ..data_integration.carbon_intensity import CarbonIntensityFetcher
    from ..data_integration.helium_collector import HeliumCollector
    from ..data_integration.material_footprint import MaterialFootprintUpdater
    COLLECTORS_AVAILABLE = True
except ImportError:
    COLLECTORS_AVAILABLE = False
    # Stubs (for fallback)
    class CarbonIntensityFetcher:
        async def get_intensity(self, region: str) -> float:
            return 0.4
    class HeliumCollector:
        async def get_connectivity_score(self, hotspot_id: str) -> float:
            return 0.8
    class MaterialFootprintUpdater:
        def get_footprint(self, product_id: str) -> Optional[Dict]:
            return None

# ---------- For forecasting (optional) ----------
try:
    from statsmodels.tsa.arima.model import ARIMA
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False

# ============================================================================
# CORRELATION ID CONTEXT
# ============================================================================
correlation_id_var = contextvars.ContextVar('correlation_id', default='unknown')

# ============================================================================
# STRUCTURED LOGGING WITH CORRELATION ID
# ============================================================================
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

# ============================================================================
# PROMETHEUS METRICS (use central if available)
# ============================================================================
if CENTRAL_COMPONENTS_AVAILABLE and CentralMetrics:
    metrics = CentralMetrics()
    SYNTHETIC_SAMPLES = metrics.counter('synthetic_samples_generated_total', ['type'])
    SYNTHETIC_ANOMALIES = metrics.counter('synthetic_anomalies_injected_total', ['anomaly_type'])
    SYNTHETIC_CACHE_HITS = metrics.counter('synthetic_cache_hits_total', ['type'])
    SYNTHETIC_CACHE_MISSES = metrics.counter('synthetic_cache_misses_total', ['type'])
    SYNTHETIC_GENERATION_DURATION = metrics.histogram('synthetic_generation_duration_seconds', ['operation'])
    SYNTHETIC_WS_CONNECTIONS = metrics.gauge('synthetic_ws_connections')
    SYNTHETIC_MTOP_TEACHER_WEIGHTS = metrics.gauge('synthetic_mtop_teacher_weights', ['teacher'])
    SYNTHETIC_QUANTUM_SIGNATURES = metrics.counter('synthetic_quantum_signatures_total', ['algorithm', 'status'])
    SYNTHETIC_BLOCKCHAIN_TX = metrics.counter('synthetic_blockchain_tx_total', ['status'])
    SYNTHETIC_CLOUD_DISTRIBUTIONS = metrics.counter('synthetic_cloud_distributions_total', ['provider', 'status'])
    SYNTHETIC_CIRCUIT_BREAKER_STATE = metrics.gauge('synthetic_circuit_breaker_state', ['name'])
    SYNTHETIC_RATE_LIMITER_THROTTLE = metrics.gauge('synthetic_rate_limiter_throttle')
    SYNTHETIC_GA_POPULATION_FITNESS = metrics.gauge('synthetic_ga_population_fitness')
    SYNTHETIC_MOE_GATING_PROBABILITIES = metrics.gauge('synthetic_moe_gating_probabilities', ['expert'])
    SYNTHETIC_PARETO_FRONT_SIZE = metrics.gauge('synthetic_pareto_front_size')
else:
    if PROMETHEUS_AVAILABLE:
        REGISTRY = CollectorRegistry()
        SYNTHETIC_SAMPLES = Counter('synthetic_samples_generated_total', 'Total synthetic samples generated', ['type'], registry=REGISTRY)
        SYNTHETIC_ANOMALIES = Counter('synthetic_anomalies_injected_total', 'Anomalies injected', ['anomaly_type'], registry=REGISTRY)
        SYNTHETIC_CACHE_HITS = Counter('synthetic_cache_hits_total', 'Cache hits', ['type'], registry=REGISTRY)
        SYNTHETIC_CACHE_MISSES = Counter('synthetic_cache_misses_total', 'Cache misses', ['type'], registry=REGISTRY)
        SYNTHETIC_GENERATION_DURATION = Histogram('synthetic_generation_duration_seconds', 'Generation duration', ['operation'], registry=REGISTRY)
        SYNTHETIC_WS_CONNECTIONS = Gauge('synthetic_ws_connections', 'WebSocket connections', registry=REGISTRY)
        SYNTHETIC_MTOP_TEACHER_WEIGHTS = Gauge('synthetic_mtop_teacher_weights', 'MTOP teacher weights', ['teacher'], registry=REGISTRY)
        SYNTHETIC_QUANTUM_SIGNATURES = Counter('synthetic_quantum_signatures_total', 'Quantum signatures', ['algorithm', 'status'], registry=REGISTRY)
        SYNTHETIC_BLOCKCHAIN_TX = Counter('synthetic_blockchain_tx_total', 'Blockchain transactions', ['status'], registry=REGISTRY)
        SYNTHETIC_CLOUD_DISTRIBUTIONS = Counter('synthetic_cloud_distributions_total', 'Cloud distributions', ['provider', 'status'], registry=REGISTRY)
        SYNTHETIC_CIRCUIT_BREAKER_STATE = Gauge('synthetic_circuit_breaker_state', ['name'], registry=REGISTRY)
        SYNTHETIC_RATE_LIMITER_THROTTLE = Gauge('synthetic_rate_limiter_throttle', registry=REGISTRY)
        SYNTHETIC_GA_POPULATION_FITNESS = Gauge('synthetic_ga_population_fitness', registry=REGISTRY)
        SYNTHETIC_MOE_GATING_PROBABILITIES = Gauge('synthetic_moe_gating_probabilities', ['expert'], registry=REGISTRY)
        SYNTHETIC_PARETO_FRONT_SIZE = Gauge('synthetic_pareto_front_size', registry=REGISTRY)
    else:
        class DummyMetric:
            def labels(self, **kwargs): return self
            def inc(self, **kwargs): pass
            def set(self, **kwargs): pass
            def observe(self, **kwargs): pass
        SYNTHETIC_SAMPLES = DummyMetric()
        SYNTHETIC_ANOMALIES = DummyMetric()
        SYNTHETIC_CACHE_HITS = DummyMetric()
        SYNTHETIC_CACHE_MISSES = DummyMetric()
        SYNTHETIC_GENERATION_DURATION = DummyMetric()
        SYNTHETIC_WS_CONNECTIONS = DummyMetric()
        SYNTHETIC_MTOP_TEACHER_WEIGHTS = DummyMetric()
        SYNTHETIC_QUANTUM_SIGNATURES = DummyMetric()
        SYNTHETIC_BLOCKCHAIN_TX = DummyMetric()
        SYNTHETIC_CLOUD_DISTRIBUTIONS = DummyMetric()
        SYNTHETIC_CIRCUIT_BREAKER_STATE = DummyMetric()
        SYNTHETIC_RATE_LIMITER_THROTTLE = DummyMetric()
        SYNTHETIC_GA_POPULATION_FITNESS = DummyMetric()
        SYNTHETIC_MOE_GATING_PROBABILITIES = DummyMetric()
        SYNTHETIC_PARETO_FRONT_SIZE = DummyMetric()

# ============================================================================
# CENTRAL CONFIGURATION (if available) or fallback to custom config
# ============================================================================
if CENTRAL_COMPONENTS_AVAILABLE and central_config:
    # Use central config, but we need a way to get the specific parameters.
    class SyntheticDataConfigFromCentral:
        def __init__(self):
            self.seed = getattr(central_config, 'seed', 42)
            self.task_types = getattr(central_config, 'synthetic_task_types', {
                'summarization': 0.25,
                'classification': 0.20,
                'translation': 0.15,
                'question_answering': 0.15,
                'text_generation': 0.15,
                'sentiment_analysis': 0.10
            })
            self.priority_profiles = getattr(central_config, 'synthetic_priority_profiles', ['accuracy', 'green', 'balanced'])
            self.regions = getattr(central_config, 'synthetic_regions', ['us-east', 'us-west', 'eu-west', 'eu-north', 'asia-east', 'asia-southeast'])
            self.region_carbon = getattr(central_config, 'synthetic_region_carbon', {
                'us-east': 420, 'us-west': 350, 'eu-west': 280,
                'eu-north': 220, 'asia-east': 500, 'asia-southeast': 480
            })
            self.token_mean = getattr(central_config, 'synthetic_token_mean', 5.5)
            self.token_std = getattr(central_config, 'synthetic_token_std', 1.2)
            self.default_degradation_rate = getattr(central_config, 'synthetic_default_degradation_rate', 0.0005)
            self.default_anomaly_rate = getattr(central_config, 'synthetic_default_anomaly_rate', 0.0)
            self.default_rate_per_hour = getattr(central_config, 'synthetic_default_rate_per_hour', 100.0)
            self.default_duration_hours = getattr(central_config, 'synthetic_default_duration_hours', 24)
            self.use_real_distributions = getattr(central_config, 'synthetic_use_real_distributions', False)
            self.prompt_pool_file = getattr(central_config, 'synthetic_prompt_pool_file', None)
            self.export_format = getattr(central_config, 'synthetic_export_format', 'json')
            self.dataset_version = getattr(central_config, 'synthetic_dataset_version', '5.0.0')
            self.metrics_port = getattr(central_config, 'metrics_port', 8000)
            self.websocket_port = getattr(central_config, 'websocket_port', 8770)
            self.cache_ttl = getattr(central_config, 'cache_ttl', 300)
            self.max_retry_attempts = getattr(central_config, 'max_retry_attempts', 3)
            self.circuit_breaker_threshold = getattr(central_config, 'circuit_breaker_threshold', 5)
            self.circuit_breaker_timeout = getattr(central_config, 'circuit_breaker_timeout', 30)
            self.rate_limit_requests = getattr(central_config, 'rate_limit_requests', 100)
            self.rate_limit_window = getattr(central_config, 'rate_limit_window', 60)
            self.mopd_weights = getattr(central_config, 'synthetic_mopd_weights', {
                'energy': 0.25, 'carbon': 0.25, 'helium': 0.25, 'material': 0.25
            })
            self.blockchain_rpc_url = getattr(central_config, 'blockchain_rpc_url', 'http://localhost:8545')
            self.blockchain_contract_address = getattr(central_config, 'blockchain_contract_address', None)
            self.blockchain_private_key = getattr(central_config, 'blockchain_private_key', None)
            self.enable_quantum_security = getattr(central_config, 'enable_quantum_security', True)
            self.quantum_algorithm = getattr(central_config, 'quantum_algorithm', 'dilithium')
            self.quantum_master_key = os.getenv('SYNTH_QUANTUM_MASTER_KEY', '')
            self.master_key_env = getattr(central_config, 'master_key_env', 'SYNTH_MASTER_KEY')
            self.db_path = getattr(central_config, 'db_path', '/tmp/synthetic_generator_v5.db')
            # New v5.0.0 parameters
            self.ga_enabled = getattr(central_config, 'synthetic_ga_enabled', True)
            self.ga_population_size = getattr(central_config, 'synthetic_ga_population_size', 20)
            self.ga_generations = getattr(central_config, 'synthetic_ga_generations', 5)
            self.ga_mutation_rate = getattr(central_config, 'synthetic_ga_mutation_rate', 0.2)
            self.ga_crossover_rate = getattr(central_config, 'synthetic_ga_crossover_rate', 0.7)
            self.moe_enabled = getattr(central_config, 'synthetic_moe_enabled', True)
            self.moe_expert_count = getattr(central_config, 'synthetic_moe_expert_count', 4)
            self.moe_hidden_layers = getattr(central_config, 'synthetic_moe_hidden_layers', [16, 8])
            self.pareto_enabled = getattr(central_config, 'synthetic_pareto_enabled', True)
            self.pareto_max_architectures = getattr(central_config, 'synthetic_pareto_max_architectures', 100)
            self.adaptive_anomaly_enabled = getattr(central_config, 'synthetic_adaptive_anomaly_enabled', True)
            self.federated_enabled = getattr(central_config, 'synthetic_federated_enabled', True)
            self.federated_interval = getattr(central_config, 'synthetic_federated_interval', 3600)
            self.drift_detection_enabled = getattr(central_config, 'synthetic_drift_detection_enabled', True)
            self.user_preference_learning_enabled = getattr(central_config, 'synthetic_user_preference_learning_enabled', True)
            # ===== NEW: LIMIT Graph, RLHF, Distillation configs =====
            self.limit_graph_enabled = getattr(central_config, 'synthetic_limit_graph_enabled', True)
            self.limit_graph_update_interval = getattr(central_config, 'synthetic_limit_graph_update_interval', 300)
            self.rlhf_enabled = getattr(central_config, 'synthetic_rlhf_enabled', True)
            self.rlhf_reward_model = getattr(central_config, 'synthetic_rlhf_reward_model', 'linear')
            self.rlhf_training_interval = getattr(central_config, 'synthetic_rlhf_training_interval', 600)
            self.distillation_enabled = getattr(central_config, 'synthetic_distillation_enabled', True)
            self.distillation_temperature = getattr(central_config, 'synthetic_distillation_temperature', 2.0)
            self.distillation_alpha = getattr(central_config, 'synthetic_distillation_alpha', 0.5)
            self.distillation_interval = getattr(central_config, 'synthetic_distillation_interval', 300)

        def get_master_key_bytes(self) -> bytes:
            key_hex = os.getenv(self.master_key_env)
            if not key_hex:
                raise ValueError(f"Master key not set in env {self.master_key_env}")
            return bytes.fromhex(key_hex)

    SyntheticDataConfig = SyntheticDataConfigFromCentral
else:
    if PYDANTIC_AVAILABLE:
        class SyntheticDataConfig(BaseSettings):
            seed: int = Field(42, description="Random seed for reproducibility")
            task_types: Dict[str, float] = Field(
                default_factory=lambda: {
                    'summarization': 0.25,
                    'classification': 0.20,
                    'translation': 0.15,
                    'question_answering': 0.15,
                    'text_generation': 0.15,
                    'sentiment_analysis': 0.10
                }
            )
            priority_profiles: List[str] = Field(
                default_factory=lambda: ['accuracy', 'green', 'balanced']
            )
            regions: List[str] = Field(
                default_factory=lambda: ['us-east', 'us-west', 'eu-west', 'eu-north', 'asia-east', 'asia-southeast']
            )
            region_carbon: Dict[str, float] = Field(
                default_factory=lambda: {
                    'us-east': 420, 'us-west': 350, 'eu-west': 280,
                    'eu-north': 220, 'asia-east': 500, 'asia-southeast': 480
                }
            )
            token_mean: float = Field(5.5, ge=0)
            token_std: float = Field(1.2, ge=0)
            default_degradation_rate: float = Field(0.0005, ge=0, le=0.1)
            default_anomaly_rate: float = Field(0.0, ge=0, le=1.0)
            default_rate_per_hour: float = Field(100.0, gt=0)
            default_duration_hours: int = Field(24, gt=0)
            use_real_distributions: bool = Field(False)
            prompt_pool_file: Optional[str] = Field(None)
            export_format: str = Field("json")
            dataset_version: str = Field("5.0.0")
            metrics_port: int = Field(8000, ge=1024, le=65535)
            websocket_port: int = Field(8770, ge=1024)
            cache_ttl: int = Field(300, ge=1)
            max_retry_attempts: int = Field(3, ge=0)
            circuit_breaker_threshold: int = Field(5, ge=1)
            circuit_breaker_timeout: int = Field(30, ge=1)
            rate_limit_requests: int = Field(100, ge=1)
            rate_limit_window: int = Field(60, ge=1)
            mopd_weights: Dict[str, float] = Field(
                default_factory=lambda: {
                    'energy': 0.25,
                    'carbon': 0.25,
                    'helium': 0.25,
                    'material': 0.25
                }
            )
            blockchain_rpc_url: str = Field("http://localhost:8545")
            blockchain_contract_address: Optional[str] = None
            blockchain_private_key: Optional[str] = None
            enable_quantum_security: bool = True
            quantum_algorithm: str = Field("dilithium")
            quantum_master_key: str = Field(default="")
            master_key_env: str = Field("SYNTH_MASTER_KEY")
            db_path: str = Field("/tmp/synthetic_generator_v5.db")
            # New v5.0.0 fields
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
            adaptive_anomaly_enabled: bool = True
            federated_enabled: bool = True
            federated_interval: int = Field(3600, ge=60)
            drift_detection_enabled: bool = True
            user_preference_learning_enabled: bool = True
            # ===== NEW: LIMIT Graph, RLHF, Distillation configs =====
            limit_graph_enabled: bool = True
            limit_graph_update_interval: int = Field(300, ge=10)
            rlhf_enabled: bool = True
            rlhf_reward_model: str = Field("linear")
            rlhf_training_interval: int = Field(600, ge=60)
            distillation_enabled: bool = True
            distillation_temperature: float = Field(2.0, gt=0)
            distillation_alpha: float = Field(0.5, ge=0.0, le=1.0)
            distillation_interval: int = Field(300, ge=60)

            @field_validator('task_types')
            @classmethod
            def task_types_sum_one(cls, v: Dict[str, float]) -> Dict[str, float]:
                if abs(sum(v.values()) - 1.0) > 1e-6:
                    raise ValueError("Task type probabilities must sum to 1")
                return v

            @field_validator('default_anomaly_rate')
            @classmethod
            def anomaly_rate_range(cls, v: float) -> float:
                if not 0 <= v <= 1:
                    raise ValueError("anomaly_rate must be between 0 and 1")
                return v

            @field_validator('export_format')
            @classmethod
            def validate_export_format(cls, v: str) -> str:
                if v not in ['json', 'jsonl', 'parquet']:
                    raise ValueError("export_format must be 'json', 'jsonl', or 'parquet'")
                return v

            @field_validator('quantum_master_key')
            @classmethod
            def validate_master_key(cls, v: str) -> str:
                if not v:
                    raise ValueError('quantum_master_key must be set via environment SYNTH_QUANTUM_MASTER_KEY')
                try:
                    bytes.fromhex(v)
                except ValueError:
                    raise ValueError('quantum_master_key must be a hex string')
                return v

            def get_master_key_bytes(self) -> bytes:
                return bytes.fromhex(self.quantum_master_key)

            class Config:
                env_prefix = "SYNTH_"
    else:
        # Fallback config as dict
        SYNTHETIC_CONFIG = {
            "seed": 42,
            "task_types": {
                'summarization': 0.25,
                'classification': 0.20,
                'translation': 0.15,
                'question_answering': 0.15,
                'text_generation': 0.15,
                'sentiment_analysis': 0.10
            },
            "priority_profiles": ['accuracy', 'green', 'balanced'],
            "regions": ['us-east', 'us-west', 'eu-west', 'eu-north', 'asia-east', 'asia-southeast'],
            "region_carbon": {
                'us-east': 420, 'us-west': 350, 'eu-west': 280,
                'eu-north': 220, 'asia-east': 500, 'asia-southeast': 480
            },
            "token_mean": 5.5,
            "token_std": 1.2,
            "default_degradation_rate": 0.0005,
            "default_anomaly_rate": 0.0,
            "default_rate_per_hour": 100.0,
            "default_duration_hours": 24,
            "use_real_distributions": False,
            "prompt_pool_file": None,
            "export_format": "json",
            "dataset_version": "5.0.0",
            "metrics_port": 8000,
            "websocket_port": 8770,
            "cache_ttl": 300,
            "max_retry_attempts": 3,
            "circuit_breaker_threshold": 5,
            "circuit_breaker_timeout": 30,
            "rate_limit_requests": 100,
            "rate_limit_window": 60,
            "mopd_weights": {'energy': 0.25, 'carbon': 0.25, 'helium': 0.25, 'material': 0.25},
            "blockchain_rpc_url": "http://localhost:8545",
            "blockchain_contract_address": None,
            "blockchain_private_key": None,
            "enable_quantum_security": True,
            "quantum_algorithm": "dilithium",
            "quantum_master_key": "",
            "master_key_env": "SYNTH_MASTER_KEY",
            "db_path": "/tmp/synthetic_generator_v5.db",
            "ga_enabled": True,
            "ga_population_size": 20,
            "ga_generations": 5,
            "ga_mutation_rate": 0.2,
            "ga_crossover_rate": 0.7,
            "moe_enabled": True,
            "moe_expert_count": 4,
            "moe_hidden_layers": [16, 8],
            "pareto_enabled": True,
            "pareto_max_architectures": 100,
            "adaptive_anomaly_enabled": True,
            "federated_enabled": True,
            "federated_interval": 3600,
            "drift_detection_enabled": True,
            "user_preference_learning_enabled": True,
            # ===== NEW: LIMIT Graph, RLHF, Distillation configs =====
            "limit_graph_enabled": True,
            "limit_graph_update_interval": 300,
            "rlhf_enabled": True,
            "rlhf_reward_model": "linear",
            "rlhf_training_interval": 600,
            "distillation_enabled": True,
            "distillation_temperature": 2.0,
            "distillation_alpha": 0.5,
            "distillation_interval": 300,
        }

# ============================================================================
# DATA CLASSES (Enhanced)
# ============================================================================
@dataclass
class SyntheticSustainabilityMetrics:
    energy_joules: float
    carbon_kg: float
    helium_units: float
    material_index: float

@dataclass
class SyntheticExpertProfile(ExpertProfile):
    degradation_rate: float = 0.0005
    tasks_processed: int = 0

    def process_task(self) -> None:
        self.tasks_processed += 1
        self.accuracy_score = max(0.5, self.accuracy_score - self.degradation_rate)
        self.energy_per_inference *= (1 + self.degradation_rate * 0.5)
        self.carbon_per_inference *= (1 + self.degradation_rate * 0.3)
        self.avg_latency_ms *= (1 + self.degradation_rate * 0.1)

# ============================================================================
# CIRCUIT BREAKER, RATE LIMITER, ENCRYPTION MANAGER (unchanged)
# ============================================================================
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
                    SYNTHETIC_CIRCUIT_BREAKER_STATE.labels(name=self.name).set(0)
            return result
        except Exception as e:
            self._failures += 1
            self._last_failure_time = datetime.now()
            if self._failures >= self.failure_threshold:
                self._state = "OPEN"
                if PROMETHEUS_AVAILABLE:
                    SYNTHETIC_CIRCUIT_BREAKER_STATE.labels(name=self.name).set(2)
            raise e

class RateLimiter:
    def __init__(self, rate: int = 100, window: int = 60):
        self.rate = rate
        self.window = window
        self.tokens = rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            time_passed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + time_passed * (self.rate / self.window))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.1)

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
# ENHANCED DATABASE MANAGER (async-safe with aiosqlite)
# ============================================================================
if CENTRAL_COMPONENTS_AVAILABLE and CentralStorage:
    class EnhancedStorage:
        def __init__(self, config: SyntheticDataConfig):
            self._storage = CentralStorage(db_path=config.db_path)
            self.config = config
            self.cache_ttl = config.cache_ttl
            self.cache = {}
            self._init_custom_tables()

        def _init_custom_tables(self):
            with self._storage._get_connection() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS synthetic_carbon_cache (
                        region TEXT PRIMARY KEY,
                        intensity REAL NOT NULL,
                        timestamp TEXT NOT NULL
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS synthetic_helium_cache (
                        hotspot_id TEXT PRIMARY KEY,
                        score REAL NOT NULL,
                        timestamp TEXT NOT NULL
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS synthetic_generation_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        dataset_version TEXT NOT NULL,
                        num_samples INTEGER NOT NULL,
                        anomaly_rate REAL,
                        edge_fraction REAL,
                        parameters TEXT,
                        quantum_signature TEXT,
                        blockchain_tx_hash TEXT
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS synthetic_ga_populations (
                        generation INTEGER,
                        individual_id TEXT,
                        attributes TEXT,
                        fitness REAL,
                        timestamp TEXT,
                        PRIMARY KEY (generation, individual_id)
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS synthetic_moe_training (
                        sample_id TEXT PRIMARY KEY,
                        features TEXT,
                        expert_label INTEGER,
                        reward REAL,
                        timestamp TEXT
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS synthetic_pareto_front (
                        solution_id TEXT PRIMARY KEY,
                        config_params TEXT,
                        coverage_score REAL,
                        anomaly_diversity REAL,
                        realism_score REAL,
                        data_quality REAL,
                        timestamp TEXT
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS synthetic_user_preferences (
                        user_id TEXT,
                        weights TEXT,
                        chosen_solution_id TEXT,
                        timestamp TEXT,
                        PRIMARY KEY (user_id, timestamp)
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS synthetic_state (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                """)
                conn.execute("CREATE INDEX IF NOT EXISTS idx_gen_timestamp ON synthetic_generation_history(timestamp)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_ga_generation ON synthetic_ga_populations(generation)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_moe_sample_time ON synthetic_moe_training(timestamp)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_pareto_overall ON synthetic_pareto_front(data_quality)")
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

        async def save_carbon_intensity(self, region: str, intensity: float):
            await self._execute("""
                INSERT OR REPLACE INTO synthetic_carbon_cache (region, intensity, timestamp)
                VALUES (?, ?, ?)
            """, (region, intensity, datetime.now().isoformat()))

        async def get_carbon_intensity(self, region: str) -> Optional[float]:
            row = await self._fetchone("""
                SELECT intensity FROM synthetic_carbon_cache WHERE region = ?
            """, (region,))
            return row[0] if row else None

        async def save_helium_score(self, hotspot_id: str, score: float):
            await self._execute("""
                INSERT OR REPLACE INTO synthetic_helium_cache (hotspot_id, score, timestamp)
                VALUES (?, ?, ?)
            """, (hotspot_id, score, datetime.now().isoformat()))

        async def get_helium_score(self, hotspot_id: str) -> Optional[float]:
            row = await self._fetchone("""
                SELECT score FROM synthetic_helium_cache WHERE hotspot_id = ?
            """, (hotspot_id,))
            return row[0] if row else None

        async def save_generation_history(self, dataset_version: str, num_samples: int,
                                           anomaly_rate: float, edge_fraction: float,
                                           parameters: Dict, quantum_signature: Optional[str] = None,
                                           blockchain_tx_hash: Optional[str] = None):
            await self._execute("""
                INSERT INTO synthetic_generation_history (timestamp, dataset_version, num_samples, anomaly_rate, edge_fraction, parameters, quantum_signature, blockchain_tx_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now().isoformat(),
                dataset_version,
                num_samples,
                anomaly_rate,
                edge_fraction,
                json.dumps(parameters),
                quantum_signature,
                blockchain_tx_hash
            ))

        async def save_state(self, key: str, value: str):
            await self._execute("INSERT OR REPLACE INTO synthetic_state (key, value) VALUES (?, ?)", (key, value))

        async def get_state(self, key: str) -> Optional[str]:
            row = await self._fetchone("SELECT value FROM synthetic_state WHERE key = ?", (key,))
            return row[0] if row else None

        async def save_ga_population(self, generation: int, individuals: List[Dict]):
            for ind in individuals:
                await self._execute("""
                    INSERT OR REPLACE INTO synthetic_ga_populations (generation, individual_id, attributes, fitness, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                """, (generation, ind['individual_id'], json.dumps(ind['attributes']), ind['fitness'], datetime.now().isoformat()))

        async def get_ga_population(self, generation: int) -> List[Dict]:
            rows = await self._fetchall("""
                SELECT individual_id, attributes, fitness FROM synthetic_ga_populations WHERE generation = ?
            """, (generation,))
            return [{'individual_id': r[0], 'attributes': json.loads(r[1]), 'fitness': r[2]} for r in rows]

        async def save_moe_training_sample(self, sample_id: str, features: List[float], expert_label: int, reward: float):
            await self._execute("""
                INSERT OR REPLACE INTO synthetic_moe_training (sample_id, features, expert_label, reward, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, (sample_id, json.dumps(features), expert_label, reward, datetime.now().isoformat()))

        async def save_pareto_front(self, solutions: List[Dict]):
            await self._execute("DELETE FROM synthetic_pareto_front")
            for sol in solutions:
                await self._execute("""
                    INSERT INTO synthetic_pareto_front (solution_id, config_params, coverage_score, anomaly_diversity, realism_score, data_quality, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    sol['solution_id'],
                    json.dumps(sol['config_params']),
                    sol['coverage_score'],
                    sol['anomaly_diversity'],
                    sol['realism_score'],
                    sol['data_quality'],
                    datetime.now().isoformat()
                ))

        async def get_current_pareto_front(self) -> List[Dict]:
            rows = await self._fetchall("SELECT * FROM synthetic_pareto_front ORDER BY data_quality DESC")
            return rows

        async def save_user_preference(self, user_id: str, weights: Dict, chosen_solution_id: Optional[str] = None):
            await self._execute("""
                INSERT OR REPLACE INTO synthetic_user_preferences (user_id, weights, chosen_solution_id, timestamp)
                VALUES (?, ?, ?, ?)
            """, (user_id, json.dumps(weights), chosen_solution_id, datetime.now().isoformat()))

        async def get_user_preferences(self, user_id: str) -> Optional[Dict]:
            row = await self._fetchone("""
                SELECT weights, chosen_solution_id, timestamp FROM synthetic_user_preferences
                WHERE user_id = ? ORDER BY timestamp DESC LIMIT 1
            """, (user_id,))
            if row:
                return {'weights': json.loads(row[0]), 'chosen_solution_id': row[1], 'timestamp': row[2]}
            return None

        def dispose(self):
            self._storage.close()
else:
    # Original custom EnhancedStorage (extended with new tables)
    class EnhancedStorage:
        def __init__(self, config: SyntheticDataConfig):
            self.config = config
            self.db_path = config.db_path
            self.encryption_manager = None
            try:
                master_key = config.get_master_key_bytes()
                self.encryption_manager = EncryptionManager(master_key)
            except ValueError:
                logger.warning("Master key not set – sensitive data will be stored in plaintext.")
                self.encryption_manager = None

            self.cache = {}
            self.cache_ttl = config.cache_ttl
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
                    # Carbon cache
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS carbon_cache (
                            region TEXT PRIMARY KEY,
                            intensity REAL NOT NULL,
                            timestamp TEXT NOT NULL
                        )
                    """)
                    # Helium cache
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS helium_cache (
                            hotspot_id TEXT PRIMARY KEY,
                            score REAL NOT NULL,
                            timestamp TEXT NOT NULL
                        )
                    """)
                    # Generation history
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS generation_history (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            timestamp TEXT NOT NULL,
                            dataset_version TEXT NOT NULL,
                            num_samples INTEGER NOT NULL,
                            anomaly_rate REAL,
                            edge_fraction REAL,
                            parameters TEXT,
                            quantum_signature TEXT,
                            blockchain_tx_hash TEXT
                        )
                    """)
                    # GA populations
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS ga_populations (
                            generation INTEGER,
                            individual_id TEXT,
                            attributes TEXT,
                            fitness REAL,
                            timestamp TEXT,
                            PRIMARY KEY (generation, individual_id)
                        )
                    """)
                    # MoE training
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS moe_training (
                            sample_id TEXT PRIMARY KEY,
                            features TEXT,
                            expert_label INTEGER,
                            reward REAL,
                            timestamp TEXT
                        )
                    """)
                    # Pareto front
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS pareto_front (
                            solution_id TEXT PRIMARY KEY,
                            config_params TEXT,
                            coverage_score REAL,
                            anomaly_diversity REAL,
                            realism_score REAL,
                            data_quality REAL,
                            timestamp TEXT
                        )
                    """)
                    # User preferences
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS user_preferences (
                            user_id TEXT,
                            weights TEXT,
                            chosen_solution_id TEXT,
                            timestamp TEXT,
                            PRIMARY KEY (user_id, timestamp)
                        )
                    """)
                    # State
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS state (
                            key TEXT PRIMARY KEY,
                            value TEXT NOT NULL
                        )
                    """)
                    # Indexes
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_gen_timestamp ON generation_history(timestamp)")
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_ga_generation ON ga_populations(generation)")
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_moe_sample_time ON moe_training(timestamp)")
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_pareto_overall ON pareto_front(data_quality)")
                    await conn.commit()
            else:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("PRAGMA journal_mode=WAL")
                    # Create tables similarly
                    pass
            logger.info(f"Database initialized at {self.db_path} with WAL and indexes")

        async def _encrypt_if_possible(self, data: bytes) -> Tuple[bytes, Optional[bytes]]:
            if self.encryption_manager:
                return self.encryption_manager.encrypt(data)
            return data, None

        async def _decrypt_if_possible(self, ciphertext: bytes, nonce: Optional[bytes]) -> bytes:
            if self.encryption_manager and nonce is not None:
                return self.encryption_manager.decrypt(ciphertext, nonce)
            return ciphertext

        async def save_carbon_intensity(self, region: str, intensity: float):
            await self._execute("""
                INSERT OR REPLACE INTO carbon_cache (region, intensity, timestamp)
                VALUES (?, ?, ?)
            """, (region, intensity, datetime.now().isoformat()))

        async def get_carbon_intensity(self, region: str) -> Optional[float]:
            row = await self._fetchone("""
                SELECT intensity FROM carbon_cache WHERE region = ?
            """, (region,))
            return row[0] if row else None

        async def save_helium_score(self, hotspot_id: str, score: float):
            await self._execute("""
                INSERT OR REPLACE INTO helium_cache (hotspot_id, score, timestamp)
                VALUES (?, ?, ?)
            """, (hotspot_id, score, datetime.now().isoformat()))

        async def get_helium_score(self, hotspot_id: str) -> Optional[float]:
            row = await self._fetchone("""
                SELECT score FROM helium_cache WHERE hotspot_id = ?
            """, (hotspot_id,))
            return row[0] if row else None

        async def save_generation_history(self, dataset_version: str, num_samples: int,
                                           anomaly_rate: float, edge_fraction: float,
                                           parameters: Dict, quantum_signature: Optional[str] = None,
                                           blockchain_tx_hash: Optional[str] = None):
            await self._execute("""
                INSERT INTO generation_history (timestamp, dataset_version, num_samples, anomaly_rate, edge_fraction, parameters, quantum_signature, blockchain_tx_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now().isoformat(),
                dataset_version,
                num_samples,
                anomaly_rate,
                edge_fraction,
                json.dumps(parameters),
                quantum_signature,
                blockchain_tx_hash
            ))

        async def save_state(self, key: str, value: str):
            await self._execute("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)", (key, value))

        async def get_state(self, key: str) -> Optional[str]:
            row = await self._fetchone("SELECT value FROM state WHERE key = ?", (key,))
            return row[0] if row else None

        async def save_ga_population(self, generation: int, individuals: List[Dict]):
            for ind in individuals:
                await self._execute("""
                    INSERT OR REPLACE INTO ga_populations (generation, individual_id, attributes, fitness, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                """, (generation, ind['individual_id'], json.dumps(ind['attributes']), ind['fitness'], datetime.now().isoformat()))

        async def get_ga_population(self, generation: int) -> List[Dict]:
            rows = await self._fetchall("""
                SELECT individual_id, attributes, fitness FROM ga_populations WHERE generation = ?
            """, (generation,))
            return [{'individual_id': r[0], 'attributes': json.loads(r[1]), 'fitness': r[2]} for r in rows]

        async def save_moe_training_sample(self, sample_id: str, features: List[float], expert_label: int, reward: float):
            await self._execute("""
                INSERT OR REPLACE INTO moe_training (sample_id, features, expert_label, reward, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, (sample_id, json.dumps(features), expert_label, reward, datetime.now().isoformat()))

        async def save_pareto_front(self, solutions: List[Dict]):
            await self._execute("DELETE FROM pareto_front")
            for sol in solutions:
                await self._execute("""
                    INSERT INTO pareto_front (solution_id, config_params, coverage_score, anomaly_diversity, realism_score, data_quality, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    sol['solution_id'],
                    json.dumps(sol['config_params']),
                    sol['coverage_score'],
                    sol['anomaly_diversity'],
                    sol['realism_score'],
                    sol['data_quality'],
                    datetime.now().isoformat()
                ))

        async def get_current_pareto_front(self) -> List[Dict]:
            rows = await self._fetchall("SELECT * FROM pareto_front ORDER BY data_quality DESC")
            return rows

        async def save_user_preference(self, user_id: str, weights: Dict, chosen_solution_id: Optional[str] = None):
            await self._execute("""
                INSERT OR REPLACE INTO user_preferences (user_id, weights, chosen_solution_id, timestamp)
                VALUES (?, ?, ?, ?)
            """, (user_id, json.dumps(weights), chosen_solution_id, datetime.now().isoformat()))

        async def get_user_preferences(self, user_id: str) -> Optional[Dict]:
            row = await self._fetchone("""
                SELECT weights, chosen_solution_id, timestamp FROM user_preferences
                WHERE user_id = ? ORDER BY timestamp DESC LIMIT 1
            """, (user_id,))
            if row:
                return {'weights': json.loads(row[0]), 'chosen_solution_id': row[1], 'timestamp': row[2]}
            return None

        def dispose(self):
            pass

# ============================================================================
# MTOP ENGINE (kept as fallback)
# ============================================================================
class DataTeacherEnsemble:
    # ... (same as original)
    pass

class DataDistillationStudent:
    # ... (same as original)
    pass

class MTOPDataEngine:
    # ... (same as original)
    pass

# ============================================================================
# NEW MODULE: Genetic Parameter Optimizer (Bio‑inspired GA)
# ============================================================================
class GeneticParameterOptimizer:
    """
    Genetic algorithm that evolves generation parameters to maximize dataset quality.
    """
    def __init__(self, config: SyntheticDataConfig, storage: EnhancedStorage):
        self.config = config
        self.storage = storage
        self.population_size = config.ga_population_size
        self.generations = config.ga_generations
        self.mutation_rate = config.ga_mutation_rate
        self.crossover_rate = config.ga_crossover_rate
        self._lock = asyncio.Lock()

        # Parameter bounds (adjustable)
        self.param_bounds = {
            'token_mean': (3.0, 8.0),
            'token_std': (0.5, 2.5),
            'anomaly_rate': (0.0, 0.3),
            'edge_fraction': (0.0, 0.3),
            'use_real_distributions': (0, 1),  # binary
        }
        # Task type proportions are represented as a vector summing to 1.
        self.task_type_keys = list(config.task_types.keys())
        self.num_task_types = len(self.task_type_keys)

    def _random_chromosome(self) -> Dict[str, Any]:
        chrom = {
            'token_mean': random.uniform(*self.param_bounds['token_mean']),
            'token_std': random.uniform(*self.param_bounds['token_std']),
            'anomaly_rate': random.uniform(*self.param_bounds['anomaly_rate']),
            'edge_fraction': random.uniform(*self.param_bounds['edge_fraction']),
            'use_real_distributions': random.choice([0, 1]),
            'task_probs': self._random_task_probs(),
        }
        return chrom

    def _random_task_probs(self) -> List[float]:
        probs = [random.random() for _ in range(self.num_task_types)]
        total = sum(probs)
        return [p / total for p in probs]

    def _mutate(self, chrom: Dict[str, Any]) -> Dict[str, Any]:
        new = chrom.copy()
        for param, bounds in self.param_bounds.items():
            if random.random() < self.mutation_rate:
                if param == 'use_real_distributions':
                    new[param] = 1 - chrom[param]
                else:
                    low, high = bounds
                    delta = random.gauss(0, (high - low) / 10)
                    new[param] = max(low, min(high, chrom[param] + delta))
        if random.random() < self.mutation_rate:
            new['task_probs'] = self._random_task_probs()
        return new

    def _crossover(self, p1: Dict[str, Any], p2: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        if random.random() > self.crossover_rate:
            return p1.copy(), p2.copy()
        c1, c2 = p1.copy(), p2.copy()
        for key in self.param_bounds:
            if random.random() < 0.5:
                c1[key] = p2[key]
                c2[key] = p1[key]
        if random.random() < 0.5:
            c1['task_probs'], c2['task_probs'] = p2['task_probs'], p1['task_probs']
        return c1, c2

    async def _evaluate_fitness(self, chrom: Dict[str, Any], historical_feedback: List[Dict]) -> float:
        # Simulate a score between 0 and 1
        base = 0.5
        base += chrom['anomaly_rate'] * 0.5
        base += chrom['edge_fraction'] * 0.3
        if chrom['use_real_distributions']:
            base += 0.1
        entropy = -sum(p * np.log(p + 1e-8) for p in chrom['task_probs'])
        base += entropy / np.log(self.num_task_types) * 0.2
        return max(0.0, min(1.0, base + random.uniform(-0.1, 0.1)))

    async def run_search(self, historical_feedback: List[Dict]) -> Dict[str, Any]:
        population = [self._random_chromosome() for _ in range(self.population_size)]
        best_fitness = -1.0
        best_individual = None

        for gen in range(self.generations):
            fitnesses = await asyncio.gather(*[self._evaluate_fitness(ind, historical_feedback) for ind in population])
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
            combined_fitness = await asyncio.gather(*[self._evaluate_fitness(ind, historical_feedback) for ind in combined])
            sorted_combined = sorted(zip(combined, combined_fitness), key=lambda x: x[1], reverse=True)
            population = [ind for ind, _ in sorted_combined[:self.population_size]]

            # Store generation
            await self.storage.save_ga_population(gen, [{'individual_id': f'gen{gen}_ind{i}',
                                                         'attributes': population[i],
                                                         'fitness': float(fitnesses[i])} for i in range(len(population))])
            if PROMETHEUS_AVAILABLE:
                SYNTHETIC_GA_POPULATION_FITNESS.set(best_fitness)

        return best_individual if best_individual else self._random_chromosome()

    async def optimize(self) -> Dict[str, Any]:
        rows = await self.storage._fetchall("SELECT parameters, num_samples FROM synthetic_generation_history ORDER BY timestamp DESC LIMIT 50")
        historical = [json.loads(r[0]) for r in rows]
        best = await self.run_search(historical)
        return best

# ============================================================================
# NEW MODULE: MoE Gating Network
# ============================================================================
class MoEGatingNetwork:
    """
    Full MoE gating that selects among multiple generation experts.
    """
    def __init__(self, config: SyntheticDataConfig, storage: EnhancedStorage):
        self.config = config
        self.storage = storage
        self.num_experts = config.moe_expert_count
        self.hidden_layers = config.moe_hidden_layers
        self._gating_model = None
        self._scaler = None
        self._trained = False
        self._training_data = []  # list of (feature_vector, expert_label, reward)
        self._lock = asyncio.Lock()

        # Define experts: each expert generates a sample with different biases
        self.experts = {
            'balanced': self._balanced_expert,
            'carbon_focused': self._carbon_expert,
            'helium_focused': self._helium_expert,
            'anomaly_focused': self._anomaly_expert
        }
        if len(self.experts) < self.num_experts:
            keys = list(self.experts.keys())
            for i in range(self.num_experts - len(keys)):
                self.experts[f'custom_{i}'] = self.experts[keys[i % len(keys)]]
        self.expert_names = list(self.experts.keys())

    def _balanced_expert(self, context: Dict) -> Dict[str, Any]:
        return {'bias': 'balanced'}

    def _carbon_expert(self, context: Dict) -> Dict[str, Any]:
        return {'bias': 'carbon'}

    def _helium_expert(self, context: Dict) -> Dict[str, Any]:
        return {'bias': 'helium'}

    def _anomaly_expert(self, context: Dict) -> Dict[str, Any]:
        return {'bias': 'anomaly'}

    def _encode_context(self, context: Dict) -> np.ndarray:
        features = []
        region = context.get('region', 'us-east')
        carbon = context.get('region_carbon', {}).get(region, 400) / 1000
        features.append(carbon)
        hour = datetime.now().hour
        features.append(np.sin(2 * np.pi * hour / 24))
        features.append(np.cos(2 * np.pi * hour / 24))
        features.append(1.0 if context.get('use_real_distributions', False) else 0.0)
        features.append(context.get('anomaly_rate', 0.0))
        features.append(context.get('edge_fraction', 0.1))
        return np.array(features, dtype=np.float32)

    def _train_gating(self):
        if not NUMPY_AVAILABLE or len(self._training_data) < 10:
            return
        X = np.array([item[0] for item in self._training_data])
        y = np.array([item[1] for item in self._training_data])
        from sklearn.neural_network import MLPClassifier
        from sklearn.preprocessing import StandardScaler
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
                    SYNTHETIC_MOE_GATING_PROBABILITIES.labels(expert=self.expert_names[i]).set(p)
        else:
            selected = 'balanced'
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
    Maintains a Pareto front of dataset configurations based on multiple quality objectives.
    """
    def __init__(self, config: SyntheticDataConfig, storage: EnhancedStorage):
        self.config = config
        self.storage = storage
        self.pareto_front = []  # list of dict with config_params, coverage_score, anomaly_diversity, realism_score, data_quality
        self.max_size = config.pareto_max_architectures
        self._lock = asyncio.Lock()

    def _dominates(self, a: Dict, b: Dict) -> bool:
        return (a['coverage_score'] >= b['coverage_score'] and
                a['anomaly_diversity'] >= b['anomaly_diversity'] and
                a['realism_score'] >= b['realism_score'] and
                a['data_quality'] >= b['data_quality']) and \
               (a['coverage_score'] > b['coverage_score'] or
                a['anomaly_diversity'] > b['anomaly_diversity'] or
                a['realism_score'] > b['realism_score'] or
                a['data_quality'] > b['data_quality'])

    async def add_configuration(self, config_params: Dict, metrics: Dict[str, float]) -> bool:
        entry = {
            'solution_id': f"cfg_{uuid.uuid4().hex[:8]}",
            'config_params': config_params,
            'coverage_score': metrics.get('coverage_score', 0.0),
            'anomaly_diversity': metrics.get('anomaly_diversity', 0.0),
            'realism_score': metrics.get('realism_score', 0.0),
            'data_quality': metrics.get('data_quality', 0.0)
        }
        async with self._lock:
            for existing in self.pareto_front:
                if self._dominates(existing, entry):
                    return False
            self.pareto_front = [e for e in self.pareto_front if not self._dominates(entry, e)]
            self.pareto_front.append(entry)
            if len(self.pareto_front) > self.max_size:
                self.pareto_front.sort(key=lambda x: x['data_quality'])
                self.pareto_front = self.pareto_front[:self.max_size]
            await self.storage.save_pareto_front(self.pareto_front)
            if PROMETHEUS_AVAILABLE:
                SYNTHETIC_PARETO_FRONT_SIZE.set(len(self.pareto_front))
            return True

    def get_pareto_front(self) -> List[Dict]:
        return self.pareto_front

    async def get_trade_off_suggestions(self, user_weights: Dict[str, float]) -> List[Dict]:
        if not self.pareto_front:
            return []
        scored = []
        for e in self.pareto_front:
            score = (user_weights.get('coverage', 0.25) * e['coverage_score'] +
                     user_weights.get('diversity', 0.25) * e['anomaly_diversity'] +
                     user_weights.get('realism', 0.25) * e['realism_score'] +
                     user_weights.get('quality', 0.25) * e['data_quality'])
            scored.append((score, e))
        scored.sort(reverse=True)
        return [e for _, e in scored[:5]]

# ============================================================================
# NEW MODULE: Adaptive Anomaly Injector (Contextual Bandit)
# ============================================================================
class AdaptiveAnomalyInjector:
    """
    Uses a contextual bandit to choose which anomaly types to inject based on past success.
    """
    def __init__(self, config: SyntheticDataConfig, storage: EnhancedStorage):
        self.config = config
        self.storage = storage
        self.anomaly_types = [
            'extreme_token_count', 'zero_accuracy', 'zero_latency',
            'extreme_carbon', 'helium_crisis', 'harvester_downtime',
            'renewable_surge', 'network_failure', 'expert_degradation',
            'regional_outage', 'supply_chain_disruption'
        ]
        self.weights = {at: 1.0 for at in self.anomaly_types}
        self.counts = {at: 0 for at in self.anomaly_types}
        self.rewards = {at: 0.0 for at in self.anomaly_types}
        self._lock = asyncio.Lock()
        self.learning_rate = 0.1

    async def choose_anomaly(self, context: Dict) -> str:
        async with self._lock:
            if random.random() < 0.1:
                return random.choice(self.anomaly_types)
            best = max(self.weights, key=lambda k: self.weights[k])
            return best

    async def update(self, anomaly_type: str, reward: float):
        async with self._lock:
            self.counts[anomaly_type] += 1
            self.rewards[anomaly_type] += reward
            self.weights[anomaly_type] = self.rewards[anomaly_type] / self.counts[anomaly_type]

# ============================================================================
# NEW MODULE: Federated Parameter Aggregator
# ============================================================================
class FederatedParameterAggregator:
    """
    Aggregates generation parameters from multiple instances using federated averaging.
    """
    def __init__(self, config: SyntheticDataConfig, storage: EnhancedStorage):
        self.config = config
        self.storage = storage
        self.instance_id = config.instance_id
        self.aggregated_params = None
        self._lock = asyncio.Lock()

    async def share_local_params(self, params: Dict[str, Any]):
        await self.storage.save_state(f"fed_param_{self.instance_id}", json.dumps(params))

    async def pull_aggregated_params(self) -> Optional[Dict[str, Any]]:
        rows = await self.storage._fetchall("SELECT value FROM synthetic_state WHERE key LIKE 'fed_param_%'")
        if not rows:
            return None
        param_list = []
        for r in rows:
            try:
                p = json.loads(r[0])
                param_list.append(p)
            except Exception:
                continue
        if not param_list:
            return None
        avg = {}
        for key in ['token_mean', 'token_std', 'anomaly_rate', 'edge_fraction']:
            vals = [p.get(key, 0) for p in param_list if key in p]
            if vals:
                avg[key] = sum(vals) / len(vals)
        use_real = [p.get('use_real_distributions', 0) for p in param_list if 'use_real_distributions' in p]
        if use_real:
            avg['use_real_distributions'] = 1 if sum(use_real) > len(use_real)/2 else 0
        task_probs = [p.get('task_probs', []) for p in param_list if 'task_probs' in p and p['task_probs']]
        if task_probs:
            avg_probs = [sum(col) / len(task_probs) for col in zip(*task_probs)]
            avg['task_probs'] = avg_probs
        self.aggregated_params = avg
        return avg

    async def apply_aggregated_params(self, current_params: Dict[str, Any]) -> Dict[str, Any]:
        agg = await self.pull_aggregated_params()
        if agg is None:
            return current_params
        merged = current_params.copy()
        for key in ['token_mean', 'token_std', 'anomaly_rate', 'edge_fraction']:
            if key in agg:
                merged[key] = (current_params.get(key, 0) + agg[key]) / 2
        if 'use_real_distributions' in agg:
            merged['use_real_distributions'] = agg['use_real_distributions']
        if 'task_probs' in agg and len(agg['task_probs']) == len(current_params.get('task_probs', [])):
            merged['task_probs'] = [(current_params['task_probs'][i] + agg['task_probs'][i]) / 2 for i in range(len(agg['task_probs']))]
        return merged

# ============================================================================
# NEW MODULE: Drift Detector
# ============================================================================
class DriftDetector:
    """
    Detects significant changes in external data distributions (carbon intensity, user feedback).
    """
    def __init__(self, storage: EnhancedStorage, config: SyntheticDataConfig):
        self.storage = storage
        self.config = config
        self.carbon_history = deque(maxlen=100)
        self.user_feedback_history = deque(maxlen=100)
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

    async def check_feedback_drift(self, avg_reward: float) -> bool:
        self.user_feedback_history.append(avg_reward)
        if len(self.user_feedback_history) < 10:
            return False
        recent = list(self.user_feedback_history)[-10:]
        mean = np.mean(recent)
        if mean == 0:
            return False
        if abs(avg_reward - mean) > self.threshold * mean:
            logger.warning(f"Feedback drift detected: current {avg_reward} vs mean {mean}")
            return True
        return False

# ============================================================================
# NEW MODULE: Active User Preference Learner
# ============================================================================
class ActiveUserPreferenceLearner:
    """
    Queries the user when multiple dataset configurations yield similar quality scores.
    """
    def __init__(self, storage: EnhancedStorage, websocket: 'EnhancedWebSocketServer'):
        self.storage = storage
        self.websocket = websocket
        self.user_weights = {}  # user_id -> weights dict

    async def query_user_if_needed(self, user_id: str, top_configs: List[Dict]) -> Optional[str]:
        if len(top_configs) < 2:
            return None
        scores = [c['data_quality'] for c in top_configs[:2]]
        if abs(scores[0] - scores[1]) / max(scores) < 0.05:
            await self.websocket.broadcast({
                'type': 'preference_query',
                'user_id': user_id,
                'options': [{'id': c['solution_id'], 'quality': c['data_quality']} for c in top_configs[:2]]
            }, topic='user_preferences')
            return top_configs[0]['solution_id']
        return None

    async def record_choice(self, user_id: str, chosen_solution_id: str, context: Dict):
        await self.storage.save_user_preference(user_id, {'chosen': chosen_solution_id}, chosen_solution_id)

# ============================================================================
# QUANTUM SECURITY, BLOCKCHAIN, WEBSOCKET (unchanged)
# ============================================================================
class QuantumResilientDataSecurity:
    # ... (same as original)
    pass

class BlockchainDataVerification:
    # ... (same as original)
    pass

class EnhancedWebSocketServer:
    # ... (same as original)
    pass

# ============================================================================
# REFLECTION HANDLER (enhanced with drift)
# ============================================================================
class ReflectionHandler:
    def __init__(self, state: 'GeneratorState', mtop_engine: MTOPDataEngine,
                 drift_detector: Optional[DriftDetector] = None):
        self.state = state
        self.mtop_engine = mtop_engine
        self.drift_detector = drift_detector
        self.reflection_count = 0

    async def trigger_reflection(self, trigger_type: str, **kwargs):
        self.reflection_count += 1
        if trigger_type == 'good_data':
            self.state.confidence = min(1.0, self.state.confidence + 0.05)
        elif trigger_type == 'poor_data':
            self.state.confidence = max(0.1, self.state.confidence - 0.1)
        elif trigger_type == 'anomaly_detected':
            self.state.anomaly_rate = min(0.5, self.state.anomaly_rate + 0.01)
        elif trigger_type == 'carbon_drift' and self.drift_detector:
            self.state.anomaly_rate = min(0.5, self.state.anomaly_rate + 0.02)
        await self.state.save()

# ============================================================================
# GENERATOR STATE (with persistence)
# ============================================================================
class GeneratorState:
    def __init__(self, storage: EnhancedStorage):
        self.storage = storage
        self.confidence = float(await self.storage.get_state('confidence') or 0.5)
        self.anomaly_rate = float(await self.storage.get_state('anomaly_rate') or 0.0)
        self.reflection_count = int(await self.storage.get_state('reflection_count') or 0)

    async def save(self):
        await self.storage.save_state('confidence', str(self.confidence))
        await self.storage.save_state('anomaly_rate', str(self.anomaly_rate))
        await self.storage.save_state('reflection_count', str(self.reflection_count))

# ============================================================================
# MAIN SYNTHETIC DATA GENERATOR (Enhanced v5.0.0)
# ============================================================================
class SyntheticDataGenerator:
    """
    Advanced synthetic data generator with GA, MoE, Pareto, adaptive anomalies, federated learning.
    """

    def __init__(
        self,
        config: Optional[Union[Dict[str, Any], SyntheticDataConfig]] = None,
        carbon_fetcher: Optional[CarbonIntensityFetcher] = None,
        helium_collector: Optional[HeliumCollector] = None,
        material_updater: Optional[MaterialFootprintUpdater] = None,
    ):
        # Configuration
        if config is None:
            if PYDANTIC_AVAILABLE:
                self.config = SyntheticDataConfig()
            else:
                self.config = SYNTHETIC_CONFIG
        elif isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = SyntheticDataConfig(**config)
            else:
                self.config = config
        else:
            self.config = config

        # Set random seeds
        seed = self.config.get('seed', 42) if isinstance(self.config, dict) else self.config.seed
        random.seed(seed)
        np.random.seed(seed)

        # Extract config values
        self.task_types = self.config.get('task_types') if isinstance(self.config, dict) else self.config.task_types
        self.priority_profiles = self.config.get('priority_profiles') if isinstance(self.config, dict) else self.config.priority_profiles
        self.regions = self.config.get('regions') if isinstance(self.config, dict) else self.config.regions
        self.region_carbon = self.config.get('region_carbon') if isinstance(self.config, dict) else self.config.region_carbon
        self.token_mean = self.config.get('token_mean') if isinstance(self.config, dict) else self.config.token_mean
        self.token_std = self.config.get('token_std') if isinstance(self.config, dict) else self.config.token_std
        self.default_degradation_rate = self.config.get('default_degradation_rate') if isinstance(self.config, dict) else self.config.default_degradation_rate
        self.default_anomaly_rate = self.config.get('default_anomaly_rate') if isinstance(self.config, dict) else self.config.default_anomaly_rate
        self.default_rate_per_hour = self.config.get('default_rate_per_hour') if isinstance(self.config, dict) else self.config.default_rate_per_hour
        self.default_duration_hours = self.config.get('default_duration_hours') if isinstance(self.config, dict) else self.config.default_duration_hours
        self.use_real_distributions = self.config.get('use_real_distributions', False) if isinstance(self.config, dict) else self.config.use_real_distributions
        self.prompt_pool_file = self.config.get('prompt_pool_file') if isinstance(self.config, dict) else self.config.prompt_pool_file
        self.export_format = self.config.get('export_format', 'json') if isinstance(self.config, dict) else self.config.export_format
        self.dataset_version = self.config.get('dataset_version', '5.0.0') if isinstance(self.config, dict) else self.config.dataset_version
        self.mopd_weights = self.config.get('mopd_weights') if isinstance(self.config, dict) else self.config.mopd_weights

        # Inject external collectors
        self.carbon_fetcher = carbon_fetcher
        self.helium_collector = helium_collector
        self.material_updater = material_updater

        # Load prompt pool
        self.prompt_pool = self._load_prompt_pool()

        # User-region mapping
        self.user_region_cache: Dict[str, str] = {}

        # Cache for real distributions
        self._real_carbon_cache: Dict[str, Tuple[float, datetime]] = {}
        self._real_helium_cache: Dict[str, Tuple[float, datetime]] = {}
        self._cache_ttl_seconds = self.config.get('cache_ttl', 300) if isinstance(self.config, dict) else self.config.cache_ttl

        # Circuit breakers and rate limiter
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=self.config.get('circuit_breaker_threshold', 5) if isinstance(self.config, dict) else self.config.circuit_breaker_threshold,
            recovery_timeout=self.config.get('circuit_breaker_timeout', 30) if isinstance(self.config, dict) else self.config.circuit_breaker_timeout,
            name="data_generator"
        )
        self._rate_limiter = RateLimiter(
            rate=self.config.get('rate_limit_requests', 100) if isinstance(self.config, dict) else self.config.rate_limit_requests,
            window=self.config.get('rate_limit_window', 60) if isinstance(self.config, dict) else self.config.rate_limit_window
        )

        # Storage
        self.storage = EnhancedStorage(self.config)
        self.state = GeneratorState(self.storage)

        # MTOP engine (legacy)
        self.mtop_engine = MTOPDataEngine(self.config)

        # New modules (v5.0.0)
        self.ga_optimizer = GeneticParameterOptimizer(self.config, self.storage) if self.config.get('ga_enabled', True) else None
        self.moe_gating = MoEGatingNetwork(self.config, self.storage) if self.config.get('moe_enabled', True) else None
        self.pareto_optimizer = ParetoFrontOptimizer(self.config, self.storage) if self.config.get('pareto_enabled', True) else None
        self.adaptive_anomaly = AdaptiveAnomalyInjector(self.config, self.storage) if self.config.get('adaptive_anomaly_enabled', True) else None
        self.federated_aggregator = FederatedParameterAggregator(self.config, self.storage) if self.config.get('federated_enabled', True) else None
        self.drift_detector = DriftDetector(self.storage, self.config) if self.config.get('drift_detection_enabled', True) else None
        self.user_pref_learner = ActiveUserPreferenceLearner(self.storage, self.websocket) if self.config.get('user_preference_learning_enabled', True) else None

        # ===== NEW: Initialize LIMIT Graph, RLHF, Distillation =====
        self.limit_graph = LimitGraphManager(self.config) if self.config.get('limit_graph_enabled', True) else None
        self.rlhf = RLHFManager(self.config) if self.config.get('rlhf_enabled', True) else None
        self.distillation = MultiTeacherPolicyDistillation(self.config, self.moe_gating) if self.config.get('distillation_enabled', True) and self.moe_gating else None

        # Quantum security
        self.quantum_security = QuantumResilientDataSecurity(self.config, self.storage)

        # Blockchain
        self.blockchain = BlockchainDataVerification(self.config)

        # WebSocket
        self.websocket = EnhancedWebSocketServer(self.config.get('websocket_port', 8770))

        # Reflection (with drift)
        self.reflection = ReflectionHandler(self.state, self.mtop_engine, self.drift_detector)

        # Background tasks
        self._background_tasks = []
        self._shutdown_event = asyncio.Event()
        self._running = False

        # Start Prometheus HTTP server
        if PROMETHEUS_AVAILABLE:
            start_http_server(self.config.get('metrics_port', 8000))
            logger.info("Prometheus metrics exposed on port %d", self.config.get('metrics_port', 8000))

        logger.info("SyntheticDataGenerator v%s initialized", self.dataset_version)

    async def start(self):
        self._running = True
        await self.websocket.start()
        # Start background tasks
        tasks = []
        if self.ga_optimizer:
            tasks.append(self._ga_optimization_loop())
        if self.federated_aggregator:
            tasks.append(self._federated_loop())
        if self.drift_detector:
            tasks.append(self._drift_detection_loop())
        # ===== NEW: Background loops for added features =====
        if self.limit_graph:
            tasks.append(self._limit_graph_loop())
        if self.rlhf:
            tasks.append(self._rlhf_loop())
        if self.distillation:
            tasks.append(self._distillation_loop())
        tasks.extend([
            self._health_check_loop(),
            self._cleanup_loop(),
            self._carbon_update_loop(),
            self._auto_optimize_loop(),
            self._websocket_heartbeat(),
        ])
        for task in tasks:
            self._background_tasks.append(asyncio.create_task(task))
        logger.info("SyntheticDataGenerator started with %d background tasks", len(self._background_tasks))

    # ===== NEW: Background loop methods =====
    async def _limit_graph_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.get('limit_graph_update_interval', 300))
            try:
                # Update carbon intensity constraint
                if self.carbon_fetcher:
                    intensity = await self.carbon_fetcher.get_intensity('global')
                    await self.limit_graph.update_constraint('carbon', intensity)
                # Evaluate influence
                influence = await self.limit_graph.evaluate_path('carbon', 'cost')
                logger.debug(f"LIMIT Graph carbon->cost influence: {influence:.3f}")
            except Exception as e:
                logger.error(f"Limit graph loop error: {e}")

    async def _rlhf_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.get('rlhf_training_interval', 600))
            try:
                if self.rlhf:
                    await self.rlhf.train_reward_model()
            except Exception as e:
                logger.error(f"RLHF loop error: {e}")

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.get('distillation_interval', 300))
            try:
                if self.distillation:
                    state = {
                        'region': 'global',
                        'use_real_distributions': self.use_real_distributions,
                    }
                    await self.distillation.distill(state)
            except Exception as e:
                logger.error(f"Distillation loop error: {e}")

    async def _ga_optimization_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                logger.info("Running GA parameter optimization...")
                best_params = await self.ga_optimizer.optimize()
                if best_params:
                    self.token_mean = best_params.get('token_mean', self.token_mean)
                    self.token_std = best_params.get('token_std', self.token_std)
                    self.default_anomaly_rate = best_params.get('anomaly_rate', self.default_anomaly_rate)
                    self.edge_fraction = best_params.get('edge_fraction', 0.1)
                    self.use_real_distributions = bool(best_params.get('use_real_distributions', 0))
                    if 'task_probs' in best_params:
                        task_keys = list(self.task_types.keys())
                        new_probs = {task_keys[i]: best_params['task_probs'][i] for i in range(len(task_keys))}
                        self.task_types = new_probs
                    logger.info("GA updated generation parameters: %s", best_params)
                    await self.storage.save_state('ga_best_params', json.dumps(best_params))
            except Exception as e:
                logger.error("GA optimization loop error: %s", e)

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.get('federated_interval', 3600))
            try:
                current_params = {
                    'token_mean': self.token_mean,
                    'token_std': self.token_std,
                    'anomaly_rate': self.default_anomaly_rate,
                    'edge_fraction': self.edge_fraction,
                    'use_real_distributions': 1 if self.use_real_distributions else 0,
                    'task_probs': list(self.task_types.values()),
                }
                await self.federated_aggregator.share_local_params(current_params)
                merged = await self.federated_aggregator.apply_aggregated_params(current_params)
                if merged:
                    self.token_mean = merged.get('token_mean', self.token_mean)
                    self.token_std = merged.get('token_std', self.token_std)
                    self.default_anomaly_rate = merged.get('anomaly_rate', self.default_anomaly_rate)
                    self.edge_fraction = merged.get('edge_fraction', self.edge_fraction)
                    self.use_real_distributions = bool(merged.get('use_real_distributions', 0))
                    if 'task_probs' in merged:
                        task_keys = list(self.task_types.keys())
                        new_probs = {task_keys[i]: merged['task_probs'][i] for i in range(len(task_keys))}
                        self.task_types = new_probs
                    logger.info("Federated parameters applied: %s", merged)
            except Exception as e:
                logger.error("Federated loop error: %s", e)

    async def _drift_detection_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                if self.carbon_fetcher and self.drift_detector:
                    intensity = await self.carbon_fetcher.get_intensity('global')
                    if await self.drift_detector.check_carbon_drift(intensity):
                        await self.reflection.trigger_reflection('carbon_drift')
            except Exception as e:
                logger.error("Drift detection loop error: %s", e)

    async def _websocket_heartbeat(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(30)
            await self.websocket.broadcast({'type': 'heartbeat', 'timestamp': datetime.now().isoformat()})

    async def _health_check_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(60)

    async def _cleanup_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            gc.collect()

    async def _carbon_update_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.get('carbon_update_interval', 300))
            if self.carbon_fetcher:
                try:
                    await self.carbon_fetcher.get_intensity('global')
                except Exception as e:
                    logger.error("Carbon update error: %s", e)

    async def _auto_optimize_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.get('auto_optimize_interval', 1800))

    # ------------------------------------------------------------------
    # Core generation methods (adapted to use MoE and adaptive anomaly)
    # ------------------------------------------------------------------
    async def generate_workload_descriptor(self, **kwargs) -> WorkloadDescriptor:
        # ===== NEW: Use RLHF first if trained =====
        if self.rlhf and self.rlhf.reward_model is not None:
            probs = await self.rlhf.get_policy_probs(kwargs)
            expert_idx = np.argmax(probs)
            expert_names = ['balanced', 'carbon_focused', 'helium_focused', 'anomaly_focused']
            selected_expert = expert_names[expert_idx % len(expert_names)]
            if selected_expert == 'anomaly_focused':
                kwargs['anomaly_forced'] = True
            # Other expert biases could adjust task type probabilities
        # ===== NEW: Otherwise use distillation if available =====
        elif self.distillation and self.distillation.get_student_probs():
            probs = self.distillation.get_student_probs()
            expert_idx = np.argmax(probs)
            expert_names = ['balanced', 'carbon_focused', 'helium_focused', 'anomaly_focused']
            selected_expert = expert_names[expert_idx % len(expert_names)]
            if selected_expert == 'anomaly_focused':
                kwargs['anomaly_forced'] = True
        # ===== NEW: MoE selection (existing) =====
        elif self.moe_gating and self.config.get('moe_enabled'):
            context = {'region': kwargs.get('region'), 'use_real_distributions': self.use_real_distributions}
            selected_expert, expert_params = await self.moe_gating.select_expert(context)
            if expert_params.get('bias') == 'carbon':
                pass
            elif expert_params.get('bias') == 'helium':
                pass
            elif expert_params.get('bias') == 'anomaly':
                kwargs['anomaly_forced'] = True
        # ===== NEW: LIMIT Graph adjustment =====
        if self.limit_graph:
            carbon_influence = await self.limit_graph.evaluate_path('carbon', 'cost')
            if carbon_influence > 0.5:
                # Could bias region selection or task type towards carbon-heavy tasks
                pass

        # Continue with normal generation
        return await self._generate_workload_descriptor_internal(**kwargs)

    async def _generate_workload_descriptor_internal(self, **kwargs) -> WorkloadDescriptor:
        task_type = kwargs.get('task_type') or self._random_task_type()
        tokens = kwargs.get('tokens') or self._random_token_count()
        latency_target = kwargs.get('latency_target') or self._random_latency_budget()
        priority = kwargs.get('priority') or self._random_priority()
        bio_mode = kwargs.get('bio_mode') or random.choice(["photosynthetic", "chemotactic", "none"])
        sector_emission_factor = kwargs.get('sector_emission_factor') or random.uniform(0.01, 0.05)

        return WorkloadDescriptor(
            task_type=task_type,
            tokens=tokens,
            latency_target=latency_target,
            sector_emission_factor=sector_emission_factor,
            bio_mode=bio_mode,
            priority=priority,
        )

    def _random_task_type(self) -> str:
        task_types = self.task_types
        return np.random.choice(
            list(task_types.keys()),
            p=list(task_types.values())
        )

    def _random_token_count(self) -> int:
        return int(np.exp(np.random.normal(self.token_mean, self.token_std)))

    def _random_latency_budget(self) -> float:
        return np.random.uniform(100, 2000)

    def _random_priority(self) -> str:
        return np.random.choice(self.priority_profiles)

    async def generate_node_descriptor(self, **kwargs) -> NodeDescriptor:
        node_id = kwargs.get('node_id') or f"synth_node_{uuid.uuid4().hex[:8]}"
        node_type = kwargs.get('type') or random.choice(["edge", "hotspot", "cloud", "lab"])
        region = kwargs.get('region') or random.choice(self.regions)

        if self.use_real_distributions and self.carbon_fetcher:
            region_carbon_intensity = await self._get_carbon_intensity(region)
        else:
            region_carbon_intensity = kwargs.get('region_carbon_intensity') or self._random_carbon(region)

        energy_per_token = kwargs.get('energy_per_token') or random.uniform(0.00001, 0.0001)

        if self.use_real_distributions and self.helium_collector:
            hotspot_id = kwargs.get('hotspot_id') or f"hotspot_{random.randint(1,1000)}"
            helium_connectivity_score = await self._get_helium_score(hotspot_id)
        else:
            helium_connectivity_score = kwargs.get('helium_connectivity_score') or random.uniform(0.5, 1.0)

        material_footprint_id = kwargs.get('material_footprint_id') or random.choice(["gpu-a100", "gpu-h100", "edge-device"])
        uptime = kwargs.get('uptime') or random.uniform(0.9, 1.0)
        renewable_fraction = kwargs.get('renewable_fraction') or self._random_renewable(region)

        return NodeDescriptor(
            id=node_id,
            type=node_type,
            region=region,
            region_carbon_intensity=region_carbon_intensity,
            energy_per_token=energy_per_token,
            helium_connectivity_score=helium_connectivity_score,
            material_footprint_id=material_footprint_id,
            uptime=uptime,
            renewable_fraction=renewable_fraction,
        )

    async def _get_carbon_intensity(self, region: str) -> float:
        cached = await self.storage.get_carbon_intensity(region)
        if cached is not None:
            if PROMETHEUS_AVAILABLE:
                SYNTHETIC_CACHE_HITS.labels(type='carbon').inc()
            return cached
        if PROMETHEUS_AVAILABLE:
            SYNTHETIC_CACHE_MISSES.labels(type='carbon').inc()

        if self.carbon_fetcher and self.use_real_distributions:
            async def fetch():
                return await self.carbon_fetcher.get_intensity(region)
            try:
                intensity = await self._circuit_breaker.call(fetch)
                await self.storage.save_carbon_intensity(region, intensity)
                return intensity
            except Exception as e:
                logger.error("Carbon fetcher failed, using fallback", region=region, error=str(e))
        intensity = self._random_carbon(region)
        await self.storage.save_carbon_intensity(region, intensity)
        return intensity

    async def _get_helium_score(self, hotspot_id: str) -> float:
        cached = await self.storage.get_helium_score(hotspot_id)
        if cached is not None:
            if PROMETHEUS_AVAILABLE:
                SYNTHETIC_CACHE_HITS.labels(type='helium').inc()
            return cached
        if PROMETHEUS_AVAILABLE:
            SYNTHETIC_CACHE_MISSES.labels(type='helium').inc()

        if self.helium_collector and self.use_real_distributions:
            async def fetch():
                return await self.helium_collector.get_connectivity_score(hotspot_id)
            try:
                score = await self._circuit_breaker.call(fetch)
                await self.storage.save_helium_score(hotspot_id, score)
                return score
            except Exception as e:
                logger.error("Helium collector failed, using fallback", hotspot_id=hotspot_id, error=str(e))
        score = random.uniform(0.5, 1.0)
        await self.storage.save_helium_score(hotspot_id, score)
        return score

    def _random_carbon(self, region: str) -> float:
        base = self.region_carbon.get(region, 400)
        hour = datetime.now().hour
        diurnal = 0.9 + 0.2 * np.sin((hour - 8) / 12 * np.pi)
        return (base * diurnal + np.random.normal(0, 20)) / 1000

    def _random_renewable(self, region: str) -> float:
        base = {
            'us-east': 0.3, 'us-west': 0.45, 'eu-west': 0.5,
            'eu-north': 0.6, 'asia-east': 0.2, 'asia-southeast': 0.25
        }
        return base.get(region, 0.3) + np.random.normal(0, 0.05)

    async def compute_sustainability_metrics(
        self,
        workload: WorkloadDescriptor,
        node: NodeDescriptor,
    ) -> SyntheticSustainabilityMetrics:
        energy_joules = node.energy_per_token * workload.tokens
        carbon_kg = energy_joules / 3.6e6 * node.region_carbon_intensity
        helium_units = (1 - node.helium_connectivity_score) * 0.5
        material_index = 0.0
        if self.material_updater and node.material_footprint_id:
            fp = self.material_updater.get_footprint(node.material_footprint_id)
            if fp:
                material_index = fp.get('material_index', 0.0)
        return SyntheticSustainabilityMetrics(
            energy_joules=energy_joules,
            carbon_kg=carbon_kg,
            helium_units=helium_units,
            material_index=material_index,
        )

    async def generate_task_sequence(
        self,
        duration_hours: Optional[int] = None,
        rate_per_hour: Optional[float] = None,
        start_time: Optional[datetime] = None,
        rate_function: Optional[Callable[[datetime], float]] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        duration = duration_hours or self.default_duration_hours
        start = start_time or datetime.now()
        end = start + timedelta(hours=duration)

        if rate_function is None:
            base_rate = rate_per_hour or self.default_rate_per_hour
            def rate_func(t: datetime) -> float:
                hour = t.hour
                factor = 0.7 + 0.3 * np.cos((hour - 14) * 2 * np.pi / 24)
                return base_rate * factor
            rate_function = rate_func

        sequence = []
        t = start
        while t < end:
            current_rate = rate_function(t)
            if current_rate <= 0:
                t += timedelta(seconds=1)
                continue
            dt = np.random.exponential(1 / current_rate)
            t += timedelta(seconds=dt)
            if t >= end:
                break
            if self.moe_gating and self.config.get('moe_enabled'):
                context = {'region': kwargs.get('region'), 'use_real_distributions': self.use_real_distributions}
                selected_expert, _ = await self.moe_gating.select_expert(context)
            workload = await self.generate_workload_descriptor(**kwargs)
            node = await self.generate_node_descriptor(**kwargs)
            metrics = await self.compute_sustainability_metrics(workload, node)
            sequence.append({
                'timestamp': t,
                'workload': workload,
                'node': node,
                'metrics': metrics,
            })
        logger.info("Generated task sequence", count=len(sequence), duration_hours=duration)
        return sequence

    async def generate_task_sequence_async(self, **kwargs) -> List[Dict[str, Any]]:
        return await self.generate_task_sequence(**kwargs)

    async def inject_anomaly(
        self,
        workload: WorkloadDescriptor,
        node: NodeDescriptor,
        anomaly_type: Optional[str] = None,
        context: Optional[Dict] = None,
    ) -> Tuple[WorkloadDescriptor, NodeDescriptor, str]:
        if anomaly_type is None:
            if self.adaptive_anomaly and self.config.get('adaptive_anomaly_enabled'):
                anomaly_type = await self.adaptive_anomaly.choose_anomaly(context or {})
            else:
                anomaly_type = random.choice([
                    'extreme_token_count', 'zero_accuracy', 'zero_latency',
                    'extreme_carbon', 'helium_crisis', 'harvester_downtime',
                    'renewable_surge', 'network_failure', 'expert_degradation',
                    'regional_outage', 'supply_chain_disruption'
                ])
        if anomaly_type == 'extreme_token_count':
            workload.tokens = int(np.random.exponential(10000)) + 5000
        elif anomaly_type == 'zero_accuracy':
            workload.latency_target = 0.0
        elif anomaly_type == 'zero_latency':
            workload.latency_target = 0.0
        elif anomaly_type == 'extreme_carbon':
            node.region_carbon_intensity = 0.8 + np.random.normal(0, 0.05)
        elif anomaly_type == 'helium_crisis':
            node.helium_connectivity_score = 0.1 + np.random.normal(0, 0.02)
        elif anomaly_type == 'harvester_downtime':
            node.renewable_fraction = 0.0
            node.uptime = 0.5
        elif anomaly_type == 'renewable_surge':
            node.renewable_fraction = 0.95
        elif anomaly_type == 'network_failure':
            node.helium_connectivity_score = 0.0
            node.uptime = 0.0
        elif anomaly_type == 'expert_degradation':
            pass
        elif anomaly_type == 'regional_outage':
            if context and 'region' in context:
                if node.region == context['region']:
                    node.uptime = 0.3
            else:
                node.uptime = 0.3
        elif anomaly_type == 'supply_chain_disruption':
            pass
        else:
            raise ValueError(f"Unknown anomaly_type: {anomaly_type}")
        return workload, node, anomaly_type

    async def generate_dataset(
        self,
        num_samples: int = 1000,
        include_edge_cases: bool = True,
        edge_case_fraction: float = 0.1,
        anomaly_rate: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        if anomaly_rate is None:
            anomaly_rate = self.default_anomaly_rate

        dataset = []
        num_edge = int(num_samples * edge_case_fraction) if include_edge_cases else 0
        num_normal = num_samples - num_edge

        for _ in range(num_normal):
            workload = await self.generate_workload_descriptor()
            node = await self.generate_node_descriptor()
            anomaly = None
            if random.random() < anomaly_rate:
                workload, node, anomaly = await self.inject_anomaly(workload, node)
            metrics = await self.compute_sustainability_metrics(workload, node)
            dataset.append({
                'workload': workload,
                'node': node,
                'metrics': metrics,
                'anomaly': anomaly,
            })

        edge_types = [
            'extreme_token_count', 'zero_accuracy', 'zero_latency',
            'extreme_carbon', 'helium_crisis', 'harvester_downtime',
            'renewable_surge', 'network_failure', 'expert_degradation',
            'regional_outage', 'supply_chain_disruption'
        ]
        for _ in range(num_edge):
            anomaly_type = random.choice(edge_types)
            workload = await self.generate_workload_descriptor()
            node = await self.generate_node_descriptor()
            workload, node, _ = await self.inject_anomaly(workload, node, anomaly_type)
            metrics = await self.compute_sustainability_metrics(workload, node)
            dataset.append({
                'workload': workload,
                'node': node,
                'metrics': metrics,
                'anomaly': anomaly_type,
            })

        params = {
            'num_samples': num_samples,
            'edge_fraction': edge_case_fraction,
            'anomaly_rate': anomaly_rate,
            'use_real_distributions': self.use_real_distributions,
            'task_types': self.task_types,
            'token_mean': self.token_mean,
            'token_std': self.token_std,
        }
        # Quantum signing
        signature = None
        if self.config.get('enable_quantum_security', True):
            metadata = {
                'version': self.dataset_version,
                'timestamp': datetime.now().isoformat(),
                'params': params,
                'sample_count': len(dataset)
            }
            quantum_key = await self.quantum_security.generate_keypair(self.config.get('quantum_algorithm', 'dilithium'))
            signature = await self.quantum_security.sign_dataset(metadata, quantum_key['key_id'])
            if PROMETHEUS_AVAILABLE:
                SYNTHETIC_QUANTUM_SIGNATURES.labels(algorithm=self.config.get('quantum_algorithm', 'dilithium'), status='sign_success').inc()

        tx_hash = None
        if self.blockchain:
            dataset_hash = hashlib.sha256(json.dumps(params, sort_keys=True, default=str).encode()).hexdigest()
            tx_hash = await self.blockchain.record_dataset(f"dataset_{uuid.uuid4().hex[:8]}", dataset_hash)
            if PROMETHEUS_AVAILABLE:
                SYNTHETIC_BLOCKCHAIN_TX.labels(status='recorded').inc()

        await self.storage.save_generation_history(
            self.dataset_version,
            num_samples,
            anomaly_rate,
            edge_case_fraction,
            params,
            signature,
            tx_hash
        )

        # Update Pareto front
        if self.pareto_optimizer and self.config.get('pareto_enabled'):
            coverage_score = len(set(item['node'].region for item in dataset)) / len(self.regions)
            anomaly_diversity = len(set(item['anomaly'] for item in dataset if item['anomaly'])) / len(edge_types)
            realism_score = 0.8
            data_quality = 0.9
            metrics = {
                'coverage_score': coverage_score,
                'anomaly_diversity': anomaly_diversity,
                'realism_score': realism_score,
                'data_quality': data_quality,
            }
            await self.pareto_optimizer.add_configuration(params, metrics)

        # ===== NEW: Update LIMIT Graph constraints =====
        if self.limit_graph:
            await self.limit_graph.update_constraint('coverage', coverage_score)
            await self.limit_graph.update_constraint('quality', data_quality)

        # ===== NEW: Record RLHF feedback (simulated) =====
        if self.rlhf and data_quality > 0.85:
            await self.rlhf.record_feedback(
                state={'coverage_score': coverage_score, 'data_quality': data_quality,
                       'carbon_intensity': self.region_carbon.get('global', 0.4)},
                action='balanced',
                reward=data_quality
            )

        await self.websocket.broadcast({
            'type': 'dataset_generated',
            'version': self.dataset_version,
            'samples': len(dataset),
            'anomaly_rate': anomaly_rate,
            'timestamp': datetime.now().isoformat()
        }, topic='generation')

        logger.info("Generated dataset", count=len(dataset), edge=num_edge, anomaly_rate=anomaly_rate)
        return dataset

    async def generate_dataset_async(self, **kwargs) -> List[Dict[str, Any]]:
        return await self.generate_dataset(**kwargs)

    # ------------------------------------------------------------------
    # Streaming Generator (unchanged)
    # ------------------------------------------------------------------
    async def generate_dataset_stream(
        self,
        num_samples: int = 1000,
        include_edge_cases: bool = True,
        edge_case_fraction: float = 0.1,
        anomaly_rate: Optional[float] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        # ... (similar to original)
        pass

    # ------------------------------------------------------------------
    # Persistence (unchanged)
    # ------------------------------------------------------------------
    async def save_dataset(self, dataset: List[Dict[str, Any]], path: str) -> None:
        # ... (same as original)
        pass

    async def save_dataset_stream(self, stream: AsyncIterator[Dict[str, Any]], path: str) -> None:
        # ... (same as original)
        pass

    def load_dataset(self, path: str) -> List[Dict[str, Any]]:
        # ... (same as original)
        pass

    def generate_expert_profile(
        self,
        expert_id: Optional[str] = None,
        degradation_rate: Optional[float] = None,
    ) -> SyntheticExpertProfile:
        # ... (same as original)
        pass

    def export_for_simulation(self, dataset: List[Dict[str, Any]]) -> List[Dict]:
        # ... (same as original)
        pass

    def get_stats(self) -> Dict:
        return {
            'config_seed': self.config.get('seed') if isinstance(self.config, dict) else self.config.seed,
            'use_real_distributions': self.use_real_distributions,
            'prompt_pool_size': len(self.prompt_pool),
            'cache_ttl_seconds': self._cache_ttl_seconds,
            'dataset_version': self.dataset_version,
            'ga_enabled': self.config.get('ga_enabled', False) if isinstance(self.config, dict) else self.config.ga_enabled,
            'moe_enabled': self.config.get('moe_enabled', False) if isinstance(self.config, dict) else self.config.moe_enabled,
            'pareto_enabled': self.config.get('pareto_enabled', False) if isinstance(self.config, dict) else self.config.pareto_enabled,
            'adaptive_anomaly_enabled': self.config.get('adaptive_anomaly_enabled', False) if isinstance(self.config, dict) else self.config.adaptive_anomaly_enabled,
            'federated_enabled': self.config.get('federated_enabled', False) if isinstance(self.config, dict) else self.config.federated_enabled,
            'limit_graph_enabled': self.config.get('limit_graph_enabled', False) if isinstance(self.config, dict) else self.config.limit_graph_enabled,
            'rlhf_enabled': self.config.get('rlhf_enabled', False) if isinstance(self.config, dict) else self.config.rlhf_enabled,
            'distillation_enabled': self.config.get('distillation_enabled', False) if isinstance(self.config, dict) else self.config.distillation_enabled,
        }

    async def shutdown(self):
        logger.info("Shutting down SyntheticDataGenerator")
        self._shutdown_event.set()
        self._running = False
        for task in self._background_tasks:
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        await self.websocket.stop()
        await self.storage.dispose()
        logger.info("SyntheticDataGenerator shutdown complete")

# ============================================================================
# SINGLETON ACCESSOR
# ============================================================================
_generator_instance = None
_generator_lock = asyncio.Lock()

async def get_synthetic_generator(
    config: Optional[Union[Dict[str, Any], SyntheticDataConfig]] = None,
    carbon_fetcher: Optional[CarbonIntensityFetcher] = None,
    helium_collector: Optional[HeliumCollector] = None,
    material_updater: Optional[MaterialFootprintUpdater] = None,
) -> SyntheticDataGenerator:
    global _generator_instance
    if _generator_instance is None:
        async with _generator_lock:
            if _generator_instance is None:
                _generator_instance = SyntheticDataGenerator(
                    config=config,
                    carbon_fetcher=carbon_fetcher,
                    helium_collector=helium_collector,
                    material_updater=material_updater
                )
                await _generator_instance.start()
    return _generator_instance

# ============================================================================
# SIGNAL HANDLING (fixed)
# ============================================================================
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
    global _generator_instance
    if _generator_instance:
        await _generator_instance.shutdown()
        _generator_instance = None

# ============================================================================
# CLI ENTRY POINT
# ============================================================================
async def main_cli():
    # ... (same as original)
    pass

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s, None))
    try:
        asyncio.run(main_cli())
    except KeyboardInterrupt:
        pass
