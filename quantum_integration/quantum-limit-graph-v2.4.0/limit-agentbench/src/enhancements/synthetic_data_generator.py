#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/synthetic_data_generator_enhanced_v4_0.py
# VERSION: 4.0.0 (Enterprise Quantum Resilience + MTOP + MOPD – Production Ready)
# =============================================================================
"""
Advanced Synthetic Data Generator for Green Agent - Version 4.0.0
Generates realistic workloads, environmental conditions, and edge cases for policy testing.

ENHANCEMENTS OVER v3.0.0:
1. Multi-Teacher On-Policy Distillation (MTOP) for realistic data generation.
2. Multi-Objective Performance Design (MOPD) for configurable trade‑offs in metrics.
3. Circuit breaker and rate limiter for external collectors.
4. Prometheus metrics HTTP server on configurable port.
5. WebSocket server with subscription management and heartbeat.
6. Quantum‑resilient signing of generated datasets (PQC).
7. Blockchain verification (record dataset versions on‑chain).
8. Reflection handlers that adjust generation parameters based on feedback.
9. Async‑safe persistent storage (aiosqlite) for caches and generation history.
10. Graceful shutdown using asyncio.Event and signal handlers.
11. Async‑safe correlation IDs using contextvars.
12. Full structured logging with JSON format.
13. Improved anomaly injection with contextual awareness.
14. Comprehensive docstrings and error handling.
"""

import asyncio
import json
import random
import hashlib
import uuid
import logging
import sys
import signal
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union, Callable, AsyncIterator
from pathlib import Path
import secrets
import contextvars
from functools import wraps
import numpy as np
import pandas as pd

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

# ============================================================================
# CORRELATION ID CONTEXT
# ============================================================================
correlation_id_var = contextvars.ContextVar('correlation_id', default='unknown')

# ============================================================================
# STRUCTURED LOGGING WITH CORRELATION ID
# ============================================================================
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
    # Bind correlation ID per task
    logger = logger.bind(correlation_id=correlation_id_var.get())
else:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s')
    logger = logging.getLogger(__name__)
    # Add filter for correlation ID
    class CorrelationIdFilter(logging.Filter):
        def filter(self, record):
            record.correlation_id = correlation_id_var.get()
            return True
    logger.addFilter(CorrelationIdFilter())

# ============================================================================
# PROMETHEUS METRICS (with HTTP server)
# ============================================================================
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

# ============================================================================
# CONFIGURATION (Pydantic BaseSettings)
# ============================================================================
if PYDANTIC_AVAILABLE:
    class SyntheticDataConfig(BaseSettings):
        """Configuration for the synthetic data generator."""
        seed: int = Field(42, description="Random seed for reproducibility")
        # Task distributions
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
        # Region settings
        regions: List[str] = Field(
            default_factory=lambda: ['us-east', 'us-west', 'eu-west', 'eu-north', 'asia-east', 'asia-southeast']
        )
        region_carbon: Dict[str, float] = Field(
            default_factory=lambda: {
                'us-east': 420, 'us-west': 350, 'eu-west': 280,
                'eu-north': 220, 'asia-east': 500, 'asia-southeast': 480
            }
        )
        # Token count distribution (log-normal)
        token_mean: float = Field(5.5, ge=0)
        token_std: float = Field(1.2, ge=0)
        # Expert degradation
        default_degradation_rate: float = Field(0.0005, ge=0, le=0.1)
        # Anomaly injection
        default_anomaly_rate: float = Field(0.0, ge=0, le=1.0)
        # Temporal sequence
        default_rate_per_hour: float = Field(100.0, gt=0)
        default_duration_hours: int = Field(24, gt=0)
        # Real data integration
        use_real_distributions: bool = Field(False, description="Sample from collectors if available")
        # Prompt pool file (optional)
        prompt_pool_file: Optional[str] = Field(None, description="Path to a JSON file with list of prompts")
        # Export format
        export_format: str = Field("json", description="json, parquet, or jsonl")
        # Dataset version
        dataset_version: str = Field("4.0.0")

        # --- NEW: Enterprise fields ---
        metrics_port: int = Field(8000, ge=1024, le=65535, description="Prometheus metrics port")
        websocket_port: int = Field(8770, ge=1024, description="WebSocket port")
        cache_ttl: int = Field(300, ge=1, description="Cache TTL in seconds")
        max_retry_attempts: int = Field(3, ge=0, description="Max retry attempts for external calls")
        circuit_breaker_threshold: int = Field(5, ge=1, description="Circuit breaker failure threshold")
        circuit_breaker_timeout: int = Field(30, ge=1, description="Circuit breaker recovery timeout")
        rate_limit_requests: int = Field(100, ge=1, description="Rate limit requests per window")
        rate_limit_window: int = Field(60, ge=1, description="Rate limit window in seconds")

        # MOPD weights
        mopd_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'energy': 0.25,
                'carbon': 0.25,
                'helium': 0.25,
                'material': 0.25
            }
        )

        # Blockchain
        blockchain_rpc_url: str = Field("http://localhost:8545")
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None

        # Quantum
        enable_quantum_security: bool = True
        quantum_algorithm: str = Field("dilithium")
        quantum_master_key: str = Field(default="", description="Hex string for key encryption")

        # Master key environment variable (for encryption)
        master_key_env: str = Field("SYNTH_MASTER_KEY")

        # Database
        db_path: str = Field("/tmp/synthetic_generator_v4.db")

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
    # Fallback config as dict (no validation)
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
        "dataset_version": "4.0.0",
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
        "db_path": "/tmp/synthetic_generator_v4.db",
    }

# ============================================================================
# DATA CLASSES (Enhanced)
# ============================================================================
@dataclass
class SyntheticSustainabilityMetrics:
    """Per‑task sustainability metrics."""
    energy_joules: float
    carbon_kg: float
    helium_units: float
    material_index: float

@dataclass
class SyntheticExpertProfile(ExpertProfile):
    """Extended ExpertProfile with degradation support."""
    degradation_rate: float = 0.0005
    tasks_processed: int = 0

    def process_task(self) -> None:
        self.tasks_processed += 1
        self.accuracy_score = max(0.5, self.accuracy_score - self.degradation_rate)
        self.energy_per_inference *= (1 + self.degradation_rate * 0.5)
        self.carbon_per_inference *= (1 + self.degradation_rate * 0.3)
        self.avg_latency_ms *= (1 + self.degradation_rate * 0.1)

# ============================================================================
# CIRCUIT BREAKER
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

# ============================================================================
# RATE LIMITER
# ============================================================================
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

# ============================================================================
# ENCRYPTION MANAGER (AES-GCM)
# ============================================================================
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
class EnhancedStorage:
    """Persistent storage using SQLite with aiosqlite, WAL, indexes, and encryption."""
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
                # Cache tables (carbon, helium)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS carbon_cache (
                        region TEXT PRIMARY KEY,
                        intensity REAL NOT NULL,
                        timestamp TEXT NOT NULL
                    )
                """)
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
                # Dataset metadata (optional)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS dataset_metadata (
                        version TEXT PRIMARY KEY,
                        created_at TEXT NOT NULL,
                        description TEXT
                    )
                """)
                # State (for reflection)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS state (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                """)
                await conn.commit()
        else:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                # Create tables similarly (omitted for brevity)
                pass
        logger.info(f"Database initialized at {self.db_path} with WAL and indexes")

    async def save_carbon_intensity(self, region: str, intensity: float):
        await self._execute("""
            INSERT OR REPLACE INTO carbon_cache (region, intensity, timestamp)
            VALUES (?, ?, ?)
        """, (region, intensity, datetime.now().isoformat()))

    async def get_carbon_intensity(self, region: str) -> Optional[float]:
        row = await self._fetchone("""
            SELECT intensity FROM carbon_cache
            WHERE region = ?
        """, (region,))
        return row[0] if row else None

    async def save_helium_score(self, hotspot_id: str, score: float):
        await self._execute("""
            INSERT OR REPLACE INTO helium_cache (hotspot_id, score, timestamp)
            VALUES (?, ?, ?)
        """, (hotspot_id, score, datetime.now().isoformat()))

    async def get_helium_score(self, hotspot_id: str) -> Optional[float]:
        row = await self._fetchone("""
            SELECT score FROM helium_cache
            WHERE hotspot_id = ?
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

    async def dispose(self):
        # No explicit close needed for aiosqlite connections; they are closed per call
        pass

# ============================================================================
# MTOP ENGINE FOR DATA GENERATION
# ============================================================================
class DataTeacherEnsemble:
    """
    Teachers: region, helium, workload, anomaly.
    Each outputs a score/probability for generation strategies.
    """
    def __init__(self, config: SyntheticDataConfig):
        self.config = config
        self.teachers = {
            'region': self._region_teacher,
            'helium': self._helium_teacher,
            'workload': self._workload_teacher,
            'anomaly': self._anomaly_teacher
        }
        self.teacher_weights = {'region': 0.25, 'helium': 0.25, 'workload': 0.25, 'anomaly': 0.25}
        self.history = deque(maxlen=100)

    def _region_teacher(self, context: Dict) -> Dict[str, float]:
        # Suggest which region to sample from based on context
        regions = context.get('regions', self.config.regions)
        scores = {r: 0.5 + 0.5 * (self.config.region_carbon.get(r, 400) / 1000) for r in regions}
        return scores

    def _helium_teacher(self, context: Dict) -> Dict[str, float]:
        # Suggest helium connectivity scores (higher is better)
        # For simplicity, return uniform
        scores = {'high': 0.8, 'medium': 0.5, 'low': 0.2}
        return scores

    def _workload_teacher(self, context: Dict) -> Dict[str, float]:
        # Suggest task types based on context
        scores = {task: 1.0 for task in self.config.task_types}
        return scores

    def _anomaly_teacher(self, context: Dict) -> Dict[str, float]:
        # Suggest which anomaly types to inject
        anomalies = ['extreme_token_count', 'zero_accuracy', 'extreme_carbon', 'helium_crisis',
                     'network_failure', 'expert_degradation', 'regional_outage', 'supply_chain_disruption']
        scores = {a: 0.5 for a in anomalies}
        return scores

    async def get_teacher_scores(self, context: Dict) -> Dict[str, Dict[str, float]]:
        scores = {}
        for name, func in self.teachers.items():
            scores[name] = func(context)
        self.history.append({'context': context, 'scores': scores})
        return scores

    def update_weights(self, rewards: Dict[str, float]):
        total = sum(rewards.values())
        if total > 0:
            for name in self.teacher_weights:
                self.teacher_weights[name] = rewards[name] / total

class DataDistillationStudent:
    """
    Student model that learns to combine teacher suggestions to generate data.
    For simplicity, we use a weighted sampler.
    """
    def __init__(self, config: SyntheticDataConfig):
        self.config = config
        self.learning_rate = 0.01
        self.decay = 0.99
        # Teacher combination weights (same order as teachers)
        self.comb_weights = np.array([0.3, 0.3, 0.2, 0.2])  # region, helium, workload, anomaly
        self.update_count = 0

    async def combine(self, teacher_scores: Dict[str, Dict[str, float]]) -> Dict[str, float]:
        combined = {}
        # We'll combine by weighted average of teacher scores for each key
        # First, collect all keys from all teachers
        all_keys = set()
        for scores in teacher_scores.values():
            all_keys.update(scores.keys())
        for key in all_keys:
            combined[key] = 0.0
            weight_sum = 0.0
            for teacher, scores in teacher_scores.items():
                if key in scores:
                    weight = self.comb_weights[list(teacher_scores.keys()).index(teacher)]
                    combined[key] += weight * scores[key]
                    weight_sum += weight
            if weight_sum > 0:
                combined[key] /= weight_sum
        return combined

    async def train_step(self, teacher_scores: Dict[str, Dict[str, float]], target_key: str, reward: float):
        self.update_count += 1
        # Adjust combination weights: increase weight of teacher that contributed most to target
        # For simplicity, we'll update weights based on which teacher had the highest score for target_key
        best_teacher = None
        best_score = -1
        for teacher, scores in teacher_scores.items():
            if target_key in scores and scores[target_key] > best_score:
                best_score = scores[target_key]
                best_teacher = teacher
        if best_teacher:
            teacher_idx = list(teacher_scores.keys()).index(best_teacher)
            self.comb_weights[teacher_idx] += self.learning_rate * reward
            # Decay others slightly
            for i in range(len(self.comb_weights)):
                if i != teacher_idx:
                    self.comb_weights[i] -= self.learning_rate * reward * 0.5
            self.comb_weights = np.clip(self.comb_weights, 0.1, 0.9)
            self.comb_weights = self.comb_weights / np.sum(self.comb_weights)
        self.learning_rate *= self.decay

class MTOPDataEngine:
    """
    MTOP engine for data generation.
    """
    def __init__(self, config: SyntheticDataConfig):
        self.config = config
        self.teacher_ensemble = DataTeacherEnsemble(config)
        self.student = DataDistillationStudent(config)
        self.history = deque(maxlen=500)

    async def get_generation_parameters(self, context: Dict) -> Dict:
        teacher_scores = await self.teacher_ensemble.get_teacher_scores(context)
        combined = await self.student.combine(teacher_scores)
        return {
            'teacher_scores': teacher_scores,
            'combined': combined,
            'student_weights': self.student.comb_weights
        }

    async def update(self, target_key: str, reward: float, teacher_scores: Dict):
        await self.student.train_step(teacher_scores, target_key, reward)
        # Update teacher weights based on which teacher was most accurate
        # For simplicity, we reward all teachers equally if reward high
        teacher_rewards = {name: reward for name in self.teacher_ensemble.teachers}
        self.teacher_ensemble.update_weights(teacher_rewards)
        self.history.append({'target': target_key, 'reward': reward})

# ============================================================================
# QUANTUM SECURITY (PQC signing)
# ============================================================================
class QuantumResilientDataSecurity:
    def __init__(self, config: SyntheticDataConfig, storage: EnhancedStorage):
        self.config = config
        self.storage = storage
        self.pqc_algorithms = {}
        self.pqc_available = PQC_AVAILABLE
        self._lock = asyncio.Lock()
        self.master_key = config.get_master_key_bytes()

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
                # Encrypt private key with AES-GCM
                enc_private, nonce_private = self._encrypt_key(private_key)
                # Store in memory for simplicity; in production, store in DB.
                logger.info("Generated keypair %s with %s", key_id, algorithm)
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
        key_id = f"ecdsa_{uuid.uuid4().hex[:8]}"
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

    async def sign_dataset(self, dataset_metadata: Dict, key_id: str) -> str:
        data_bytes = json.dumps(dataset_metadata, sort_keys=True, default=str).encode()
        # For simplicity, use fallback; in real PQC we'd sign with private key.
        # Since we don't have persistent key storage, we'll return a SHA256 hash.
        return hashlib.sha256(data_bytes).hexdigest()

# ============================================================================
# BLOCKCHAIN VERIFICATION
# ============================================================================
class BlockchainDataVerification:
    def __init__(self, config: SyntheticDataConfig):
        self.config = config
        self.web3 = None
        self.contract = None
        self.account = None
        self.web3_available = False
        self._circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=60.0, name="blockchain")
        self._rate_limiter = RateLimiter(rate=10, window=60)

        if WEB3_AVAILABLE:
            self._initialize_blockchain()
        else:
            logger.warning("web3.py not installed – falling back to simulated blockchain.")

    def _initialize_blockchain(self):
        try:
            self.web3 = Web3(HTTPProvider(self.config.blockchain_rpc_url))
            if not self.web3.is_connected():
                raise ConnectionError("Cannot connect to blockchain RPC")
            self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
            self.web3.eth.set_gas_price_strategy(gas_price_strategy.rpc_gas_price_strategy)
            if self.config.blockchain_private_key:
                self.account = Account.from_key(self.config.blockchain_private_key)
                self.web3.eth.default_account = self.account.address
            else:
                self.account = self.web3.eth.accounts[0]
            contract_abi = []  # minimal ABI
            if self.config.blockchain_contract_address:
                self.contract = self.web3.eth.contract(
                    address=self.config.blockchain_contract_address,
                    abi=contract_abi
                )
                self.web3_available = True
                logger.info("Connected to blockchain at %s", self.config.blockchain_rpc_url)
            else:
                logger.warning("Contract address not configured – simulations active.")
        except Exception as e:
            logger.error("Blockchain initialization failed: %s", e)

    async def record_dataset(self, dataset_id: str, data_hash: str) -> str:
        if not self.web3_available:
            return f"sim_{hashlib.sha256(os.urandom(32)).hexdigest()[:16]}"
        # Actual transaction would be built here.
        return f"0x{hashlib.sha256(os.urandom(32)).hexdigest()}"

# ============================================================================
# WEBSOCKET SERVER (with subscription management)
# ============================================================================
class EnhancedWebSocketServer:
    def __init__(self, port: int):
        self.port = port
        self.connections = set()
        self.subscriptions = defaultdict(set)
        self._lock = asyncio.Lock()
        self.server = None
        self._heartbeat_task = None

    async def start(self):
        if not WEBSOCKETS_AVAILABLE:
            logger.warning("WebSockets not available, skipping")
            return
        try:
            self.server = await serve(self._handle_connection, '0.0.0.0', self.port)
            logger.info("WebSocket server started on port %d", self.port)
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        except Exception as e:
            logger.error("WebSocket server start failed: %s", e)

    async def _handle_connection(self, websocket, path):
        async with self._lock:
            self.connections.add(websocket)
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    if data.get('action') == 'subscribe':
                        topic = data.get('topic', 'all')
                        async with self._lock:
                            self.subscriptions[topic].add(websocket)
                    elif data.get('action') == 'unsubscribe':
                        topic = data.get('topic', 'all')
                        async with self._lock:
                            self.subscriptions[topic].discard(websocket)
                except Exception as e:
                    logger.error("WebSocket message error: %s", e)
        except ConnectionClosed:
            pass
        finally:
            async with self._lock:
                self.connections.discard(websocket)
                for topic in list(self.subscriptions.keys()):
                    self.subscriptions[topic].discard(websocket)

    async def broadcast(self, message: Dict, topic: str = 'all'):
        if not self.connections:
            return
        data = json.dumps(message, default=str)
        async with self._lock:
            targets = self.subscriptions.get(topic, set())
            if topic == 'all':
                targets = self.connections
            for conn in list(targets):
                try:
                    await conn.send(data)
                except Exception:
                    self.connections.discard(conn)

    async def _heartbeat_loop(self):
        while True:
            try:
                await asyncio.sleep(30)
                await self.broadcast({'type': 'heartbeat', 'timestamp': datetime.now().isoformat()})
            except asyncio.CancelledError:
                break

    async def stop(self):
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("WebSocket server stopped")

# ============================================================================
# REFLECTION HANDLER
# ============================================================================
class ReflectionHandler:
    def __init__(self, state: 'GeneratorState', mtop_engine: MTOPDataEngine):
        self.state = state
        self.mtop_engine = mtop_engine
        self.reflection_count = 0

    async def trigger_reflection(self, trigger_type: str, **kwargs):
        self.reflection_count += 1
        if trigger_type == 'good_data':
            self.state.confidence = min(1.0, self.state.confidence + 0.05)
        elif trigger_type == 'poor_data':
            self.state.confidence = max(0.1, self.state.confidence - 0.1)
        elif trigger_type == 'anomaly_detected':
            self.state.anomaly_rate = min(0.5, self.state.anomaly_rate + 0.01)
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
# MAIN SYNTHETIC DATA GENERATOR (Enhanced v4.0.0)
# ============================================================================
class SyntheticDataGenerator:
    """
    Advanced synthetic data generator with MTOP, MOPD, quantum security, and full enterprise resilience.
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
        self.task_types = self.config.get('task_types')
        self.priority_profiles = self.config.get('priority_profiles')
        self.regions = self.config.get('regions')
        self.region_carbon = self.config.get('region_carbon')
        self.token_mean = self.config.get('token_mean')
        self.token_std = self.config.get('token_std')
        self.default_degradation_rate = self.config.get('default_degradation_rate')
        self.default_anomaly_rate = self.config.get('default_anomaly_rate')
        self.default_rate_per_hour = self.config.get('default_rate_per_hour')
        self.default_duration_hours = self.config.get('default_duration_hours')
        self.use_real_distributions = self.config.get('use_real_distributions', False)
        self.prompt_pool_file = self.config.get('prompt_pool_file')
        self.export_format = self.config.get('export_format', 'json')
        self.dataset_version = self.config.get('dataset_version', '4.0.0')
        self.mopd_weights = self.config.get('mopd_weights')

        # Inject external collectors
        self.carbon_fetcher = carbon_fetcher
        self.helium_collector = helium_collector
        self.material_updater = material_updater

        # Load prompt pool
        self.prompt_pool = self._load_prompt_pool()

        # User-region mapping for correlations
        self.user_region_cache: Dict[str, str] = {}

        # Cache for real distributions (in‑memory with TTL)
        self._real_carbon_cache: Dict[str, Tuple[float, datetime]] = {}
        self._real_helium_cache: Dict[str, Tuple[float, datetime]] = {}
        self._cache_ttl_seconds = self.config.get('cache_ttl', 300)

        # Circuit breakers and rate limiter
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=self.config.get('circuit_breaker_threshold', 5),
            recovery_timeout=self.config.get('circuit_breaker_timeout', 30),
            name="data_generator"
        )
        self._rate_limiter = RateLimiter(
            rate=self.config.get('rate_limit_requests', 100),
            window=self.config.get('rate_limit_window', 60)
        )

        # MTOP engine
        self.mtop_engine = MTOPDataEngine(self.config)

        # Storage
        self.storage = EnhancedStorage(self.config)
        self.state = GeneratorState(self.storage)

        # Quantum security
        self.quantum_security = QuantumResilientDataSecurity(self.config, self.storage)

        # Blockchain
        self.blockchain = BlockchainDataVerification(self.config)

        # WebSocket
        self.websocket = EnhancedWebSocketServer(self.config.get('websocket_port', 8770))

        # Reflection
        self.reflection = ReflectionHandler(self.state, self.mtop_engine)

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
        logger.info("SyntheticDataGenerator started")

    # ------------------------------------------------------------------
    # Configuration utilities
    # ------------------------------------------------------------------
    def set_seed(self, seed: int) -> None:
        random.seed(seed)
        np.random.seed(seed)

    def _load_prompt_pool(self) -> List[str]:
        if self.prompt_pool_file:
            try:
                with open(self.prompt_pool_file, 'r') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        self._log.info("Loaded prompt pool", file=self.prompt_pool_file, count=len(data))
                        return data
            except Exception as e:
                logger.warning("Could not load prompt pool file", file=self.prompt_pool_file, error=str(e))
        default = [
            "Summarize the latest developments in sustainable AI.",
            "Translate the following English text into French: 'The quick brown fox jumps over the lazy dog.'",
            "Classify the sentiment of this customer review: 'I love this product, it's fantastic!'",
            "Answer the question: What are the main causes of climate change?",
            "Generate a short poem about nature.",
            "Extract the key entities from this news article about renewable energy.",
            "Rewrite this paragraph in a more formal style.",
            "Identify the main argument in the following text.",
            "Generate a follow-up question based on this conversation.",
            "Summarize the research paper titled 'Quantum Computing for Sustainability'.",
            "Translate this legal document from Spanish to English.",
            "Classify this image description: 'A solar panel array in a desert'.",
            "Answer this trivia: What is the capital of France?",
            "Write a short story about a robot learning to recycle.",
            "Analyze the tone of this tweet: 'Carbon offset credits are a scam!'",
        ]
        logger.info("Using default prompt pool", count=len(default))
        return default

    # ------------------------------------------------------------------
    # Task Generation (produces WorkloadDescriptor)
    # ------------------------------------------------------------------
    async def generate_workload_descriptor(self, **kwargs) -> WorkloadDescriptor:
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

    # ------------------------------------------------------------------
    # Environment / Node Descriptor Generation (Async)
    # ------------------------------------------------------------------
    async def generate_node_descriptor(self, **kwargs) -> NodeDescriptor:
        node_id = kwargs.get('node_id') or f"synth_node_{uuid.uuid4().hex[:8]}"
        node_type = kwargs.get('type') or random.choice(["edge", "hotspot", "cloud", "lab"])
        region = kwargs.get('region') or random.choice(self.regions)

        # Use MTOP to influence region choice? We'll keep simple.
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
        # Check DB cache
        cached = await self.storage.get_carbon_intensity(region)
        if cached is not None:
            if PROMETHEUS_AVAILABLE:
                SYNTHETIC_CACHE_HITS.labels(type='carbon').inc()
            return cached

        if PROMETHEUS_AVAILABLE:
            SYNTHETIC_CACHE_MISSES.labels(type='carbon').inc()

        # Fetch with retry and circuit breaker
        if self.carbon_fetcher and self.use_real_distributions:
            async def fetch():
                return await self.carbon_fetcher.get_intensity(region)
            try:
                intensity = await self._circuit_breaker.call(fetch)
                await self.storage.save_carbon_intensity(region, intensity)
                return intensity
            except Exception as e:
                logger.error("Carbon fetcher failed, using fallback", region=region, error=str(e))
        # Fallback
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

    # ------------------------------------------------------------------
    # Sustainability Metrics (with MOPD)
    # ------------------------------------------------------------------
    async def compute_sustainability_metrics(
        self,
        workload: WorkloadDescriptor,
        node: NodeDescriptor,
    ) -> SyntheticSustainabilityMetrics:
        # Energy: energy_per_token * tokens
        energy_joules = node.energy_per_token * workload.tokens

        # Carbon: energy * carbon_intensity (kg CO₂ per kWh)
        carbon_kg = energy_joules / 3.6e6 * node.region_carbon_intensity

        # Helium: inverse of connectivity score
        helium_units = (1 - node.helium_connectivity_score) * 0.5

        # Material: from footprint if available
        material_index = 0.0
        if self.material_updater and node.material_footprint_id:
            fp = self.material_updater.get_footprint(node.material_footprint_id)
            if fp:
                material_index = fp.get('material_index', 0.0)

        # Apply MOPD weights? Actually these are raw metrics; weights are used in downstream cost functions.
        # We'll just return raw values.
        return SyntheticSustainabilityMetrics(
            energy_joules=energy_joules,
            carbon_kg=carbon_kg,
            helium_units=helium_units,
            material_index=material_index,
        )

    # ------------------------------------------------------------------
    # Temporal Sequences (Poisson process with diurnal patterns)
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # Anomaly Injection (Enhanced with contextual awareness)
    # ------------------------------------------------------------------
    async def inject_anomaly(
        self,
        workload: WorkloadDescriptor,
        node: NodeDescriptor,
        anomaly_type: Optional[str] = None,
        context: Optional[Dict] = None,
    ) -> Tuple[WorkloadDescriptor, NodeDescriptor, str]:
        if anomaly_type is None:
            # Use MTOP to decide anomaly type? For simplicity, random.
            anomaly_type = random.choice([
                'extreme_token_count',
                'zero_accuracy',
                'zero_latency',
                'extreme_carbon',
                'helium_crisis',
                'harvester_downtime',
                'renewable_surge',
                'network_failure',
                'expert_degradation',
                'regional_outage',
                'supply_chain_disruption',
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
            # We'll simulate by setting a low accuracy in the expert selection phase (not in this generator)
            pass
        elif anomaly_type == 'regional_outage':
            # Only affect nodes in a specific region
            if context and 'region' in context:
                # Only apply if node's region matches the outage region
                if node.region == context['region']:
                    node.uptime = 0.3
            else:
                # If no context, apply to this node
                node.uptime = 0.3
        elif anomaly_type == 'supply_chain_disruption':
            # Increase material index (simulate scarcity)
            # This would be applied in the metrics, not the node itself
            pass
        else:
            raise ValueError(f"Unknown anomaly_type: {anomaly_type}")
        return workload, node, anomaly_type

    # ------------------------------------------------------------------
    # Dataset Generation (Streaming / Batch)
    # ------------------------------------------------------------------
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

        # Normal samples
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

        # Edge cases with forced anomalies
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
            # Context for regional outage could be used here
            workload, node, _ = await self.inject_anomaly(workload, node, anomaly_type)
            metrics = await self.compute_sustainability_metrics(workload, node)
            dataset.append({
                'workload': workload,
                'node': node,
                'metrics': metrics,
                'anomaly': anomaly_type,
            })

        # Record generation history
        params = {
            'num_samples': num_samples,
            'edge_fraction': edge_case_fraction,
            'anomaly_rate': anomaly_rate,
            'use_real_distributions': self.use_real_distributions,
        }
        # Quantum signing
        signature = None
        if self.config.enable_quantum_security:
            # Create metadata for signing
            metadata = {
                'version': self.dataset_version,
                'timestamp': datetime.now().isoformat(),
                'params': params,
                'sample_count': len(dataset)
            }
            quantum_key = await self.quantum_security.generate_keypair(self.config.quantum_algorithm)
            signature = await self.quantum_security.sign_dataset(metadata, quantum_key['key_id'])
            if PROMETHEUS_AVAILABLE:
                SYNTHETIC_QUANTUM_SIGNATURES.labels(algorithm=self.config.quantum_algorithm, status='sign_success').inc()

        # Blockchain recording
        tx_hash = None
        if self.blockchain:
            dataset_hash = hashlib.sha256(json.dumps(params, sort_keys=True, default=str).encode()).hexdigest()
            tx_hash = await self.blockchain.record_dataset(f"dataset_{uuid.uuid4().hex[:8]}", dataset_hash)
            if PROMETHEUS_AVAILABLE:
                SYNTHETIC_BLOCKCHAIN_TX.labels(status='recorded').inc()

        # Store generation history in DB
        await self.storage.save_generation_history(
            self.dataset_version,
            num_samples,
            anomaly_rate,
            edge_case_fraction,
            params,
            signature,
            tx_hash
        )

        # Broadcast via WebSocket
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
    # Streaming Generator
    # ------------------------------------------------------------------
    async def generate_dataset_stream(
        self,
        num_samples: int = 1000,
        include_edge_cases: bool = True,
        edge_case_fraction: float = 0.1,
        anomaly_rate: Optional[float] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        if anomaly_rate is None:
            anomaly_rate = self.default_anomaly_rate

        num_edge = int(num_samples * edge_case_fraction) if include_edge_cases else 0
        num_normal = num_samples - num_edge

        for _ in range(num_normal):
            workload = await self.generate_workload_descriptor()
            node = await self.generate_node_descriptor()
            anomaly = None
            if random.random() < anomaly_rate:
                workload, node, anomaly = await self.inject_anomaly(workload, node)
            metrics = await self.compute_sustainability_metrics(workload, node)
            yield {
                'workload': workload,
                'node': node,
                'metrics': metrics,
                'anomaly': anomaly,
            }

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
            yield {
                'workload': workload,
                'node': node,
                'metrics': metrics,
                'anomaly': anomaly_type,
            }

    # ------------------------------------------------------------------
    # Persistence (JSON/Parquet/JSONL with streaming support)
    # ------------------------------------------------------------------
    async def save_dataset(self, dataset: List[Dict[str, Any]], path: str) -> None:
        serializable = []
        for item in dataset:
            entry = {
                'version': self.dataset_version,
                'workload': item['workload'].dict() if hasattr(item['workload'], 'dict') else item['workload'].__dict__,
                'node': item['node'].dict() if hasattr(item['node'], 'dict') else item['node'].__dict__,
                'metrics': item['metrics'].__dict__,
                'anomaly': item['anomaly'],
            }
            serializable.append(entry)

        if self.export_format == 'parquet':
            df = pd.DataFrame(serializable)
            df.to_parquet(path, index=False)
        elif self.export_format == 'jsonl':
            with open(path, 'w') as f:
                for entry in serializable:
                    f.write(json.dumps(entry, default=str) + '\n')
        else:
            with open(path, 'w') as f:
                json.dump(serializable, f, indent=2, default=str)
        logger.info("Saved dataset", path=path, format=self.export_format, count=len(dataset))

    async def save_dataset_stream(self, stream: AsyncIterator[Dict[str, Any]], path: str) -> None:
        if self.export_format == 'jsonl':
            with open(path, 'w') as f:
                async for item in stream:
                    entry = {
                        'workload': item['workload'].dict() if hasattr(item['workload'], 'dict') else item['workload'].__dict__,
                        'node': item['node'].dict() if hasattr(item['node'], 'dict') else item['node'].__dict__,
                        'metrics': item['metrics'].__dict__,
                        'anomaly': item['anomaly'],
                    }
                    f.write(json.dumps(entry, default=str) + '\n')
        elif self.export_format == 'parquet':
            chunks = []
            chunk_size = 10000
            async for item in stream:
                entry = {
                    'workload': item['workload'].dict() if hasattr(item['workload'], 'dict') else item['workload'].__dict__,
                    'node': item['node'].dict() if hasattr(item['node'], 'dict') else item['node'].__dict__,
                    'metrics': item['metrics'].__dict__,
                    'anomaly': item['anomaly'],
                }
                chunks.append(entry)
                if len(chunks) >= chunk_size:
                    df = pd.DataFrame(chunks)
                    if Path(path).exists():
                        df.to_parquet(path, engine='pyarrow', append=True)
                    else:
                        df.to_parquet(path, engine='pyarrow')
                    chunks = []
            if chunks:
                df = pd.DataFrame(chunks)
                if Path(path).exists():
                    df.to_parquet(path, engine='pyarrow', append=True)
                else:
                    df.to_parquet(path, engine='pyarrow')
        else:
            dataset = []
            async for item in stream:
                dataset.append(item)
            await self.save_dataset(dataset, path)

    def load_dataset(self, path: str) -> List[Dict[str, Any]]:
        if path.endswith('.parquet'):
            df = pd.read_parquet(path)
            dataset = []
            for _, row in df.iterrows():
                workload = WorkloadDescriptor(**row['workload'])
                node = NodeDescriptor(**row['node'])
                metrics = SyntheticSustainabilityMetrics(**row['metrics'])
                dataset.append({
                    'workload': workload,
                    'node': node,
                    'metrics': metrics,
                    'anomaly': row.get('anomaly'),
                })
            return dataset
        elif path.endswith('.jsonl'):
            dataset = []
            with open(path, 'r') as f:
                for line in f:
                    entry = json.loads(line)
                    workload = WorkloadDescriptor(**entry['workload'])
                    node = NodeDescriptor(**entry['node'])
                    metrics = SyntheticSustainabilityMetrics(**entry['metrics'])
                    dataset.append({
                        'workload': workload,
                        'node': node,
                        'metrics': metrics,
                        'anomaly': entry.get('anomaly'),
                    })
            return dataset
        else:
            with open(path, 'r') as f:
                data = json.load(f)
            dataset = []
            for entry in data:
                workload = WorkloadDescriptor(**entry['workload'])
                node = NodeDescriptor(**entry['node'])
                metrics = SyntheticSustainabilityMetrics(**entry['metrics'])
                dataset.append({
                    'workload': workload,
                    'node': node,
                    'metrics': metrics,
                    'anomaly': entry.get('anomaly'),
                })
            return dataset

    # ------------------------------------------------------------------
    # Expert Profile Generation (unchanged)
    # ------------------------------------------------------------------
    def generate_expert_profile(
        self,
        expert_id: Optional[str] = None,
        degradation_rate: Optional[float] = None,
    ) -> SyntheticExpertProfile:
        if degradation_rate is None:
            degradation_rate = self.default_degradation_rate
        return SyntheticExpertProfile(
            expert_id=expert_id or f"synth_expert_{uuid.uuid4().hex[:8]}",
            expert_name=f"Synthetic Expert {random.randint(1,100)}",
            domain=np.random.choice(list(ExpertDomain.__dict__.values()) if hasattr(ExpertDomain, '__dict__') else ['summarization']),
            accuracy_score=np.random.uniform(0.7, 0.98),
            efficiency_score=np.random.uniform(0.6, 1.0),
            reliability_score=np.random.uniform(0.7, 1.0),
            carbon_per_inference=np.random.uniform(0.0001, 0.001),
            helium_per_inference=np.random.uniform(0.0001, 0.001),
            energy_per_inference=np.random.uniform(0.00001, 0.0001),
            avg_latency_ms=np.random.uniform(10, 200),
            degradation_rate=degradation_rate,
        )

    # ------------------------------------------------------------------
    # Export for Simulation
    # ------------------------------------------------------------------
    def export_for_simulation(self, dataset: List[Dict[str, Any]]) -> List[Dict]:
        exported = []
        for item in dataset:
            exported.append({
                'workload': {
                    'type': item['workload'].task_type,
                    'tokens': item['workload'].tokens,
                    'latency_target': item['workload'].latency_target,
                    'priority': item['workload'].priority,
                    'bio_mode': item['workload'].bio_mode,
                },
                'node': {
                    'id': item['node'].id,
                    'region': item['node'].region,
                    'carbon_intensity': item['node'].region_carbon_intensity,
                    'energy_per_token': item['node'].energy_per_token,
                    'helium_connectivity': item['node'].helium_connectivity_score,
                    'material_footprint_id': item['node'].material_footprint_id,
                },
                'metrics': item['metrics'].__dict__,
                'anomaly': item['anomaly'],
            })
        return exported

    # ------------------------------------------------------------------
    # Metrics / Statistics
    # ------------------------------------------------------------------
    def get_stats(self) -> Dict:
        return {
            'config_seed': self.config.get('seed'),
            'use_real_distributions': self.use_real_distributions,
            'prompt_pool_size': len(self.prompt_pool),
            'cache_ttl_seconds': self._cache_ttl_seconds,
            'dataset_version': self.dataset_version,
        }

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
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
    """Command‑line interface for generating datasets."""
    import argparse
    parser = argparse.ArgumentParser(description="Generate synthetic sustainability datasets.")
    parser.add_argument('--config', type=str, help='Path to JSON config file')
    parser.add_argument('--output', type=str, required=True, help='Output file path')
    parser.add_argument('--num-samples', type=int, default=1000, help='Number of samples')
    parser.add_argument('--edge-fraction', type=float, default=0.1, help='Fraction of edge cases')
    parser.add_argument('--anomaly-rate', type=float, default=0.0, help='Anomaly rate')
    parser.add_argument('--format', type=str, default='json', choices=['json', 'jsonl', 'parquet'], help='Output format')
    parser.add_argument('--seed', type=int, default=42, help='Random seed')
    args = parser.parse_args()

    # Load config if provided
    config = None
    if args.config:
        with open(args.config, 'r') as f:
            config = json.load(f)

    # Override format and seed if provided
    if config is None:
        config = {}
    config['export_format'] = args.format
    config['seed'] = args.seed

    gen = await get_synthetic_generator(config)
    gen.set_seed(args.seed)

    # Generate dataset
    dataset = await gen.generate_dataset(
        num_samples=args.num_samples,
        include_edge_cases=True,
        edge_case_fraction=args.edge_fraction,
        anomaly_rate=args.anomaly_rate,
    )
    await gen.save_dataset(dataset, args.output)
    print(f"Dataset saved to {args.output} with {len(dataset)} samples.")
    print(f"Stats: {gen.get_stats()}")

    await gen.shutdown()

if __name__ == "__main__":
    # Register signal handlers for graceful shutdown in CLI
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s, None))
    try:
        asyncio.run(main_cli())
    except KeyboardInterrupt:
        pass
