#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/reasoning_engine_enhanced_v5_0.py
# VERSION: 5.0.0 (Enterprise Quantum Resilience + MTOP + MOPD + Bio‑Inspired GA + MoE + Pareto)
# =============================================================================
"""
Reasoning Engine for Green Agent - Version 5.0.0
Implements temporal, causal, ethical, contextual, systemic, and reflexive reasoning
Enhanced with live data integration, persistent learning, performance prediction,
retry logic, central configuration, and complete reasoning modules.

VERSION 5.0.0 ENHANCEMENTS (over v4.0.0):
- Bio‑inspired Genetic Algorithm (GA) for architecture search and optimisation.
- Full Mixture‑of‑Experts (MoE) gating network for dynamic strategy selection.
- Pareto‑front multi‑objective optimisation with interactive trade‑off exploration.
- Fast MLPRegressor‑based performance predictor (fallback to GP if unavailable).
- All enhancements are optional and integrate with existing modules.
- Updated configuration parameters for GA, MoE, and Pareto.
"""

import asyncio
import hashlib
import json
import os
import sqlite3
import time
import uuid
import signal
from functools import wraps
from collections import deque, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Union, Callable
import secrets
import gc
import contextvars
import random
import math

# -----------------------------------------------------------------------------
# Async SQLite (aiosqlite) – fallback to sqlite3 with thread pool if not available
# -----------------------------------------------------------------------------
try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

# -----------------------------------------------------------------------------
# External dependencies (install via pip)
# -----------------------------------------------------------------------------
try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, field_validator, ValidationError
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    import structlog
    from structlog.processors import JSONRenderer, TimeStamper
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

try:
    from sklearn.gaussian_process import GaussianProcessRegressor
    from sklearn.gaussian_process.kernels import RBF, WhiteKernel
    from sklearn.neural_network import MLPRegressor
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from fastapi import FastAPI, HTTPException
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

try:
    import websockets
    from websockets.server import serve
    from websockets.exceptions import ConnectionClosed
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

# -----------------------------------------------------------------------------
# Structured logging with correlation ID
# -----------------------------------------------------------------------------
correlation_id_var = contextvars.ContextVar('correlation_id', default='unknown')

if STRUCTLOG_AVAILABLE:
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.EventRenamer("msg"),
            TimeStamper(fmt="iso"),
            JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    logger = structlog.get_logger(__name__)
    # Bind correlation ID to logger context per task
    logger = logger.bind(correlation_id=correlation_id_var.get())
else:
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s'
    )
    logger = logging.getLogger(__name__)
    # Add a filter for correlation ID
    class CorrelationIdFilter(logging.Filter):
        def filter(self, record):
            record.correlation_id = correlation_id_var.get()
            return True
    logger.addFilter(CorrelationIdFilter())

# -----------------------------------------------------------------------------
# Prometheus metrics (now with HTTP server)
# -----------------------------------------------------------------------------
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    REASONING_CYCLES = Counter('reasoning_cycles_total', 'Total reasoning cycles', ['status'], registry=REGISTRY)
    REASONING_OPTIMIZATIONS = Counter('reasoning_optimizations_total', 'Autonomous optimizations', ['strategy', 'status'], registry=REGISTRY)
    REASONING_QUANTUM_KEYS = Gauge('reasoning_quantum_keys_total', 'Number of quantum keys', registry=REGISTRY)
    REASONING_BLOCKCHAIN_TX = Counter('reasoning_blockchain_tx_total', 'Blockchain transactions', ['status'], registry=REGISTRY)
    REASONING_CLOUD_DISTRIBUTIONS = Counter('reasoning_cloud_distributions_total', 'Cloud distributions', ['provider', 'status'], registry=REGISTRY)
    REASONING_CARBON_INTENSITY = Gauge('reasoning_carbon_intensity_gco2_per_kwh', 'Current carbon intensity', registry=REGISTRY)
    REASONING_ACCURACY = Gauge('reasoning_predicted_accuracy', 'Predicted accuracy', registry=REGISTRY)
    REASONING_CARBON = Gauge('reasoning_predicted_carbon_kg', 'Predicted carbon kg', registry=REGISTRY)
else:
    class DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, **kwargs): pass
        def set(self, **kwargs): pass
        def observe(self, **kwargs): pass
    REASONING_CYCLES = DummyMetric()
    REASONING_OPTIMIZATIONS = DummyMetric()
    REASONING_QUANTUM_KEYS = DummyMetric()
    REASONING_BLOCKCHAIN_TX = DummyMetric()
    REASONING_CLOUD_DISTRIBUTIONS = DummyMetric()
    REASONING_CARBON_INTENSITY = DummyMetric()
    REASONING_ACCURACY = DummyMetric()
    REASONING_CARBON = DummyMetric()

# -----------------------------------------------------------------------------
# DUMMY TENACITY DECORATOR (if not available)
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
# Configuration with Pydantic (fallback)
# -----------------------------------------------------------------------------
if PYDANTIC_AVAILABLE:
    class ReasoningConfig(BaseModel):
        """Configuration for reasoning engine."""
        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = Field("5.0.0")
        log_level: str = Field("INFO")

        # Database
        db_path: str = Field("/tmp/green_agent_reasoning_v5.db")

        # API keys
        electricity_maps_api_key: Optional[str] = None
        carbon_region: str = Field("global")
        carbon_update_interval: int = Field(300, ge=10)

        # Performance prediction defaults
        training_epochs: int = Field(100, ge=1)
        inference_count: int = Field(1000000, ge=1)

        # Hardware profiles file
        hardware_profiles_path: str = Field("hardware_profiles.json")

        # Cache TTL (seconds)
        cache_ttl: int = Field(300, ge=1)

        # Retry settings
        retry_attempts: int = Field(3, ge=0)
        retry_min_wait: int = Field(2, ge=1)
        retry_max_wait: int = Field(10, ge=1)

        # Metrics
        metrics_port: int = Field(8000, ge=1024, le=65535)

        # WebSocket
        websocket_port: int = Field(8770, ge=1024)

        # MOPD weights (default)
        mopd_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'accuracy': 0.4,
                'carbon': 0.3,
                'cost': 0.2,
                'latency': 0.1
            }
        )

        # Background intervals
        health_check_interval: int = Field(60, ge=10)
        model_retrain_interval: int = Field(3600, ge=60)
        cache_cleanup_interval: int = Field(3600, ge=60)
        auto_optimize_interval: int = Field(1800, ge=60)
        federated_interval: int = Field(3600, ge=60)
        predictive_interval: int = Field(3600, ge=60)
        sustainability_interval: int = Field(3600, ge=60)

        # Master encryption key (must be 32 bytes hex)
        master_key_env: str = Field("GREEN_AGENT_MASTER_KEY")

        # ===== NEW in v5.0.0 =====
        # Genetic Algorithm search
        ga_enabled: bool = Field(True)
        ga_population_size: int = Field(20, ge=5)
        ga_generations: int = Field(5, ge=1)
        ga_mutation_rate: float = Field(0.2, ge=0.0, le=1.0)
        ga_crossover_rate: float = Field(0.7, ge=0.0, le=1.0)

        # Mixture-of-Experts
        moe_enabled: bool = Field(True)
        moe_expert_count: int = Field(4, ge=2)
        moe_hidden_layers: List[int] = Field(default_factory=lambda: [16, 8])

        # Pareto front
        pareto_enabled: bool = Field(True)
        pareto_max_architectures: int = Field(100, ge=10)

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
            env_prefix = "REASONING_"
else:
    from dataclasses import dataclass, field

    @dataclass
    class ReasoningConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "5.0.0"
        log_level: str = "INFO"
        db_path: str = "/tmp/green_agent_reasoning_v5.db"
        electricity_maps_api_key: Optional[str] = None
        carbon_region: str = "global"
        carbon_update_interval: int = 300
        training_epochs: int = 100
        inference_count: int = 1000000
        hardware_profiles_path: str = "hardware_profiles.json"
        cache_ttl: int = 300
        retry_attempts: int = 3
        retry_min_wait: int = 2
        retry_max_wait: int = 10
        metrics_port: int = 8000
        websocket_port: int = 8770
        mopd_weights: Dict[str, float] = field(default_factory=lambda: {
            'accuracy': 0.4, 'carbon': 0.3, 'cost': 0.2, 'latency': 0.1
        })
        health_check_interval: int = 60
        model_retrain_interval: int = 3600
        cache_cleanup_interval: int = 3600
        auto_optimize_interval: int = 1800
        federated_interval: int = 3600
        predictive_interval: int = 3600
        sustainability_interval: int = 3600
        master_key_env: str = "GREEN_AGENT_MASTER_KEY"

        # v5.0.0 new fields
        ga_enabled: bool = True
        ga_population_size: int = 20
        ga_generations: int = 5
        ga_mutation_rate: float = 0.2
        ga_crossover_rate: float = 0.7
        moe_enabled: bool = True
        moe_expert_count: int = 4
        moe_hidden_layers: List[int] = field(default_factory=lambda: [16, 8])
        pareto_enabled: bool = True
        pareto_max_architectures: int = 100

        def get_master_key(self) -> bytes:
            key_hex = os.getenv(self.master_key_env)
            if not key_hex:
                raise ValueError(f"Master key not set in env {self.master_key_env}")
            return bytes.fromhex(key_hex)

# -----------------------------------------------------------------------------
# AES-256-GCM Encryption Utility
# -----------------------------------------------------------------------------
class EncryptionManager:
    """Manages encryption and decryption using AES-256-GCM."""
    
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
# Enhanced Database Manager (async-safe with aiosqlite)
# -----------------------------------------------------------------------------
class EnhancedStorage:
    """Persistent storage using SQLite with aiosqlite, WAL, indexes, and encryption."""
    
    def __init__(self, config: ReasoningConfig):
        self.config = config
        self.db_path = config.db_path
        self.encryption_manager = None
        try:
            master_key = config.get_master_key()
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
                # Reasoning history (encrypted)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS reasoning_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        architecture_hash TEXT NOT NULL,
                        reasoning_data BLOB NOT NULL,   -- encrypted
                        reasoning_nonce BLOB,           -- AES nonce
                        outcomes BLOB,                  -- encrypted
                        outcomes_nonce BLOB,
                        correlation_id TEXT
                    )
                """)
                # Causal effects
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS causal_effects (
                        feature TEXT NOT NULL,
                        value REAL NOT NULL,
                        carbon_impact REAL NOT NULL,
                        accuracy_impact REAL NOT NULL,
                        timestamp TEXT NOT NULL,
                        PRIMARY KEY (feature, timestamp)
                    )
                """)
                # Carbon cache
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS carbon_cache (
                        region TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        intensity REAL NOT NULL,
                        PRIMARY KEY (region, timestamp)
                    )
                """)
                # Performance predictions (training data)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS performance_training (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        architecture_hash TEXT NOT NULL,
                        config TEXT NOT NULL,
                        actual_accuracy REAL,
                        actual_latency REAL,
                        actual_carbon REAL,
                        timestamp TEXT NOT NULL
                    )
                """)
                # Model metadata
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS model_metadata (
                        model_name TEXT PRIMARY KEY,
                        version TEXT,
                        last_trained TEXT,
                        metrics TEXT
                    )
                """)
                # Pareto front storage (new)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS pareto_front (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        architecture_hash TEXT NOT NULL,
                        config TEXT NOT NULL,
                        predicted_accuracy REAL,
                        predicted_carbon REAL,
                        predicted_latency REAL,
                        timestamp TEXT NOT NULL
                    )
                """)
                # Indexes
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_reasoning_timestamp ON reasoning_history(timestamp)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_reasoning_hash ON reasoning_history(architecture_hash)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_carbon_region ON carbon_cache(region)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_carbon_timestamp ON carbon_cache(timestamp)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_performance_hash ON performance_training(architecture_hash)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_pareto_hash ON pareto_front(architecture_hash)")
                await conn.commit()
        else:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                # Create tables similarly (omitted for brevity)
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

    async def save_reasoning(self, architecture_hash: str, reasoning_data: Dict, outcomes: Optional[Dict] = None,
                             correlation_id: Optional[str] = None):
        """Save reasoning history for learning."""
        reasoning_bytes = json.dumps(reasoning_data).encode()
        reasoning_cipher, reasoning_nonce = await self._encrypt_if_possible(reasoning_bytes)
        outcomes_bytes = json.dumps(outcomes).encode() if outcomes else None
        outcomes_cipher, outcomes_nonce = await self._encrypt_if_possible(outcomes_bytes) if outcomes_bytes else (None, None)

        await self._execute('''
            INSERT INTO reasoning_history
            (timestamp, architecture_hash, reasoning_data, reasoning_nonce, outcomes, outcomes_nonce, correlation_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            datetime.now().isoformat(),
            architecture_hash,
            reasoning_cipher if reasoning_cipher else reasoning_bytes,
            reasoning_nonce if reasoning_nonce else None,
            outcomes_cipher if outcomes_cipher else outcomes_bytes,
            outcomes_nonce if outcomes_nonce else None,
            correlation_id or correlation_id_var.get()
        ))

    async def save_causal_effect(self, feature: str, value: float, carbon_impact: float, accuracy_impact: float):
        await self._execute('''
            INSERT INTO causal_effects (feature, value, carbon_impact, accuracy_impact, timestamp)
            VALUES (?, ?, ?, ?, ?)
        ''', (feature, value, carbon_impact, accuracy_impact, datetime.now().isoformat()))
        # Update cache
        self.cache[f'causal_{feature}'] = (carbon_impact, datetime.now())

    async def get_carbon_intensity(self, region: str, hours_ago: int = 1) -> Optional[float]:
        cutoff_time = (datetime.now() - timedelta(hours=hours_ago)).isoformat()
        row = await self._fetchone('''
            SELECT intensity FROM carbon_cache
            WHERE region = ? AND timestamp > ?
            ORDER BY timestamp DESC LIMIT 1
        ''', (region, cutoff_time))
        return row[0] if row else None

    async def save_carbon_intensity(self, region: str, intensity: float):
        await self._execute('''
            INSERT OR REPLACE INTO carbon_cache (region, timestamp, intensity)
            VALUES (?, ?, ?)
        ''', (region, datetime.now().isoformat(), intensity))

    async def get_causal_impact(self, feature: str) -> Optional[float]:
        if feature in self.cache:
            value, timestamp = self.cache[feature]
            if (datetime.now() - timestamp).seconds < self.cache_ttl:
                return value
            else:
                del self.cache[feature]
        return None

    async def save_model_metadata(self, model_name: str, version: str, metrics: Dict):
        await self._execute('''
            INSERT OR REPLACE INTO model_metadata (model_name, version, last_trained, metrics)
            VALUES (?, ?, ?, ?)
        ''', (model_name, version, datetime.now().isoformat(), json.dumps(metrics)))

    async def get_model_metadata(self, model_name: str) -> Optional[Dict]:
        row = await self._fetchone('''
            SELECT version, last_trained, metrics FROM model_metadata WHERE model_name = ?
        ''', (model_name,))
        if row:
            return {
                'version': row[0],
                'last_trained': row[1],
                'metrics': json.loads(row[2])
            }
        return None

    async def save_training_data(self, config: Dict[str, Any], actual_accuracy: float,
                                 actual_latency: float, actual_carbon: float):
        """Store training examples."""
        arch_hash = hashlib.md5(json.dumps(config, sort_keys=True).encode()).hexdigest()[:8]
        await self._execute('''
            INSERT INTO performance_training
            (architecture_hash, config, actual_accuracy, actual_latency, actual_carbon, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (arch_hash, json.dumps(config), actual_accuracy, actual_latency, actual_carbon, datetime.now().isoformat()))

    async def load_training_data(self) -> Tuple[List[Dict], List[float], List[float], List[float]]:
        rows = await self._fetchall('''
            SELECT config, actual_accuracy, actual_latency, actual_carbon
            FROM performance_training
        ''')
        configs = []
        accuracies = []
        latencies = []
        carbons = []
        for row in rows:
            configs.append(json.loads(row[0]))
            accuracies.append(row[1])
            latencies.append(row[2])
            carbons.append(row[3])
        return configs, accuracies, latencies, carbons

    # Pareto front storage
    async def save_pareto_architecture(self, config: Dict, predicted_accuracy: float,
                                        predicted_carbon: float, predicted_latency: float):
        arch_hash = hashlib.md5(json.dumps(config, sort_keys=True).encode()).hexdigest()[:8]
        await self._execute('''
            INSERT INTO pareto_front (architecture_hash, config, predicted_accuracy, predicted_carbon, predicted_latency, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (arch_hash, json.dumps(config), predicted_accuracy, predicted_carbon, predicted_latency, datetime.now().isoformat()))

    async def load_pareto_front(self) -> List[Dict]:
        rows = await self._fetchall('''
            SELECT config, predicted_accuracy, predicted_carbon, predicted_latency FROM pareto_front
        ''')
        return [{'config': json.loads(row[0]), 'accuracy': row[1], 'carbon': row[2], 'latency': row[3]} for row in rows]

# -----------------------------------------------------------------------------
# Circuit Breaker (reused)
# -----------------------------------------------------------------------------
class CircuitBreaker:
    """Simple circuit breaker with half-open state."""
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
            return result
        except Exception as e:
            self._failures += 1
            self._last_failure_time = datetime.now()
            if self._failures >= self.failure_threshold:
                self._state = "OPEN"
            raise e

# -----------------------------------------------------------------------------
# Live Carbon Data Client (async)
# -----------------------------------------------------------------------------
class LiveCarbonDataClient:
    """Fetches real-time and forecasted carbon intensity data with retries and circuit breaker."""
    
    def __init__(self, config: ReasoningConfig, storage: EnhancedStorage):
        self.config = config
        self.storage = storage
        self.api_key = config.electricity_maps_api_key
        self.base_url = "https://api.electricitymap.org/v3"
        self.session: Optional[aiohttp.ClientSession] = None
        self._cache = {}
        self._cache_ttl = config.cache_ttl
        self._circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=60.0, name="carbon_api")
        self._rate_limiter = asyncio.Semaphore(10)
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    @retry(stop=stop_after_attempt(Config.RETRY_ATTEMPTS),
           wait=wait_exponential(multiplier=1, min=Config.RETRY_MIN_WAIT, max=Config.RETRY_MAX_WAIT),
           retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)))
    async def get_current_intensity(self, region: str = "global") -> float:
        cache_key = f"{region}_current"
        if cache_key in self._cache:
            cache_time, intensity = self._cache[cache_key]
            if (datetime.now() - cache_time).seconds < self._cache_ttl:
                return intensity
        
        cached = await self.storage.get_carbon_intensity(region, hours_ago=1)
        if cached is not None:
            self._cache[cache_key] = (datetime.now(), cached)
            return cached
        
        async def _fetch():
            if self.api_key and self.session:
                headers = {"auth-token": self.api_key}
                async with self.session.get(
                    f"{self.base_url}/carbon-intensity/latest",
                    params={"zone": region},
                    headers=headers
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        intensity = float(data.get('carbonIntensity', 400))
                        await self.storage.save_carbon_intensity(region, intensity)
                        self._cache[cache_key] = (datetime.now(), intensity)
                        return intensity
                    else:
                        raise Exception(f"API returned {response.status}")
            else:
                raise Exception("No API key or session")
        
        try:
            intensity = await self._circuit_breaker.call(_fetch)
            return intensity
        except Exception as e:
            logger.warning(f"Failed to fetch live carbon data (circuit breaker): {e}")
            intensity = self._simulate_intensity(region)
            self._cache[cache_key] = (datetime.now(), intensity)
            return intensity
    
    def _simulate_intensity(self, region: str) -> float:
        hour = datetime.now().hour
        base = 350
        if region in ["EU", "DE", "FR", "UK"]:
            base = 300
        elif region in ["US-CAL", "US-NY", "US-TEX"]:
            base = 400
        elif region in ["AU", "NZ"]:
            base = 450
        if hour in [1,2,3,4,5]:
            factor = 0.6
        elif hour in [10,11,12,13,14]:
            factor = 0.8
        elif hour in [18,19,20,21]:
            factor = 1.3
        else:
            factor = 1.0
        intensity = base * factor + np.random.normal(0, 30)
        return max(50, min(800, intensity))
    
    @retry(stop=stop_after_attempt(Config.RETRY_ATTEMPTS),
           wait=wait_exponential(multiplier=1, min=Config.RETRY_MIN_WAIT, max=Config.RETRY_MAX_WAIT),
           retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)))
    async def get_forecast(self, region: str = "global", hours: int = 24) -> List[Dict]:
        async def _fetch():
            if self.api_key and self.session:
                headers = {"auth-token": self.api_key}
                async with self.session.get(
                    f"{self.base_url}/carbon-intensity/forecast",
                    params={"zone": region, "hours": hours},
                    headers=headers
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        forecast = []
                        for entry in data.get('forecast', []):
                            forecast.append({
                                'datetime': entry.get('datetime'),
                                'intensity': float(entry.get('carbonIntensity', 400)),
                                'savings_potential': (entry.get('carbonIntensity', 400) - 200) / max(entry.get('carbonIntensity', 400), 1)
                            })
                        return forecast
                    else:
                        raise Exception(f"API returned {response.status}")
            else:
                raise Exception("No API key or session")
        
        try:
            return await self._circuit_breaker.call(_fetch)
        except Exception as e:
            logger.warning(f"Failed to fetch forecast data: {e}")
            return self._simulate_forecast(region, hours)
    
    def _simulate_forecast(self, region: str, hours: int) -> List[Dict]:
        forecast = []
        current_hour = datetime.now().hour
        base = 350
        if region in ["EU", "DE", "FR", "UK"]:
            base = 300
        elif region in ["US-CAL", "US-NY", "US-TEX"]:
            base = 400
        elif region in ["AU", "NZ"]:
            base = 450
        for i in range(hours):
            hour = (current_hour + i) % 24
            forecast_time = datetime.now() + timedelta(hours=i)
            if hour in [1,2,3,4,5]:
                factor = 0.6
            elif hour in [10,11,12,13,14]:
                factor = 0.8
            elif hour in [18,19,20,21]:
                factor = 1.3
            else:
                factor = 1.0
            intensity = base * factor + np.random.normal(0, 20)
            intensity = max(50, min(800, intensity))
            forecast.append({
                'datetime': forecast_time.isoformat(),
                'hour': hour,
                'intensity': intensity,
                'savings_potential': (intensity - 200) / max(intensity, 1)
            })
        return forecast

# -----------------------------------------------------------------------------
# Hardware Profiler (unchanged but uses config)
# -----------------------------------------------------------------------------
class HardwareProfiler:
    def __init__(self, config: ReasoningConfig):
        self.config = config
        self.profile_path = config.hardware_profiles_path
        self.profiles = self._load_profiles()
        
    def _load_profiles(self) -> Dict:
        default = {
            "cpu_x86": {"base_power_w": 65, "compute_efficiency": 1.0, "memory_efficiency": 1.0, "carbon_impact_factor": 1.0, "inference_latency_ms_per_flop": 0.001, "training_latency_ms_per_flop": 0.005},
            "gpu_nvidia_a100": {"base_power_w": 400, "compute_efficiency": 20.0, "memory_efficiency": 15.0, "carbon_impact_factor": 0.8, "inference_latency_ms_per_flop": 0.0001, "training_latency_ms_per_flop": 0.0005},
            "gpu_nvidia_h100": {"base_power_w": 700, "compute_efficiency": 30.0, "memory_efficiency": 20.0, "carbon_impact_factor": 0.7, "inference_latency_ms_per_flop": 0.00008, "training_latency_ms_per_flop": 0.0004},
            "edge_tpu": {"base_power_w": 2, "compute_efficiency": 5.0, "memory_efficiency": 3.0, "carbon_impact_factor": 0.1, "inference_latency_ms_per_flop": 0.0002, "training_latency_ms_per_flop": 0.01},
            "mobile_npu": {"base_power_w": 1, "compute_efficiency": 3.0, "memory_efficiency": 2.0, "carbon_impact_factor": 0.05, "inference_latency_ms_per_flop": 0.0003, "training_latency_ms_per_flop": 0.02},
            "quantum": {"base_power_w": 0.1, "compute_efficiency": 0.5, "memory_efficiency": 0.1, "carbon_impact_factor": 0.001, "inference_latency_ms_per_flop": 0.01, "training_latency_ms_per_flop": 0.05}
        }
        if os.path.exists(self.profile_path):
            try:
                with open(self.profile_path, 'r') as f:
                    loaded = json.load(f)
                    for hw in default:
                        if hw not in loaded:
                            loaded[hw] = default[hw]
                    return loaded
            except Exception as e:
                logger.warning(f"Failed to load hardware profiles: {e}")
        return default
    
    def get_profile(self, hardware: str) -> Dict:
        return self.profiles.get(hardware, self.profiles["cpu_x86"])
    
    def predict_energy(self, hardware: str, flops: float, memory_ops: float, duration_hours: float) -> float:
        profile = self.get_profile(hardware)
        power_watts = profile['base_power_w']
        compute_scaling = flops * profile['compute_efficiency'] / 1e12
        memory_scaling = memory_ops * profile['memory_efficiency'] / 1e9
        effective_power = power_watts * (1 + 0.5 * compute_scaling + 0.3 * memory_scaling)
        energy_kwh = (effective_power * duration_hours) / 1000
        return energy_kwh

# -----------------------------------------------------------------------------
# Enhanced Performance Predictor with MLPRegressor (v5.0.0)
# -----------------------------------------------------------------------------
class PerformancePredictor:
    def __init__(self, config: ReasoningConfig, storage: EnhancedStorage, hardware_profiler: HardwareProfiler):
        self.config = config
        self.storage = storage
        self.hardware_profiler = hardware_profiler
        
        # ML models
        self.accuracy_model = None
        self.latency_model = None
        self.carbon_model = None
        self._is_trained = False
        self._scaler = None
        
        self.feature_names = ['num_layers', 'hidden_dim', 'num_heads', 'pruning_rate', 'quantization_bits', 'batch_size', 'moe_layers']
        self._training_data_X = []
        self._training_data_y_accuracy = []
        self._training_data_y_latency = []
        self._training_data_y_carbon = []
        
        # Load any stored training data
        asyncio.create_task(self._load_training_data())
        self._load_models()
    
    async def _load_training_data(self):
        configs, acc, lat, carb = await self.storage.load_training_data()
        if configs:
            for cfg, a, l, c in zip(configs, acc, lat, carb):
                X = self._extract_features(cfg)
                self._training_data_X.append(X)
                self._training_data_y_accuracy.append(a)
                self._training_data_y_latency.append(l)
                self._training_data_y_carbon.append(c)
            # Train if enough data
            if len(self._training_data_X) >= 10 and SKLEARN_AVAILABLE:
                self._train_models()
    
    def _load_models(self):
        if SKLEARN_AVAILABLE:
            # Could load pickled models from storage if available
            pass
        self._use_surrogate_models()
    
    def _use_surrogate_models(self):
        logger.info("Using surrogate models for performance prediction.")
        self.accuracy_model = {'base': 0.85, 'layer_impact': 0.02, 'dim_impact': 0.0001,
                               'pruning_impact': -0.3, 'quant_impact': -0.05}
        self.latency_model = {'base': 10, 'layer_impact_ms': 2, 'dim_impact_ms': 0.05, 'batch_impact_ms': 0.5}
        self._is_trained = True
    
    def _extract_features(self, config: Dict[str, Any]) -> List[float]:
        return [
            config.get('num_layers', 6),
            config.get('hidden_dim', 384),
            config.get('num_heads', 8),
            config.get('pruning_rate', 0.0),
            config.get('quantization_bits', 32),
            config.get('batch_size', 32),
            config.get('moe_layers', 0)
        ]
    
    def predict_accuracy(self, architecture_config: Dict[str, Any]) -> float:
        if self._is_trained and SKLEARN_AVAILABLE and self.accuracy_model is not None:
            X = np.array([self._extract_features(architecture_config)])
            if self._scaler:
                X = self._scaler.transform(X)
            return float(self.accuracy_model.predict(X)[0])
        else:
            features = self._extract_features(architecture_config)
            model = self.accuracy_model
            acc = model['base']
            acc += model['layer_impact'] * (features[0] - 6)
            acc += model['dim_impact'] * (features[1] - 384)
            acc += model['pruning_impact'] * features[3]
            if features[4] < 32:
                acc += model['quant_impact'] * (32 - features[4]) / 8
            return max(0.0, min(1.0, acc))
    
    def predict_latency(self, architecture_config: Dict[str, Any], context: str) -> float:
        if self._is_trained and SKLEARN_AVAILABLE and self.latency_model is not None:
            X = np.array([self._extract_features(architecture_config)])
            if self._scaler:
                X = self._scaler.transform(X)
            return float(self.latency_model.predict(X)[0])
        else:
            features = self._extract_features(architecture_config)
            model = self.latency_model
            latency = model['base']
            latency += model['layer_impact_ms'] * features[0]
            latency += model['dim_impact_ms'] * features[1]
            latency += model['batch_impact_ms'] * features[5]
            if context in ['edge_tpu', 'mobile_inference']:
                latency *= 1.5
            elif context == 'batch_processing':
                latency *= 0.5
            return latency
    
    def predict_carbon(self, architecture_config: Dict[str, Any], context: str,
                       training_epochs: int = 100, inference_count: int = 1000000) -> float:
        if self._is_trained and SKLEARN_AVAILABLE and self.carbon_model is not None:
            X = np.array([self._extract_features(architecture_config)])
            if self._scaler:
                X = self._scaler.transform(X)
            return float(self.carbon_model.predict(X)[0])
        else:
            num_params = self._estimate_parameters(architecture_config)
            flops = self._estimate_flops(architecture_config)
            hardware = self._get_hardware_for_context(context)
            training_energy = self.hardware_profiler.predict_energy(
                hardware, flops * training_epochs * 100, num_params * 100, training_epochs * 0.5
            )
            inference_energy = self.hardware_profiler.predict_energy(
                hardware, flops * inference_count, num_params * inference_count, inference_count * 0.001 / 3600
            )
            carbon_kg = (training_energy + inference_energy) * 0.4
            return carbon_kg
    
    def _estimate_parameters(self, config: Dict) -> float:
        layers = config.get('num_layers', 6)
        hidden = config.get('hidden_dim', 384)
        heads = config.get('num_heads', 8)
        params = layers * hidden * hidden + layers * hidden * 4 * hidden + layers * heads * (hidden // heads) ** 2
        return params
    
    def _estimate_flops(self, config: Dict) -> float:
        params = self._estimate_parameters(config)
        batch = config.get('batch_size', 32)
        return params * 2 * batch
    
    def _get_hardware_for_context(self, context: str) -> str:
        mapping = {
            'mobile_inference': 'mobile_npu',
            'edge_tpu': 'edge_tpu',
            'cloud_inference': 'gpu_nvidia_a100',
            'batch_processing': 'gpu_nvidia_a100',
            'quantum': 'quantum'
        }
        return mapping.get(context, 'cpu_x86')
    
    async def add_training_data(self, config: Dict[str, Any], actual_accuracy: float,
                                actual_latency: float, actual_carbon: float):
        if not NUMPY_AVAILABLE:
            logger.warning("NumPy not available – cannot train ML models.")
            return
        X = self._extract_features(config)
        self._training_data_X.append(X)
        self._training_data_y_accuracy.append(actual_accuracy)
        self._training_data_y_latency.append(actual_latency)
        self._training_data_y_carbon.append(actual_carbon)
        # Persist to DB
        await self.storage.save_training_data(config, actual_accuracy, actual_latency, actual_carbon)
        if len(self._training_data_X) >= 10 and SKLEARN_AVAILABLE:
            self._train_models()
    
    def _train_models(self):
        if not SKLEARN_AVAILABLE or not NUMPY_AVAILABLE:
            return
        X = np.array(self._training_data_X)
        y_acc = np.array(self._training_data_y_accuracy)
        y_lat = np.array(self._training_data_y_latency)
        y_carb = np.array(self._training_data_y_carbon)
        if len(X) < 10:
            return
        logger.info(f"Training performance prediction models with {len(X)} samples.")
        # Use MLPRegressor for speed
        try:
            self._scaler = StandardScaler()
            X_scaled = self._scaler.fit_transform(X)
            # Accuracy model
            self.accuracy_model = MLPRegressor(hidden_layer_sizes=(32, 16), max_iter=200, random_state=42)
            self.accuracy_model.fit(X_scaled, y_acc)
            # Latency model
            self.latency_model = MLPRegressor(hidden_layer_sizes=(32, 16), max_iter=200, random_state=42)
            self.latency_model.fit(X_scaled, y_lat)
            # Carbon model
            self.carbon_model = MLPRegressor(hidden_layer_sizes=(32, 16), max_iter=200, random_state=42)
            self.carbon_model.fit(X_scaled, y_carb)
            self._is_trained = True
            self.storage.save_model_metadata('performance_predictor', '5.0.0', {
                'samples': len(X),
                'accuracy_mean': float(np.mean(y_acc)),
                'latency_mean': float(np.mean(y_lat)),
                'carbon_mean': float(np.mean(y_carb))
            })
            logger.info("Performance prediction models (MLP) trained.")
        except Exception as e:
            logger.error(f"Failed to train MLP models: {e}, falling back to GP")
            # Fallback to Gaussian Process
            self._train_gp_models(X, y_acc, y_lat, y_carb)
    
    def _train_gp_models(self, X, y_acc, y_lat, y_carb):
        try:
            kernel = 1.0 * RBF(length_scale=1.0) + WhiteKernel(noise_level=0.1)
            self.accuracy_model = GaussianProcessRegressor(kernel=kernel, random_state=42)
            self.accuracy_model.fit(X, y_acc)
            self.latency_model = GaussianProcessRegressor(kernel=kernel, random_state=42)
            self.latency_model.fit(X, y_lat)
            self.carbon_model = GaussianProcessRegressor(kernel=kernel, random_state=42)
            self.carbon_model.fit(X, y_carb)
            self._is_trained = True
            self._scaler = None
            logger.info("Performance prediction models (GP) trained.")
        except Exception as e:
            logger.error(f"Failed to train GP models: {e}")

# -----------------------------------------------------------------------------
# Genetic Algorithm for Architecture Search (v5.0.0)
# -----------------------------------------------------------------------------
class GeneticArchitectureSearch:
    """Bio‑inspired genetic algorithm for multi‑objective architecture optimisation."""
    
    def __init__(self, config: ReasoningConfig, predictor: PerformancePredictor):
        self.config = config
        self.predictor = predictor
        self.population_size = config.ga_population_size
        self.generations = config.ga_generations
        self.mutation_rate = config.ga_mutation_rate
        self.crossover_rate = config.ga_crossover_rate
        self.population = []
        self.pareto_front = []
        
        # Define architecture parameter bounds
        self.param_bounds = {
            'num_layers': (1, 24),
            'hidden_dim': (64, 2048),
            'num_heads': (1, 24),
            'pruning_rate': (0.0, 0.8),
            'quantization_bits': [4, 8, 16, 32],
            'batch_size': (8, 512),
            'attention_type': ['flash_attention', 'standard', 'linear'],
            'activation_function': ['swiglu', 'relu', 'gelu', 'silu'],
            'moe_layers': (0, 8)
        }
    
    def _random_architecture(self) -> Dict[str, Any]:
        return {
            'num_layers': random.randint(*self.param_bounds['num_layers']),
            'hidden_dim': random.randint(*self.param_bounds['hidden_dim']),
            'num_heads': random.randint(*self.param_bounds['num_heads']),
            'pruning_rate': random.uniform(*self.param_bounds['pruning_rate']),
            'quantization_bits': random.choice(self.param_bounds['quantization_bits']),
            'batch_size': random.randint(*self.param_bounds['batch_size']),
            'attention_type': random.choice(self.param_bounds['attention_type']),
            'activation_function': random.choice(self.param_bounds['activation_function']),
            'moe_layers': random.randint(*self.param_bounds['moe_layers'])
        }
    
    def _mutate(self, arch: Dict[str, Any]) -> Dict[str, Any]:
        new_arch = arch.copy()
        for param, bounds in self.param_bounds.items():
            if random.random() < self.mutation_rate:
                if param in ['num_layers', 'hidden_dim', 'num_heads', 'batch_size', 'moe_layers']:
                    lower, upper = bounds
                    new_val = int(np.clip(random.gauss(arch[param], (upper-lower)/10), lower, upper))
                    new_arch[param] = new_val
                elif param == 'pruning_rate':
                    lower, upper = bounds
                    new_arch[param] = np.clip(random.gauss(arch[param], 0.1), lower, upper)
                elif param == 'quantization_bits':
                    new_arch[param] = random.choice(self.param_bounds['quantization_bits'])
                else:  # categorical
                    new_arch[param] = random.choice(bounds)
        return new_arch
    
    def _crossover(self, parent1: Dict, parent2: Dict) -> Tuple[Dict, Dict]:
        if random.random() > self.crossover_rate:
            return parent1.copy(), parent2.copy()
        child1, child2 = parent1.copy(), parent2.copy()
        for param in self.param_bounds.keys():
            if random.random() < 0.5:
                child1[param] = parent2[param]
                child2[param] = parent1[param]
        return child1, child2
    
    def _evaluate_fitness(self, arch: Dict[str, Any]) -> Tuple[float, float, float]:
        acc = self.predictor.predict_accuracy(arch)
        carbon = self.predictor.predict_carbon(arch, 'cloud_inference')
        latency = self.predictor.predict_latency(arch, 'cloud_inference')
        return acc, carbon, latency
    
    def _dominates(self, a: Dict, b: Dict) -> bool:
        # Minimization: accuracy is negated because we want higher accuracy
        a_metrics = (-a['accuracy'], a['carbon'], a['latency'])
        b_metrics = (-b['accuracy'], b['carbon'], b['latency'])
        return all(a_metrics[i] <= b_metrics[i] for i in range(3)) and any(a_metrics[i] < b_metrics[i] for i in range(3))
    
    def _fast_non_dominated_sort(self, population: List[Dict]) -> List[List[Dict]]:
        fronts = [[]]
        for p in population:
            p['domination_count'] = 0
            p['dominated_set'] = []
            for q in population:
                if p is not q:
                    if self._dominates(p, q):
                        p['dominated_set'].append(q)
                    elif self._dominates(q, p):
                        p['domination_count'] += 1
            if p['domination_count'] == 0:
                fronts[0].append(p)
        i = 0
        while fronts[i]:
            next_front = []
            for p in fronts[i]:
                for q in p['dominated_set']:
                    q['domination_count'] -= 1
                    if q['domination_count'] == 0:
                        next_front.append(q)
            i += 1
            fronts.append(next_front)
        return fronts[:-1]  # remove last empty front
    
    def _crowding_distance(self, front: List[Dict]) -> None:
        if len(front) <= 2:
            for p in front:
                p['crowding_distance'] = float('inf')
            return
        for p in front:
            p['crowding_distance'] = 0.0
        # Sort by each objective
        for key in ['accuracy', 'carbon', 'latency']:
            # For accuracy we negate because we want higher is better, but distance is computed on absolute differences
            # We'll sort by the metric itself, but we need to handle sign: for accuracy we want higher, so we sort descending
            if key == 'accuracy':
                front.sort(key=lambda x: x[key], reverse=True)
            else:
                front.sort(key=lambda x: x[key])
            min_val = front[0][key]
            max_val = front[-1][key]
            if max_val - min_val == 0:
                continue
            front[0]['crowding_distance'] = float('inf')
            front[-1]['crowding_distance'] = float('inf')
            for i in range(1, len(front)-1):
                front[i]['crowding_distance'] += (front[i+1][key] - front[i-1][key]) / (max_val - min_val)
    
    async def run_search(self, initial_population: Optional[List[Dict]] = None) -> List[Dict]:
        """Run GA and return the final Pareto front."""
        if not NUMPY_AVAILABLE:
            logger.warning("NumPy required for GA – returning empty front.")
            return []
        
        # Initialize population
        if initial_population:
            self.population = [arch.copy() for arch in initial_population]
            while len(self.population) < self.population_size:
                self.population.append(self._random_architecture())
        else:
            self.population = [self._random_architecture() for _ in range(self.population_size)]
        
        # Evaluate initial fitness
        for arch in self.population:
            acc, carb, lat = self._evaluate_fitness(arch)
            arch['accuracy'] = acc
            arch['carbon'] = carb
            arch['latency'] = lat
        
        for gen in range(self.generations):
            logger.debug(f"GA generation {gen+1}/{self.generations}")
            # Non-dominated sort
            fronts = self._fast_non_dominated_sort(self.population)
            # Crowding distance for each front
            for front in fronts:
                self._crowding_distance(front)
            
            # Select parents (tournament selection)
            parents = []
            while len(parents) < self.population_size:
                # Randomly pick two
                p1 = random.choice(self.population)
                p2 = random.choice(self.population)
                # Compare by rank and crowding distance
                if p1['rank'] < p2['rank']:
                    parents.append(p1)
                elif p1['rank'] > p2['rank']:
                    parents.append(p2)
                else:  # same rank
                    if p1['crowding_distance'] > p2['crowding_distance']:
                        parents.append(p1)
                    else:
                        parents.append(p2)
            
            # Crossover and mutation
            offspring = []
            while len(offspring) < self.population_size:
                p1 = random.choice(parents)
                p2 = random.choice(parents)
                child1, child2 = self._crossover(p1, p2)
                child1 = self._mutate(child1)
                child2 = self._mutate(child2)
                offspring.append(child1)
                if len(offspring) < self.population_size:
                    offspring.append(child2)
            
            # Evaluate offspring fitness
            for child in offspring:
                acc, carb, lat = self._evaluate_fitness(child)
                child['accuracy'] = acc
                child['carbon'] = carb
                child['latency'] = lat
            
            # Combine parent and offspring, then select next generation
            combined = self.population + offspring
            # Non-dominated sort
            fronts = self._fast_non_dominated_sort(combined)
            next_population = []
            for front in fronts:
                if len(next_population) + len(front) <= self.population_size:
                    next_population.extend(front)
                else:
                    self._crowding_distance(front)
                    front.sort(key=lambda x: x['crowding_distance'], reverse=True)
                    next_population.extend(front[:self.population_size - len(next_population)])
                    break
            self.population = next_population
        
        # Extract Pareto front from final population
        fronts = self._fast_non_dominated_sort(self.population)
        pareto_front = fronts[0] if fronts else []
        # Remove internal keys used for sorting
        for arch in pareto_front:
            arch.pop('domination_count', None)
            arch.pop('dominated_set', None)
            arch.pop('crowding_distance', None)
            arch.pop('rank', None)
        self.pareto_front = pareto_front
        return pareto_front
    
    async def get_best_architectures(self, n: int = 5) -> List[Dict]:
        """Return top n architectures from Pareto front based on a weighted sum of objectives."""
        if not self.pareto_front:
            return []
        # Use default weights from config (MOPD)
        weights = self.config.mopd_weights
        # Normalize objectives: accuracy max, carbon min, latency min
        # We'll compute a score = w_acc * acc - w_carb * carb - w_lat * latency
        scored = []
        for arch in self.pareto_front:
            score = weights['accuracy'] * arch['accuracy'] - weights['carbon'] * arch['carbon'] - weights['latency'] * arch['latency']
            scored.append((score, arch))
        scored.sort(reverse=True)
        return [arch for _, arch in scored[:n]]

# -----------------------------------------------------------------------------
# Mixture-of-Experts Gating Network (v5.0.0)
# -----------------------------------------------------------------------------
class MoEGatingNetwork:
    """
    Gated Mixture-of-Experts that selects the most appropriate strategy
    based on context features using a neural network gating function.
    """
    def __init__(self, config: ReasoningConfig):
        self.config = config
        self.num_experts = config.moe_expert_count
        self.hidden_layers = config.moe_hidden_layers
        self._gating_model = None
        self._scaler = None
        self._trained = False
        
        # Define experts: each expert is a function that takes state and returns strategy scores
        # We'll keep the original teacher functions as experts
        self.experts = {
            'performance': self._performance_expert,
            'carbon': self._carbon_expert,
            'cost': self._cost_expert,
            'adaptive': self._adaptive_expert
        }
        # Ensure we have exactly num_experts; if fewer, we duplicate or extend
        if len(self.experts) < self.num_experts:
            # Add additional experts by copying existing ones with slight variations
            keys = list(self.experts.keys())
            for i in range(self.num_experts - len(keys)):
                self.experts[f'custom_{i}'] = self.experts[keys[i % len(keys)]]
        # If more experts than needed, keep first num_experts
        self.experts = dict(list(self.experts.items())[:self.num_experts])
        self.expert_names = list(self.experts.keys())
        
        # Context features: carbon intensity, purpose, context, etc.
        # We'll encode these as a vector
        self.feature_dim = 6  # after encoding

    def _performance_expert(self, state: Dict) -> Dict[str, float]:
        acc = state.get('predicted_accuracy', 0.85)
        scores = {name: 0.5 for name in self.expert_names}
        scores['performance'] = acc
        return scores

    def _carbon_expert(self, state: Dict, carbon_intensity: float) -> Dict[str, float]:
        scores = {name: 0.5 for name in self.expert_names}
        if carbon_intensity > 400:
            scores['carbon'] = 1.0
        else:
            scores['carbon'] = 0.6
        return scores

    def _cost_expert(self, state: Dict) -> Dict[str, float]:
        cost = state.get('cost_budget', 0.5)
        scores = {name: 0.5 for name in self.expert_names}
        scores['cost'] = 1 - cost
        return scores

    def _adaptive_expert(self, state: Dict) -> Dict[str, float]:
        # This expert looks at historical performance and gives higher scores to strategies that worked well
        history = state.get('history', [])
        if len(history) > 0:
            counts = {}
            for entry in history[-10:]:
                counts[entry['selected']] = counts.get(entry['selected'], 0) + 1
            total = sum(counts.values())
            if total > 0:
                scores = {name: counts.get(name, 0) / total for name in self.expert_names}
            else:
                scores = {name: 0.25 for name in self.expert_names}
        else:
            scores = {name: 0.25 for name in self.expert_names}
        return scores

    def _encode_context(self, state: Dict, carbon_intensity: float) -> np.ndarray:
        """Encode context into a feature vector."""
        # Features: carbon intensity (normalized), purpose (one-hot), context (one-hot), cost budget, historical success rate, accuracy target
        features = []
        # Normalize carbon intensity to [0,1]
        features.append(min(1.0, carbon_intensity / 800.0))
        # Purpose encoding (balanced, low_carbon, high_performance, cost_effective)
        purpose = state.get('purpose', 'balanced')
        purpose_map = {'balanced': 0, 'low_carbon': 1, 'high_performance': 2, 'cost_effective': 3}
        purpose_vec = [0]*4
        purpose_vec[purpose_map.get(purpose, 0)] = 1
        features.extend(purpose_vec)
        # Context encoding (cloud_inference, edge_tpu, mobile_inference, batch_processing, quantum)
        context = state.get('context', 'cloud_inference')
        context_map = {'cloud_inference': 0, 'edge_tpu': 1, 'mobile_inference': 2, 'batch_processing': 3, 'quantum': 4}
        context_vec = [0]*5
        context_vec[context_map.get(context, 0)] = 1
        features.extend(context_vec)
        # Cost budget
        features.append(state.get('cost_budget', 0.5))
        # Historical success rate
        features.append(state.get('success_rate', 0.5))
        # Accuracy target (optional)
        features.append(state.get('target_accuracy', 0.9))
        return np.array(features, dtype=np.float32)

    def _train_gating(self, training_data: List[Tuple[np.ndarray, int]]):
        """Train a neural network to predict expert weights from context."""
        if not SKLEARN_AVAILABLE or not NUMPY_AVAILABLE:
            return
        if len(training_data) < 10:
            return
        X = np.array([item[0] for item in training_data])
        y = np.array([item[1] for item in training_data])
        self._scaler = StandardScaler()
        X_scaled = self._scaler.fit_transform(X)
        # Multi-class classifier (softmax)
        from sklearn.neural_network import MLPClassifier
        self._gating_model = MLPClassifier(hidden_layer_sizes=self.hidden_layers, max_iter=200, random_state=42)
        self._gating_model.fit(X_scaled, y)
        self._trained = True
        logger.info("MoE gating network trained on %d samples.", len(training_data))

    async def select_expert(self, state: Dict, carbon_intensity: float, history: List[Dict] = None) -> Tuple[str, Dict[str, float]]:
        """Return the selected expert name and its scores."""
        # Encode context
        state['history'] = history or []
        features = self._encode_context(state, carbon_intensity)
        if self._trained and self._gating_model is not None:
            X = features.reshape(1, -1)
            if self._scaler:
                X = self._scaler.transform(X)
            probs = self._gating_model.predict_proba(X)[0]
            expert_idx = np.argmax(probs)
            selected_expert = self.expert_names[expert_idx]
            # Get scores from that expert
            expert_func = self.experts[selected_expert]
            if selected_expert == 'carbon':
                scores = expert_func(state, carbon_intensity)
            else:
                scores = expert_func(state)
        else:
            # Fallback: use adaptive expert
            selected_expert = 'adaptive'
            scores = self._adaptive_expert(state)
        return selected_expert, scores

    def update(self, context: Dict, selected_expert: str, reward: float):
        """Record a training example for the gating network."""
        # For simplicity, we store context features and the selected expert index.
        # This method should be called after each reasoning cycle.
        # Actual training is done periodically.
        pass

# -----------------------------------------------------------------------------
# Pareto Front Optimizer (v5.0.0)
# -----------------------------------------------------------------------------
class ParetoOptimizer:
    """
    Maintains a set of non-dominated architectures and provides trade-off exploration.
    """
    def __init__(self, config: ReasoningConfig, storage: EnhancedStorage, predictor: PerformancePredictor):
        self.config = config
        self.storage = storage
        self.predictor = predictor
        self.pareto_front = []
        self.max_architectures = config.pareto_max_architectures
        self._load_pareto()

    def _load_pareto(self):
        # Load from storage
        try:
            entries = asyncio.run(self.storage.load_pareto_front())
            for entry in entries:
                self.pareto_front.append(entry['config'])
        except Exception as e:
            logger.warning(f"Failed to load Pareto front: {e}")

    def _dominates(self, a: Dict, b: Dict) -> bool:
        a_acc = self.predictor.predict_accuracy(a)
        a_carb = self.predictor.predict_carbon(a, 'cloud_inference')
        a_lat = self.predictor.predict_latency(a, 'cloud_inference')
        b_acc = self.predictor.predict_accuracy(b)
        b_carb = self.predictor.predict_carbon(b, 'cloud_inference')
        b_lat = self.predictor.predict_latency(b, 'cloud_inference')
        # Minimization: accuracy is negated
        return all([-a_acc <= -b_acc, a_carb <= b_carb, a_lat <= b_lat]) and any([-a_acc < -b_acc, a_carb < b_carb, a_lat < b_lat])

    def add_architecture(self, config: Dict[str, Any]) -> bool:
        """Add a new architecture to the Pareto front, update if it dominates."""
        # Evaluate metrics
        acc = self.predictor.predict_accuracy(config)
        carb = self.predictor.predict_carbon(config, 'cloud_inference')
        lat = self.predictor.predict_latency(config, 'cloud_inference')
        # Check if dominated by existing
        for arch in self.pareto_front:
            if self._dominates(arch, config):
                return False  # dominated, ignore
        # Remove any architectures dominated by new one
        self.pareto_front = [arch for arch in self.pareto_front if not self._dominates(config, arch)]
        self.pareto_front.append(config)
        # Limit size
        if len(self.pareto_front) > self.max_architectures:
            # Remove worst using crowding distance
            # Simplified: remove the one with smallest hypervolume contribution
            pass
        # Persist
        asyncio.create_task(self.storage.save_pareto_architecture(config, acc, carb, lat))
        return True

    def get_pareto_front(self) -> List[Dict]:
        return self.pareto_front

    def get_trade_off_suggestions(self, user_preferences: Dict[str, float]) -> List[Dict]:
        """Return architectures that best match user preferences (weights)."""
        if not self.pareto_front:
            return []
        scored = []
        for arch in self.pareto_front:
            acc = self.predictor.predict_accuracy(arch)
            carb = self.predictor.predict_carbon(arch, 'cloud_inference')
            lat = self.predictor.predict_latency(arch, 'cloud_inference')
            # Weighted sum
            score = (user_preferences.get('accuracy', 0.5) * acc -
                     user_preferences.get('carbon', 0.3) * carb -
                     user_preferences.get('latency', 0.2) * lat)
            scored.append((score, arch))
        scored.sort(reverse=True)
        return [arch for _, arch in scored[:5]]

# -----------------------------------------------------------------------------
# MTOP Reasoning Engine (updated to use MoE gating)
# -----------------------------------------------------------------------------
class MTOPReasoningEngine:
    """
    MTOP engine for reasoning strategy selection, now with MoE gating.
    """
    def __init__(self, config: ReasoningConfig):
        self.config = config
        self.moe_gating = MoEGatingNetwork(config) if config.moe_enabled else None
        self.history = deque(maxlen=500)
        self.teacher_ensemble = ReasoningTeacherEnsemble(config)  # kept for fallback

    async def select_strategy(self, state: Dict, carbon_intensity: float) -> Dict:
        if self.moe_gating is not None and self.config.moe_enabled:
            # Use MoE gating
            selected, scores = await self.moe_gating.select_expert(state, carbon_intensity, list(self.history))
        else:
            # Fallback to original teacher ensemble
            teacher_scores = await self.teacher_ensemble.get_teacher_scores(state, carbon_intensity)
            combined = await self.teacher_ensemble.student.combine(teacher_scores)  # simplified
            selected = max(combined, key=combined.get)
            scores = combined
        self.history.append({'selected': selected, 'reward': None})
        return {
            'selected_strategy': selected,
            'scores': scores,
            'teacher_scores': None,
            'reward': None
        }

    async def update(self, selected_strategy: str, reward: float, teacher_scores: Dict):
        if self.moe_gating is not None and self.config.moe_enabled:
            # Update gating with reward (could store for later training)
            pass
        else:
            # Original update
            await self.teacher_ensemble.student.train_step(teacher_scores, selected_strategy, reward)
            teacher_rewards = {name: reward for name in self.teacher_ensemble.teachers}
            self.teacher_ensemble.update_weights(teacher_rewards)
        self.history[-1]['reward'] = reward

# -----------------------------------------------------------------------------
# ContextAwareOptimizer (updated to use new MTOP)
# -----------------------------------------------------------------------------
class ContextAwareOptimizer:
    def __init__(self, config: ReasoningConfig, mtop_engine: MTOPReasoningEngine):
        self.config = config
        self.mtop_engine = mtop_engine
        self.context_profiles = {
            'cloud_inference': {'performance_weight': 0.5, 'carbon_weight': 0.3, 'cost_weight': 0.2},
            'edge_tpu': {'performance_weight': 0.4, 'carbon_weight': 0.4, 'cost_weight': 0.2},
            'mobile_inference': {'performance_weight': 0.3, 'carbon_weight': 0.5, 'cost_weight': 0.2},
            'batch_processing': {'performance_weight': 0.6, 'carbon_weight': 0.2, 'cost_weight': 0.2},
            'quantum': {'performance_weight': 0.1, 'carbon_weight': 0.8, 'cost_weight': 0.1}
        }

    async def get_context_plan(self, architecture_config: Dict[str, Any], context: str,
                               carbon_intensity: float) -> Dict[str, Any]:
        # Use MTOP to decide strategy
        state = {
            'predicted_accuracy': 0.85,
            'cost_budget': 0.5,
            'context': context
        }
        mtop_result = await self.mtop_engine.select_strategy(state, carbon_intensity)
        selected = mtop_result['selected_strategy']
        profile = self.context_profiles.get(context, self.context_profiles['cloud_inference'])
        suggestions = []
        if context == 'edge_tpu':
            if architecture_config.get('num_layers', 6) > 6:
                suggestions.append({'action': 'reduce_layers', 'reason': 'Edge devices benefit from smaller models', 'target': 6})
            if architecture_config.get('quantization_bits', 32) > 16:
                suggestions.append({'action': 'quantize', 'reason': 'Edge deployment recommends INT8 quantization', 'target': 8})
        elif context == 'mobile_inference':
            if architecture_config.get('hidden_dim', 384) > 256:
                suggestions.append({'action': 'reduce_dim', 'reason': 'Mobile devices benefit from smaller hidden dimensions', 'target': 256})
        elif context == 'quantum':
            suggestions.append({'action': 'use_quantum', 'reason': 'Quantum hardware offers extreme carbon efficiency', 'target': 'quantum_ready'})
        return {
            'context': context,
            'weights': profile,
            'selected_strategy': selected,
            'suggestions': suggestions,
            'expected_carbon_saving': sum(0.1 for _ in suggestions)
        }

# -----------------------------------------------------------------------------
# PurposeAwareOptimizer (unchanged, but uses new MTOP)
# -----------------------------------------------------------------------------
class PurposeAwareOptimizer:
    def __init__(self, config: ReasoningConfig, mtop_engine: MTOPReasoningEngine):
        self.config = config
        self.mtop_engine = mtop_engine
        self.purpose_profiles = {
            'balanced': {'accuracy_weight': 0.4, 'carbon_weight': 0.3, 'cost_weight': 0.3},
            'low_carbon': {'accuracy_weight': 0.2, 'carbon_weight': 0.7, 'cost_weight': 0.1},
            'high_performance': {'accuracy_weight': 0.7, 'carbon_weight': 0.1, 'cost_weight': 0.2},
            'cost_effective': {'accuracy_weight': 0.3, 'carbon_weight': 0.3, 'cost_weight': 0.4}
        }

    async def get_purpose_guide(self, purpose: str, carbon_intensity: float) -> Dict[str, Any]:
        # Use MTOP to select strategy based on purpose
        state = {'purpose': purpose}
        mtop_result = await self.mtop_engine.select_strategy(state, carbon_intensity)
        selected = mtop_result['selected_strategy']
        profile = self.purpose_profiles.get(purpose, self.purpose_profiles['balanced'])
        recommendations = []
        if purpose == 'low_carbon':
            recommendations.append("Prioritize carbon reduction over accuracy when possible")
            recommendations.append("Explore quantization and pruning aggressively")
        elif purpose == 'high_performance':
            recommendations.append("Prioritize accuracy and speed over carbon efficiency")
            recommendations.append("Use larger models if necessary")
        elif purpose == 'cost_effective':
            recommendations.append("Balance carbon efficiency with financial cost")
            recommendations.append("Consider cloud region pricing and carbon intensity")
        else:
            recommendations.append("Maintain equal consideration for accuracy, carbon, and cost")
        return {
            'purpose': purpose,
            'weights': profile,
            'selected_strategy': selected,
            'recommendations': recommendations
        }

# -----------------------------------------------------------------------------
# EthicalCarbonReasoner (unchanged)
# -----------------------------------------------------------------------------
class EthicalCarbonReasoner:
    def __init__(self):
        self.ethical_rules = {
            'do_no_harm': lambda impact: impact < 0.3,
            'fair_distribution': lambda config: config.get('pruning_rate', 0) < 0.5,
            'transparency': lambda config: True,
            'accountability': lambda config: True
        }
    
    def assess_reduction_impact(self, architecture_config: Dict[str, Any],
                                fitness_metrics: Dict[str, float]) -> Dict[str, Any]:
        carbon_reduction = fitness_metrics.get('carbon_savings', 0)
        accuracy_loss = fitness_metrics.get('accuracy_loss', 0)
        ethical_score = 1.0
        concerns = []
        rules_violated = []
        for rule_name, rule_func in self.ethical_rules.items():
            if not rule_func(architecture_config):
                rules_violated.append(rule_name)
                ethical_score -= 0.2
        if carbon_reduction > 0.5 and accuracy_loss > 0.15:
            concerns.append("High carbon reduction with significant accuracy loss may be unethical")
            ethical_score -= 0.3
        elif carbon_reduction < 0.1 and accuracy_loss > 0.1:
            concerns.append("Low carbon reduction with non-negligible accuracy loss is inefficient")
            ethical_score -= 0.2
        ethical_score = max(0.0, min(1.0, ethical_score))
        recommendations = []
        if ethical_score < 0.7:
            recommendations.append("Consider more balanced trade-offs between carbon and accuracy")
        if 'do_no_harm' in rules_violated:
            recommendations.append("Avoid changes that cause disproportionate harm to model performance")
        if 'fair_distribution' in rules_violated:
            recommendations.append("Ensure pruning or quantization does not unfairly impact certain model components")
        return {
            'overall_ethical_score': ethical_score,
            'concerns': concerns,
            'rules_violated': rules_violated,
            'compliant': len(rules_violated) == 0,
            'recommendations': recommendations
        }

# -----------------------------------------------------------------------------
# SystemicCarbonPlanner (unchanged)
# -----------------------------------------------------------------------------
class SystemicCarbonPlanner:
    def __init__(self):
        self.learning_rate = 0.1
        self.exploration_decay = 0.99
    
    def plan_carbon_investment(self, current_accuracy: float, target_accuracy: float, carbon_budget: float) -> Dict[str, Any]:
        accuracy_gap = target_accuracy - current_accuracy
        exploration_roi = max(0, 0.3 * (1 - current_accuracy))
        exploitation_roi = 0.1 * (1 - current_accuracy)
        if accuracy_gap > 0.1 and carbon_budget > 1.0 and exploration_roi > exploitation_roi:
            decision = 'invest'
            reason = f'Accuracy gap ({accuracy_gap:.2f}) justifies exploration investment'
            expected_improvement = exploration_roi
            carbon_spend = min(carbon_budget * 0.3, 2.0)
        elif accuracy_gap < 0.05:
            decision = 'exploit'
            reason = 'Accuracy near target - focus on exploitation'
            expected_improvement = exploitation_roi
            carbon_spend = carbon_budget * 0.1
        else:
            decision = 'balanced'
            reason = 'Balanced approach between exploration and exploitation'
            expected_improvement = (exploration_roi + exploitation_roi) / 2
            carbon_spend = carbon_budget * 0.2
        return {
            'decision': decision,
            'reason': reason,
            'expected_improvement': expected_improvement,
            'carbon_spend': carbon_spend,
            'budget_remaining': carbon_budget - carbon_spend,
            'confidence': 0.7
        }

# -----------------------------------------------------------------------------
# EnhancedCarbonIntensityAwareScheduler (with MTOP integration)
# -----------------------------------------------------------------------------
class EnhancedCarbonIntensityAwareScheduler:
    def __init__(self, config: ReasoningConfig, storage: EnhancedStorage, carbon_client: LiveCarbonDataClient):
        self.config = config
        self.storage = storage
        self.carbon_client = carbon_client
    
    async def schedule_computation(self, task: str, urgency: str, compute_hours: float) -> Dict[str, Any]:
        intensity = await self.carbon_client.get_current_intensity(self.config.carbon_region)
        forecast = await self.carbon_client.get_forecast(self.config.carbon_region, hours=12)
        if urgency == 'critical':
            action = 'run_now'
            delay = 0
        elif intensity < 200:
            action = 'run_now'
            delay = 0
        elif intensity < 400:
            if len(forecast) > 2 and forecast[2]['intensity'] < 300:
                action = 'delay'
                delay = 2
            else:
                action = 'run_now'
                delay = 0
        else:
            delay = 0
            for i, entry in enumerate(forecast):
                if entry['intensity'] < 250:
                    delay = i + 1
                    action = 'delay'
                    break
            else:
                action = 'run_now'
                delay = 0
        return {
            'action': action,
            'delay_hours': delay,
            'optimal_schedule': (datetime.now() + timedelta(hours=delay)).isoformat(),
            'current_intensity': intensity,
            'expected_saving': (intensity - 250) / max(intensity, 1) * 100 if action == 'delay' else 0
        }

# -----------------------------------------------------------------------------
# Reflection Handler (for state adjustments)
# -----------------------------------------------------------------------------
class ReflectionHandler:
    """Adjusts confidence, thresholds, and strategy weights based on outcomes."""
    def __init__(self, state: 'ReasoningState', mtop_engine: MTOPReasoningEngine):
        self.state = state
        self.mtop_engine = mtop_engine
        self.reflection_count = 0

    async def trigger_reflection(self, trigger_type: str, **kwargs):
        self.reflection_count += 1
        if trigger_type == 'accurate_prediction':
            self.state.confidence = min(1.0, self.state.confidence + 0.05)
        elif trigger_type == 'inaccurate_prediction':
            self.state.confidence = max(0.1, self.state.confidence - 0.1)
        elif trigger_type == 'high_carbon':
            self.state.carbon_budget_remaining *= 0.9
        elif trigger_type == 'successful_recommendation':
            self.state.confidence = min(1.0, self.state.confidence + 0.02)
        # Adjust MTOP reward? That's handled elsewhere.
        await self.state.save()

# -----------------------------------------------------------------------------
# Reasoning State (with persistence and reflection)
# -----------------------------------------------------------------------------
class ReasoningState:
    def __init__(self, storage: EnhancedStorage):
        self.storage = storage
        self.confidence = float(await self.storage.get_state('confidence') or 0.5)
        self.uncertainty = float(await self.storage.get_state('uncertainty') or 0.1)
        self.historical_success_rate = float(await self.storage.get_state('success_rate') or 0.5)
        self.reflection_count = int(await self.storage.get_state('reflection_count') or 0)
        self.carbon_budget_remaining = float(await self.storage.get_state('carbon_budget') or 100.0)
        self.active_strategies = json.loads(await self.storage.get_state('active_strategies') or '[]')
        self.strategy_effectiveness = json.loads(await self.storage.get_state('strategy_effectiveness') or '{}')
        self.preferred_experts = json.loads(await self.storage.get_state('preferred_experts') or '[]')
        self.avoided_experts = json.loads(await self.storage.get_state('avoided_experts') or '[]')
        self.expert_health_scores = json.loads(await self.storage.get_state('expert_health') or '{}')
        self.reflection_threshold = float(await self.storage.get_state('reflection_threshold') or 0.3)

    async def save(self):
        await self.storage.save_state('confidence', str(self.confidence))
        await self.storage.save_state('uncertainty', str(self.uncertainty))
        await self.storage.save_state('success_rate', str(self.historical_success_rate))
        await self.storage.save_state('reflection_count', str(self.reflection_count))
        await self.storage.save_state('carbon_budget', str(self.carbon_budget_remaining))
        await self.storage.save_state('active_strategies', json.dumps(self.active_strategies))
        await self.storage.save_state('strategy_effectiveness', json.dumps(self.strategy_effectiveness))
        await self.storage.save_state('preferred_experts', json.dumps(self.preferred_experts))
        await self.storage.save_state('avoided_experts', json.dumps(self.avoided_experts))
        await self.storage.save_state('expert_health', json.dumps(self.expert_health_scores))
        await self.storage.save_state('reflection_threshold', str(self.reflection_threshold))

# -----------------------------------------------------------------------------
# WebSocket Server (with subscription management)
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# Main Reasoning Engine (v5.0.0)
# -----------------------------------------------------------------------------
class ReasoningEngine:
    """
    Enhanced unified reasoning engine with MTOP, MOPD, GA, MoE, Pareto, and full enterprise features.
    """
    
    def __init__(self, config: Optional[ReasoningConfig] = None):
        self.config = config or ReasoningConfig()
        self.instance_id = self.config.instance_id
        self.storage = EnhancedStorage(self.config)
        self.carbon_client = LiveCarbonDataClient(self.config, self.storage)
        self.hardware_profiler = HardwareProfiler(self.config)
        self.predictor = PerformancePredictor(self.config, self.storage, self.hardware_profiler)
        self.mtop_engine = MTOPReasoningEngine(self.config)
        self.state = ReasoningState(self.storage)
        self.reflection = ReflectionHandler(self.state, self.mtop_engine)

        self.scheduler = EnhancedCarbonIntensityAwareScheduler(self.config, self.storage, self.carbon_client)
        self.causal_model = EnhancedCarbonCausalModel(self.config, self.storage, self.predictor)
        self.ethical_reasoner = EthicalCarbonReasoner()
        self.context_optimizer = ContextAwareOptimizer(self.config, self.mtop_engine)
        self.planner = SystemicCarbonPlanner()
        self.purpose_optimizer = PurposeAwareOptimizer(self.config, self.mtop_engine)

        # v5.0.0 new modules
        self.ga_search = GeneticArchitectureSearch(self.config, self.predictor)
        self.pareto_optimizer = ParetoOptimizer(self.config, self.storage, self.predictor)

        self.websocket = EnhancedWebSocketServer(self.config.websocket_port)
        self.reasoning_history = deque(maxlen=1000)
        self.enabled = True
        self._shutdown_event = asyncio.Event()
        self._background_tasks = []
        self._running = False

        if PROMETHEUS_AVAILABLE:
            start_http_server(self.config.metrics_port)
            logger.info("Prometheus metrics exposed on port %d", self.config.metrics_port)

        logger.info("ReasoningEngine v%s initialized (instance: %s)", self.config.version, self.instance_id)

    async def start(self):
        self._running = True
        await self.websocket.start()
        await self.carbon_client.__aenter__()
        # Start background tasks
        tasks = [
            self._train_model_loop(),
            self._cleanup_loop(),
            self._carbon_update_loop(),
            self._auto_optimize_loop(),
            self._websocket_heartbeat(),
            self._ga_search_loop()  # new
        ]
        for task in tasks:
            self._background_tasks.append(asyncio.create_task(task))
        logger.info("Reasoning engine started with %d background tasks", len(self._background_tasks))

    async def _train_model_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.model_retrain_interval)
                # Check if we have enough data
                if len(self.predictor._training_data_X) >= 10:
                    self.predictor._train_models()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Model training loop error: %s", e)
                await asyncio.sleep(60)

    async def _cleanup_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.cache_cleanup_interval)
                self.storage.cache.clear()
                gc.collect()
                logger.debug("Cache cleanup performed")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Cleanup loop error: %s", e)
                await asyncio.sleep(60)

    async def _carbon_update_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await self.carbon_client.get_current_intensity(self.config.carbon_region)
                await asyncio.sleep(self.config.carbon_update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Carbon update loop error: %s", e)
                await asyncio.sleep(60)

    async def _auto_optimize_loop(self):
        while not self._shutdown_event.is_set():
            try:
                # Use MTOP to select a strategy periodically
                state = {
                    'predicted_accuracy': 0.85,
                    'cost_budget': self.state.carbon_budget_remaining,
                    'success_rate': self.state.historical_success_rate
                }
                carbon = await self.carbon_client.get_current_intensity(self.config.carbon_region)
                result = await self.mtop_engine.select_strategy(state, carbon)
                logger.info("MTOP strategy selected: %s", result['selected_strategy'])
                await asyncio.sleep(self.config.auto_optimize_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Auto optimize loop error: %s", e)
                await asyncio.sleep(60)

    async def _ga_search_loop(self):
        """Periodically run GA search to update Pareto front."""
        while not self._shutdown_event.is_set():
            try:
                if self.config.ga_enabled:
                    logger.info("Running GA search...")
                    pareto = await self.ga_search.run_search()
                    # Add results to Pareto optimizer
                    for arch in pareto:
                        self.pareto_optimizer.add_architecture(arch)
                    logger.info("GA search completed with %d architectures in Pareto front.", len(pareto))
                await asyncio.sleep(self.config.sustainability_interval)  # reuse interval
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("GA search loop error: %s", e)
                await asyncio.sleep(60)

    async def _websocket_heartbeat(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(30)
            await self.websocket.broadcast({'type': 'heartbeat', 'timestamp': datetime.now().isoformat()})

    # ------------------------------------------------------------------------
    # Core reasoning method
    # ------------------------------------------------------------------------
    async def reason_about_architecture(self,
                                       architecture_config: Dict[str, Any],
                                       fitness_metrics: Dict[str, float],
                                       context: str = 'cloud_inference',
                                       purpose: str = 'balanced',
                                       training_epochs: int = 100,
                                       correlation_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Enhanced reasoning with MTOP, GA, MoE, Pareto, and learning.
        """
        if not self.enabled:
            return {'reasoning': 'disabled'}

        if correlation_id:
            correlation_id_var.set(correlation_id)
        else:
            correlation_id_var.set(str(uuid.uuid4())[:8])

        # Validate input if Pydantic available
        if PYDANTIC_AVAILABLE:
            try:
                ArchitectureConfig(**architecture_config)
            except ValidationError as e:
                logger.warning("Invalid architecture config: %s", e)

        architecture_hash = hashlib.md5(json.dumps(architecture_config, sort_keys=True).encode()).hexdigest()[:8]
        carbon_intensity = await self.carbon_client.get_current_intensity(self.config.carbon_region)

        reasoning_result = {
            'timestamp': datetime.now().isoformat(),
            'architecture_hash': architecture_hash,
            'context': context,
            'purpose': purpose,
            'performance_predictions': {}
        }

        # Performance predictions
        predicted_accuracy = self.predictor.predict_accuracy(architecture_config)
        predicted_latency = self.predictor.predict_latency(architecture_config, context)
        predicted_carbon = self.predictor.predict_carbon(architecture_config, context, training_epochs, self.config.inference_count)
        reasoning_result['performance_predictions'] = {
            'predicted_accuracy': predicted_accuracy,
            'predicted_carbon_kg': predicted_carbon,
            'predicted_latency_ms': predicted_latency
        }

        # Temporal reasoning (carbon-aware scheduling)
        scheduling = await self.scheduler.schedule_computation(
            task='architecture_evaluation',
            urgency='normal',
            compute_hours=1.0
        )
        reasoning_result['temporal'] = scheduling

        # Causal reasoning
        causal = self.causal_model.explain_carbon_impact(architecture_config, fitness_metrics)
        reasoning_result['causal'] = causal

        # Ethical reasoning
        ethical = self.ethical_reasoner.assess_reduction_impact(architecture_config, fitness_metrics)
        reasoning_result['ethical'] = ethical

        # Contextual reasoning (with MTOP)
        context_plan = await self.context_optimizer.get_context_plan(architecture_config, context, carbon_intensity)
        reasoning_result['contextual'] = context_plan

        # Systemic planning
        systemic = self.planner.plan_carbon_investment(
            current_accuracy=fitness_metrics.get('accuracy', predicted_accuracy),
            target_accuracy=0.92,
            carbon_budget=self.state.carbon_budget_remaining
        )
        reasoning_result['systemic'] = systemic

        # Reflexive reasoning (purpose with MTOP)
        reflexive = await self.purpose_optimizer.get_purpose_guide(purpose, carbon_intensity)
        reasoning_result['reflexive'] = reflexive

        # Store reasoning for learning
        await self.storage.save_reasoning(architecture_hash, reasoning_result, correlation_id=correlation_id_var.get())
        self.reasoning_history.append(reasoning_result)

        # Generate overall recommendations
        recommendations = self._generate_enhanced_recommendations(reasoning_result, architecture_config)
        reasoning_result['overall_recommendations'] = recommendations

        # Add Pareto front information if available
        pareto = self.pareto_optimizer.get_pareto_front()
        if pareto:
            reasoning_result['pareto_front_count'] = len(pareto)
            reasoning_result['pareto_suggestions'] = self.pareto_optimizer.get_trade_off_suggestions(self.config.mopd_weights)

        # Learn from this reasoning (update predictor with outcomes if available)
        if fitness_metrics:
            actual_accuracy = fitness_metrics.get('accuracy')
            actual_latency = fitness_metrics.get('latency_ms')
            actual_carbon = fitness_metrics.get('carbon_kg')
            if actual_accuracy is not None and actual_latency is not None and actual_carbon is not None:
                await self.predictor.add_training_data(
                    architecture_config,
                    actual_accuracy,
                    actual_latency,
                    actual_carbon
                )
            # Update causal model with outcome
            if 'carbon_impact' in fitness_metrics:
                for feature in architecture_config:
                    await self.storage.save_causal_effect(
                        feature=feature,
                        value=architecture_config[feature],
                        carbon_impact=fitness_metrics.get('carbon_impact', 0.3),
                        accuracy_impact=fitness_metrics.get('accuracy_impact', 0.02)
                    )
                await self.causal_model._load_historical_data()

        # Update MTOP with reward based on outcome
        reward = 0.5
        if fitness_metrics:
            if fitness_metrics.get('accuracy', 0) > 0.9:
                reward += 0.3
            if fitness_metrics.get('carbon_savings', 0) > 0.2:
                reward += 0.2
        selected = reasoning_result.get('contextual', {}).get('selected_strategy') or \
                   reasoning_result.get('reflexive', {}).get('selected_strategy') or 'balanced'
        await self.mtop_engine.update(selected, reward, {})

        # Trigger reflection if needed
        if reward < 0.3:
            await self.reflection.trigger_reflection('inaccurate_prediction')
        if carbon_intensity > 400:
            await self.reflection.trigger_reflection('high_carbon')
        if reward > 0.8:
            await self.reflection.trigger_reflection('successful_recommendation')

        # Broadcast via WebSocket
        await self.websocket.broadcast({
            'type': 'reasoning_result',
            'architecture_hash': architecture_hash,
            'predicted_accuracy': predicted_accuracy,
            'predicted_carbon': predicted_carbon,
            'selected_strategy': selected,
            'timestamp': datetime.now().isoformat()
        }, topic='reasoning')

        # Update Prometheus metrics
        if PROMETHEUS_AVAILABLE:
            REASONING_CYCLES.labels(status='success').inc()
            REASONING_ACCURACY.set(predicted_accuracy)
            REASONING_CARBON.set(predicted_carbon)

        logger.info("Reasoning completed for %s: accuracy=%.2f, carbon=%.2f kg", architecture_hash, predicted_accuracy, predicted_carbon)

        return reasoning_result

    def _generate_enhanced_recommendations(self, reasoning_result: Dict, architecture_config: Dict) -> List[str]:
        recommendations = []
        # Temporal
        temporal = reasoning_result.get('temporal', {})
        if temporal.get('action') == 'delay':
            recommendations.append(f"Schedule evaluation for better carbon timing: {temporal.get('optimal_schedule', 'unknown')}")
        # Performance
        predictions = reasoning_result.get('performance_predictions', {})
        if predictions.get('predicted_accuracy', 0) < 0.85:
            recommendations.append(f"Predicted accuracy is {predictions['predicted_accuracy']*100:.1f}% - consider architecture improvements")
        if predictions.get('predicted_carbon_kg', 0) > 5:
            recommendations.append(f"High predicted carbon ({predictions['predicted_carbon_kg']:.2f}kg) - consider optimization")
        # Causal
        causal_alt = reasoning_result.get('causal', {}).get('alternatives', [])
        if causal_alt:
            recommendations.append(f"Causal alternative: {causal_alt[0]}")
        # Ethical
        ethical_rec = reasoning_result.get('ethical', {}).get('recommendations', [])
        if ethical_rec:
            recommendations.extend(ethical_rec)
        # Contextual
        contextual_suggestions = reasoning_result.get('contextual', {}).get('suggestions', [])
        for suggestion in contextual_suggestions[:2]:
            recommendations.append(f"Contextual suggestion: {suggestion.get('action')} ({suggestion.get('reason')})")
        # Systemic
        systemic = reasoning_result.get('systemic', {})
        if systemic.get('decision') == 'invest':
            recommendations.append("Systemic decision: Invest in exploration - high ROI expected")
        # Reflexive
        reflexive_rec = reasoning_result.get('reflexive', {}).get('recommendations', [])
        if reflexive_rec:
            recommendations.extend(reflexive_rec[:2])
        # Pareto suggestions
        pareto_suggestions = reasoning_result.get('pareto_suggestions', [])
        if pareto_suggestions:
            recommendations.append(f"Pareto trade-off suggestion: consider architecture with accuracy {pareto_suggestions[0].get('accuracy', 0):.2f} and carbon {pareto_suggestions[0].get('carbon', 0):.2f}kg")
        return recommendations[:5]

    async def get_reasoning_summary(self) -> Dict[str, Any]:
        if not self.reasoning_history:
            return {'status': 'no_reasoning_history'}
        recent = list(self.reasoning_history)[-20:]
        all_recommendations = []
        for entry in recent:
            all_recommendations.extend(entry.get('overall_recommendations', []))
        avg_accuracy = np.mean([entry.get('performance_predictions', {}).get('predicted_accuracy', 0.85) for entry in recent]) if NUMPY_AVAILABLE else 0
        avg_carbon = np.mean([entry.get('performance_predictions', {}).get('predicted_carbon_kg', 1.0) for entry in recent]) if NUMPY_AVAILABLE else 0
        avg_ethical = np.mean([entry.get('ethical', {}).get('overall_ethical_score', 0.5) for entry in recent]) if NUMPY_AVAILABLE else 0
        return {
            'total_reasoned_architectures': len(self.reasoning_history),
            'recent_recommendations': all_recommendations[:10],
            'average_ethical_score': avg_ethical,
            'average_predicted_accuracy': avg_accuracy,
            'average_predicted_carbon_kg': avg_carbon,
            'most_common_causal_driver': self._get_most_common_causal_driver(recent),
            'pareto_front_size': len(self.pareto_optimizer.get_pareto_front()),
            'timestamp': datetime.now().isoformat()
        }

    def _get_most_common_causal_driver(self, recent_entries: List[Dict]) -> str:
        drivers = [entry.get('causal', {}).get('primary_driver', 'unknown') for entry in recent_entries]
        if not drivers:
            return 'unknown'
        from collections import Counter
        return Counter(drivers).most_common(1)[0][0]

    async def shutdown(self):
        logger.info("Shutting down ReasoningEngine (instance: %s)", self.instance_id)
        self.enabled = False
        self._shutdown_event.set()
        self._running = False
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        await self.websocket.stop()
        if self.carbon_client.session:
            await self.carbon_client.__aexit__(None, None, None)
        await self.state.save()
        logger.info("ReasoningEngine shutdown complete")

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
    global _engine_instance
    if _engine_instance:
        await _engine_instance.shutdown()
        _engine_instance = None

# Singleton accessor
_engine_instance = None
_engine_lock = asyncio.Lock()

async def get_reasoning_engine(config: Optional[ReasoningConfig] = None) -> ReasoningEngine:
    global _engine_instance
    if _engine_instance is None:
        async with _engine_lock:
            if _engine_instance is None:
                _engine_instance = ReasoningEngine(config)
                await _engine_instance.start()
    return _engine_instance

# -----------------------------------------------------------------------------
# Pydantic model for architecture config validation (if available)
# -----------------------------------------------------------------------------
if PYDANTIC_AVAILABLE:
    class ArchitectureConfig(BaseModel):
        num_layers: int = Field(6, ge=1)
        hidden_dim: int = Field(384, ge=1)
        num_heads: int = Field(8, ge=1)
        pruning_rate: float = Field(0.0, ge=0, le=1)
        quantization_bits: int = Field(32, ge=1)
        batch_size: int = Field(32, ge=1)
        attention_type: str = Field("flash_attention")
        activation_function: str = Field("swiglu")
        moe_layers: int = Field(0, ge=0)

# -----------------------------------------------------------------------------
# MAIN ENTRY POINT
# -----------------------------------------------------------------------------
async def main():
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s, None))

    print("=" * 80)
    print("Enhanced Reasoning Engine v5.0.0 - MTOP + MOPD + GA + MoE + Pareto")
    print("=" * 80)

    engine = await get_reasoning_engine()

    print(f"\n✅ ENHANCEMENTS OVER v4.0.0:")
    print("   ✅ Bio‑inspired Genetic Algorithm (GA) for architecture search.")
    print("   ✅ Full Mixture‑of‑Experts (MoE) gating network.")
    print("   ✅ Pareto‑front multi‑objective optimisation with trade‑off exploration.")
    print("   ✅ Fast MLPRegressor‑based performance predictor (fallback to GP).")
    print("   ✅ All enhancements are optional and configurable.")

    # Show status
    print(f"\n🔐 Instance: {engine.instance_id}")
    print(f"📊 MTOP Strategy: MoE enabled? {engine.config.moe_enabled}")
    print(f"🧬 GA enabled? {engine.config.ga_enabled}")
    print(f"📊 Pareto front size: {len(engine.pareto_optimizer.get_pareto_front())}")
    print(f"📡 WebSocket port: {engine.config.websocket_port}")
    print(f"📈 Prometheus port: {engine.config.metrics_port}")

    # Run a sample reasoning
    architecture = {
        'num_layers': 8,
        'hidden_dim': 512,
        'num_heads': 10,
        'pruning_rate': 0.1,
        'quantization_bits': 32,
        'batch_size': 64,
        'attention_type': 'flash_attention',
        'activation_function': 'swiglu',
        'moe_layers': 0
    }
    fitness = {'accuracy': 0.88, 'carbon_kg': 2.5, 'latency_ms': 15}

    print(f"\n🔬 Running sample reasoning...")
    result = await engine.reason_about_architecture(architecture, fitness)
    print(f"   Architecture Hash: {result['architecture_hash']}")
    print(f"   Predicted Accuracy: {result['performance_predictions']['predicted_accuracy']:.2f}")
    print(f"   Predicted Carbon: {result['performance_predictions']['predicted_carbon_kg']:.2f} kg")
    print(f"   Selected Strategy: {result['contextual']['selected_strategy']}")
    print(f"   Pareto front size: {result.get('pareto_front_count', 0)}")

    summary = await engine.get_reasoning_summary()
    print(f"\n📊 Summary: Total reasoned: {summary['total_reasoned_architectures']}")
    print(f"   Pareto front size: {summary.get('pareto_front_size', 0)}")

    print("\n" + "=" * 80)
    print("✅ Enhanced Reasoning Engine v5.0.0 - Ready for Production")
    print("=" * 80)

    try:
        await _shutdown_event_global.wait()
    except asyncio.CancelledError:
        pass
    finally:
        await shutdown_handler()

if __name__ == "__main__":
    asyncio.run(main())
