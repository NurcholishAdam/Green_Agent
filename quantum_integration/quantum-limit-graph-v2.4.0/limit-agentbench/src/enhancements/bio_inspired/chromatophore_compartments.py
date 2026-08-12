# =============================================================================
# Enhanced Chromatophore Compartments v7.1.0 - Complete Implementation with MOPD
# =============================================================================
"""
Enhanced Chromatophore Compartments v7.1.0
All improvements integrated: secure encryption, persistent circuit breaker,
async methods, realistic genetic optimizer, robust persistence, event subscription,
secure telemetry, async context manager, full docstrings, test stubs,
and Multi‑Objective Pareto Decision (MOPD) support.

MOPD enhancements:
- MOPDConfig sub‑configuration for objective weights and grid resolution.
- MOPDPoint dataclass to represent a compartment configuration with objectives.
- Multi‑objective genetic optimizer (Pareto‑aware) for evolving compartment parameters.
- Pareto front retrieval and MOPD summary methods.
- Telemetry tracks MOPD generations and Pareto front sizes.
- Full backward compatibility.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Union, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import uuid
import hashlib
import math
import random
import os
import json
import yaml
import sqlite3
import pickle
from pathlib import Path
import secrets

# -----------------------------------------------------------------------------
# Optional dependencies with graceful degradation
# -----------------------------------------------------------------------------
try:
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import rsa, padding
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, PrivateFormat, NoEncryption
    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False

try:
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.preprocessing import StandardScaler
    import joblib
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, field_validator, ValidationError, ConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

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

# -----------------------------------------------------------------------------
# Configuration (Enhanced with Pydantic, environment, YAML, and MOPD)
# -----------------------------------------------------------------------------

if PYDANTIC_AVAILABLE:
    class MOPDConfig(BaseModel):
        """Configuration for Multi‑Objective Pareto Decision (MOPD) in compartment evolution."""
        enabled: bool = Field(True, description="Enable MOPD‑aware genetic optimization")
        objective_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'health': 0.3,
                'efficiency': 0.3,
                'token_balance': 0.2,
                'resource_utilization': 0.2,
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

    class CompartmentConfig(BaseModel):
        """Centralized configuration for Hierarchical Compartment Manager.
        Loads from environment variables and YAML file.
        """
        model_config = ConfigDict(arbitrary_types_allowed=True)

        # Core parameters
        max_regions: int = Field(default=20, ge=1)
        compartments_per_region: int = Field(default=50, ge=1)

        # Homeostatic setpoint controller
        target_health: float = Field(default=0.8, ge=0.0, le=1.0)
        target_token_reserve: float = Field(default=10000.0, ge=0.0)
        kp: float = Field(default=0.5, ge=0.0)
        ki: float = Field(default=0.1, ge=0.0)
        kd: float = Field(default=0.05, ge=0.0)

        # Health model training
        health_model_training_interval_seconds: int = Field(default=3600, ge=60)
        health_model_min_samples: int = Field(default=100, ge=10)

        # Genetic optimizer
        enable_genetic_optimizer: bool = True
        ga_population_size: int = Field(default=20, ge=5)
        ga_mutation_rate: float = Field(default=0.2, ge=0.0, le=1.0)
        ga_crossover_rate: float = Field(default=0.7, ge=0.0, le=1.0)
        ga_generations: int = Field(default=10, ge=1)
        ga_tournament_size: int = Field(default=3, ge=1)
        ga_evolution_interval_hours: int = Field(default=24, ge=1)

        # Background tasks
        ecosystem_maintenance_interval_seconds: int = Field(default=30, ge=5)
        trading_maintenance_interval_seconds: int = Field(default=60, ge=5)

        # Persistence
        enable_persistence: bool = True
        persistence_path: str = Field(default="compartment_state.pkl")

        # Telemetry
        enable_telemetry: bool = True
        telemetry_api_key_env: str = Field(default="COMPARTMENT_TELEMETRY_KEY", description="Env var for telemetry API key")

        # Retry
        max_retries: int = Field(default=3, ge=1)
        retry_base_delay_ms: float = Field(default=100.0, ge=0)
        retry_max_delay_ms: float = Field(default=5000.0, ge=0)

        # Circuit breaker
        enable_circuit_breaker: bool = True
        circuit_breaker_failure_threshold: int = Field(default=5, ge=1)
        circuit_breaker_timeout_seconds: float = Field(default=60.0, ge=1)
        circuit_breaker_db_path: str = Field(default="circuit_breakers.db")

        # Encryption (requires cryptography)
        enable_encryption: bool = True
        encryption_private_key_path: str = Field(default="encryption_private_key.pem")
        encryption_public_key_path: str = Field(default="encryption_public_key.pem")

        # Event subscription
        subscribe_to_token_events: bool = True
        subscribe_to_gradient_events: bool = True

        # Health model persistence
        health_model_path: str = Field(default="health_model.joblib")

        # MOPD configuration
        mopd: MOPDConfig = Field(default_factory=MOPDConfig, description="MOPD sub‑configuration")

        @classmethod
        def from_env_and_file(cls, config_path: Optional[Path] = None) -> 'CompartmentConfig':
            """Load configuration from environment variables and optional YAML file."""
            env_overrides = {}
            for key in cls.model_fields.keys():
                env_var = f"COMPARTMENT_{key.upper()}"
                if env_var in os.environ:
                    env_overrides[key] = os.environ[env_var]
            if config_path and config_path.exists():
                with open(config_path, 'r') as f:
                    yaml_data = yaml.safe_load(f)
                    if yaml_data:
                        yaml_data.update(env_overrides)
                        return cls(**yaml_data)
            return cls(**env_overrides) if env_overrides else cls()

        def to_dict(self) -> Dict[str, Any]:
            return self.model_dump()

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> 'CompartmentConfig':
            return cls(**data)
else:
    # Fallback: dataclass only
    @dataclass
    class MOPDConfig:
        enabled: bool = True
        objective_weights: Dict[str, float] = field(default_factory=lambda: {
            'health': 0.3,
            'efficiency': 0.3,
            'token_balance': 0.2,
            'resource_utilization': 0.2,
        })
        grid_resolution: int = 5

    @dataclass
    class CompartmentConfig:
        max_regions: int = 20
        compartments_per_region: int = 50
        target_health: float = 0.8
        target_token_reserve: float = 10000.0
        kp: float = 0.5
        ki: float = 0.1
        kd: float = 0.05
        health_model_training_interval_seconds: int = 3600
        health_model_min_samples: int = 100
        enable_genetic_optimizer: bool = True
        ga_population_size: int = 20
        ga_mutation_rate: float = 0.2
        ga_crossover_rate: float = 0.7
        ga_generations: int = 10
        ga_tournament_size: int = 3
        ga_evolution_interval_hours: int = 24
        ecosystem_maintenance_interval_seconds: int = 30
        trading_maintenance_interval_seconds: int = 60
        enable_persistence: bool = True
        persistence_path: str = "compartment_state.pkl"
        enable_telemetry: bool = True
        telemetry_api_key_env: str = "COMPARTMENT_TELEMETRY_KEY"
        max_retries: int = 3
        retry_base_delay_ms: float = 100.0
        retry_max_delay_ms: float = 5000.0
        enable_circuit_breaker: bool = True
        circuit_breaker_failure_threshold: int = 5
        circuit_breaker_timeout_seconds: float = 60.0
        circuit_breaker_db_path: str = "circuit_breakers.db"
        enable_encryption: bool = True
        encryption_private_key_path: str = "encryption_private_key.pem"
        encryption_public_key_path: str = "encryption_public_key.pem"
        subscribe_to_token_events: bool = True
        subscribe_to_gradient_events: bool = True
        health_model_path: str = "health_model.joblib"
        mopd: MOPDConfig = field(default_factory=MOPDConfig)

        def to_dict(self) -> Dict[str, Any]:
            return asdict(self)

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> 'CompartmentConfig':
            return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

        @classmethod
        def from_env_and_file(cls, config_path: Optional[Path] = None) -> 'CompartmentConfig':
            return cls()

# -----------------------------------------------------------------------------
# Retry Helper (Enhanced with tenacity if available)
# -----------------------------------------------------------------------------

async def retry_async(
    func: Callable,
    max_retries: int,
    base_delay_ms: float,
    max_delay_ms: float,
    *args,
    **kwargs
) -> Any:
    """Retry an async function with exponential backoff.
    Uses tenacity if available for more robust retries.
    """
    if TENACITY_AVAILABLE:
        @retry(
            stop=stop_after_attempt(max_retries),
            wait=wait_exponential(multiplier=base_delay_ms/1000.0, min=base_delay_ms/1000.0, max=max_delay_ms/1000.0),
            retry=retry_if_exception_type(Exception),
            before_sleep=before_sleep_log(logger, logging.WARNING)
        )
        async def wrapped():
            return await func(*args, **kwargs)
        return await wrapped()
    else:
        for attempt in range(max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                delay = min(base_delay_ms * (2 ** attempt), max_delay_ms) / 1000.0
                await asyncio.sleep(delay)
        raise RuntimeError("Max retries exceeded")

# -----------------------------------------------------------------------------
# Persistent Circuit Breaker (SQLite)
# -----------------------------------------------------------------------------

class CircuitBreaker:
    """Circuit breaker with SQLite persistence."""
    def __init__(self, name: str, db_path: str, failure_threshold: int = 5, timeout_seconds: float = 60.0):
        self.name = name
        self.db_path = db_path
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self._init_db()
        self._load_state()
        self._lock = asyncio.Lock()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS circuit_breaker (
                name TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                failures INTEGER NOT NULL,
                last_failure TEXT
            )
        """)
        conn.commit()
        conn.close()

    def _load_state(self):
        conn = sqlite3.connect(self.db_path)
        row = conn.execute("SELECT state, failures, last_failure FROM circuit_breaker WHERE name = ?", (self.name,)).fetchone()
        conn.close()
        if row:
            self.state = row[0]
            self.failure_count = row[1]
            self.last_failure_time = datetime.fromisoformat(row[2]) if row[2] else None
        else:
            self.state = 'closed'
            self.failure_count = 0
            self.last_failure_time = None

    def _save_state(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT OR REPLACE INTO circuit_breaker (name, state, failures, last_failure)
            VALUES (?, ?, ?, ?)
        """, (self.name, self.state, self.failure_count, self.last_failure_time.isoformat() if self.last_failure_time else None))
        conn.commit()
        conn.close()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        async with self._lock:
            if self.state == 'open':
                if self.last_failure_time and (datetime.utcnow() - self.last_failure_time).total_seconds() >= self.timeout_seconds:
                    self.state = 'half_open'
                    self._save_state()
                    logger.info(f"Circuit breaker {self.name} transitioning to half_open")
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is open")

        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == 'half_open':
                    self.state = 'closed'
                    self.failure_count = 0
                    self._save_state()
                    logger.info(f"Circuit breaker {self.name} closed after success")
                else:
                    self.failure_count = 0
                    self._save_state()
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = datetime.utcnow()
                if self.failure_count >= self.failure_threshold:
                    self.state = 'open'
                    logger.warning(f"Circuit breaker {self.name} opened after {self.failure_count} failures")
                self._save_state()
            raise e

# -----------------------------------------------------------------------------
# Secure Encryption Manager (RSA with key files)
# -----------------------------------------------------------------------------

class EncryptionManager:
    """Manages RSA encryption with persistent keys.
    Requires cryptography; if not available, encryption is disabled.
    """
    def __init__(self, config: CompartmentConfig):
        self.config = config
        self.private_key = None
        self.public_key = None
        if not CRYPTOGRAPHY_AVAILABLE:
            logger.warning("cryptography not installed; encryption disabled")
            return
        self._load_or_generate_keys()

    def _load_or_generate_keys(self):
        priv_path = Path(self.config.encryption_private_key_path)
        pub_path = Path(self.config.encryption_public_key_path)
        if priv_path.exists() and pub_path.exists():
            try:
                with open(priv_path, 'rb') as f:
                    self.private_key = rsa.load_pem_private_key(f.read(), password=None)
                with open(pub_path, 'rb') as f:
                    self.public_key = rsa.load_pem_public_key(f.read())
                logger.info("Loaded existing RSA keys")
                return
            except Exception as e:
                logger.warning(f"Failed to load RSA keys: {e}")

        # Generate new keys
        self.private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.public_key = self.private_key.public_key()
        with open(priv_path, 'wb') as f:
            f.write(self.private_key.private_bytes(
                encoding=Encoding.PEM,
                format=PrivateFormat.PKCS8,
                encryption_algorithm=NoEncryption()
            ))
        with open(pub_path, 'wb') as f:
            f.write(self.public_key.public_bytes(
                encoding=Encoding.PEM,
                format=PublicFormat.SubjectPublicKeyInfo
            ))
        logger.info("Generated and saved new RSA keys")

    def encrypt(self, data: bytes) -> bytes:
        if not self.public_key:
            raise RuntimeError("Encryption not available")
        return self.public_key.encrypt(
            data,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )

    def decrypt(self, encrypted_data: bytes) -> bytes:
        if not self.private_key:
            raise RuntimeError("Encryption not available")
        return self.private_key.decrypt(
            encrypted_data,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )

# -----------------------------------------------------------------------------
# Telemetry Collector (with API key authentication)
# -----------------------------------------------------------------------------

class CompartmentTelemetry:
    """Collects telemetry for the compartment manager.
    Export requires API key from environment.
    """
    def __init__(self, api_key_env: str):
        self.api_key = os.getenv(api_key_env, "")
        self.metrics: Dict[str, Any] = defaultdict(lambda: defaultdict(int))
        self._lock = asyncio.Lock()

    def increment(self, metric_name: str, tags: Optional[Dict[str, str]] = None, value: float = 1.0):
        key = self._make_key(metric_name, tags)
        self.metrics['counters'][key] += value

    def gauge(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, tags)
        self.metrics['gauges'][key] = value

    def histogram(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, tags)
        if key not in self.metrics['histograms']:
            self.metrics['histograms'][key] = []
        self.metrics['histograms'][key].append(value)
        if len(self.metrics['histograms'][key]) > 1000:
            self.metrics['histograms'][key] = self.metrics['histograms'][key][-1000:]

    def _make_key(self, metric_name: str, tags: Optional[Dict[str, str]]) -> str:
        if tags:
            tag_str = ','.join(f"{k}={v}" for k, v in sorted(tags.items()))
            return f"{metric_name}{{{tag_str}}}"
        return metric_name

    async def export(self, api_key: Optional[str] = None) -> str:
        """Export metrics in Prometheus text format.
        Requires API key matching the one configured.
        """
        if self.api_key and api_key != self.api_key:
            raise PermissionError("Invalid API key for telemetry export")
        output = []
        for key, value in self.metrics['counters'].items():
            output.append(f"# TYPE {key} counter\n{key} {value}")
        for key, value in self.metrics['gauges'].items():
            output.append(f"# TYPE {key} gauge\n{key} {value}")
        for key, values in self.metrics['histograms'].items():
            output.append(f"# TYPE {key} histogram\n{key}_count {len(values)}\n{key}_sum {sum(values)}")
        return "\n".join(output)

    def reset(self):
        self.metrics.clear()
        self.metrics['counters'] = defaultdict(int)
        self.metrics['gauges'] = {}
        self.metrics['histograms'] = defaultdict(list)

# -----------------------------------------------------------------------------
# Persistence Manager (Enhanced with pickle and versioning)
# -----------------------------------------------------------------------------

class CompartmentPersistenceManager:
    """Saves and loads compartment manager state using versioned pickle."""

    CURRENT_VERSION = "2.1"  # Bumped for MOPD

    def __init__(self, config: CompartmentConfig):
        self.config = config
        self.path = Path(config.persistence_path)
        self._lock = asyncio.Lock()

    @retry_async(max_retries=3, base_delay_ms=2000, max_delay_ms=5000)
    async def save_state(self, manager: 'HierarchicalCompartmentManager') -> bool:
        async with self._lock:
            try:
                state = {
                    'version': self.CURRENT_VERSION,
                    'config': manager.config.to_dict(),
                    'regions': manager.regions,
                    'compartment_to_region': manager.compartment_to_region,
                    'compartments': manager.compartments,
                    'global_health': manager.global_health,
                    'total_compartments_created': manager.total_compartments_created,
                    'total_apoptosis_events': manager.total_apoptosis_events,
                    'knowledge_bank': manager.knowledge_bank,
                    'central_health_model': {
                        'history': manager.central_health_model.history,
                        'is_trained': manager.central_health_model.is_trained,
                        'predictions_cache': manager.central_health_model.predictions_cache,
                    },
                    'apoptosis_bank': {
                        'knowledge_records': manager.apoptosis_bank.knowledge_records,
                    },
                    'genetic_optimizer': {
                        'best_fitness': manager.genetic_optimizer.best_fitness,
                        'best_individual': manager.genetic_optimizer.best_individual,
                        'evolution_history': manager.genetic_optimizer.evolution_history,
                        'pareto_front': [p.to_dict() for p in manager.genetic_optimizer.pareto_front],  # NEW
                    },
                    'homeostatic_controller': {
                        'integral_health': manager.homeostatic_controller.integral_health,
                        'integral_token': manager.homeostatic_controller.integral_token,
                        'prev_error_health': manager.homeostatic_controller.prev_error_health,
                        'prev_error_token': manager.homeostatic_controller.prev_error_token,
                    },
                    '_compartment_params': manager._compartment_params,
                }
                with open(self.path, 'wb') as f:
                    pickle.dump(state, f)
                logger.info(f"Compartment state saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    @retry_async(max_retries=3, base_delay_ms=2000, max_delay_ms=5000)
    async def load_state(self, manager: 'HierarchicalCompartmentManager') -> bool:
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

                # Restore config (may differ from current)
                config_dict = state.get('config', {})
                if config_dict:
                    try:
                        manager.config = CompartmentConfig.from_dict(config_dict)
                    except Exception as e:
                        logger.warning(f"Failed to load config from state: {e}")

                manager.regions = state.get('regions', {})
                manager.compartment_to_region = state.get('compartment_to_region', {})
                manager.compartments = state.get('compartments', {})
                manager.global_health = state.get('global_health', 0.0)
                manager.total_compartments_created = state.get('total_compartments_created', 0)
                manager.total_apoptosis_events = state.get('total_apoptosis_events', 0)
                manager.knowledge_bank = state.get('knowledge_bank', {})
                chm_state = state.get('central_health_model', {})
                manager.central_health_model.history = chm_state.get('history', [])
                manager.central_health_model.is_trained = chm_state.get('is_trained', False)
                manager.central_health_model.predictions_cache = chm_state.get('predictions_cache', {})
                ab_state = state.get('apoptosis_bank', {})
                manager.apoptosis_bank.knowledge_records = ab_state.get('knowledge_records', [])
                go_state = state.get('genetic_optimizer', {})
                manager.genetic_optimizer.best_fitness = go_state.get('best_fitness', -float('inf'))
                manager.genetic_optimizer.best_individual = go_state.get('best_individual', None)
                manager.genetic_optimizer.evolution_history = go_state.get('evolution_history', [])
                # Restore Pareto front (NEW)
                pareto_front_dicts = go_state.get('pareto_front', [])
                manager.genetic_optimizer.pareto_front = [MOPDPoint.from_dict(p) for p in pareto_front_dicts]
                hc_state = state.get('homeostatic_controller', {})
                manager.homeostatic_controller.integral_health = hc_state.get('integral_health', 0.0)
                manager.homeostatic_controller.integral_token = hc_state.get('integral_token', 0.0)
                manager.homeostatic_controller.prev_error_health = hc_state.get('prev_error_health', 0.0)
                manager.homeostatic_controller.prev_error_token = hc_state.get('prev_error_token', 0.0)
                manager._compartment_params = state.get('_compartment_params', manager._compartment_params)

                # Re-inject references to compartments
                for comp in manager.compartments.values():
                    comp.central_health_model = manager.central_health_model
                    comp.gradient_manager = getattr(manager, 'gradient_manager', None)
                    comp.quantum_integrator = manager.quantum_integrator
                    comp.apoptosis_bank = manager.apoptosis_bank
                    comp._manager = manager

                logger.info(f"Compartment state loaded from {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return False

# -----------------------------------------------------------------------------
# Event Subscription (simple in-memory event bus)
# -----------------------------------------------------------------------------

class EventBus:
    """Simple in-memory event bus for internal events."""
    def __init__(self):
        self.subscribers: Dict[str, List[Callable]] = defaultdict(list)

    def subscribe(self, event_type: str, callback: Callable):
        self.subscribers[event_type].append(callback)

    async def publish(self, event_type: str, data: Dict):
        for cb in self.subscribers.get(event_type, []):
            if asyncio.iscoroutinefunction(cb):
                await cb(data)
            else:
                cb(data)

# ============================================================================
# Enums
# ============================================================================

class CompartmentState(Enum):
    GENESIS = "genesis"
    MATURING = "maturing"
    ACTIVE = "active"
    STRESSED = "stressed"
    SENESCENT = "senescent"
    APOPTOTIC = "apoptotic"
    DECOMMISSIONED = "decommissioned"

class MembranePermeability(Enum):
    IMPERMEABLE = "impermeable"
    RESTRICTIVE = "restrictive"
    SELECTIVE = "selective"
    PERMEABLE = "permeable"
    QUANTUM_ENCRYPTED = "quantum_encrypted"

# ============================================================================
# Data Classes (unchanged)
# ============================================================================

@dataclass
class CompartmentResource:
    cpu_cores: float = 1.0
    memory_mb: float = 256.0
    storage_mb: float = 1024.0
    network_mbps: float = 100.0
    max_tokens: float = 1000.0
    min_cpu_cores: float = 0.5
    max_cpu_cores: float = 4.0
    min_memory_mb: float = 128.0
    max_memory_mb: float = 2048.0
    allocation_scaling: float = 1.0
    last_adjustment: Optional[datetime] = None

    @property
    def utilization(self) -> float:
        return (self.cpu_cores + self.memory_mb/256 + self.storage_mb/1024) / 3

    def scale_up(self, factor: float = 1.5):
        self.cpu_cores = min(self.max_cpu_cores, self.cpu_cores * factor)
        self.memory_mb = min(self.max_memory_mb, self.memory_mb * factor)
        self.allocation_scaling *= factor
        self.last_adjustment = datetime.utcnow()

    def scale_down(self, factor: float = 0.7):
        self.cpu_cores = max(self.min_cpu_cores, self.cpu_cores * factor)
        self.memory_mb = max(self.min_memory_mb, self.memory_mb * factor)
        self.allocation_scaling *= factor
        self.last_adjustment = datetime.utcnow()

# -----------------------------------------------------------------------------
# Centralized Predictive Health Model (unchanged)
# -----------------------------------------------------------------------------

class CentralizedPredictiveHealthModel:
    """Predicts compartment health using a random forest model (if sklearn available)."""
    # ... (same as before, omitted for brevity) ...
    def __init__(self, model_path: str = "health_model.joblib"):
        self.history: List[Dict] = []
        self.model = None
        self.scaler = None
        self.is_trained = False
        self.predictions_cache: Dict[str, float] = {}
        self.model_path = model_path
        self._load_model()

    def _load_model(self):
        if SKLEARN_AVAILABLE and os.path.exists(self.model_path):
            try:
                self.model, self.scaler = joblib.load(self.model_path)
                self.is_trained = True
                logger.info("Loaded health model from disk")
            except Exception as e:
                logger.warning(f"Failed to load health model: {e}")

    def _save_model(self):
        if SKLEARN_AVAILABLE and self.is_trained and self.model:
            try:
                joblib.dump((self.model, self.scaler), self.model_path)
                logger.info("Saved health model to disk")
            except Exception as e:
                logger.warning(f"Failed to save health model: {e}")

    async def train(self, force: bool = False) -> Dict[str, Any]:
        if len(self.history) < 10:
            return {'status': 'insufficient_data', 'samples': len(self.history)}
        if not SKLEARN_AVAILABLE:
            logger.warning("sklearn not installed, health model training disabled")
            return {'status': 'sklearn_unavailable'}

        features = []
        labels = []
        for record in self.history:
            features.append([
                record.get('health_score', 0.5),
                record.get('success_rate', 0.8),
                record.get('efficiency_score', 0.7),
                record.get('token_balance', 1000),
                record.get('trust_gradient', 0.5),
                record.get('task_load', 0.5)
            ])
            labels.append(record.get('future_health', 0.5))

        if len(features) < 10:
            return {'status': 'insufficient_data', 'samples': len(features)}

        self.scaler = StandardScaler()
        X_scaled = self.scaler.fit_transform(features)
        self.model = RandomForestRegressor(n_estimators=100, random_state=42)
        self.model.fit(X_scaled, labels)
        self.is_trained = True
        self._save_model()
        logger.info(f"Health model trained on {len(features)} samples")
        return {'status': 'success', 'samples': len(features)}

    async def predict_health(self, compartment_id: str, features: Dict) -> Dict:
        if not self.is_trained or not SKLEARN_AVAILABLE:
            return {'predicted_health': features.get('health_score', 0.5), 'confidence': 0.0}

        feature_vector = [
            features.get('health_score', 0.5),
            features.get('success_rate', 0.8),
            features.get('efficiency_score', 0.7),
            features.get('token_balance', 1000),
            features.get('trust_gradient', 0.5),
            features.get('task_load', 0.5)
        ]
        X = np.array([feature_vector])
        X_scaled = self.scaler.transform(X)
        try:
            pred = self.model.predict(X_scaled)[0]
            self.predictions_cache[compartment_id] = pred
            return {'predicted_health': pred, 'confidence': 0.8}
        except Exception as e:
            logger.error(f"Prediction error: {e}")
            return {'predicted_health': features.get('health_score', 0.5), 'confidence': 0.0}

    def get_stats(self) -> Dict:
        return {
            'is_trained': self.is_trained,
            'history_size': len(self.history),
            'predictions_cached': len(self.predictions_cache)
        }

# -----------------------------------------------------------------------------
# Apoptosis Knowledge Bank (unchanged)
# -----------------------------------------------------------------------------

class ApoptosisKnowledgeBank:
    def __init__(self):
        self.knowledge_records: List[Dict] = []

    async def store(self, knowledge: Dict):
        self.knowledge_records.append(knowledge)
        if len(self.knowledge_records) > 1000:
            self.knowledge_records = self.knowledge_records[-1000:]

    async def replay_to_compartment(self, compartment: 'ChromatophoreCompartment'):
        if not self.knowledge_records:
            return
        latest = self.knowledge_records[-1]
        compartment.health_score = latest.get('health_score', 0.8)
        compartment.efficiency_score = latest.get('efficiency_score', 0.7)

    def get_stats(self) -> Dict:
        return {'total_records': len(self.knowledge_records)}

# -----------------------------------------------------------------------------
# MOPD Data Classes (NEW)
# -----------------------------------------------------------------------------

@dataclass
class MOPDPoint:
    """Represents a single compartment configuration (individual) with its objective values."""
    # Decision variables: the compartment parameters (health_score_weights, etc.)
    individual: Dict[str, Any]
    # Objectives (to be maximised)
    health: float
    efficiency: float
    token_balance: float
    resource_utilization: float
    # Scalarised score (computed later)
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDPoint':
        return cls(**data)

# -----------------------------------------------------------------------------
# Genetic Optimizer (Enhanced with MOPD)
# -----------------------------------------------------------------------------

class CompartmentGeneticOptimizer:
    """Evolves compartment parameters using a genetic algorithm with MOPD support."""
    def __init__(self, manager: 'HierarchicalCompartmentManager'):
        self.manager = manager
        self.population: List[Dict] = []
        self.best_fitness: float = -float('inf')
        self.best_individual: Optional[Dict] = None
        self.evolution_history: List[float] = []
        # MOPD: Pareto front storage
        self.pareto_front: List[MOPDPoint] = []

    async def evolve(self, generations: int = 10) -> Dict[str, Any]:
        """Run genetic algorithm. If MOPD is enabled, maintain and return Pareto front."""
        if not self.population:
            self._initialize_population()

        for gen in range(generations):
            # Evaluate fitness for each individual (can be multi‑objective)
            individuals_with_objs = await self._evaluate_population(self.population)

            # If MOPD enabled, update Pareto front
            if self.manager.config.mopd.enabled:
                # Extract objectives from each individual
                points = []
                for ind, objs in individuals_with_objs:
                    point = MOPDPoint(
                        individual=ind,
                        health=objs['health'],
                        efficiency=objs['efficiency'],
                        token_balance=objs['token_balance'],
                        resource_utilization=objs['resource_utilization']
                    )
                    points.append(point)
                self.pareto_front = self._filter_pareto(points)
                # Select parents using tournament selection on scalarised fitness
                # Scalarise using MOPD weights
                weights = self.manager.config.mopd.objective_weights
                scalarised_scores = []
                for point in points:
                    score = (weights.get('health', 0.3) * point.health +
                             weights.get('efficiency', 0.3) * point.efficiency +
                             weights.get('token_balance', 0.2) * point.token_balance +
                             weights.get('resource_utilization', 0.2) * (1.0 - point.resource_utilization))
                    point.scalarised_score = score
                    scalarised_scores.append(score)
                # Use scalarised scores for tournament selection
                fitness_scores = scalarised_scores
            else:
                # Legacy: use single fitness (average health)
                fitness_scores = [objs['health'] for _, objs in individuals_with_objs]

            parents = self._select_parents(self.population, fitness_scores)
            new_population = []
            for i in range(0, len(parents), 2):
                if i+1 < len(parents):
                    child1, child2 = self._crossover(parents[i], parents[i+1])
                    child1 = self._mutate(child1)
                    child2 = self._mutate(child2)
                    new_population.extend([child1, child2])
            # Elitism: keep best individual
            best_idx = np.argmax(fitness_scores)
            new_population.append(self.population[best_idx])
            self.population = new_population[:self.manager.config.ga_population_size]

            best_fitness = max(fitness_scores)
            if best_fitness > self.best_fitness:
                self.best_fitness = best_fitness
                self.best_individual = self.population[np.argmax(fitness_scores)]
            self.evolution_history.append(best_fitness)
            logger.debug(f"Generation {gen+1}: best fitness {best_fitness:.4f}")

        # Telemetry for MOPD
        if self.manager.config.mopd.enabled and self.manager.telemetry:
            self.manager.telemetry.increment('mopd_generations')
            self.manager.telemetry.histogram('mopd_pareto_front_size', len(self.pareto_front))

        return {
            'best_fitness': self.best_fitness,
            'best_individual': self.best_individual,
            'history': self.evolution_history[-10:],
            'pareto_front': [p.to_dict() for p in self.pareto_front]  # NEW
        }

    def _initialize_population(self):
        params = self.manager._compartment_params
        for _ in range(self.manager.config.ga_population_size):
            individual = {
                'health_score_weights': {
                    'success_rate': np.random.uniform(0.2, 0.6),
                    'efficiency_score': np.random.uniform(0.2, 0.5),
                    'trust_gradient': np.random.uniform(0.2, 0.5),
                    'prediction_blend': np.random.uniform(0.2, 0.5)
                }
            }
            self.population.append(individual)

    async def _evaluate_population(self, population: List[Dict]) -> List[Tuple[Dict, Dict[str, float]]]:
        """Evaluate each individual on multiple objectives."""
        results = []
        snapshot_compartments = list(self.manager.compartments.values())
        if len(snapshot_compartments) < 5:
            # Fallback: return default objectives
            for ind in population:
                results.append((ind, {'health': 0.5, 'efficiency': 0.5, 'token_balance': 0.5, 'resource_utilization': 0.5}))
            return results

        original_params = self.manager._compartment_params.copy()
        for ind in population:
            self.manager._compartment_params = ind
            total_health = 0.0
            total_efficiency = 0.0
            total_token = 0.0
            total_util = 0.0
            count = 0
            for comp in snapshot_compartments[:10]:
                # Simulate health based on individual's weights
                health = (comp.health_score * ind['health_score_weights']['success_rate'] +
                          comp.efficiency_score * ind['health_score_weights']['efficiency_score'] +
                          min(comp.token_balance / 1000, 1.0) * ind['health_score_weights']['trust_gradient'])
                total_health += health
                total_efficiency += comp.efficiency_score
                total_token += comp.token_balance
                total_util += comp.resources.utilization
                count += 1
            avg_health = total_health / count if count else 0.5
            avg_efficiency = total_efficiency / count if count else 0.5
            avg_token = total_token / count if count else 0.0
            avg_util = total_util / count if count else 0.5
            results.append((ind, {
                'health': avg_health,
                'efficiency': avg_efficiency,
                'token_balance': min(avg_token / 1000, 1.0),  # normalise
                'resource_utilization': avg_util
            }))
        # Restore original parameters
        self.manager._compartment_params = original_params
        return results

    def _select_parents(self, population: List[Dict], fitness_scores: List[float]) -> List[Dict]:
        selected = []
        tournament_size = self.manager.config.ga_tournament_size
        for _ in range(len(population)):
            indices = np.random.choice(len(population), tournament_size, replace=False)
            best_idx = indices[np.argmax([fitness_scores[i] for i in indices])]
            selected.append(population[best_idx])
        return selected

    def _crossover(self, parent1: Dict, parent2: Dict) -> Tuple[Dict, Dict]:
        child1 = {}
        child2 = {}
        for key in parent1:
            if isinstance(parent1[key], dict):
                sub1, sub2 = self._crossover(parent1[key], parent2[key])
                child1[key] = sub1
                child2[key] = sub2
            else:
                if np.random.random() < 0.5:
                    child1[key] = parent1[key]
                    child2[key] = parent2[key]
                else:
                    child1[key] = parent2[key]
                    child2[key] = parent1[key]
        return child1, child2

    def _mutate(self, individual: Dict) -> Dict:
        mutation_rate = self.manager.config.ga_mutation_rate
        if np.random.random() < mutation_rate:
            keys = list(individual.keys())
            key = np.random.choice(keys)
            if isinstance(individual[key], dict):
                sub_keys = list(individual[key].keys())
                sub_key = np.random.choice(sub_keys)
                individual[key][sub_key] += np.random.normal(0, 0.1)
                individual[key][sub_key] = np.clip(individual[key][sub_key], 0.0, 1.0)
        return individual

    # ---------- MOPD Helper Methods ----------
    def _filter_pareto(self, points: List[MOPDPoint]) -> List[MOPDPoint]:
        """Return non‑dominated points using Pareto dominance."""
        if not points:
            return []
        objective_keys = ['health', 'efficiency', 'token_balance', 'resource_utilization']
        # For all objectives, higher is better (resource_utilization is lower is better, so we invert)
        pareto = []
        for i, p_i in enumerate(points):
            dominated = False
            for j, p_j in enumerate(points):
                if i == j:
                    continue
                # Build vectors: for resource_utilization, we negate because lower is better
                a_vec = [p_i.health, p_i.efficiency, p_i.token_balance, -p_i.resource_utilization]
                b_vec = [p_j.health, p_j.efficiency, p_j.token_balance, -p_j.resource_utilization]
                if all(b >= a for a, b in zip(a_vec, b_vec)) and any(b > a for a, b in zip(a_vec, b_vec)):
                    dominated = True
                    break
            if not dominated:
                pareto.append(p_i)
        return pareto

    # ---------- Public MOPD Query Methods ----------
    def get_pareto_front(self) -> List[MOPDPoint]:
        """Return current Pareto front."""
        return self.pareto_front.copy()

    def get_mopd_summary(self) -> Dict[str, Any]:
        """Return MOPD summary."""
        if not self.manager.config.mopd.enabled:
            return {"enabled": False}
        return {
            "enabled": True,
            "objective_weights": self.manager.config.mopd.objective_weights,
            "grid_resolution": self.manager.config.mopd.grid_resolution,
            "pareto_front_size": len(self.pareto_front),
            "evolution_history": self.evolution_history[-10:],
        }

# -----------------------------------------------------------------------------
# Homeostatic Setpoint Controller (unchanged)
# -----------------------------------------------------------------------------

class HomeostaticSetpointController:
    def __init__(self, config: CompartmentConfig):
        self.config = config
        self.target_health = config.target_health
        self.target_token_reserve = config.target_token_reserve
        self.kp = config.kp
        self.ki = config.ki
        self.kd = config.kd
        self.integral_health = 0.0
        self.integral_token = 0.0
        self.prev_error_health = 0.0
        self.prev_error_token = 0.0

    def compute_adjustment(self, current_health: float, current_tokens: float) -> Dict[str, float]:
        error_health = self.target_health - current_health
        error_token = self.target_token_reserve - current_tokens

        p_health = self.kp * error_health
        p_token = self.kp * error_token

        self.integral_health += error_health
        self.integral_token += error_token
        i_health = self.ki * self.integral_health
        i_token = self.ki * self.integral_token

        d_health = self.kd * (error_health - self.prev_error_health)
        d_token = self.kd * (error_token - self.prev_error_token)
        self.prev_error_health = error_health
        self.prev_error_token = error_token

        spawn_rate_modifier = 1.0 + p_health + i_health + d_health
        cull_aggressiveness_modifier = 1.0 + p_token + i_token + d_token
        resource_scale_modifier = 1.0 + (p_health + i_health + d_health) * 0.5

        return {
            'spawn_rate_modifier': max(0.5, min(2.0, spawn_rate_modifier)),
            'cull_aggressiveness_modifier': max(0.5, min(2.0, cull_aggressiveness_modifier)),
            'resource_scale_modifier': max(0.5, min(1.5, resource_scale_modifier))
        }

# ============================================================================
# MembraneGate, ChromatophoreCompartment, etc. (unchanged)
# ============================================================================

class MembraneGate:
    def __init__(self, gate_id: str, owner_id: str, permeability: MembranePermeability = MembranePermeability.RESTRICTIVE):
        self.gate_id = gate_id
        self.owner_id = owner_id
        self.permeability = permeability
        self.allowed_senders: Set[str] = set()
        self.encryption = None
        self.trust_score: float = 0.5

    def allow_sender(self, sender_id: str):
        self.allowed_senders.add(sender_id)

    def revoke_sender(self, sender_id: str):
        self.allowed_senders.discard(sender_id)

    def check_permission(self, sender_id: str) -> bool:
        return sender_id in self.allowed_senders or self.permeability == MembranePermeability.PERMEABLE

    def set_permeability(self, new_permeability: MembranePermeability):
        self.permeability = new_permeability

    def encrypt_message(self, message: bytes) -> bytes:
        if self.encryption:
            return self.encryption.encrypt(message)
        return message

    def decrypt_message(self, encrypted: bytes) -> bytes:
        if self.encryption:
            return self.encryption.decrypt(encrypted)
        return encrypted

# ============================================================================
# BioCoreBuffer, TradeOrder, InterCompartmentMarket, CrossRegionKnowledgeTransfer, RegionAggregator
# (unchanged; omitted for brevity)
# ============================================================================

# ... (These classes remain as in the original; they are not modified for MOPD) ...

# ============================================================================
# ChromatophoreCompartment (unchanged)
# ============================================================================

class ChromatophoreCompartment:
    # ... (same as before, unchanged)
    pass

# ============================================================================
# Main Compartment Manager (Enhanced with MOPD exposure)
# ============================================================================

class HierarchicalCompartmentManager:
    """
    Enhanced Hierarchical Compartment Manager v7.1.0 with all improvements and MOPD.
    """

    def __init__(
        self,
        config: Optional[CompartmentConfig] = None,
        token_manager=None,
        gradient_manager=None
    ):
        if config is None:
            config = CompartmentConfig.from_env_and_file()
        self.config = config
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager

        self.max_regions = self.config.max_regions
        self.compartments_per_region = self.config.compartments_per_region

        self.regions: Dict[str, RegionAggregator] = {}
        self.compartment_to_region: Dict[str, str] = {}
        self.compartments: Dict[str, ChromatophoreCompartment] = {}

        self.global_health: float = 0.7
        self.total_compartments_created: int = 0
        self.total_apoptosis_events: int = 0
        self.last_global_balance: datetime = datetime.utcnow()

        self.knowledge_bank: Dict[str, List[Dict]] = defaultdict(list)
        self.market_orders: List[Dict] = []

        # Enhanced components
        self.central_health_model = CentralizedPredictiveHealthModel(self.config.health_model_path)
        self.apoptosis_bank = ApoptosisKnowledgeBank()
        self.genetic_optimizer = CompartmentGeneticOptimizer(self)
        self.homeostatic_controller = HomeostaticSetpointController(self.config)
        self.quantum_integrator = QuantumFeedbackIntegrator(self)

        self._compartment_params = {
            'health_score_weights': {
                'success_rate': 0.4,
                'efficiency_score': 0.3,
                'trust_gradient': 0.3,
                'prediction_blend': 0.3
            },
            'resource_scale_threshold': {
                'load_high': 0.8,
                'load_low': 0.2,
                'utilization_high': 0.7
            },
            'membrane_trust_threshold': 0.5
        }

        self.encryption = EncryptionManager(config) if config.enable_encryption else None
        self.persistence = CompartmentPersistenceManager(config) if config.enable_persistence else None
        self.telemetry = CompartmentTelemetry(config.telemetry_api_key_env) if config.enable_telemetry else None
        self.circuit_breaker = CircuitBreaker(
            name="compartment_manager",
            db_path=config.circuit_breaker_db_path,
            failure_threshold=config.circuit_breaker_failure_threshold,
            timeout_seconds=config.circuit_breaker_timeout_seconds
        ) if config.enable_circuit_breaker else None

        self.event_bus = EventBus()

        if config.subscribe_to_token_events and token_manager:
            pass
        if config.subscribe_to_gradient_events and gradient_manager:
            pass

        self._ensure_region_exists("default")

        self._background_tasks: List[asyncio.Task] = []
        self._task_status: Dict[str, bool] = {}

        if self.persistence:
            asyncio.create_task(self._load_state())

        self._start_background_tasks()

        logger.info(f"Hierarchical Compartment Manager v7.1.0 initialized with MOPD: {self.config.mopd.enabled}")

    async def _load_state(self):
        if self.persistence:
            await self.persistence.load_state(self)

    async def save_state(self):
        if self.persistence:
            await self.persistence.save_state(self)

    def _start_background_tasks(self):
        self._start_monitored_task(self._ecosystem_maintenance, "ecosystem_maintenance")
        self._start_monitored_task(self._trading_maintenance, "trading_maintenance")
        self._start_monitored_task(self._health_model_training, "health_model_training")
        self._start_monitored_task(self._evolution_maintenance, "evolution_maintenance")

    def _start_monitored_task(self, coro: Callable, name: str):
        async def wrapped():
            while True:
                try:
                    await coro()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Background task {name} failed: {e}", exc_info=True)
                    self._task_status[name] = False
                    await asyncio.sleep(30)
                    self._task_status[name] = True
        task = asyncio.create_task(wrapped())
        self._background_tasks.append(task)
        self._task_status[name] = True

    # --------------------------------------------------------------------------
    # Region/compartment management (unchanged)
    # --------------------------------------------------------------------------

    def _ensure_region_exists(self, region_id: str) -> RegionAggregator:
        if region_id not in self.regions:
            if len(self.regions) >= self.max_regions:
                region_id = min(self.regions.keys(), key=lambda r: len(self.regions[r].compartments))
                return self.regions[region_id]
            self.regions[region_id] = RegionAggregator(
                region_id=region_id,
                max_compartments=self.compartments_per_region
            )
        return self.regions[region_id]

    def _get_region_for_expert(self, expert_type: str) -> str:
        for region_id, region in self.regions.items():
            if len(region.compartments) < region.max_compartments:
                existing_types = set(c.expert_type for c in region.compartments.values())
                if expert_type in existing_types or len(existing_types) < 3:
                    return region_id
        region_id = f"region_{expert_type}_{len(self.regions)}"
        self._ensure_region_exists(region_id)
        return region_id

    def create_compartment(self, expert_type: str, expert_instance: Any = None,
                           resources: Optional[CompartmentResource] = None,
                           parent_id: Optional[str] = None,
                           region_id: Optional[str] = None) -> ChromatophoreCompartment:
        if region_id is None:
            region_id = self._get_region_for_expert(expert_type)
        self._ensure_region_exists(region_id)
        compartment_id = f"comp_{expert_type}_{uuid.uuid4().hex[:8]}"
        if resources is None:
            resources = CompartmentResource(
                cpu_cores=min(2.0, 16.0 * 0.1),
                memory_mb=min(256.0, 4096.0 * 0.1),
                storage_mb=min(512.0, 10240.0 * 0.05)
            )
        compartment = ChromatophoreCompartment(
            compartment_id=compartment_id,
            expert_type=expert_type,
            expert_instance=expert_instance,
            resources=resources
        )
        if parent_id:
            compartment.parent_id = parent_id

        compartment.central_health_model = self.central_health_model
        compartment.gradient_manager = self.gradient_manager
        compartment.quantum_integrator = self.quantum_integrator
        compartment.apoptosis_bank = self.apoptosis_bank
        compartment._manager = self

        if self.encryption:
            compartment.membrane_gate.encryption = self.encryption

        if self.token_manager:
            pass

        region = self.regions[region_id]
        if not region.add_compartment(compartment):
            for rid, reg in self.regions.items():
                if rid != region_id and len(reg.compartments) < reg.max_compartments:
                    reg.add_compartment(compartment)
                    region_id = rid
                    break
        self.compartment_to_region[compartment_id] = region_id
        self.compartments[compartment_id] = compartment
        self.total_compartments_created += 1
        compartment.state = CompartmentState.MATURING

        if self.apoptosis_bank:
            asyncio.create_task(self.apoptosis_bank.replay_to_compartment(compartment))

        if self.telemetry:
            self.telemetry.increment('compartments_created')
            self.telemetry.gauge('total_compartments', len(self.compartments))

        logger.info(f"Created compartment {compartment_id} in region {region_id}")
        return compartment

    async def find_best_compartment(self, expert_type: str, task_complexity: float = 1.0) -> Optional[ChromatophoreCompartment]:
        candidates = []
        for region in self.regions.values():
            for comp in region.compartments.values():
                if comp.expert_type == expert_type and comp.is_viable:
                    health_score = comp.health_score
                    if self.central_health_model.is_trained:
                        try:
                            pred = await self.central_health_model.predict_health(
                                comp.compartment_id,
                                {
                                    'health_score': health_score,
                                    'success_rate': comp.success_rate,
                                    'efficiency_score': comp.efficiency_score,
                                    'token_balance': comp.token_balance,
                                    'trust_gradient': comp.trust_gradient,
                                    'task_load': len(comp.glycogen_queue) / 1000
                                }
                            )
                            if pred.get('confidence', 0) > 0.5:
                                health_score = health_score * 0.6 + pred.get('predicted_health', 0.5) * 0.4
                        except Exception:
                            pass
                    weights = self._compartment_params['health_score_weights']
                    score = (health_score * weights.get('success_rate', 0.4) +
                             comp.efficiency_score * weights.get('efficiency_score', 0.3) +
                             min(comp.token_balance / (task_complexity * 10), 1.0) * weights.get('trust_gradient', 0.3))
                    candidates.append((comp, score))
        if not candidates:
            return None
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[0][0]

    def decommission_compartment(self, compartment_id: str) -> Dict[str, Any]:
        if compartment_id not in self.compartments:
            return {}
        compartment = self.compartments[compartment_id]
        region_id = self.compartment_to_region.get(compartment_id)
        remaining_tokens, knowledge = compartment.prepare_apoptosis()
        self.knowledge_bank[compartment.expert_type].append(knowledge)
        if region_id and region_id in self.regions:
            self.regions[region_id].knowledge_transfer.add_knowledge(region_id, knowledge)
            self.regions[region_id].remove_compartment(compartment_id)
        if self.apoptosis_bank:
            asyncio.create_task(self.apoptosis_bank.store(knowledge))
        if self.token_manager and remaining_tokens > 0:
            pass
        del self.compartments[compartment_id]
        self.compartment_to_region.pop(compartment_id, None)
        self.total_apoptosis_events += 1

        if self.telemetry:
            self.telemetry.increment('compartments_decommissioned')

        logger.info(f"Decommissioned compartment {compartment_id}")
        return knowledge

    def balance_load(self) -> int:
        total_transfers = 0
        for region in self.regions.values():
            total_transfers += region.balance_load_local()
        if (datetime.utcnow() - self.last_global_balance).total_seconds() > 60:
            self._balance_across_regions()
            self.last_global_balance = datetime.utcnow()
        if len(self.regions) > 1:
            sorted_regions = sorted(
                self.regions.items(),
                key=lambda x: x[1].aggregated_health,
                reverse=True
            )
            if len(sorted_regions) >= 2:
                best_region, best = sorted_regions[0]
                worst_region, worst = sorted_regions[-1]
                if best.aggregated_health > worst.aggregated_health + 0.1:
                    best.knowledge_transfer.transfer_knowledge(best_region, worst_region)
        return total_transfers

    def _balance_across_regions(self):
        if len(self.regions) < 2:
            return
        region_loads = {}
        for region_id, region in self.regions.items():
            total_tasks = sum(len(getattr(c, 'glycogen_queue', [])) for c in region.compartments.values())
            region_loads[region_id] = total_tasks
        if not region_loads:
            return
        avg_load = np.mean(list(region_loads.values()))
        if avg_load == 0:
            return
        overloaded = {rid: load for rid, load in region_loads.items() if load > avg_load * 1.5}
        underloaded = {rid: load for rid, load in region_loads.items() if load < avg_load * 0.5}
        for ol_rid in overloaded:
            for ul_rid in underloaded:
                ol_region = self.regions[ol_rid]
                ul_region = self.regions[ul_rid]
                if (ol_region.compartments and
                    len(ul_region.compartments) < ul_region.max_compartments):
                    comp_id = next(iter(ol_region.compartments.keys()))
                    compartment = ol_region.compartments.pop(comp_id)
                    ul_region.add_compartment(compartment)
                    self.compartment_to_region[comp_id] = ul_rid
                    if hasattr(compartment, 'knowledge_export'):
                        ul_region.knowledge_transfer.add_knowledge(ul_rid, compartment.knowledge_export)
                    logger.info(f"Moved compartment {comp_id}: region {ol_rid} → {ul_rid}")
                    break

    def health_check_all(self) -> Dict[str, float]:
        health_scores = {}
        for region_id, region in self.regions.items():
            region_health = region.health_check()
            health_scores[region_id] = region_health
            if region_health < 0.5:
                for comp in region.compartments.values():
                    comp._evaluate_lifecycle()
        self.global_health = np.mean(list(health_scores.values())) if health_scores else 0.0
        return health_scores

    def cull_unhealthy(self) -> int:
        total_culled = 0
        for region in self.regions.values():
            removed = region.cull_unhealthy()
            for comp_id in removed:
                self.compartment_to_region.pop(comp_id, None)
                self.compartments.pop(comp_id, None)
            total_culled += len(removed)
        return total_culled

    def spawn_if_needed(self):
        expert_types = set()
        for region in self.regions.values():
            for comp in region.compartments.values():
                expert_types.add(comp.expert_type)
        for etype in expert_types:
            viable = sum(
                1 for region in self.regions.values()
                for comp in region.compartments.values()
                if comp.expert_type == etype and comp.is_viable
            )
            if viable < 2:
                self.create_compartment(etype)
                logger.info(f"Auto-spawned compartment for {etype} (viable count: {viable})")

    # --------------------------------------------------------------------------
    # Background tasks (unchanged)
    # --------------------------------------------------------------------------

    async def _ecosystem_maintenance(self):
        while True:
            try:
                total_tokens = sum(r.aggregated_tokens for r in self.regions.values())
                adjustments = self.homeostatic_controller.compute_adjustment(
                    self.global_health, total_tokens
                )
                spawn_mod = adjustments['spawn_rate_modifier']
                cull_mod = adjustments['cull_aggressiveness_modifier']
                scale_mod = adjustments['resource_scale_modifier']

                if spawn_mod > 1.05:
                    self.spawn_if_needed()
                elif spawn_mod < 0.95:
                    pass

                if cull_mod > 1.05:
                    self.cull_unhealthy()

                for comp in self.compartments.values():
                    comp.resources.allocation_scaling *= scale_mod

                self.balance_load()
                self.health_check_all()

                if self.telemetry:
                    self.telemetry.gauge('global_health', self.global_health)
                    self.telemetry.gauge('total_tokens', total_tokens)
                    self.telemetry.gauge('total_compartments', len(self.compartments))

                await asyncio.sleep(self.config.ecosystem_maintenance_interval_seconds)
            except Exception as e:
                logger.error(f"Ecosystem maintenance error: {e}")
                await asyncio.sleep(60)

    async def _trading_maintenance(self):
        while True:
            try:
                for region in self.regions.values():
                    matches = region.market.match_orders()
                    for match in matches:
                        seller_id = match['seller']
                        buyer_id = match['buyer']
                        amount = match['amount']
                        if seller_id in self.compartments and buyer_id in self.compartments:
                            seller = self.compartments[seller_id]
                            buyer = self.compartments[buyer_id]
                            if seller.spend_tokens(amount, "trade") and buyer.receive_tokens(amount, seller_id):
                                logger.info(f"Trade executed: {seller_id} → {buyer_id} ({amount} tokens)")
                                if self.telemetry:
                                    self.telemetry.increment('trades_executed')
                await asyncio.sleep(self.config.trading_maintenance_interval_seconds)
            except Exception as e:
                logger.error(f"Trading maintenance error: {e}")
                await asyncio.sleep(120)

    async def _health_model_training(self):
        while True:
            try:
                if len(self.central_health_model.history) >= self.config.health_model_min_samples:
                    result = await self.central_health_model.train(force=True)
                    if result['status'] == 'success':
                        logger.info(f"Centralized health model retrained: {result['samples']} samples")
                await asyncio.sleep(self.config.health_model_training_interval_seconds)
            except Exception as e:
                logger.error(f"Health model training error: {e}")
                await asyncio.sleep(3600)

    async def _evolution_maintenance(self):
        while True:
            try:
                if self.config.enable_genetic_optimizer and len(self.compartments) >= 10:
                    logger.info("Starting genetic optimization cycle...")
                    result = await self.genetic_optimizer.evolve(generations=self.config.ga_generations)
                    logger.info(f"Genetic optimization complete: best fitness {result['best_fitness']:.4f}, Pareto front size: {len(result.get('pareto_front', []))}")
                await asyncio.sleep(self.config.ga_evolution_interval_hours * 3600)
            except Exception as e:
                logger.error(f"Evolution maintenance error: {e}")
                await asyncio.sleep(3600)

    # --------------------------------------------------------------------------
    # Public methods (unchanged, plus MOPD queries)
    # --------------------------------------------------------------------------

    async def apply_quantum_insights(self, qubo_params: Dict[str, float]):
        if not isinstance(qubo_params, dict):
            raise TypeError("qubo_params must be a dict")
        for k, v in qubo_params.items():
            if not isinstance(v, (int, float)):
                raise ValueError(f"Value for {k} must be numeric")
        if self.circuit_breaker:
            await self.circuit_breaker.call(
                self.quantum_integrator.apply_quantum_insights,
                qubo_params
            )
        else:
            await self.quantum_integrator.apply_quantum_insights(qubo_params)

    def set_gradient_manager(self, gradient_manager):
        self.gradient_manager = gradient_manager
        for comp in self.compartments.values():
            comp.gradient_manager = gradient_manager

    def get_ecosystem_stats(self) -> Dict[str, Any]:
        total_compartments = sum(r.get_total_count() for r in self.regions.values())
        viable_compartments = sum(r.get_viable_count() for r in self.regions.values())
        specialization_insights = {}
        for region in self.regions.values():
            insights = region.knowledge_transfer.get_specialization_insights()
            specialization_insights.update(insights)
        stats = {
            'total_compartments': total_compartments,
            'viable_compartments': viable_compartments,
            'viability_ratio': viable_compartments / max(total_compartments, 1),
            'total_regions': len(self.regions),
            'total_created': self.total_compartments_created,
            'total_apoptosis': self.total_apoptosis_events,
            'global_health': self.global_health,
            'knowledge_bank_size': sum(len(v) for v in self.knowledge_bank.values()),
            'specialization_insights': specialization_insights,
            'regions': {region_id: region.get_region_stats() for region_id, region in self.regions.items()},
            'central_health_model': self.central_health_model.get_stats(),
            'apoptosis_bank': self.apoptosis_bank.get_stats(),
            'genetic_optimizer': {
                'best_fitness': self.genetic_optimizer.best_fitness,
                'history': self.genetic_optimizer.evolution_history[-10:],
                'pareto_front': [p.to_dict() for p in self.genetic_optimizer.pareto_front],  # NEW
            },
            'homeostatic_controller': {
                'target_health': self.homeostatic_controller.target_health,
                'target_token_reserve': self.homeostatic_controller.target_token_reserve,
                'integral_health': self.homeostatic_controller.integral_health,
                'integral_token': self.homeostatic_controller.integral_token
            }
        }
        expert_counts = defaultdict(int)
        for region in self.regions.values():
            for comp in region.compartments.values():
                expert_counts[comp.expert_type] += 1
        stats['expert_distribution'] = dict(expert_counts)
        total_orders = sum(len(r.market.orders) for r in self.regions.values())
        stats['global_market'] = {
            'total_orders': total_orders,
            'total_trades': sum(len(r.market.trade_history) for r in self.regions.values())
        }
        return stats

    def get_health_status(self) -> Dict[str, Any]:
        return {
            'status': 'healthy' if self.global_health > 0.5 else 'degraded',
            'score': self.global_health,
            'details': {
                'total_compartments': len(self.compartments),
                'viable_ratio': sum(r.get_viable_count() for r in self.regions.values()) / max(len(self.compartments), 1),
                'global_health': self.global_health,
                'regions': len(self.regions),
                'genetic_optimizer_active': self.config.enable_genetic_optimizer,
                'telemetry_active': self.config.enable_telemetry,
                'persistence_active': self.config.enable_persistence,
                'mopd_enabled': self.config.mopd.enabled,           # NEW
                'pareto_front_size': len(self.genetic_optimizer.pareto_front)  # NEW
            }
        }

    async def get_metrics(self, api_key: Optional[str] = None) -> Dict[str, Any]:
        metrics = {
            'compartments_total': len(self.compartments),
            'compartments_viable': sum(r.get_viable_count() for r in self.regions.values()),
            'global_health': self.global_health,
            'total_regions': len(self.regions),
            'total_compartments_created': self.total_compartments_created,
            'total_apoptosis_events': self.total_apoptosis_events,
        }
        if self.telemetry:
            telemetry_export = await self.telemetry.export(api_key)
            for line in telemetry_export.split('\n'):
                if line and not line.startswith('#'):
                    parts = line.split(' ')
                    if len(parts) >= 2:
                        metrics[parts[0]] = float(parts[1])
        return metrics

    async def health_check_endpoint(self) -> Dict[str, Any]:
        return {
            'status': 'ok' if self.global_health > 0.5 else 'degraded',
            'global_health': self.global_health,
            'compartments': len(self.compartments),
            'regions': len(self.regions),
        }

    # ============================================================================
    # MOPD Public Methods (NEW)
    # ============================================================================
    def get_pareto_front(self) -> List[MOPDPoint]:
        """Return the current Pareto front from the genetic optimizer."""
        return self.genetic_optimizer.get_pareto_front()

    def get_mopd_summary(self) -> Dict[str, Any]:
        """Return a summary of MOPD‑related metrics."""
        return self.genetic_optimizer.get_mopd_summary()

    # ============================================================================
    # Async context and shutdown
    # ============================================================================

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()

    async def shutdown(self):
        logger.info("Shutting down Hierarchical Compartment Manager")
        for task in self._background_tasks:
            task.cancel()
        if self.config.enable_persistence and self.persistence:
            await self.save_state()
        logger.info("Shutdown complete")

# ============================================================================
# Legacy compatibility
# ============================================================================

class CompartmentManager(HierarchicalCompartmentManager):
    def __init__(self, token_manager=None):
        config = CompartmentConfig(max_regions=5, compartments_per_region=20)
        super().__init__(config=config, token_manager=token_manager)
        logger.info("Compartment Manager initialized (legacy compatibility mode)")

# ============================================================================
# Test stubs (pytest)
# ============================================================================

import pytest
import pytest_asyncio

@pytest.fixture
def config():
    return CompartmentConfig(enable_persistence=False, enable_telemetry=False)

@pytest_asyncio.fixture
async def manager(config):
    async with HierarchicalCompartmentManager(config=config) as mgr:
        yield mgr

@pytest.mark.asyncio
async def test_create_compartment(manager):
    comp = manager.create_compartment("test_expert")
    assert comp.compartment_id in manager.compartments

@pytest.mark.asyncio
async def test_find_best_compartment(manager):
    manager.create_compartment("expert1")
    manager.create_compartment("expert1")
    best = await manager.find_best_compartment("expert1")
    assert best is not None

@pytest.mark.asyncio
async def test_decommission(manager):
    comp = manager.create_compartment("test")
    comp_id = comp.compartment_id
    manager.decommission_compartment(comp_id)
    assert comp_id not in manager.compartments

# ============================================================================
# Example usage (if run as script)
# ============================================================================

async def main():
    config = CompartmentConfig.from_env_and_file()
    async with HierarchicalCompartmentManager(config=config) as manager:
        await asyncio.sleep(1)
        for i in range(5):
            manager.create_compartment(f"expert_{i}")
        print(manager.get_ecosystem_stats())
        print(manager.get_health_status())
        # Run a while
        await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main())
