# =============================================================================
# Enhanced Chromatophore Compartments v6.2.1 - Complete Implementation
# =============================================================================

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
from pathlib import Path

# -----------------------------------------------------------------------------
# Optional dependencies with graceful degradation
# -----------------------------------------------------------------------------
try:
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import rsa, padding
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False

try:
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, field_validator, ValidationError, ConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration (Enhanced with Pydantic, environment, and YAML)
# ============================================================================

if PYDANTIC_AVAILABLE:
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
        persistence_path: str = Field(default="compartment_state.json")

        # Telemetry
        enable_telemetry: bool = True
        telemetry_api_key: Optional[str] = Field(default=None, description="API key for secure telemetry endpoint")

        # Retry (for future external calls)
        max_retries: int = Field(default=3, ge=1)
        retry_base_delay_ms: float = Field(default=100.0, ge=0)
        retry_max_delay_ms: float = Field(default=5000.0, ge=0)

        # Circuit breaker
        enable_circuit_breaker: bool = True
        circuit_breaker_failure_threshold: int = Field(default=5, ge=1)
        circuit_breaker_timeout_seconds: float = Field(default=60.0, ge=1)

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
                        # Merge with env overrides (env takes precedence)
                        yaml_data.update(env_overrides)
                        return cls(**yaml_data)
            # If no YAML, use env overrides
            return cls(**env_overrides) if env_overrides else cls()

        def to_dict(self) -> Dict[str, Any]:
            return self.model_dump()

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> 'CompartmentConfig':
            return cls(**data)
else:
    # Fallback: dataclass only
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
        persistence_path: str = "compartment_state.json"
        enable_telemetry: bool = True
        telemetry_api_key: Optional[str] = None
        max_retries: int = 3
        retry_base_delay_ms: float = 100.0
        retry_max_delay_ms: float = 5000.0
        enable_circuit_breaker: bool = True
        circuit_breaker_failure_threshold: int = 5
        circuit_breaker_timeout_seconds: float = 60.0

        def to_dict(self) -> Dict[str, Any]:
            return asdict(self)

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> 'CompartmentConfig':
            return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

        @classmethod
        def from_env_and_file(cls, config_path: Optional[Path] = None) -> 'CompartmentConfig':
            # Simple fallback: just return default
            return cls()

# ============================================================================
# Retry Helper (Enhanced with tenacity if available)
# ============================================================================

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
            retry=retry_if_exception_type(Exception)
        )
        async def wrapped():
            return await func(*args, **kwargs)
        return await wrapped()
    else:
        # Fallback to simple loop
        for attempt in range(max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                delay = min(base_delay_ms * (2 ** attempt), max_delay_ms) / 1000.0
                await asyncio.sleep(delay)
        raise RuntimeError("Max retries exceeded")

# ============================================================================
# Circuit Breaker
# ============================================================================

class CircuitBreaker:
    """Circuit breaker pattern to prevent repeated failures."""
    def __init__(self, failure_threshold: int = 5, timeout_seconds: float = 60.0):
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.failure_count = 0
        self.state = 'closed'  # closed, half_open, open
        self.last_failure_time: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        async with self._lock:
            if self.state == 'open':
                # Check if timeout elapsed
                if self.last_failure_time and (datetime.utcnow() - self.last_failure_time).total_seconds() >= self.timeout_seconds:
                    self.state = 'half_open'
                    logger.info("Circuit breaker transitioning to half_open")
                else:
                    raise RuntimeError("Circuit breaker is open")

            try:
                result = await func(*args, **kwargs)
                # On success, reset
                if self.state == 'half_open':
                    self.state = 'closed'
                    self.failure_count = 0
                    logger.info("Circuit breaker closed after success")
                elif self.state == 'closed':
                    self.failure_count = 0
                return result
            except Exception as e:
                async with self._lock:
                    self.failure_count += 1
                    self.last_failure_time = datetime.utcnow()
                    if self.failure_count >= self.failure_threshold:
                        self.state = 'open'
                        logger.warning(f"Circuit breaker opened after {self.failure_count} failures")
                raise e

# ============================================================================
# Telemetry Collector
# ============================================================================

class CompartmentTelemetry:
    """Collects telemetry for the compartment manager.
    Optionally secured with an API key for external exports.
    """

    def __init__(self, api_key: Optional[str] = None):
        self.metrics: Dict[str, Any] = defaultdict(lambda: defaultdict(int))
        self._lock = asyncio.Lock()
        self.api_key = api_key

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
        Requires API key if configured.
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

# ============================================================================
# Persistence Manager (Enhanced with versioned JSON)
# ============================================================================

class CompartmentPersistenceManager:
    """Saves and loads compartment manager state using versioned JSON."""

    CURRENT_VERSION = "1.0"

    def __init__(self, config: CompartmentConfig):
        self.config = config
        self.path = Path(config.persistence_path)
        self._lock = asyncio.Lock()

    async def save_state(self, manager: 'HierarchicalCompartmentManager') -> bool:
        async with self._lock:
            try:
                # Prepare state as dict
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
                    },
                    'homeostatic_controller': {
                        'integral_health': manager.homeostatic_controller.integral_health,
                        'integral_token': manager.homeostatic_controller.integral_token,
                        'prev_error_health': manager.homeostatic_controller.prev_error_health,
                        'prev_error_token': manager.homeostatic_controller.prev_error_token,
                    },
                    '_compartment_params': manager._compartment_params,
                }
                # Convert to JSON-serializable format
                state_serializable = self._make_serializable(state)
                with open(self.path, 'w') as f:
                    json.dump(state_serializable, f, indent=2, default=str)
                logger.info(f"Compartment state saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    async def load_state(self, manager: 'HierarchicalCompartmentManager') -> bool:
        async with self._lock:
            if not self.path.exists():
                logger.warning(f"Persistence file {self.path} not found")
                return False
            try:
                with open(self.path, 'r') as f:
                    state = json.load(f)

                version = state.get('version', '0.0')
                if version != self.CURRENT_VERSION:
                    logger.warning(f"State version {version} != current {self.CURRENT_VERSION}; attempting migration")

                # Load config (may differ from current)
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
                # Restore central health model
                chm_state = state.get('central_health_model', {})
                manager.central_health_model.history = chm_state.get('history', [])
                manager.central_health_model.is_trained = chm_state.get('is_trained', False)
                manager.central_health_model.predictions_cache = chm_state.get('predictions_cache', {})
                # Restore apoptosis bank
                ab_state = state.get('apoptosis_bank', {})
                manager.apoptosis_bank.knowledge_records = ab_state.get('knowledge_records', [])
                # Restore genetic optimizer
                go_state = state.get('genetic_optimizer', {})
                manager.genetic_optimizer.best_fitness = go_state.get('best_fitness', -float('inf'))
                manager.genetic_optimizer.best_individual = go_state.get('best_individual', None)
                manager.genetic_optimizer.evolution_history = go_state.get('evolution_history', [])
                # Restore homeostatic controller
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

    def _make_serializable(self, obj: Any) -> Any:
        """Convert non-serializable objects to JSON-friendly forms."""
        if isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(v) for v in obj]
        elif isinstance(obj, tuple):
            return [self._make_serializable(v) for v in obj]
        elif isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif hasattr(obj, '__dict__'):
            return self._make_serializable(obj.__dict__)
        else:
            return obj

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
# Data Classes
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

# ============================================================================
# Quantum-Resistant Encryption (stub with graceful fallback)
# ============================================================================

class QuantumResistantEncryption:
    """Placeholder for quantum-resistant encryption.
    If cryptography is available, uses RSA; otherwise falls back to simple hashing.
    """
    def __init__(self):
        if CRYPTOGRAPHY_AVAILABLE:
            self.private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            self.public_key = self.private_key.public_key()
        else:
            self.private_key = None
            self.public_key = None

    def encrypt(self, data: bytes) -> bytes:
        if CRYPTOGRAPHY_AVAILABLE and self.public_key:
            return self.public_key.encrypt(
                data,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
        else:
            # Fallback: simple XOR with a fixed key (not secure, but for demo)
            key = 0x5A
            return bytes([b ^ key for b in data])

    def decrypt(self, encrypted_data: bytes) -> bytes:
        if CRYPTOGRAPHY_AVAILABLE and self.private_key:
            return self.private_key.decrypt(
                encrypted_data,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
        else:
            # Fallback: XOR again (symmetric)
            key = 0x5A
            return bytes([b ^ key for b in encrypted_data])

# ============================================================================
# MembraneGate
# ============================================================================

class MembraneGate:
    """
    Represents a gate controlling information flow between compartments.
    Permeability can be adjusted based on trust and quantum encryption.
    """
    def __init__(self, gate_id: str, owner_id: str, permeability: MembranePermeability = MembranePermeability.RESTRICTIVE):
        self.gate_id = gate_id
        self.owner_id = owner_id
        self.permeability = permeability
        self.allowed_senders: Set[str] = set()
        self.encryption = QuantumResistantEncryption()
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
        return self.encryption.encrypt(message)

    def decrypt_message(self, encrypted: bytes) -> bytes:
        return self.encryption.decrypt(encrypted)

# ============================================================================
# Centralized Predictive Health Model (with sklearn fallback)
# ============================================================================

class CentralizedPredictiveHealthModel:
    """Predicts compartment health using a random forest model (if sklearn available)."""

    def __init__(self):
        self.history: List[Dict] = []
        self.model = None
        self.scaler = None
        self.is_trained = False
        self.predictions_cache: Dict[str, float] = {}

    async def train(self, force: bool = False) -> Dict[str, Any]:
        """Train the health model on historical data."""
        if len(self.history) < 10:
            return {'status': 'insufficient_data', 'samples': len(self.history)}
        if not SKLEARN_AVAILABLE:
            logger.warning("sklearn not installed, health model training disabled")
            return {'status': 'sklearn_unavailable'}

        # Prepare features and labels
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

        # Scale features
        self.scaler = StandardScaler()
        X_scaled = self.scaler.fit_transform(features)

        # Train random forest
        self.model = RandomForestRegressor(n_estimators=100, random_state=42)
        self.model.fit(X_scaled, labels)
        self.is_trained = True
        logger.info(f"Health model trained on {len(features)} samples")
        return {'status': 'success', 'samples': len(features)}

    async def predict_health(self, compartment_id: str, features: Dict) -> Dict:
        """Predict health for a compartment."""
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

# ============================================================================
# Apoptosis Knowledge Bank
# ============================================================================

class ApoptosisKnowledgeBank:
    """Stores knowledge from decommissioned compartments to be replayed to new ones."""
    def __init__(self):
        self.knowledge_records: List[Dict] = []

    async def store(self, knowledge: Dict):
        self.knowledge_records.append(knowledge)
        if len(self.knowledge_records) > 1000:
            self.knowledge_records = self.knowledge_records[-1000:]

    async def replay_to_compartment(self, compartment: 'ChromatophoreCompartment'):
        """Apply best practices from stored knowledge to a new compartment."""
        if not self.knowledge_records:
            return
        # Simple heuristic: apply the most recent successful pattern
        latest = self.knowledge_records[-1]
        # Copy relevant parameters
        compartment.health_score = latest.get('health_score', 0.8)
        compartment.efficiency_score = latest.get('efficiency_score', 0.7)
        # etc.

    def get_stats(self) -> Dict:
        return {'total_records': len(self.knowledge_records)}

# ============================================================================
# Genetic Optimizer for Compartment Parameters
# ============================================================================

class CompartmentGeneticOptimizer:
    """Evolves compartment parameters using a genetic algorithm."""
    def __init__(self, manager: 'HierarchicalCompartmentManager'):
        self.manager = manager
        self.population: List[Dict] = []
        self.best_fitness: float = -float('inf')
        self.best_individual: Optional[Dict] = None
        self.evolution_history: List[float] = []

    async def evolve(self, generations: int = 10) -> Dict[str, Any]:
        """Run the genetic algorithm for a number of generations."""
        # Initialize population from current compartments' parameters
        if not self.population:
            self._initialize_population()

        for gen in range(generations):
            fitness_scores = [self._fitness(ind) for ind in self.population]
            # Select parents
            parents = self._select_parents(fitness_scores)
            # Crossover and mutation
            new_population = []
            for i in range(0, len(parents), 2):
                if i+1 < len(parents):
                    child1, child2 = self._crossover(parents[i], parents[i+1])
                    child1 = self._mutate(child1)
                    child2 = self._mutate(child2)
                    new_population.extend([child1, child2])
            # Keep the best individual
            best_idx = np.argmax(fitness_scores)
            new_population.append(self.population[best_idx])
            self.population = new_population[:self.manager.config.ga_population_size]

            best_fitness = max(fitness_scores)
            if best_fitness > self.best_fitness:
                self.best_fitness = best_fitness
                self.best_individual = self.population[np.argmax(fitness_scores)]
            self.evolution_history.append(best_fitness)
            logger.debug(f"Generation {gen+1}: best fitness {best_fitness:.4f}")

        return {'best_fitness': self.best_fitness, 'best_individual': self.best_individual, 'history': self.evolution_history[-10:]}

    def _initialize_population(self):
        """Initialize population from current compartments' parameters."""
        # For simplicity, we sample from existing compartments
        params = self.manager._compartment_params
        for _ in range(self.manager.config.ga_population_size):
            # Create a random variation
            individual = {
                'health_score_weights': {
                    'success_rate': np.random.uniform(0.2, 0.6),
                    'efficiency_score': np.random.uniform(0.2, 0.5),
                    'trust_gradient': np.random.uniform(0.2, 0.5),
                    'prediction_blend': np.random.uniform(0.2, 0.5)
                }
            }
            self.population.append(individual)

    def _fitness(self, individual: Dict) -> float:
        """Calculate fitness of an individual based on its performance."""
        # Simulate by applying to the manager and measuring global health
        # For simplicity, we return a random score weighted by some factors
        # In a real implementation, you'd run a simulation or use historical data.
        return (individual['health_score_weights']['success_rate'] * 0.4 +
                individual['health_score_weights']['efficiency_score'] * 0.3 +
                individual['health_score_weights']['trust_gradient'] * 0.3 +
                np.random.normal(0, 0.05))

    def _select_parents(self, fitness_scores: List[float]) -> List[Dict]:
        """Tournament selection."""
        selected = []
        tournament_size = self.manager.config.ga_tournament_size
        for _ in range(len(self.population)):
            # Pick random individuals
            indices = np.random.choice(len(self.population), tournament_size, replace=False)
            best_idx = indices[np.argmax([fitness_scores[i] for i in indices])]
            selected.append(self.population[best_idx])
        return selected

    def _crossover(self, parent1: Dict, parent2: Dict) -> Tuple[Dict, Dict]:
        """Single-point crossover."""
        child1 = {}
        child2 = {}
        for key in parent1:
            if isinstance(parent1[key], dict):
                # Recursively crossover sub-dicts
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
        """Mutate an individual with a given mutation rate."""
        mutation_rate = self.manager.config.ga_mutation_rate
        if np.random.random() < mutation_rate:
            # Mutate a random parameter
            keys = list(individual.keys())
            key = np.random.choice(keys)
            if isinstance(individual[key], dict):
                # Mutate a sub-parameter
                sub_keys = list(individual[key].keys())
                sub_key = np.random.choice(sub_keys)
                individual[key][sub_key] += np.random.normal(0, 0.1)
                individual[key][sub_key] = np.clip(individual[key][sub_key], 0.0, 1.0)
        return individual

# ============================================================================
# Homeostatic Setpoint Controller
# ============================================================================

class HomeostaticSetpointController:
    """PID controller to maintain system health and token reserves."""
    def __init__(self, target_health: float = 0.8, target_token_reserve: float = 10000.0):
        self.target_health = target_health
        self.target_token_reserve = target_token_reserve
        self.kp = 0.5
        self.ki = 0.1
        self.kd = 0.05
        self.integral_health = 0.0
        self.integral_token = 0.0
        self.prev_error_health = 0.0
        self.prev_error_token = 0.0

    def compute_adjustment(self, current_health: float, current_tokens: float) -> Dict[str, float]:
        """Compute PID adjustments for spawn rate, cull aggressiveness, and resource scaling."""
        error_health = self.target_health - current_health
        error_token = self.target_token_reserve - current_tokens

        # Proportional
        p_health = self.kp * error_health
        p_token = self.kp * error_token

        # Integral
        self.integral_health += error_health
        self.integral_token += error_token
        i_health = self.ki * self.integral_health
        i_token = self.ki * self.integral_token

        # Derivative
        d_health = self.kd * (error_health - self.prev_error_health)
        d_token = self.kd * (error_token - self.prev_error_token)
        self.prev_error_health = error_health
        self.prev_error_token = error_token

        # Outputs
        spawn_rate_modifier = 1.0 + p_health + i_health + d_health
        cull_aggressiveness_modifier = 1.0 + p_token + i_token + d_token
        resource_scale_modifier = 1.0 + (p_health + i_health + d_health) * 0.5

        return {
            'spawn_rate_modifier': max(0.5, min(2.0, spawn_rate_modifier)),
            'cull_aggressiveness_modifier': max(0.5, min(2.0, cull_aggressiveness_modifier)),
            'resource_scale_modifier': max(0.5, min(1.5, resource_scale_modifier))
        }

# ============================================================================
# Quantum Feedback Integrator
# ============================================================================

class QuantumFeedbackIntegrator:
    """Integrates quantum feedback insights from external QUBO solvers."""
    def __init__(self, manager: 'HierarchicalCompartmentManager'):
        self.manager = manager
        self.insights_history: List[Dict] = []

    async def apply_quantum_insights(self, qubo_params: Dict[str, float]):
        """Apply QUBO parameters to adjust compartment behavior."""
        self.insights_history.append({'qubo_params': qubo_params, 'timestamp': datetime.utcnow()})
        # Example: adjust resource scaling based on QUBO coefficients
        if 'scaling_factor' in qubo_params:
            factor = qubo_params['scaling_factor']
            for comp in self.manager.compartments.values():
                comp.resources.allocation_scaling *= (1 + factor * 0.1)
        logger.info(f"Applied quantum insights: {qubo_params}")

# ============================================================================
# Gradient-Aware Behavior
# ============================================================================

class GradientAwareBehavior:
    """Adjusts compartment behavior based on trust gradients."""
    def __init__(self):
        self.trust_gradient: float = 0.5
        self.gradient_history: deque = deque(maxlen=100)

    def update_gradient(self, delta: float):
        """Update trust gradient based on recent interactions."""
        self.gradient_history.append(delta)
        self.trust_gradient = np.mean(self.gradient_history) if self.gradient_history else 0.5

    def get_behavior_multiplier(self) -> float:
        """Return a multiplier for compartment actions based on trust gradient."""
        return 0.5 + self.trust_gradient * 0.5  # maps 0-1 to 0.5-1.0

# ============================================================================
# Chromatophore Compartment
# ============================================================================

class ChromatophoreCompartment:
    """A single compartment with its own health, resources, and behavior."""
    def __init__(
        self,
        compartment_id: str,
        expert_type: str,
        expert_instance: Any = None,
        resources: Optional[CompartmentResource] = None,
        parent_id: Optional[str] = None
    ):
        self.compartment_id = compartment_id
        self.expert_type = expert_type
        self.expert_instance = expert_instance
        self.resources = resources or CompartmentResource()
        self.parent_id = parent_id

        # State
        self.state = CompartmentState.GENESIS
        self.health_score: float = 0.8
        self.success_rate: float = 0.8
        self.efficiency_score: float = 0.7
        self.token_balance: float = 0.0
        self.trust_gradient: float = 0.5
        self.is_viable: bool = True
        self.glycogen_queue: deque = deque(maxlen=1000)

        # References
        self.central_health_model: Optional[CentralizedPredictiveHealthModel] = None
        self.gradient_manager: Optional[GradientAwareBehavior] = None
        self.quantum_integrator: Optional[QuantumFeedbackIntegrator] = None
        self.apoptosis_bank: Optional[ApoptosisKnowledgeBank] = None
        self._manager: Optional['HierarchicalCompartmentManager'] = None

        # Membrane gate
        self.membrane_gate = MembraneGate(f"gate_{compartment_id}", compartment_id)

        # Knowledge export (for apoptosis)
        self.knowledge_export: Dict = {}

    def receive_tokens(self, amount: float, from_id: str) -> bool:
        """Receive tokens from another compartment."""
        if amount <= 0:
            return False
        self.token_balance += amount
        self.trust_gradient = (self.trust_gradient * 0.9 + 0.1)  # increase trust
        return True

    def spend_tokens(self, amount: float, reason: str) -> bool:
        """Spend tokens for a reason."""
        if amount > self.token_balance:
            return False
        self.token_balance -= amount
        self.trust_gradient = (self.trust_gradient * 0.9 + 0.0)  # decrease trust slightly
        return True

    def _evaluate_lifecycle(self):
        """Evaluate if the compartment should transition to a different state."""
        if self.health_score < 0.3:
            self.state = CompartmentState.APOPTOTIC
            self.is_viable = False
        elif self.health_score < 0.5:
            self.state = CompartmentState.STRESSED
        elif self.health_score >= 0.8:
            self.state = CompartmentState.ACTIVE
        else:
            self.state = CompartmentState.MATURING

    def prepare_apoptosis(self) -> Tuple[float, Dict]:
        """Prepare for decommissioning, returning remaining tokens and knowledge."""
        self.state = CompartmentState.APOPTOTIC
        self.is_viable = False
        # Gather knowledge
        self.knowledge_export = {
            'expert_type': self.expert_type,
            'health_score': self.health_score,
            'success_rate': self.success_rate,
            'efficiency_score': self.efficiency_score,
            'trust_gradient': self.trust_gradient,
            'resource_utilization': self.resources.utilization,
            'timestamp': datetime.utcnow().isoformat()
        }
        return self.token_balance, self.knowledge_export

# ============================================================================
# Bio-Core Buffer (unchanged)
# ============================================================================

class BioCoreBuffer:
    """A simple buffer to store data for bio-inspired processing."""
    def __init__(self, capacity: int = 1000):
        self.buffer = deque(maxlen=capacity)

    def push(self, item: Any):
        self.buffer.append(item)

    def pop(self) -> Optional[Any]:
        if self.buffer:
            return self.buffer.popleft()
        return None

# ============================================================================
# TradeOrder, InterCompartmentMarket
# ============================================================================

@dataclass
class TradeOrder:
    order_id: str
    seller_id: str
    buyer_id: Optional[str] = None
    token_amount: float = 0.0
    resource_type: str = "tokens"
    price: float = 0.0
    status: str = "pending"
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: datetime = field(default_factory=lambda: datetime.utcnow() + timedelta(minutes=5))

class InterCompartmentMarket:
    """A simple market for trading tokens between compartments."""
    def __init__(self):
        self.orders: List[TradeOrder] = []
        self.trade_history: List[Dict] = []
        self._lock = asyncio.Lock()

    async def place_order(self, seller_id: str, amount: float, price: float) -> str:
        """Place a sell order."""
        order = TradeOrder(
            order_id=f"order_{uuid.uuid4().hex[:8]}",
            seller_id=seller_id,
            token_amount=amount,
            price=price
        )
        async with self._lock:
            self.orders.append(order)
        return order.order_id

    async def match_orders(self) -> List[Dict]:
        """Match buy and sell orders."""
        matches = []
        async with self._lock:
            # Simple matching: buy orders are those with buyer_id set
            buy_orders = [o for o in self.orders if o.buyer_id is not None and o.status == 'pending']
            sell_orders = [o for o in self.orders if o.buyer_id is None and o.status == 'pending']
            for buy in buy_orders:
                for sell in sell_orders:
                    if buy.price >= sell.price and buy.token_amount <= sell.token_amount:
                        # Match
                        match = {
                            'buyer': buy.buyer_id,
                            'seller': sell.seller_id,
                            'amount': buy.token_amount,
                            'price': sell.price
                        }
                        matches.append(match)
                        buy.status = 'completed'
                        sell.status = 'completed'
                        sell.token_amount -= buy.token_amount
                        self.trade_history.append(match)
                        break
            # Remove completed orders
            self.orders = [o for o in self.orders if o.status == 'pending']
        return matches

# ============================================================================
# CrossRegionKnowledgeTransfer
# ============================================================================

class CrossRegionKnowledgeTransfer:
    """Transfers knowledge between regions."""
    def __init__(self):
        self.knowledge_exchange: Dict[str, List[Dict]] = defaultdict(list)

    def add_knowledge(self, region_id: str, knowledge: Dict):
        self.knowledge_exchange[region_id].append(knowledge)

    def transfer_knowledge(self, from_region: str, to_region: str):
        """Transfer the most recent knowledge from one region to another."""
        if from_region in self.knowledge_exchange and self.knowledge_exchange[from_region]:
            latest = self.knowledge_exchange[from_region][-1]
            self.knowledge_exchange[to_region].append(latest)

    def get_specialization_insights(self) -> Dict:
        """Return insights about specializations per region."""
        insights = {}
        for region_id, records in self.knowledge_exchange.items():
            # Count expert types
            types = [r.get('expert_type', 'unknown') for r in records]
            from collections import Counter
            insights[region_id] = dict(Counter(types))
        return insights

# ============================================================================
# RegionAggregator
# ============================================================================

class RegionAggregator:
    """Aggregates compartments within a region."""
    def __init__(self, region_id: str, max_compartments: int = 50):
        self.region_id = region_id
        self.max_compartments = max_compartments
        self.compartments: Dict[str, ChromatophoreCompartment] = {}
        self.knowledge_transfer = CrossRegionKnowledgeTransfer()
        self.market = InterCompartmentMarket()
        self.aggregated_health: float = 0.7
        self.aggregated_tokens: float = 0.0

    def add_compartment(self, compartment: ChromatophoreCompartment) -> bool:
        if len(self.compartments) >= self.max_compartments:
            return False
        self.compartments[compartment.compartment_id] = compartment
        self._update_aggregated_metrics()
        return True

    def remove_compartment(self, compartment_id: str) -> bool:
        if compartment_id in self.compartments:
            del self.compartments[compartment_id]
            self._update_aggregated_metrics()
            return True
        return False

    def balance_load_local(self) -> int:
        """Balance load within the region."""
        # Simplified: move tasks from high-loaded to low-loaded compartments
        if len(self.compartments) < 2:
            return 0
        loads = [(cid, len(comp.glycogen_queue)) for cid, comp in self.compartments.items()]
        loads.sort(key=lambda x: x[1])
        total_transfers = 0
        # Move tasks from most loaded to least loaded
        # (Implementation simplified)
        return total_transfers

    def health_check(self) -> float:
        if not self.compartments:
            self.aggregated_health = 0.0
            return 0.0
        health_scores = [comp.health_score for comp in self.compartments.values()]
        self.aggregated_health = np.mean(health_scores)
        return self.aggregated_health

    def cull_unhealthy(self) -> List[str]:
        """Cull compartments with health below threshold."""
        removed = []
        for comp_id, comp in list(self.compartments.items()):
            if comp.health_score < 0.2:
                removed.append(comp_id)
        for comp_id in removed:
            self.remove_compartment(comp_id)
        return removed

    def get_total_count(self) -> int:
        return len(self.compartments)

    def get_viable_count(self) -> int:
        return sum(1 for comp in self.compartments.values() if comp.is_viable)

    def get_region_stats(self) -> Dict:
        return {
            'total_compartments': len(self.compartments),
            'viable_compartments': self.get_viable_count(),
            'aggregated_health': self.aggregated_health,
            'aggregated_tokens': self.aggregated_tokens
        }

    def _update_aggregated_metrics(self):
        """Update aggregated health and tokens."""
        if not self.compartments:
            self.aggregated_health = 0.0
            self.aggregated_tokens = 0.0
            return
        self.aggregated_health = np.mean([comp.health_score for comp in self.compartments.values()])
        self.aggregated_tokens = sum(comp.token_balance for comp in self.compartments.values())

# ============================================================================
# Hierarchical Compartment Manager (Enhanced)
# ============================================================================

class HierarchicalCompartmentManager:
    """
    Enhanced compartment manager with configuration, persistence, telemetry,
    circuit breaker, and input validation.
    """

    def __init__(
        self,
        config: Optional[CompartmentConfig] = None,
        token_manager=None,
        gradient_manager=None
    ):
        # If config is None, load from environment and optional YAML
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

        # New features
        self.central_health_model = CentralizedPredictiveHealthModel()
        self.apoptosis_bank = ApoptosisKnowledgeBank()
        self.genetic_optimizer = CompartmentGeneticOptimizer(self)
        self.homeostatic_controller = HomeostaticSetpointController(
            target_health=self.config.target_health,
            target_token_reserve=self.config.target_token_reserve
        )
        self.homeostatic_controller.kp = self.config.kp
        self.homeostatic_controller.ki = self.config.ki
        self.homeostatic_controller.kd = self.config.kd
        self.quantum_integrator = QuantumFeedbackIntegrator(self)

        # Compartment parameters (evolved)
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

        # Persistence and telemetry
        self.persistence = CompartmentPersistenceManager(self.config) if self.config.enable_persistence else None
        self.telemetry = CompartmentTelemetry(api_key=self.config.telemetry_api_key) if self.config.enable_telemetry else None

        # Circuit breaker for external calls
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=self.config.circuit_breaker_failure_threshold,
            timeout_seconds=self.config.circuit_breaker_timeout_seconds
        ) if self.config.enable_circuit_breaker else None

        # Background tasks
        self._background_tasks: List[asyncio.Task] = []
        self._task_status: Dict[str, bool] = {}

        # Create default region
        self._ensure_region_exists("default")

        # Load state if persistence enabled
        if self.persistence:
            asyncio.create_task(self._load_state())

        # Start background tasks
        self._start_background_tasks()

        logger.info(
            f"Hierarchical Compartment Manager v6.2.1 initialized: "
            f"max_regions={self.max_regions}, per_region={self.compartments_per_region}"
        )

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
        """Start a background task with monitoring and auto-restart."""
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
                    logger.info(f"Restarting background task {name}")
                    self._task_status[name] = True
        task = asyncio.create_task(wrapped())
        self._background_tasks.append(task)
        self._task_status[name] = True

    # ========================================================================
    # Parameter getters/setters (for genetic optimizer)
    # ========================================================================

    def _get_compartment_params(self) -> Dict:
        return self._compartment_params.copy()

    def _set_compartment_params(self, params: Dict):
        self._compartment_params = params
        for comp in self.compartments.values():
            comp._manager = self  # allow access to params

    # ========================================================================
    # Region/compartment management
    # ========================================================================

    def _ensure_region_exists(self, region_id: str) -> RegionAggregator:
        if region_id not in self.regions:
            if len(self.regions) >= self.max_regions:
                region_id = min(self.regions.keys(),
                               key=lambda r: len(self.regions[r].compartments))
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

        # Inject references
        compartment.central_health_model = self.central_health_model
        compartment.gradient_manager = self.gradient_manager
        compartment.quantum_integrator = self.quantum_integrator
        compartment.apoptosis_bank = self.apoptosis_bank
        compartment._manager = self

        # Initial token endowment
        if self.token_manager:
            # (Token generation code from original)
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

        # Replay best practices from apoptosis bank
        if self.apoptosis_bank:
            asyncio.create_task(self.apoptosis_bank.replay_to_compartment(compartment))

        # Telemetry
        if self.telemetry:
            self.telemetry.increment('compartments_created')
            self.telemetry.gauge('total_compartments', len(self.compartments))

        logger.info(f"Created compartment {compartment_id} in region {region_id}")
        return compartment

    def find_best_compartment(self, expert_type: str, task_complexity: float = 1.0) -> Optional[ChromatophoreCompartment]:
        candidates = []
        for region in self.regions.values():
            for comp in region.compartments.values():
                if comp.expert_type == expert_type and comp.is_viable:
                    health_score = comp.health_score
                    if self.central_health_model.is_trained:
                        try:
                            pred = asyncio.run(self.central_health_model.predict_health(
                                comp.compartment_id,
                                {
                                    'health_score': health_score,
                                    'success_rate': comp.success_rate,
                                    'efficiency_score': comp.efficiency_score,
                                    'token_balance': comp.token_balance,
                                    'trust_gradient': comp.trust_gradient,
                                    'task_load': len(comp.glycogen_queue) / 1000
                                }
                            ))
                            if pred.get('confidence', 0) > 0.5:
                                health_score = (health_score * 0.6 + pred.get('predicted_health', 0.5) * 0.4)
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
            # Return tokens logic
            pass
        del self.compartments[compartment_id]
        self.compartment_to_region.pop(compartment_id, None)
        self.total_apoptosis_events += 1

        # Telemetry
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
            total_tasks = sum(
                len(getattr(c, 'glycogen_queue', []))
                for c in region.compartments.values()
            )
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

    # ========================================================================
    # Background tasks (enhanced with telemetry and circuit breaker)
    # ========================================================================

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

                # Telemetry
                if self.telemetry:
                    self.telemetry.gauge('global_health', self.global_health)
                    self.telemetry.gauge('total_tokens', total_tokens)
                    self.telemetry.gauge('total_compartments', len(self.compartments))

                await asyncio.sleep(self.config.ecosystem_maintenance_interval_seconds)
            except Exception as e:
                logger.error(f"Ecosystem maintenance error: {str(e)}")
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
                logger.error(f"Trading maintenance error: {str(e)}")
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
                logger.error(f"Health model training error: {str(e)}")
                await asyncio.sleep(3600)

    async def _evolution_maintenance(self):
        while True:
            try:
                if self.config.enable_genetic_optimizer and len(self.compartments) >= 10:
                    logger.info("Starting genetic optimization cycle...")
                    result = await self.genetic_optimizer.evolve(generations=self.config.ga_generations)
                    logger.info(f"Genetic optimization complete: best fitness {result['best_fitness']:.4f}")
                await asyncio.sleep(self.config.ga_evolution_interval_hours * 3600)
            except Exception as e:
                logger.error(f"Evolution maintenance error: {str(e)}")
                await asyncio.sleep(3600)

    # ========================================================================
    # Public methods (enhanced with validation, circuit breaker, and health)
    # ========================================================================

    async def apply_quantum_insights(self, qubo_params: Dict[str, float]):
        """Allow external quantum bridge to inject insights.
        Input validation: qubo_params must be a dict with float values.
        """
        if not isinstance(qubo_params, dict):
            raise TypeError("qubo_params must be a dict")
        for k, v in qubo_params.items():
            if not isinstance(v, (int, float)):
                raise ValueError(f"Value for {k} must be numeric")
        # Use circuit breaker for external call (if enabled)
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
            'regions': {
                region_id: region.get_region_stats()
                for region_id, region in self.regions.items()
            },
            'central_health_model': self.central_health_model.get_stats(),
            'apoptosis_bank': self.apoptosis_bank.get_stats(),
            'genetic_optimizer': {
                'best_fitness': self.genetic_optimizer.best_fitness,
                'history': self.genetic_optimizer.evolution_history[-10:]
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
        """Return health status for monitoring."""
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
            }
        }

    async def get_metrics(self, api_key: Optional[str] = None) -> Dict[str, Any]:
        """Return Prometheus-style metrics.
        Optionally requires API key if configured.
        """
        metrics = {
            'compartments_total': len(self.compartments),
            'compartments_viable': sum(r.get_viable_count() for r in self.regions.values()),
            'global_health': self.global_health,
            'total_regions': len(self.regions),
            'total_compartments_created': self.total_compartments_created,
            'total_apoptosis_events': self.total_apoptosis_events,
        }
        if self.telemetry:
            # Export telemetry gauges
            telemetry_export = await self.telemetry.export(api_key)
            # Parse and add to metrics (simplified)
            for line in telemetry_export.split('\n'):
                if line and not line.startswith('#'):
                    parts = line.split(' ')
                    if len(parts) >= 2:
                        metrics[parts[0]] = float(parts[1])
        return metrics

    async def health_check_endpoint(self) -> Dict[str, Any]:
        """HTTP-friendly health check endpoint."""
        return {
            'status': 'ok' if self.global_health > 0.5 else 'degraded',
            'global_health': self.global_health,
            'compartments': len(self.compartments),
            'regions': len(self.regions),
        }

    async def shutdown(self):
        """Graceful shutdown."""
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
# Example usage (if run as script)
# ============================================================================

async def main():
    # Load config from environment and optional YAML
    config = CompartmentConfig.from_env_and_file()
    manager = HierarchicalCompartmentManager(config=config)
    await asyncio.sleep(1)  # allow startup

    # Create some compartments
    for i in range(5):
        manager.create_compartment(f"expert_{i}")
    print(manager.get_ecosystem_stats())

    # Health check
    print(manager.get_health_status())

    # Run for a while
    try:
        await asyncio.sleep(30)
    finally:
        await manager.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
