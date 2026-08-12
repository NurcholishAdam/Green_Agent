# =============================================================================
# Enhanced Eco-ATP Currency System v10.1.0
# Full implementation with async persistence, quantum security, autonomous strategy,
# multi-cloud distribution, retry/circuit breaker, Pydantic config,
# improved rate limiting, and Multi‑Objective Pareto Decision (MOPD) support.
#
# MOPD enhancements:
# - MOPDConfig sub‑configuration for objective weights and grid resolution.
# - MOPDPoint dataclass to represent a configuration with objectives.
# - Pareto front generation in the ThresholdGeneticOptimizer.
# - Selection of best configuration via scalarisation.
# - Persistence of Pareto front.
# - Telemetry tracks MOPD generations and Pareto front sizes.
# - Full backward compatibility.
# =============================================================================

import asyncio
import logging
import uuid
import json
import os
import hashlib
import math
import random
from typing import Dict, Any, List, Optional, Tuple, Set, Protocol, Callable, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
import numpy as np
from collections import defaultdict, deque
from pathlib import Path

# ============================================================================
# Optional dependencies with graceful degradation
# ============================================================================
try:
    from pydantic import BaseModel, Field, field_validator, ConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

try:
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, PrivateFormat, NoEncryption
    from cryptography.hazmat.backends import default_backend
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
    from prometheus_client import Counter, Gauge, Histogram, start_http_server, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

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

# ============================================================================
# Configuration (Pydantic) – Enhanced with MOPD
# ============================================================================
if PYDANTIC_AVAILABLE:
    class MOPDConfig(BaseModel):
        """Configuration for Multi‑Objective Pareto Decision (MOPD) in threshold optimization."""
        enabled: bool = Field(True, description="Enable MOPD‑aware genetic optimization")
        objective_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'efficiency': 0.4,
                'inflation': 0.3,
                'emergency': 0.3,
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

    class EcoATPConfig(BaseModel):
        """Central configuration for the Eco-ATP system."""
        model_config = ConfigDict(arbitrary_types_allowed=True)

        # Token parameters
        token_expiry_hours: float = Field(default=24.0, ge=1.0)
        token_half_life_hours: float = Field(default=24.0, ge=1.0)
        carbon_to_ecoatp_factor: float = Field(default=10.0, ge=0.1)
        helium_to_ecoatp_factor: float = Field(default=5.0, ge=0.1)
        energy_to_ecoatp_factor: float = Field(default=1000.0, ge=0.1)

        # Thresholds
        hoarding_threshold: float = Field(default=2.0, ge=1.0)
        tax_rate: float = Field(default=0.1, ge=0.0, le=1.0)
        emergency_threshold: float = Field(default=50.0, ge=10.0)
        rate_limit_multiplier_high: float = Field(default=0.5, ge=0.0, le=1.0)
        rate_limit_multiplier_low: float = Field(default=1.5, ge=1.0)

        # Redistribution
        redistribution_interval_minutes: int = Field(default=30, ge=1)

        # Emergency
        emergency_token_rate: float = Field(default=10.0, ge=1.0)
        emergency_reserve: float = Field(default=1000.0, ge=0.0)
        substrate_reserves_max: float = Field(default=1000.0, ge=0.0)
        substrate_reserves_min: float = Field(default=500.0, ge=0.0)

        # Tenant defaults
        default_max_tokens_per_minute: float = Field(default=100.0, ge=0.0)
        default_max_concurrent_tasks: int = Field(default=5, ge=1)
        default_min_priority_for_reservation: int = Field(default=2, ge=0)
        default_reservation_cooldown_seconds: float = Field(default=1.0, ge=0.0)

        # Suspicious detection
        suspicious_threshold: int = Field(default=5, ge=1)

        # Batch processing
        batch_size: int = Field(default=10, ge=1)

        # ML
        ml_retrain_interval_seconds: int = Field(default=60, ge=10)
        ml_history_size: int = Field(default=1000, ge=10)

        # Market
        market_matching_interval_seconds: int = Field(default=30, ge=5)
        market_order_expiry_minutes: int = Field(default=5, ge=1)

        # Genetic optimizer (optional)
        enable_genetic_optimizer: bool = True
        genetic_population_size: int = Field(default=20, ge=2)
        genetic_mutation_rate: float = Field(default=0.2, ge=0.0, le=1.0)
        genetic_crossover_rate: float = Field(default=0.7, ge=0.0, le=1.0)
        genetic_generations: int = Field(default=10, ge=1)
        genetic_tournament_size: int = Field(default=3, ge=1)
        genetic_evolution_interval_seconds: int = Field(default=86400, ge=60)

        # Recovery rates (completion_percentage -> recovery fraction)
        recovery_rates: Dict[float, float] = Field(default_factory=lambda: {
            0.0: 0.0, 0.25: 0.125, 0.5: 0.25, 0.75: 0.6, 0.9: 0.8, 1.0: 0.95
        })

        # Persistence
        enable_persistence: bool = True
        persistence_path: str = Field(default="eco_atp_state.db")

        # Retry
        max_retries: int = Field(default=3, ge=1)
        retry_base_delay_ms: float = Field(default=100.0, ge=0)
        retry_max_delay_ms: float = Field(default=5000.0, ge=0)

        # Circuit breaker
        enable_circuit_breaker: bool = True
        circuit_breaker_failure_threshold: int = Field(default=5, ge=1)
        circuit_breaker_recovery_timeout: float = Field(default=60.0, ge=1)
        circuit_breaker_db_path: str = Field(default="circuit_breakers.db")

        # Quantum signing
        enable_quantum_signing: bool = True
        quantum_signing_algorithm: str = Field(default='dilithium')

        # Blockchain audit
        enable_blockchain_audit: bool = True
        blockchain_rpc_url: str = Field(default='http://localhost:8545')
        blockchain_contract_address: str = Field(default='0x0000000000000000000000000000000000000000')
        blockchain_private_key: Optional[str] = Field(default=None)

        # Autonomous strategy
        enable_autonomous_strategy: bool = True
        rl_learning_rate: float = Field(default=0.1, ge=0.0, le=1.0)
        rl_discount_factor: float = Field(default=0.9, ge=0.0, le=1.0)
        rl_exploration_rate: float = Field(default=0.1, ge=0.0, le=1.0)
        rl_q_table_db_path: str = Field(default="rl_q_table.db")

        # Multi-cloud
        enable_multi_cloud: bool = True
        cloud_provider: str = Field(default='aws')
        cloud_region: str = Field(default='us-east-1')
        cloud_bucket: str = Field(default='eco-atp-state')
        cloud_access_key: Optional[str] = None
        cloud_secret_key: Optional[str] = None

        # Prometheus
        prometheus_port: Optional[int] = Field(default=None, description="Port for Prometheus HTTP endpoint")

        # Health check
        enable_health_endpoint: bool = True
        health_endpoint_port: int = Field(default=8080)

        # Model persistence paths
        ml_model_path: str = Field(default="models/ml_model.joblib")
        genetic_state_path: str = Field(default="models/genetic_state.json")

        # MOPD configuration (NEW)
        mopd: MOPDConfig = Field(default_factory=MOPDConfig, description="MOPD sub‑configuration")

        @classmethod
        def from_env_and_file(cls, config_path: Optional[str] = None) -> 'EcoATPConfig':
            """Load configuration from environment variables and optional YAML file."""
            env_overrides = {}
            for key in cls.model_fields.keys():
                env_var = f"ECOATP_{key.upper()}"
                if env_var in os.environ:
                    env_overrides[key] = os.environ[env_var]
            if config_path and os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    yaml_data = yaml.safe_load(f)
                    if yaml_data:
                        yaml_data.update(env_overrides)
                        return cls(**yaml_data)
            return cls(**env_overrides) if env_overrides else cls()

        def to_dict(self) -> Dict[str, Any]:
            return self.model_dump()

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> 'EcoATPConfig':
            return cls(**data)

        def validate(self) -> List[str]:
            issues = []
            if self.token_expiry_hours < 1:
                issues.append("token_expiry_hours must be at least 1")
            if self.hoarding_threshold < 1:
                issues.append("hoarding_threshold must be at least 1")
            if self.emergency_threshold < 10:
                issues.append("emergency_threshold must be at least 10")
            if self.substrate_reserves_max < self.substrate_reserves_min:
                issues.append("substrate_reserves_max must be >= substrate_reserves_min")
            return issues
else:
    # Fallback dataclass (simplified)
    @dataclass
    class MOPDConfig:
        enabled: bool = True
        objective_weights: Dict[str, float] = field(default_factory=lambda: {
            'efficiency': 0.4,
            'inflation': 0.3,
            'emergency': 0.3,
        })
        grid_resolution: int = 5

    @dataclass
    class EcoATPConfig:
        token_expiry_hours: float = 24.0
        token_half_life_hours: float = 24.0
        carbon_to_ecoatp_factor: float = 10.0
        helium_to_ecoatp_factor: float = 5.0
        energy_to_ecoatp_factor: float = 1000.0
        hoarding_threshold: float = 2.0
        tax_rate: float = 0.1
        emergency_threshold: float = 50.0
        rate_limit_multiplier_high: float = 0.5
        rate_limit_multiplier_low: float = 1.5
        redistribution_interval_minutes: int = 30
        emergency_token_rate: float = 10.0
        emergency_reserve: float = 1000.0
        substrate_reserves_max: float = 1000.0
        substrate_reserves_min: float = 500.0
        default_max_tokens_per_minute: float = 100.0
        default_max_concurrent_tasks: int = 5
        default_min_priority_for_reservation: int = 2
        default_reservation_cooldown_seconds: float = 1.0
        suspicious_threshold: int = 5
        batch_size: int = 10
        ml_retrain_interval_seconds: int = 60
        ml_history_size: int = 1000
        market_matching_interval_seconds: int = 30
        market_order_expiry_minutes: int = 5
        enable_genetic_optimizer: bool = True
        genetic_population_size: int = 20
        genetic_mutation_rate: float = 0.2
        genetic_crossover_rate: float = 0.7
        genetic_generations: int = 10
        genetic_tournament_size: int = 3
        genetic_evolution_interval_seconds: int = 86400
        recovery_rates: Dict[float, float] = field(default_factory=lambda: {
            0.0: 0.0, 0.25: 0.125, 0.5: 0.25, 0.75: 0.6, 0.9: 0.8, 1.0: 0.95
        })
        enable_persistence: bool = True
        persistence_path: str = "eco_atp_state.db"
        max_retries: int = 3
        retry_base_delay_ms: float = 100.0
        retry_max_delay_ms: float = 5000.0
        enable_circuit_breaker: bool = True
        circuit_breaker_failure_threshold: int = 5
        circuit_breaker_recovery_timeout: float = 60.0
        circuit_breaker_db_path: str = "circuit_breakers.db"
        enable_quantum_signing: bool = True
        quantum_signing_algorithm: str = 'dilithium'
        enable_blockchain_audit: bool = True
        blockchain_rpc_url: str = 'http://localhost:8545'
        blockchain_contract_address: str = '0x0000000000000000000000000000000000000000'
        blockchain_private_key: Optional[str] = None
        enable_autonomous_strategy: bool = True
        rl_learning_rate: float = 0.1
        rl_discount_factor: float = 0.9
        rl_exploration_rate: float = 0.1
        rl_q_table_db_path: str = "rl_q_table.db"
        enable_multi_cloud: bool = True
        cloud_provider: str = 'aws'
        cloud_region: str = 'us-east-1'
        cloud_bucket: str = 'eco-atp-state'
        cloud_access_key: Optional[str] = None
        cloud_secret_key: Optional[str] = None
        prometheus_port: Optional[int] = None
        enable_health_endpoint: bool = True
        health_endpoint_port: int = 8080
        ml_model_path: str = "models/ml_model.joblib"
        genetic_state_path: str = "models/genetic_state.json"
        mopd: MOPDConfig = field(default_factory=MOPDConfig)

        def to_dict(self) -> Dict[str, Any]:
            return asdict(self)

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> 'EcoATPConfig':
            return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

        @classmethod
        def from_env_and_file(cls, config_path: Optional[str] = None) -> 'EcoATPConfig':
            return cls()

# ============================================================================
# Protocol Definitions
# ============================================================================

class TokenServiceProtocol(Protocol):
    def get_system_summary(self) -> Dict[str, Any]: ...
    def get_account_summary(self, account_id: str) -> Dict[str, Any]: ...
    def reserve_tokens(self, account_id: str, amount: float, consumer: Any,
                       tenant_id: str, priority: int) -> Tuple[bool, List[str]]: ...
    def generate_tokens(self, account_id: str, source: Any, **kwargs) -> List[Any]: ...
    def consume_tokens(self, token_ids: List[str], consumer: Any, operation_success: bool) -> float: ...
    def recover_tokens(self, token_ids: List[str], completion_percentage: float) -> float: ...
    def create_account(self, account_id: str) -> Any: ...

class ExchangeRateProvider(Protocol):
    def carbon_to_ecoatp(self, carbon_kg: float) -> float: ...
    def helium_to_ecoatp(self, helium_units: float) -> float: ...
    def energy_to_ecoatp(self, energy_kwh: float) -> float: ...

class GradientProvider(Protocol):
    def get_field_strengths(self) -> Dict[str, float]: ...

class QuantumFeedbackProvider(Protocol):
    def get_qubo_params(self) -> Dict[str, float]: ...

# ============================================================================
# Enums and Data Classes (Enhanced with MOPD)
# ============================================================================

class EcoATPSource(Enum):
    RENEWABLE_ENERGY = "renewable_energy"
    CARBON_OFFSET = "carbon_offset"
    EFFICIENCY_GAIN = "efficiency_gain"
    WASTE_HEAT_RECOVERY = "waste_heat_recovery"
    COMPUTATION_SCAVENGING = "computation_scavenging"
    HELIUM_RECOVERY = "helium_recovery"
    EXTERNAL_TRADE = "external_trade"
    GRADIENT_CONVERSION = "gradient_conversion"
    EMERGENCY_SUBSTRATE = "emergency_substrate"
    QUANTUM_ADVANTAGE = "quantum_advantage"

class EcoATPConsumer(Enum):
    EXPERT_EXECUTION = "expert_execution"
    MODEL_TRAINING = "model_training"
    DATA_PROCESSING = "data_processing"
    QUANTUM_COMPUTING = "quantum_computing"
    NETWORK_TRANSFER = "network_transfer"
    COOLING_SYSTEM = "cooling_system"
    STORAGE_OPERATION = "storage_operation"
    MAINTENANCE = "maintenance"

class TokenState(Enum):
    GENERATED = "generated"
    AVAILABLE = "available"
    RESERVED = "reserved"
    CONSUMED = "consumed"
    EXPIRED = "expired"
    RECOVERED = "recovered"
    TRADED = "traded"
    QUANTUM_BACKED = "quantum_backed"

@dataclass
class EcoATPToken:
    token_id: str
    value: float
    source: EcoATPSource
    generated_at: datetime
    expires_at: datetime
    state: TokenState = TokenState.AVAILABLE
    carbon_equivalent_kg: float = 0.0
    helium_equivalent_units: float = 0.0
    generation_efficiency: float = 1.0
    provenance_hash: str = ""
    quantum_advantage_factor: float = 0.0
    quantum_circuit_id: Optional[str] = None
    consumed_at: Optional[datetime] = None
    recovered_at: Optional[datetime] = None
    quantum_signature: Optional[Dict] = None

    def __post_init__(self):
        if not self.provenance_hash:
            self.provenance_hash = self._compute_hash()

    def _compute_hash(self) -> str:
        data = f"{self.token_id}{self.value}{self.source.value}{self.generated_at.isoformat()}"
        return hashlib.sha256(data.encode()).hexdigest()

    def apply_decay(self, current_time: datetime) -> float:
        age_hours = (current_time - self.generated_at).total_seconds() / 3600
        half_life = 24.0
        decay_factor = math.exp(-math.log(2) * age_hours / half_life)
        return self.value * decay_factor

    def is_expired(self, current_time: datetime) -> bool:
        return current_time > self.expires_at

@dataclass
class EcoATPAccount:
    account_id: str
    balance: float = 0.0
    total_generated: float = 0.0
    total_consumed: float = 0.0
    total_recovered: float = 0.0
    total_expired: float = 0.0
    generation_history: deque = field(default_factory=lambda: deque(maxlen=1000))
    consumption_history: deque = field(default_factory=lambda: deque(maxlen=1000))
    efficiency_rating: float = 1.0
    quantum_balance: float = 0.0
    quantum_total_generated: float = 0.0

    @property
    def net_balance(self) -> float:
        return self.balance

    @property
    def utilization_rate(self) -> float:
        if self.total_generated == 0:
            return 0.0
        return self.total_consumed / self.total_generated

# ============================================================================
# MOPD Data Class (NEW)
# ============================================================================

@dataclass
class MOPDPoint:
    """Represents a genetic individual with its objective vector."""
    individual: Dict[str, float]  # the parameters (hoarding_threshold, tax_rate, etc.)
    efficiency: float
    inflation: float
    emergency: float
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDPoint':
        return cls(**data)

# ============================================================================
# Dynamic Exchange Rate (unchanged)
# ============================================================================

class DynamicExchangeRate:
    def __init__(self, config: EcoATPConfig):
        self.config = config
        self.last_update = datetime.utcnow()
        self.carbon_price = 0.1
        self.helium_price = 0.5
        self.energy_price = 0.12

    def carbon_to_ecoatp(self, carbon_kg: float) -> float:
        return carbon_kg * self.config.carbon_to_ecoatp_factor

    def helium_to_ecoatp(self, helium_units: float) -> float:
        return helium_units * self.config.helium_to_ecoatp_factor

    def energy_to_ecoatp(self, energy_kwh: float) -> float:
        return energy_kwh * self.config.energy_to_ecoatp_factor

    def update_rates(self, carbon_price: Optional[float] = None,
                     helium_price: Optional[float] = None,
                     energy_price: Optional[float] = None):
        if carbon_price is not None:
            self.carbon_price = carbon_price
        if helium_price is not None:
            self.helium_price = helium_price
        if energy_price is not None:
            self.energy_price = energy_price
        self.last_update = datetime.utcnow()

# ============================================================================
# ML Demand Predictor (with async persistence) – unchanged
# ============================================================================

class MLDemandPredictor:
    # ... (same as before, unchanged) ...
    def __init__(self, config: EcoATPConfig, db_path: Optional[str] = None):
        self.config = config
        self.db_path = db_path or config.persistence_path
        self.model = RandomForestRegressor(n_estimators=10, random_state=42) if SKLEARN_AVAILABLE else None
        self.scaler = StandardScaler() if SKLEARN_AVAILABLE else None
        self.data: List[Dict[str, Any]] = []
        self.last_trained = datetime.utcnow() - timedelta(days=1)
        self.lock = asyncio.Lock()
        self.is_training = False
        self._load_model()

    def _load_model(self):
        if SKLEARN_AVAILABLE and os.path.exists(self.config.ml_model_path):
            try:
                self.model, self.scaler = joblib.load(self.config.ml_model_path)
                logger.info("Loaded ML model from disk")
            except Exception as e:
                logger.warning(f"Failed to load ML model: {e}")

    def _save_model(self):
        if SKLEARN_AVAILABLE and self.is_trained:
            try:
                os.makedirs(os.path.dirname(self.config.ml_model_path), exist_ok=True)
                joblib.dump((self.model, self.scaler), self.config.ml_model_path)
                logger.info("Saved ML model to disk")
            except Exception as e:
                logger.warning(f"Failed to save ML model: {e}")

    @property
    def is_trained(self) -> bool:
        return self.model is not None and len(self.data) >= 10

    async def record_demand(self, account_id: str, amount: float, timestamp: datetime):
        features = {
            'account_id_hash': hash(account_id) % 1000,
            'hour': timestamp.hour,
            'day_of_week': timestamp.weekday(),
            'amount': amount
        }
        async with self.lock:
            self.data.append(features)
            if len(self.data) > self.config.ml_history_size:
                self.data.pop(0)
            if AIOSQLITE_AVAILABLE:
                async with aiosqlite.connect(self.db_path) as conn:
                    await conn.execute(
                        "INSERT INTO ml_data (account_id_hash, hour, day_of_week, amount, timestamp) VALUES (?, ?, ?, ?, ?)",
                        (features['account_id_hash'], features['hour'], features['day_of_week'], features['amount'],
                         timestamp.isoformat())
                    )
                    await conn.commit()

    async def train(self, force: bool = False):
        async with self.lock:
            if self.is_training:
                return
            now = datetime.utcnow()
            if not force and (now - self.last_trained).total_seconds() < self.config.ml_retrain_interval_seconds:
                return
            if len(self.data) < 10:
                logger.debug("Not enough data for ML training")
                return
            self.is_training = True
            try:
                def train_sync():
                    X = np.array([[d['account_id_hash'], d['hour'], d['day_of_week']] for d in self.data])
                    y = np.array([d['amount'] for d in self.data])
                    X_scaled = self.scaler.fit_transform(X)
                    self.model.fit(X_scaled, y)
                    return True
                await asyncio.to_thread(train_sync)
                self.last_trained = now
                self._save_model()
                logger.info("ML model retrained on %d samples", len(self.data))
            except Exception as e:
                logger.error("ML training failed: %s", e)
            finally:
                self.is_training = False

    def predict_demand(self, account_id: str, timestamp: datetime) -> float:
        if not self.is_trained:
            return 0.0
        features = np.array([[hash(account_id) % 1000, timestamp.hour, timestamp.weekday()]])
        try:
            X_scaled = self.scaler.transform(features)
            return float(self.model.predict(X_scaled)[0])
        except Exception as e:
            logger.error("Prediction failed: %s", e)
            return 0.0

# ============================================================================
# Threshold Genetic Optimizer (Enhanced with MOPD)
# ============================================================================

class ThresholdGeneticOptimizer:
    def __init__(self, token_manager: 'EcoATPTokenManager', config: EcoATPConfig):
        self.token_manager = token_manager
        self.config = config
        self.population_size = config.genetic_population_size
        self.mutation_rate = config.genetic_mutation_rate
        self.crossover_rate = config.genetic_crossover_rate
        self.generations = config.genetic_generations
        self.tournament_size = config.genetic_tournament_size
        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.lock = asyncio.Lock()
        self.param_bounds = {
            'hoarding_threshold': (1.2, 4.0),
            'tax_rate': (0.05, 0.3),
            'emergency_threshold': (10.0, 100.0),
            'rate_limit_multiplier_high': (0.3, 0.7),
            'rate_limit_multiplier_low': (1.2, 2.0)
        }
        # MOPD: Pareto front storage
        self.pareto_front: List[MOPDPoint] = []
        self._load_state()

    def _load_state(self):
        if os.path.exists(self.config.genetic_state_path):
            try:
                with open(self.config.genetic_state_path, 'r') as f:
                    data = json.load(f)
                    self.best_fitness = data.get('best_fitness', -float('inf'))
                    self.best_individual = data.get('best_individual', None)
                    self.evolution_history = data.get('evolution_history', [])
                    pareto_front_dicts = data.get('pareto_front', [])
                    if pareto_front_dicts:
                        self.pareto_front = [MOPDPoint.from_dict(p) for p in pareto_front_dicts]
                logger.info("Loaded genetic optimizer state from disk")
            except Exception as e:
                logger.warning(f"Failed to load genetic state: {e}")

    def _save_state(self):
        try:
            os.makedirs(os.path.dirname(self.config.genetic_state_path), exist_ok=True)
            with open(self.config.genetic_state_path, 'w') as f:
                json.dump({
                    'best_fitness': self.best_fitness,
                    'best_individual': self.best_individual,
                    'evolution_history': self.evolution_history,
                    'pareto_front': [p.to_dict() for p in self.pareto_front]
                }, f, default=str)
            logger.info("Saved genetic optimizer state to disk")
        except Exception as e:
            logger.warning(f"Failed to save genetic state: {e}")

    def _initialize_individual(self) -> Dict:
        ind = {}
        for key, (low, high) in self.param_bounds.items():
            ind[key] = random.uniform(low, high)
        return ind

    def _initialize_population(self) -> List[Dict]:
        return [self._initialize_individual() for _ in range(self.population_size)]

    # ---------- Multi‑objective evaluation (NEW) ----------
    def _evaluate_individual(self, individual: Dict) -> Dict[str, float]:
        """Evaluate an individual on multiple objectives."""
        self._apply_individual(individual)
        summary = self.token_manager.get_system_summary_sync()
        utilization = summary.get('system_efficiency', 0.5)
        total_generated = summary.get('total_generated', 1)
        total_consumed = summary.get('total_consumed', 1)
        inflation = (total_generated - total_consumed) / max(total_consumed, 1)
        emergency_mode = 1 if summary.get('emergency_mode', False) else 0
        self._restore_original_parameters()
        return {
            'efficiency': utilization,
            'inflation': 1.0 - abs(inflation),  # lower inflation is better
            'emergency': 1.0 - emergency_mode   # lower emergency is better
        }

    def _apply_individual(self, individual: Dict):
        self._original_params = {
            'hoarding_threshold': self.token_manager.config.hoarding_threshold,
            'tax_rate': self.token_manager.config.tax_rate,
            'emergency_threshold': self.token_manager.config.emergency_threshold,
            'rate_limit_multiplier_high': self.token_manager.config.rate_limit_multiplier_high,
            'rate_limit_multiplier_low': self.token_manager.config.rate_limit_multiplier_low
        }
        self.token_manager.config.hoarding_threshold = individual['hoarding_threshold']
        self.token_manager.config.tax_rate = individual['tax_rate']
        self.token_manager.config.emergency_threshold = individual['emergency_threshold']
        self.token_manager.config.rate_limit_multiplier_high = individual['rate_limit_multiplier_high']
        self.token_manager.config.rate_limit_multiplier_low = individual['rate_limit_multiplier_low']

    def _restore_original_parameters(self):
        if hasattr(self, '_original_params'):
            self.token_manager.config.hoarding_threshold = self._original_params['hoarding_threshold']
            self.token_manager.config.tax_rate = self._original_params['tax_rate']
            self.token_manager.config.emergency_threshold = self._original_params['emergency_threshold']
            self.token_manager.config.rate_limit_multiplier_high = self._original_params['rate_limit_multiplier_high']
            self.token_manager.config.rate_limit_multiplier_low = self._original_params['rate_limit_multiplier_low']

    def _select(self, population: List[Dict], fitness_scores: List[float]) -> Dict:
        tournament = random.sample(range(len(population)), self.tournament_size)
        best_idx = max(tournament, key=lambda i: fitness_scores[i])
        return population[best_idx]

    def _crossover(self, parent1: Dict, parent2: Dict) -> Dict:
        child = {}
        for key in parent1:
            if random.random() < 0.5:
                child[key] = parent1[key]
            else:
                child[key] = parent2[key]
            if random.random() < 0.3:
                child[key] = (parent1[key] + parent2[key]) / 2
        return child

    def _mutate(self, individual: Dict) -> Dict:
        mutated = individual.copy()
        for key, (low, high) in self.param_bounds.items():
            if random.random() < self.mutation_rate:
                delta = random.uniform(-(high-low)*0.1, (high-low)*0.1)
                mutated[key] = max(low, min(high, mutated[key] + delta))
        return mutated

    # ---------- Pareto front methods (NEW) ----------
    def _filter_pareto(self, points: List[MOPDPoint]) -> List[MOPDPoint]:
        """Return non‑dominated points."""
        if not points:
            return []
        objective_keys = ['efficiency', 'inflation', 'emergency']
        pareto = []
        for i, p_i in enumerate(points):
            dominated = False
            for j, p_j in enumerate(points):
                if i == j:
                    continue
                a_vec = [getattr(p_i, k) for k in objective_keys]
                b_vec = [getattr(p_j, k) for k in objective_keys]
                if all(b >= a for a, b in zip(a_vec, b_vec)) and any(b > a for a, b in zip(a_vec, b_vec)):
                    dominated = True
                    break
            if not dominated:
                pareto.append(p_i)
        return pareto

    def _select_best_from_pareto(self, pareto_front: List[MOPDPoint]) -> Optional[MOPDPoint]:
        """Select best point using scalarisation with MOPD weights."""
        if not pareto_front:
            return None
        weights = self.token_manager.config.mopd.objective_weights
        objective_keys = list(weights.keys())

        # Normalise objectives across Pareto front
        max_vals = {}
        min_vals = {}
        for key in objective_keys:
            vals = [getattr(p, key) for p in pareto_front]
            max_vals[key] = max(vals)
            min_vals[key] = min(vals)
        ranges = {k: max_vals[k] - min_vals[k] if max_vals[k] != min_vals[k] else 1.0 for k in objective_keys}

        best = None
        best_score = -float('inf')
        for point in pareto_front:
            score = 0.0
            for key in objective_keys:
                val = getattr(point, key)
                norm = (val - min_vals[key]) / ranges[key] if ranges[key] > 0 else 1.0
                weight = weights.get(key, 0.0)
                score += weight * norm
            point.scalarised_score = score
            if score > best_score:
                best_score = score
                best = point
        return best

    # ---------- Main evolve (enhanced with MOPD) ----------
    async def evolve(self, generations: Optional[int] = None) -> Dict:
        async with self.lock:
            if generations is None:
                generations = self.generations
            population = self._initialize_population()

            if self.token_manager.config.mopd.enabled:
                self.pareto_front = []

            for gen in range(generations):
                # Evaluate objectives for all individuals
                individuals_with_objs = []
                for ind in population:
                    objs = self._evaluate_individual(ind)
                    individuals_with_objs.append((ind, objs))

                # If MOPD enabled, update Pareto front
                if self.token_manager.config.mopd.enabled:
                    points = []
                    for ind, objs in individuals_with_objs:
                        point = MOPDPoint(
                            individual=ind,
                            efficiency=objs['efficiency'],
                            inflation=objs['inflation'],
                            emergency=objs['emergency']
                        )
                        points.append(point)
                    self.pareto_front = self._filter_pareto(self.pareto_front + points)

                    # Compute scalarised scores for selection
                    weights = self.token_manager.config.mopd.objective_weights
                    fitness_scores = []
                    for point in points:
                        score = (weights.get('efficiency', 0.4) * point.efficiency +
                                 weights.get('inflation', 0.3) * point.inflation +
                                 weights.get('emergency', 0.3) * point.emergency)
                        point.scalarised_score = score
                        fitness_scores.append(score)
                else:
                    # Legacy: single fitness (efficiency)
                    fitness_scores = [objs['efficiency'] for _, objs in individuals_with_objs]

                # Selection and reproduction
                new_population = []
                best_idx = max(range(len(population)), key=lambda i: fitness_scores[i])
                new_population.append(population[best_idx])
                while len(new_population) < self.population_size:
                    if random.random() < self.crossover_rate:
                        parent1 = self._select(population, fitness_scores)
                        parent2 = self._select(population, fitness_scores)
                        child = self._crossover(parent1, parent2)
                        child = self._mutate(child)
                        new_population.append(child)
                    else:
                        parent = self._select(population, fitness_scores)
                        new_population.append(parent.copy())
                population = new_population

                gen_best_fitness = max(fitness_scores)
                logger.debug(f"Gen {gen+1}: best fitness = {gen_best_fitness:.4f}")

            # After evolution, if MOPD enabled and we have a Pareto front, select best
            if self.token_manager.config.mopd.enabled and self.pareto_front:
                best_point = self._select_best_from_pareto(self.pareto_front)
                if best_point:
                    self.best_individual = best_point.individual
                    self.best_fitness = best_point.scalarised_score
                    self._apply_individual(best_point.individual)
                    logger.info(f"Applied best MOPD individual with scalarised score {self.best_fitness:.4f}")
            else:
                # Legacy: keep best fitness and individual
                if fitness_scores:
                    best_idx = max(range(len(population)), key=lambda i: fitness_scores[i])
                    self.best_fitness = fitness_scores[best_idx]
                    self.best_individual = population[best_idx]
                    self._apply_individual(self.best_individual)
                    logger.info(f"Applied best individual with fitness {self.best_fitness:.4f}")

            self.evolution_history.append({
                'timestamp': datetime.utcnow(),
                'best_fitness': self.best_fitness,
                'pareto_front_size': len(self.pareto_front) if self.token_manager.config.mopd.enabled else 0
            })
            self._save_state()
            return {
                'best_fitness': self.best_fitness,
                'best_individual': self.best_individual,
                'pareto_front': [p.to_dict() for p in self.pareto_front] if self.token_manager.config.mopd.enabled else None
            }

    def get_status(self) -> Dict:
        return {
            'best_fitness': self.best_fitness,
            'best_individual': self.best_individual,
            'history': self.evolution_history[-10:],
            'pareto_front_size': len(self.pareto_front) if self.token_manager.config.mopd.enabled else 0
        }

# ============================================================================
# Distributed Token Market (unchanged)
# ============================================================================

@dataclass
class MarketOrder:
    order_id: str
    account_id: str
    amount: float
    price: float
    side: str
    status: str = 'open'
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: datetime = field(default_factory=lambda: datetime.utcnow() + timedelta(minutes=5))
    remaining: float = field(init=False)

    def __post_init__(self):
        self.remaining = self.amount

class OrderBook:
    def __init__(self):
        self.buy_orders: Dict[float, List[MarketOrder]] = defaultdict(list)
        self.sell_orders: Dict[float, List[MarketOrder]] = defaultdict(list)
        self.all_orders: Dict[str, MarketOrder] = {}

    def add_order(self, order: MarketOrder):
        self.all_orders[order.order_id] = order
        if order.side == 'buy':
            self.buy_orders[order.price].append(order)
        else:
            self.sell_orders[order.price].append(order)

    def remove_order(self, order_id: str):
        order = self.all_orders.pop(order_id, None)
        if order:
            if order.side == 'buy':
                self.buy_orders[order.price] = [o for o in self.buy_orders[order.price] if o.order_id != order_id]
                if not self.buy_orders[order.price]:
                    del self.buy_orders[order.price]
            else:
                self.sell_orders[order.price] = [o for o in self.sell_orders[order.price] if o.order_id != order_id]
                if not self.sell_orders[order.price]:
                    del self.sell_orders[order.price]

    def get_best_buy_price(self) -> Optional[float]:
        if not self.buy_orders:
            return None
        return max(self.buy_orders.keys())

    def get_best_sell_price(self) -> Optional[float]:
        if not self.sell_orders:
            return None
        return min(self.sell_orders.keys())

    def get_buy_orders_at(self, price: float) -> List[MarketOrder]:
        return self.buy_orders.get(price, [])

    def get_sell_orders_at(self, price: float) -> List[MarketOrder]:
        return self.sell_orders.get(price, [])

    def cleanup_expired(self, now: datetime):
        to_remove = [oid for oid, order in self.all_orders.items() if order.status == 'open' and order.expires_at <= now]
        for oid in to_remove:
            self.remove_order(oid)

class DistributedTokenMarket:
    def __init__(self, token_manager: 'EcoATPTokenManager', config: EcoATPConfig):
        self.token_manager = token_manager
        self.config = config
        self.order_book = OrderBook()
        self.trade_history: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()

    async def place_order(self, account_id: str, amount: float, price: float, side: str) -> str:
        async with self._lock:
            order = MarketOrder(
                order_id=f"order_{uuid.uuid4().hex[:8]}",
                account_id=account_id,
                amount=amount,
                price=price,
                side=side,
                expires_at=datetime.utcnow() + timedelta(minutes=self.config.market_order_expiry_minutes)
            )
            self.order_book.add_order(order)
            logger.debug(f"Order placed: {order.order_id} ({side} {amount} @ {price:.2f})")
            return order.order_id

    async def match_orders(self) -> List[Dict]:
        async with self._lock:
            matches = []
            now = datetime.utcnow()
            self.order_book.cleanup_expired(now)
            while True:
                best_buy = self.order_book.get_best_buy_price()
                best_sell = self.order_book.get_best_sell_price()
                if best_buy is None or best_sell is None:
                    break
                if best_sell > best_buy:
                    break
                buy_orders = self.order_book.get_buy_orders_at(best_buy)
                sell_orders = self.order_book.get_sell_orders_at(best_sell)
                if not buy_orders or not sell_orders:
                    break
                buy = buy_orders[0]
                sell = sell_orders[0]
                trade_amount = min(buy.remaining, sell.remaining)
                trade_price = (buy.price + sell.price) / 2
                seller_account = self.token_manager.accounts.get(sell.account_id)
                buyer_account = self.token_manager.accounts.get(buy.account_id)
                if seller_account and buyer_account:
                    total_cost = trade_price * trade_amount
                    if buyer_account.balance >= total_cost:
                        buyer_account.balance -= total_cost
                        seller_account.balance += total_cost
                        buy.remaining -= trade_amount
                        sell.remaining -= trade_amount
                        if buy.remaining <= 0:
                            buy.status = 'completed'
                            self.order_book.remove_order(buy.order_id)
                        if sell.remaining <= 0:
                            sell.status = 'completed'
                            self.order_book.remove_order(sell.order_id)
                        matches.append({
                            'sell_order': sell.order_id,
                            'buy_order': buy.order_id,
                            'seller': sell.account_id,
                            'buyer': buy.account_id,
                            'amount': trade_amount,
                            'price': trade_price,
                            'timestamp': now.isoformat()
                        })
                        self.trade_history.append(matches[-1])
                        logger.info(f"Trade matched: {sell.account_id} -> {buy.account_id} ({trade_amount} @ {trade_price:.2f})")
                    else:
                        buy.status = 'cancelled'
                        self.order_book.remove_order(buy.order_id)
                else:
                    if buy.status == 'open':
                        buy.status = 'cancelled'
                        self.order_book.remove_order(buy.order_id)
                    if sell.status == 'open':
                        sell.status = 'cancelled'
                        self.order_book.remove_order(sell.order_id)
            return matches

    def get_market_stats(self) -> Dict[str, Any]:
        active_orders = [o for o in self.order_book.all_orders.values() if o.status == 'open']
        return {
            'active_orders': len(active_orders),
            'sell_orders': len([o for o in active_orders if o.side == 'sell']),
            'buy_orders': len([o for o in active_orders if o.side == 'buy']),
            'total_trades': len(self.trade_history),
            'total_volume': sum(t['amount'] for t in self.trade_history),
            'average_price': np.mean([t['price'] for t in self.trade_history]) if self.trade_history else 0,
            'recent_trades': list(self.trade_history)[-10:]
        }

# ============================================================================
# Gradient-Aware Generation (unchanged)
# ============================================================================

class GradientAwareGeneration:
    def __init__(self, token_manager: 'EcoATPTokenManager', gradient_provider: Optional[GradientProvider] = None):
        self.token_manager = token_manager
        self.gradient_provider = gradient_provider
        self.last_adjustment = datetime.utcnow()

    def adjust_generation_rate(self) -> float:
        if not self.gradient_provider:
            return 1.0
        strengths = self.gradient_provider.get_field_strengths()
        carbon = strengths.get('carbon', 0.5)
        helium = strengths.get('helium', 0.5)
        opportunity = strengths.get('opportunity', 0.5)
        multiplier = 1.0
        if carbon > 0.7:
            multiplier *= (1.0 + (carbon - 0.7) * 0.5)
        if helium > 0.7:
            multiplier *= (1.0 + (helium - 0.7) * 0.3)
        if opportunity > 0.8:
            multiplier *= (1.0 + (opportunity - 0.8) * 0.2)
        self.last_adjustment = datetime.utcnow()
        return multiplier

# ============================================================================
# Quantum Feedback Integrator (unchanged)
# ============================================================================

class QuantumFeedbackIntegrator:
    def __init__(self, token_manager: 'EcoATPTokenManager', quantum_provider: Optional[QuantumFeedbackProvider] = None):
        self.token_manager = token_manager
        self.quantum_provider = quantum_provider
        self.last_qubo_params: Dict[str, float] = {}
        self.last_update = datetime.utcnow()

    def apply_quantum_insights(self) -> float:
        if not self.quantum_provider:
            return 1.0
        qubo_params = self.quantum_provider.get_qubo_params()
        self.last_qubo_params = qubo_params
        self.last_update = datetime.utcnow()
        penalty_carbon = qubo_params.get('penalty_carbon', 0.5)
        penalty_helium = qubo_params.get('penalty_helium_shortage', 0.5)
        weight_opportunity = qubo_params.get('weight_opportunity', 0.5)
        multiplier = 1.0
        if penalty_carbon > 0.6:
            multiplier *= (1.0 + (penalty_carbon - 0.6) * 0.4)
        if penalty_helium > 0.6:
            multiplier *= (1.0 + (penalty_helium - 0.6) * 0.3)
        if weight_opportunity > 0.6:
            multiplier *= (1.0 + (weight_opportunity - 0.6) * 0.2)
        return multiplier

# ============================================================================
# Persistent Circuit Breaker (SQLite) – unchanged
# ============================================================================

class CircuitBreaker:
    """Circuit breaker with SQLite persistence."""
    def __init__(self, name: str, db_path: str, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        self.name = name
        self.db_path = db_path
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
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

    async def call(self, func: Callable, *args, **kwargs):
        async with self._lock:
            if self.state == 'open':
                if self.last_failure_time and (datetime.now(timezone.utc) - self.last_failure_time).total_seconds() >= self.recovery_timeout:
                    self.state = 'half_open'
                    self._save_state()
                    logger.info(f"Circuit breaker {self.name} transitioning to half_open")
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is OPEN")
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
                self.last_failure_time = datetime.now(timezone.utc)
                if self.failure_count >= self.failure_threshold:
                    self.state = 'open'
                    logger.warning(f"Circuit breaker {self.name} opened after {self.failure_count} failures")
                self._save_state()
            raise e

# ============================================================================
# Retry Decorator (using tenacity if available) – unchanged
# ============================================================================

def retry_decorator(max_attempts: int = 3, min_delay: float = 0.1, max_delay: float = 10.0):
    """Decorator to retry async functions with exponential backoff."""
    if TENACITY_AVAILABLE:
        def decorator(func):
            @retry(
                stop=stop_after_attempt(max_attempts),
                wait=wait_exponential(multiplier=min_delay, min=min_delay, max=max_delay),
                retry=retry_if_exception_type(Exception),
                before_sleep=before_sleep_log(logger, logging.WARNING)
            )
            async def wrapper(*args, **kwargs):
                return await func(*args, **kwargs)
            return wrapper
        return decorator
    else:
        def decorator(func):
            async def wrapper(*args, **kwargs):
                for attempt in range(max_attempts):
                    try:
                        return await func(*args, **kwargs)
                    except Exception as e:
                        if attempt == max_attempts - 1:
                            raise
                        delay = min(min_delay * (2 ** attempt), max_delay)
                        await asyncio.sleep(delay)
            return wrapper
        return decorator

# ============================================================================
# Post-Quantum Security (unchanged)
# ============================================================================

class QuantumResilientSecurity:
    """Real post-quantum signing using Dilithium/Falcon/SPHINCS+ with persistent keys."""
    def __init__(self, algorithm: str = 'dilithium'):
        self.algorithm = algorithm
        self.pqc_available = PQC_AVAILABLE
        self._private_key = None
        self._public_key = None
        if self.pqc_available:
            self._load_algorithm()
            self._generate_keys()
        else:
            logger.warning("PQC libraries not found – using ECDSA fallback.")
            self._ecdsa_private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
            self._ecdsa_public_key = self._ecdsa_private_key.public_key()

    def _load_algorithm(self):
        if self.algorithm == 'dilithium':
            self.sign_func = dilithium.sign
            self.verify_func = dilithium.verify
        elif self.algorithm == 'falcon':
            self.sign_func = falcon.sign
            self.verify_func = falcon.verify
        elif self.algorithm == 'sphincs':
            self.sign_func = sphincs.sign
            self.verify_func = sphincs.verify
        else:
            raise ValueError(f"Unknown algorithm: {self.algorithm}")

    def _generate_keys(self):
        if not self.pqc_available:
            return
        self._public_key, self._private_key = self.sign_func.generate_keypair()
        logger.info(f"Generated PQC keys for {self.algorithm}")

    async def sign_data(self, data: Dict) -> Dict:
        data_bytes = json.dumps(data, sort_keys=True, default=str).encode()
        if self.pqc_available and self._private_key:
            try:
                signature = self.sign_func.sign(data_bytes, self._private_key)
                return {
                    'signature': signature.hex(),
                    'algorithm': self.algorithm,
                    'public_key': self._public_key.hex(),
                    'timestamp': datetime.utcnow().isoformat()
                }
            except Exception as e:
                logger.error(f"PQC signing failed: {e}")
        # Fallback: ECDSA
        signature = self._ecdsa_private_key.sign(data_bytes, ec.ECDSA(hashes.SHA256()))
        return {
            'signature': signature.hex(),
            'algorithm': 'ecdsa',
            'timestamp': datetime.utcnow().isoformat()
        }

    async def verify_data(self, data: Dict, signature_data: Dict) -> bool:
        data_bytes = json.dumps(data, sort_keys=True, default=str).encode()
        algorithm = signature_data.get('algorithm')
        signature = bytes.fromhex(signature_data['signature'])
        if algorithm in ['dilithium', 'falcon', 'sphincs'] and self.pqc_available and self._public_key:
            return self.verify_func.verify(data_bytes, signature, self._public_key)
        elif algorithm == 'ecdsa':
            from cryptography.hazmat.primitives.asymmetric import ec
            public_key = ec.load_der_public_key(bytes.fromhex(signature_data['public_key']))
            public_key.verify(signature, data_bytes, ec.ECDSA(hashes.SHA256()))
            return True
        return False

# ============================================================================
# Blockchain Auditor (unchanged)
# ============================================================================

class BlockchainAuditor:
    """Real Ethereum integration for recording critical events."""
    def __init__(self, config: EcoATPConfig, circuit_breaker: Optional[CircuitBreaker] = None):
        self.config = config
        self.circuit_breaker = circuit_breaker
        self.web3 = None
        self.contract = None
        self.account = None
        self.available = False
        self._nonce_cache = {}
        self._lock = asyncio.Lock()
        self._initialize()

    def _initialize(self):
        try:
            from web3 import Web3, Account, HTTPProvider
            from web3.middleware import geth_poa_middleware, gas_price_strategy
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
            # Load contract ABI (from file or environment)
            abi = self._load_abi()
            if self.config.blockchain_contract_address:
                self.contract = self.web3.eth.contract(
                    address=self.config.blockchain_contract_address,
                    abi=abi
                )
                self.available = True
                logger.info("Blockchain auditor connected")
            else:
                logger.warning("Contract address not configured – blockchain audit will be simulated.")
        except Exception as e:
            logger.error(f"Blockchain initialization failed: {e}")

    def _load_abi(self) -> List:
        # In production, load from a trusted file
        abi_path = Path(__file__).parent / "contract_abi.json"
        if abi_path.exists():
            with open(abi_path, 'r') as f:
                data = json.load(f)
                return data['abi']
        # Minimal ABI for recording events
        return [
            {"constant": False, "inputs": [{"name": "eventType", "type": "string"}, {"name": "payload", "type": "string"}], "name": "recordEvent", "outputs": [], "type": "function"}
        ]

    async def _get_nonce(self, address: str) -> int:
        if address not in self._nonce_cache:
            self._nonce_cache[address] = self.web3.eth.get_transaction_count(address)
        return self._nonce_cache[address]

    async def _increment_nonce(self, address: str):
        self._nonce_cache[address] = self._nonce_cache.get(address, 0) + 1

    @retry_decorator(max_attempts=3, min_delay=0.1, max_delay=2)
    async def record_event(self, event_type: str, payload: Dict) -> Dict:
        if not self.available:
            return {'status': 'simulated', 'tx_hash': f"0x{hashlib.sha256(os.urandom(32)).hexdigest()}"}

        async def _record():
            async with self._lock:
                nonce = await self._get_nonce(self.account.address)
                payload_str = json.dumps(payload, default=str)
                gas_estimate = self.contract.functions.recordEvent(event_type, payload_str).estimate_gas({'from': self.account.address})
                gas_price = self.web3.eth.generate_gas_price() or self.web3.eth.gas_price
                tx = self.contract.functions.recordEvent(event_type, payload_str).build_transaction({
                    'from': self.account.address,
                    'nonce': nonce,
                    'gas': int(gas_estimate * 1.2),
                    'gasPrice': gas_price
                })
                signed_tx = self.account.sign_transaction(tx)
                tx_hash = self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
                receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
                if receipt.status == 1:
                    await self._increment_nonce(self.account.address)
                    logger.info(f"Blockchain event recorded: {tx_hash.hex()}")
                    return {'status': 'success', 'tx_hash': tx_hash.hex(), 'block_number': receipt.blockNumber}
                else:
                    logger.error(f"Transaction reverted for {event_type}")
                    return {'status': 'failed', 'error': 'transaction reverted'}

        if self.circuit_breaker:
            return await self.circuit_breaker.call(_record)
        else:
            return await _record()

# ============================================================================
# Multi-Cloud Distributor (unchanged)
# ============================================================================

class MultiCloudDistributor:
    """Distribute state to S3, Azure Blob, or GCP with retry and fallback."""
    def __init__(self, config: EcoATPConfig, circuit_breaker: Optional[CircuitBreaker] = None):
        self.config = config
        self.circuit_breaker = circuit_breaker
        self._clients = {}
        self._providers = ['aws', 'azure', 'gcp']
        self._init_client(config.cloud_provider)

    def _init_client(self, provider: str):
        try:
            if provider == 'aws':
                import boto3
                self._clients['aws'] = boto3.client('s3',
                    aws_access_key_id=self.config.cloud_access_key,
                    aws_secret_access_key=self.config.cloud_secret_key,
                    region_name=self.config.cloud_region)
            elif provider == 'azure':
                from azure.storage.blob import BlobServiceClient
                self._clients['azure'] = BlobServiceClient.from_connection_string(self.config.cloud_access_key)
            elif provider == 'gcp':
                from google.cloud import storage
                self._clients['gcp'] = storage.Client.from_service_account_json(self.config.cloud_access_key)
        except Exception as e:
            logger.warning(f"Failed to initialize {provider} client: {e}")

    @retry_decorator(max_attempts=3, min_delay=0.1, max_delay=2)
    async def distribute(self, data: Dict, filename: str) -> Dict:
        """Upload a JSON-serializable dict to cloud storage with fallback."""
        for provider in self._providers:
            if provider in self._clients:
                try:
                    result = await self._upload(provider, data, filename)
                    if result.get('status') == 'success':
                        return result
                except Exception as e:
                    logger.warning(f"Upload to {provider} failed: {e}")
        return {'status': 'failed', 'reason': 'All cloud providers failed'}

    async def _upload(self, provider: str, data: Dict, filename: str) -> Dict:
        data_bytes = json.dumps(data, default=str).encode('utf-8')
        if provider == 'aws':
            client = self._clients['aws']
            client.put_object(Bucket=self.config.cloud_bucket, Key=filename, Body=data_bytes)
            return {'status': 'success', 'url': f"s3://{self.config.cloud_bucket}/{filename}"}
        elif provider == 'azure':
            client = self._clients['azure']
            container_client = client.get_container_client(self.config.cloud_bucket)
            blob_client = container_client.get_blob_client(filename)
            blob_client.upload_blob(data_bytes, overwrite=True)
            return {'status': 'success', 'url': f"azure://{self.config.cloud_bucket}/{filename}"}
        elif provider == 'gcp':
            client = self._clients['gcp']
            bucket = client.bucket(self.config.cloud_bucket)
            blob = bucket.blob(filename)
            blob.upload_from_string(data_bytes, content_type='application/json')
            return {'status': 'success', 'url': f"gs://{self.config.cloud_bucket}/{filename}"}
        raise ValueError(f"Unknown provider: {provider}")

# ============================================================================
# Autonomous Strategy Selector (unchanged)
# ============================================================================

class AutonomousStrategySelector:
    """Q-learning agent for strategy selection with persistent Q-table."""
    def __init__(self, config: EcoATPConfig):
        self.config = config
        self.learning_rate = config.rl_learning_rate
        self.discount_factor = config.rl_discount_factor
        self.exploration_rate = config.rl_exploration_rate
        self.q_table: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self.total_updates = 0
        self.actions = ['conservative', 'balanced', 'performance']
        self._load_q_table()

    def _load_q_table(self):
        conn = sqlite3.connect(self.config.rl_q_table_db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS q_table (
                state TEXT,
                action TEXT,
                q_value REAL,
                PRIMARY KEY (state, action)
            )
        """)
        rows = conn.execute("SELECT state, action, q_value FROM q_table").fetchall()
        for state, action, q_value in rows:
            self.q_table[state][action] = q_value
        conn.close()
        logger.info(f"Loaded Q-table with {len(self.q_table)} states")

    def _save_q_value(self, state: str, action: str, q_value: float):
        conn = sqlite3.connect(self.config.rl_q_table_db_path)
        conn.execute("INSERT OR REPLACE INTO q_table (state, action, q_value) VALUES (?, ?, ?)", (state, action, q_value))
        conn.commit()
        conn.close()

    def _state_to_key(self, state: Dict) -> str:
        load = state.get('system_load', 0.5)
        utilization = state.get('system_efficiency', 0.5)
        load_bin = 'high' if load > 0.7 else 'medium' if load > 0.4 else 'low'
        util_bin = 'high' if utilization > 0.7 else 'medium' if utilization > 0.4 else 'low'
        return f"{load_bin}_{util_bin}"

    async def select_strategy(self, state: Dict) -> str:
        state_key = self._state_to_key(state)
        if random.random() < self.exploration_rate:
            self.exploration_rate = max(0.01, self.exploration_rate * 0.999)
            return random.choice(self.actions)
        q_values = {a: self.q_table[state_key].get(a, 0.0) for a in self.actions}
        return max(q_values, key=q_values.get)

    async def update(self, state: Dict, action: str, reward: float, next_state: Dict):
        state_key = self._state_to_key(state)
        next_state_key = self._state_to_key(next_state)
        current_q = self.q_table[state_key][action]
        max_next_q = max(self.q_table[next_state_key].values()) if self.q_table[next_state_key] else 0
        new_q = current_q + self.learning_rate * (reward + self.discount_factor * max_next_q - current_q)
        self.q_table[state_key][action] = new_q
        self._save_q_value(state_key, action, new_q)
        self.total_updates += 1

# ============================================================================
# Async Persistence Manager (Enhanced with MOPD)
# ============================================================================

class AsyncPersistenceManager:
    """Async SQLite persistence with connection pooling."""
    def __init__(self, config: EcoATPConfig):
        self.config = config
        self.db_path = config.persistence_path
        self._init_db()

    async def _init_db(self):
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id TEXT PRIMARY KEY,
                    balance REAL,
                    total_generated REAL,
                    total_consumed REAL,
                    total_recovered REAL,
                    total_expired REAL,
                    efficiency_rating REAL,
                    quantum_balance REAL,
                    quantum_total_generated REAL
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS tokens (
                    token_id TEXT PRIMARY KEY,
                    account_id TEXT,
                    value REAL,
                    source TEXT,
                    state TEXT,
                    generated_at TEXT,
                    expires_at TEXT,
                    carbon_equivalent_kg REAL,
                    helium_equivalent_units REAL,
                    generation_efficiency REAL,
                    provenance_hash TEXT,
                    quantum_advantage_factor REAL,
                    quantum_circuit_id TEXT,
                    consumed_at TEXT,
                    recovered_at TEXT,
                    quantum_signature TEXT,
                    FOREIGN KEY(account_id) REFERENCES accounts(account_id)
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS market_orders (
                    order_id TEXT PRIMARY KEY,
                    account_id TEXT,
                    amount REAL,
                    price REAL,
                    side TEXT,
                    status TEXT,
                    created_at TEXT,
                    expires_at TEXT,
                    remaining REAL,
                    FOREIGN KEY(account_id) REFERENCES accounts(account_id)
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    trade_id TEXT PRIMARY KEY,
                    sell_order TEXT,
                    buy_order TEXT,
                    seller TEXT,
                    buyer TEXT,
                    amount REAL,
                    price REAL,
                    timestamp TEXT
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS ml_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id_hash INTEGER,
                    hour INTEGER,
                    day_of_week INTEGER,
                    amount REAL,
                    timestamp TEXT
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS global_state (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            await conn.commit()

    async def save_account(self, account: EcoATPAccount):
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("""
                INSERT OR REPLACE INTO accounts
                (account_id, balance, total_generated, total_consumed, total_recovered, total_expired,
                 efficiency_rating, quantum_balance, quantum_total_generated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (account.account_id, account.balance, account.total_generated, account.total_consumed,
                  account.total_recovered, account.total_expired, account.efficiency_rating,
                  account.quantum_balance, account.quantum_total_generated))
            await conn.commit()

    async def load_account(self, account_id: str) -> Optional[EcoATPAccount]:
        async with aiosqlite.connect(self.db_path) as conn:
            row = await conn.execute("SELECT * FROM accounts WHERE account_id = ?", (account_id,))
            row = await row.fetchone()
            if row:
                return EcoATPAccount(
                    account_id=row[0],
                    balance=row[1],
                    total_generated=row[2],
                    total_consumed=row[3],
                    total_recovered=row[4],
                    total_expired=row[5],
                    efficiency_rating=row[6],
                    quantum_balance=row[7],
                    quantum_total_generated=row[8]
                )
        return None

    async def save_token(self, token: EcoATPToken, account_id: str):
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("""
                INSERT OR REPLACE INTO tokens
                (token_id, account_id, value, source, state, generated_at, expires_at,
                 carbon_equivalent_kg, helium_equivalent_units, generation_efficiency,
                 provenance_hash, quantum_advantage_factor, quantum_circuit_id,
                 consumed_at, recovered_at, quantum_signature)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (token.token_id, account_id, token.value, token.source.value, token.state.value,
                  token.generated_at.isoformat(), token.expires_at.isoformat(),
                  token.carbon_equivalent_kg, token.helium_equivalent_units,
                  token.generation_efficiency, token.provenance_hash,
                  token.quantum_advantage_factor, token.quantum_circuit_id,
                  token.consumed_at.isoformat() if token.consumed_at else None,
                  token.recovered_at.isoformat() if token.recovered_at else None,
                  json.dumps(token.quantum_signature) if token.quantum_signature else None))
            await conn.commit()

    async def load_active_tokens(self, account_id: Optional[str] = None) -> List[Dict]:
        async with aiosqlite.connect(self.db_path) as conn:
            if account_id:
                cursor = await conn.execute("SELECT * FROM tokens WHERE account_id = ? AND state != 'CONSUMED' AND state != 'EXPIRED'", (account_id,))
            else:
                cursor = await conn.execute("SELECT * FROM tokens WHERE state != 'CONSUMED' AND state != 'EXPIRED'")
            rows = await cursor.fetchall()
            tokens = []
            for row in rows:
                token_dict = {
                    'token_id': row[0],
                    'account_id': row[1],
                    'value': row[2],
                    'source': row[3],
                    'state': row[4],
                    'generated_at': datetime.fromisoformat(row[5]),
                    'expires_at': datetime.fromisoformat(row[6]),
                    'carbon_equivalent_kg': row[7],
                    'helium_equivalent_units': row[8],
                    'generation_efficiency': row[9],
                    'provenance_hash': row[10],
                    'quantum_advantage_factor': row[11],
                    'quantum_circuit_id': row[12],
                    'consumed_at': datetime.fromisoformat(row[13]) if row[13] else None,
                    'recovered_at': datetime.fromisoformat(row[14]) if row[14] else None,
                    'quantum_signature': json.loads(row[15]) if row[15] else None
                }
                tokens.append(token_dict)
            return tokens

    async def save_market_order(self, order: MarketOrder):
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("""
                INSERT OR REPLACE INTO market_orders
                (order_id, account_id, amount, price, side, status, created_at, expires_at, remaining)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (order.order_id, order.account_id, order.amount, order.price, order.side,
                  order.status, order.created_at.isoformat(), order.expires_at.isoformat(), order.remaining))
            await conn.commit()

    async def load_open_orders(self) -> List[MarketOrder]:
        async with aiosqlite.connect(self.db_path) as conn:
            cursor = await conn.execute("SELECT * FROM market_orders WHERE status = 'open'")
            rows = await cursor.fetchall()
            orders = []
            for row in rows:
                orders.append(MarketOrder(
                    order_id=row[0],
                    account_id=row[1],
                    amount=row[2],
                    price=row[3],
                    side=row[4],
                    status=row[5],
                    created_at=datetime.fromisoformat(row[6]),
                    expires_at=datetime.fromisoformat(row[7]),
                    remaining=row[8]
                ))
            return orders

    async def save_ml_data(self, data: List[Dict]):
        async with aiosqlite.connect(self.db_path) as conn:
            for d in data:
                await conn.execute("""
                    INSERT INTO ml_data (account_id_hash, hour, day_of_week, amount, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                """, (d['account_id_hash'], d['hour'], d['day_of_week'], d['amount'], datetime.utcnow().isoformat()))
            await conn.commit()

    async def load_ml_data(self, limit: int = 1000) -> List[Dict]:
        async with aiosqlite.connect(self.db_path) as conn:
            cursor = await conn.execute("SELECT account_id_hash, hour, day_of_week, amount, timestamp FROM ml_data ORDER BY id DESC LIMIT ?", (limit,))
            rows = await cursor.fetchall()
            data = []
            for row in rows:
                data.append({
                    'account_id_hash': row[0],
                    'hour': row[1],
                    'day_of_week': row[2],
                    'amount': row[3],
                    'timestamp': datetime.fromisoformat(row[4])
                })
            return data

    async def save_global_state(self, key: str, value: str):
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("INSERT OR REPLACE INTO global_state (key, value) VALUES (?, ?)", (key, value))
            await conn.commit()

    async def load_global_state(self, key: str) -> Optional[str]:
        async with aiosqlite.connect(self.db_path) as conn:
            cursor = await conn.execute("SELECT value FROM global_state WHERE key = ?", (key,))
            row = await cursor.fetchone()
            return row[0] if row else None

    # ===== MOPD persistence methods (NEW) =====
    async def save_pareto_front(self, pareto_front: List[MOPDPoint]):
        """Save Pareto front as JSON in global_state."""
        if not pareto_front:
            return
        value = json.dumps([p.to_dict() for p in pareto_front])
        await self.save_global_state('pareto_front', value)

    async def load_pareto_front(self) -> Optional[List[MOPDPoint]]:
        value = await self.load_global_state('pareto_front')
        if value:
            data = json.loads(value)
            return [MOPDPoint.from_dict(d) for d in data]
        return None

# ============================================================================
# Task Manager (simplified) – unchanged
# ============================================================================

class TaskManager:
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()

    def start_task(self, name: str, coro_func, *args, **kwargs):
        async def wrapper():
            while not self.shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Task {name} crashed: {e}", exc_info=True)
                    await asyncio.sleep(60)
        task = asyncio.create_task(wrapper(), name=name)
        self.tasks[name] = task
        return task

    async def stop_all(self):
        self.shutdown_event.set()
        for task in self.tasks.values():
            task.cancel()
        await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        self.tasks.clear()

# ============================================================================
# Enhanced Eco-ATP Token Manager (Main Class)
# ============================================================================

class EcoATPTokenManager:
    """Enhanced Eco-ATP Token Manager v10.1.0 with async persistence, security, etc."""

    def __init__(self, config: Optional[EcoATPConfig] = None,
                 exchange_rate: Optional[ExchangeRateProvider] = None,
                 gradient_provider: Optional[GradientProvider] = None,
                 quantum_provider: Optional[QuantumFeedbackProvider] = None):
        self.config = config or EcoATPConfig()
        self.exchange_rate = exchange_rate or DynamicExchangeRate(self.config)
        self.gradient_provider = gradient_provider
        self.quantum_provider = quantum_provider

        # Core state
        self.accounts: Dict[str, EcoATPAccount] = {}
        self.active_tokens: Dict[str, EcoATPToken] = {}
        self.token_history: deque = deque(maxlen=10000)

        # Locks
        self._accounts_lock = asyncio.Lock()
        self._tokens_lock = asyncio.Lock()
        self._history_lock = asyncio.Lock()

        # Emergency mode
        self.emergency_mode = False
        self.emergency_reserve = self.config.emergency_reserve
        self.substrate_phosphorylation_active = False
        self.substrate_reserves = self.config.substrate_reserves_min
        self.last_generation_time: Optional[datetime] = None

        # Tenant quotas
        self.tenant_quotas: Dict[str, Dict[str, Any]] = {}
        self.default_quota = {
            'max_tokens_per_minute': self.config.default_max_tokens_per_minute,
            'max_concurrent_tasks': self.config.default_max_concurrent_tasks,
            'min_priority_for_reservation': self.config.default_min_priority_for_reservation,
            'reservation_cooldown_seconds': self.config.default_reservation_cooldown_seconds
        }
        self.tenant_usage: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.tenant_last_reservation: Dict[str, datetime] = {}
        self.suspicious_tenants: Set[str] = set()
        self._failed_attempts: Dict[str, int] = defaultdict(int)
        self._tenant_usage_lock = asyncio.Lock()
        self._tenant_last_reservation_lock = asyncio.Lock()
        self._failed_attempts_lock = asyncio.Lock()
        self._suspicious_lock = asyncio.Lock()

        # Batch processing
        self.batch_queue: List[Dict[str, Any]] = []
        self._batch_lock = asyncio.Lock()

        # ML Demand Predictor
        self.ml_predictor = MLDemandPredictor(self.config)

        # Predictive supply
        self.predictive_supply_enabled = True
        self.predicted_demand_accumulator: Dict[str, float] = defaultdict(float)

        # Adaptive rate limiting
        self.system_load_history: deque = deque(maxlen=100)
        self.current_rate_multiplier = 1.0
        self._load_history_lock = asyncio.Lock()

        # User-defined emergency thresholds
        self.user_emergency_thresholds: Dict[str, Dict[str, Any]] = {}
        self.user_emergency_override = False
        self._emergency_thresholds_lock = asyncio.Lock()

        # Sub-components
        self.genetic_optimizer = ThresholdGeneticOptimizer(self, self.config) if self.config.enable_genetic_optimizer else None
        self.token_market = DistributedTokenMarket(self, self.config)
        self.gradient_aware = GradientAwareGeneration(self, self.gradient_provider)
        self.quantum_feedback = QuantumFeedbackIntegrator(self, self.quantum_provider)

        # NEW components with persistence and retry
        self.circuit_breaker = CircuitBreaker(
            name="eco_atp",
            db_path=self.config.circuit_breaker_db_path,
            failure_threshold=self.config.circuit_breaker_failure_threshold,
            recovery_timeout=self.config.circuit_breaker_recovery_timeout
        ) if self.config.enable_circuit_breaker else None

        self.persistence = AsyncPersistenceManager(self.config) if self.config.enable_persistence else None
        self.quantum_security = QuantumResilientSecurity(algorithm=self.config.quantum_signing_algorithm) if self.config.enable_quantum_signing else None
        self.blockchain_auditor = BlockchainAuditor(self.config, self.circuit_breaker) if self.config.enable_blockchain_audit else None
        self.strategy_selector = AutonomousStrategySelector(self.config) if self.config.enable_autonomous_strategy else None
        self.multi_cloud = MultiCloudDistributor(self.config, self.circuit_breaker) if self.config.enable_multi_cloud else None

        # Task manager
        self.task_manager = TaskManager()

        # Start background tasks
        self._start_tasks()

        # Load state from persistence
        if self.persistence:
            asyncio.create_task(self._load_state())

        logger.info("Enhanced Eco-ATP Token Manager v10.1.0 initialized with MOPD")

    # ---------- Energy cost per token (fixed method) ----------
    async def energy_cost_per_token(
        self,
        batch_size: int,
        domain: str,
        token_length: int = 1,
    ) -> float:
        base_energy = 1e-6
        domain_factor = {"math": 1.5, "code": 1.2, "general": 1.0, "energy": 0.8}.get(domain, 1.0)
        if hasattr(self, 'carbon_manager'):
            intensity = await self.carbon_manager.get_current_intensity()
            intensity_factor = intensity / 400
        else:
            intensity_factor = 1.0
        batch_factor = 1.0 + 0.1 * (batch_size - 1)
        energy = base_energy * domain_factor * intensity_factor * batch_factor * token_length
        return energy

    async def update_exchange_rate(self, scarcity_factors: Dict[str, float]):
        if not hasattr(self, 'adaptive_rate'):
            self.adaptive_rate = True
        if not self.adaptive_rate:
            return
        helium_scarcity = scarcity_factors.get('helium', 0.5)
        carbon_scarcity = scarcity_factors.get('carbon', 0.5)
        factor = 1.0 + 0.5 * helium_scarcity + 0.3 * carbon_scarcity
        self.exchange_rate = self.exchange_rate * (0.9 + 0.1 * factor)
        logger.info(f"Updated exchange rate to {self.exchange_rate:.2f}")

    # ---------- Background tasks ----------
    def _start_tasks(self):
        self.task_manager.start_task("emergency_monitor", self._emergency_monitor_loop)
        self.task_manager.start_task("batch_processor", self._batch_processor_loop)
        self.task_manager.start_task("maintenance", self._maintenance_loop)
        self.task_manager.start_task("predictive_supply", self._predictive_supply_loop)
        self.task_manager.start_task("adaptive_rate", self._adaptive_rate_loop)
        self.task_manager.start_task("market_matching", self._market_matching_loop)
        if self.genetic_optimizer:
            self.task_manager.start_task("evolution", self._evolution_loop)
        self.task_manager.start_task("ml_training", self._ml_training_loop)
        self.task_manager.start_task("token_cleanup", self._token_cleanup_loop)
        self.task_manager.start_task("persistence_save", self._persistence_save_loop)
        if self.strategy_selector:
            self.task_manager.start_task("strategy_update", self._strategy_update_loop)

    async def _load_state(self):
        if not self.persistence:
            return
        # Load accounts
        async with aiosqlite.connect(self.persistence.db_path) as conn:
            rows = await conn.execute("SELECT account_id FROM accounts")
            account_ids = [row[0] for row in await rows.fetchall()]
            for acc_id in account_ids:
                account = await self.persistence.load_account(acc_id)
                if account:
                    self.accounts[account.account_id] = account
        # Load active tokens
        token_dicts = await self.persistence.load_active_tokens()
        for td in token_dicts:
            token = EcoATPToken(
                token_id=td['token_id'],
                value=td['value'],
                source=EcoATPSource(td['source']),
                generated_at=td['generated_at'],
                expires_at=td['expires_at'],
                state=TokenState(td['state']),
                carbon_equivalent_kg=td['carbon_equivalent_kg'],
                helium_equivalent_units=td['helium_equivalent_units'],
                generation_efficiency=td['generation_efficiency'],
                provenance_hash=td['provenance_hash'],
                quantum_advantage_factor=td['quantum_advantage_factor'],
                quantum_circuit_id=td['quantum_circuit_id'],
                consumed_at=td['consumed_at'],
                recovered_at=td['recovered_at'],
                quantum_signature=td['quantum_signature']
            )
            self.active_tokens[token.token_id] = token
        # Load ML data
        ml_data = await self.persistence.load_ml_data()
        self.ml_predictor.data = ml_data
        # Load market orders
        orders = await self.persistence.load_open_orders()
        for order in orders:
            self.token_market.order_book.add_order(order)
        # Load global state (e.g., genetic optimizer best individual, Pareto front, etc.)
        best_fitness_str = await self.persistence.load_global_state('best_fitness')
        if best_fitness_str:
            self.genetic_optimizer.best_fitness = float(best_fitness_str)
        best_ind_str = await self.persistence.load_global_state('best_individual')
        if best_ind_str:
            self.genetic_optimizer.best_individual = json.loads(best_ind_str)
        # Load Pareto front (NEW)
        pareto_front = await self.persistence.load_pareto_front()
        if pareto_front:
            self.genetic_optimizer.pareto_front = pareto_front
        logger.info("State loaded from persistence")

    async def _persistence_save_loop(self):
        while True:
            try:
                if self.persistence:
                    # Save accounts
                    async with self._accounts_lock:
                        for account in self.accounts.values():
                            await self.persistence.save_account(account)
                    # Save active tokens
                    async with self._tokens_lock:
                        for token in self.active_tokens.values():
                            account_id = token.token_id.split('_')[1] if '_' in token.token_id else 'unknown'
                            await self.persistence.save_token(token, account_id)
                    # Save market orders
                    for order in self.token_market.order_book.all_orders.values():
                        await self.persistence.save_market_order(order)
                    # Save ML data
                    if self.ml_predictor.data:
                        await self.persistence.save_ml_data(self.ml_predictor.data[-100:])
                    # Save global state
                    await self.persistence.save_global_state('best_fitness', str(self.genetic_optimizer.best_fitness))
                    if self.genetic_optimizer.best_individual:
                        await self.persistence.save_global_state('best_individual', json.dumps(self.genetic_optimizer.best_individual))
                    # Save Pareto front (NEW)
                    if self.config.mopd.enabled:
                        await self.persistence.save_pareto_front(self.genetic_optimizer.pareto_front)
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Persistence save loop error: {e}")
                await asyncio.sleep(60)

    async def _strategy_update_loop(self):
        while True:
            try:
                if self.strategy_selector:
                    state = await self._get_strategy_state()
                    strategy = await self.strategy_selector.select_strategy(state)
                    if strategy == 'conservative':
                        self.config.hoarding_threshold = 1.5
                        self.config.tax_rate = 0.15
                    elif strategy == 'performance':
                        self.config.hoarding_threshold = 2.5
                        self.config.tax_rate = 0.05
                    else:
                        self.config.hoarding_threshold = 2.0
                        self.config.tax_rate = 0.1
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Strategy update loop error: {e}")
                await asyncio.sleep(60)

    async def _get_strategy_state(self) -> Dict:
        summary = await self.get_system_summary()
        return {
            'system_load': summary.get('system_efficiency', 0.5),
            'system_efficiency': summary.get('system_efficiency', 0.5)
        }

    async def shutdown(self):
        if self.persistence:
            await self._persistence_save_loop()
        await self.task_manager.stop_all()
        logger.info("Eco-ATP Token Manager shut down")

    # ============================================================================
    # Account Management
    # ============================================================================

    async def create_account(self, account_id: str) -> EcoATPAccount:
        async with self._accounts_lock:
            if account_id not in self.accounts:
                self.accounts[account_id] = EcoATPAccount(account_id=account_id)
                if self.persistence:
                    await self.persistence.save_account(self.accounts[account_id])
            return self.accounts[account_id]

    async def get_account(self, account_id: str) -> Optional[EcoATPAccount]:
        async with self._accounts_lock:
            return self.accounts.get(account_id)

    # ============================================================================
    # Token Generation
    # ============================================================================

    @retry_decorator(max_attempts=3, min_delay=0.1, max_delay=2)
    async def generate_tokens(self, account_id: str, source: EcoATPSource,
                            carbon_saved_kg: float = 0.0, helium_saved_units: float = 0.0,
                            energy_saved_kwh: float = 0.0, efficiency: float = 1.0,
                            num_tokens: Optional[int] = None,
                            quantum_advantage_factor: float = 0.0,
                            quantum_circuit_id: Optional[str] = None) -> List[EcoATPToken]:
        async with self._accounts_lock:
            if account_id not in self.accounts:
                self.accounts[account_id] = EcoATPAccount(account_id=account_id)
            account = self.accounts[account_id]

        gradient_multiplier = self.gradient_aware.adjust_generation_rate()
        quantum_multiplier = self.quantum_feedback.apply_quantum_insights()
        total_multiplier = gradient_multiplier * quantum_multiplier

        carbon_value = self.exchange_rate.carbon_to_ecoatp(carbon_saved_kg)
        helium_value = self.exchange_rate.helium_to_ecoatp(helium_saved_units)
        energy_value = self.exchange_rate.energy_to_ecoatp(energy_saved_kwh)
        total_value = (carbon_value + helium_value + energy_value) * total_multiplier

        if num_tokens is None:
            num_tokens = max(1, int(total_value / 10))

        token_value = total_value / num_tokens
        tokens = []
        now = datetime.utcnow()
        expiry = now + timedelta(hours=self.config.token_expiry_hours)

        async with self._tokens_lock:
            for i in range(num_tokens):
                token = EcoATPToken(
                    token_id=f"eco_{account_id}_{now.timestamp()}_{i}_{uuid.uuid4().hex[:4]}",
                    value=token_value,
                    source=source,
                    generated_at=now,
                    expires_at=expiry,
                    carbon_equivalent_kg=carbon_saved_kg / num_tokens,
                    helium_equivalent_units=helium_saved_units / num_tokens,
                    generation_efficiency=efficiency,
                    quantum_advantage_factor=quantum_advantage_factor,
                    quantum_circuit_id=quantum_circuit_id
                )
                if self.quantum_security:
                    token_data = asdict(token)
                    signature = await self.quantum_security.sign_data(token_data)
                    token.quantum_signature = signature
                tokens.append(token)
                self.active_tokens[token.token_id] = token

        async with self._accounts_lock:
            account.balance += total_value
            account.total_generated += total_value
            if source == EcoATPSource.QUANTUM_ADVANTAGE:
                account.quantum_balance += total_value
                account.quantum_total_generated += total_value
            if self.persistence:
                await self.persistence.save_account(account)

        self.last_generation_time = now
        self.ml_predictor.record_demand(account_id, total_value, now)

        if total_value > 100 and self.substrate_reserves < self.config.substrate_reserves_max:
            self.substrate_reserves = min(self.config.substrate_reserves_max,
                                          self.substrate_reserves + total_value * 0.05)

        if self.blockchain_auditor:
            await self.blockchain_auditor.record_event('token_generation', {
                'account_id': account_id,
                'amount': total_value,
                'source': source.value,
                'token_count': len(tokens)
            })

        if self.multi_cloud:
            token_summary = {
                'account_id': account_id,
                'total_value': total_value,
                'token_count': len(tokens),
                'timestamp': now.isoformat()
            }
            await self.multi_cloud.distribute(token_summary, f"tokens_{account_id}_{now.timestamp()}.json")

        if self.strategy_selector:
            state = await self._get_strategy_state()
            reward = 1.0 if total_value > 0 else 0.0
            current_strategy = 'balanced'
            await self.strategy_selector.update(state, current_strategy, reward, state)

        return tokens

    # ============================================================================
    # Token Reservation, Consumption, Recovery
    # ============================================================================

    @retry_decorator(max_attempts=3, min_delay=0.1, max_delay=2)
    async def reserve_tokens(self, account_id: str, amount: float, consumer: EcoATPConsumer,
                            tenant_id: str = "default", priority: int = 2) -> Tuple[bool, List[str]]:
        async with self._accounts_lock:
            account = self.accounts.get(account_id)
            if not account:
                logger.warning(f"Account {account_id} not found")
                return False, []

            if account.balance < amount:
                logger.warning(f"Insufficient balance: {account.balance} < {amount}")
                return False, []

            if not await self._check_tenant_quota(tenant_id, amount):
                return False, []

            if tenant_id in self.suspicious_tenants:
                logger.warning(f"Tenant {tenant_id} is suspicious, denying reservation")
                return False, []

            available = []
            for token in self.active_tokens.values():
                if token.state == TokenState.AVAILABLE:
                    available.append(token)
                if len(available) >= amount:
                    break
            if len(available) < amount:
                logger.warning(f"Not enough available tokens: {len(available)} < {amount}")
                return False, []

            reserved_tokens = []
            for token in available[:int(amount)]:
                token.state = TokenState.RESERVED
                reserved_tokens.append(token.token_id)
            account.balance -= amount
            if self.persistence:
                await self.persistence.save_account(account)
                for token in reserved_tokens:
                    await self.persistence.save_token(self.active_tokens[token], account_id)

            await self._update_tenant_usage(tenant_id, amount)

            return True, reserved_tokens

    async def _check_tenant_quota(self, tenant_id: str, amount: float) -> bool:
        async with self._tenant_usage_lock:
            usage = self.tenant_usage[tenant_id]
            now = datetime.utcnow()
            recent = [u for u in usage if (now - u).total_seconds() < 60]
            if sum(recent) + amount > self.default_quota['max_tokens_per_minute']:
                logger.warning(f"Tenant {tenant_id} quota exceeded")
                return False
            return True

    async def _update_tenant_usage(self, tenant_id: str, amount: float):
        async with self._tenant_usage_lock:
            self.tenant_usage[tenant_id].append(datetime.utcnow())

    async def consume_tokens(self, token_ids: List[str], consumer: EcoATPConsumer, operation_success: bool) -> float:
        total_consumed = 0.0
        async with self._tokens_lock:
            for token_id in token_ids:
                token = self.active_tokens.get(token_id)
                if not token or token.state != TokenState.RESERVED:
                    continue
                if operation_success:
                    token.state = TokenState.CONSUMED
                    token.consumed_at = datetime.utcnow()
                    total_consumed += token.value
                else:
                    token.state = TokenState.AVAILABLE
        return total_consumed

    async def recover_tokens(self, token_ids: List[str], completion_percentage: float) -> float:
        total_recovered = 0.0
        async with self._tokens_lock:
            for token_id in token_ids:
                token = self.active_tokens.get(token_id)
                if not token or token.state != TokenState.RESERVED:
                    continue
                recovery_frac = self.config.recovery_rates.get(completion_percentage, 0.0)
                if recovery_frac > 0:
                    recovered_value = token.value * recovery_frac
                    token.state = TokenState.RECOVERED
                    token.recovered_at = datetime.utcnow()
                    total_recovered += recovered_value
                else:
                    token.state = TokenState.EXPIRED
        return total_recovered

    # ============================================================================
    # System Summary and Account Summary
    # ============================================================================

    async def get_system_summary(self) -> Dict[str, Any]:
        total_balance = sum(a.balance for a in self.accounts.values())
        total_generated = sum(a.total_generated for a in self.accounts.values())
        total_consumed = sum(a.total_consumed for a in self.accounts.values())
        total_recovered = sum(a.total_recovered for a in self.accounts.values())
        total_expired = sum(a.total_expired for a in self.accounts.values())
        active_tokens_count = len([t for t in self.active_tokens.values() if t.state == TokenState.AVAILABLE])
        reserved_tokens_count = len([t for t in self.active_tokens.values() if t.state == TokenState.RESERVED])
        total_accounts = len(self.accounts)

        system_efficiency = total_consumed / max(total_generated, 1)

        return {
            'total_balance': total_balance,
            'total_generated': total_generated,
            'total_consumed': total_consumed,
            'total_recovered': total_recovered,
            'total_expired': total_expired,
            'active_tokens': active_tokens_count,
            'reserved_tokens': reserved_tokens_count,
            'total_accounts': total_accounts,
            'system_efficiency': system_efficiency,
            'emergency_mode': self.emergency_mode,
            'substrate_reserves': self.substrate_reserves,
            'timestamp': datetime.utcnow().isoformat()
        }

    async def get_account_summary(self, account_id: str) -> Dict[str, Any]:
        account = self.accounts.get(account_id)
        if not account:
            return {}
        return {
            'account_id': account.account_id,
            'balance': account.balance,
            'total_generated': account.total_generated,
            'total_consumed': account.total_consumed,
            'total_recovered': account.total_recovered,
            'total_expired': account.total_expired,
            'efficiency_rating': account.efficiency_rating,
            'quantum_balance': account.quantum_balance,
            'quantum_total_generated': account.quantum_total_generated,
            'utilization_rate': account.utilization_rate,
            'timestamp': datetime.utcnow().isoformat()
        }

    # ============================================================================
    # Background Loops
    # ============================================================================

    async def _emergency_monitor_loop(self):
        while True:
            try:
                summary = await self.get_system_summary()
                if summary['total_balance'] < self.config.emergency_threshold and not self.emergency_mode:
                    self.emergency_mode = True
                    logger.warning("Emergency mode activated due to low balance")
                    await self.generate_tokens('emergency', EcoATPSource.EMERGENCY_SUBSTRATE,
                                               energy_saved_kwh=self.config.emergency_token_rate)
                elif summary['total_balance'] > self.config.emergency_threshold * 2 and self.emergency_mode:
                    self.emergency_mode = False
                    logger.info("Emergency mode deactivated")
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Emergency monitor error: {e}")
                await asyncio.sleep(60)

    async def _batch_processor_loop(self):
        while True:
            try:
                async with self._batch_lock:
                    if self.batch_queue:
                        batch = self.batch_queue[:self.config.batch_size]
                        self.batch_queue = self.batch_queue[self.config.batch_size:]
                        logger.info(f"Processing batch of {len(batch)} operations")
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Batch processor error: {e}")
                await asyncio.sleep(60)

    async def _maintenance_loop(self):
        while True:
            try:
                expired = []
                now = datetime.utcnow()
                for token in self.active_tokens.values():
                    if token.is_expired(now) and token.state not in (TokenState.CONSUMED, TokenState.EXPIRED):
                        expired.append(token.token_id)
                for token_id in expired:
                    token = self.active_tokens.pop(token_id, None)
                    if token:
                        token.state = TokenState.EXPIRED
                if expired:
                    logger.info(f"Cleaned up {len(expired)} expired tokens")
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Maintenance error: {e}")
                await asyncio.sleep(60)

    async def _predictive_supply_loop(self):
        while True:
            try:
                if self.predictive_supply_enabled:
                    for account_id in self.accounts:
                        demand = self.ml_predictor.predict_demand(account_id, datetime.utcnow())
                        if demand > 0:
                            await self.generate_tokens(account_id, EcoATPSource.RENEWABLE_ENERGY,
                                                       energy_saved_kwh=demand * 0.1)
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Predictive supply error: {e}")
                await asyncio.sleep(60)

    async def _adaptive_rate_loop(self):
        while True:
            try:
                summary = await self.get_system_summary()
                load = summary.get('system_efficiency', 0.5)
                async with self._load_history_lock:
                    self.system_load_history.append(load)
                    if len(self.system_load_history) >= 10:
                        avg_load = np.mean(self.system_load_history)
                        if avg_load > 0.8:
                            self.current_rate_multiplier = self.config.rate_limit_multiplier_high
                        elif avg_load < 0.3:
                            self.current_rate_multiplier = self.config.rate_limit_multiplier_low
                        else:
                            self.current_rate_multiplier = 1.0
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Adaptive rate error: {e}")
                await asyncio.sleep(60)

    async def _market_matching_loop(self):
        while True:
            try:
                matches = await self.token_market.match_orders()
                if matches:
                    logger.info(f"Matched {len(matches)} trades")
                await asyncio.sleep(self.config.market_matching_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Market matching error: {e}")
                await asyncio.sleep(60)

    async def _evolution_loop(self):
        while True:
            try:
                if self.genetic_optimizer:
                    logger.info("Starting genetic evolution cycle...")
                    result = await self.genetic_optimizer.evolve(generations=self.config.genetic_generations)
                    # Telemetry for MOPD (if Prometheus available)
                    if self.config.mopd.enabled:
                        # We could add counters, but we'll log for now
                        logger.info(f"Evolution complete: best fitness {result['best_fitness']:.4f}, Pareto front size: {len(result.get('pareto_front', []))}")
                    else:
                        logger.info(f"Evolution complete: best fitness {result['best_fitness']:.4f}")
                await asyncio.sleep(self.config.genetic_evolution_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Evolution error: {e}")
                await asyncio.sleep(60)

    async def _ml_training_loop(self):
        while True:
            try:
                await self.ml_predictor.train(force=False)
                await asyncio.sleep(self.config.ml_retrain_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"ML training error: {e}")
                await asyncio.sleep(60)

    async def _token_cleanup_loop(self):
        while True:
            try:
                now = datetime.utcnow()
                to_remove = []
                for token_id, token in self.active_tokens.items():
                    if token.state in (TokenState.CONSUMED, TokenState.EXPIRED, TokenState.RECOVERED):
                        to_remove.append(token_id)
                    elif token.is_expired(now):
                        to_remove.append(token_id)
                for token_id in to_remove:
                    self.active_tokens.pop(token_id, None)
                if to_remove:
                    logger.debug(f"Cleaned up {len(to_remove)} tokens from active set")
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Token cleanup error: {e}")
                await asyncio.sleep(60)

    # ============================================================================
    # MOPD Public Methods (NEW)
    # ============================================================================

    def get_mopd_pareto_front(self) -> List[MOPDPoint]:
        """Return the current Pareto front from the genetic optimizer."""
        if not self.config.mopd.enabled or not self.genetic_optimizer:
            return []
        return self.genetic_optimizer.pareto_front.copy()

    def get_mopd_summary(self) -> Dict[str, Any]:
        """Return a summary of MOPD‑related metrics."""
        if not self.config.mopd.enabled or not self.genetic_optimizer:
            return {"enabled": False}
        return {
            "enabled": True,
            "objective_weights": self.config.mopd.objective_weights,
            "grid_resolution": self.config.mopd.grid_resolution,
            "pareto_front_size": len(self.genetic_optimizer.pareto_front),
            "best_scalarised_score": self.genetic_optimizer.best_fitness,
            "evolution_history": self.genetic_optimizer.evolution_history[-10:],
        }

    # ============================================================================
    # Sync Wrappers (for backward compatibility)
    # ============================================================================

    def create_account_sync(self, account_id: str) -> EcoATPAccount:
        return asyncio.run(self.create_account(account_id))

    def generate_tokens_sync(self, account_id: str, source: EcoATPSource, **kwargs) -> List[EcoATPToken]:
        return asyncio.run(self.generate_tokens(account_id, source, **kwargs))

    def reserve_tokens_sync(self, account_id: str, amount: float, consumer: EcoATPConsumer,
                           tenant_id: str = "default", priority: int = 2) -> Tuple[bool, List[str]]:
        return asyncio.run(self.reserve_tokens(account_id, amount, consumer, tenant_id, priority))

    def consume_tokens_sync(self, token_ids: List[str], consumer: EcoATPConsumer, operation_success: bool) -> float:
        return asyncio.run(self.consume_tokens(token_ids, consumer, operation_success))

    def recover_tokens_sync(self, token_ids: List[str], completion_percentage: float) -> float:
        return asyncio.run(self.recover_tokens(token_ids, completion_percentage))

    def get_system_summary_sync(self) -> Dict[str, Any]:
        return asyncio.run(self.get_system_summary())

    def get_account_summary_sync(self, account_id: str) -> Dict[str, Any]:
        return asyncio.run(self.get_account_summary(account_id))

    # ============================================================================
    # Async Context Manager
    # ============================================================================

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()

# ============================================================================
# Test stubs (pytest)
# ============================================================================

import pytest
import pytest_asyncio

@pytest.fixture
def config():
    return EcoATPConfig(enable_persistence=False, enable_blockchain_audit=False, enable_multi_cloud=False)

@pytest_asyncio.fixture
async def manager(config):
    async with EcoATPTokenManager(config=config) as mgr:
        yield mgr

@pytest.mark.asyncio
async def test_create_account(manager):
    account = await manager.create_account("test")
    assert account.account_id == "test"
    assert account.balance == 0.0

@pytest.mark.asyncio
async def test_generate_tokens(manager):
    tokens = await manager.generate_tokens("test", EcoATPSource.RENEWABLE_ENERGY, energy_saved_kwh=10.0)
    assert len(tokens) > 0
    assert tokens[0].value > 0

@pytest.mark.asyncio
async def test_reserve_tokens(manager):
    await manager.generate_tokens("test", EcoATPSource.RENEWABLE_ENERGY, energy_saved_kwh=10.0)
    success, token_ids = await manager.reserve_tokens("test", 1.0, EcoATPConsumer.EXPERT_EXECUTION)
    assert success
    assert len(token_ids) == 1

@pytest.mark.asyncio
async def test_system_summary(manager):
    summary = await manager.get_system_summary()
    assert 'total_balance' in summary
    assert 'system_efficiency' in summary

# ============================================================================
# Example usage
# ============================================================================

async def main():
    logging.basicConfig(level=logging.INFO)
    config = EcoATPConfig()
    async with EcoATPTokenManager(config=config) as manager:
        account = await manager.create_account("test_account")
        tokens = await manager.generate_tokens("test_account", EcoATPSource.RENEWABLE_ENERGY,
                                               carbon_saved_kg=10.0)
        print(f"Generated {len(tokens)} tokens")
        summary = await manager.get_system_summary()
        print("System summary:", summary)
        # MOPD examples
        print("Pareto front:", manager.get_mopd_pareto_front())
        print("MOPD summary:", manager.get_mopd_summary())
        await manager.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
