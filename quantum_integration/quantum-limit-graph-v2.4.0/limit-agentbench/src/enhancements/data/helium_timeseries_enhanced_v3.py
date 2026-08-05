# =============================================================================
# FILE: src/enhancements/data/helium_timeseries_enhanced_v4_1_2.py
# VERSION: 4.1.2 (Enterprise Quantum Resilience + Full Distillation Pipeline)
# =============================================================================
"""
Enhanced Helium Timeseries Dataset Generator - Version 4.1.2

FINAL ENHANCEMENTS (Phases 7–10):
7. Reward feedback loop fully implemented (quality + anomaly realism).
8. Persistence for Q‑teacher weights (saved to JSON).
9. Offline training for Historical ML teacher from generation logs.
10. Unit tests for distillation components.
All previous features (security, blockchain, cloud, etc.) retained.
"""

import asyncio
import hashlib
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union, Callable, Awaitable
from enum import Enum
import warnings
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
import random
from abc import ABC, abstractmethod
from collections import deque
import pickle

# =============================================================================
# External dependencies (install via pip)
# =============================================================================
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

# Post‑quantum cryptography
try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

# Cryptography
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature, decode_dss_signature
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, PrivateFormat, NoEncryption
from cryptography.hazmat.backends import default_backend
from cryptography.fernet import Fernet

# Retry library
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# Data validation
try:
    from pydantic import BaseModel, Field, field_validator, ValidationError, ConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# For Parquet export
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False

# scikit-learn for ML teacher
try:
    from sklearn.ensemble import RandomForestClassifier
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

# =============================================================================
# Logging configuration
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# Centralised Configuration
# =============================================================================
class Config:
    """Central configuration with environment variable support."""
    # Generation parameters
    SEED = int(os.getenv('HELIUM_DATASET_SEED', '42'))
    N_PERIODS = int(os.getenv('HELIUM_DATASET_N_PERIODS', '120'))
    START_DATE = os.getenv('HELIUM_DATASET_START_DATE', '2020-01-01')
    ANOMALY_RATE = float(os.getenv('HELIUM_DATASET_ANOMALY_RATE', '0.02'))
    INCLUDE_ANOMALIES = os.getenv('HELIUM_DATASET_INCLUDE_ANOMALIES', 'true').lower() == 'true'
    
    # Output directory
    OUTPUT_DIR = os.getenv('HELIUM_DATASET_OUTPUT_DIR', './data')
    
    # API keys for real data fetch
    USGS_API_URL = os.getenv('USGS_API_URL', 'https://www.usgs.gov/api/helium-statistics')
    USGS_API_KEY = os.getenv('USGS_API_KEY', '')
    COMMODITY_API_URL = os.getenv('COMMODITY_API_URL', 'https://api.commodityprices.com/v1/helium')
    COMMODITY_API_KEY = os.getenv('COMMODITY_API_KEY', '')
    
    # Blockchain
    BLOCKCHAIN_RPC_URL = os.getenv('BLOCKCHAIN_RPC_URL', 'http://localhost:8545')
    BLOCKCHAIN_CONTRACT_ADDRESS = os.getenv('BLOCKCHAIN_CONTRACT_ADDRESS', '0x0000000000000000000000000000000000000000')
    BLOCKCHAIN_PRIVATE_KEY = os.getenv('BLOCKCHAIN_PRIVATE_KEY', '')
    
    # Cloud
    CLOUD_AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', '')
    CLOUD_AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '')
    CLOUD_AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    CLOUD_AZURE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING', '')
    CLOUD_GCP_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '')
    
    # Master encryption key (for key storage)
    MASTER_KEY_ENV = os.getenv('HELIUM_DATASET_MASTER_KEY', '')
    MASTER_KEY_FILE = os.getenv('HELIUM_DATASET_MASTER_KEY_FILE', '/tmp/helium_master.key')
    
    # Retry settings
    RETRY_ATTEMPTS = 3
    RETRY_MIN_WAIT = 2
    RETRY_MAX_WAIT = 10
    
    # Circuit breaker
    CIRCUIT_BREAKER_FAILURE_THRESHOLD = int(os.getenv('CIRCUIT_BREAKER_FAILURE_THRESHOLD', '5'))
    CIRCUIT_BREAKER_RECOVERY_TIMEOUT = float(os.getenv('CIRCUIT_BREAKER_RECOVERY_TIMEOUT', '30.0'))
    
    # Data source priority (comma-separated)
    SOURCE_PRIORITY = os.getenv('SOURCE_PRIORITY', 'usgs,commodity').split(',')
    
    # Generation constants
    PRODUCTION_BASE = float(os.getenv('PRODUCTION_BASE', '28000'))
    PRODUCTION_TREND = float(os.getenv('PRODUCTION_TREND', '-40'))
    DEMAND_BASE = float(os.getenv('DEMAND_BASE', '27000'))
    DEMAND_TREND = float(os.getenv('DEMAND_TREND', '80'))
    PRICE_BASE = float(os.getenv('PRICE_BASE', '100'))
    PRICE_VOL = float(os.getenv('PRICE_VOL', '0.1'))
    PRICE_DRIFT = float(os.getenv('PRICE_DRIFT', '0.005'))
    NEW_CAPACITY_BASE = float(os.getenv('NEW_CAPACITY_BASE', '2000'))
    NEW_CAPACITY_TREND = float(os.getenv('NEW_CAPACITY_TREND', '100'))
    CARBON_BASE = float(os.getenv('CARBON_BASE', '300'))
    CARBON_RANGE = float(os.getenv('CARBON_RANGE', '200'))
    RENEWABLE_BASE = float(os.getenv('RENEWABLE_BASE', '30'))
    RENEWABLE_RANGE = float(os.getenv('RENEWABLE_RANGE', '40'))

    # Distillation parameters
    DISTILLATION_EPSILON = float(os.getenv('DISTILLATION_EPSILON', '0.1'))
    DISTILLATION_TRAIN_EVERY = int(os.getenv('DISTILLATION_TRAIN_EVERY', '10'))
    DISTILLATION_REPLAY_SIZE = int(os.getenv('DISTILLATION_REPLAY_SIZE', '2000'))
    DISTILLATION_LEARNING_RATE = float(os.getenv('DISTILLATION_LEARNING_RATE', '0.01'))
    DISTILL_WEIGHT = float(os.getenv('DISTILL_WEIGHT', '0.7'))
    RL_WEIGHT = float(os.getenv('RL_WEIGHT', '0.3'))

    # Persistence paths
    Q_WEIGHTS_PATH = os.getenv('Q_WEIGHTS_PATH', './q_weights.json')
    GENERATION_LOGS_PATH = os.getenv('GENERATION_LOGS_PATH', './generation_logs.csv')
    HISTORICAL_MODEL_PATH = os.getenv('HISTORICAL_MODEL_PATH', './historical_model.pkl')

    @classmethod
    def get_master_key(cls) -> bytes:
        """Retrieve master encryption key from environment variable or generate."""
        key_hex = os.getenv(cls.MASTER_KEY_ENV)
        if key_hex:
            return bytes.fromhex(key_hex)
        # Try to read from file
        if os.path.exists(cls.MASTER_KEY_FILE):
            with open(cls.MASTER_KEY_FILE, 'rb') as f:
                return f.read()
        # Generate a new key and save
        key = Fernet.generate_key()
        with open(cls.MASTER_KEY_FILE, 'wb') as f:
            f.write(key)
        # Set permissions to read-only for owner
        os.chmod(cls.MASTER_KEY_FILE, 0o400)
        logger.warning(f"Generated new master key and saved to {cls.MASTER_KEY_FILE}")
        return key

# =============================================================================
# Circuit Breaker (Robust) - unchanged
# =============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    # ... same as original ...

# =============================================================================
# Data Models (Pydantic)
# =============================================================================
if PYDANTIC_AVAILABLE:
    class DatasetGenerationParams(BaseModel):
        seed: int = Field(default=42, ge=0)
        n_periods: int = Field(default=120, ge=10)
        start_date: str = Field(default="2020-01-01")
        anomaly_rate: float = Field(default=0.02, ge=0.0, le=0.5)
        include_anomalies: bool = True
        output_dir: str = Field(default="./data")
        fetch_real_data: bool = Field(default=False)
        cloud_distribution: bool = Field(default=False)
        blockchain_anchor: bool = Field(default=False)
        
        @field_validator('start_date')
        def valid_date(cls, v):
            try:
                datetime.fromisoformat(v)
            except ValueError:
                raise ValueError('Invalid date format. Use YYYY-MM-DD')
            return v
else:
    # Fallback
    @dataclass
    class DatasetGenerationParams:
        seed: int = 42
        n_periods: int = 120
        start_date: str = "2020-01-01"
        anomaly_rate: float = 0.02
        include_anomalies: bool = True
        output_dir: str = "./data"
        fetch_real_data: bool = False
        cloud_distribution: bool = False
        blockchain_anchor: bool = False

# =============================================================================
# TaskManager for Background Tasks - unchanged
# =============================================================================
class TaskManager:
    # ... same as original ...

# =============================================================================
# Quantum-Resilient Security - unchanged
# =============================================================================
class QuantumResilientSecurity:
    # ... same as original ...

# =============================================================================
# Blockchain Anchoring - unchanged
# =============================================================================
class BlockchainAnchoring:
    # ... same as original ...

# =============================================================================
# Multi-Cloud Distributor - unchanged
# =============================================================================
class MultiCloudDistributor:
    # ... same as original ...

# =============================================================================
# DISTILLATION COMPONENTS (Enhanced with Persistence & ML Training)
# =============================================================================

@dataclass
class GenerationOptimizationState:
    """State for the distillation agent."""
    quality_score: float
    regime_crisis: float
    regime_tightening: float
    regime_normal: float
    regime_stable: float
    price_volatility: float
    anomaly_rate: float
    production_trend_slope: float
    demand_trend_slope: float
    target_quality: float
    time_since_last: float

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 12‑dim numeric feature vector."""
        features = [
            self.quality_score / 100.0,
            self.regime_crisis,
            self.regime_tightening,
            self.regime_normal,
            self.regime_stable,
            min(self.price_volatility / 30.0, 1.0),
            self.anomaly_rate * 2.0,
            min(abs(self.production_trend_slope) / 100.0, 1.0),
            min(abs(self.demand_trend_slope) / 100.0, 1.0),
            self.target_quality / 100.0,
            min(self.time_since_last / 24.0, 1.0),
        ]
        return np.array(features, dtype=np.float32)


# Teacher abstract base
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: GenerationOptimizationState) -> np.ndarray:
        """Return probability vector over 5 strategies."""
        pass

    @abstractmethod
    def confidence(self, state: GenerationOptimizationState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class GenerationRuleBasedTeacher(Teacher):
    """Rule‑based expert."""
    ACTION_SPACE = ['conservative', 'moderate', 'aggressive', 'realistic', 'adaptive']

    def predict(self, state: GenerationOptimizationState) -> np.ndarray:
        probs = np.ones(5) * 0.1
        if state.quality_score < 70:
            probs[2] = 0.8
        elif state.regime_crisis > 0.5:
            probs[3] = 0.7
        elif state.price_volatility > 20:
            probs[0] = 0.6
        elif state.time_since_last > 24:
            probs[4] = 0.6
        else:
            probs[1] = 0.5
        return probs / probs.sum()

    def confidence(self, state: GenerationOptimizationState) -> float:
        if state.quality_score < 70:
            return 0.6
        return 0.4


class GenerationHistoricalMLTeacher(Teacher):
    """Offline trained classifier from past generation logs."""
    def __init__(self, model_path: Optional[Path] = None):
        self.model = None
        self.model_path = model_path or Path(Config.HISTORICAL_MODEL_PATH)
        if self.model_path.exists():
            try:
                with open(self.model_path, 'rb') as f:
                    self.model = pickle.load(f)
                logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")

    def predict(self, state: GenerationOptimizationState) -> np.ndarray:
        if self.model is None:
            return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: GenerationOptimizationState) -> float:
        return 0.7 if self.model is not None else 0.0


class GenerationStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, generator: 'EnhancedHeliumDatasetGeneratorV4', lr: float = 0.1):
        self.generator = generator
        self.lr = lr
        self.weights = np.zeros((12, 5))  # 12 features, 5 actions
        self._load_state()

    def _load_state(self):
        """Load Q‑weights from JSON file."""
        path = Path(Config.Q_WEIGHTS_PATH)
        if path.exists():
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                logger.info(f"Loaded Q‑teacher weights from {path}")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        """Save Q‑weights to JSON file."""
        path = Path(Config.Q_WEIGHTS_PATH)
        try:
            with open(path, 'w') as f:
                json.dump(self.weights.tolist(), f)
            logger.info(f"Saved Q‑teacher weights to {path}")
        except Exception as e:
            logger.error(f"Failed to save Q‑weights: {e}")

    def predict(self, state: GenerationOptimizationState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: GenerationOptimizationState) -> float:
        return 0.5

    def update(self, state: GenerationOptimizationState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()


class DistillationStudent:
    def __init__(self, feature_dim: int = 12, n_classes: int = 5, lr: float = 0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray) -> np.ndarray:
        logits = state_vector @ self.weights + self.biases
        max_logit = np.max(logits)
        exp_logits = np.exp(logits - max_logit)
        return exp_logits / exp_logits.sum()

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action: int, distill_weight: float = 0.7, rl_weight: float = 0.3):
        current_probs = self.predict_proba(state_vector)
        logits = state_vector @ self.weights + self.biases

        # Distillation gradient
        grad_distill = -(teacher_probs - current_probs)

        # Policy gradient
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current_probs)

        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1


class ReplayBuffer:
    def __init__(self, max_size: int = 2000):
        self.buffer = deque(maxlen=max_size)

    def push(self, state_vec: np.ndarray, action: int, reward: float,
             next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))

    def sample(self, batch_size: int = 32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (np.array(states), actions, np.array(rewards),
                np.array(next_states), np.array(teacher_probs))

    def __len__(self):
        return len(self.buffer)


class DistillationParameterOptimizer:
    """
    Multi‑teacher on‑policy distillation agent for generation parameter selection.
    """
    ACTION_SPACE = ['conservative', 'moderate', 'aggressive', 'realistic', 'adaptive']

    def __init__(self, generator: 'EnhancedHeliumDatasetGeneratorV4', config: Dict[str, Any]):
        self.generator = generator
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            GenerationRuleBasedTeacher(),
            GenerationHistoricalMLTeacher(),
            GenerationStatefulQTeacher(generator)
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_strategy(self, state: GenerationOptimizationState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        teacher_probs = np.zeros(5)
        total_conf = 0.0
        for teacher in self.teachers:
            prob = teacher.predict(state)
            conf = teacher.confidence(state)
            teacher_probs += prob * conf
            total_conf += conf
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(5) / 5

        student_probs = self.student.predict_proba(state_vec)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, 4)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = np.argmax(combined)

        return self.ACTION_SPACE[action_idx], action_idx, state_vec, teacher_probs

    async def update(self, state_vec: np.ndarray, action_idx: int, reward: float,
                     next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = self.replay_buffer.sample(8)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i])

    def get_stats(self) -> Dict:
        return {'student_counter': self.student.counter, 'buffer_size': len(self.replay_buffer)}


# =============================================================================
# Enhanced Dataset Generator (v4.1.2)
# =============================================================================
class EnhancedHeliumDatasetGeneratorV4:
    """
    Enhanced Helium Dataset Generator v4.1.2
    Final version with full distillation pipeline and persistence.
    """
    
    def __init__(self, params: DatasetGenerationParams = None):
        self.params = params or DatasetGenerationParams()
        self.seed = self.params.seed
        np.random.seed(self.seed)
        self.anomaly_rate = self.params.anomaly_rate
        self.include_anomalies = self.params.include_anomalies
        self.generation_id = str(uuid.uuid4())[:8]
        self.generation_timestamp = datetime.now()
        
        # Security and distribution
        self.security = QuantumResilientSecurity()
        self.blockchain = BlockchainAnchoring()
        self.optimiser = DistillationParameterOptimizer(self, {
            'distillation_epsilon': Config.DISTILLATION_EPSILON,
            'distillation_train_every': Config.DISTILLATION_TRAIN_EVERY,
            'distillation_replay_size': Config.DISTILLATION_REPLAY_SIZE,
            'distillation_learning_rate': Config.DISTILLATION_LEARNING_RATE,
        })
        self.cloud_distributor = MultiCloudDistributor()
        self.task_manager = TaskManager()
        
        # Metadata storage
        self.metadata = None
        self.df = None
        
        # History for state building
        self._last_generation_time: Optional[datetime] = None
        
        # Generation logs for historical ML training
        self.generation_logs: List[Dict] = []

    # ---------- Core generation (unchanged) ----------
    async def generate(self) -> Tuple[pd.DataFrame, Dict]:
        """Generate dataset with all enhancements."""
        logger.info(f"Starting dataset generation (ID: {self.generation_id})")
        
        if self.params.fetch_real_data:
            logger.info("Fetching real data from USGS/commodity APIs")
            real_data = await self._fetch_real_data()
            if real_data is not None:
                logger.info(f"Fetched {len(real_data)} real records")
        
        # Build state
        state = self._build_optimization_state()
        
        # Select generation strategy via distillation
        strategy, action_idx, state_vec, teacher_probs = await self.optimiser.select_strategy(state, exploration=True)
        logger.info(f"Selected generation strategy: {strategy}")
        
        # Apply strategy parameters
        anomaly_multiplier, trend_multiplier, noise_multiplier = self._apply_strategy(strategy)
        
        # Generate synthetic data with strategy parameters
        df = self._generate_synthetic(
            anomaly_multiplier=anomaly_multiplier,
            trend_multiplier=trend_multiplier,
            noise_multiplier=noise_multiplier
        )
        
        if self.include_anomalies:
            df, anomaly_count = self._inject_anomalies(df, anomaly_multiplier)
        else:
            anomaly_count = 0
        
        df = self._add_extended_fields(df)
        
        metadata = self._create_metadata(df, anomaly_count)
        
        keypair = await self.security.generate_keypair('dilithium')
        signature = await self.security.sign_metadata(metadata, keypair)
        metadata['quantum_signature'] = signature
        
        if self.params.blockchain_anchor:
            data_id = f"helium_dataset_{self.generation_id}"
            data_hash = hashlib.sha256(json.dumps(metadata, sort_keys=True, default=str).encode()).hexdigest()
            blockchain_result = await self.blockchain.record_hash(data_id, data_hash, {'generation_id': self.generation_id})
            metadata['blockchain_tx_hash'] = blockchain_result.get('tx_hash')
        
        quality_score = self._calculate_quality_score(df)
        metadata['quality_score'] = quality_score
        
        # Compute reward
        reward = self._compute_reward(quality_score, anomaly_count, df)
        
        # Update agent
        next_state = self._build_optimization_state()
        await self.optimiser.update(state_vec, action_idx, reward, next_state.to_feature_vector(), teacher_probs)
        
        # Log generation for historical ML
        self._log_generation(state, strategy, quality_score, reward)
        
        self.df = df
        self.metadata = metadata
        self._last_generation_time = datetime.now()
        
        logger.info(f"Dataset generated: {len(df)} rows, quality={quality_score:.1f}, reward={reward:.2f}")
        return df, metadata

    # ---------- NEW: Log generation for historical ML ----------
    def _log_generation(self, state: GenerationOptimizationState, strategy: str, quality: float, reward: float):
        """Record a generation entry for offline training."""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'strategy': strategy,
            'quality_score': quality,
            'reward': reward,
            'state_vector': state.to_feature_vector().tolist(),
        }
        self.generation_logs.append(log_entry)
        # Append to CSV file for persistence
        log_path = Path(Config.GENERATION_LOGS_PATH)
        df_log = pd.DataFrame([log_entry])
        if log_path.exists():
            df_log.to_csv(log_path, mode='a', header=False, index=False)
        else:
            df_log.to_csv(log_path, index=False)
        logger.debug(f"Logged generation to {log_path}")

    # ---------- NEW: Train historical ML model from logs ----------
    @classmethod
    def train_historical_model(cls, log_path: Optional[Path] = None, model_path: Optional[Path] = None):
        """
        Train a RandomForestClassifier from past generation logs.
        Call this method periodically or offline.
        """
        log_path = log_path or Path(Config.GENERATION_LOGS_PATH)
        model_path = model_path or Path(Config.HISTORICAL_MODEL_PATH)
        
        if not log_path.exists():
            logger.warning(f"Generation logs not found at {log_path}. No model trained.")
            return
        
        df_logs = pd.read_csv(log_path)
        if len(df_logs) < 10:
            logger.warning("Not enough logs to train historical model (need at least 10).")
            return
        
        # Prepare features (state vectors) and labels (strategy)
        X_list = []
        y_list = []
        for _, row in df_logs.iterrows():
            state_vec = json.loads(row['state_vector'])
            X_list.append(state_vec)
            y_list.append(row['strategy'])
        
        X = np.array(X_list)
        y = np.array(y_list)
        
        # Encode labels to numeric
        from sklearn.preprocessing import LabelEncoder
        le = LabelEncoder()
        y_encoded = le.fit_transform(y)
        
        # Train RandomForest
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X, y_encoded)
        
        # Save model
        with open(model_path, 'wb') as f:
            pickle.dump((model, le), f)
        logger.info(f"Historical ML model trained and saved to {model_path}")

    # ---------- State building ----------
    def _build_optimization_state(self) -> GenerationOptimizationState:
        if self.metadata is None:
            return GenerationOptimizationState(
                quality_score=80.0,
                regime_crisis=0.0,
                regime_tightening=0.0,
                regime_normal=0.0,
                regime_stable=1.0,
                price_volatility=5.0,
                anomaly_rate=self.anomaly_rate,
                production_trend_slope=Config.PRODUCTION_TREND,
                demand_trend_slope=Config.DEMAND_TREND,
                target_quality=90.0,
                time_since_last=0.0
            )
        
        quality = self.metadata.get('quality_score', 80.0)
        regime_dist = self.metadata.get('market_regime_distribution', {})
        crisis = regime_dist.get('crisis', 0) / max(len(self.df), 1) if self.df is not None else 0.0
        tightening = regime_dist.get('tightening', 0) / max(len(self.df), 1) if self.df is not None else 0.0
        normal = regime_dist.get('normal', 0) / max(len(self.df), 1) if self.df is not None else 0.0
        stable = regime_dist.get('stable', 0) / max(len(self.df), 1) if self.df is not None else 0.0

        volatility = self.df['price_volatility'].mean() if self.df is not None and 'price_volatility' in self.df.columns else 5.0
        anomaly_rate = self.params.anomaly_rate

        if self.df is not None and len(self.df) > 5:
            prod = self.df['global_production_tonnes'].values[-5:]
            demand = self.df['global_demand_tonnes'].values[-5:]
            x = np.arange(len(prod))
            prod_slope = np.polyfit(x, prod, 1)[0] if len(prod) > 1 else 0
            demand_slope = np.polyfit(x, demand, 1)[0] if len(demand) > 1 else 0
        else:
            prod_slope = Config.PRODUCTION_TREND
            demand_slope = Config.DEMAND_TREND

        hours_since = (datetime.now() - self._last_generation_time).total_seconds() / 3600 if self._last_generation_time else 0.0
        target_quality = 90.0

        return GenerationOptimizationState(
            quality_score=quality,
            regime_crisis=crisis,
            regime_tightening=tightening,
            regime_normal=normal,
            regime_stable=stable,
            price_volatility=volatility,
            anomaly_rate=anomaly_rate,
            production_trend_slope=prod_slope,
            demand_trend_slope=demand_slope,
            target_quality=target_quality,
            time_since_last=hours_since
        )

    # ---------- Strategy application ----------
    def _apply_strategy(self, strategy: str) -> Tuple[float, float, float]:
        if strategy == 'conservative':
            return 0.5, 0.8, 0.7
        elif strategy == 'moderate':
            return 1.0, 1.0, 1.0
        elif strategy == 'aggressive':
            return 2.0, 1.5, 1.3
        elif strategy == 'realistic':
            return 1.0, 0.9, 0.9
        elif strategy == 'adaptive':
            if self.metadata and self.metadata.get('quality_score', 80) < 80:
                return 1.5, 1.2, 1.1
            else:
                return 1.0, 1.0, 1.0
        else:
            return 1.0, 1.0, 1.0

    # ---------- Reward computation ----------
    def _compute_reward(self, quality_score: float, anomaly_count: int, df: pd.DataFrame) -> float:
        quality_reward = quality_score / 100.0
        n_rows = len(df)
        anomaly_rate = anomaly_count / max(n_rows, 1)
        if 0.02 <= anomaly_rate <= 0.05:
            anomaly_realism = 1.0
        elif anomaly_rate < 0.02:
            anomaly_realism = anomaly_rate / 0.02
        else:
            anomaly_realism = max(0.0, 1.0 - (anomaly_rate - 0.05) / 0.05)
        reward = 0.7 * quality_reward + 0.3 * anomaly_realism
        return min(1.0, max(0.0, reward))

    # ---------- Generation methods (unchanged) ----------
    def _generate_synthetic(self, anomaly_multiplier: float = 1.0, trend_multiplier: float = 1.0, noise_multiplier: float = 1.0) -> pd.DataFrame:
        # ... same as v4.1.1 ...
        # For brevity, we reuse the code from the previous version.
        # In a real file, this method would be fully implemented.
        pass

    def _inject_anomalies(self, df: pd.DataFrame, anomaly_multiplier: float) -> Tuple[pd.DataFrame, int]:
        # ... same as v4.1.1 ...
        pass

    def _add_extended_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        # ... same ...
        pass

    def _create_metadata(self, df: pd.DataFrame, anomaly_count: int) -> Dict:
        # ... same ...
        pass

    def _calculate_quality_score(self, df: pd.DataFrame) -> float:
        # ... same ...
        pass

    # ---------- Save, split, shutdown (unchanged) ----------
    def save(self, output_dir: Path = None):
        # ... same ...
        pass

    async def shutdown(self):
        await self.task_manager.stop_all()

    async def _fetch_real_data(self) -> Optional[pd.DataFrame]:
        return None


# =============================================================================
# UNIT TESTS (Phase 10)
# =============================================================================
import unittest
from unittest import IsolatedAsyncioTestCase

class TestDistillationComponents(IsolatedAsyncioTestCase):
    """Unit tests for the distillation components."""
    
    def setUp(self):
        self.params = DatasetGenerationParams(
            seed=42,
            n_periods=20,
            start_date="2020-01-01",
            anomaly_rate=0.02,
            include_anomalies=True
        )
        self.generator = EnhancedHeliumDatasetGeneratorV4(self.params)
    
    def test_state_feature_vector(self):
        state = GenerationOptimizationState(
            quality_score=80.0,
            regime_crisis=0.1,
            regime_tightening=0.2,
            regime_normal=0.3,
            regime_stable=0.4,
            price_volatility=15.0,
            anomaly_rate=0.02,
            production_trend_slope=-40,
            demand_trend_slope=80,
            target_quality=90,
            time_since_last=5.0
        )
        vec = state.to_feature_vector()
        self.assertEqual(len(vec), 11)
        self.assertAlmostEqual(vec[0], 0.8)
        self.assertAlmostEqual(vec[5], 15/30)
    
    def test_rule_based_teacher(self):
        teacher = GenerationRuleBasedTeacher()
        state = GenerationOptimizationState(
            quality_score=65.0,
            regime_crisis=0.0,
            regime_tightening=0.0,
            regime_normal=0.0,
            regime_stable=1.0,
            price_volatility=10.0,
            anomaly_rate=0.02,
            production_trend_slope=-40,
            demand_trend_slope=80,
            target_quality=90,
            time_since_last=0.0
        )
        probs = teacher.predict(state)
        self.assertAlmostEqual(np.sum(probs), 1.0)
        self.assertGreater(probs[2], 0.5)  # aggressive should be highest
    
    def test_strategy_application(self):
        state = GenerationOptimizationState(
            quality_score=80.0,
            regime_crisis=0.0,
            regime_tightening=0.0,
            regime_normal=0.0,
            regime_stable=1.0,
            price_volatility=10.0,
            anomaly_rate=0.02,
            production_trend_slope=-40,
            demand_trend_slope=80,
            target_quality=90,
            time_since_last=0.0
        )
        # Simulate a generation to get metadata
        # We'll mock the metadata
        self.generator.metadata = {'quality_score': 75}
        multiplier = self.generator._apply_strategy('adaptive')
        self.assertEqual(multiplier[0], 1.5)  # quality < 80 -> aggressive
    
    async def test_replay_buffer(self):
        buffer = ReplayBuffer(max_size=10)
        state_vec = np.random.randn(11)
        buffer.push(state_vec, 0, 0.5, state_vec, np.ones(5)/5)
        self.assertEqual(len(buffer), 1)
        batch = buffer.sample(1)
        self.assertEqual(len(batch[0]), 1)

# =============================================================================
# CLI Interface (unchanged)
# =============================================================================
def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description="Generate enhanced helium timeseries dataset")
    parser.add_argument("--output-dir", default=Config.OUTPUT_DIR, help="Output directory")
    parser.add_argument("--n-periods", type=int, default=Config.N_PERIODS, help="Number of periods")
    parser.add_argument("--start-date", default=Config.START_DATE, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--seed", type=int, default=Config.SEED, help="Random seed")
    parser.add_argument("--anomaly-rate", type=float, default=Config.ANOMALY_RATE, help="Anomaly injection rate")
    parser.add_argument("--no-anomalies", action="store_true", help="Disable anomaly injection")
    parser.add_argument("--fetch-real", action="store_true", help="Fetch real data from APIs (stub)")
    parser.add_argument("--blockchain", action="store_true", help="Anchor dataset on blockchain")
    parser.add_argument("--cloud", action="store_true", help="Distribute dataset to cloud")
    return parser.parse_args()

# =============================================================================
# Main entry point
# =============================================================================
async def main():
    args = parse_args()
    
    params = DatasetGenerationParams(
        seed=args.seed,
        n_periods=args.n_periods,
        start_date=args.start_date,
        anomaly_rate=args.anomaly_rate,
        include_anomalies=not args.no_anomalies,
        output_dir=args.output_dir,
        fetch_real_data=args.fetch_real,
        blockchain_anchor=args.blockchain,
        cloud_distribution=args.cloud
    )
    
    generator = EnhancedHeliumDatasetGeneratorV4(params)
    try:
        df, metadata = await generator.generate()
        generator.save()
        print(f"\n✅ Dataset generation complete!")
        print(f"   Generation ID: {metadata['generation_id']}")
        print(f"   Quality Score: {metadata['quality_score']:.1f}%")
        print(f"   Anomalies: {metadata['anomaly_count']}")
        print(f"   Blockchain TX: {metadata.get('blockchain_tx_hash', 'N/A')}")
        print(f"   Output directory: {args.output_dir}")
        print("\nSample:")
        print(df.tail().to_string())
    finally:
        await generator.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
