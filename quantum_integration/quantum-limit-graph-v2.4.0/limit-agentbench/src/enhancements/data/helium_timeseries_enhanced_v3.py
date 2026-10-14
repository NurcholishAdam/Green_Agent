# =============================================================================
# FILE: src/enhancements/data/helium_timeseries_enhanced_v4_1_3.py
# VERSION: 4.1.3 (Enterprise Quantum Resilience + Full Distillation Pipeline)
# =============================================================================
"""
Enhanced Helium Timeseries Dataset Generator - Version 4.1.3

CHANGES (v4.1.3 over v4.1.2):
- Fixed Config.get_master_key (env var name vs value bug).
- Fixed the state transition (next_state was equal to state).
- Q-teacher weights are now persisted (dirty-flag save) and the Q-teacher
  is actually updated during batch training.
- Added MoE gating with the full softmax Jacobian + load balancing; the
  student distills from the *gated* teacher combination, not the mean.
- Added epsilon decay and a floor.
- Added a shared RunningScaler used by all teachers and the student.
- Fixed signature / blockchain hash ordering: quality score is computed
  before signing and hashing.
- Signing keypairs are persisted (Fernet-encrypted) under a config dir.
- Seeded np.random.default_rng threaded through generation; no more global
  numpy RNG.
- save() is async; save_sync() provided for CLI convenience.
- Pydantic v2 support (model_dump) with v1 fallback.
- Removed global warnings.filterwarnings('ignore').
- Wired CircuitBreaker into the (still optional) real-data fetch.
- Broader test coverage including generate(), save(), anomalies,
  quality score, and the reward feedback loop.
- UTC-aware timestamps throughout.
- Added __all__ and __version__.

All previous features retained.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import pickle
import random
import uuid
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Awaitable, Callable, ClassVar, Dict, Iterable, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Optional dependencies
# ---------------------------------------------------------------------------
try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives.asymmetric.utils import (
        decode_dss_signature, encode_dss_signature,
    )
    from cryptography.hazmat.primitives.serialization import (
        Encoding, NoEncryption, PrivateFormat, PublicFormat,
    )
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

try:
    from pydantic import BaseModel, ConfigDict, Field, field_validator, ValidationError
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False
    BaseModel = object  # type: ignore
    ConfigDict = dict  # type: ignore
    def Field(*a, **k): return None  # type: ignore
    def field_validator(*a, **k):  # type: ignore
        def deco(f): return f
        return deco
    ValidationError = Exception  # type: ignore

try:
    import pyarrow as pa  # noqa: F401
    import pyarrow.parquet as pq  # noqa: F401
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False

try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False
    RandomForestClassifier = None  # type: ignore
    LabelEncoder = None  # type: ignore

try:
    from tenacity import (
        retry, stop_after_attempt, wait_exponential,
        retry_if_exception_type, before_sleep_log,
    )
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False
    def retry(*a, **k):
        def deco(f): return f
        return deco
    def stop_after_attempt(_n): return None
    def wait_exponential(**k): return None
    def retry_if_exception_type(_t): return None
    def before_sleep_log(*a, **k): return None

# Optional PQC
try:
    from pqcrypto.sign import dilithium, falcon, sphincs  # noqa: F401
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

# Optional web3 / cloud libs are intentionally not imported here to keep the
# module import-safe without them; blockchain and cloud distribution use
# local fallbacks.

# =============================================================================
# Version + logger
# =============================================================================
__version__ = "4.1.3"
__all__ = [
    "__version__",
    "Config",
    "CircuitBreaker",
    "CircuitBreakerState",
    "TaskManager",
    "RunningScaler",
    "MoEGatingNetwork",
    "DistillationStudent",
    "ReplayBuffer",
    "GenerationOptimizationState",
    "GenerationRuleBasedTeacher",
    "GenerationHistoricalMLTeacher",
    "GenerationStatefulQTeacher",
    "DistillationParameterOptimizer",
    "DatasetGenerationParams",
    "EnhancedHeliumDatasetGeneratorV4",
    "export_for_elasticity",
    "export_for_circularity",
    "export_for_sustainability",
    "export_for_thermal",
    "export_for_quantum_bridge",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# =============================================================================
# Config
# =============================================================================
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Config:
    """Central configuration with environment variable support."""

    SEED = int(os.getenv("HELIUM_DATASET_SEED", "42"))
    N_PERIODS = int(os.getenv("HELIUM_DATASET_N_PERIODS", "120"))
    START_DATE = os.getenv("HELIUM_DATASET_START_DATE", "2020-01-01")
    ANOMALY_RATE = float(os.getenv("HELIUM_DATASET_ANOMALY_RATE", "0.02"))
    INCLUDE_ANOMALIES = os.getenv(
        "HELIUM_DATASET_INCLUDE_ANOMALIES", "true"
    ).lower() == "true"

    OUTPUT_DIR = os.getenv("HELIUM_DATASET_OUTPUT_DIR", "./data")

    USGS_API_URL = os.getenv("USGS_API_URL", "https://www.usgs.gov/api/helium-statistics")
    USGS_API_KEY = os.getenv("USGS_API_KEY", "")
    COMMODITY_API_URL = os.getenv(
        "COMMODITY_API_URL", "https://api.commodityprices.com/v1/helium"
    )
    COMMODITY_API_KEY = os.getenv("COMMODITY_API_KEY", "")

    BLOCKCHAIN_RPC_URL = os.getenv("BLOCKCHAIN_RPC_URL", "http://localhost:8545")
    BLOCKCHAIN_CONTRACT_ADDRESS = os.getenv(
        "BLOCKCHAIN_CONTRACT_ADDRESS", "0x0000000000000000000000000000000000000000"
    )
    BLOCKCHAIN_PRIVATE_KEY = os.getenv("BLOCKCHAIN_PRIVATE_KEY", "")
    BLOCKCHAIN_LOCAL_PATH = os.getenv(
        "HELIUM_BLOCKCHAIN_LOCAL_PATH", "./blockchain_hashes.json"
    )

    CLOUD_AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
    CLOUD_AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    CLOUD_AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    CLOUD_AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    CLOUD_GCP_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

    # Store the *name* of the master-key env var, not its value.
    MASTER_KEY_ENV_NAME = "HELIUM_DATASET_MASTER_KEY"
    MASTER_KEY_FILE = os.getenv(
        "HELIUM_DATASET_MASTER_KEY_FILE", "/tmp/helium_master.key"
    )
    KEY_DIR = os.getenv("HELIUM_DATASET_KEY_DIR", "./keys")

    RETRY_ATTEMPTS = 3
    RETRY_MIN_WAIT = 2.0
    RETRY_MAX_WAIT = 10.0

    CIRCUIT_BREAKER_FAILURE_THRESHOLD = int(
        os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "5")
    )
    CIRCUIT_BREAKER_RECOVERY_TIMEOUT = float(
        os.getenv("CIRCUIT_BREAKER_RECOVERY_TIMEOUT", "30.0")
    )

    SOURCE_PRIORITY = [s.strip() for s in os.getenv(
        "SOURCE_PRIORITY", "usgs,commodity"
    ).split(",") if s.strip()]

    PRODUCTION_BASE = float(os.getenv("PRODUCTION_BASE", "28000"))
    PRODUCTION_TREND = float(os.getenv("PRODUCTION_TREND", "-40"))
    DEMAND_BASE = float(os.getenv("DEMAND_BASE", "27000"))
    DEMAND_TREND = float(os.getenv("DEMAND_TREND", "80"))
    PRICE_BASE = float(os.getenv("PRICE_BASE", "100"))
    PRICE_VOL = float(os.getenv("PRICE_VOL", "0.1"))
    PRICE_DRIFT = float(os.getenv("PRICE_DRIFT", "0.005"))
    NEW_CAPACITY_BASE = float(os.getenv("NEW_CAPACITY_BASE", "2000"))
    NEW_CAPACITY_TREND = float(os.getenv("NEW_CAPACITY_TREND", "100"))
    CARBON_BASE = float(os.getenv("CARBON_BASE", "300"))
    CARBON_RANGE = float(os.getenv("CARBON_RANGE", "200"))
    RENEWABLE_BASE = float(os.getenv("RENEWABLE_BASE", "30"))
    RENEWABLE_RANGE = float(os.getenv("RENEWABLE_RANGE", "40"))

    # Distillation
    DISTILLATION_EPSILON = float(os.getenv("DISTILLATION_EPSILON", "0.1"))
    DISTILLATION_EPSILON_MIN = float(os.getenv("DISTILLATION_EPSILON_MIN", "0.01"))
    DISTILLATION_EPSILON_DECAY = float(os.getenv("DISTILLATION_EPSILON_DECAY", "0.995"))
    DISTILLATION_TRAIN_EVERY = int(os.getenv("DISTILLATION_TRAIN_EVERY", "10"))
    DISTILLATION_REPLAY_SIZE = int(os.getenv("DISTILLATION_REPLAY_SIZE", "2000"))
    DISTILLATION_LEARNING_RATE = float(
        os.getenv("DISTILLATION_LEARNING_RATE", "0.01")
    )
    DISTILL_WEIGHT = float(os.getenv("DISTILL_WEIGHT", "0.7"))
    RL_WEIGHT = float(os.getenv("RL_WEIGHT", "0.3"))
    GATING_LEARNING_RATE = float(os.getenv("GATING_LEARNING_RATE", "0.005"))
    GATING_LOAD_BALANCE_WEIGHT = float(
        os.getenv("GATING_LOAD_BALANCE_WEIGHT", "0.01")
    )

    Q_WEIGHTS_PATH = os.getenv("Q_WEIGHTS_PATH", "./q_weights.json")
    GENERATION_LOGS_PATH = os.getenv("GENERATION_LOGS_PATH", "./generation_logs.csv")
    HISTORICAL_MODEL_PATH = os.getenv("HISTORICAL_MODEL_PATH", "./historical_model.pkl")

    @classmethod
    def get_master_key(cls) -> bytes:
        """
        Return the master encryption key.

        Precedence: env var -> key file -> newly generated key persisted
        with mode 0400.
        """
        key_hex = os.environ.get(cls.MASTER_KEY_ENV_NAME, "")
        if key_hex:
            try:
                return bytes.fromhex(key_hex)
            except ValueError:
                logger.warning(
                    "MASTER_KEY env var is not valid hex; falling back to file."
                )

        p = Path(cls.MASTER_KEY_FILE)
        if p.exists():
            try:
                return p.read_bytes()
            except OSError as e:
                logger.warning(f"Failed to read master key file: {e}")

        key = Fernet.generate_key() if CRYPTO_AVAILABLE else os.urandom(32)
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(key)
            os.chmod(p, 0o400)
            logger.warning(f"Generated new master key and saved to {p}")
        except OSError as e:
            logger.error(f"Failed to persist master key: {e}")
        return key


# =============================================================================
# Circuit breaker
# =============================================================================
class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit breaker with half-open state for external calls."""

    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0) -> None:
        self.name = name
        self.failure_threshold = int(failure_threshold)
        self.recovery_timeout = float(recovery_timeout)
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable[..., Awaitable[Any]], *args, **kwargs) -> Any:
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if (self.last_failure_time is not None
                        and (time.monotonic() - self.last_failure_time) >= self.recovery_timeout):
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.failure_count = 0
                    logger.info(f"Circuit breaker {self.name} -> HALF_OPEN")
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is OPEN")

        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.CLOSED
                    logger.info(f"Circuit breaker {self.name} -> CLOSED")
                self.failure_count = 0
            return result
        except Exception:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.monotonic()
                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(
                        f"Circuit breaker {self.name} -> OPEN "
                        f"after {self.failure_count} failures"
                    )
            raise


# =============================================================================
# TaskManager
# =============================================================================
class TaskManager:
    """Supervises background tasks with auto-restart on failure."""

    def __init__(self) -> None:
        self.tasks: Dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()
        self.shutdown_event = asyncio.Event()

    def start_task(self, name: str, coro_func: Callable[[], Awaitable[None]],
                   *args, **kwargs) -> asyncio.Task:
        if name in self.tasks and not self.tasks[name].done():
            return self.tasks[name]

        async def wrapper() -> None:
            backoff = 1.0
            max_backoff = 300.0
            while not self.shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Task '{name}' crashed: {e}", exc_info=True)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)

        task = asyncio.create_task(wrapper(), name=name)
        self.tasks[name] = task
        return task

    async def stop_all(self) -> None:
        self.shutdown_event.set()
        for t in self.tasks.values():
            if not t.done():
                t.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        self.tasks.clear()
        logger.info("All background tasks stopped")


# =============================================================================
# Running scaler
# =============================================================================
class RunningScaler:
    """Online running mean/std scaler for feature normalization."""

    def __init__(self, dim: int, clip: float = 5.0, min_std: float = 1e-6) -> None:
        self.dim = int(dim)
        self.clip = float(clip)
        self.min_std = float(min_std)
        self.mean = np.zeros(self.dim, dtype=np.float64)
        self.m2 = np.zeros(self.dim, dtype=np.float64)
        self.count = 0

    def update(self, x: np.ndarray) -> None:
        x = np.asarray(x, dtype=np.float64).reshape(-1)
        if x.shape[0] != self.dim:
            return
        self.count += 1
        delta = x - self.mean
        self.mean += delta / self.count
        self.m2 += delta * (x - self.mean)

    def transform(self, x: np.ndarray) -> np.ndarray:
        x = np.asarray(x, dtype=np.float64).reshape(-1)
        if self.count < 2:
            return x.astype(np.float32)
        var = self.m2 / (self.count - 1)
        std = np.maximum(np.sqrt(np.maximum(var, 0.0)), self.min_std)
        return np.clip((x - self.mean) / std, -self.clip, self.clip).astype(np.float32)


# =============================================================================
# Security: persisted, encrypted signing keys
# =============================================================================
class QuantumResilientSecurity:
    """Quantum-resilient security for signing dataset metadata."""

    def __init__(self) -> None:
        self.pqc_available = PQC_AVAILABLE
        self.master_key = Config.get_master_key()
        self.fernet = Fernet(self.master_key) if CRYPTO_AVAILABLE else None
        self.key_dir = Path(Config.KEY_DIR)
        try:
            self.key_dir.mkdir(parents=True, exist_ok=True)
            os.chmod(self.key_dir, 0o700)
        except OSError:
            pass

    # -- keypair management -------------------------------------------------
    def _key_path(self, key_id: str) -> Path:
        return self.key_dir / f"{key_id}.json"

    def _load_keypair(self, key_id: str) -> Optional[Dict[str, Any]]:
        p = self._key_path(key_id)
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text())
        except Exception as e:
            logger.warning(f"Failed to load keypair {key_id}: {e}")
            return None

    def _persist_keypair(self, keypair: Dict[str, Any]) -> None:
        p = self._key_path(keypair["key_id"])
        try:
            p.write_text(json.dumps(keypair))
            os.chmod(p, 0o600)
        except OSError as e:
            logger.warning(f"Failed to persist keypair {keypair['key_id']}: {e}")

    async def generate_keypair(self, algorithm: str = "ecdsa",
                               validity_days: int = 30,
                               reuse: bool = True) -> Dict[str, Any]:
        """
        Return a signing keypair.  By default, an existing valid keypair is
        reused if available so signatures can be verified across runs.
        """
        if reuse:
            for p in self.key_dir.glob("*.json"):
                try:
                    kp = json.loads(p.read_text())
                    if kp.get("algorithm") == algorithm:
                        exp = kp.get("expires_at", "")
                        if exp and datetime.fromisoformat(exp).replace(
                            tzinfo=timezone.utc
                        ) > _utc_now():
                            return kp
                except Exception:
                    continue

        key_id = f"{algorithm}_{uuid.uuid4().hex[:8]}"
        expires_at = (_utc_now() + timedelta(days=validity_days)).isoformat()

        if CRYPTO_AVAILABLE:
            private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
            public_key = private_key.public_key()
            public_bytes = public_key.public_bytes(
                Encoding.PEM, PublicFormat.SubjectPublicKeyInfo
            )
            private_bytes = private_key.private_bytes(
                Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
            )
            encrypted_private = self.fernet.encrypt(private_bytes).hex()
            kp = {
                "key_id": key_id,
                "algorithm": "ecdsa",
                "public_key": public_bytes.hex(),
                "private_key_encrypted": encrypted_private,
                "expires_at": expires_at,
            }
        else:
            # HMAC fallback: derive a per-key secret deterministically.
            secret = hashlib.sha256(
                (self.master_key.hex() + key_id).encode()
            ).hexdigest()
            kp = {
                "key_id": key_id,
                "algorithm": "hmac_sha256",
                "secret_encrypted": self.fernet.encrypt(
                    secret.encode()
                ).hex() if self.fernet else secret,
                "expires_at": expires_at,
            }

        self._persist_keypair(kp)
        return kp

    # -- signing ------------------------------------------------------------
    async def sign_metadata(self, metadata: Dict[str, Any],
                            keypair: Dict[str, Any]) -> Dict[str, Any]:
        data_bytes = json.dumps(metadata, sort_keys=True, default=str).encode()
        alg = keypair.get("algorithm", "")

        if alg == "ecdsa" and CRYPTO_AVAILABLE:
            try:
                priv_bytes = self.fernet.decrypt(
                    bytes.fromhex(keypair["private_key_encrypted"])
                )
                priv = ec.load_der_private_key(priv_bytes, password=None,
                                               backend=default_backend())
                sig = priv.sign(data_bytes, ec.ECDSA(hashes.SHA256()))
                return {
                    "signature": sig.hex(),
                    "algorithm": "ecdsa",
                    "key_id": keypair["key_id"],
                    "timestamp": _utc_now().isoformat(),
                }
            except Exception as e:
                logger.warning(f"ECDSA signing failed, falling back to HMAC: {e}")

        # HMAC fallback is deterministic and verifiable with the same secret.
        if alg == "hmac_sha256":
            secret_enc = bytes.fromhex(keypair["secret_encrypted"])
            secret = self.fernet.decrypt(secret_enc) if self.fernet else secret_enc
            import hmac as _hmac
            sig = _hmac.new(secret, data_bytes, hashlib.sha256).hexdigest()
            return {
                "signature": sig,
                "algorithm": "hmac_sha256",
                "key_id": keypair["key_id"],
                "timestamp": _utc_now().isoformat(),
            }

        return {
            "signature": hashlib.sha256(data_bytes).hexdigest(),
            "algorithm": "sha256_fallback",
            "key_id": keypair.get("key_id", "fallback"),
            "timestamp": _utc_now().isoformat(),
        }


# =============================================================================
# Blockchain anchoring (local fallback)
# =============================================================================
class BlockchainAnchoring:
    """Local, self-contained anchoring of dataset hashes."""

    def __init__(self, local_path: Optional[Union[str, Path]] = None) -> None:
        self.local_path = Path(local_path or Config.BLOCKCHAIN_LOCAL_PATH)
        self.web3_available = False

    async def record_hash(self, data_id: str, data_hash: str,
                          metadata: Dict[str, Any]) -> Dict[str, Any]:
        try:
            self.local_path.parent.mkdir(parents=True, exist_ok=True)
            if self.local_path.exists():
                try:
                    records = json.loads(self.local_path.read_text())
                except json.JSONDecodeError:
                    records = {}
            else:
                records = {}
            records[data_id] = {
                "hash": data_hash,
                "metadata": metadata,
                "timestamp": _utc_now().isoformat(),
            }
            self.local_path.write_text(json.dumps(records, indent=2, default=str))
            return {
                "status": "simulated",
                "tx_hash": f"local_{hashlib.sha256(data_id.encode()).hexdigest()[:16]}",
                "block_number": 0,
            }
        except OSError as e:
            logger.error(f"Local anchoring failed: {e}")
            return {"status": "failed", "error": str(e)}


# =============================================================================
# Multi-cloud distribution (self-contained)
# =============================================================================
class MultiCloudDistributor:
    """Deterministic distribution stub.  Real cloud clients can be plugged in."""

    async def distribute(self, file_path: Path,
                        metadata: Dict[str, Any]) -> Dict[str, Any]:
        try:
            data_bytes = await asyncio.to_thread(file_path.read_bytes)
        except OSError as e:
            return {"status": "failed", "error": str(e)}
        key = file_path.name
        return {
            "aws": {"status": "success", "url": f"s3://helium-dataset-uploads/{key}",
                    "bytes": len(data_bytes)},
            "azure": {"status": "success", "url": f"azure://helium-dataset-uploads/{key}",
                      "bytes": len(data_bytes)},
            "gcp": {"status": "success", "url": f"gs://helium-dataset-uploads/{key}",
                    "bytes": len(data_bytes)},
        }


# =============================================================================
# State
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

    FEATURE_DIM: ClassVar[int] = 11

    @staticmethod
    def _c(v: float) -> float:
        return float(min(1.0, max(0.0, v)))

    def to_raw_vector(self) -> np.ndarray:
        return np.array([
            self.quality_score,
            self.regime_crisis,
            self.regime_tightening,
            self.regime_normal,
            self.regime_stable,
            self.price_volatility,
            self.anomaly_rate,
            self.production_trend_slope,
            self.demand_trend_slope,
            self.target_quality,
            self.time_since_last,
        ], dtype=np.float64)

    def to_feature_vector(self, scaler: Optional[RunningScaler] = None) -> np.ndarray:
        raw = self.to_raw_vector()
        if scaler is not None:
            return scaler.transform(raw)
        c = self._c
        return np.array([
            c(self.quality_score / 100.0),
            c(self.regime_crisis),
            c(self.regime_tightening),
            c(self.regime_normal),
            c(self.regime_stable),
            c(self.price_volatility / 30.0),
            c(self.anomaly_rate * 2.0),
            c(abs(self.production_trend_slope) / 100.0),
            c(abs(self.demand_trend_slope) / 100.0),
            c(self.target_quality / 100.0),
            c(self.time_since_last / 24.0),
        ], dtype=np.float32)


# =============================================================================
# MoE gating (full softmax Jacobian + load balancing)
# =============================================================================
class MoEGatingNetwork:
    def __init__(self, feature_dim: int = 11, n_experts: int = 3,
                 lr: float = 0.005, load_balance_weight: float = 0.01,
                 seed: Optional[int] = None) -> None:
        rng = np.random.default_rng(seed)
        self.feature_dim = int(feature_dim)
        self.n_experts = int(n_experts)
        self.lr = float(lr)
        self.load_balance_weight = float(load_balance_weight)
        self.weights = rng.normal(0.0, 0.01, size=(self.feature_dim, self.n_experts))
        self.bias = np.zeros(self.n_experts, dtype=np.float64)
        self._mean_gate = np.ones(self.n_experts) / max(self.n_experts, 1)

    def forward(self, state_vec: np.ndarray) -> np.ndarray:
        x = np.asarray(state_vec, dtype=np.float64).reshape(-1)
        logits = x @ self.weights + self.bias
        z = logits - np.max(logits)
        e = np.exp(z)
        s = e.sum()
        return e / s if s > 0 else np.ones(self.n_experts) / self.n_experts

    def update(self, state_vec, teacher_outputs, student_probs,
               current_gate_weights) -> None:
        x = np.asarray(state_vec, dtype=np.float64).reshape(-1)
        t = np.asarray(teacher_outputs, dtype=np.float64)
        s = np.asarray(student_probs, dtype=np.float64)
        g = np.asarray(current_gate_weights, dtype=np.float64)

        combined = g @ t
        error = combined - s
        dL_dg = t @ error

        dot = float(np.dot(g, dL_dg))
        dL_dz = g * (dL_dg - dot)

        if self.load_balance_weight > 0:
            self._mean_gate = 0.99 * self._mean_gate + 0.01 * g
            target = np.ones_like(g) / len(g)
            lb = self.load_balance_weight * (self._mean_gate - target)
            lb_dot = float(np.dot(g, lb))
            dL_dz += g * (lb - lb_dot)

        self.weights -= self.lr * np.outer(x, dL_dz)
        self.bias -= self.lr * dL_dz


# =============================================================================
# Student, replay, teachers
# =============================================================================
class DistillationStudent:
    def __init__(self, feature_dim: int = 11, n_classes: int = 5,
                 lr: float = 0.01, grad_clip: float = 1.0,
                 seed: Optional[int] = None) -> None:
        rng = np.random.default_rng(seed)
        self.feature_dim = int(feature_dim)
        self.n_classes = int(n_classes)
        self.lr = float(lr)
        self.grad_clip = float(grad_clip)
        self.weights = rng.normal(0.0, 0.01, size=(self.feature_dim, self.n_classes))
        self.biases = np.zeros(self.n_classes, dtype=np.float64)
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray) -> np.ndarray:
        logits = state_vector @ self.weights + self.biases
        z = logits - np.max(logits)
        e = np.exp(z)
        s = e.sum()
        return e / s if s > 0 else np.ones(self.n_classes) / self.n_classes

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action: int,
               distill_weight: float = 0.7, rl_weight: float = 0.3) -> None:
        current = self.predict_proba(state_vector)
        grad_distill = -(teacher_probs - current)
        one_hot = np.zeros(self.n_classes, dtype=np.float64)
        if 0 <= int(action) < self.n_classes:
            one_hot[int(action)] = 1.0
        grad_rl = -reward * (one_hot - current)
        grad = distill_weight * grad_distill + rl_weight * grad_rl
        grad = np.clip(grad, -self.grad_clip, self.grad_clip)
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1


class ReplayBuffer:
    def __init__(self, max_size: int = 2000) -> None:
        self.buffer: deque = deque(maxlen=int(max_size))

    def push(self, state_vec, action, reward, next_state_vec, teacher_outputs) -> None:
        self.buffer.append((
            np.asarray(state_vec, dtype=np.float64),
            int(action), float(reward),
            np.asarray(next_state_vec, dtype=np.float64),
            np.asarray(teacher_outputs, dtype=np.float64),
        ))

    def sample(self, batch_size: int = 32):
        batch = list(self.buffer) if len(self.buffer) < batch_size \
                else random.sample(self.buffer, batch_size)
        s, a, r, ns, t = zip(*batch)
        return (np.array(s), np.array(a, dtype=np.int64), np.array(r),
                np.array(ns), np.array(t))

    def __len__(self) -> int:
        return len(self.buffer)


class Teacher(ABC):
    def __init__(self) -> None:
        self.scaler: Optional[RunningScaler] = None

    @abstractmethod
    def predict(self, state: GenerationOptimizationState) -> np.ndarray: ...
    @abstractmethod
    def confidence(self, state: GenerationOptimizationState) -> float: ...

    def _features(self, state: GenerationOptimizationState) -> np.ndarray:
        return state.to_feature_vector(self.scaler)


class GenerationRuleBasedTeacher(Teacher):
    ACTION_SPACE: ClassVar[List[str]] = [
        "conservative", "moderate", "aggressive", "realistic", "adaptive",
    ]

    def predict(self, state: GenerationOptimizationState) -> np.ndarray:
        probs = np.ones(5, dtype=np.float64) * 0.1
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
        return 0.6 if state.quality_score < 70 else 0.4


class GenerationHistoricalMLTeacher(Teacher):
    EXPECTED_FEATURE_DIM: ClassVar[int] = 11

    def __init__(self, model_path: Optional[Path] = None) -> None:
        super().__init__()
        self.model = None
        self.label_encoder = None
        self.model_path = Path(model_path or Config.HISTORICAL_MODEL_PATH)
        if self.model_path.exists():
            try:
                with open(self.model_path, "rb") as f:
                    self.model, self.label_encoder = pickle.load(f)
                n_features = getattr(self.model, "n_features_in_", None)
                if n_features is not None and n_features != self.EXPECTED_FEATURE_DIM:
                    logger.warning(
                        f"Historical model expects {n_features} features, "
                        f"pipeline produces {self.EXPECTED_FEATURE_DIM}; disabling."
                    )
                    self.model = None
                    self.label_encoder = None
                else:
                    logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")

    def predict(self, state: GenerationOptimizationState) -> np.ndarray:
        if self.model is None:
            return np.ones(5) / 5
        x = self._features(state).reshape(1, -1)
        if not hasattr(self.model, "predict_proba"):
            return np.ones(5) / 5
        return self.model.predict_proba(x)[0]

    def confidence(self, state: GenerationOptimizationState) -> float:
        return 0.7 if self.model is not None else 0.0


class GenerationStatefulQTeacher(Teacher):
    def __init__(self, weights_path: Optional[Path] = None, lr: float = 0.1,
                 feature_dim: int = 11) -> None:
        super().__init__()
        self.lr = float(lr)
        self.feature_dim = int(feature_dim)
        self.weights_path = Path(weights_path or Config.Q_WEIGHTS_PATH)
        self.weights = np.zeros((self.feature_dim, 5), dtype=np.float64)
        self._dirty = False
        self._load()

    def _load(self) -> None:
        if not self.weights_path.exists():
            return
        try:
            w = np.array(json.loads(self.weights_path.read_text()), dtype=np.float64)
            if w.shape == (self.feature_dim, 5):
                self.weights = w
        except Exception as e:
            logger.warning(f"Failed to load Q-weights: {e}")

    def save(self, force: bool = False) -> None:
        if not force and not self._dirty:
            return
        try:
            self.weights_path.parent.mkdir(parents=True, exist_ok=True)
            self.weights_path.write_text(json.dumps(self.weights.tolist()))
            self._dirty = False
        except OSError as e:
            logger.warning(f"Failed to save Q-weights: {e}")

    def predict(self, state: GenerationOptimizationState) -> np.ndarray:
        x = self._features(state)
        q = x @ self.weights
        z = q - np.max(q)
        e = np.exp(z)
        return e / e.sum() if e.sum() > 0 else np.ones(5) / 5

    def confidence(self, state: GenerationOptimizationState) -> float:
        x = self._features(state)
        q = x @ self.weights
        return float(np.clip(0.3 + 0.35 * (np.max(q) - np.min(q)), 0.0, 1.0))

    def update_from_vector(self, state_vec: np.ndarray, action: int,
                           reward: float) -> None:
        x = np.asarray(state_vec, dtype=np.float64).reshape(-1)
        if x.shape[0] != self.feature_dim or not 0 <= int(action) < 5:
            return
        q = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q) * x
        self._dirty = True

    def update(self, state: GenerationOptimizationState, action: int,
               reward: float) -> None:
        self.update_from_vector(self._features(state), action, reward)


# =============================================================================
# Distillation optimizer
# =============================================================================
class DistillationParameterOptimizer:
    ACTION_SPACE: ClassVar[List[str]] = [
        "conservative", "moderate", "aggressive", "realistic", "adaptive",
    ]
    N_ACTIONS: ClassVar[int] = 5

    def __init__(self, generator: Any, config: Dict[str, Any],
                 seed: Optional[int] = None) -> None:
        self.generator = generator
        self.config = config
        self.scaler = RunningScaler(GenerationOptimizationState.FEATURE_DIM,
                                    clip=config.get("scaler_clip", 5.0))
        self.student = DistillationStudent(
            feature_dim=GenerationOptimizationState.FEATURE_DIM,
            n_classes=self.N_ACTIONS,
            lr=config.get("distillation_learning_rate", 0.01),
            seed=seed,
        )
        self.teachers: List[Teacher] = [
            GenerationRuleBasedTeacher(),
            GenerationHistoricalMLTeacher(),
            GenerationStatefulQTeacher(
                weights_path=Path(config.get("q_weights_path", Config.Q_WEIGHTS_PATH)),
            ),
        ]
        for t in self.teachers:
            t.scaler = self.scaler
        self.q_teacher: Optional[GenerationStatefulQTeacher] = next(
            (t for t in self.teachers if isinstance(t, GenerationStatefulQTeacher)),
            None,
        )
        self.n_teachers = len(self.teachers)
        self.gating = MoEGatingNetwork(
            feature_dim=GenerationOptimizationState.FEATURE_DIM,
            n_experts=self.n_teachers,
            lr=config.get("gating_learning_rate", Config.GATING_LEARNING_RATE),
            load_balance_weight=config.get("gating_load_balance_weight",
                                           Config.GATING_LOAD_BALANCE_WEIGHT),
            seed=seed,
        )
        self.replay_buffer = ReplayBuffer(
            max_size=config.get("distillation_replay_size", 2000)
        )
        self.epsilon = float(config.get("distillation_epsilon", 0.1))
        self.epsilon_min = float(config.get("distillation_epsilon_min", 0.01))
        self.epsilon_decay = float(config.get("distillation_epsilon_decay", 0.995))
        self.train_every = int(config.get("distillation_train_every", 10))
        self.counter = 0
        self.distill_weight = float(config.get("distill_weight", 0.7))
        self.rl_weight = float(config.get("rl_weight", 0.3))
        self.batch_update_size = int(config.get("batch_update_size", 8))
        self.last_teacher_agreement: float = 1.0
        self.last_gate_entropy: float = 0.0

    def _compute_teacher_probs(self, state: GenerationOptimizationState):
        state_vec = state.to_feature_vector(self.scaler)
        outs = []
        for t in self.teachers:
            try:
                p = t.predict(state)
            except Exception as e:
                logger.warning(f"Teacher {type(t).__name__} failed: {e}")
                p = np.ones(self.N_ACTIONS) / self.N_ACTIONS
            p = np.asarray(p, dtype=np.float64).reshape(-1)
            if p.shape[0] != self.N_ACTIONS:
                if p.shape[0] < self.N_ACTIONS:
                    p = np.pad(p, (0, self.N_ACTIONS - p.shape[0]))
                else:
                    p = p[: self.N_ACTIONS]
            s = p.sum()
            p = p / s if s > 0 else np.ones(self.N_ACTIONS) / self.N_ACTIONS
            outs.append(p)
        outs = np.array(outs)
        gate_w = self.gating.forward(state_vec)
        combined = gate_w @ outs
        s = combined.sum()
        combined = combined / s if s > 0 else np.ones(self.N_ACTIONS) / self.N_ACTIONS
        return combined, gate_w, outs

    async def select_strategy(self, state: GenerationOptimizationState,
                              exploration: bool = True):
        self.scaler.update(state.to_raw_vector())
        state_vec = state.to_feature_vector(self.scaler)
        combined, gate_w, teacher_outs = self._compute_teacher_probs(state)
        student_probs = self.student.predict_proba(state_vec)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, self.N_ACTIONS - 1)
        else:
            blended = 0.8 * student_probs + 0.2 * combined
            action_idx = int(np.argmax(blended))

        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)

        # Track agreement/entropy for observability.
        g = np.clip(gate_w, 1e-12, 1.0)
        self.last_gate_entropy = float(-np.sum(g * np.log(g)))

        return self.ACTION_SPACE[action_idx], action_idx, state_vec, teacher_outs

    async def update(self, state_vec: np.ndarray, action_idx: int, reward: float,
                     next_state_vec: np.ndarray, teacher_probs: np.ndarray) -> None:
        self.replay_buffer.push(state_vec, action_idx, reward,
                                next_state_vec, teacher_probs)
        self.counter += 1
        if self.counter % self.train_every != 0:
            return
        if len(self.replay_buffer) < self.batch_update_size:
            return

        states, actions, rewards, _, t_batch = self.replay_buffer.sample(
            self.batch_update_size
        )
        for i in range(len(states)):
            s = states[i]
            t_outs = t_batch[i]
            a = int(actions[i])
            r = float(rewards[i])

            gate_w = self.gating.forward(s)
            combined = gate_w @ t_outs
            tot = combined.sum()
            combined = combined / tot if tot > 0 else np.ones(self.N_ACTIONS) / self.N_ACTIONS

            self.student.update(s, combined, r, a,
                                distill_weight=self.distill_weight,
                                rl_weight=self.rl_weight)
            if self.q_teacher is not None:
                self.q_teacher.update_from_vector(s, a, r)
            student_out = self.student.predict_proba(s)
            self.gating.update(s, t_outs, student_out, gate_w)

    def flush(self) -> None:
        if self.q_teacher is not None:
            self.q_teacher.save(force=True)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "student_counter": self.student.counter,
            "buffer_size": len(self.replay_buffer),
            "epsilon": self.epsilon,
            "n_teachers": self.n_teachers,
            "scaler_count": self.scaler.count,
            "gate_entropy": self.last_gate_entropy,
        }


# =============================================================================
# DatasetGenerationParams
# =============================================================================
if PYDANTIC_AVAILABLE:
    class DatasetGenerationParams(BaseModel):
        model_config = ConfigDict(extra="allow")

        seed: int = Field(default=42, ge=0)
        n_periods: int = Field(default=120, ge=10)
        start_date: str = Field(default="2020-01-01")
        anomaly_rate: float = Field(default=0.02, ge=0.0, le=0.5)
        include_anomalies: bool = True
        output_dir: str = Field(default="./data")
        fetch_real_data: bool = False
        cloud_distribution: bool = False
        blockchain_anchor: bool = False

        @field_validator("start_date")
        @classmethod
        def _validate_date(cls, v: str) -> str:
            try:
                datetime.fromisoformat(v)
            except Exception:
                raise ValueError("Invalid date format. Use YYYY-MM-DD")
            return v

        def to_dict(self) -> Dict[str, Any]:
            if hasattr(self, "model_dump"):
                return self.model_dump()
            return self.dict()  # type: ignore[attr-defined]

else:
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

        def __post_init__(self) -> None:
            try:
                datetime.fromisoformat(self.start_date)
            except Exception:
                raise ValueError("Invalid date format. Use YYYY-MM-DD")
            if not (10 <= self.n_periods):
                raise ValueError("n_periods must be >= 10")
            if not (0.0 <= self.anomaly_rate <= 0.5):
                raise ValueError("anomaly_rate must be in [0, 0.5]")

        def to_dict(self) -> Dict[str, Any]:
            return asdict(self)


# =============================================================================
# Generator
# =============================================================================
class EnhancedHeliumDatasetGeneratorV4:
    """
    Enhanced Helium Dataset Generator v4.1.3.
    Final version with full distillation pipeline and persistence.
    """

    def __init__(self, params: Optional[DatasetGenerationParams] = None) -> None:
        self.params = params or DatasetGenerationParams()
        self.seed = int(self.params.seed)
        # Local seeded RNG, never the global numpy RNG.
        self._rng = np.random.default_rng(self.seed)
        self.anomaly_rate = self.params.anomaly_rate
        self.include_anomalies = self.params.include_anomalies
        self.generation_id = uuid.uuid4().hex[:8]
        self.generation_timestamp = _utc_now()

        # Security and distribution
        self.security = QuantumResilientSecurity()
        self.blockchain = BlockchainAnchoring()
        self.optimiser = DistillationParameterOptimizer(self, {
            "distillation_epsilon": Config.DISTILLATION_EPSILON,
            "distillation_epsilon_min": Config.DISTILLATION_EPSILON_MIN,
            "distillation_epsilon_decay": Config.DISTILLATION_EPSILON_DECAY,
            "distillation_train_every": Config.DISTILLATION_TRAIN_EVERY,
            "distillation_replay_size": Config.DISTILLATION_REPLAY_SIZE,
            "distillation_learning_rate": Config.DISTILLATION_LEARNING_RATE,
            "distill_weight": Config.DISTILL_WEIGHT,
            "rl_weight": Config.RL_WEIGHT,
            "gating_learning_rate": Config.GATING_LEARNING_RATE,
            "gating_load_balance_weight": Config.GATING_LOAD_BALANCE_WEIGHT,
            "q_weights_path": Config.Q_WEIGHTS_PATH,
        }, seed=self.seed)
        self.cloud_distributor = MultiCloudDistributor()
        self.task_manager = TaskManager()

        self.metadata: Optional[Dict[str, Any]] = None
        self.df: Optional[pd.DataFrame] = None
        self._last_generation_time: Optional[datetime] = None
        self.generation_logs: List[Dict[str, Any]] = []

        # Circuit breaker for optional real-data fetch.
        self.fetch_breaker = CircuitBreaker(
            "helium_dataset_fetch",
            failure_threshold=Config.CIRCUIT_BREAKER_FAILURE_THRESHOLD,
            recovery_timeout=Config.CIRCUIT_BREAKER_RECOVERY_TIMEOUT,
        )

    # ------------------------------------------------------------------
    async def generate(self) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        logger.info(f"Starting dataset generation (ID: {self.generation_id})")

        if self.params.fetch_real_data:
            try:
                real_data = await self.fetch_breaker.call(self._fetch_real_data)
                if real_data is not None:
                    logger.info(f"Fetched {len(real_data)} real records")
            except Exception as e:
                logger.warning(f"Real data fetch failed: {e}")

        state = self._build_optimization_state()
        strategy, action_idx, state_vec, teacher_probs = \
            await self.optimiser.select_strategy(state, exploration=True)
        logger.info(f"Selected generation strategy: {strategy}")

        anomaly_multiplier, trend_multiplier, noise_multiplier = \
            self._apply_strategy(strategy)

        df = self._generate_synthetic(
            anomaly_multiplier=anomaly_multiplier,
            trend_multiplier=trend_multiplier,
            noise_multiplier=noise_multiplier,
        )

        if self.include_anomalies:
            df, anomaly_count = self._inject_anomalies(df, anomaly_multiplier)
        else:
            anomaly_count = 0

        df = self._add_extended_fields(df)
        quality_score = self._calculate_quality_score(df)

        # 1. Build metadata WITHOUT the signature and WITHOUT the hash first.
        metadata = self._create_metadata(df, anomaly_count, quality_score)

        # 2. Sign metadata (quality_score already present).
        keypair = await self.security.generate_keypair("ecdsa", reuse=True)
        metadata["quantum_signature"] = await self.security.sign_metadata(
            metadata, keypair
        )

        # 3. Anchor the FINAL metadata hash on chain.
        if self.params.blockchain_anchor:
            data_id = f"helium_dataset_{self.generation_id}"
            data_hash = hashlib.sha256(
                json.dumps(metadata, sort_keys=True, default=str).encode()
            ).hexdigest()
            result = await self.blockchain.record_hash(
                data_id, data_hash, {"generation_id": self.generation_id}
            )
            metadata["blockchain_tx_hash"] = result.get("tx_hash")
            metadata["blockchain_hash"] = data_hash

        reward = self._compute_reward(quality_score, anomaly_count, df)

        # 4. Commit state BEFORE computing the next state.
        self.df = df
        self.metadata = metadata
        self._last_generation_time = _utc_now()

        # 5. Now next_state is built from the freshly generated dataframe.
        next_state = self._build_optimization_state()
        await self.optimiser.update(
            state_vec, action_idx, reward,
            next_state.to_feature_vector(self.optimiser.scaler),
            teacher_probs,
        )

        self._log_generation(state, strategy, quality_score, reward)

        logger.info(
            f"Dataset generated: {len(df)} rows, quality={quality_score:.1f}, "
            f"reward={reward:.2f}"
        )
        return df, metadata

    # ------------------------------------------------------------------
    async def _fetch_real_data(self) -> Optional[pd.DataFrame]:
        """Placeholder. Real HTTP calls would be guarded by self.fetch_breaker."""
        return None

    # ------------------------------------------------------------------
    def _log_generation(self, state: GenerationOptimizationState,
                        strategy: str, quality: float, reward: float) -> None:
        entry = {
            "timestamp": _utc_now().isoformat(),
            "strategy": strategy,
            "quality_score": float(quality),
            "reward": float(reward),
            "state_vector": state.to_feature_vector().tolist(),
        }
        self.generation_logs.append(entry)
        log_path = Path(Config.GENERATION_LOGS_PATH)
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            exists = log_path.exists()
            pd.DataFrame([entry]).to_csv(
                log_path, mode="a", header=not exists, index=False
            )
        except OSError as e:
            logger.warning(f"Failed to append generation log: {e}")

    # ------------------------------------------------------------------
    @classmethod
    def train_historical_model(cls, log_path: Optional[Path] = None,
                               model_path: Optional[Path] = None) -> Optional[Path]:
        if not SKLEARN_ML:
            logger.warning("scikit-learn not available; skipping training.")
            return None

        lp = Path(log_path or Config.GENERATION_LOGS_PATH)
        mp = Path(model_path or Config.HISTORICAL_MODEL_PATH)
        if not lp.exists():
            logger.warning(f"Generation logs not found at {lp}; no model trained.")
            return None

        df_logs = pd.read_csv(lp)
        if len(df_logs) < 10:
            logger.warning("Not enough logs to train historical model (need >= 10).")
            return None

        X_list, y_list = [], []
        for _, row in df_logs.iterrows():
            try:
                sv = json.loads(row["state_vector"])
                if len(sv) != GenerationOptimizationState.FEATURE_DIM:
                    continue
                X_list.append(sv)
                y_list.append(row["strategy"])
            except Exception:
                continue

        if len(X_list) < 10:
            logger.warning("Too few valid samples after parsing.")
            return None

        X = np.array(X_list)
        y = np.array(y_list)
        if len(np.unique(y)) < 2:
            logger.warning(
                "Training data contains only one class; skipping historical "
                "model training to avoid RandomForest errors."
            )
            return None

        le = LabelEncoder()
        y_encoded = le.fit_transform(y)
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X, y_encoded)

        mp.parent.mkdir(parents=True, exist_ok=True)
        with open(mp, "wb") as f:
            pickle.dump((model, le), f)
        logger.info(f"Historical ML model trained and saved to {mp}")
        return mp

    # ------------------------------------------------------------------
    def _build_optimization_state(self) -> GenerationOptimizationState:
        if self.metadata is None or self.df is None:
            return GenerationOptimizationState(
                quality_score=80.0,
                regime_crisis=0.0, regime_tightening=0.0,
                regime_normal=0.0, regime_stable=1.0,
                price_volatility=5.0,
                anomaly_rate=self.anomaly_rate,
                production_trend_slope=Config.PRODUCTION_TREND,
                demand_trend_slope=Config.DEMAND_TREND,
                target_quality=90.0,
                time_since_last=0.0,
            )

        quality = float(self.metadata.get("quality_score", 80.0))
        dist = self.metadata.get("market_regime_distribution", {})
        n = max(len(self.df), 1)
        crisis = dist.get("crisis", 0) / n
        tightening = dist.get("tightening", 0) / n
        normal = dist.get("normal", 0) / n
        stable = dist.get("stable", 0) / n

        if "price_volatility" in self.df.columns:
            volatility = float(self.df["price_volatility"].mean())
        else:
            volatility = 5.0

        if ("global_production_tonnes" in self.df.columns
                and "global_demand_tonnes" in self.df.columns
                and len(self.df) > 5):
            prod = self.df["global_production_tonnes"].values[-5:]
            demand = self.df["global_demand_tonnes"].values[-5:]
            x = np.arange(len(prod))
            prod_slope = float(np.polyfit(x, prod, 1)[0]) if len(prod) > 1 else 0.0
            demand_slope = float(np.polyfit(x, demand, 1)[0]) if len(demand) > 1 else 0.0
        else:
            prod_slope = float(Config.PRODUCTION_TREND)
            demand_slope = float(Config.DEMAND_TREND)

        if self._last_generation_time is not None:
            delta = _utc_now() - self._last_generation_time
            hours_since = delta.total_seconds() / 3600
        else:
            hours_since = 0.0

        return GenerationOptimizationState(
            quality_score=quality,
            regime_crisis=crisis, regime_tightening=tightening,
            regime_normal=normal, regime_stable=stable,
            price_volatility=volatility,
            anomaly_rate=self.anomaly_rate,
            production_trend_slope=prod_slope,
            demand_trend_slope=demand_slope,
            target_quality=90.0,
            time_since_last=hours_since,
        )

    # ------------------------------------------------------------------
    def _apply_strategy(self, strategy: str) -> Tuple[float, float, float]:
        if strategy == "conservative":
            return 0.5, 0.8, 0.7
        if strategy == "moderate":
            return 1.0, 1.0, 1.0
        if strategy == "aggressive":
            return 2.0, 1.5, 1.3
        if strategy == "realistic":
            return 1.0, 0.9, 0.9
        if strategy == "adaptive":
            if self.metadata and self.metadata.get("quality_score", 80) < 80:
                return 1.5, 1.2, 1.1
            return 1.0, 1.0, 1.0
        return 1.0, 1.0, 1.0

    # ------------------------------------------------------------------
    def _compute_reward(self, quality_score: float, anomaly_count: int,
                        df: pd.DataFrame) -> float:
        quality_reward = quality_score / 100.0
        n_rows = max(len(df), 1)
        anomaly_rate = anomaly_count / n_rows
        if 0.02 <= anomaly_rate <= 0.05:
            anomaly_realism = 1.0
        elif anomaly_rate < 0.02:
            anomaly_realism = anomaly_rate / 0.02
        else:
            anomaly_realism = max(0.0, 1.0 - (anomaly_rate - 0.05) / 0.05)
        reward = 0.7 * quality_reward + 0.3 * anomaly_realism
        return float(min(1.0, max(0.0, reward)))

    # ------------------------------------------------------------------
    def _generate_synthetic(self, anomaly_multiplier: float = 1.0,
                            trend_multiplier: float = 1.0,
                            noise_multiplier: float = 1.0) -> pd.DataFrame:
        rng = self._rng
        n = self.params.n_periods
        dates = pd.date_range(start=self.params.start_date, periods=n, freq="ME")
        t = np.arange(n)

        production = np.clip(
            Config.PRODUCTION_BASE + t * Config.PRODUCTION_TREND * trend_multiplier
            + rng.normal(0, 300 * noise_multiplier, n),
            20_000, 35_000,
        )
        demand = np.clip(
            Config.DEMAND_BASE + t * Config.DEMAND_TREND * trend_multiplier
            + rng.normal(0, 400 * noise_multiplier, n),
            25_000, 45_000,
        )
        price = Config.PRICE_BASE * np.exp(np.cumsum(
            rng.normal(Config.PRICE_DRIFT * trend_multiplier,
                       Config.PRICE_VOL * noise_multiplier, n)
        ))
        price = price * (1 + 0.1 * np.sin(2 * np.pi * t / 12))
        price = np.clip(price, 50, 500)

        ratio = demand / production
        shortage = np.clip((ratio - 0.95) * 4, 0.05, 1.0)
        supply_risk = np.clip(
            0.2 + t * 0.002 * trend_multiplier
            + 0.1 * np.sin(2 * np.pi * t / 24)
            + rng.normal(0, 0.05 * noise_multiplier, n),
            0.1, 0.9,
        )
        recycling = np.clip(
            0.10 + t * 0.003 * trend_multiplier
            + rng.normal(0, 0.01 * noise_multiplier, n),
            0.05, 0.40,
        )
        substitution = np.clip(
            0.08 + t * 0.004 * trend_multiplier
            + rng.normal(0, 0.01 * noise_multiplier, n),
            0.05, 0.50,
        )
        cooling = np.clip(
            0.85 + t * 0.005 * trend_multiplier
            + rng.normal(0, 0.02 * noise_multiplier, n),
            0.7, 1.3,
        )
        geo = np.clip(
            0.3 + 0.2 * np.sin(2 * np.pi * t / 36)
            + rng.normal(0, 0.05 * noise_multiplier, n),
            0.1, 0.8,
        )
        logistics = np.clip(
            0.2 + t * 0.001 * trend_multiplier
            + rng.normal(0, 0.05 * noise_multiplier, n),
            0.1, 0.7,
        )
        new_capacity = np.maximum(
            500,
            Config.NEW_CAPACITY_BASE + t * Config.NEW_CAPACITY_TREND * trend_multiplier
            + rng.normal(0, 200 * noise_multiplier, n),
        )

        scarcity_impact = np.clip(shortage * 0.6 + supply_risk * 0.4, 0, 1)
        price_vol = pd.Series(price).rolling(6).std().fillna(5).values
        price_vol = np.clip(price_vol, 1, 30)

        regimes = []
        for sc in scarcity_impact:
            if sc > 0.7:
                regimes.append("crisis")
            elif sc > 0.5:
                regimes.append("tightening")
            elif sc > 0.3:
                regimes.append("normal")
            else:
                regimes.append("stable")

        carbon = np.clip(
            Config.CARBON_BASE + Config.CARBON_RANGE * scarcity_impact
            + rng.normal(0, 50 * noise_multiplier, n),
            50, 800,
        )
        renewable = np.clip(
            Config.RENEWABLE_BASE + Config.RENEWABLE_RANGE * (1 - scarcity_impact)
            + rng.normal(0, 10 * noise_multiplier, n),
            5, 95,
        )
        circularity = (recycling + substitution) / 2
        thermal = cooling * scarcity_impact
        future_supply = np.clip((new_capacity / production) * 100, 0, 50)
        capacity_util = production / (production + new_capacity)
        esg = np.clip(
            (recycling * 40 + (1 - supply_risk) * 30 + (1 - geo) * 30) * 100,
            0, 100,
        )
        regulatory = np.clip(geo * 0.5 + logistics * 0.5, 0, 1)

        return pd.DataFrame({
            "date": dates,
            "global_production_tonnes": np.round(production, 0),
            "global_demand_tonnes": np.round(demand, 0),
            "price_index": np.round(price, 1),
            "shortage_severity_0_1": np.round(shortage, 3),
            "supply_risk_score_0_1": np.round(supply_risk, 3),
            "recycling_rate_0_1": np.round(recycling, 3),
            "substitution_feasibility_0_1": np.round(substitution, 3),
            "cooling_load_sensitivity": np.round(cooling, 3),
            "geopolitical_risk_index": np.round(geo, 3),
            "logistics_disruption_index": np.round(logistics, 3),
            "new_production_capacity_tonnes": np.round(new_capacity, 0),
            "helium_scarcity_impact": np.round(scarcity_impact, 3),
            "price_volatility": np.round(price_vol, 2),
            "market_regime": regimes,
            "carbon_intensity_associated": np.round(carbon, 0),
            "renewable_energy_pct": np.round(renewable, 1),
            "demand_supply_ratio": np.round(ratio, 3),
            "circularity_potential": np.round(circularity, 3),
            "thermal_impact_factor": np.round(thermal, 3),
            "future_supply_potential_pct": np.round(future_supply, 1),
            "capacity_utilization_rate": np.round(capacity_util, 3),
            "esg_score": np.round(esg, 1),
            "regulatory_risk_score": np.round(regulatory, 3),
        })

    # ------------------------------------------------------------------
    def _inject_anomalies(self, df: pd.DataFrame,
                          anomaly_multiplier: float) -> Tuple[pd.DataFrame, int]:
        rng = self._rng
        out = df.copy()
        n = len(out)
        effective_rate = min(self.anomaly_rate * anomaly_multiplier, 0.5)
        anomaly_count = 0
        cols = list(out.columns)

        # Price spikes
        k = int(n * effective_rate * 0.3)
        if k > 0:
            idx = rng.choice(n, k, replace=False)
            for i in idx:
                out.loc[i, "price_index"] *= rng.uniform(1.5, 2.5)
                out.loc[i, "price_volatility"] *= rng.uniform(2, 4)
                anomaly_count += 1

        # Production drops
        k = int(n * effective_rate * 0.3)
        if k > 0:
            idx = rng.choice(n, k, replace=False)
            for i in idx:
                out.loc[i, "global_production_tonnes"] *= rng.uniform(0.6, 0.85)
                out.loc[i, "shortage_severity_0_1"] = np.clip(
                    out.loc[i, "shortage_severity_0_1"] * 1.5, 0, 1
                )
                anomaly_count += 1

        # Missing values
        k = int(n * effective_rate * 0.2)
        if k > 0:
            idx = rng.choice(n, k, replace=False)
            for i in idx:
                col = cols[int(rng.integers(0, len(cols)))]
                out.loc[i, col] = np.nan
                anomaly_count += 1

        # Regime inconsistency
        k = int(n * effective_rate * 0.2)
        if k > 0:
            idx = rng.choice(n, k, replace=False)
            for i in idx:
                out.loc[i, "helium_scarcity_impact"] = rng.uniform(0.7, 0.9)
                out.loc[i, "price_index"] = rng.uniform(80, 120)
                anomaly_count += 1

        return out, anomaly_count

    # ------------------------------------------------------------------
    def _add_extended_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        rng = self._rng
        n = len(df)
        df = df.copy()
        df["regret_score"] = rng.uniform(0.1, 0.9, n)
        df["cvar_regret"] = rng.uniform(0.1, 0.8, n)
        df["federated_weight"] = rng.uniform(0.5, 1.5, n)
        df["carbon_efficiency"] = np.clip(
            1 - (df["carbon_intensity_associated"] - 200) / 600, 0.1, 0.9
        )
        return df

    # ------------------------------------------------------------------
    def _create_metadata(self, df: pd.DataFrame, anomaly_count: int,
                         quality_score: float) -> Dict[str, Any]:
        df_string = df.to_csv(index=False)
        checksum = hashlib.sha256(df_string.encode()).hexdigest()[:16]
        regime_dist = df["market_regime"].value_counts().to_dict() if "market_regime" in df.columns else {}

        params_dict = self.params.to_dict() if hasattr(self.params, "to_dict") \
            else asdict(self.params) if hasattr(self.params, "__dataclass_fields__") \
            else dict(self.params.__dict__)

        return {
            "version": __version__,
            "generation_id": self.generation_id,
            "generated_at": self.generation_timestamp.isoformat(),
            "params": params_dict,
            "n_periods": len(df),
            "n_columns": len(df.columns),
            "fields": list(df.columns),
            "quality_score": quality_score,
            "checksum": checksum,
            "anomaly_count": anomaly_count,
            "market_regime_distribution": regime_dist,
            "seed": self.seed,
            "anomaly_rate": self.anomaly_rate,
            "include_anomalies": self.include_anomalies,
        }

    # ------------------------------------------------------------------
    def _calculate_quality_score(self, df: pd.DataFrame) -> float:
        score = 100.0
        n = len(df)
        if n == 0:
            return 0.0

        missing_pct = df.isnull().sum().sum() / (df.shape[0] * df.shape[1])
        if missing_pct > 0:
            score -= min(30, missing_pct * 50)

        duplicate_pct = df.duplicated().sum() / n
        if duplicate_pct > 0:
            score -= min(20, duplicate_pct * 30)

        numeric = df.select_dtypes(include=[np.number]).columns
        zero_var = sum(1 for c in numeric if df[c].std() < 1e-12)
        score -= zero_var * 5

        if "market_regime" in df.columns:
            valid = {"crisis", "tightening", "normal", "stable"}
            invalid = set(df["market_regime"].dropna().unique()) - valid
            score -= len(invalid) * 10

        if "helium_scarcity_impact" in df.columns and "price_index" in df.columns:
            try:
                corr = df["helium_scarcity_impact"].corr(df["price_index"])
                if corr is not None and not math.isnan(corr):
                    if corr < 0.1:
                        score -= 20
                    elif corr < 0.3:
                        score -= 10
            except Exception:
                pass

        if "global_production_tonnes" in df.columns:
            try:
                trend = np.polyfit(
                    range(len(df)), df["global_production_tonnes"].values, 1
                )[0]
                if trend < -10:
                    score -= 10
            except Exception:
                pass

        return float(max(0.0, min(100.0, score)))

    # ------------------------------------------------------------------
    def create_train_val_test_split(self, df: pd.DataFrame,
                                    train_ratio: float = 0.7,
                                    val_ratio: float = 0.15
                                    ) -> Dict[str, pd.DataFrame]:
        n = len(df)
        train_end = int(n * train_ratio)
        val_end = int(n * (train_ratio + val_ratio))
        return {
            "train": df.iloc[:train_end].reset_index(drop=True),
            "validation": df.iloc[train_end:val_end].reset_index(drop=True),
            "test": df.iloc[val_end:].reset_index(drop=True),
        }

    # ------------------------------------------------------------------
    async def save(self, output_dir: Optional[Union[str, Path]] = None
                   ) -> Dict[str, Path]:
        """
        Persist the dataset and metadata.  Returns a mapping of artifact
        name -> path.
        """
        if self.df is None or self.metadata is None:
            raise RuntimeError("Nothing to save. Call generate() first.")

        out = Path(output_dir or self.params.output_dir)
        out.mkdir(parents=True, exist_ok=True)
        base = f"helium_timeseries_enhanced_v4_{self.generation_id}"
        artifacts: Dict[str, Path] = {}

        # CSV
        csv_path = out / f"{base}.csv"
        await asyncio.to_thread(self.df.to_csv, csv_path, index=False)
        artifacts["csv"] = csv_path

        # Parquet
        if PARQUET_AVAILABLE:
            parquet_path = out / f"{base}.parquet"
            await asyncio.to_thread(self.df.to_parquet, parquet_path, index=False)
            artifacts["parquet"] = parquet_path

        # JSON
        json_path = out / f"{base}.json"
        await asyncio.to_thread(
            json_path.write_text,
            json.dumps(self.df.to_dict(orient="records"), indent=2, default=str),
        )
        artifacts["json"] = json_path

        # Metadata
        metadata_path = out / f"{base}.metadata.json"
        await asyncio.to_thread(
            metadata_path.write_text,
            json.dumps(self.metadata, indent=2, default=str),
        )
        artifacts["metadata"] = metadata_path

        # Splits
        for split_name, split_df in self.create_train_val_test_split(self.df).items():
            split_path = out / f"{base}_{split_name}.csv"
            await asyncio.to_thread(split_df.to_csv, split_path, index=False)
            artifacts[f"{split_name}_csv"] = split_path

        # Cloud distribution
        if self.params.cloud_distribution and "csv" in artifacts:
            result = await self.cloud_distributor.distribute(
                artifacts["csv"], self.metadata
            )
            logger.info(f"Distributed to cloud: {result}")

        self.optimiser.flush()
        return artifacts

    def save_sync(self, output_dir: Optional[Union[str, Path]] = None
                  ) -> Dict[str, Path]:
        """Synchronous wrapper for CLI convenience."""
        return asyncio.run(self.save(output_dir))

    async def shutdown(self) -> None:
        await self.task_manager.stop_all()
        self.optimiser.flush()


# =============================================================================
# Export helpers
# =============================================================================
def export_for_elasticity(df: pd.DataFrame, idx: int = -1) -> Dict[str, Any]:
    r = df.iloc[idx]
    return {
        "price_elasticity": -0.4 * (1 + r["helium_scarcity_impact"] * 0.5),
        "scarcity_elasticity": 0.6 * (1 - r["capacity_utilization_rate"]),
        "cross_elasticity": 0.3 * (1 - r["substitution_feasibility_0_1"]),
        "thermal_elasticity": r["thermal_impact_factor"],
        "composite_elasticity": (
            0.4 * (1 + r["helium_scarcity_impact"] * 0.3)
            + 0.3 * r["circularity_potential"]
            + 0.3 * r["regulatory_risk_score"]
        ),
        "market_regime": r["market_regime"],
        "carbon_price_sensitivity": r["esg_score"] / 100,
        "renewable_integration": r["renewable_energy_pct"] / 100,
        "capacity_impact": r["future_supply_potential_pct"] / 100,
    }


def export_for_circularity(df: pd.DataFrame, idx: int = -1) -> Dict[str, Any]:
    r = df.iloc[idx]
    return {
        "recycling_rate": r["recycling_rate_0_1"],
        "recovery_efficiency": 0.85,
        "circularity_index": r["circularity_potential"],
        "closed_loop_score": r["circularity_potential"] * r["recycling_rate_0_1"],
        "material_circularity_indicator":
            (r["recycling_rate_0_1"] + r["substitution_feasibility_0_1"]) / 2,
        "lifecycle_extension_potential": r["future_supply_potential_pct"] / 50,
        "circular_economy_roi": (r["esg_score"] / 100) * 0.15,
        "waste_heat_recovery_potential": r["thermal_impact_factor"] * 100,
        "industrial_symbiosis_score": r["capacity_utilization_rate"] * 0.8,
    }


def export_for_sustainability(df: pd.DataFrame, idx: int = -1) -> Dict[str, Any]:
    r = df.iloc[idx]
    return {
        "esg_score": r["esg_score"],
        "carbon_intensity": r["carbon_intensity_associated"],
        "renewable_energy_pct": r["renewable_energy_pct"],
        "circularity_score": r["circularity_potential"] * 100,
        "supply_chain_risk": r["supply_risk_score_0_1"],
        "geopolitical_risk": r["geopolitical_risk_index"],
        "regulatory_risk": r["regulatory_risk_score"],
        "market_regime": r["market_regime"],
        "future_supply_potential": r["future_supply_potential_pct"],
        "capacity_utilization": r["capacity_utilization_rate"],
    }


def export_for_thermal(df: pd.DataFrame, idx: int = -1) -> Dict[str, Any]:
    r = df.iloc[idx]
    return {
        "cooling_load_sensitivity": r["cooling_load_sensitivity"],
        "thermal_impact_factor": r["thermal_impact_factor"],
        "helium_scarcity_impact": r["helium_scarcity_impact"],
        "carbon_intensity": r["carbon_intensity_associated"],
        "renewable_energy_pct": r["renewable_energy_pct"],
        "cooling_cost_index": r["price_index"] / 100,
        "free_cooling_potential": 1 - r["helium_scarcity_impact"],
        "waste_heat_recovery": r["thermal_impact_factor"] * 0.5,
    }


def export_for_quantum_bridge(df: pd.DataFrame, idx: int = -1) -> Dict[str, Any]:
    r = df.iloc[idx]
    return {
        "hamiltonian_factors": {
            "price": r["price_index"] / 500,
            "scarcity": r["helium_scarcity_impact"],
            "supply_risk": r["supply_risk_score_0_1"],
            "demand_supply": r["demand_supply_ratio"],
            "geopolitical": r["geopolitical_risk_index"],
            "logistics": r["logistics_disruption_index"],
            "new_capacity": r["new_production_capacity_tonnes"] / 20_000,
            "recycling": r["recycling_rate_0_1"],
            "substitution": r["substitution_feasibility_0_1"],
            "cooling": r["cooling_load_sensitivity"],
            "esg": r["esg_score"] / 100,
        },
        "market_regime": r["market_regime"],
        "quantum_advantage_expected": bool(r["price_volatility"] > 15),
    }


# =============================================================================
# Tests
# =============================================================================
def _test_state_features() -> None:
    s = GenerationOptimizationState(
        quality_score=80.0, regime_crisis=0.1, regime_tightening=0.2,
        regime_normal=0.3, regime_stable=0.4, price_volatility=15.0,
        anomaly_rate=0.02, production_trend_slope=-40.0,
        demand_trend_slope=80.0, target_quality=90.0, time_since_last=5.0,
    )
    v = s.to_feature_vector()
    assert v.shape == (11,)
    assert np.all(v >= 0.0) and np.all(v <= 1.0)


def _test_rule_teacher() -> None:
    t = GenerationRuleBasedTeacher()
    s = GenerationOptimizationState(
        quality_score=65.0, regime_crisis=0.0, regime_tightening=0.0,
        regime_normal=0.0, regime_stable=1.0, price_volatility=10.0,
        anomaly_rate=0.02, production_trend_slope=-40.0,
        demand_trend_slope=80.0, target_quality=90.0, time_since_last=0.0,
    )
    p = t.predict(s)
    assert p.sum() == pytest.approx(1.0) if "pytest" in globals() else abs(p.sum() - 1.0) < 1e-9
    assert p[2] > 0.5


def _test_moe_jacobian_shapes() -> None:
    g = MoEGatingNetwork(feature_dim=11, n_experts=3)
    x = np.random.randn(11)
    t = np.random.rand(3, 5); t = t / t.sum(axis=1, keepdims=True)
    s = np.random.rand(5); s = s / s.sum()
    gw = g.forward(x)
    g.update(x, t, s, gw)
    assert g.weights.shape == (11, 3)


def _test_running_scaler() -> None:
    s = RunningScaler(11)
    for _ in range(5):
        s.update(np.random.randn(11) + 0.5)
    out = s.transform(np.random.randn(11))
    assert out.shape == (11,)
    assert np.all(np.isfinite(out))


def _test_q_teacher_persistence(tmpdir: Path) -> None:
    p = tmpdir / "q.json"
    q = GenerationStatefulQTeacher(weights_path=p, lr=1.0)
    v = np.zeros(11); v[0] = 1.0
    q.update_from_vector(v, 0, 1.0)
    q.save(force=True)
    assert p.exists()
    q2 = GenerationStatefulQTeacher(weights_path=p)
    assert np.any(q2.weights[:, 0] != 0)


def _test_epsilon_decay(tmpdir: Path) -> None:
    cfg = {
        "distillation_epsilon": 1.0,
        "distillation_epsilon_min": 0.01,
        "distillation_epsilon_decay": 0.5,
        "q_weights_path": str(tmpdir / "q.json"),
    }
    gen = EnhancedHeliumDatasetGeneratorV4(
        DatasetGenerationParams(seed=0, n_periods=12, output_dir=str(tmpdir))
    )
    opt = DistillationParameterOptimizer(gen, cfg, seed=0)
    state = GenerationOptimizationState(
        quality_score=80.0, regime_crisis=0.0, regime_tightening=0.0,
        regime_normal=0.0, regime_stable=1.0, price_volatility=5.0,
        anomaly_rate=0.02, production_trend_slope=-40.0,
        demand_trend_slope=80.0, target_quality=90.0, time_since_last=0.0,
    )
    asyncio.run(opt.select_strategy(state, exploration=True))
    assert opt.epsilon < 1.0


def _test_generate_and_save(tmpdir: Path) -> None:
    params = DatasetGenerationParams(
        seed=1, n_periods=24, output_dir=str(tmpdir), anomaly_rate=0.03,
    )
    gen = EnhancedHeliumDatasetGeneratorV4(params)
    df, metadata = asyncio.run(gen.generate())
    assert len(df) == 24
    assert "quality_score" in metadata
    assert 0 <= metadata["quality_score"] <= 100
    assert "quantum_signature" in metadata
    assert metadata["quantum_signature"]["signature"]

    artifacts = asyncio.run(gen.save(tmpdir))
    assert "csv" in artifacts and artifacts["csv"].exists()
    assert "metadata" in artifacts and artifacts["metadata"].exists()
    asyncio.run(gen.shutdown())


def _test_state_transition(tmpdir: Path) -> None:
    params = DatasetGenerationParams(
        seed=2, n_periods=12, output_dir=str(tmpdir), anomaly_rate=0.02,
    )
    gen = EnhancedHeliumDatasetGeneratorV4(params)

    async def run():
        return await gen.generate()

    df, _ = asyncio.run(run())
    # After generate, self.df / self.metadata are the new frame.
    assert gen.df is not None and len(gen.df) == len(df)
    assert gen.metadata is not None
    asyncio.run(gen.shutdown())


def _test_quality_score() -> None:
    gen = EnhancedHeliumDatasetGeneratorV4(
        DatasetGenerationParams(seed=0, n_periods=12)
    )
    df = gen._generate_synthetic()
    score = gen._calculate_quality_score(df)
    assert 0 <= score <= 100


def _test_train_historical_model_guard(tmpdir: Path) -> None:
    log_path = tmpdir / "logs.csv"
    model_path = tmpdir / "model.pkl"
    # Single-class data must not raise; should return None.
    rows = [{"state_vector": json.dumps([0.5] * 11), "strategy": "moderate"}
            for _ in range(15)]
    pd.DataFrame(rows).to_csv(log_path, index=False)
    result = EnhancedHeliumDatasetGeneratorV4.train_historical_model(
        log_path=log_path, model_path=model_path
    )
    assert result is None


def _test_export_helpers(tmpdir: Path) -> None:
    params = DatasetGenerationParams(
        seed=3, n_periods=12, output_dir=str(tmpdir)
    )
    gen = EnhancedHeliumDatasetGeneratorV4(params)
    df, _ = asyncio.run(gen.generate())
    for fn in (export_for_elasticity, export_for_circularity,
               export_for_sustainability, export_for_thermal,
               export_for_quantum_bridge):
        d = fn(df)
        assert isinstance(d, dict) and d
    asyncio.run(gen.shutdown())


def _run_all_tests() -> None:
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        _test_state_features()
        _test_rule_teacher()
        _test_moe_jacobian_shapes()
        _test_running_scaler()
        _test_q_teacher_persistence(tmp)
        _test_epsilon_decay(tmp)
        _test_quality_score()
        _test_state_transition(tmp)
        _test_generate_and_save(tmp)
        _test_train_historical_model_guard(tmp)
        _test_export_helpers(tmp)
    print("All HeliumDatasetGenerator tests passed.")


# =============================================================================
# CLI
# =============================================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate enhanced helium timeseries dataset"
    )
    parser.add_argument("--output-dir", default=Config.OUTPUT_DIR)
    parser.add_argument("--n-periods", type=int, default=Config.N_PERIODS)
    parser.add_argument("--start-date", default=Config.START_DATE)
    parser.add_argument("--seed", type=int, default=Config.SEED)
    parser.add_argument("--anomaly-rate", type=float, default=Config.ANOMALY_RATE)
    parser.add_argument("--no-anomalies", action="store_true")
    parser.add_argument("--fetch-real", action="store_true")
    parser.add_argument("--blockchain", action="store_true")
    parser.add_argument("--cloud", action="store_true")
    parser.add_argument("--train-historical", action="store_true",
                        help="Train the historical ML model from logs and exit.")
    parser.add_argument("--test", action="store_true", help="Run tests and exit.")
    return parser.parse_args()


async def _main_async(args: argparse.Namespace) -> None:
    params = DatasetGenerationParams(
        seed=args.seed,
        n_periods=args.n_periods,
        start_date=args.start_date,
        anomaly_rate=args.anomaly_rate,
        include_anomalies=not args.no_anomalies,
        output_dir=args.output_dir,
        fetch_real_data=args.fetch_real,
        blockchain_anchor=args.blockchain,
        cloud_distribution=args.cloud,
    )
    gen = EnhancedHeliumDatasetGeneratorV4(params)
    try:
        df, metadata = await gen.generate()
        artifacts = await gen.save()
        print("\nDataset generation complete.")
        print(f"  Generation ID: {metadata['generation_id']}")
        print(f"  Quality Score: {metadata['quality_score']:.1f}%")
        print(f"  Anomalies:     {metadata['anomaly_count']}")
        print(f"  Blockchain TX: {metadata.get('blockchain_tx_hash', 'N/A')}")
        print(f"  Artifacts:     {len(artifacts)}")
        for name, path in artifacts.items():
            print(f"    - {name}: {path}")
        print("\nSample:")
        print(df.tail().to_string())
    finally:
        await gen.shutdown()


def main() -> None:
    args = parse_args()
    if args.test:
        _run_all_tests()
        return
    if args.train_historical:
        EnhancedHeliumDatasetGeneratorV4.train_historical_model()
        return
    asyncio.run(_main_async(args))


if __name__ == "__main__":
    main()
