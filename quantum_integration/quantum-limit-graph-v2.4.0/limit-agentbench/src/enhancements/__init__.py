"""
Green Agent Core Enhancements & Scientific Integration Gateway (v3.1.0)
=======================================================================
Upgraded with:
- Multi-Teacher On-Policy Distillation (MTPD) optimizer (PyTorch)
- Secure master key retrieval from HashiCorp Vault (no temp file)
- Tenacity retries, enhanced circuit breakers, multi-cloud fallback chains
- Prometheus metrics and structured logging with correlation IDs
"""
import asyncio
import gc
import hashlib
import json
import logging
import os
import random
import secrets
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# ---------- External dependencies (install with pip) ----------
# pip install structlog cryptography web3 boto3 azure-storage-blob google-cloud-storage tenacity pydantic hvac torch prometheus-client
try:
    import structlog
    from structlog.processors import JSONRenderer, TimeStamper
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False

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
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

# ---------- Cryptography ----------
try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.primitives.asymmetric import ec
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False
    raise ImportError("cryptography is required. Install with: pip install cryptography")

# ---------- Post-Quantum Cryptography ----------
try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

# ---------- Web3 Blockchain ----------
try:
    from web3 import Web3, Account, HTTPProvider
    from web3.middleware import geth_poa_middleware, gas_price_strategy
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

# ---------- Cloud SDKs ----------
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

# ---------- Retry ----------
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False
    # fallback no-op decorator
    def retry(*args, **kwargs):
        return lambda f: f

# ---------- Pydantic ----------
try:
    from pydantic import BaseSettings, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False
    raise ImportError("pydantic is required. Install with: pip install pydantic")

# ---------- Vault ----------
try:
    import hvac
    VAULT_AVAILABLE = True
except ImportError:
    VAULT_AVAILABLE = False

# ---------- PyTorch (for MTPD) ----------
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import DataLoader, TensorDataset
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    raise ImportError("PyTorch is required for MTPD optimizer. Install with: pip install torch")

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- Domain Engines (optional) ----------
try:
    from .thermal_optimizer import ThermalAwareOptimizer, ThermalDecision
    from .phase_energy_model import PhaseAwareEnergyModel, PhaseEnergyProfile
    from .energy_scaler import EnergyProportionalScaler, ScaledModel, ScalingDecision
    from .marginal_carbon import MarginalCarbonIntensityForecaster, MarginalCarbonForecast
    from .dual_accountant import DualCarbonAccountant, CarbonAccounting
    from .carbon_nas import CarbonAwareNAS, ArchitectureConfig, ArchitectureMetrics
    from .helium_elasticity import HeliumPriceElasticityModel, ElasticityDecision, WorkloadPriority
    from .material_substitution import MaterialSubstitutionEngine, SubstitutionDecision
    from .helium_circularity import HeliumCircularityTracker, CircularityMetrics
    from .regret_optimizer import RegretMinimizationOptimizer, RegretDecision
    from .federated_learning import FederatedGreenLearning, FederatedPolicy
    DOMAIN_ENGINES_AVAILABLE = True
except ImportError as err:
    DOMAIN_ENGINES_AVAILABLE = False
    logger.warning("Domain engine imports incomplete: %s. Proceeding with stub implementations.", err)

# ============================================================================
# 1. CONFIGURATION WITH PYDANTIC
# ============================================================================

class Config(BaseSettings):
    """Centralised configuration with strict validation and environment fallback."""
    DB_PATH: str = Field("green_agent_enhancements.db", env="GREEN_AGENT_DB_PATH")
    MASTER_KEY_ENV: str = Field("ENHANCEMENTS_MASTER_KEY", env="MASTER_KEY_ENV_VAR_NAME")
    DEFAULT_CHAIN_ID: int = Field(1, env="DEFAULT_CHAIN_ID")
    RPC_URL: Optional[str] = Field(None, env="ETHEREUM_RPC_URL")
    GAS_MULTIPLIER: float = Field(1.2, env="GAS_MULTIPLIER")
    CLOUD_REGION: str = Field("us-east-1", env="DEFAULT_CLOUD_REGION")
    AUTO_PERSIST: bool = Field(True, env="ENABLE_AUTO_PERSISTENCE")
    CIRCUIT_BREAKER_FAILURE_THRESHOLD: int = Field(5, env="CIRCUIT_BREAKER_FAILURE_THRESHOLD")
    CIRCUIT_BREAKER_RECOVERY_TIMEOUT: int = Field(60, env="CIRCUIT_BREAKER_RECOVERY_TIMEOUT")
    KEY_ROTATION_DAYS: int = Field(30, env="KEY_ROTATION_DAYS")
    LOG_LEVEL: str = Field("INFO", env="LOG_LEVEL")
    PROMETHEUS_PORT: Optional[int] = Field(None, env="PROMETHEUS_PORT")
    
    # Vault settings
    VAULT_ADDR: Optional[str] = Field(None, env="VAULT_ADDR")
    VAULT_TOKEN: Optional[str] = Field(None, env="VAULT_TOKEN")
    VAULT_SECRET_PATH: str = Field("green_agent/master_key", env="VAULT_SECRET_PATH")
    VAULT_USE_KV_V2: bool = Field(True, env="VAULT_USE_KV_V2")

    # MTPD settings
    MTPD_STATE_DIM: int = Field(8, env="MTPD_STATE_DIM")
    MTPD_ACTION_DIM: int = Field(5, env="MTPD_ACTION_DIM")
    MTPD_HIDDEN_SIZE: int = Field(128, env="MTPD_HIDDEN_SIZE")
    MTPD_LR: float = Field(1e-3, env="MTPD_LR")
    MTPD_BETA: float = Field(0.5, env="MTPD_BETA")  # distillation weight
    MTPD_GAMMA: float = Field(0.99, env="MTPD_GAMMA")
    MTPD_BUFFER_SIZE: int = Field(10000, env="MTPD_BUFFER_SIZE")
    MTPD_TRAIN_INTERVAL: int = Field(10, env="MTPD_TRAIN_INTERVAL")
    MTPD_BATCH_SIZE: int = Field(32, env="MTPD_BATCH_SIZE")

    @validator("GAS_MULTIPLIER")
    def validate_gas_multiplier(cls, v):
        if v < 1.0:
            raise ValueError("GAS_MULTIPLIER must be >= 1.0")
        return v

    @validator("KEY_ROTATION_DAYS")
    def validate_key_rotation(cls, v):
        if v < 1:
            raise ValueError("KEY_ROTATION_DAYS must be >= 1")
        return v

    class Config:
        env_file = ".env"
        case_sensitive = True


config = Config()
logging.getLogger().setLevel(config.LOG_LEVEL.upper())

# ============================================================================
# 2. ENHANCED CIRCUIT BREAKER (with timeout)
# ============================================================================

class EnhancedCircuitBreaker:
    """Circuit breaker with half‑open state and timeout support."""
    def __init__(self, name: str, failure_threshold: int = config.CIRCUIT_BREAKER_FAILURE_THRESHOLD,
                 recovery_timeout: float = config.CIRCUIT_BREAKER_RECOVERY_TIMEOUT,
                 timeout_seconds: float = 10.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.timeout = timeout_seconds
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
            result = await asyncio.wait_for(func(*args, **kwargs), timeout=self.timeout)
            if self._state == "HALF_OPEN":
                self._state = "CLOSED"
                self._failures = 0
            return result
        except (asyncio.TimeoutError, Exception) as e:
            self._failures += 1
            self._last_failure_time = datetime.now()
            if self._failures >= self.failure_threshold:
                self._state = "OPEN"
            raise e

    def get_state(self) -> str:
        return self._state

    def set_timeout(self, seconds: float):
        self.timeout = seconds

# ============================================================================
# 3. PERSISTENT SQLITE STORAGE (unchanged, but we add a method for model saving)
# ============================================================================

class Storage:
    """Persistent SQLite storage with WAL, indexes, and connection pooling."""
    # ... (keep existing implementation as is, but add method to store/load model weights)
    def save_model_weights(self, model_id: str, weights_bytes: bytes):
        """Store serialized model weights in a dedicated table."""
        with self._get_connection() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS model_weights ("
                "model_id TEXT PRIMARY KEY, weights BLOB, timestamp REAL)"
            )
            conn.execute(
                "INSERT OR REPLACE INTO model_weights VALUES (?, ?, ?)",
                (model_id, weights_bytes, time.time())
            )
            conn.commit()

    def load_model_weights(self, model_id: str) -> Optional[bytes]:
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT weights FROM model_weights WHERE model_id = ?", (model_id,)
            ).fetchone()
            return row[0] if row else None

    # The rest of the methods (store_encrypted_key, etc.) remain unchanged.
    # For brevity, we don't duplicate them here; they are as in the original.

# ============================================================================
# 4. QUANTUM-RESILIENT SECURITY WITH VAULT INTEGRATION
# ============================================================================

class QuantumResilientEnhancementsSecurity:
    """Post-Quantum Cryptographic key generation, signing, and AES-256-GCM key storage.
    Master key is retrieved from HashiCorp Vault or environment variable (no temp file)."""

    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage or Storage()
        self.master_key = self._get_master_key()
        self._pqc_algorithms = {}
        if PQC_AVAILABLE:
            self._initialize_pqc()
        else:
            logger.warning("PQC libraries not found. Using ECDSA fallback.")

    def _get_master_key(self) -> bytes:
        """Retrieve master key from Vault, then environment, otherwise raise error."""
        # 1. Try Vault
        if VAULT_AVAILABLE and config.VAULT_ADDR:
            try:
                client = hvac.Client(
                    url=config.VAULT_ADDR,
                    token=config.VAULT_TOKEN
                )
                if client.is_authenticated():
                    if config.VAULT_USE_KV_V2:
                        secret = client.secrets.kv.v2.read_secret_version(
                            path=config.VAULT_SECRET_PATH
                        )
                        key_hex = secret['data']['data']['key']
                    else:
                        secret = client.read(config.VAULT_SECRET_PATH)
                        key_hex = secret['data']['key']
                    return bytes.fromhex(key_hex)
                else:
                    logger.warning("Vault authentication failed, falling back to environment.")
            except Exception as e:
                logger.warning(f"Vault retrieval failed: {e}, falling back to environment.")

        # 2. Environment variable (mandatory, no temp file)
        key_hex = os.getenv(config.MASTER_KEY_ENV) or os.getenv("ENHANCEMENTS_MASTER_KEY")
        if not key_hex:
            raise RuntimeError(
                f"Master key not found. Please set {config.MASTER_KEY_ENV} or configure Vault."
            )
        try:
            key_bytes = bytes.fromhex(key_hex)
            if len(key_bytes) != 32:
                logger.warning("Master key length is not 32 bytes; hashing it.")
                return hashlib.sha256(key_bytes).digest()
            return key_bytes
        except ValueError:
            logger.warning("Master key is not a valid hex string; hashing it.")
            return hashlib.sha256(key_hex.encode()).digest()

    def _initialize_pqc(self):
        self._pqc_algorithms['dilithium'] = dilithium
        self._pqc_algorithms['falcon'] = falcon
        self._pqc_algorithms['sphincs'] = sphincs

    def _encrypt_bytes(self, data: bytes) -> Tuple[bytes, bytes]:
        aesgcm = AESGCM(self.master_key)
        nonce = secrets.token_bytes(12)
        return aesgcm.encrypt(nonce, data, None), nonce

    def _decrypt_bytes(self, ciphertext: bytes, nonce: bytes) -> bytes:
        aesgcm = AESGCM(self.master_key)
        return aesgcm.decrypt(nonce, ciphertext, None)

    def generate_keypair(self, algorithm: str = "dilithium", key_id: Optional[str] = None) -> Dict[str, Any]:
        key_id = key_id or f"key_{secrets.token_hex(8)}"

        if PQC_AVAILABLE and algorithm in self._pqc_algorithms:
            algo_obj = self._pqc_algorithms[algorithm]
            pk, sk = algo_obj.generate_keypair()
            algo_used = f"PQC-{algorithm.capitalize()}"
        else:
            # ECDSA fallback
            private_key = ec.generate_private_key(ec.SECP256R1())
            sk = private_key.private_bytes(
                ec.Encoding.DER, ec.PrivateFormat.PKCS8, ec.NoEncryption()
            )
            pk = private_key.public_key().public_bytes(
                ec.Encoding.DER, ec.PublicFormat.SubjectPublicKeyInfo
            )
            algo_used = "ECDSA-SECP256R1"

        ciphertext, nonce = self._encrypt_bytes(sk)
        self.storage.store_encrypted_key(key_id, algo_used, ciphertext, nonce)

        logger.info("Generated keypair %s with %s", key_id, algo_used)
        return {"key_id": key_id, "algorithm": algo_used, "public_key_hex": pk.hex(), "status": "stored_and_encrypted"}

    def sign_message(self, key_id: str, message: bytes) -> Dict[str, Any]:
        record = self.storage.get_encrypted_key(key_id)
        if not record:
            raise ValueError(f"Key ID '{key_id}' not found.")

        sk = self._decrypt_bytes(record["ciphertext"], record["nonce"])
        algo = record["algorithm"]

        if PQC_AVAILABLE and algo.startswith("PQC-"):
            algo_name = algo.split("-")[1].lower()
            if algo_name in self._pqc_algorithms:
                signature = self._pqc_algorithms[algo_name].sign(sk, message)
            else:
                raise ValueError(f"Unknown PQC algorithm: {algo}")
        else:
            # ECDSA or fallback
            if CRYPTO_AVAILABLE:
                private_key = ec.load_der_private_key(sk, password=None)
                signature = private_key.sign(message, ec.ECDSA(hashes.SHA256()))
            else:
                signature = hashlib.sha256(sk + message).digest()

        return {
            "key_id": key_id,
            "algorithm": algo,
            "signature_hex": signature.hex() if isinstance(signature, bytes) else signature,
            "timestamp": time.time()
        }

    def rotate_keys(self, force: bool = False) -> List[Dict]:
        """Rotate keys that are near expiry (older than KEY_ROTATION_DAYS)."""
        rotated = []
        for key_id in self.storage.list_key_ids():
            record = self.storage.get_encrypted_key(key_id)
            if not record:
                continue
            created_at = datetime.fromtimestamp(record["created_at"])
            age_days = (datetime.now() - created_at).days
            if age_days >= config.KEY_ROTATION_DAYS or force:
                new_key = self.generate_keypair(record["algorithm"])
                rotated.append(new_key)
                logger.info("Rotated key %s to %s", key_id, new_key["key_id"])
        return rotated

# ============================================================================
# 5. BLOCKCHAIN VERIFICATION ENGINE (with retry and enhanced breaker)
# ============================================================================

class BlockchainEnhancementsVerification:
    """Ethereum smart contract integration with nonce caching, dynamic gas pricing,
       tenacity retries, and enhanced circuit breaker."""

    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage or Storage()
        self.web3 = None
        self.account = None
        self.contract = None
        self.web3_available = False
        self._nonce_cache = {}  # address -> nonce
        self._circuit_breaker = EnhancedCircuitBreaker("blockchain", timeout_seconds=30)

        if WEB3_AVAILABLE and config.RPC_URL:
            self._initialize_blockchain()

    def _initialize_blockchain(self):
        try:
            self.web3 = Web3(HTTPProvider(config.RPC_URL))
            if not self.web3.is_connected():
                raise ConnectionError("Cannot connect to blockchain RPC")

            self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
            self.web3.eth.set_gas_price_strategy(gas_price_strategy.rpc_gas_price_strategy)

            private_key = os.getenv("BLOCKCHAIN_PRIVATE_KEY")
            if private_key:
                self.account = Account.from_key(private_key)
                self.web3.eth.default_account = self.account.address
            else:
                self.account = self.web3.eth.accounts[0]

            self.contract = self._load_contract()
            if self.contract:
                self.web3_available = True
                logger.info("Connected to blockchain at %s", config.RPC_URL)
            else:
                logger.warning("Contract not loaded – blockchain verification will be simulated.")
        except Exception as e:
            logger.error("Blockchain initialization failed: %s", e)
            self.web3_available = False

    def _load_contract(self):
        # same as original
        pass

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((Exception,))
    )
    async def _get_nonce(self, address: str) -> int:
        if address not in self._nonce_cache:
            self._nonce_cache[address] = self.web3.eth.get_transaction_count(address)
        return self._nonce_cache[address]

    async def _increment_nonce(self, address: str):
        self._nonce_cache[address] = self._nonce_cache.get(address, 0) + 1

    async def verify_contract_execution(self, contract_address: str, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        async def _execute():
            if not self.web3_available:
                return self._simulate_record(contract_address, method, params)

            try:
                nonce = await self._get_nonce(self.account.address)
                gas_estimate = self.contract.functions.recordData(
                    params.get('dataId', ''),
                    params.get('dataHash', ''),
                    json.dumps(params.get('metadata', {}))
                ).estimate_gas({'from': self.account.address})
                gas_price = self.web3.eth.generate_gas_price() or self.web3.eth.gas_price

                tx = self.contract.functions.recordData(
                    params.get('dataId', ''),
                    params.get('dataHash', ''),
                    json.dumps(params.get('metadata', {}))
                ).build_transaction({
                    'from': self.account.address,
                    'nonce': nonce,
                    'gas': int(gas_estimate * config.GAS_MULTIPLIER),
                    'gasPrice': gas_price
                })

                signed_tx = self.account.sign_transaction(tx)
                tx_hash = self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
                receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

                if receipt.status == 1:
                    await self._increment_nonce(self.account.address)
                    block_number = receipt.blockNumber
                    self.storage.record_blockchain_tx(tx_hash.hex(), contract_address, method, params, "confirmed", block_number)
                    logger.info("Recorded transaction %s at block %d", tx_hash.hex(), block_number)
                    return {'status': 'success', 'tx_hash': tx_hash.hex(), 'block_number': block_number}
                else:
                    logger.error("Transaction failed")
                    return {'status': 'failed', 'error': 'transaction reverted'}
            except Exception as e:
                logger.error("Blockchain execution failed: %s", e)
                raise

        return await self._circuit_breaker.call(_execute)

    def _simulate_record(self, contract_address: str, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        # unchanged
        pass

# ============================================================================
# 6. MULTI-TEACHER ON-POLICY DISTILLATION OPTIMIZER
# ============================================================================

class StudentPolicy(nn.Module):
    """Small neural network for student policy."""
    def __init__(self, state_dim: int, action_dim: int, hidden: int = 128):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(state_dim, hidden),
            nn.ReLU(),
            nn.Linear(hidden, hidden),
            nn.ReLU(),
            nn.Linear(hidden, action_dim)
        )

    def forward(self, x):
        return torch.softmax(self.net(x), dim=-1)


class MTPDOptimizer:
    """
    Multi-Teacher On-Policy Distillation optimizer.
    Replaces the ε-greedy bandit. Teachers are the domain engines.
    """
    def __init__(self, storage: Storage, teachers: List[Callable],
                 state_dim: int = config.MTPD_STATE_DIM,
                 action_dim: int = config.MTPD_ACTION_DIM,
                 hidden: int = config.MTPD_HIDDEN_SIZE,
                 lr: float = config.MTPD_LR,
                 beta: float = config.MTPD_BETA,
                 gamma: float = config.MTPD_GAMMA,
                 buffer_size: int = config.MTPD_BUFFER_SIZE,
                 train_interval: int = config.MTPD_TRAIN_INTERVAL,
                 batch_size: int = config.MTPD_BATCH_SIZE):
        self.storage = storage
        self.teachers = teachers
        self.state_dim = state_dim
        self.action_dim = action_dim
        self.beta = beta
        self.gamma = gamma
        self.train_interval = train_interval
        self.batch_size = batch_size

        self.student = StudentPolicy(state_dim, action_dim, hidden)
        self.optimizer = optim.Adam(self.student.parameters(), lr=lr)
        self.buffer = deque(maxlen=buffer_size)
        self.step_counter = 0

        # Load previously saved model if exists
        self._load_model()

    def _encode_state(self, raw_state: Dict) -> np.ndarray:
        """Convert state dict to a fixed-size feature vector."""
        # Example features: carbon intensity, spot price, workload size, time of day,
        # cloud region costs, etc. You can extend this.
        features = [
            raw_state.get('carbon_intensity', 0.0),
            raw_state.get('spot_price', 0.0),
            raw_state.get('workload_size', 0.5),
            datetime.now().hour / 24.0,
            raw_state.get('latency_ms', 0.0) / 1000.0,
            raw_state.get('cost_usd', 0.0) / 10.0,
            raw_state.get('temperature', 25.0) / 50.0,
            raw_state.get('q_value_avg', 0.0)
        ]
        # Pad or truncate to state_dim
        if len(features) < self.state_dim:
            features += [0.0] * (self.state_dim - len(features))
        return np.array(features[:self.state_dim], dtype=np.float32)

    def select_strategy(self, state: Dict, candidates: List[StrategyMetrics]) -> StrategyMetrics:
        """
        Select an action based on student policy.
        Map action indices to candidates.
        """
        state_vec = self._encode_state(state)
        with torch.no_grad():
            probs = self.student(torch.FloatTensor(state_vec).unsqueeze(0)).squeeze(0).numpy()
        action_idx = np.random.choice(len(probs), p=probs)
        # Ensure the chosen action index is valid
        if action_idx >= len(candidates):
            action_idx = random.choice(range(len(candidates)))
        chosen = candidates[action_idx]
        # Store the index for later update
        chosen.action_idx = action_idx
        return chosen

    async def update(self, state: Dict, chosen: StrategyMetrics, reward: float):
        """Update the student policy using on-policy data."""
        state_vec = self._encode_state(state)
        # Compute teacher ensemble distribution (average of teacher logits)
        teacher_probs = np.zeros(self.action_dim)
        for teacher in self.teachers:
            try:
                # Each teacher must return a dict with 'action_probs' or we call it with state
                # For simplicity, we assume teacher returns a probability vector.
                t_probs = await teacher(state)  # or teacher(state) if sync
                teacher_probs += t_probs
            except Exception as e:
                logger.warning(f"Teacher failed: {e}, using uniform")
                teacher_probs += np.ones(self.action_dim) / self.action_dim
        teacher_probs /= len(self.teachers)
        # Normalise
        teacher_probs = teacher_probs / teacher_probs.sum()

        # Push to buffer
        self.buffer.append((
            state_vec,
            chosen.action_idx,
            reward,
            teacher_probs
        ))

        self.step_counter += 1
        if self.step_counter % self.train_interval == 0 and len(self.buffer) >= self.batch_size:
            self._train_step()
            # Save model periodically
            self._save_model()

    def _train_step(self):
        batch = random.sample(self.buffer, self.batch_size)
        states, actions, rewards, teacher_probs = zip(*batch)
        states = torch.FloatTensor(np.array(states))
        actions = torch.LongTensor(actions)
        rewards = torch.FloatTensor(rewards)
        teacher_probs = torch.FloatTensor(np.array(teacher_probs))

        student_probs = self.student(states)

        # Policy gradient loss (REINFORCE)
        log_probs = torch.log(student_probs[range(self.batch_size), actions])
        loss_rl = -(log_probs * rewards).mean()

        # Distillation loss (KL divergence)
        loss_distill = torch.sum(
            teacher_probs * (torch.log(teacher_probs + 1e-8) - torch.log(student_probs + 1e-8)),
            dim=1
        ).mean()

        total_loss = loss_rl + self.beta * loss_distill

        self.optimizer.zero_grad()
        total_loss.backward()
        self.optimizer.step()

    def _save_model(self):
        """Serialize student model weights and save to SQLite."""
        buffer = io.BytesIO()
        torch.save(self.student.state_dict(), buffer)
        self.storage.save_model_weights("mtpd_student", buffer.getvalue())

    def _load_model(self):
        """Load model weights from SQLite if available."""
        data = self.storage.load_model_weights("mtpd_student")
        if data:
            buffer = io.BytesIO(data)
            state_dict = torch.load(buffer)
            self.student.load_state_dict(state_dict)
            logger.info("Loaded MTPD student model from storage.")

# ============================================================================
# 7. MULTI-CLOUD DISTRIBUTOR (with retries and fallback chains)
# ============================================================================

class MultiCloudDistributor:
    """Multi-Cloud management with retries, fallback chains, and enhanced circuit breaker."""

    def __init__(self, region: Optional[str] = None):
        self.region = region or config.CLOUD_REGION
        self._circuit_breaker = EnhancedCircuitBreaker("cloud", timeout_seconds=20)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ClientError, ConnectionError, TimeoutError, Exception))
    )
    async def _dispatch_aws(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        s3 = boto3.client('s3', region_name=self.region,
                          aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                          aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
        bucket = "green-agent-workloads"
        key = f"workload_{secrets.token_hex(6)}.json"
        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(payload).encode())
        logger.info("Uploaded to S3: %s/%s", bucket, key)
        return {"provider": "aws", "region": self.region, "status": "dispatched", "object": f"s3://{bucket}/{key}"}

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _dispatch_azure(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        conn_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        if not conn_str:
            raise ValueError("Azure connection string not set.")
        blob_service = BlobServiceClient.from_connection_string(conn_str)
        container = "green-agent-workloads"
        blob_name = f"workload_{secrets.token_hex(6)}.json"
        blob_client = blob_service.get_blob_client(container, blob_name)
        blob_client.upload_blob(json.dumps(payload).encode(), overwrite=True)
        logger.info("Uploaded to Azure: %s/%s", container, blob_name)
        return {"provider": "azure", "region": self.region, "status": "dispatched", "object": f"azure://{container}/{blob_name}"}

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _dispatch_gcp(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        storage_client = storage.Client()
        bucket = storage_client.bucket("green-agent-workloads")
        blob_name = f"workload_{secrets.token_hex(6)}.json"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(json.dumps(payload).encode())
        logger.info("Uploaded to GCS: %s/%s", bucket.name, blob_name)
        return {"provider": "gcp", "region": self.region, "status": "dispatched", "object": f"gs://{bucket.name}/{blob_name}"}

    def _simulate_dispatch(self, target_provider: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {"provider": target_provider, "region": self.region, "status": "simulated", "task_id": f"sim_{secrets.token_hex(6)}"}

    async def dispatch_workload(self, target_provider: str, workload_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Dispatch with fallback chain."""
        providers = ['aws', 'azure', 'gcp', 'simulation']
        # Rotate so that target_provider is tried first
        idx = providers.index(target_provider.lower()) if target_provider.lower() in providers else 0
        ordered = providers[idx:] + providers[:idx]

        last_error = None
        for provider in ordered:
            try:
                if provider == 'aws' and AWS_AVAILABLE:
                    return await self._circuit_breaker.call(self._dispatch_aws, workload_payload)
                elif provider == 'azure' and AZURE_AVAILABLE:
                    return await self._circuit_breaker.call(self._dispatch_azure, workload_payload)
                elif provider == 'gcp' and GCP_AVAILABLE:
                    return await self._circuit_breaker.call(self._dispatch_gcp, workload_payload)
                elif provider == 'simulation':
                    return self._simulate_dispatch(provider, workload_payload)
                else:
                    continue
            except Exception as e:
                last_error = e
                logger.warning(f"{provider} failed, trying next: {e}")
                continue
        raise Exception(f"All cloud providers failed: {last_error}")

# ============================================================================
# 8. PROMETHEUS METRICS REGISTRY
# ============================================================================

class MetricsRegistry:
    """Centralized Prometheus metrics registry and HTTP server."""
    def __init__(self, port: Optional[int] = config.PROMETHEUS_PORT):
        self.port = port
        if PROMETHEUS_AVAILABLE and port:
            self.registry = CollectorRegistry()
            self.carbon_saved_total = Counter(
                'green_agent_carbon_saved_total_g',
                'Total carbon saved in grams',
                registry=self.registry
            )
            self.optimizer_decisions = Counter(
                'green_agent_optimizer_decisions_total',
                'Total decisions made by optimizer',
                ['strategy'],
                registry=self.registry
            )
            self.operation_latency = Histogram(
                'green_agent_operation_latency_seconds',
                'Operation latency in seconds',
                ['operation'],
                registry=self.registry
            )
            self.circuit_breaker_state = Gauge(
                'green_agent_circuit_breaker_state',
                'State of circuit breakers (0=CLOSED,1=HALF_OPEN,2=OPEN)',
                ['name'],
                registry=self.registry
            )
            self.cloud_dispatches = Counter(
                'green_agent_cloud_dispatches_total',
                'Cloud dispatches by provider',
                ['provider'],
                registry=self.registry
            )
            start_http_server(port, registry=self.registry)
            logger.info(f"Prometheus metrics exposed on port {port}")
        else:
            self.registry = None
            logger.warning("Prometheus not available or port not set.")

    def update_circuit_breaker(self, name: str, state: str):
        if self.registry:
            state_val = {'CLOSED':0, 'HALF_OPEN':1, 'OPEN':2}.get(state, 0)
            self.circuit_breaker_state.labels(name=name).set(state_val)

    # Other methods to increment counters, observe latency, etc.

# ============================================================================
# 9. STUB DOMAIN ENGINES (unchanged)
# ============================================================================
# ... (keep all stub classes as in original)

# ============================================================================
# 10. ASYNC LIFECYCLE MANAGER (modified to use new components)
# ============================================================================

class LifecycleManager:
    """Async-aware lifecycle manager with MTPD, enhanced security, metrics, and resilience."""

    def __init__(self):
        self.storage = Storage()
        self.security = QuantumResilientEnhancementsSecurity(self.storage)
        self.blockchain = BlockchainEnhancementsVerification(storage=self.storage)
        self.cloud = MultiCloudDistributor()
        self.metrics = MetricsRegistry()

        # Domain engines (real or stub)
        if DOMAIN_ENGINES_AVAILABLE:
            self.thermal_optimizer = ThermalAwareOptimizer()
            self.phase_energy_model = PhaseAwareEnergyModel()
            self.energy_scaler = EnergyProportionalScaler()
            self.marginal_carbon = MarginalCarbonIntensityForecaster()
            self.dual_accountant = DualCarbonAccountant()
            self.carbon_nas = CarbonAwareNAS()
            self.helium_elasticity = HeliumPriceElasticityModel()
            self.material_substitution = MaterialSubstitutionEngine()
            self.helium_circularity = HeliumCircularityTracker()
            self.regret_optimizer = RegretMinimizationOptimizer()
            self.federated_learning = FederatedGreenLearning()
        else:
            # Stub instances
            self.thermal_optimizer = StubThermalAwareOptimizer()
            # ... all other stubs

        # Build teacher list for MTPD
        # Each teacher must be a callable that takes a state dict and returns action probabilities (list or np.array)
        # We wrap engines to produce probabilities. For demonstration, we assume they have a method 'policy_probs'.
        teachers = [
            self.thermal_optimizer.policy_probs,    # assume such method exists
            self.phase_energy_model.policy_probs,
            self.energy_scaler.policy_probs,
            self.marginal_carbon.policy_probs,
            self.dual_accountant.policy_probs,
            self.carbon_nas.policy_probs,
        ]
        self.optimizer = MTPDOptimizer(
            storage=self.storage,
            teachers=teachers,
            state_dim=config.MTPD_STATE_DIM,
            action_dim=config.MTPD_ACTION_DIM
        )

        self._background_tasks: List[asyncio.Task] = []
        self._is_running = False

    async def startup(self) -> None:
        self._is_running = True
        logger.info("Green Agent Enhancements Gateway (v3.1.0) starting up...")
        loop = asyncio.get_running_loop()
        tasks = [
            loop.create_task(self._health_check_loop()),
            loop.create_task(self._key_rotation_loop()),
            loop.create_task(self._model_sync_loop()),
        ]
        self._background_tasks.extend(tasks)

    async def _health_check_loop(self) -> None:
        while self._is_running:
            await asyncio.sleep(60)
            logger.debug("System periodic health heart-beat OK.")
            # Optionally update metrics

    async def _key_rotation_loop(self) -> None:
        while self._is_running:
            await asyncio.sleep(86400)  # daily
            try:
                rotated = self.security.rotate_keys()
                if rotated:
                    logger.info("Rotated %d keys", len(rotated))
            except Exception as e:
                logger.error("Key rotation error: %s", e)

    async def _model_sync_loop(self) -> None:
        """Periodically save the MTPD student model."""
        while self._is_running:
            await asyncio.sleep(300)  # every 5 minutes
            self.optimizer._save_model()

    def get_health_status(self) -> Dict[str, Any]:
        active_tasks = [t for t in self._background_tasks if not t.done()]
        return {
            "status": "healthy" if self._is_running else "degraded",
            "uptime_seconds": time.time(),
            "pqc_available": PQC_AVAILABLE,
            "web3_available": WEB3_AVAILABLE,
            "crypto_available": CRYPTO_AVAILABLE,
            "domain_engines_available": DOMAIN_ENGINES_AVAILABLE,
            "active_tasks_count": len(active_tasks),
            "key_count": len(self.storage.list_key_ids()),
            "blockchain_connected": self.blockchain.web3_available,
            "mtpd_model_loaded": hasattr(self.optimizer, 'student') and self.optimizer.student is not None,
        }

    async def shutdown(self) -> None:
        logger.info("Initiating graceful shutdown sequence...")
        self._is_running = False
        for task in self._background_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._background_tasks.clear()
        gc.collect()
        logger.info("Graceful shutdown completed successfully.")

# ============================================================================
# 11. MODULE EXPORTS
# ============================================================================
__all__ = [
    "Config",
    "Storage",
    "QuantumResilientEnhancementsSecurity",
    "BlockchainEnhancementsVerification",
    "MTPDOptimizer",        # replaced AutonomousEnhancementsOptimizer
    "StrategyMetrics",
    "MultiCloudDistributor",
    "LifecycleManager",
    "PQC_AVAILABLE",
    "WEB3_AVAILABLE",
    "CRYPTO_AVAILABLE",
    "DOMAIN_ENGINES_AVAILABLE",
    "ThermalAwareOptimizer",
    "PhaseAwareEnergyModel",
    "EnergyProportionalScaler",
    "MarginalCarbonIntensityForecaster",
    "DualCarbonAccountant",
    "CarbonAwareNAS",
    "HeliumPriceElasticityModel",
    "MaterialSubstitutionEngine",
    "HeliumCircularityTracker",
    "RegretMinimizationOptimizer",
    "FederatedGreenLearning",
]
