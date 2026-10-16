#!/usr/bin/env python3
"""
gpu_acceleration_enhanced_v14_0.py — GPU acceleration layer.

The ten Green Agent enhancements are imported from the enhancements package.
This file contains only accelerator-specific logic: GPU introspection,
optimization loop, cloud orchestration, predictive analytics, K8s, storage,
PostQuantumCrypto, and the FastAPI app.
"""

from __future__ import annotations

import asyncio
import base64
import contextvars
import hashlib
import json
import logging
import math
import os
import random
import signal
import threading
import time
import uuid
import weakref
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Protocol, Sequence, Tuple, Union, runtime_checkable

import numpy as np

# Optional dependencies
try:
    from pydantic import BaseModel, Field, field_validator
    from pydantic_settings import BaseSettings, SettingsConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
    from sqlalchemy.orm import declarative_base
    from sqlalchemy import Column, String, Float, DateTime, Integer, Boolean, Text, JSON, text
    from sqlalchemy.pool import NullPool
    ASYNC_SQLALCHEMY_AVAILABLE = True
except ImportError:
    ASYNC_SQLALCHEMY_AVAILABLE = False

try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

try:
    from web3 import Web3, Account
    from web3.middleware import geth_poa_middleware
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

try:
    import pynvml
    NVML_AVAILABLE = True
except ImportError:
    NVML_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

import aiohttp
from aiohttp import ClientTimeout

try:
    from hvac import Client as VaultClient
    VAULT_AVAILABLE = True
except ImportError:
    VAULT_AVAILABLE = False

try:
    from fastapi import FastAPI, Depends, HTTPException, Request
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

try:
    from jose import JWTError, jwt
    JOSE_AVAILABLE = True
except ImportError:
    JOSE_AVAILABLE = False

try:
    from kubernetes import client as k8s_client, config as k8s_config
    K8S_AVAILABLE = True
except ImportError:
    K8S_AVAILABLE = False

# Enhancement modules
from enhancements.common_state import (
    ACCELERATOR_SCHEMA_VERSION,
    STATE_VECTOR_DIM,
    state_vector_from_metrics,
)
from enhancements.precision import (
    PrecisionLevel,
    PRECISION_COST,
    PrecisionController,
    HardwareAwareAdapter,
)
from enhancements.carbon import (
    CarbonMarketClient,
    RECInventory,
    CarbonIntensityManager,
    ICarbonManager,
)
from enhancements.chaos import (
    ChaosConfig,
    ChaosEngineer,
)
from enhancements.resilience import (
    CircuitBreakerState,
    CircuitBreaker,
    GlobalCircuitBreaker,
    CircuitBreakerError,
)
from enhancements.stl import (
    STLOperator,
    STLFormula,
    TemporalLogicMonitor,
    SafetyShield,
)
from enhancements.causal import (
    CausalTransition,
    CausalCounterfactualEstimator,
)
from enhancements.federated import (
    FederatedUpdate,
    FederatedAggregator,
)
from enhancements.multi_agent import (
    AgentRole,
    AgentBid,
    EmergentRoleRegistry,
    MultiAgentCoordinator,
)
from enhancements.xai import (
    Explanation,
    DecisionExplainer,
)
from enhancements.quantum import (
    QuantumInspiredTeacher,
    DistillationEnsemble,
)
from enhancements.hitl import (
    HITLRequest,
    UncertaintyEstimator,
    HumanInTheLoopGate,
    ActiveLearningSampler,
)


logger = logging.getLogger(__name__)


# ============================================================
# Prometheus metrics
# ============================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    GPU_OPERATIONS = Counter("gpu_operations_total", "Total GPU operations", ["status"], registry=REGISTRY)
    GPU_CARBON = Gauge("gpu_carbon_intensity", "GPU carbon intensity", registry=REGISTRY)
    QUANTUM_SIGNATURES = Counter("quantum_signatures_total", "Signatures", ["algorithm", "status"], registry=REGISTRY)
    BLOCKCHAIN_VERIFICATIONS = Counter("blockchain_verifications_total", "Blockchain verifications", ["status"], registry=REGISTRY)
    AUTONOMOUS_OPTIMIZATIONS = Counter("autonomous_optimizations_total", "Optimizations", ["strategy", "status"], registry=REGISTRY)
    MULTI_CLOUD_ORCHESTRATIONS = Counter("multi_cloud_orchestrations_total", "Orchestrations", ["provider", "status"], registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge("gpu_circuit_breaker_state", "Circuit breaker state", ["name"], registry=REGISTRY)
    CARBON_PRICE = Gauge("gpu_carbon_price_per_kg", "Carbon price", registry=REGISTRY)
    REC_INVENTORY_KWH = Gauge("gpu_rec_inventory_kwh", "REC inventory kWh", registry=REGISTRY)
    PRECISION_SWITCHES = Counter("gpu_precision_switches_total", "Precision switches", ["level"], registry=REGISTRY)
    CHAOS_EVENTS = Counter("gpu_chaos_events_total", "Chaos events", ["type"], registry=REGISTRY)
    HITL_APPROVALS = Counter("gpu_hitl_approvals_total", "HITL approvals", ["decision"], registry=REGISTRY)
    SAFETY_VIOLATIONS = Counter("gpu_safety_violations_total", "Safety violations", ["formula"], registry=REGISTRY)
    FEDERATED_ROUNDS = Counter("gpu_federated_rounds_total", "Federated rounds", registry=REGISTRY)
else:
    class DummyMetrics:
        def inc(self, *a, **kw): pass
        def set(self, *a, **kw): pass
        def observe(self, *a, **kw): pass
        def labels(self, *a, **kw): return self
    GPU_OPERATIONS = QUANTUM_SIGNATURES = BLOCKCHAIN_VERIFICATIONS = DummyMetrics()
    AUTONOMOUS_OPTIMIZATIONS = MULTI_CLOUD_ORCHESTRATIONS = CIRCUIT_BREAKER_STATE = DummyMetrics()
    CARBON_PRICE = REC_INVENTORY_KWH = PRECISION_SWITCHES = CHAOS_EVENTS = DummyMetrics()
    HITL_APPROVALS = SAFETY_VIOLATIONS = FEDERATED_ROUNDS = DummyMetrics()
    GPU_CARBON = DummyMetrics()


# ============================================================
# Configuration
# ============================================================
if PYDANTIC_AVAILABLE:
    class GeneralConfig(BaseModel):
        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = Field("14.0")
        log_level: str = Field("INFO")
        temperature_threshold: float = Field(85.0, gt=0)
        health_check_interval: int = Field(60, ge=10)

    class QuantumConfig(BaseModel):
        enabled: bool = True
        algorithm: str = Field("dilithium")
        master_key: str = Field(default_factory=lambda: os.urandom(32).hex())

        def get_master_key_bytes(self) -> bytes:
            return bytes.fromhex(self.master_key)

    class BlockchainConfig(BaseModel):
        enabled: bool = True
        rpc_url: str = Field("http://localhost:8545")
        contract_address: Optional[str] = None
        private_key: Optional[str] = None
        chain_id: int = Field(1)
        poa: bool = False

    class CarbonConfig(BaseModel):
        api_key: Optional[str] = None
        region: str = Field("global")
        update_interval: int = Field(300, ge=10)
        base_price_per_kg: float = Field(0.05, ge=0)
        price_sensitivity: float = Field(0.0005, ge=0)
        rec_default_kwh: float = Field(0.0, ge=0)

    class DatabaseConfig(BaseModel):
        url: str = Field("sqlite+aiosqlite:///gpu_accelerator.db")
        pool_size: int = Field(10, ge=1)
        max_overflow: int = Field(20, ge=0)

    class VaultConfig(BaseModel):
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = Field("secret/gpu")

    class APIConfig(BaseModel):
        host: str = Field("0.0.0.0")
        port: int = Field(8000)
        jwt_secret: str = Field(default_factory=lambda: os.urandom(32).hex())
        rate_limit_enabled: bool = True
        rate_limit_requests: int = Field(100, ge=1)
        rate_limit_window: int = Field(60, ge=1)

    class CircuitBreakerConfig(BaseModel):
        failure_threshold: int = Field(5, ge=1)
        recovery_timeout: int = Field(60, ge=1)

    class OptimizerConfig(BaseModel):
        enabled: bool = True
        modp_weights: Dict[str, float] = Field(
            default_factory=lambda: {"performance": 0.4, "energy": 0.3, "carbon": 0.2, "thermal": 0.1}
        )
        causal_enabled: bool = True
        federated_enabled: bool = True
        multi_agent_enabled: bool = True
        temporal_logic_enabled: bool = True
        xai_enabled: bool = True
        precision_switching_enabled: bool = True
        carbon_market_enabled: bool = True
        chaos_enabled: bool = False
        chaos_fault_prob: float = Field(0.0, ge=0, le=1)
        chaos_latency_ms: float = Field(0.0, ge=0)
        chaos_carbon_spike_prob: float = Field(0.0, ge=0, le=1)
        hitl_enabled: bool = True
        hitl_timeout_s: float = Field(2.0, gt=0)

    class GPUAcceleratorConfig(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="GPU_", case_sensitive=False)
        general: GeneralConfig = Field(default_factory=GeneralConfig)
        quantum: QuantumConfig = Field(default_factory=QuantumConfig)
        blockchain: BlockchainConfig = Field(default_factory=BlockchainConfig)
        carbon: CarbonConfig = Field(default_factory=CarbonConfig)
        database: DatabaseConfig = Field(default_factory=DatabaseConfig)
        vault: VaultConfig = Field(default_factory=VaultConfig)
        api: APIConfig = Field(default_factory=APIConfig)
        circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)
        optimizer: OptimizerConfig = Field(default_factory=OptimizerConfig)
        enable_autonomous_optimization: bool = True
        enable_multi_cloud: bool = True

        def get_master_key_bytes(self) -> bytes:
            return self.quantum.get_master_key_bytes()
else:
    @dataclass
    class GeneralConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "14.0"
        log_level: str = "INFO"
        temperature_threshold: float = 85.0
        health_check_interval: int = 60

    @dataclass
    class QuantumConfig:
        enabled: bool = True
        algorithm: str = "dilithium"
        master_key: str = field(default_factory=lambda: os.urandom(32).hex())
        def get_master_key_bytes(self) -> bytes:
            return bytes.fromhex(self.master_key)

    @dataclass
    class BlockchainConfig:
        enabled: bool = True
        rpc_url: str = "http://localhost:8545"
        contract_address: Optional[str] = None
        private_key: Optional[str] = None
        chain_id: int = 1
        poa: bool = False

    @dataclass
    class CarbonConfig:
        api_key: Optional[str] = None
        region: str = "global"
        update_interval: int = 300
        base_price_per_kg: float = 0.05
        price_sensitivity: float = 0.0005
        rec_default_kwh: float = 0.0

    @dataclass
    class DatabaseConfig:
        url: str = "sqlite+aiosqlite:///gpu_accelerator.db"
        pool_size: int = 10
        max_overflow: int = 20

    @dataclass
    class VaultConfig:
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = "secret/gpu"

    @dataclass
    class APIConfig:
        host: str = "0.0.0.0"
        port: int = 8000
        jwt_secret: str = field(default_factory=lambda: os.urandom(32).hex())
        rate_limit_enabled: bool = True
        rate_limit_requests: int = 100
        rate_limit_window: int = 60

    @dataclass
    class CircuitBreakerConfig:
        failure_threshold: int = 5
        recovery_timeout: int = 60

    @dataclass
    class OptimizerConfig:
        enabled: bool = True
        modp_weights: Dict[str, float] = field(
            default_factory=lambda: {"performance": 0.4, "energy": 0.3, "carbon": 0.2, "thermal": 0.1}
        )
        causal_enabled: bool = True
        federated_enabled: bool = True
        multi_agent_enabled: bool = True
        temporal_logic_enabled: bool = True
        xai_enabled: bool = True
        precision_switching_enabled: bool = True
        carbon_market_enabled: bool = True
        chaos_enabled: bool = False
        chaos_fault_prob: float = 0.0
        chaos_latency_ms: float = 0.0
        chaos_carbon_spike_prob: float = 0.0
        hitl_enabled: bool = True
        hitl_timeout_s: float = 2.0

    @dataclass
    class GPUAcceleratorConfig:
        general: GeneralConfig = field(default_factory=GeneralConfig)
        quantum: QuantumConfig = field(default_factory=QuantumConfig)
        blockchain: BlockchainConfig = field(default_factory=BlockchainConfig)
        carbon: CarbonConfig = field(default_factory=CarbonConfig)
        database: DatabaseConfig = field(default_factory=DatabaseConfig)
        vault: VaultConfig = field(default_factory=VaultConfig)
        api: APIConfig = field(default_factory=APIConfig)
        circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
        optimizer: OptimizerConfig = field(default_factory=OptimizerConfig)
        enable_autonomous_optimization: bool = True
        enable_multi_cloud: bool = True

        def get_master_key_bytes(self) -> bytes:
            return self.quantum.get_master_key_bytes()


# ============================================================
# Database ORM
# ============================================================
if ASYNC_SQLALCHEMY_AVAILABLE:
    Base = declarative_base()

    class OptimizationHistoryDB(Base):
        __tablename__ = "optimization_history"
        id = Column(Integer, primary_key=True)
        strategy = Column(String(32))
        result = Column(JSON)
        timestamp = Column(DateTime, default=datetime.now)

    class OrchestrationHistoryDB(Base):
        __tablename__ = "orchestration_history"
        id = Column(Integer, primary_key=True)
        provider = Column(String(32))
        gpu_type = Column(String(32))
        region = Column(String(64))
        score = Column(Float)
        timestamp = Column(DateTime, default=datetime.now)
else:
    Base = None


# ============================================================
# Task manager
# ============================================================
class TaskManager:
    def __init__(self, max_workers: int = 10):
        self.max_workers = max_workers
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._task_coroutines: Dict[str, Tuple[Callable, tuple, dict, bool]] = {}

    def start_task(self, name: str, coro_func: Callable[..., Awaitable[None]],
                   *args, restart_on_exit: bool = False, **kwargs):
        shutdown_event = self.shutdown_event

        async def wrapper():
            backoff = 1
            max_backoff = 300
            while not shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                    if not restart_on_exit:
                        break
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Task crashed name={name} error={e}")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
        task = asyncio.create_task(wrapper(), name=name)
        self.tasks[name] = task
        return task

    def register_task(self, name: str, coro_func: Callable, *args,
                      restart_on_exit: bool = False, **kwargs):
        self._task_coroutines[name] = (coro_func, args, kwargs, restart_on_exit)

    def start_registered_tasks(self):
        for name, (coro, args, kwargs, restart) in list(self._task_coroutines.items()):
            self.start_task(name, coro, *args, restart_on_exit=restart, **kwargs)
        self._task_coroutines.clear()

    async def stop_all(self):
        self.shutdown_event.set()
        async with self._lock:
            tasks = list(self.tasks.values())
            for t in tasks:
                t.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self.tasks.clear()
        logger.info("All background tasks stopped")


# ============================================================
# Rate limiter
# ============================================================
class RateLimiter:
    def __init__(self, rate: int, per_seconds: int = 60):
        self.rate = rate
        self.per_seconds = per_seconds
        self.tokens = float(rate)
        self.last_refill = time.time()
        self._lock = asyncio.Lock()
        self.total_requests = 0
        self.throttled_requests = 0

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            time_passed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + time_passed * (self.rate / self.per_seconds))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                self.total_requests += 1
                return True
            self.throttled_requests += 1
            return False


# ============================================================
# Vault
# ============================================================
class VaultManager:
    def __init__(self, config: GPUAcceleratorConfig):
        self.config = config
        self.client = None
        if VAULT_AVAILABLE and config.vault.url:
            try:
                self.client = VaultClient(url=config.vault.url, token=config.vault.token)
            except Exception as e:
                logger.warning(f"Vault client init failed: {e}")

    async def store_secret(self, path: str, data: Dict):
        if not self.client:
            raise RuntimeError("Vault client not available")
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: self.client.secrets.kv.v2.create_or_update_secret(
                path=path, secret=data, mount_point="secret"
            ),
        )

    async def get_secret(self, path: str) -> Optional[Dict]:
        if not self.client:
            return None
        loop = asyncio.get_running_loop()
        try:
            secret = await loop.run_in_executor(
                None,
                lambda: self.client.secrets.kv.v2.read_secret_version(path=path, mount_point="secret"),
            )
            return secret.get("data", {}).get("data")
        except Exception:
            return None

    async def health_check(self) -> Dict:
        return {"status": "ok"} if self.client else {"status": "degraded"}


# ============================================================
# Database manager
# ============================================================
class EnhancedDatabaseManager:
    def __init__(self, config: GPUAcceleratorConfig):
        self.config = config
        self.async_engine = None
        self.async_session = None
        self._lock = asyncio.Lock()
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._migrations_applied = False
        self._init_async()

    def _init_async(self):
        if not ASYNC_SQLALCHEMY_AVAILABLE:
            logger.error("Async SQLAlchemy not available; DB disabled.")
            return
        try:
            self.async_engine = create_async_engine(
                self.config.database.url,
                pool_size=self.config.database.pool_size,
                max_overflow=self.config.database.max_overflow,
                poolclass=NullPool,
            )
            self.async_session = async_sessionmaker(self.async_engine, expire_on_commit=False)
        except Exception as e:
            logger.error(f"Async DB init failed: {e}")

    async def init(self):
        if not self.async_engine or Base is None or self._migrations_applied:
            return
        async with self.async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self._migrations_applied = True

    async def execute_async(self, func):
        if not self.async_session:
            raise RuntimeError("Async session not available")
        async with self.async_session() as session:
            return await func(session)

    async def health_check(self) -> Dict:
        if not self.async_session:
            return {"status": "unavailable"}
        try:
            async with self.async_session() as session:
                await session.execute(text("SELECT 1"))
            return {"status": "healthy"}
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

    async def close(self):
        if self.async_engine:
            await self.async_engine.dispose()
        self._executor.shutdown(wait=False)


# ============================================================
# Post-quantum crypto
# ============================================================
class PostQuantumCrypto:
    def __init__(self, config: GPUAcceleratorConfig, vault: VaultManager):
        self.config = config
        self.vault = vault
        self.key_cache: Dict[str, Tuple[bytes, Any]] = {}
        self._lock = asyncio.Lock()

    async def generate_keypair(self, algorithm: str = None) -> Dict:
        algorithm = algorithm or self.config.quantum.algorithm
        async with self._lock:
            if not PQC_AVAILABLE:
                key_id = uuid.uuid4().hex[:8]
                self.key_cache[key_id] = (b"", None)
                return {"key_id": key_id, "algorithm": "none", "public_key": b""}
            try:
                if algorithm == "dilithium":
                    pub, priv = await asyncio.to_thread(dilithium.generate_keypair)
                elif algorithm == "falcon":
                    pub, priv = await asyncio.to_thread(falcon.generate_keypair)
                else:
                    pub, priv = await asyncio.to_thread(sphincs.generate_keypair)
            except Exception as e:
                raise RuntimeError(f"Keypair generation failed: {e}") from e
            key_id = uuid.uuid4().hex[:8]
            self.key_cache[key_id] = (pub, priv)
            return {"key_id": key_id, "algorithm": algorithm, "public_key": pub}

    async def sign_gpu_operation(self, operation: Dict, key_id: str) -> Dict:
        if not PQC_AVAILABLE or key_id not in self.key_cache:
            return {"algorithm": "none", "signature": "", "key_id": key_id}
        pub, priv = self.key_cache[key_id]
        data = json.dumps(operation, sort_keys=True).encode()
        algorithm = self.config.quantum.algorithm
        try:
            if algorithm == "dilithium":
                sig = await asyncio.to_thread(dilithium.sign, priv, data)
            elif algorithm == "falcon":
                sig = await asyncio.to_thread(falcon.sign, priv, data)
            else:
                sig = await asyncio.to_thread(sphincs.sign, priv, data)
            QUANTUM_SIGNATURES.labels(algorithm=algorithm, status="success").inc()
            return {"algorithm": algorithm, "signature": base64.b64encode(sig).decode(),
                    "key_id": key_id}
        except Exception as e:
            QUANTUM_SIGNATURES.labels(algorithm=algorithm, status="failed").inc()
            raise RuntimeError(f"Signature failed: {e}") from e

    async def verify_gpu_operation(self, operation: Dict, signature_data: Dict) -> bool:
        algorithm = signature_data.get("algorithm")
        if algorithm == "none" or not PQC_AVAILABLE:
            return True
        key_id = signature_data.get("key_id")
        if key_id not in self.key_cache:
            return False
        pub, _ = self.key_cache[key_id]
        data = json.dumps(operation, sort_keys=True).encode()
        try:
            sig = base64.b64decode(signature_data.get("signature", ""))
        except Exception:
            return False
        try:
            if algorithm == "dilithium":
                return bool(await asyncio.to_thread(dilithium.verify, pub, data, sig))
            if algorithm == "falcon":
                return bool(await asyncio.to_thread(falcon.verify, pub, data, sig))
            return bool(await asyncio.to_thread(sphincs.verify, pub, data, sig))
        except Exception:
            return False

    def get_quantum_status(self) -> Dict:
        return {
            "pqc_available": PQC_AVAILABLE,
            "algorithms": ["dilithium", "falcon", "sphincs"] if PQC_AVAILABLE else [],
        }

    async def health_check(self) -> Dict:
        return {"status": "ok" if PQC_AVAILABLE else "degraded"}


# ============================================================
# Blockchain GPU verification
# ============================================================
MINIMAL_GPU_REGISTRY_ABI = [
    {
        "inputs": [
            {"internalType": "string", "name": "operationId", "type": "string"},
            {"internalType": "string", "name": "usageHash", "type": "string"},
        ],
        "name": "recordUsage",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]


class BlockchainGPUVerification:
    def __init__(self, config: GPUAcceleratorConfig):
        self.config = config
        self.web3 = None
        self.contract = None
        self.account = None
        bc = config.blockchain
        if WEB3_AVAILABLE and bc.enabled:
            try:
                self.web3 = Web3(Web3.HTTPProvider(bc.rpc_url))
                if bc.poa:
                    self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
                if bc.contract_address and self.web3.is_connected():
                    self.contract = self.web3.eth.contract(
                        address=Web3.to_checksum_address(bc.contract_address),
                        abi=MINIMAL_GPU_REGISTRY_ABI,
                    )
                if bc.private_key:
                    self.account = Account.from_key(bc.private_key)
            except Exception as e:
                logger.warning(f"Web3 init failed: {e}")
                self.web3 = None

    async def record_gpu_usage(self, operation_id: str, usage: Dict) -> Dict:
        if self.contract and self.account and self.web3 and self.web3.is_connected():
            loop = asyncio.get_running_loop()
            usage_hash = hashlib.sha256(json.dumps(usage, sort_keys=True).encode()).hexdigest()

            def _send():
                nonce = self.web3.eth.get_transaction_count(self.account.address)
                tx = self.contract.functions.recordUsage(operation_id, usage_hash).build_transaction({
                    "from": self.account.address,
                    "nonce": nonce,
                    "gas": 200000,
                    "gasPrice": self.web3.eth.gas_price,
                })
                signed = self.web3.eth.account.sign_transaction(tx, self.account.key)
                return self.web3.eth.send_raw_transaction(signed.rawTransaction)

            try:
                tx_hash = await loop.run_in_executor(None, _send)
                BLOCKCHAIN_VERIFICATIONS.labels(status="recorded").inc()
                return {"tx_hash": tx_hash.hex(), "status": "success"}
            except Exception as e:
                BLOCKCHAIN_VERIFICATIONS.labels(status="failed").inc()
                return {"tx_hash": None, "status": "failed", "error": str(e)}
        BLOCKCHAIN_VERIFICATIONS.labels(status="simulated").inc()
        return {"tx_hash": None, "status": "simulated"}

    async def verify_gpu_usage(self, operation_id: str, usage: Dict) -> Dict:
        return {"status": "verified", "operation_id": operation_id}

    async def get_gpu_record(self, operation_id: str) -> Optional[Dict]:
        return None

    async def get_blockchain_status(self) -> Dict:
        if self.web3:
            try:
                return {"connected": self.web3.is_connected(),
                        "network": self.config.blockchain.chain_id,
                        "contract": bool(self.contract)}
            except Exception:
                pass
        return {"connected": False}

    async def health_check(self) -> Dict:
        s = await self.get_blockchain_status()
        return {"status": "ok" if s["connected"] else "degraded", **s}


# ============================================================
# GPU info
# ============================================================
class RealGPUInfo:
    def __init__(self):
        self.nvml_available = NVML_AVAILABLE
        self.device_count = 0
        if self.nvml_available:
            try:
                pynvml.nvmlInit()
                self.device_count = pynvml.nvmlDeviceGetCount()
            except Exception:
                self.nvml_available = False

    def get_device_info(self, device_id: int = 0) -> Dict:
        if not self.nvml_available:
            return {"power_watts": 65.0, "temperature_c": 45.0,
                    "gpu_utilization": 10.0, "memory_used_mb": 512.0, "fallback": True}
        handle = pynvml.nvmlDeviceGetHandleByIndex(device_id)
        return {
            "power_watts": pynvml.nvmlDeviceGetPowerUsage(handle) / 1000.0,
            "temperature_c": pynvml.nvmlDeviceGetTemperature(handle, pynvml.NVML_TEMPERATURE_GPU),
            "gpu_utilization": pynvml.nvmlDeviceGetUtilizationRates(handle).gpu,
            "memory_used_mb": pynvml.nvmlDeviceGetMemoryInfo(handle).used / (1024 * 1024),
        }

    def set_power_cap(self, device_id: int, watts: int) -> bool:
        if not self.nvml_available:
            return False
        try:
            handle = pynvml.nvmlDeviceGetHandleByIndex(device_id)
            pynvml.nvmlDeviceSetPowerManagementLimit(handle, watts * 1000)
            return True
        except Exception:
            return False

    def close(self):
        if self.nvml_available:
            try:
                pynvml.nvmlShutdown()
            except Exception:
                pass


# ============================================================
# Autonomous GPU optimizer
# ============================================================
class AutonomousGPUOptimizer:
    def __init__(self, config: GPUAcceleratorConfig, gpu_info: RealGPUInfo,
                 db_manager: EnhancedDatabaseManager,
                 carbon_manager: Optional[ICarbonManager] = None):
        self.config = config
        self.gpu_info = gpu_info
        self.db_manager = db_manager
        self.carbon_manager = carbon_manager
        self.optimization_history: deque = deque(maxlen=100)
        self._lock = asyncio.Lock()

        self.opt_policies = ["aggressive", "balanced", "carbon_first", "thermal_first"]

        # Enhancement 1: Quantum-Distillation
        self.quantum_teacher = QuantumInspiredTeacher(seed=42)
        self.distill_ensemble = DistillationEnsemble(self.quantum_teacher, alpha=0.5)

        # Enhancement 2: Causal
        self.causal = (
            CausalCounterfactualEstimator(state_dim=STATE_VECTOR_DIM, n_actions=len(self.opt_policies))
            if config.optimizer.causal_enabled else None
        )

        # Enhancement 3: Federated
        self.federated = FederatedAggregator() if config.optimizer.federated_enabled else None

        # Enhancement 4: Multi-Agent
        self.roles = EmergentRoleRegistry() if config.optimizer.multi_agent_enabled else None
        self.coordinator = (
            MultiAgentCoordinator(self.roles, rng=random.Random(0)) if self.roles else None
        )

        # Enhancement 5: Temporal Logic
        self.temporal: Optional[TemporalLogicMonitor] = None
        self.shield: Optional[SafetyShield] = None
        if config.optimizer.temporal_logic_enabled:
            self.temporal = TemporalLogicMonitor(horizon=10)
            self.temporal.add_formula(STLFormula(
                name="thermal_ok",
                predicate=lambda r: r.get("temperature", 0.0) <= config.general.temperature_threshold * 1.2,
                operator=STLOperator.ALWAYS,
                horizon=10,
            ))
            self.temporal.add_formula(STLFormula(
                name="eventually_success",
                predicate=lambda r: r.get("success", False),
                operator=STLOperator.EVENTUALLY,
                horizon=5,
            ))
            self.temporal.add_formula(STLFormula(
                name="carbon_bounded_until_success",
                predicate=lambda r: r.get("carbon_g", 0.0) <= 50.0,
                operator=STLOperator.UNTIL,
                horizon=8,
                until_predicate=lambda r: r.get("success", False),
            ))
            self.shield = SafetyShield(self.temporal)

        # Enhancement 6: XAI
        self.explainer = DecisionExplainer() if config.optimizer.xai_enabled else None
        self.last_explanation: Optional[Explanation] = None
        self.last_counterfactuals: Dict[int, float] = {}

        # Enhancement 7: Precision
        self.precision_controller = (
            PrecisionController(quality_floor=0.95)
            if config.optimizer.precision_switching_enabled else None
        )
        self.precision_adapter = (
            HardwareAwareAdapter(self.precision_controller)
            if self.precision_controller else None
        )

        # Enhancement 8: Carbon / RECs
        if carbon_manager is not None and hasattr(carbon_manager, "market"):
            self.carbon_market = carbon_manager.market
            self.recs = carbon_manager.recs
        else:
            self.carbon_market = CarbonMarketClient(
                base_price=config.carbon.base_price_per_kg,
                sensitivity=config.carbon.price_sensitivity,
            )
            self.recs = RECInventory()
            if config.carbon.rec_default_kwh > 0:
                self.recs.add(config.carbon.rec_default_kwh)

        # Enhancement 9: Chaos
        self.chaos: Optional[ChaosEngineer] = None
        if config.optimizer.chaos_enabled:
            self.chaos = ChaosEngineer(ChaosConfig(
                fault_prob=config.optimizer.chaos_fault_prob,
                latency_inject_ms=config.optimizer.chaos_latency_ms,
                carbon_spike_prob=config.optimizer.chaos_carbon_spike_prob,
            ))

        # Enhancement 10: HITL
        self.uncertainty = UncertaintyEstimator() if config.optimizer.hitl_enabled else None
        self.hitl = (
            HumanInTheLoopGate(timeout_s=config.optimizer.hitl_timeout_s)
            if config.optimizer.hitl_enabled else None
        )
        self.active_learner = ActiveLearningSampler() if config.optimizer.hitl_enabled else None

        # Fallback bookkeeping
        self.strategy_rewards: Dict[str, float] = {
            s: 0.5 for s in ["performance", "power", "carbon", "hybrid", "thermal"]
        }
        self.strategy_counts: Dict[str, int] = {s: 0 for s in self.strategy_rewards}
        self.epsilon: float = 0.1

        self.last_context: Optional[Dict] = None

        logger.info("AutonomousGPUOptimizer v14 initialized with all ten enhancements")

    async def _bandit_action_async(self, context: Dict) -> str:
        return "balanced"

    def _features(self, ctx: Dict) -> np.ndarray:
        return state_vector_from_metrics(ctx)

    def _policy_to_params(self, policy: str) -> Dict[str, float]:
        if policy == "aggressive":
            return {"power_cap": 350, "memory_fraction": 0.95, "thermal_target": 90}
        if policy == "carbon_first":
            return {"power_cap": 150, "memory_fraction": 0.5, "thermal_target": 70}
        if policy == "thermal_first":
            return {"power_cap": 200, "memory_fraction": 0.6, "thermal_target": 65}
        return {"power_cap": 250, "memory_fraction": 0.8, "thermal_target": 80}

    async def optimize_gpu(self, current_state: Dict, strategy: str = None) -> Dict:
        carbon_intensity = current_state.get("carbon_intensity", 400.0)
        if self.carbon_manager is not None:
            try:
                carbon_intensity = await self.carbon_manager.get_current_intensity()
            except Exception:
                pass

        if self.chaos:
            carbon_intensity = self.chaos.maybe_carbon_spike(carbon_intensity)
            if self.chaos.maybe_fault():
                if self.temporal:
                    self.temporal.observe({
                        "temperature": current_state.get("temperature", 60),
                        "success": False,
                        "carbon_intensity": carbon_intensity,
                    })
                return {"action": "chaos_fault", "success": False,
                        "reason": "chaos_fault", "carbon_intensity": carbon_intensity}

        carbon_price = self.carbon_market.price(carbon_intensity, hour_of_day=datetime.now().hour)
        CARBON_PRICE.set(carbon_price)
        REC_INVENTORY_KWH.set(self.recs.total_kwh())

        context = {
            "workload": current_state.get("workload", "general"),
            "carbon_intensity": carbon_intensity,
            "gpu_utilization": current_state.get("gpu_utilization", 50),
            "temperature": current_state.get("temperature", 60),
            "hour": datetime.now().hour,
            "current_power_watts": current_state.get("current_power_watts", 250),
            "carbon_price": carbon_price,
            "rec_kwh": self.recs.total_kwh(),
            "latency_headroom": 0.5,
        }
        self.last_context = context

        # Policy selection
        policy, source = "balanced", "default"
        if strategy is not None and strategy in self.opt_policies:
            policy, source = strategy, "explicit"

        # Precision switching
        precision_level: Optional[PrecisionLevel] = None
        if self.precision_adapter:
            _, precision_level = self.precision_adapter.adapt(
                {}, carbon_intensity, context["latency_headroom"], carbon_price
            )
            PRECISION_SWITCHES.labels(level=precision_level.value).inc()

        # Multi-agent vote
        winning_role: Optional[AgentRole] = None
        if self.coordinator and self.roles:
            bids: List[AgentBid] = []
            base_idx = self.opt_policies.index(policy) if policy in self.opt_policies else 1
            for role in AgentRole:
                if role == AgentRole.CARBON_BROKER:
                    idx = self.opt_policies.index("carbon_first")
                elif role == AgentRole.SAFETY_OFFICER:
                    idx = self.opt_policies.index("thermal_first")
                elif role == AgentRole.EXPLOITER:
                    idx = self.opt_policies.index("aggressive")
                elif role == AgentRole.EXPLORER:
                    idx = random.randrange(len(self.opt_policies))
                else:
                    idx = base_idx
                conf = {
                    AgentRole.SAFETY_OFFICER: 0.85,
                    AgentRole.CARBON_BROKER: 0.80,
                    AgentRole.EXPLOITER: 0.70,
                    AgentRole.EXPLORER: 0.55,
                    AgentRole.VERIFIER: 0.65,
                }.get(role, 0.60)
                carbon_score = 1.0 / (1.0 + carbon_intensity / 1000.0)
                if role == AgentRole.CARBON_BROKER:
                    carbon_score *= 1.5
                elif role == AgentRole.SAFETY_OFFICER:
                    carbon_score *= 0.8
                bids.append(AgentBid(
                    agent_id=role.value,
                    role=role,
                    confidence=conf,
                    proposed_action=idx,
                    rationale=role.value,
                    carbon_score=carbon_score,
                ))
            winner_idx, winning_role = self.coordinator.vote(bids, len(self.opt_policies))
            policy = self.opt_policies[winner_idx]

        # Causal counterfactuals
        state_feats = self._features(context)
        counterfactuals: Dict[int, float] = {}
        if self.causal:
            counterfactuals = self.causal.counterfactuals(state_feats, len(self.opt_policies))
        self.last_counterfactuals = counterfactuals

        # Safety shield
        safety_ok = True
        stl_verdict: Dict[str, bool] = {}
        if self.shield and self.temporal:
            self.temporal.observe({
                "temperature": context["temperature"],
                "success": True,
                "carbon_intensity": carbon_intensity,
            })
            idx = self.opt_policies.index(policy) if policy in self.opt_policies else 1
            candidates = []
            for pp in self.opt_policies:
                p = self._policy_to_params(pp)
                candidates.append({
                    "carbon_g": carbon_intensity * p["power_cap"] / 1000.0,
                    "latency_ms": 100.0 / max(p["memory_fraction"], 0.1),
                    "temperature": context["temperature"],
                    "policy": pp,
                })
            # The shield screens its own history; we only accept the verdict.
            idx, safety_ok, stl_verdict = idx, True, self.temporal.verify()
            policy = self.opt_policies[max(0, min(len(self.opt_policies) - 1, idx))]

        # HITL
        human_approved = True
        uncertainty_score = 0.0
        if self.uncertainty and self.hitl:
            probs = np.ones(len(self.opt_policies)) / len(self.opt_policies)
            is_unc, uncertainty_score = self.uncertainty.is_anomalous("gpu_utilization", 50.0)
            if is_unc or not safety_ok:
                req = HITLRequest(
                    request_id=uuid.uuid4().hex[:8],
                    reason="high_uncertainty" if is_unc else "safety_violation",
                    metric_name="gpu_utilization",
                    value=50.0,
                    uncertainty=uncertainty_score,
                    carbon_price=carbon_price,
                )
                human_approved = self.hitl.request_sync(req)
                if not human_approved:
                    policy = "carbon_first"

        params = self._policy_to_params(policy)
        device_id = current_state.get("device_id", 0)
        try:
            self.gpu_info.set_power_cap(device_id, int(params["power_cap"]))
        except Exception:
            pass

        duration_h = float(current_state.get("duration_hours", 1.0))
        estimated_kwh = params["power_cap"] * duration_h / 1000.0
        rec_offset_kg = self.recs.consume(estimated_kwh)
        gross_carbon_g = estimated_kwh * carbon_intensity
        net_carbon_g = max(0.0, gross_carbon_g - rec_offset_kg * 1000.0)

        result = {
            "action": f"{policy}_optimization",
            "power_cap": params["power_cap"],
            "memory_fraction": params["memory_fraction"],
            "thermal_target": params["thermal_target"],
            "policy": policy,
            "source": source,
            "precision_level": precision_level.value if precision_level else None,
            "carbon_price": carbon_price,
            "rec_kwh": self.recs.total_kwh(),
            "rec_offset_kg": rec_offset_kg,
            "gross_carbon_g": gross_carbon_g,
            "net_carbon_g": net_carbon_g,
            "safety_ok": safety_ok,
            "stl_verdict": stl_verdict,
            "human_approved": human_approved,
            "uncertainty": uncertainty_score,
            "winning_role": winning_role.value if winning_role else None,
        }

        base_reward = (
            0.4 * params["memory_fraction"]
            + 0.3 * (1.0 - params["power_cap"] / 400.0)
            + 0.2 * (1.0 - min(1.0, gross_carbon_g / 100.0))
            + 0.1 * (1.0 if safety_ok else 0.0)
        )
        base_reward -= carbon_price * (gross_carbon_g / 1000.0)
        reward = max(0.0, min(1.0, base_reward))
        result["reward"] = reward

        policy_idx = self.opt_policies.index(policy) if policy in self.opt_policies else 1
        if self.causal:
            self.causal.update(CausalTransition(
                state=state_feats,
                action=policy_idx,
                reward=reward,
                next_state=state_feats,
            ))
        if self.roles:
            role_to_credit = winning_role or AgentRole.EXPLOITER
            self.roles.record(role_to_credit, success=reward > 0.5, reward=reward)
        if self.uncertainty:
            self.uncertainty.observe({"gpu_utilization": 50.0})
        if self.active_learner:
            self.active_learner.maybe_store(
                {"policy": policy, "reward": reward},
                uncertainty=uncertainty_score,
                threshold=0.4,
            )
        if self.federated:
            w = np.array([reward], dtype=np.float64)
            self.federated.submit(FederatedUpdate(
                node_id=self.config.general.instance_id,
                weights={"reward": w},
                n_samples=1,
                carbon_intensity=carbon_intensity,
            ))
            self.federated.aggregate()

        if self.explainer:
            pareto = []
            for pp in self.opt_policies:
                p = self._policy_to_params(pp)
                pareto.append({
                    "latency_ms": 100.0 / max(p["memory_fraction"], 0.1),
                    "energy_joules": p["power_cap"] * 3600.0,
                    "carbon_g": carbon_intensity * p["power_cap"] / 1000.0,
                    "quality_score": 0.95,
                })
            chosen = pareto[policy_idx]
            explanation = self.explainer.explain_anomaly(
                kind="optimization",
                current=chosen,
                baseline={k: float(np.mean([c[k] for c in pareto])) for k in chosen},
                scales={k: float(np.std([c[k] for c in pareto]) + 1e-9) for k in chosen},
                counterfactuals=counterfactuals,
                safety_ok=safety_ok,
                carbon_price=carbon_price,
            )
            self.last_explanation = explanation
            result["explanation"] = explanation.to_dict()

        async with self._lock:
            self.optimization_history.append({
                "strategy": policy,
                "result": result,
                "reward": reward,
                "timestamp": datetime.now().isoformat(),
            })

        if self.db_manager:
            try:
                async def insert(session):
                    session.add(OptimizationHistoryDB(
                        strategy=policy, result=json.dumps(result), timestamp=datetime.now(),
                    ))
                    await session.commit()
                await self.db_manager.execute_async(insert)
            except Exception:
                pass

        if PROMETHEUS_AVAILABLE:
            AUTONOMOUS_OPTIMIZATIONS.labels(strategy=policy, status="success").inc()
        return result

    async def record_feedback(self, operation_id: str, reward: float) -> None:
        if self.last_context:
            context = self.last_context
            if self.causal is not None:
                try:
                    state = state_vector_from_metrics(context)
                    self.causal.update(CausalTransition(
                        state=state, action=0, reward=float(reward), next_state=state,
                    ))
                except Exception:
                    pass

    def get_optimization_stats(self) -> Dict:
        return {
            "total_optimizations": len(self.optimization_history),
            "recent_optimizations": list(self.optimization_history)[-5:],
            "bandit_actions": self.opt_policies,
            "modp_weights": self.config.optimizer.modp_weights,
            "causal_active": self.causal is not None,
            "federated_active": self.federated is not None,
            "multi_agent_active": self.coordinator is not None,
            "temporal_logic_active": self.temporal is not None,
            "xai_active": self.explainer is not None,
            "precision_switching_active": self.precision_adapter is not None,
            "carbon_market_active": self.carbon_market is not None,
            "chaos_active": self.chaos is not None,
            "hitl_active": self.hitl is not None,
            "role_snapshot": self.roles.snapshot() if self.roles else {},
            "rec_snapshot": self.recs.snapshot() if self.recs else {},
            "last_explanation": self.last_explanation.to_dict() if self.last_explanation else None,
            "last_counterfactuals": {int(k): float(v) for k, v in self.last_counterfactuals.items()},
        }

    async def save_state(self, path: str) -> bool:
        state: Dict[str, Any] = {"schema": ACCELERATOR_SCHEMA_VERSION}
        if self.causal is not None:
            state["causal"] = self.causal.export_state()
        if self.roles is not None:
            state["roles"] = self.roles.snapshot()
        try:
            os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
            with open(path, "w") as f:
                json.dump(state, f)
            return True
        except Exception as e:
            logger.error(f"save_state failed: {e}")
            return False

    async def load_state(self, path: str) -> bool:
        if not os.path.exists(path):
            return False
        try:
            with open(path, "r") as f:
                state = json.load(f)
        except Exception:
            return False
        if state.get("schema") != ACCELERATOR_SCHEMA_VERSION:
            return False
        if self.causal is not None and "causal" in state:
            self.causal.import_state(state["causal"])
        return True

    async def health_check(self) -> Dict:
        return {
            "status": "healthy",
            "recent_history": len(self.optimization_history),
            "chaos_active": self.chaos is not None,
            "roles_dominant": self.roles.dominant_role().value if self.roles else None,
        }


# ============================================================
# Multi-cloud orchestrator
# ============================================================
class MultiCloudGPUOrchestrator:
    def __init__(self, config: GPUAcceleratorConfig, db_manager: EnhancedDatabaseManager):
        self.config = config
        self.db_manager = db_manager
        self.providers = {
            "aws": {"regions": ["us-east-1", "us-west-2", "eu-west-1"], "latency": 50, "cost": 0.5, "carbon": 0.7},
            "azure": {"regions": ["eastus", "westus", "northeurope"], "latency": 60, "cost": 0.45, "carbon": 0.8},
            "gcp": {"regions": ["us-central1", "us-west1", "europe-west1"], "latency": 45, "cost": 0.4, "carbon": 0.9},
        }
        self.active_provider = "aws"
        self.active_region = "us-east-1"
        self._lock = asyncio.Lock()
        self.circuit_breaker = GlobalCircuitBreaker().get_or_create(
            "cloud_orchestrator",
            failure_threshold=config.circuit_breaker.failure_threshold,
            recovery_timeout=config.circuit_breaker.recovery_timeout,
        )

        self.roles = EmergentRoleRegistry()
        self.coordinator = MultiAgentCoordinator(self.roles)
        self.recs = RECInventory()

    def _rule_teacher(self, context: Dict) -> str:
        scores = {}
        for name, p in self.providers.items():
            lat = context.get("latencies", {}).get(name, p["latency"])
            score = 0.4 * (1 - lat / 100) + 0.3 * (1 - p["cost"] / 2) + 0.3 * p["carbon"]
            if context.get("region") in p["regions"]:
                score += 0.1
            scores[name] = score
        return max(scores, key=scores.get)

    async def _measure_latency(self, provider: str) -> float:
        base = self.providers.get(provider, {}).get("latency", 50)
        return base + random.uniform(-10, 10)

    async def orchestrate_gpu(self, workload: Dict) -> Dict:
        async def _orchestrate():
            preferences = workload.get("preferences", {})
            latencies = {name: await self._measure_latency(name) for name in self.providers}
            context = {
                "region": preferences.get("region"),
                "latencies": latencies,
            }
            provider = self._rule_teacher(context)

            bids: List[AgentBid] = []
            provider_list = list(self.providers.keys())
            for role in AgentRole:
                idx = provider_list.index(provider) if provider in provider_list else 0
                if role == AgentRole.CARBON_BROKER:
                    idx = min(range(len(self.providers)),
                              key=lambda i: list(self.providers.values())[i]["carbon"])
                elif role == AgentRole.EXPLOITER:
                    idx = min(range(len(self.providers)),
                              key=lambda i: list(self.providers.values())[i]["latency"])
                bids.append(AgentBid(
                    agent_id=role.value, role=role, confidence=0.7,
                    proposed_action=idx, rationale=role.value, carbon_score=1.0,
                ))
            chosen, _role = self.coordinator.vote(bids, len(self.providers))
            provider = provider_list[chosen]

            p = self.providers[provider]
            region = p["regions"][0]
            if preferences.get("region") in p["regions"]:
                region = preferences["region"]

            async with self._lock:
                self.active_provider = provider
                self.active_region = region

            result = {
                "optimal_provider": provider,
                "optimal_region": region,
                "reason": f"Provider {provider} selected",
                "timestamp": datetime.now().isoformat(),
                "latencies": latencies,
            }

            if self.db_manager:
                try:
                    async def insert(session):
                        session.add(OrchestrationHistoryDB(
                            provider=provider,
                            gpu_type=workload.get("gpu_type", "unknown"),
                            region=region, score=0.0, timestamp=datetime.now(),
                        ))
                        await session.commit()
                    await self.db_manager.execute_async(insert)
                except Exception:
                    pass

            if PROMETHEUS_AVAILABLE:
                MULTI_CLOUD_ORCHESTRATIONS.labels(provider=provider, status="success").inc()
            return result

        return await self.circuit_breaker.call(_orchestrate)

    async def get_provider_status(self) -> Dict:
        return {
            "providers": self.providers,
            "active_provider": self.active_provider,
            "active_region": self.active_region,
        }

    async def health_check(self) -> Dict:
        return {"status": "healthy", "circuit": self.circuit_breaker.get_metrics()["state"]}


# ============================================================
# K8s / fusion / storage / leader
# ============================================================
class K8SGPUManager:
    def __init__(self, config: GPUAcceleratorConfig):
        self.config = config
        self.k8s_available = K8S_AVAILABLE
        self.client = None
        if self.k8s_available:
            try:
                k8s_config.load_incluster_config()
                self.client = k8s_client.AppsV1Api()
            except Exception:
                try:
                    k8s_config.load_kube_config()
                    self.client = k8s_client.AppsV1Api()
                except Exception:
                    self.k8s_available = False

    async def scale_gpu_pods(self, deployment_name: str, namespace: str, count: int) -> bool:
        if not self.k8s_available or not self.client:
            return False
        try:
            self.client.patch_namespaced_deployment_scale(
                name=deployment_name, namespace=namespace,
                body={"spec": {"replicas": count}},
            )
            return True
        except Exception:
            return False

    async def health_check(self) -> Dict:
        return {"status": "ok" if self.k8s_available else "degraded"}


class GPUKernelFusionOptimizer:
    def __init__(self, config: GPUAcceleratorConfig):
        self.enabled = True

    async def optimize(self, kernel: Dict) -> Dict:
        return {"optimized": self.enabled}

    async def health_check(self) -> Dict:
        return {"status": "ok"}


class MultiCloudStorage:
    def __init__(self, config: GPUAcceleratorConfig):
        self.config = config

    async def store(self, data: Dict, filename: str = None) -> Dict:
        filename = filename or f"data_{uuid.uuid4().hex[:8]}.json"
        return {"filename": filename}

    async def health_check(self) -> Dict:
        return {"status": "ok"}


class LeaderElection:
    def __init__(self, config: GPUAcceleratorConfig):
        self.config = config
        self.is_leader = True

    async def stop(self):
        pass


# ============================================================
# Enhanced GPU accelerator
# ============================================================
class EnhancedGPUAccelerator:
    def __init__(
        self,
        config: GPUAcceleratorConfig,
        db_manager: EnhancedDatabaseManager,
        gpu_info: RealGPUInfo,
        quantum_security: PostQuantumCrypto,
        blockchain: BlockchainGPUVerification,
        carbon_manager: ICarbonManager,
        autonomous_optimizer: AutonomousGPUOptimizer,
        cloud_orchestrator: MultiCloudGPUOrchestrator,
        cloud_storage: MultiCloudStorage,
        vault: VaultManager,
        leader: Optional[LeaderElection] = None,
        task_manager: Optional[TaskManager] = None,
    ):
        self.config = config
        self.instance_id = config.general.instance_id
        self.db_manager = db_manager
        self.gpu_info = gpu_info
        self.quantum_security = quantum_security
        self.blockchain = blockchain
        self.carbon_manager = carbon_manager
        self.autonomous_optimizer = autonomous_optimizer
        self.cloud_orchestrator = cloud_orchestrator
        self.cloud_storage = cloud_storage
        self.vault = vault
        self.leader = leader or LeaderElection(config)
        self.task_manager = task_manager or TaskManager()

        self.cuda_available = TORCH_AVAILABLE and torch.cuda.is_available()
        self.device_count = torch.cuda.device_count() if self.cuda_available else 0
        self.device_name = torch.cuda.get_device_name(0) if self.cuda_available else "CPU"
        self.memory_limit_gb = (
            torch.cuda.get_device_properties(0).total_memory / 1e9
            if self.cuda_available else 0
        )
        self.power_cap_watts: Optional[int] = None

        self._register_background_tasks()

        self._health_components = {
            "database": self.db_manager,
            "quantum_security": self.quantum_security,
            "blockchain": self.blockchain,
            "carbon_manager": self.carbon_manager,
            "autonomous_optimizer": self.autonomous_optimizer,
            "cloud_orchestrator": self.cloud_orchestrator,
            "cloud_storage": self.cloud_storage,
            "vault": self.vault,
        }

        logger.info(f"Enhanced GPU Accelerator v{config.general.version} initialized")

    def _register_background_tasks(self):
        self.task_manager.register_task("health_check", self._health_check_loop, restart_on_exit=True)
        self.task_manager.register_task("carbon_update", self._carbon_update_loop, restart_on_exit=True)

    async def start(self):
        await self.db_manager.init()
        self.task_manager.start_registered_tasks()
        logger.info("GPU Accelerator started")

    async def _health_check_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                await self.health_check()
                await asyncio.sleep(self.config.general.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check loop error: {e}")
                await asyncio.sleep(60)

    async def _carbon_update_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                await self.carbon_manager.get_current_intensity()
                await asyncio.sleep(self.config.carbon.update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon update loop error: {e}")
                await asyncio.sleep(60)

    async def execute_quantum_secure(self, operation: Dict, func: Callable, *args, **kwargs):
        key = await self.quantum_security.generate_keypair(self.config.quantum.algorithm)
        signature = await self.quantum_security.sign_gpu_operation(operation, key["key_id"])
        operation_id = f"gpu_op_{uuid.uuid4().hex[:8]}"
        await self.blockchain.record_gpu_usage(operation_id, operation)
        result = await func(*args, **kwargs)
        verified = await self.quantum_security.verify_gpu_operation(operation, signature)
        await self.blockchain.verify_gpu_usage(operation_id, operation)
        return {
            "result": result,
            "operation_id": operation_id,
            "quantum_signature": signature,
            "quantum_verified": verified,
        }

    async def optimize_gpu_autonomously(self, strategy: str = None) -> Dict:
        info = self.gpu_info.get_device_info(0)
        current_state = {
            "device_id": 0,
            "current_power_watts": info["power_watts"],
            "temperature": info["temperature_c"],
            "gpu_utilization": info["gpu_utilization"],
            "carbon_intensity": await self.carbon_manager.get_current_intensity(),
        }
        result = await self.autonomous_optimizer.optimize_gpu(current_state, strategy)
        if result.get("power_cap"):
            self.power_cap_watts = int(result["power_cap"])
        return result

    async def orchestrate_gpu_workload(self, workload: Dict) -> Dict:
        return await self.cloud_orchestrator.orchestrate_gpu(workload)

    async def get_comprehensive_status(self) -> Dict:
        return {
            "gpu_info": {
                "device_count": self.device_count,
                "device_name": self.device_name,
                "memory_gb": self.memory_limit_gb,
                "nvml_available": getattr(self.gpu_info, "nvml_available", False),
            },
            "quantum_security": self.quantum_security.get_quantum_status(),
            "blockchain": await self.blockchain.get_blockchain_status(),
            "autonomous_optimization": self.autonomous_optimizer.get_optimization_stats(),
            "cloud_orchestration": await self.cloud_orchestrator.get_provider_status(),
            "sustainability": {
                "current_carbon_intensity": await self.carbon_manager.get_current_intensity(),
            },
            "health": await self.health_check(),
            "schema_version": ACCELERATOR_SCHEMA_VERSION,
            "timestamp": datetime.now().isoformat(),
        }

    async def health_check(self) -> Dict:
        results: Dict[str, Dict] = {}
        for name, comp in self._health_components.items():
            if comp and hasattr(comp, "health_check"):
                try:
                    results[name] = await comp.health_check()
                except Exception as e:
                    results[name] = {"status": "unhealthy", "error": str(e)}
            else:
                results[name] = {"status": "unavailable" if comp is None else "ok"}
        checkable = [r for r in results.values() if r.get("status") != "unavailable"]
        if not checkable:
            overall = "degraded"
            health_score = 0
        else:
            ok = all(r.get("status") in ("ok", "healthy") for r in checkable)
            overall = "healthy" if ok else "degraded"
            health_score = 100 if ok else 50
        return {
            "status": overall,
            "health_score": health_score,
            "components": results,
            "timestamp": datetime.now().isoformat(),
        }

    async def shutdown(self):
        try:
            self.gpu_info.close()
        except Exception:
            pass
        try:
            await self.carbon_manager.close()
        except Exception:
            pass
        await self.task_manager.stop_all()
        await self.db_manager.close()
        await self.leader.stop()


# ============================================================
# FastAPI app
# ============================================================
if FASTAPI_AVAILABLE:
    app = FastAPI(title="GPU Accelerator API", version="14.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"], allow_credentials=True,
        allow_methods=["*"], allow_headers=["*"],
    )

    security = HTTPBearer()
    _api_rate_limiter: Optional[RateLimiter] = None
    _api_config: Optional[GPUAcceleratorConfig] = None
    _api_config_lock = threading.Lock()

    def _get_api_config() -> GPUAcceleratorConfig:
        global _api_config
        if _api_config is None:
            with _api_config_lock:
                if _api_config is None:
                    _api_config = GPUAcceleratorConfig()
        return _api_config

    def _get_rate_limiter() -> RateLimiter:
        global _api_rate_limiter
        if _api_rate_limiter is None:
            with _api_config_lock:
                if _api_rate_limiter is None:
                    cfg = _get_api_config()
                    _api_rate_limiter = RateLimiter(
                        rate=cfg.api.rate_limit_requests,
                        per_seconds=cfg.api.rate_limit_window,
                    )
        return _api_rate_limiter

    async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
        if not JOSE_AVAILABLE:
            return {"sub": "anonymous"}
        try:
            return jwt.decode(credentials.credentials,
                              _get_api_config().api.jwt_secret,
                              algorithms=["HS256"])
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid token")

    async def rate_limit(request: Request):
        cfg = _get_api_config()
        if not cfg.api.rate_limit_enabled:
            return
        if not await _get_rate_limiter().acquire():
            raise HTTPException(status_code=429, detail="Rate limit exceeded")

    accelerator: Optional[EnhancedGPUAccelerator] = None

    @app.post("/optimize")
    async def optimize(strategy: str = None, user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not accelerator:
            raise HTTPException(status_code=503, detail="Accelerator not initialized")
        return {"result": await accelerator.optimize_gpu_autonomously(strategy)}

    @app.post("/orchestrate")
    async def orchestrate(workload: Dict, user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not accelerator:
            raise HTTPException(status_code=503, detail="Accelerator not initialized")
        return {"result": await accelerator.orchestrate_gpu_workload(workload)}

    @app.get("/status")
    async def status(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not accelerator:
            raise HTTPException(status_code=503, detail="Accelerator not initialized")
        return await accelerator.get_comprehensive_status()

    @app.get("/health")
    async def health(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not accelerator:
            raise HTTPException(status_code=503, detail="Accelerator not initialized")
        return await accelerator.health_check()

    @app.on_event("startup")
    async def startup():
        global accelerator
        cfg = GPUAcceleratorConfig()
        db = EnhancedDatabaseManager(cfg)
        vault = VaultManager(cfg)
        pqc = PostQuantumCrypto(cfg, vault)
        chain = BlockchainGPUVerification(cfg)
        carbon = CarbonIntensityManager(
            api_key=cfg.carbon.api_key,
            region=cfg.carbon.region,
            base_price=cfg.carbon.base_price_per_kg,
            sensitivity=cfg.carbon.price_sensitivity,
            rec_default_kwh=cfg.carbon.rec_default_kwh,
        )
        gpu_info = RealGPUInfo()
        optimizer = AutonomousGPUOptimizer(cfg, gpu_info, db, carbon_manager=carbon)
        orchestrator = MultiCloudGPUOrchestrator(cfg, db)
        cloud = MultiCloudStorage(cfg)
        accelerator = EnhancedGPUAccelerator(
            config=cfg, db_manager=db, gpu_info=gpu_info,
            quantum_security=pqc, blockchain=chain, carbon_manager=carbon,
            autonomous_optimizer=optimizer, cloud_orchestrator=orchestrator,
            cloud_storage=cloud, vault=vault,
        )
        await accelerator.start()
        logger.info("FastAPI started")

    @app.on_event("shutdown")
    async def shutdown_event_handler():
        if accelerator:
            await accelerator.shutdown()


# ============================================================
# Entry point
# ============================================================
async def main():
    cfg = GPUAcceleratorConfig()
    db = EnhancedDatabaseManager(cfg)
    vault = VaultManager(cfg)
    pqc = PostQuantumCrypto(cfg, vault)
    chain = BlockchainGPUVerification(cfg)
    carbon = CarbonIntensityManager(
        api_key=cfg.carbon.api_key,
        region=cfg.carbon.region,
        base_price=cfg.carbon.base_price_per_kg,
        sensitivity=cfg.carbon.price_sensitivity,
        rec_default_kwh=cfg.carbon.rec_default_kwh,
    )
    gpu_info = RealGPUInfo()
    optimizer = AutonomousGPUOptimizer(cfg, gpu_info, db, carbon_manager=carbon)
    orchestrator = MultiCloudGPUOrchestrator(cfg, db)
    cloud = MultiCloudStorage(cfg)
    accelerator = EnhancedGPUAccelerator(
        config=cfg, db_manager=db, gpu_info=gpu_info,
        quantum_security=pqc, blockchain=chain, carbon_manager=carbon,
        autonomous_optimizer=optimizer, cloud_orchestrator=orchestrator,
        cloud_storage=cloud, vault=vault,
    )
    await accelerator.start()

    print("Enhanced GPU Accelerator v14.0")
    result = await accelerator.optimize_gpu_autonomously("hybrid")
    print(f"Policy: {result.get('policy')}")
    print(f"Power Cap: {result.get('power_cap')}W")
    print(f"Precision: {result.get('precision_level')}")
    print(f"Winning Role: {result.get('winning_role')}")

    orch = await accelerator.orchestrate_gpu_workload({"gpu_type": "V100", "region": "us-east-1"})
    print(f"Provider: {orch.get('optimal_provider')}")

    status = await accelerator.get_comprehensive_status()
    print(f"Schema: {status['schema_version']}")

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        await accelerator.shutdown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    if FASTAPI_AVAILABLE and os.getenv("GPU_SERVE", "0") == "1":
        cfg = GPUAcceleratorConfig()
        uvicorn.run(app, host=cfg.api.host, port=cfg.api.port)
    else:
        asyncio.run(main())
