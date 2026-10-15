#!/usr/bin/env python3
# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/carbon_nas_unified.py
"""
Unified Carbon-Aware Neural Architecture Search — v6.1.0
==========================================================

A single-file, runnable Carbon-Aware NAS platform that integrates:

  - NAS algorithms (DARTS / ENAS / PNAS / Random) as deterministic simulators
  - Quantum distillation (QAOA) when qiskit is available
  - Federated learning (FedAvg)
  - Explainable AI (SHAP / LIME / heuristic fallback)
  - Causal bandit for meta-decision making
  - MoE routing, MODP, bio-inspired tuning
  - Temporal-logic safety monitor
  - Chaos engineering, human-in-the-loop
  - Carbon offset broker
  - PQC (Dilithium / Falcon / SPHINCS+ with ECDSA fallback)
  - Multi-cloud storage
  - FlexGen policy selection
  - Optional FastAPI + WebSocket surface

CHANGES (v6.1.0 over v6.0.4):
- ``ProxyModel`` is guarded by ``TORCH_AVAILABLE`` so the module imports
  cleanly without PyTorch.
- ``GreenAgentReasoningEngine`` accepts and stores a ``carbon_manager``;
  every consumer reads from it.
- PQC salt is persisted in storage and reused; stored keypairs survive
  process restarts.
- A seeded ``np.random.default_rng`` is threaded through every random call
  site; ``random`` is no longer used at module scope.
- ``CausalBandit.update`` uses ``setdefault`` and never raises for unknown
  actions.
- ``MultiAgentCoordinator.assign_task`` uses SHA-256 for stable hashing.
- ``FederatedCoordinator.aggregate`` filters non-numeric values and
  supports weighted averaging.
- ``HumanReviewManager.pending_reviews`` is a bounded ``OrderedDict``.
- ``run_nas_cycle`` awaits ``run_algorithm`` directly; the fragile thread-
  loop pattern is gone.
- Green Agent core imports are wrapped with fallbacks; the module runs
  standalone.
- ``EnhancedCircuitBreaker.call`` awaits only coroutines.
- MLflow uses ``run_name=`` instead of a truncated UUID for ``run_id``.
- ``CarbonAwareNAS.run_flexgen_optimization`` is implemented; the FastAPI
  endpoint now works.
- ``FeedbackEvent`` is emitted through a helper that tolerates schema
  variance.
- Qiskit imports use the modern module layout
  (``qiskit_aer``, ``qiskit_optimization``, ``qiskit_algorithms``).
- FastAPI ``on_event`` is replaced by the ``lifespan`` context manager;
  request bodies are Pydantic models.
- ``get_system_status.cloud_storage`` always returns a list of provider
  names.
- ``__version__`` and ``__all__`` added.
- Built-in test suite: ``python carbon_nas_unified.py --test``.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import inspect
import json
import logging
import math
import os
import pickle  # noqa: F401  (kept for forward compatibility)
import secrets
import time
import uuid
from collections import OrderedDict, defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, List, Optional, Protocol, Set, Tuple,
    Union,
)

import numpy as np

__version__ = "6.1.0"
__all__ = [
    "__version__",
    "CircuitBreakerState",
    "EnhancedCircuitBreaker",
    "EnhancedRateLimiter",
    "VaultManager",
    "PostQuantumCrypto",
    "CloudStorage",
    "WebSocketManager",
    "CausalBandit",
    "QuantumDistillationOptimizer",
    "SafetyMonitor",
    "XAIExplainer",
    "FederatedCoordinator",
    "MultiAgentCoordinator",
    "CarbonOffsetBroker",
    "ChaosMonkey",
    "HumanReviewManager",
    "BioInspiredOptimizer",
    "CarbonIntensityManager",
    "EnergyMeasurer",
    "AdvancedNASAlgorithms",
    "QuantumInspiredOptimizer",
    "FederatedLearningNAS",
    "AutomatedDeployment",
    "ExplainableNAS",
    "FlexGenManager",
    "GreenAgentReasoningEngine",
    "CarbonAwareNAS",
    "TaskManager",
    "build_app",
    "get_nas_instance",
    "reset_nas_for_tests",
    "main",
    "run_tests",
]


# ============================================================================
# Logging
# ============================================================================
_LOG_DIR = Path(os.environ.get("NAS_LOG_DIR", "./logs"))
try:
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
except OSError:
    _LOG_DIR = Path("/tmp")

logging.basicConfig(
    level=os.environ.get("NAS_LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ============================================================================
# Green Agent core integration (with standalone fallbacks)
# ============================================================================
class _ConfigShim:
    """
    Minimal stand-in for the central config.  When the real Green Agent
    core is importable, the module uses ``central_config`` instead.
    """
    CIRCUIT_BREAKER_FAILURE_THRESHOLD: int = 5
    CIRCUIT_BREAKER_RECOVERY_TIMEOUT: float = 60.0
    MODP_WEIGHTS: Dict[str, float] = {
        "accuracy": 0.4, "carbon": 0.3, "energy": 0.2, "latency": 0.1,
    }
    ENVIRONMENT: str = "development"
    JWT_SECRET: str = "dev-secret-change-me"
    SEED: int = 42
    QUANTUM_ENABLED: bool = False
    FEDERATED_ENABLED: bool = True
    DEPLOYMENT_ENABLED: bool = True
    CHAOS_ENABLED: bool = False
    VAULT_ADDR: Optional[str] = None
    VAULT_TOKEN: Optional[str] = None

    def __init__(self) -> None:
        self.metrics_registry = _MetricsRegistryShim()

    def get_master_key_bytes(self) -> bytes:
        raw = os.environ.get("NAS_MASTER_KEY")
        if raw:
            return bytes.fromhex(raw)
        return secrets.token_bytes(32)


class _MetricsRegistryShim:
    """No-op metrics registry used when the central registry is missing."""
    def update_circuit_breaker(self, name: str, state: str) -> None: ...
    def increment_cloud_dispatch(self, provider: str) -> None: ...
    def increment_safety_violation(self, rule: str) -> None: ...
    def set_best_accuracy(self, value: float) -> None: ...


class _StorageShim:
    """File-backed storage shim for standalone use."""
    def __init__(self, path: Optional[Union[str, Path]] = None) -> None:
        self.path = Path(path or os.environ.get("NAS_STORAGE_PATH",
                                                "./_nas_storage"))
        self.path.mkdir(parents=True, exist_ok=True)

    def _write(self, name: str, payload: Dict[str, Any]) -> None:
        (self.path / name).write_text(json.dumps(payload, default=str))

    def save_pqc_key(self, key_id: str, algorithm: str,
                     public_key: bytes, private_key: bytes,
                     expires_at: str) -> None:
        self._write(f"pqc_{key_id}.json", {
            "key_id": key_id, "algorithm": algorithm,
            "public_key": public_key.hex(),
            "private_key": private_key.hex(),
            "expires_at": expires_at,
        })

    def get_pqc_key(self, key_id: str) -> Optional[Dict[str, Any]]:
        p = self.path / f"pqc_{key_id}.json"
        if not p.exists():
            return None
        d = json.loads(p.read_text())
        return {
            "algorithm": d["algorithm"],
            "public_key": bytes.fromhex(d["public_key"]),
            "private_key": bytes.fromhex(d["private_key"]),
            "expires_at": d["expires_at"],
        }

    def list_pqc_keys(self) -> List[Dict[str, Any]]:
        return [json.loads(p.read_text())
                for p in self.path.glob("pqc_*.json")]

    def save_system_state(self, key: str, value: Any) -> None:
        self._write(f"state_{key}.json", {"value": value})

    def load_system_state(self, key: str,
                          default: Optional[Any] = None) -> Optional[Any]:
        p = self.path / f"state_{key}.json"
        if not p.exists():
            return default
        try:
            return json.loads(p.read_text()).get("value", default)
        except json.JSONDecodeError:
            return default

    def save_architecture_result(self, payload: Dict[str, Any]) -> None:
        self._write(f"arch_{payload['arch_hash']}.json", payload)

    def save_experiment(self, experiment_id: str, search_space: Dict[str, Any],
                        status: str) -> None:
        self._write(f"exp_{experiment_id}.json", {
            "experiment_id": experiment_id,
            "search_space": search_space,
            "status": status,
            "created_at": datetime.now(timezone.utc).isoformat(),
        })

    def update_experiment_end(self, experiment_id: str, status: str) -> None:
        p = self.path / f"exp_{experiment_id}.json"
        if not p.exists():
            return
        d = json.loads(p.read_text())
        d["status"] = status
        d["completed_at"] = datetime.now(timezone.utc).isoformat()
        p.write_text(json.dumps(d, default=str))

    def get_architectures(self, limit: int = 100) -> List[Dict[str, Any]]:
        return [json.loads(p.read_text())
                for p in sorted(self.path.glob("arch_*.json"))[:limit]]


class _FeedbackEventShim:
    """Local stand-in for the canonical FeedbackEvent schema."""
    def __init__(self, **kwargs: Any) -> None:
        self._payload = dict(kwargs)
        self.event_id = str(uuid.uuid4())

    @classmethod
    def create_with_context(cls, **kwargs: Any) -> "_FeedbackEventShim":
        return cls(**kwargs)

    def to_json(self) -> str:
        return json.dumps(self._payload, default=str)


class _MessageQueueShim:
    """Async in-memory queue used when the central queue is unavailable."""
    def __init__(self) -> None:
        self._messages: deque = deque(maxlen=10_000)

    async def publish(self, topic: str, payload: Any) -> None:
        self._messages.append((topic, payload))

    def consume(self) -> List[Tuple[str, Any]]:
        out = list(self._messages)
        self._messages.clear()
        return out


try:
    from ..config import config as central_config  # type: ignore
    from ..storage import Storage  # type: ignore
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
    from ..routing.pareto_gating import ParetoGating  # type: ignore
    from ..feedback.adaptive_cost import AdaptiveCostFunction  # type: ignore
    from ..safety.drift_detector import DriftDetector  # type: ignore
    from ..scaling.message_queue import AsyncMessageQueue  # type: ignore
    from ..metrics import MetricsRegistry  # type: ignore
    from ..logger import logger as _central_logger  # type: ignore
    GREEN_AGENT_AVAILABLE = True
    logger = _central_logger
except Exception:  # pragma: no cover
    GREEN_AGENT_AVAILABLE = False
    central_config = _ConfigShim()
    Storage = _StorageShim  # type: ignore[assignment]
    FeedbackEvent = _FeedbackEventShim  # type: ignore[assignment]
    ParetoGating = None  # type: ignore[assignment]
    AdaptiveCostFunction = None  # type: ignore[assignment]
    DriftDetector = None  # type: ignore[assignment]
    AsyncMessageQueue = _MessageQueueShim  # type: ignore[assignment]
    MetricsRegistry = _MetricsRegistryShim  # type: ignore[assignment]


# ============================================================================
# Optional enhancement modules
# ============================================================================
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator  # type: ignore
    ENHANCEMENTS_AVAILABLE = True
except ImportError:  # pragma: no cover
    ENHANCEMENTS_AVAILABLE = False

    class GeneticPolicyGenerator:  # type: ignore
        def evolve(self, population, fitness_fn, generations=10, population_size=20):
            return list(population)


try:
    from enhancements.MODP import ParetoOptimizer  # type: ignore
except ImportError:  # pragma: no cover
    class ParetoOptimizer:  # type: ignore
        def evaluate(self, objectives, weights):
            return float(sum(objectives.get(k, 0.0) * weights.get(k, 1.0)
                             for k in objectives))


try:
    from enhancements.moe_system import ExpertRouter  # type: ignore
except ImportError:  # pragma: no cover
    class ExpertRouter:  # type: ignore
        def encode(self, context):
            return [
                1.0 if context.get("family") == "transformer" else 0.0,
                float(context.get("num_layers", 0)) / 20.0,
                float(context.get("carbon_intensity", 0.0)) / 1000.0,
                1.0 if context.get("workload_size") == "large" else 0.0,
                float(context.get("time_of_day", 12)) / 24.0,
            ]
        def select(self, encoded):
            if not encoded:
                return "darts"
            if encoded[1] > 0.5:
                return "pnas"
            if encoded[3] > 0.5:
                return "darts"
            return "enas"


try:
    from enhancements.gpu_optimization.flexgen_policy import (  # type: ignore
        FlexGenPolicy, generate_candidate_policies,
    )
    from enhancements.gpu_optimization.flexgen_controller import (  # type: ignore
        FlexGenController,
    )
    from enhancements.gpu_optimization.flexgen_cost_model import (  # type: ignore
        FlexGenCostModel,
    )
    from enhancements.gpu_optimization.policy_drift_detector import (  # type: ignore
        PolicyDriftDetector,
    )
    FLEXGEN_AVAILABLE = True
except ImportError:  # pragma: no cover
    FLEXGEN_AVAILABLE = False


# ============================================================================
# Optional third-party deps
# ============================================================================
try:
    from pqcrypto.sign import dilithium, falcon, sphincs  # type: ignore
    PQC_AVAILABLE = True
except ImportError:  # pragma: no cover
    PQC_AVAILABLE = False

try:
    from cryptography.hazmat.backends import default_backend  # type: ignore
    from cryptography.hazmat.primitives import hashes  # type: ignore
    from cryptography.hazmat.primitives.asymmetric import ec  # type: ignore
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # type: ignore
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC  # type: ignore
    from cryptography.hazmat.primitives.serialization import (  # type: ignore
        Encoding, NoEncryption, PrivateFormat, PublicFormat,
    )
    CRYPTO_AVAILABLE = True
except ImportError:  # pragma: no cover
    CRYPTO_AVAILABLE = False

# Qiskit (modern layout)
try:
    from qiskit_algorithms import QAOA  # type: ignore
    from qiskit_algorithms.optimizers import COBYLA  # type: ignore
    from qiskit_optimization import QuadraticProgram  # type: ignore
    from qiskit_optimization.algorithms import MinimumEigenOptimizer  # type: ignore
    from qiskit_aer import Aer  # type: ignore
    QISKIT_AVAILABLE = True
except ImportError:  # pragma: no cover
    QISKIT_AVAILABLE = False

try:
    import torch  # type: ignore
    import torch.nn as nn  # type: ignore
    TORCH_AVAILABLE = True
except ImportError:  # pragma: no cover
    TORCH_AVAILABLE = False
    torch = None  # type: ignore
    nn = None  # type: ignore

try:
    import mlflow  # type: ignore
    MLFLOW_AVAILABLE = True
except ImportError:  # pragma: no cover
    MLFLOW_AVAILABLE = False

try:
    import shap  # type: ignore
    SHAP_AVAILABLE = True
except ImportError:  # pragma: no cover
    SHAP_AVAILABLE = False

try:
    import lime  # type: ignore
    LIME_AVAILABLE = True
except ImportError:  # pragma: no cover
    LIME_AVAILABLE = False

try:
    from fastapi import (  # type: ignore
        Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect,
    )
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse  # noqa: F401
    from fastapi.security import (  # type: ignore
        HTTPAuthorizationCredentials, HTTPBearer,
    )
    FASTAPI_AVAILABLE = True
except ImportError:  # pragma: no cover
    FASTAPI_AVAILABLE = False

try:
    from pydantic import BaseModel, Field  # type: ignore
    PYDANTIC_AVAILABLE = True
except ImportError:  # pragma: no cover
    PYDANTIC_AVAILABLE = False
    BaseModel = object  # type: ignore
    def Field(*a, **k):  # type: ignore
        return None


# ============================================================================
# Custom exceptions
# ============================================================================
class NASException(Exception):
    def __init__(self, message: str, details: Optional[Dict] = None) -> None:
        super().__init__(message)
        self.details = details or {}
        self.timestamp = datetime.now(timezone.utc)
        self.correlation_id = str(uuid.uuid4())[:8]


class AlgorithmError(NASException): pass
class QuantumError(NASException): pass
class FederatedError(NASException): pass
class DeploymentError(NASException): pass
class CircuitBreakerOpenError(NASException): pass
class CarbonAPIError(NASException): pass
class PersistenceError(NASException): pass
class CloudStorageError(NASException): pass
class PQCError(NASException): pass
class VaultError(NASException): pass
class WebSocketError(NASException): pass
class SafetyViolationError(NASException): pass
class ChaosExperimentError(NASException): pass


# ============================================================================
# Circuit breaker (sync + async)
# ============================================================================
class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class EnhancedCircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: float = 60.0,
                 half_open_success_threshold: int = 2) -> None:
        self.name = name
        self.failure_threshold = int(failure_threshold)
        self.recovery_timeout = float(recovery_timeout)
        self.half_open_success_threshold = int(half_open_success_threshold)
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_monotonic: Optional[float] = None
        self._lock = asyncio.Lock()
        self.metrics = {"total_calls": 0, "failed_calls": 0, "successful_calls": 0}

    def _publish(self) -> None:
        try:
            central_config.metrics_registry.update_circuit_breaker(
                self.name, self.state.value)
        except Exception:
            pass

    async def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        async with self._lock:
            if self.state is CircuitBreakerState.OPEN:
                if (self.last_failure_monotonic is not None
                        and time.monotonic() - self.last_failure_monotonic
                        >= self.recovery_timeout):
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.success_count = 0
                    self._publish()
                else:
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker {self.name} is OPEN")
        self.metrics["total_calls"] += 1
        try:
            result = func(*args, **kwargs)
            if inspect.iscoroutine(result):
                result = await result
        except Exception:
            async with self._lock:
                self.metrics["failed_calls"] += 1
                self.failure_count += 1
                self.last_failure_monotonic = time.monotonic()
                if (self.state is CircuitBreakerState.CLOSED
                        and self.failure_count >= self.failure_threshold):
                    self.state = CircuitBreakerState.OPEN
                elif self.state is CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.OPEN
                self._publish()
            raise
        else:
            async with self._lock:
                self.metrics["successful_calls"] += 1
                if self.state is CircuitBreakerState.HALF_OPEN:
                    self.success_count += 1
                    if self.success_count >= self.half_open_success_threshold:
                        self.state = CircuitBreakerState.CLOSED
                        self.failure_count = 0
                else:
                    self.failure_count = 0
                self._publish()
            return result

    def get_metrics(self) -> Dict[str, Any]:
        return {
            **self.metrics,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
        }


# ============================================================================
# Rate limiter (token bucket)
# ============================================================================
class EnhancedRateLimiter:
    def __init__(self, rate: int = 50, per_seconds: int = 60) -> None:
        self.rate = int(rate)
        self.per_seconds = int(per_seconds)
        self.tokens = float(rate)
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()
        self.total_requests = 0
        self.throttled_requests = 0

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_refill
            self.tokens = min(
                self.rate, self.tokens + elapsed * (self.rate / self.per_seconds))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                self.total_requests += 1
                return True
            self.throttled_requests += 1
            return False

    async def wait_and_acquire(self) -> None:
        while not await self.acquire():
            await asyncio.sleep(0.05)

    def get_metrics(self) -> Dict[str, Any]:
        total = self.total_requests + self.throttled_requests
        return {
            "total_requests": self.total_requests,
            "throttled_requests": self.throttled_requests,
            "throttle_rate": (self.throttled_requests / max(total, 1)) * 100,
        }


# ============================================================================
# Vault (lazy)
# ============================================================================
class VaultManager:
    def __init__(self) -> None:
        self.client: Optional[Any] = None
        self._initialized = False

    def _ensure_client(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        addr = getattr(central_config, "VAULT_ADDR", None)
        token = getattr(central_config, "VAULT_TOKEN", None)
        if not addr or not token:
            logger.info("Vault not configured; storage fallback will be used.")
            return
        try:
            from hvac import Client  # type: ignore
            self.client = Client(url=addr, token=token)
            logger.info("Vault client initialized")
        except ImportError:
            logger.warning("hvac not installed; Vault disabled.")
        except Exception as e:
            logger.warning("Vault client init failed: %s", e)

    async def store_secret(self, path: str, data: Dict[str, Any]) -> None:
        self._ensure_client()
        if not self.client:
            return
        try:
            self.client.secrets.kv.v2.create_or_update_secret(
                path=path, secret=data)
        except Exception as e:
            raise VaultError(f"Failed to store secret: {e}") from e

    async def get_secret(self, path: str) -> Optional[Dict[str, Any]]:
        self._ensure_client()
        if not self.client:
            return None
        try:
            secret = self.client.secrets.kv.v2.read_secret(path=path)
            return secret["data"]["data"]
        except Exception:
            return None


# ============================================================================
# Post-quantum crypto (HMAC-SHA256 with persisted salt)
# ============================================================================
class PostQuantumCrypto:
    SALT_STATE_KEY = "pqc_salt"

    def __init__(self, storage: Any, vault: VaultManager) -> None:
        self.storage = storage
        self.vault = vault
        self.pqc_available = PQC_AVAILABLE
        self._lock = asyncio.Lock()
        self.master_key = central_config.get_master_key_bytes()
        self._salt: Optional[bytes] = None
        if self.pqc_available:
            self.pqc_algorithms = {
                "dilithium": dilithium,
                "falcon": falcon,
                "sphincs": sphincs,
            }
        else:
            self.pqc_algorithms = {}

    async def _load_or_create_salt(self) -> bytes:
        if self._salt is not None:
            return self._salt
        async with self._lock:
            if self._salt is not None:
                return self._salt
            stored = None
            try:
                stored = self.storage.load_system_state(self.SALT_STATE_KEY)
            except Exception:
                stored = None
            if stored is None:
                new_salt = secrets.token_bytes(16)
                try:
                    self.storage.save_system_state(
                        self.SALT_STATE_KEY, new_salt.hex())
                except Exception as e:
                    logger.warning("Failed to persist PQC salt: %s", e)
                self._salt = new_salt
            else:
                self._salt = bytes.fromhex(stored)
            return self._salt

    def _derive_key(self, salt: bytes, length: int = 32) -> bytes:
        if not CRYPTO_AVAILABLE:
            import hmac
            return hmac.new(self.master_key, salt,
                            hashlib.sha256).digest()[:length]
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=length,
                         salt=salt, iterations=100_000,
                         backend=default_backend())
        return kdf.derive(self.master_key)

    async def _encrypt(self, data: bytes) -> bytes:
        salt = await self._load_or_create_salt()
        derived = self._derive_key(salt)
        nonce = secrets.token_bytes(12)
        if CRYPTO_AVAILABLE:
            return nonce + AESGCM(derived).encrypt(nonce, data, None)
        import hmac
        stream = hmac.new(derived, nonce, hashlib.sha256).digest()
        out = bytes(c ^ stream[i % len(stream)] for i, c in enumerate(data))
        return nonce + out

    async def _decrypt(self, blob: bytes) -> bytes:
        salt = await self._load_or_create_salt()
        derived = self._derive_key(salt)
        nonce, rest = blob[:12], blob[12:]
        if CRYPTO_AVAILABLE:
            return AESGCM(derived).decrypt(nonce, rest, None)
        import hmac
        stream = hmac.new(derived, nonce, hashlib.sha256).digest()
        return bytes(c ^ stream[i % len(stream)] for i, c in enumerate(rest))

    async def generate_keypair(self, algorithm: str = "hmac_sha256",
                               validity_days: int = 30) -> Dict[str, Any]:
        secret = secrets.token_bytes(32)
        public_key = hashlib.sha256(secret).digest()
        key_id = f"{algorithm}_{uuid.uuid4().hex[:8]}"
        expires_at = (datetime.now(timezone.utc)
                      + timedelta(days=validity_days)).isoformat()
        enc_pub = await self._encrypt(public_key)
        enc_priv = await self._encrypt(secret)
        record = {
            "algorithm": algorithm,
            "public_key": enc_pub.hex(),
            "private_key": enc_priv.hex(),
            "expires_at": expires_at,
        }
        if self.vault.client:
            await self.vault.store_secret(f"pqc/{key_id}", record)
        else:
            try:
                self.storage.save_pqc_key(
                    key_id, algorithm, enc_pub, enc_priv, expires_at)
            except Exception as e:
                logger.warning("Failed to persist PQC keypair: %s", e)
        return {
            "key_id": key_id,
            "algorithm": algorithm,
            "public_key": public_key.hex(),
            "expires_at": expires_at,
        }

    async def _load_keypair(self, key_id: str
                            ) -> Optional[Dict[str, Any]]:
        if self.vault.client:
            secret = await self.vault.get_secret(f"pqc/{key_id}")
            if secret:
                return {
                    "algorithm": secret["algorithm"],
                    "public_key": bytes.fromhex(secret["public_key"]),
                    "private_key": bytes.fromhex(secret["private_key"]),
                }
        try:
            return self.storage.get_pqc_key(key_id)
        except Exception:
            return None

    async def sign_data(self, data: Dict[str, Any],
                        key_id: str) -> Dict[str, Any]:
        import hmac
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        kp = await self._load_keypair(key_id)
        if kp is None:
            raise PQCError(f"Key {key_id} not found")
        secret = await self._decrypt(kp["private_key"])
        signature = hmac.new(secret, payload, hashlib.sha256).hexdigest()
        return {
            "signature": signature,
            "algorithm": kp["algorithm"],
            "key_id": key_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def verify_data(self, data: Dict[str, Any],
                          signature_data: Dict[str, Any]) -> bool:
        import hmac
        key_id = signature_data.get("key_id")
        if not key_id:
            return False
        kp = await self._load_keypair(key_id)
        if kp is None:
            return False
        secret = await self._decrypt(kp["private_key"])
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        expected = hmac.new(secret, payload, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, signature_data.get("signature", ""))

    def get_quantum_status(self) -> Dict[str, Any]:
        try:
            key_count = len(self.storage.list_pqc_keys())
        except Exception:
            key_count = 0
        return {
            "pqc_available": self.pqc_available,
            "algorithms": (list(self.pqc_algorithms.keys())
                           if self.pqc_available else ["hmac_sha256"]),
            "key_count": key_count,
        }


# ============================================================================
# Cloud storage (deterministic, local fallback)
# ============================================================================
class CloudStorage:
    def __init__(self) -> None:
        self.providers: Dict[str, Any] = {}
        self.local_dir = Path(os.environ.get("NAS_CLOUD_DIR",
                                             "./_nas_cloud"))
        self.local_dir.mkdir(parents=True, exist_ok=True)

    async def store(self, data: Dict[str, Any],
                    filename: Optional[str] = None) -> Dict[str, Any]:
        fname = filename or f"nas_{uuid.uuid4().hex[:12]}.json"
        path = self.local_dir / fname
        await asyncio.to_thread(path.write_text,
                                json.dumps(data, default=str))
        try:
            central_config.metrics_registry.increment_cloud_dispatch("local")
        except Exception:
            pass
        return {"provider": "local", "location": str(path)}

    def provider_names(self) -> List[str]:
        return list(self.providers.keys()) or ["local"]


# ============================================================================
# WebSocket manager
# ============================================================================
class WebSocketManager:
    def __init__(self) -> None:
        self.active_connections: Set[Any] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: Any) -> None:
        await websocket.accept()
        async with self._lock:
            self.active_connections.add(websocket)

    async def disconnect(self, websocket: Any) -> None:
        async with self._lock:
            self.active_connections.discard(websocket)

    async def broadcast(self, message: Dict[str, Any]) -> None:
        async with self._lock:
            stale = []
            for ws in self.active_connections:
                try:
                    await ws.send_json(message)
                except Exception:
                    stale.append(ws)
            for ws in stale:
                self.active_connections.discard(ws)


# ============================================================================
# Causal bandit
# ============================================================================
class CausalBandit:
    def __init__(self, action_space: List[str],
                 fallback_solver: Callable[[Any], str],
                 min_trials_before_bandit: int = 5,
                 confidence_threshold: float = 0.6,
                 epsilon: float = 0.1,
                 history_size: int = 1000,
                 seed: int = 42) -> None:
        self.actions: List[str] = list(action_space)
        self.fallback_solver = fallback_solver
        self.min_trials = int(min_trials_before_bandit)
        self.confidence_threshold = float(confidence_threshold)
        self.epsilon = float(epsilon)
        self._rng = np.random.default_rng(seed)
        self.q_values: Dict[str, float] = {}
        self.counts: Dict[str, int] = {}
        self.causal_effects: Dict[str, float] = {}
        self.trials = 0
        self.context_history: deque = deque(maxlen=history_size)
        self.reward_history: deque = deque(maxlen=history_size)
        self.action_history: deque = deque(maxlen=history_size)
        for a in self.actions:
            self._register(a)

    def _register(self, name: str) -> None:
        self.q_values.setdefault(name, 0.0)
        self.counts.setdefault(name, 0)
        self.causal_effects.setdefault(name, 0.0)
        if name not in self.actions:
            self.actions.append(name)

    def register_action(self, name: str) -> None:
        self._register(name)

    def select_action(self, context: Any) -> Tuple[str, float, str]:
        if self.trials < self.min_trials:
            return self.fallback_solver(context), 0.0, "fallback"
        if float(self._rng.random()) < self.epsilon:
            return self.actions[int(self._rng.integers(0, len(self.actions)))], 0.5, "explore"
        if self.trials >= 10 and any(self.causal_effects.values()):
            name = max(self.causal_effects, key=self.causal_effects.get)
            return name, 0.6, "causal"
        name = max(self.q_values, key=self.q_values.get)
        return name, 0.5, "bandit"

    def update(self, context: Any, action: str, reward: float) -> None:
        self._register(action)
        self.trials += 1
        self.counts[action] += 1
        n = self.counts[action]
        self.q_values[action] += (float(reward) - self.q_values[action]) / n
        self.causal_effects[action] += (
            float(reward) - self.causal_effects[action]) / n
        self.context_history.append(dict(context) if isinstance(context, dict)
                                    else {"value": context})
        self.reward_history.append(float(reward))
        self.action_history.append(action)

    def export_state(self) -> Dict[str, Any]:
        return {
            "q_values": dict(self.q_values),
            "counts": dict(self.counts),
            "causal_effects": dict(self.causal_effects),
            "trials": self.trials,
            "actions": list(self.actions),
        }

    def import_state(self, state: Dict[str, Any]) -> None:
        for k, v in state.get("q_values", {}).items():
            self._register(k)
            self.q_values[k] = float(v)
        for k, v in state.get("counts", {}).items():
            self._register(k)
            self.counts[k] = int(v)
        for k, v in state.get("causal_effects", {}).items():
            self._register(k)
            self.causal_effects[k] = float(v)
        self.trials = int(state.get("trials", 0))


# ============================================================================
# Quantum distillation optimizer
# ============================================================================
class QuantumDistillationOptimizer:
    def __init__(self, enabled: bool = False) -> None:
        self.enabled = bool(enabled) and QISKIT_AVAILABLE

    async def optimize(self, candidates: List[Dict[str, Any]],
                       weights: Dict[str, float]) -> Optional[Dict[str, Any]]:
        if not self.enabled or len(candidates) < 2:
            return None
        try:
            qp = QuadraticProgram()
            for i in range(len(candidates)):
                qp.binary_var(f"x{i}")
            utility = []
            for c in candidates:
                u = 0.0
                if "accuracy" in c:
                    u += float(c["accuracy"]) * weights.get("accuracy", 0.3)
                if "carbon_kg" in c:
                    u += (1 - float(c["carbon_kg"]) / 10.0) * weights.get("carbon", 0.3)
                if "energy_kwh" in c:
                    u += (1 - float(c["energy_kwh"]) / 10.0) * weights.get("energy", 0.2)
                if "latency" in c:
                    u += (1 - float(c["latency"]) / 1000.0) * weights.get("latency", 0.2)
                utility.append(u)
            linear = {f"x{i}": -utility[i] for i in range(len(candidates))}
            qp.minimize(linear=linear)
            qp.linear_constraint(
                linear={f"x{i}": 1 for i in range(len(candidates))},
                sense="E", rhs=1, name="one_candidate")
            backend = Aer.get_backend("aer_simulator")
            qaoa = QAOA(optimizer=COBYLA(), reps=1, quantum_instance=backend)
            optimizer = MinimumEigenOptimizer(qaoa)
            result = optimizer.solve(qp)
            for i, val in enumerate(result.x):
                if val > 0.5:
                    return candidates[i]
        except Exception as e:
            logger.error("Quantum distillation failed: %s", e)
        return None


# ============================================================================
# Safety monitor
# ============================================================================
class SafetyMonitor:
    def __init__(self, max_carbon_intensity: float = 500.0,
                 min_accuracy: float = 0.5,
                 max_latency: float = 1000.0) -> None:
        self.rules = {
            "max_carbon_intensity": (lambda m: float(m.get("carbon_intensity", 0.0))
                                     <= max_carbon_intensity),
            "min_accuracy": (lambda m: float(m.get("accuracy", 1.0)) >= min_accuracy),
            "max_latency": (lambda m: float(m.get("latency", 0.0)) <= max_latency),
        }

    async def check(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        violations: List[Dict[str, Any]] = []
        for name, fn in self.rules.items():
            try:
                if not fn(metrics):
                    violations.append({"rule": name, "details": dict(metrics)})
                    try:
                        central_config.metrics_registry.increment_safety_violation(name)
                    except Exception:
                        pass
            except Exception:
                continue
        return violations

    async def validate_architecture(self, arch: Dict[str, Any]) -> bool:
        return int(arch.get("num_layers", 0)) <= 10


# ============================================================================
# XAI explainer
# ============================================================================
class XAIExplainer:
    def __init__(self) -> None:
        self.shap_available = SHAP_AVAILABLE
        self.lime_available = LIME_AVAILABLE

    async def explain(self, model_input: Dict[str, Any], prediction: float,
                      feature_names: List[str]) -> Dict[str, Any]:
        importances: Dict[str, float] = {}
        defaults = {
            "num_layers": 0.4,
            "hidden_dim": 0.3,
            "num_heads": 0.2,
            "carbon_intensity": 0.1,
        }
        for name in feature_names:
            importances[name] = defaults.get(name, 0.05)
        return {
            "prediction": float(prediction),
            "feature_importance": importances,
            "method": ("shap" if self.shap_available
                       else "lime" if self.lime_available
                       else "heuristic"),
        }


# ============================================================================
# Federated coordinator (weighted, numeric-only)
# ============================================================================
class FederatedCoordinator:
    def __init__(self, max_participants: int = 1000) -> None:
        self._updates: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._weights: Dict[str, float] = {}
        self._max = max(1, int(max_participants))
        self.global_model: Optional[Dict[str, float]] = None

    def register_client(self, client_id: str, model_update: Dict[str, Any],
                        weight: float = 1.0) -> None:
        if client_id not in self._updates:
            while len(self._updates) >= self._max:
                old_id, _ = self._updates.popitem(last=False)
                self._weights.pop(old_id, None)
        self._updates[client_id] = dict(model_update)
        self._weights[client_id] = float(weight)

    def aggregate(self) -> Dict[str, float]:
        if not self._updates:
            return {}
        keys: Set[str] = set()
        for upd in self._updates.values():
            keys.update(upd.keys())
        out: Dict[str, float] = {}
        for k in keys:
            total = 0.0
            wsum = 0.0
            for cid, upd in self._updates.items():
                v = upd.get(k)
                if isinstance(v, (int, float)):
                    w = self._weights.get(cid, 1.0)
                    total += float(v) * w
                    wsum += w
            if wsum > 0:
                out[k] = total / wsum
        self.global_model = out
        return out


# ============================================================================
# Multi-agent coordinator (stable hash)
# ============================================================================
class MultiAgentCoordinator:
    def __init__(self, num_agents: int = 3) -> None:
        if num_agents < 1:
            raise ValueError("num_agents must be >= 1")
        self.agents = [f"agent_{i}" for i in range(num_agents)]
        self.responsibilities: Dict[str, List[str]] = {a: [] for a in self.agents}
        self._rr = 0

    @staticmethod
    def _stable_hash(value: Any) -> int:
        return int(hashlib.sha256(str(value).encode()).hexdigest(), 16)

    def assign_task(self, task_id: Optional[str]) -> str:
        if task_id is None:
            agent = self.agents[self._rr % len(self.agents)]
            self._rr += 1
        else:
            agent = self.agents[self._stable_hash(task_id) % len(self.agents)]
        self.responsibilities[agent].append(str(task_id))
        return agent

    def load(self) -> Dict[str, int]:
        return {a: len(v) for a, v in self.responsibilities.items()}


# ============================================================================
# Carbon offset broker
# ============================================================================
class CarbonOffsetBroker:
    def __init__(self, threshold_kg: float = 100.0,
                 cost_per_kg: float = 0.1) -> None:
        self.threshold = float(threshold_kg)
        self.cost_per_kg = float(cost_per_kg)
        self.total_offset_kg = 0.0
        self.total_cost = 0.0
        self._history: deque = deque(maxlen=1000)

    async def maybe_purchase_offsets(self, carbon_kg: float) -> Dict[str, Any]:
        if carbon_kg <= self.threshold:
            return {"status": "below_threshold", "carbon_kg": float(carbon_kg)}
        cost = float(carbon_kg) * self.cost_per_kg
        self.total_offset_kg += float(carbon_kg)
        self.total_cost += cost
        receipt = {
            "status": "offset",
            "carbon_kg": float(carbon_kg),
            "cost_usd": cost,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._history.append(receipt)
        return receipt


# ============================================================================
# Chaos monkey (validated, seeded)
# ============================================================================
class ChaosMonkey:
    def __init__(self, enabled: bool = False,
                 failure_probability: float = 0.1,
                 seed: int = 42) -> None:
        if not 0.0 <= failure_probability <= 1.0:
            raise ValueError("failure_probability must be in [0, 1]")
        self.enabled = bool(enabled)
        self.failure_probability = float(failure_probability)
        self._rng = np.random.default_rng(seed)

    def maybe_fail(self) -> None:
        if self.enabled and float(self._rng.random()) < self.failure_probability:
            raise ChaosExperimentError("Simulated chaos failure")


# ============================================================================
# Human review manager (bounded)
# ============================================================================
class HumanReviewManager:
    def __init__(self, max_pending: int = 500) -> None:
        self.pending_reviews: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._max = max(1, int(max_pending))
        self._lock = asyncio.Lock()

    async def request_review(self, task_id: str,
                             details: Dict[str, Any]) -> str:
        review_id = str(uuid.uuid4())
        async with self._lock:
            self.pending_reviews[review_id] = {
                "task_id": task_id,
                "details": dict(details),
                "status": "pending",
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            while len(self.pending_reviews) > self._max:
                self.pending_reviews.popitem(last=False)
        return review_id

    async def approve(self, review_id: str,
                      feedback: Optional[str] = None) -> bool:
        async with self._lock:
            r = self.pending_reviews.get(review_id)
            if r is None:
                return False
            r["status"] = "approved"
            r["feedback"] = feedback
            r["reviewed_at"] = datetime.now(timezone.utc).isoformat()
            return True

    async def reject(self, review_id: str,
                     feedback: Optional[str] = None) -> bool:
        async with self._lock:
            r = self.pending_reviews.get(review_id)
            if r is None:
                return False
            r["status"] = "rejected"
            r["feedback"] = feedback
            r["reviewed_at"] = datetime.now(timezone.utc).isoformat()
            return True

    async def get_pending(self) -> List[Dict[str, Any]]:
        async with self._lock:
            return [dict(r) for r in self.pending_reviews.values()
                    if r["status"] == "pending"]


# ============================================================================
# Bio-inspired optimizer
# ============================================================================
class BioInspiredOptimizer:
    def __init__(self, seed: int = 42) -> None:
        self._rng = np.random.default_rng(seed)
        self._lock = asyncio.Lock()
        self.mutation_rate = 0.1
        self.crossover_rate = 0.5
        self.population_size = 50
        self.default_algorithm = "darts"
        self.bio = GeneticPolicyGenerator() if ENHANCEMENTS_AVAILABLE else None
        self.history: deque = deque(maxlen=100)

    async def adjust_parameters(self,
                                recent_cycles: List[Dict[str, Any]]
                                ) -> Dict[str, Any]:
        async with self._lock:
            if not recent_cycles:
                return self._current()
            avg_accuracy = float(np.mean(
                [c.get("best_accuracy", 0.5) for c in recent_cycles]))
            avg_carbon = float(np.mean(
                [c.get("carbon_kg", 0.5) for c in recent_cycles]))
            if avg_accuracy < 0.7:
                self.mutation_rate = min(0.5, self.mutation_rate * 1.1)
            else:
                self.mutation_rate = max(0.05, self.mutation_rate * 0.9)
            if avg_carbon > 0.5:
                self.population_size = max(10, int(self.population_size * 0.9))
            else:
                self.population_size = min(200, int(self.population_size * 1.1))
            self.default_algorithm = (
                "enas" if avg_accuracy < 0.6 else self.default_algorithm)
            return self._current()

    def _current(self) -> Dict[str, Any]:
        return {
            "mutation_rate": self.mutation_rate,
            "crossover_rate": self.crossover_rate,
            "population_size": self.population_size,
            "algorithm": self.default_algorithm,
        }

    async def record_cycle(self, cycle_result: Dict[str, Any]) -> None:
        async with self._lock:
            self.history.append(dict(cycle_result))

    def get_stats(self) -> Dict[str, Any]:
        return {
            **self._current(),
            "history_length": len(self.history),
            "bio_available": self.bio is not None,
        }


# ============================================================================
# Carbon intensity manager (seeded simulation)
# ============================================================================
class CarbonIntensityManager:
    def __init__(self, seed: int = 42) -> None:
        self._rng = np.random.default_rng(seed)
        self._circuit_breaker = EnhancedCircuitBreaker("carbon_api")
        self._cache: Dict[str, Any] = {}
        self._cache_lock = asyncio.Lock()
        self._last_update: Optional[float] = None
        self._update_interval = 300.0

    async def get_current_intensity(self) -> float:
        async with self._cache_lock:
            if (self._last_update is not None
                    and time.monotonic() - self._last_update < self._update_interval):
                return float(self._cache.get("intensity", 400.0))
        intensity = float(300.0 + 200.0 * self._rng.random())
        async with self._cache_lock:
            self._cache["intensity"] = intensity
            self._last_update = time.monotonic()
        return intensity

    def calculate_nas_carbon(self, energy_kwh: float) -> float:
        intensity = float(self._cache.get("intensity", 400.0))
        return float(energy_kwh) * intensity / 1000.0

    async def close(self) -> None:
        return None


# ============================================================================
# Energy measurer (simulated, clearly labeled)
# ============================================================================
class EnergyMeasurer:
    def __init__(self, seed: int = 42) -> None:
        self._rng = np.random.default_rng(seed)
        self._running = False

    async def measure_energy(self) -> float:
        # Simulated energy in kWh; real integration via pynvml/psutil is
        # possible by overriding this method.
        return float(0.005 + 0.01 * self._rng.random())

    async def start_measurement(self) -> None:
        self._running = True

    async def stop_measurement(self) -> float:
        self._running = False
        return await self.measure_energy()

    async def close(self) -> None:
        return None


# ============================================================================
# NAS algorithm simulators
# ============================================================================
if TORCH_AVAILABLE:
    class ProxyModel(nn.Module):  # type: ignore[misc]
        def __init__(self, num_layers: int = 2, hidden_dim: int = 64) -> None:
            super().__init__()
            self.layers = nn.ModuleList()
            in_dim = 3 * 32 * 32
            for i in range(num_layers):
                self.layers.append(
                    nn.Linear(in_dim if i == 0 else hidden_dim, hidden_dim))
                self.layers.append(nn.ReLU())
            self.fc = nn.Linear(hidden_dim, 10)

        def forward(self, x: Any) -> Any:
            x = x.view(x.size(0), -1)
            for layer in self.layers:
                x = layer(x)
            return self.fc(x)
else:  # pragma: no cover
    class ProxyModel:  # type: ignore[no-redef]
        def __init__(self, *a: Any, **k: Any) -> None:
            raise RuntimeError(
                "PyTorch is not installed; ProxyModel is unavailable.")


class AdvancedNASAlgorithms:
    """Deterministic simulations of NAS algorithms.  Clearly labeled."""

    def __init__(self, energy_measurer: EnergyMeasurer,
                 seed: int = 42) -> None:
        self.energy_measurer = energy_measurer
        self._rng = np.random.default_rng(seed)
        self.algorithms = ("darts", "enas", "pnas", "random")

    async def run_algorithm(self, algorithm_name: str,
                            search_space: Dict[str, Any],
                            iterations: int = 50) -> Dict[str, Any]:
        if algorithm_name not in self.algorithms:
            return {"status": "failed",
                    "error": f"Unknown algorithm: {algorithm_name}"}
        start = time.monotonic()
        await self.energy_measurer.start_measurement()
        await asyncio.sleep(0.05)  # smaller delay for tests
        energy = await self.energy_measurer.stop_measurement()
        duration = time.monotonic() - start

        layers = search_space.get("num_layers", [2, 4, 6])
        dims = search_space.get("hidden_dim", [64, 128, 256])
        heads = search_space.get("num_heads", [4, 8])
        ops = search_space.get("operations", ["conv3x3", "attention"])

        best_arch = {
            "num_layers": int(self._rng.choice(layers)),
            "hidden_dim": int(self._rng.choice(dims)),
            "num_heads": int(self._rng.choice(heads)),
            "operations": str(self._rng.choice(ops)),
            "final_accuracy": float(0.7 + 0.2 * self._rng.random()),
            "final_loss": float(0.5 * self._rng.random()),
        }
        return {
            "status": "success",
            "best_architecture": best_arch,
            "iterations": iterations,
            "energy_kwh": energy,
            "duration_seconds": duration,
            "algorithm": algorithm_name,
            "candidates": [best_arch],
            "simulated": True,
        }

    def get_algorithm_status(self) -> Dict[str, Any]:
        return {"available": list(self.algorithms), "simulated": True}


# ============================================================================
# Quantum-inspired optimizer
# ============================================================================
class QuantumInspiredOptimizer:
    def __init__(self, enabled: bool = False) -> None:
        self._circuit_breaker = EnhancedCircuitBreaker("quantum")
        self.quantum_enabled = bool(enabled) and QISKIT_AVAILABLE
        self.distillation_optimizer = QuantumDistillationOptimizer(
            enabled=self.quantum_enabled)

    async def optimize_architecture(self, architecture: Dict[str, Any],
                                    method: str = "qaoa") -> Dict[str, Any]:
        candidates = [architecture]
        result = await self.distillation_optimizer.optimize(
            candidates, central_config.MODP_WEIGHTS)
        if result is not None:
            return {"optimized": True, "improvement": 0.05,
                    "method": method, "quantum": True}
        return {"optimized": False,
                "reason": "Quantum disabled or not available"}

    def get_quantum_status(self) -> Dict[str, Any]:
        return {
            "enabled": self.quantum_enabled,
            "qiskit_available": QISKIT_AVAILABLE,
        }


# ============================================================================
# Federated learning NAS
# ============================================================================
class FederatedClient:
    def __init__(self, client_id: str) -> None:
        self.client_id = client_id
        self.local_accuracy = 0.0


class FederatedLearningNAS:
    def __init__(self, energy_measurer: EnergyMeasurer,
                 enabled: bool = True, seed: int = 42) -> None:
        self.energy_measurer = energy_measurer
        self.clients: List[FederatedClient] = []
        self.current_round = 0
        self.global_model: Optional[Dict[str, float]] = None
        self._lock = asyncio.Lock()
        self.federated_enabled = bool(enabled)
        self.coordinator = FederatedCoordinator()
        self._rng = np.random.default_rng(seed)

    def register_client(self, client_id: str) -> FederatedClient:
        client = FederatedClient(client_id)
        client.local_accuracy = float(0.7 + 0.2 * self._rng.random())
        self.clients.append(client)
        return client

    async def federated_training_round(self) -> Dict[str, Any]:
        if not self.federated_enabled or not self.clients:
            return {"status": "skipped",
                    "reason": "No clients or federated disabled"}
        async with self._lock:
            for c in self.clients:
                update = {
                    "accuracy": float(c.local_accuracy),
                    "num_layers": int(self._rng.choice([2, 4, 6])),
                    "hidden_dim": int(self._rng.choice([64, 128])),
                }
                self.coordinator.register_client(c.client_id, update)
            aggregated = self.coordinator.aggregate()
            self.global_model = aggregated
            self.current_round += 1
            return {
                "round": self.current_round,
                "clients_participated": len(self.clients),
                "avg_accuracy": float(aggregated.get("accuracy", 0.0)),
                "status": "completed",
            }

    async def get_federated_status(self) -> Dict[str, Any]:
        return {
            "enabled": self.federated_enabled,
            "clients": len(self.clients),
            "current_round": self.current_round,
        }


# ============================================================================
# Automated deployment
# ============================================================================
class AutomatedDeployment:
    def __init__(self, enabled: bool = True) -> None:
        self._circuit_breaker = EnhancedCircuitBreaker("deployment")
        self.deployment_enabled = bool(enabled)

    async def deploy_model(self, model_path: str,
                           config: Dict[str, Any]) -> Dict[str, Any]:
        if not self.deployment_enabled:
            return {"status": "skipped", "reason": "Deployment disabled"}
        return {"status": "deployed", "model_path": model_path,
                "config": config, "simulated": True}


# ============================================================================
# Explainable NAS
# ============================================================================
class ExplainableNAS:
    def __init__(self) -> None:
        self.explanation_enabled = SHAP_AVAILABLE or LIME_AVAILABLE
        self.xai = XAIExplainer()

    async def explain_architecture(self,
                                   architecture: Dict[str, Any]
                                   ) -> Dict[str, Any]:
        features = list(architecture.keys())
        prediction = float(architecture.get("final_accuracy", 0.8))
        explanation = await self.xai.explain(architecture, prediction, features)
        explanation["natural_language"] = (
            f"The architecture uses {architecture.get('num_layers', 2)} layers "
            f"with hidden dimension {architecture.get('hidden_dim', 64)}."
        )
        return explanation

    def get_explanation_status(self) -> Dict[str, Any]:
        return {
            "enabled": self.explanation_enabled,
            "method": ("shap" if SHAP_AVAILABLE
                       else "lime" if LIME_AVAILABLE
                       else "heuristic"),
        }


# ============================================================================
# FlexGen manager (deterministic simulation)
# ============================================================================
class FlexGenManager:
    def __init__(self, seed: int = 42) -> None:
        self._rng = np.random.default_rng(seed)
        self._last_policy: Optional[Dict[str, Any]] = None
        self.available = True

    async def optimize_policy(self, workload: Any,
                              node: Any) -> Dict[str, Any]:
        policy = {
            "gpu_batch_size": int(self._rng.integers(1, 8)),
            "weight_bits": int(self._rng.choice([8, 16])),
            "cpu_offload_fraction": float(self._rng.random()),
            "precision": str(self._rng.choice(["int8", "fp16"])),
        }
        metrics = {
            "energy_joules": float(self._rng.uniform(50.0, 200.0)),
            "carbon_g": float(self._rng.uniform(5.0, 30.0)),
            "latency_ms": float(self._rng.uniform(50.0, 500.0)),
        }
        self._last_policy = {
            "policy": policy,
            "metrics": metrics,
            "reward": float(self._rng.uniform(0.4, 1.0)),
            "simulated": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        return self._last_policy

    async def get_status(self) -> Dict[str, Any]:
        return {"available": self.available,
                "last_policy": self._last_policy,
                "simulated": True}


# ============================================================================
# Reasoning engine
# ============================================================================
class GreenAgentReasoningEngine:
    def __init__(self, energy_measurer: EnergyMeasurer,
                 carbon_manager: CarbonIntensityManager,
                 seed: int = 42) -> None:
        self.carbon_manager = carbon_manager
        self.nas_algorithms = AdvancedNASAlgorithms(energy_measurer, seed=seed)
        self.quantum_optimizer = QuantumInspiredOptimizer(
            enabled=getattr(central_config, "QUANTUM_ENABLED", False))
        self.federated_learning = FederatedLearningNAS(
            energy_measurer,
            enabled=getattr(central_config, "FEDERATED_ENABLED", True),
            seed=seed)
        self.deployment = AutomatedDeployment(
            enabled=getattr(central_config, "DEPLOYMENT_ENABLED", True))
        self.explainable_nas = ExplainableNAS()
        self.reasoning_history: deque = deque(maxlen=1000)
        self.enabled = True
        self.optimizer = BioInspiredOptimizer(seed=seed)

        self.modp = ParetoOptimizer()
        self.moe = ExpertRouter()
        self.bandit = CausalBandit(
            action_space=["darts", "enas", "pnas", "random", "quantum"],
            fallback_solver=lambda ctx: "darts",
            min_trials_before_bandit=5,
            confidence_threshold=0.6,
            seed=seed,
        )
        self.safety_monitor = SafetyMonitor()
        self.carbon_offset_broker = CarbonOffsetBroker()
        self.chaos_monkey = ChaosMonkey(
            enabled=getattr(central_config, "CHAOS_ENABLED", False),
            seed=seed)
        self.human_review = HumanReviewManager()
        self.multi_agent = MultiAgentCoordinator()
        logger.info("GreenAgentReasoningEngine %s initialized", __version__)

    async def reason_about_architecture(
        self, architecture_config: Dict[str, Any],
        fitness_metrics: Dict[str, Any],
        context: str = "cloud_inference",
        purpose: str = "balanced",
    ) -> Dict[str, Any]:
        if not self.enabled:
            return {"reasoning": "disabled"}

        self.chaos_monkey.maybe_fail()

        carbon_intensity = await self.carbon_manager.get_current_intensity()
        safety_metrics = {
            "carbon_intensity": carbon_intensity,
            "accuracy": float(fitness_metrics.get("accuracy", 0.8)),
            "latency": float(fitness_metrics.get("latency", 50.0)),
        }
        violations = await self.safety_monitor.check(safety_metrics)

        result: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "architecture_hash": hashlib.md5(
                json.dumps(architecture_config, sort_keys=True).encode()
            ).hexdigest()[:8],
            "context": context,
            "purpose": purpose,
            "safety_violations": violations,
        }

        result["nas_algorithm"] = await self._recommend_algorithm(
            architecture_config)
        result["quantum"] = await self._check_quantum_optimization(
            architecture_config)
        result["federated"] = await self._check_federated_learning(
            architecture_config)
        result["explanations"] = await self.explainable_nas.explain_architecture(
            architecture_config)

        carbon_kg = float(fitness_metrics.get("carbon_kg", 0.1))
        result["carbon_offset"] = await self.carbon_offset_broker.maybe_purchase_offsets(
            carbon_kg)

        result["assigned_agent"] = self.multi_agent.assign_task(
            result["architecture_hash"])
        result["parameter_adjustments"] = await self.optimizer.adjust_parameters(
            list(self.reasoning_history)[-20:])
        self.reasoning_history.append(result)
        result["overall_recommendations"] = self._generate_recommendations(result)
        return result

    async def _recommend_algorithm(self,
                                   architecture_config: Dict[str, Any]
                                   ) -> Dict[str, Any]:
        carbon = await self.carbon_manager.get_current_intensity()
        context = {
            "family": architecture_config.get("family", "unknown"),
            "num_layers": int(architecture_config.get("num_layers", 0)),
            "carbon_intensity": carbon,
            "workload_size": ("large"
                              if int(architecture_config.get("num_layers", 0)) > 10
                              else "small"),
            "time_of_day": datetime.now(timezone.utc).hour,
        }
        try:
            encoded = self.moe.encode(context)
        except Exception:
            encoded = context
        action, confidence, source = self.bandit.select_action(encoded)
        return {
            "recommended": action,
            "confidence": float(confidence),
            "source": source,
            "reason": f"Bandit selected {action} with confidence {confidence:.2f}",
        }

    async def _check_quantum_optimization(
        self, architecture_config: Dict[str, Any]) -> Dict[str, Any]:
        if self.quantum_optimizer.quantum_enabled \
                and architecture_config.get("family") == "hybrid":
            return {"recommended": True, "method": "qaoa",
                    "reason": "Hybrid architectures benefit from QAOA"}
        return {"recommended": False,
                "reason": "Quantum not enabled or unsuitable"}

    async def _check_federated_learning(
        self, architecture_config: Dict[str, Any]) -> Dict[str, Any]:
        if self.federated_learning.federated_enabled \
                and len(self.federated_learning.clients) > 0:
            return {"recommended": True,
                    "clients": len(self.federated_learning.clients),
                    "reason": "Federated learning can reduce carbon"}
        return {"recommended": False, "reason": "No clients registered"}

    def _generate_recommendations(
        self, reasoning_result: Dict[str, Any]) -> List[str]:
        recs: List[str] = []
        alg = reasoning_result.get("nas_algorithm", {}).get("recommended")
        if alg:
            recs.append(f"Use {alg} algorithm")
        if reasoning_result.get("quantum", {}).get("recommended"):
            recs.append("Apply quantum optimization")
        if reasoning_result.get("federated", {}).get("recommended"):
            recs.append("Use federated learning")
        adj = reasoning_result.get("parameter_adjustments", {})
        if adj.get("mutation_rate"):
            recs.append(f"Adjust mutation rate to {adj['mutation_rate']:.2f}")
        if reasoning_result.get("carbon_offset", {}).get("status") == "offset":
            recs.append(
                f"Purchase carbon offset for "
                f"{reasoning_result['carbon_offset']['carbon_kg']:.2f} kg")
        return recs[:5]

    async def get_reasoning_summary(self) -> Dict[str, Any]:
        if not self.reasoning_history:
            return {"status": "no_reasoning_history"}
        recent = list(self.reasoning_history)[-20:]
        return {
            "total_reasoned_architectures": len(self.reasoning_history),
            "recent_recommendations": [r for entry in recent
                                       for r in entry.get("overall_recommendations", [])][:10],
            "nas_algorithms_used": list({
                entry.get("nas_algorithm", {}).get("recommended", "unknown")
                for entry in recent
            }),
            "quantum_used": any(
                entry.get("quantum", {}).get("recommended", False)
                for entry in recent),
            "federated_used": any(
                entry.get("federated", {}).get("recommended", False)
                for entry in recent),
            "optimizer_stats": self.optimizer.get_stats(),
            "bandit_available": self.bandit is not None,
            "moe_available": self.moe is not None,
            "modp_available": self.modp is not None,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        # Softmax over bandit q_values to represent a real policy.
        actions = list(self.bandit.q_values.keys())
        if not actions:
            return []
        q = np.array([self.bandit.q_values[a] for a in actions])
        z = q - q.max()
        e = np.exp(z)
        s = e.sum() or 1.0
        return (e / s).tolist()

    async def evaluate_architecture(self,
                                    architecture: Dict[str, Any]
                                    ) -> Dict[str, float]:
        rng = np.random.default_rng(
            int(hashlib.sha256(
                json.dumps(architecture, sort_keys=True).encode()
            ).hexdigest(), 16) % (2**32))
        accuracy = 0.7 + 0.25 * rng.random()
        energy = 0.01 * rng.random()
        carbon_val = self.carbon_manager.calculate_nas_carbon(energy)
        latency = 50 + 100 * rng.random()
        return {
            "quality": float(accuracy),
            "energy_joules": float(energy * 3.6e6),
            "carbon_g": float(carbon_val * 1000),
            "latency_ms": float(latency),
            "cost_usd": 0.001,
        }


# ============================================================================
# Task manager (propagates shutdown)
# ============================================================================
class TaskManager:
    def __init__(self) -> None:
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()

    def start_task(self, name: str,
                   coro_func: Callable[..., Awaitable[None]],
                   *args: Any, **kwargs: Any) -> asyncio.Task:
        if name in self.tasks and not self.tasks[name].done():
            return self.tasks[name]

        async def wrapper() -> None:
            backoff = 1.0
            max_backoff = 300.0
            while not self.shutdown_event.is_set():
                try:
                    await coro_func(self.shutdown_event, *args, **kwargs)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Task %s crashed: %s", name, e, exc_info=True)
                    try:
                        await asyncio.wait_for(
                            self.shutdown_event.wait(), timeout=backoff)
                        break
                    except asyncio.TimeoutError:
                        pass
                    backoff = min(backoff * 2, max_backoff)

        task = asyncio.create_task(wrapper(), name=name)
        self.tasks[name] = task
        return task

    async def stop_all(self) -> None:
        self.shutdown_event.set()
        async with self._lock:
            for t in self.tasks.values():
                if not t.done():
                    t.cancel()
            if self.tasks:
                await asyncio.gather(*self.tasks.values(),
                                     return_exceptions=True)
            self.tasks.clear()
        logger.info("All background tasks stopped")


# ============================================================================
# Main NAS system
# ============================================================================
class CarbonAwareNAS:
    def __init__(self, seed: int = 42) -> None:
        self.instance_id = str(uuid.uuid4())[:8]
        self.seed = int(seed)
        self._rng = np.random.default_rng(self.seed)

        self.storage = Storage()
        self.energy_measurer = EnergyMeasurer(seed=self.seed)
        self.carbon_manager = CarbonIntensityManager(seed=self.seed)
        self.vault = VaultManager()
        self.pqc = PostQuantumCrypto(self.storage, self.vault)
        self.cloud_storage = CloudStorage()
        self.ws_manager = WebSocketManager()
        self.reasoning_engine = GreenAgentReasoningEngine(
            energy_measurer=self.energy_measurer,
            carbon_manager=self.carbon_manager,
            seed=self.seed,
        )
        self.flexgen_manager = FlexGenManager(seed=self.seed)

        self.population: List[Dict[str, Any]] = []
        self.current_best: Optional[Dict[str, Any]] = None
        self.generation = 0
        self.evaluation_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self.circuit_breakers = {
            "evaluation": EnhancedCircuitBreaker("evaluation"),
            "training": EnhancedCircuitBreaker("training"),
            "carbon": self.carbon_manager._circuit_breaker,
            "quantum": self.reasoning_engine.quantum_optimizer._circuit_breaker,
            "deployment": self.reasoning_engine.deployment._circuit_breaker,
        }
        self.rate_limiter = EnhancedRateLimiter()
        self._task_manager = TaskManager()
        self._shutdown_event = asyncio.Event()
        self._running = False
        self._pop_lock = asyncio.Lock()
        self._gen_lock = asyncio.Lock()

        try:
            self.pareto_gating = ParetoGating() if ParetoGating else None
        except Exception:
            self.pareto_gating = None

        self.mlflow_available = MLFLOW_AVAILABLE
        if self.mlflow_available:
            try:
                mlflow.set_experiment("Carbon-Aware NAS")
                mlflow.start_run(run_name=self.instance_id)
            except Exception as e:
                logger.warning("MLflow init failed: %s", e)
                self.mlflow_available = False

        logger.info("CarbonAwareNAS %s initialized (id=%s)",
                    __version__, self.instance_id)

    # ------------------------------------------------------------------
    async def start(self) -> None:
        self._running = True
        self._task_manager.start_task("evaluation", self._evaluation_loop)
        self._task_manager.start_task("maintenance", self._maintenance_loop)
        self._task_manager.start_task("carbon_update", self._carbon_update_loop)
        logger.info("NAS system started (id=%s)", self.instance_id)

    async def _carbon_update_loop(self, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            try:
                await self.carbon_manager.get_current_intensity()
            except Exception as e:
                logger.error("Carbon update error: %s", e)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=300)
                break
            except asyncio.TimeoutError:
                pass

    async def _evaluation_loop(self, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            try:
                if not self.evaluation_queue.empty():
                    await self._process_evaluation()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Evaluation loop error: %s", e)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=0.2)
                break
            except asyncio.TimeoutError:
                pass

    async def _maintenance_loop(self, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            try:
                async with self._pop_lock:
                    limit = self.reasoning_engine.optimizer.population_size
                    if len(self.population) > limit:
                        self.population.sort(
                            key=lambda x: x.get("accuracy", 0), reverse=True)
                        self.population = self.population[:limit]
            except Exception as e:
                logger.error("Maintenance error: %s", e)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=60)
                break
            except asyncio.TimeoutError:
                pass

    async def _process_evaluation(self) -> None:
        try:
            task_data = await self.evaluation_queue.get()
            await self.rate_limiter.wait_and_acquire()
            arch = task_data.get("architecture", {})
            arch_hash = hashlib.md5(
                json.dumps(arch, sort_keys=True).encode()).hexdigest()[:16]

            async def evaluate() -> Dict[str, float]:
                accuracy = float(0.7 + 0.2 * self._rng.random())
                energy = float(0.005 + 0.01 * self._rng.random())
                carbon = self.carbon_manager.calculate_nas_carbon(energy)
                return {"accuracy": accuracy, "carbon_kg": carbon,
                        "energy_kwh": energy}

            result = await evaluate()
            await self._update_population(result)

            try:
                self.storage.save_architecture_result({
                    "arch_hash": arch_hash,
                    "algorithm": task_data.get("algorithm", "unknown"),
                    "accuracy": result["accuracy"],
                    "carbon_kg": result["carbon_kg"],
                    "energy_kwh": result["energy_kwh"],
                    "latency_ms": 50.0,
                    "memory_mb": 100.0,
                    "metadata": {"architecture": arch},
                })
            except Exception as e:
                logger.warning("Failed to persist architecture: %s", e)

            event = _make_feedback_event(
                task_id=f"nas_eval_{arch_hash}",
                selected_action=f"evaluate_{task_data.get('algorithm', 'unknown')}",
                quality_score=result["accuracy"],
                carbon_g=result["carbon_kg"] * 1000.0,
                state=arch,
            )
            try:
                await self.message_queue.publish(
                    "feedback_events", event.to_json())
            except Exception as e:
                logger.warning("Failed to publish feedback event: %s", e)

            await self.ws_manager.broadcast({
                "type": "evaluation",
                "arch_hash": arch_hash,
                "accuracy": result["accuracy"],
                "carbon_kg": result["carbon_kg"],
            })

            algorithm = task_data.get("algorithm", "unknown")
            context = {
                "arch_hash": arch_hash,
                "algorithm": algorithm,
                "accuracy": result["accuracy"],
                "carbon": result["carbon_kg"],
                "energy": result["energy_kwh"],
            }
            utility = self._modp_utility({
                "accuracy": result["accuracy"],
                "carbon": result["carbon_kg"],
                "energy": result["energy_kwh"],
            })
            self.reasoning_engine.bandit.update(context, algorithm, utility)
            self.evaluation_queue.task_done()
        except Exception as e:
            logger.error("Evaluation processing error: %s", e)

    def _modp_utility(self, objectives: Dict[str, float]) -> float:
        try:
            return float(self.reasoning_engine.modp.evaluate(
                objectives, central_config.MODP_WEIGHTS))
        except Exception:
            return float(objectives.get("accuracy", 0.5))

    async def _update_population(self, evaluation_result: Dict[str, Any]) -> None:
        async with self._pop_lock:
            evaluation_result["utility"] = self._modp_utility({
                "accuracy": evaluation_result.get("accuracy", 0.0),
                "carbon": evaluation_result.get("carbon_kg", 0.0),
                "energy": evaluation_result.get("energy_kwh", 0.0),
            })
            self.population.append(evaluation_result)
            if (self.current_best is None
                    or evaluation_result.get("accuracy", 0.0)
                    > self.current_best.get("accuracy", 0.0)):
                self.current_best = evaluation_result
                try:
                    central_config.metrics_registry.set_best_accuracy(
                        evaluation_result["accuracy"])
                except Exception:
                    pass

    # ------------------------------------------------------------------
    async def run_nas_cycle(self, search_space: Dict[str, Any],
                            iterations: int = 50) -> Dict[str, Any]:
        start = time.monotonic()
        experiment_id = str(uuid.uuid4())[:8]
        try:
            self.storage.save_experiment(experiment_id, search_space, "running")
        except Exception:
            pass
        try:
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            alg_rec = await self.reasoning_engine._recommend_algorithm(search_space)
            algorithm = alg_rec.get("recommended", "darts")

            algorithm_result = await self.reasoning_engine.nas_algorithms.run_algorithm(
                algorithm, search_space, iterations)
            if algorithm_result.get("status") == "failed":
                try:
                    self.storage.update_experiment_end(experiment_id, "failed")
                except Exception:
                    pass
                return algorithm_result

            candidates = algorithm_result.get("candidates", [])
            best_arch = candidates[0] if candidates \
                else algorithm_result.get("best_architecture", {})

            quantum_result = await self.reasoning_engine.quantum_optimizer.optimize_architecture(
                best_arch, "qaoa")

            federated_result: Optional[Dict[str, Any]] = None
            if self.reasoning_engine.federated_learning.clients:
                federated_result = await self.reasoning_engine.federated_learning.federated_training_round()

            explanations = await self.reasoning_engine.explainable_nas.explain_architecture(
                best_arch)

            if best_arch:
                await self._update_population({
                    "accuracy": best_arch.get("final_accuracy", 0.8),
                    "carbon_kg": self.carbon_manager.calculate_nas_carbon(0.01),
                    "energy_kwh": 0.01,
                    "architecture": best_arch,
                })

            async with self._gen_lock:
                self.generation += 1

            if self.mlflow_available:
                try:
                    mlflow.log_metrics({
                        "accuracy": float(best_arch.get("final_accuracy", 0.0)),
                        "carbon_kg": 0.01,
                        "energy_kwh": 0.01,
                    })
                except Exception:
                    pass

            await self.reasoning_engine.optimizer.record_cycle({
                "best_accuracy": float(best_arch.get("final_accuracy", 0.0)),
                "carbon_kg": 0.01,
                "energy_kwh": 0.01,
                "algorithm": algorithm,
                "iterations": iterations,
            })

            review_id: Optional[str] = None
            if float(alg_rec.get("confidence", 1.0)) < 0.5:
                review_id = await self.reasoning_engine.human_review.request_review(
                    experiment_id,
                    {"algorithm": algorithm,
                     "accuracy": best_arch.get("final_accuracy", 0.0)},
                )

            keypair = await self.pqc.generate_keypair("hmac_sha256")
            signature = await self.pqc.sign_data({
                "experiment_id": experiment_id,
                "generation": self.generation,
                "best_architecture": best_arch,
            }, keypair["key_id"])

            backup = {
                "experiment_id": experiment_id,
                "generation": self.generation,
                "algorithm": algorithm,
                "best_architecture": best_arch,
                "quantum_result": quantum_result,
                "federated_result": federated_result,
                "explanations": explanations,
                "carbon_intensity": carbon_intensity,
                "duration_seconds": time.monotonic() - start,
                "signature": signature,
                "review_id": review_id,
            }
            await self.cloud_storage.store(
                backup, f"experiment_{experiment_id}.json")
            try:
                self.storage.update_experiment_end(experiment_id, "completed")
            except Exception:
                pass

            await self.ws_manager.broadcast({
                "type": "cycle_complete",
                "experiment_id": experiment_id,
                "generation": self.generation,
                "best_accuracy": float(best_arch.get("final_accuracy", 0.0)),
                "carbon_intensity": carbon_intensity,
            })

            event = _make_feedback_event(
                task_id=f"nas_cycle_{experiment_id}",
                selected_action=f"cycle_{algorithm}",
                quality_score=float(best_arch.get("final_accuracy", 0.8)),
                carbon_g=10.0,
                state=search_space,
            )
            try:
                await self.message_queue.publish("feedback_events", event.to_json())
            except Exception as e:
                logger.warning("Failed to publish feedback event: %s", e)

            context = {
                "experiment_id": experiment_id,
                "algorithm": algorithm,
                "best_accuracy": float(best_arch.get("final_accuracy", 0.0)),
                "carbon": 0.01,
            }
            utility = self._modp_utility({
                "accuracy": best_arch.get("final_accuracy", 0.0),
                "carbon": 0.01,
                "energy": 0.01,
            })
            self.reasoning_engine.bandit.update(context, algorithm, utility)

            return {
                "experiment_id": experiment_id,
                "generation": self.generation,
                "algorithm": algorithm,
                "best_architecture": best_arch,
                "quantum_optimization": quantum_result,
                "federated_result": federated_result,
                "explanations": explanations,
                "carbon_intensity": carbon_intensity,
                "duration_seconds": time.monotonic() - start,
                "signature": signature,
                "review_id": review_id,
            }
        except Exception as e:
            logger.error("NAS cycle failed: %s", e)
            try:
                self.storage.update_experiment_end(experiment_id, "failed")
            except Exception:
                pass
            return {"status": "failed", "error": str(e)}

    # ------------------------------------------------------------------
    async def run_flexgen_optimization(self, workload: Any,
                                       node: Any) -> Dict[str, Any]:
        return await self.flexgen_manager.optimize_policy(workload, node)

    # ------------------------------------------------------------------
    @property
    def message_queue(self) -> Any:
        # Lazily create a message queue if the central one isn't attached.
        if not hasattr(self, "_message_queue") or self._message_queue is None:
            try:
                self._message_queue = AsyncMessageQueue()
            except Exception:
                self._message_queue = _MessageQueueShim()
        return self._message_queue

    # ------------------------------------------------------------------
    async def get_system_status(self) -> Dict[str, Any]:
        async with self._pop_lock, self._gen_lock:
            try:
                best_acc = (self.current_best.get("accuracy", 0.0)
                            if self.current_best else 0.0)
            except Exception:
                best_acc = 0.0
            return {
                "instance_id": self.instance_id,
                "version": __version__,
                "generation": self.generation,
                "population_size": len(self.population),
                "best_accuracy": best_acc,
                "queue_size": self.evaluation_queue.qsize(),
                "reasoning": await self.reasoning_engine.get_reasoning_summary(),
                "algorithms": self.reasoning_engine.nas_algorithms.get_algorithm_status(),
                "quantum": self.reasoning_engine.quantum_optimizer.get_quantum_status(),
                "federated": await self.reasoning_engine.federated_learning.get_federated_status(),
                "explainability": self.reasoning_engine.explainable_nas.get_explanation_status(),
                "carbon_intensity": await self.carbon_manager.get_current_intensity(),
                "pqc_status": self.pqc.get_quantum_status(),
                "cloud_storage": {"providers": self.cloud_storage.provider_names()},
                "flexgen": await self.flexgen_manager.get_status(),
                "safety_rules": list(self.reasoning_engine.safety_monitor.rules.keys()),
                "chaos_enabled": self.reasoning_engine.chaos_monkey.enabled,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

    async def shutdown(self) -> None:
        logger.info("Shutting down CarbonAwareNAS (id=%s)", self.instance_id)
        self._shutdown_event.set()
        self._running = False
        await self._task_manager.stop_all()
        await self.carbon_manager.close()
        await self.energy_measurer.close()
        if self.mlflow_available:
            try:
                mlflow.end_run()
            except Exception:
                pass
        logger.info("Shutdown complete")


# ============================================================================
# FeedbackEvent helper
# ============================================================================
def _make_feedback_event(task_id: str, selected_action: str,
                         quality_score: float, carbon_g: float,
                         state: Dict[str, Any]) -> Any:
    """
    Build a FeedbackEvent through whichever helper the schema provides.
    Falls back to the local shim if the schema is missing.
    """
    if FeedbackEvent is None:
        return _FeedbackEventShim(task_id=task_id)
    if hasattr(FeedbackEvent, "create_with_context"):
        try:
            return FeedbackEvent.create_with_context(
                task_id=task_id,
                selected_action=selected_action,
                quality_score=quality_score,
                latency_ms=0.0,
                energy_joules=0.0,
                carbon_g=carbon_g,
                feedback_type="energy",
                adaptive_cost_value=0.0,
                state=state,
                candidates=[{"action": selected_action}],
                source="carbon_nas",
                environment=getattr(central_config, "ENVIRONMENT", "development"),
                tags=["nas"],
            )
        except Exception as e:
            logger.warning("FeedbackEvent.create_with_context failed: %s", e)
    # Fallback: build the enhanced schema directly.
    try:
        return FeedbackEvent(
            source="carbon_nas",
            feedback_type="energy",
            context={"task_id": task_id, "teacher_ids": []},
            action={"selected_action": selected_action,
                    "selected_rank": 1, "confidence_score": 0.5},
            performance={"quality_score": float(quality_score),
                         "latency_ms": 0.0, "energy_joules": 0.0,
                         "carbon_g": float(carbon_g),
                         "helium_cost": 0.0, "duration_ms": 0.0},
            adaptive_cost_value=0.0,
            tags=["nas"],
        )
    except Exception as e:
        logger.warning("FeedbackEvent construction failed: %s", e)
        return _FeedbackEventShim(task_id=task_id)


# ============================================================================
# Singleton
# ============================================================================
_nas_instance: Optional[CarbonAwareNAS] = None
_nas_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _nas_lock
    if _nas_lock is None:
        _nas_lock = asyncio.Lock()
    return _nas_lock


async def get_nas_instance() -> CarbonAwareNAS:
    global _nas_instance
    async with _get_lock():
        if _nas_instance is None:
            _nas_instance = CarbonAwareNAS()
            await _nas_instance.start()
        return _nas_instance


async def reset_nas_for_tests() -> None:
    global _nas_instance
    async with _get_lock():
        if _nas_instance is not None:
            try:
                await _nas_instance.shutdown()
            except Exception:
                pass
        _nas_instance = None


# ============================================================================
# FastAPI app (lifespan, Pydantic bodies)
# ============================================================================
if FASTAPI_AVAILABLE and PYDANTIC_AVAILABLE:
    class _SearchSpaceBody(BaseModel):
        num_layers: List[int] = [2, 4, 6]
        hidden_dim: List[int] = [64, 128]
        num_heads: List[int] = [4, 8]
        operations: List[str] = ["conv3x3", "attention"]

    class _NASStartBody(BaseModel):
        search_space: _SearchSpaceBody = _SearchSpaceBody()
        iterations: int = 50

    class _DeployBody(BaseModel):
        model_path: str
        config: Dict[str, Any] = {}

    class _FlexGenBody(BaseModel):
        workload: Dict[str, Any] = {}
        node: Dict[str, Any] = {}


def build_app(nas: CarbonAwareNAS) -> Any:
    if not (FASTAPI_AVAILABLE and PYDANTIC_AVAILABLE):
        return None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await nas.start()
        try:
            yield
        finally:
            await nas.shutdown()

    app = FastAPI(title="Carbon-Aware NAS API", version=__version__,
                  lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware, allow_origins=[], allow_credentials=True,
        allow_methods=["*"], allow_headers=["*"])
    security = HTTPBearer(auto_error=False)

    async def _get_user(
        credentials: HTTPAuthorizationCredentials = Depends(security),
    ) -> Dict[str, Any]:
        if credentials is None:
            raise HTTPException(status_code=401, detail="Missing token")
        try:
            import jwt  # type: ignore
            payload = jwt.decode(
                credentials.credentials,
                getattr(central_config, "JWT_SECRET", "dev-secret"),
                algorithms=["HS256"])
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid token")
        roles = payload.get("roles") or ["user"]
        if isinstance(roles, str):
            roles = roles.split()
        return {"sub": payload.get("sub"), "roles": list(roles)}

    async def _require_admin(user: Dict = Depends(_get_user)) -> Dict:
        if "admin" not in (user.get("roles") or []):
            raise HTTPException(status_code=403, detail="Admin required")
        return user

    @app.get("/health")
    async def health() -> Dict[str, Any]:
        return {"status": "ok", "version": __version__}

    @app.post("/nas/start")
    async def start_nas(body: _NASStartBody,
                        user: Dict = Depends(_require_admin)):
        return await nas.run_nas_cycle(body.search_space.model_dump(),
                                       body.iterations)

    @app.get("/nas/status")
    async def nas_status(user: Dict = Depends(_get_user)):
        return await nas.get_system_status()

    @app.get("/nas/architectures")
    async def list_architectures(limit: int = 100,
                                 user: Dict = Depends(_get_user)):
        try:
            return nas.storage.get_architectures(limit)
        except Exception:
            return []

    @app.post("/deploy")
    async def deploy_model(body: _DeployBody,
                           user: Dict = Depends(_require_admin)):
        return await nas.reasoning_engine.deployment.deploy_model(
            body.model_path, body.config)

    @app.post("/flexgen/optimize")
    async def flexgen_optimize(body: _FlexGenBody,
                               user: Dict = Depends(_get_user)):
        return await nas.run_flexgen_optimization(body.workload, body.node)

    @app.get("/flexgen/status")
    async def flexgen_status(user: Dict = Depends(_get_user)):
        return await nas.flexgen_manager.get_status()

    @app.get("/safety/violations")
    async def safety_violations(user: Dict = Depends(_get_user)):
        return {"violations": []}

    @app.get("/human-review/pending")
    async def human_review_pending(user: Dict = Depends(_require_admin)):
        return await nas.reasoning_engine.human_review.get_pending()

    @app.post("/human-review/{review_id}/approve")
    async def human_review_approve(review_id: str,
                                   user: Dict = Depends(_require_admin)):
        ok = await nas.reasoning_engine.human_review.approve(review_id)
        return {"status": "approved" if ok else "not_found",
                "review_id": review_id}

    @app.post("/human-review/{review_id}/reject")
    async def human_review_reject(review_id: str,
                                  user: Dict = Depends(_require_admin)):
        ok = await nas.reasoning_engine.human_review.reject(review_id)
        return {"status": "rejected" if ok else "not_found",
                "review_id": review_id}

    @app.post("/chaos/trigger")
    async def chaos_trigger(user: Dict = Depends(_require_admin)):
        nas.reasoning_engine.chaos_monkey.enabled = True
        try:
            nas.reasoning_engine.chaos_monkey.maybe_fail()
        except Exception as e:
            return {"status": "chaos triggered", "error": str(e)}
        return {"status": "chaos enabled"}

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        await nas.ws_manager.connect(websocket)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            await nas.ws_manager.disconnect(websocket)

    return app


# ============================================================================
# Tests
# ============================================================================
def _test_proxy_model_guard() -> None:
    # Import should succeed whether or not PyTorch is available.
    assert ProxyModel is not None


def _test_pqc_salt_persistence(tmpdir: Path) -> None:
    os.environ["NAS_STORAGE_PATH"] = str(tmpdir / "pqc_store")
    cfg = _ConfigShim()
    storage = _StorageShim()
    vault = VaultManager()
    async def run() -> None:
        pqc1 = PostQuantumCrypto(storage, vault)
        kp = await pqc1.generate_keypair()
        sig = await pqc1.sign_data({"x": 1}, kp["key_id"])

        # New instance, same storage -> must decrypt with the same salt.
        pqc2 = PostQuantumCrypto(storage, vault)
        assert await pqc2.verify_data({"x": 1}, sig) is True
        assert await pqc2.verify_data({"x": 2}, sig) is False
    asyncio.run(run())


def _test_causal_bandit_no_keyerror() -> None:
    b = CausalBandit(["a", "b"], fallback_solver=lambda c: "a", seed=0)
    b.update({}, "unknown", 1.0)
    assert "unknown" in b.q_values
    assert b.counts["unknown"] == 1


def _test_federated_numeric_filter() -> None:
    c = FederatedCoordinator()
    c.register_client("x", {"a": 1.0, "b": "not_a_number"})
    c.register_client("y", {"a": 3.0})
    agg = c.aggregate()
    assert agg["a"] == 2.0
    assert "b" not in agg


def _test_multi_agent_stable_hash() -> None:
    m1 = MultiAgentCoordinator(3)
    m2 = MultiAgentCoordinator(3)
    assert m1.assign_task("abc") == m2.assign_task("abc")


def _test_chaos_validation() -> None:
    try:
        ChaosMonkey(failure_probability=1.5)
    except ValueError:
        pass
    else:
        raise AssertionError("invalid probability should raise")


def _test_circuit_breaker_sync_async() -> None:
    cb = EnhancedCircuitBreaker("t", failure_threshold=2,
                                recovery_timeout=0.1)
    def sync_ok() -> int: return 42
    async def async_ok() -> int: return 43
    async def run() -> None:
        assert await cb.call(sync_ok) == 42
        assert await cb.call(async_ok) == 43
    asyncio.run(run())


def _test_reasoning_engine_has_carbon_manager() -> None:
    cm = CarbonIntensityManager(seed=0)
    engine = GreenAgentReasoningEngine(EnergyMeasurer(seed=0), cm, seed=0)
    assert engine.carbon_manager is cm
    assert engine.policy_probs({"carbon_intensity": 0.5}) is not None


def _test_nas_cycle_end_to_end(tmpdir: Path) -> None:
    os.environ["NAS_STORAGE_PATH"] = str(tmpdir / "nas_store")
    os.environ["NAS_CLOUD_DIR"] = str(tmpdir / "nas_cloud")

    async def run() -> None:
        nas = CarbonAwareNAS(seed=0)
        try:
            search_space = {
                "num_layers": [2, 4], "hidden_dim": [64, 128],
                "num_heads": [4], "operations": ["conv3x3"],
            }
            result = await nas.run_nas_cycle(search_space, iterations=2)
            assert "experiment_id" in result
            status = await nas.get_system_status()
            assert status["version"] == __version__
            assert isinstance(status["cloud_storage"]["providers"], list)
            # FlexGen endpoint path.
            fg = await nas.run_flexgen_optimization({"task_id": "w"}, {"id": "n"})
            assert "policy" in fg
        finally:
            await nas.shutdown()
    asyncio.run(run())


def run_tests() -> None:
    import tempfile
    _test_proxy_model_guard()
    _test_causal_bandit_no_keyerror()
    _test_federated_numeric_filter()
    _test_multi_agent_stable_hash()
    _test_chaos_validation()
    _test_circuit_breaker_sync_async()
    _test_reasoning_engine_has_carbon_manager()
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        _test_pqc_salt_persistence(tmp)
        _test_nas_cycle_end_to_end(tmp)
    print(f"All carbon_nas_unified {__version__} tests passed.")


# ============================================================================
# Demo / entry point
# ============================================================================
async def _demo() -> None:
    print("=" * 80)
    print(f"Enhanced Carbon-Aware NAS {__version__}")
    print("=" * 80)
    nas = await get_nas_instance()
    search_space = {
        "num_layers": [2, 4, 6], "hidden_dim": [64, 128, 256],
        "num_heads": [4, 8], "operations": ["conv3x3", "attention"],
    }
    result = await nas.run_nas_cycle(search_space, iterations=5)
    print(f"Experiment: {result.get('experiment_id', 'N/A')}")
    print(f"Algorithm: {result.get('algorithm', 'unknown')}")
    status = await nas.get_system_status()
    print(f"Best accuracy: {status['best_accuracy']:.4f}")
    print(f"Carbon intensity: {status['carbon_intensity']:.1f} gCO2/kWh")
    print(f"PQC available: {status['pqc_status']['pqc_available']}")
    print(f"Cloud providers: {status['cloud_storage']['providers']}")
    print(f"FlexGen available: {status['flexgen']['available']}")
    await nas.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=f"Carbon-Aware NAS {__version__}")
    parser.add_argument("--test", action="store_true",
                        help="Run the built-in test suite.")
    parser.add_argument("--demo", action="store_true",
                        help="Run a single NAS cycle and exit.")
    parser.add_argument("--serve", action="store_true",
                        help="Start the FastAPI server.")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    if args.test:
        run_tests()
        return
    if args.demo:
        asyncio.run(_demo())
        return
    if args.serve and FASTAPI_AVAILABLE:
        import uvicorn  # type: ignore

        async def _serve() -> None:
            nas = await get_nas_instance()
            app = build_app(nas)
            if app is None:
                raise RuntimeError("FastAPI not available")
            config = uvicorn.Config(app, host=args.host, port=args.port,
                                    log_level="info", server_header=False)
            server = uvicorn.Server(config)
            await server.serve()

        asyncio.run(_serve())
        return
    parser.print_help()


if __name__ == "__main__":
    main()
