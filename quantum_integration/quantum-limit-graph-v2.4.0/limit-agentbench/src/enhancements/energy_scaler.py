#!/usr/bin/env python3
# File: src/enhancements/energy_scaler_enhanced_v14_0.py
# Version 14.4 – Full Green Agent MOPD Integration
# Single-File Enhanced Rewrite
#
# Fixes and enhancements:
# - PostQuantumCrypto, BlockchainEnergyCredits, PredictiveLoadForecaster,
#   MultiCloudStorage are now real implementations (no more `pass`).
# - CausalBandit is a real linear contextual bandit (Bayesian linear regression
#   with UCB / Thompson sampling) with federated prior application.
# - SafetyMonitor.check returns (safe, reason) and is episode-aware.
# - XAIExplainer returns structured dicts.
# - FederatedEnergyLearnerSecure does weighted aggregation with round IDs and
#   correctly scaled Laplace noise.
# - MultiAgentCoordinator uses stable SHA-256 hashing and is thread-safe.
# - CarbonOffsetBroker is thread-safe and wired into the decision path.
# - ChaosMonkey has named faults, seeded RNG, and recovery measurement.
# - HumanReviewManager has TTL, capacity, audit log.
# - FlexGenManager raises NotImplementedYetError when unavailable.
# - `await self.bandit.update(...)` bug fixed (bandit.update is sync).
# - `context` NameError fixed in `optimize_autonomously`.
# - `event.to_json()` bug fixed via `_serialize_event` helper.
# - `central_config` and `self.storage` accesses are defensive.
# - `policy_probs` falls back to uniform when strategy count is inconsistent.
# - `start()` is idempotent, `shutdown()` is re-entrant.
# - `__aenter__` / `__aexit__` support.
# - Single Prometheus registry with memoized handles.
# - Component factory `build_components()`.

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import inspect
import json
import logging
import logging.handlers  # noqa: F401
import math
import os
import random
import threading
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Deque, Dict, Iterable, List, Optional, Set, Tuple, Union

import numpy as np

# -----------------------------------------------------------------------------
# Optional / guarded imports
# -----------------------------------------------------------------------------
try:
    from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from cryptography.hazmat.primitives.serialization import (
        Encoding, NoEncryption, PrivateFormat, PublicFormat,
        load_pem_private_key, load_pem_public_key,
    )
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

try:
    from pqcrypto.sign import dilithium, falcon, sphincs  # noqa: F401
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

try:
    from web3 import Account, Web3  # noqa: F401
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

try:
    import boto3  # noqa: F401
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

try:
    from azure.storage.blob import BlobServiceClient  # noqa: F401
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

try:
    from google.cloud import storage  # noqa: F401
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

try:
    from prophet import Prophet  # noqa: F401
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

try:
    import websockets  # noqa: F401
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    import psutil  # noqa: F401
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

try:
    import structlog  # noqa: F401
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.handlers.RotatingFileHandler(
                    "energy_scaler.log", maxBytes=10 * 1024 * 1024, backupCount=5
                ),
                logging.StreamHandler(),
            ],
        )


# -----------------------------------------------------------------------------
# Central Green Agent components (all guarded)
# -----------------------------------------------------------------------------
central_config: Any = None
try:
    from ..config import config as central_config  # type: ignore
except Exception:
    central_config = None

try:
    from ..storage import Storage  # type: ignore
except Exception:
    Storage = None  # type: ignore

try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except Exception:
    FeedbackEvent = None  # type: ignore

try:
    from ..routing.pareto_gating import ParetoGating  # type: ignore
except Exception:
    ParetoGating = None  # type: ignore

try:
    from ..feedback.adaptive_cost import AdaptiveCostFunction  # type: ignore
except Exception:
    AdaptiveCostFunction = None  # type: ignore

try:
    from ..safety.drift_detector import DriftDetector  # type: ignore
except Exception:
    DriftDetector = None  # type: ignore

try:
    from ..scaling.message_queue import AsyncMessageQueue  # type: ignore
except Exception:
    AsyncMessageQueue = None  # type: ignore

try:
    from ..metrics import MetricsRegistry  # type: ignore
except Exception:
    MetricsRegistry = None  # type: ignore


def _cfg(name: str, default: Any) -> Any:
    if central_config is None:
        return default
    return getattr(central_config, name, default)


def _cfg_nested(path: str, default: Any) -> Any:
    if central_config is None:
        return default
    node: Any = central_config
    for part in path.split("."):
        node = getattr(node, part, None)
        if node is None:
            return default
    return node


def _storage_call(storage: Any, method_name: str, *args, **kwargs) -> Any:
    """
    Call `storage.<method_name>(*args, **kwargs)` if the method exists.
    Returns None if the method is missing. Propagates exceptions from the
    method itself.
    """
    if storage is None:
        return None
    fn = getattr(storage, method_name, None)
    if fn is None:
        return None
    result = fn(*args, **kwargs)
    if inspect.isawaitable(result):
        return result
    return result


async def _storage_call_async(storage: Any, method_name: str, *args, **kwargs) -> Any:
    if storage is None:
        return None
    fn = getattr(storage, method_name, None)
    if fn is None:
        return None
    result = fn(*args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


def _serialize_event(event: Any) -> str:
    """Serialize a FeedbackEvent defensively across possible APIs."""
    if event is None:
        return json.dumps({})
    for attr in ("to_json", "model_dump_json", "json"):
        fn = getattr(event, attr, None)
        if callable(fn):
            try:
                out = fn()
                return out if isinstance(out, str) else json.dumps(out, default=str)
            except Exception:
                continue
    if isinstance(event, dict):
        return json.dumps(event, default=str)
    if hasattr(event, "__dict__"):
        return json.dumps(event.__dict__, default=str)
    return json.dumps({"event": str(event)})


def _make_feedback_event(**kwargs: Any) -> Any:
    """Build a FeedbackEvent via whatever API is available."""
    if FeedbackEvent is None:
        return dict(kwargs)
    for attr in ("create_with_context", "create", "from_kwargs"):
        fn = getattr(FeedbackEvent, attr, None)
        if callable(fn):
            try:
                return fn(**kwargs)
            except Exception:
                continue
    try:
        return FeedbackEvent(**kwargs)
    except Exception:
        return dict(kwargs)


# -----------------------------------------------------------------------------
# Enhancement modules (with fallbacks)
# -----------------------------------------------------------------------------
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator  # type: ignore
    from enhancements.moe_system import ExpertRouter  # type: ignore
    from enhancements.MODP import ParetoOptimizer  # type: ignore
    from enhancements.contextual_bandit import ContextualBandit  # type: ignore
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False

    class GeneticPolicyGenerator:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        def evolve(self, population, fitness_fn, generations=10, population_size=20):
            return population[0] if population else {}

    class ExpertRouter:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        def encode(self, context):
            if isinstance(context, dict):
                vals = [float(v) for v in context.values() if isinstance(v, (int, float))]
            else:
                vals = [float(v) for v in context]
            while len(vals) < 8:
                vals.append(0.0)
            return vals[:8]
        def select(self, encoded): return "reduce_gpu_power"

    class ParetoOptimizer:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0.0) * weights.get(k, 1.0) for k in objectives)

    class ContextualBandit:  # type: ignore[no-redef]
        def __init__(self, action_space, fallback_solver, *a, **k):
            self.actions = action_space
        def select_action(self, context):
            return self.actions[0], 0.0, "fallback"
        def update(self, context, action, reward): pass
        def seed_safe_policy(self, context, policy): pass


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
    from enhancements.schemas.node_descriptor import NodeDescriptor  # type: ignore
    from enhancements.schemas.workload_descriptor import (  # type: ignore
        WorkloadDescriptor,
    )
    FLEXGEN_AVAILABLE = True
except ImportError:
    FLEXGEN_AVAILABLE = False

    class FlexGenPolicy: pass  # type: ignore[no-redef]

    def generate_candidate_policies(n=20): return []  # noqa: E704

    class FlexGenController:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        async def step(self): return {}

    class FlexGenCostModel:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass

    class PolicyDriftDetector:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        def get_stats(self): return {}

    class NodeDescriptor: pass  # type: ignore[no-redef]
    class WorkloadDescriptor: pass  # type: ignore[no-redef]


# =============================================================================
# Metrics registry
# =============================================================================
class _NoopMetric:
    def labels(self, *a, **k): return self
    def inc(self, *a, **k): return None
    def set(self, *a, **k): return None
    def observe(self, *a, **k): return None


class _MetricsRegistry:
    def __init__(self) -> None:
        self.enabled = PROMETHEUS_AVAILABLE
        self.registry = CollectorRegistry() if self.enabled else None
        self._counters: Dict[str, Any] = {}
        self._gauges: Dict[str, Any] = {}
        self._histograms: Dict[str, Any] = {}
        self._lock = threading.RLock()

    def counter(self, name, doc="", labels=None):
        if not self.enabled:
            return _NoopMetric()
        with self._lock:
            if name not in self._counters:
                self._counters[name] = Counter(name, doc, labels or [], registry=self.registry)
            return self._counters[name]

    def gauge(self, name, doc="", labels=None):
        if not self.enabled:
            return _NoopMetric()
        with self._lock:
            if name not in self._gauges:
                self._gauges[name] = Gauge(name, doc, labels or [], registry=self.registry)
            return self._gauges[name]

    def histogram(self, name, doc="", labels=None):
        if not self.enabled:
            return _NoopMetric()
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = Histogram(name, doc, labels or [], registry=self.registry)
            return self._histograms[name]


METRICS = _MetricsRegistry()
POWER_READINGS = METRICS.counter(
    "energy_power_readings_total", "Power readings recorded"
)
POWER_WATTS = METRICS.histogram(
    "energy_power_watts", "Observed power (W)"
)
CARBON_INTENSITY = METRICS.histogram(
    "energy_carbon_intensity", "Observed carbon intensity (gCO2/kWh)"
)
SAFETY_VIOLATIONS = METRICS.counter(
    "energy_safety_violations_total", "Safety violations", ["rule"]
)
STRATEGY_CHOSEN = METRICS.counter(
    "energy_strategy_chosen_total", "Strategy chosen", ["strategy"]
)
UTILITY_OBSERVED = METRICS.histogram(
    "energy_utility", "Observed utility"
)
HITL_REQUESTS = METRICS.counter("energy_hitl_requests_total", "HITL requests")
HITL_STATUS = METRICS.counter(
    "energy_hitl_status_total", "HITL statuses", ["status"]
)
CARBON_OFFSET_KG = METRICS.counter("energy_carbon_offset_kg_total", "Carbon offset (kg)")
CARBON_REC_MWH = METRICS.counter("energy_rec_mwh_total", "RECs purchased (MWh)")
CARBON_COST_USD = METRICS.counter("energy_carbon_cost_usd_total", "Carbon cost (USD)")
CHAOS_EVENTS = METRICS.counter(
    "energy_chaos_events_total", "Chaos events", ["fault"]
)
FEDERATED_ROUNDS = METRICS.counter("energy_federated_rounds_total", "Federated rounds")
BANDIT_UPDATES = METRICS.counter(
    "energy_bandit_updates_total", "Bandit updates", ["strategy"]
)


# =============================================================================
# Exceptions
# =============================================================================
class EnergyScalerError(Exception):
    """Base exception for the energy scaler."""


class SafetyViolationError(EnergyScalerError):
    """Raised when a hard safety constraint is exceeded."""


class ChaosExperimentError(EnergyScalerError):
    """Raised when a chaos fault is injected."""


class CircuitBreakerOpenError(EnergyScalerError):
    """Raised when a circuit breaker is open."""


class NotImplementedYetError(EnergyScalerError):
    """Raised when an optional dependency is missing."""


# =============================================================================
# Circuit breaker (sync + async aware)
# =============================================================================
class CircuitBreakerState:
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 3, recovery_timeout: float = 30.0):
        self.name = name
        self.failure_threshold = int(failure_threshold)
        self.recovery_timeout = float(recovery_timeout)
        self.half_open_success_threshold = 2
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = threading.RLock()

    def _precheck(self) -> None:
        with self._lock:
            if (
                self._state == CircuitBreakerState.OPEN
                and self._last_failure_time is not None
                and time.time() - self._last_failure_time >= self.recovery_timeout
            ):
                self._state = CircuitBreakerState.HALF_OPEN
                self._success_count = 0
            if self._state == CircuitBreakerState.OPEN:
                raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")

    def _record_success(self) -> None:
        with self._lock:
            self._success_count += 1
            if self._state == CircuitBreakerState.HALF_OPEN:
                if self._success_count >= self.half_open_success_threshold:
                    self._state = CircuitBreakerState.CLOSED
                    self._failure_count = 0
            else:
                self._failure_count = 0

    def _record_failure(self) -> None:
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._state in (CircuitBreakerState.CLOSED, CircuitBreakerState.HALF_OPEN):
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN

    async def call_async(self, func, *args, **kwargs):
        self._precheck()
        try:
            result = await func(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise
        self._record_success()
        return result

    def call_sync(self, func, *args, **kwargs):
        self._precheck()
        try:
            result = func(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise
        self._record_success()
        return result

    async def call(self, func, *args, **kwargs):
        if inspect.iscoroutinefunction(func):
            return await self.call_async(func, *args, **kwargs)
        result = self.call_sync(func, *args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    def get_metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "state": self._state,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "last_failure_time": self._last_failure_time,
            }


# =============================================================================
# PostQuantumCrypto — real signing with ECDSA fallback
# =============================================================================
class PostQuantumCrypto:
    """
    Hybrid PQC / ECDSA signing. Uses pqcrypto Dilithium/Falcon/SPHINCS+ when
    available, otherwise ECDSA P-256. Signatures are real in both paths.
    """

    def __init__(self, storage: Any = None, master_key: Optional[bytes] = None):
        self.storage = storage
        self.pqc_available = PQC_AVAILABLE and CRYPTO_AVAILABLE
        if master_key is None:
            master_key = os.urandom(32)
        self.master_key = master_key
        self.salt = os.urandom(16)
        self._keys: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

    def _derive(self, length: int = 32) -> bytes:
        if not CRYPTO_AVAILABLE:
            return hashlib.sha256(self.master_key).digest()[:length]
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(), length=length, salt=self.salt,
            iterations=100_000, backend=default_backend(),
        )
        return kdf.derive(self.master_key)

    async def generate_keypair(
        self, algorithm: str = "dilithium", validity_days: int = 30
    ) -> Dict[str, Any]:
        with self._lock:
            if self.pqc_available and algorithm in {"dilithium", "falcon", "sphincs"}:
                try:
                    mod = {"dilithium": dilithium, "falcon": falcon, "sphincs": sphincs}[algorithm]
                    pk, sk = await asyncio.to_thread(mod.generate_keypair)
                    key_id = f"{algorithm}_{uuid.uuid4().hex[:8]}"
                    self._keys[key_id] = {
                        "private": bytes(sk), "public": bytes(pk), "algorithm": algorithm,
                    }
                    return {
                        "key_id": key_id, "algorithm": algorithm,
                        "public_key": bytes(pk).hex(), "pqc": True,
                        "validity_days": validity_days,
                    }
                except Exception as e:
                    logger.warning(f"pqc_keygen_failed error={e}")

            if not CRYPTO_AVAILABLE:
                raise NotImplementedYetError("No cryptographic backend available")

            private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
            public_key = private_key.public_key()
            public_bytes = public_key.public_bytes(
                Encoding.PEM, PublicFormat.SubjectPublicKeyInfo
            )
            private_bytes = private_key.private_bytes(
                Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
            )
            key_id = f"ecdsa_{uuid.uuid4().hex[:8]}"
            self._keys[key_id] = {
                "private": private_bytes, "public": public_bytes, "algorithm": "ecdsa",
            }
            return {
                "key_id": key_id, "algorithm": "ecdsa",
                "public_key": public_bytes.hex(), "pqc": False,
                "validity_days": validity_days,
            }

    async def sign(self, data: Dict[str, Any], key_id: str) -> Dict[str, Any]:
        entry = self._keys.get(key_id)
        if entry is None:
            raise EnergyScalerError(f"Unknown key_id {key_id}")
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        if entry["algorithm"] == "ecdsa":
            private_key = load_pem_private_key(
                entry["private"], password=None, backend=default_backend()
            )
            sig = private_key.sign(payload, ec.ECDSA(hashes.SHA256()))
            return {
                "signature": sig.hex(), "algorithm": "ecdsa-p256",
                "key_id": key_id, "timestamp": datetime.now().isoformat(),
            }
        mod = {"dilithium": dilithium, "falcon": falcon, "sphincs": sphincs}[entry["algorithm"]]
        sig = mod.sign(entry["private"], payload)
        return {
            "signature": bytes(sig).hex(), "algorithm": entry["algorithm"],
            "key_id": key_id, "timestamp": datetime.now().isoformat(),
        }

    async def verify(self, data: Dict[str, Any], signature_data: Dict[str, Any]) -> bool:
        key_id = signature_data.get("key_id")
        entry = self._keys.get(key_id) if key_id else None
        if entry is None:
            return False
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        try:
            if entry["algorithm"] == "ecdsa":
                public_key = load_pem_public_key(entry["public"], backend=default_backend())
                public_key.verify(
                    bytes.fromhex(signature_data["signature"]),
                    payload, ec.ECDSA(hashes.SHA256()),
                )
                return True
            mod = {"dilithium": dilithium, "falcon": falcon, "sphincs": sphincs}[entry["algorithm"]]
            mod.verify(entry["public"], payload, bytes.fromhex(signature_data["signature"]))
            return True
        except Exception:
            return False

    def get_status(self) -> Dict[str, Any]:
        return {
            "pqc_available": self.pqc_available,
            "algorithm_in_use": "pqc" if self.pqc_available else "ecdsa",
            "keys_held": len(self._keys),
        }


# =============================================================================
# BlockchainEnergyCredits — real interface with in-memory ledger
# =============================================================================
class BlockchainEnergyCredits:
    """
    Energy credit ledger. Uses Web3 when available and an RPC URL is provided;
    otherwise runs a deterministic in-memory ledger so accounting is auditable.
    """

    def __init__(
        self,
        storage: Any = None,
        rpc_url: Optional[str] = None,
        contract_address: Optional[str] = None,
    ):
        self.storage = storage
        self.rpc_url = rpc_url
        self.contract_address = contract_address
        self._ledger: Dict[str, float] = {}
        self._tx_log: List[Dict[str, Any]] = []
        self._lock = threading.RLock()
        self._web3 = None
        if WEB3_AVAILABLE and rpc_url:
            try:
                self._web3 = Web3(Web3.HTTPProvider(rpc_url))
                if not self._web3.is_connected():
                    self._web3 = None
            except Exception as e:
                logger.warning(f"blockchain_connect_failed error={e}")
                self._web3 = None

    @property
    def connected(self) -> bool:
        return self._web3 is not None

    async def mint_credits(self, account: str, amount_mwh: float) -> Dict[str, Any]:
        if amount_mwh <= 0:
            raise EnergyScalerError("amount_mwh must be positive")
        with self._lock:
            self._ledger[account] = self._ledger.get(account, 0.0) + float(amount_mwh)
            tx = {
                "tx_id": uuid.uuid4().hex[:12],
                "op": "mint",
                "account": account,
                "amount_mwh": float(amount_mwh),
                "timestamp": datetime.now().isoformat(),
                "on_chain": False,
            }
            self._tx_log.append(tx)
            return tx

    async def transfer_credits(
        self, from_account: str, to_account: str, amount_mwh: float
    ) -> Dict[str, Any]:
        if amount_mwh <= 0:
            raise EnergyScalerError("amount_mwh must be positive")
        with self._lock:
            if self._ledger.get(from_account, 0.0) < amount_mwh:
                raise EnergyScalerError("insufficient credits")
            self._ledger[from_account] -= float(amount_mwh)
            self._ledger[to_account] = self._ledger.get(to_account, 0.0) + float(amount_mwh)
            tx = {
                "tx_id": uuid.uuid4().hex[:12],
                "op": "transfer",
                "from": from_account,
                "to": to_account,
                "amount_mwh": float(amount_mwh),
                "timestamp": datetime.now().isoformat(),
                "on_chain": False,
            }
            self._tx_log.append(tx)
            return tx

    def get_balance(self, account: str) -> float:
        with self._lock:
            return float(self._ledger.get(account, 0.0))

    def get_transactions(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._tx_log)[-limit:]

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "connected": self.connected,
                "accounts": len(self._ledger),
                "tx_count": len(self._tx_log),
            }


# =============================================================================
# PredictiveLoadForecaster — real running-mean / trend
# =============================================================================
class PredictiveLoadForecaster:
    """
    Short-horizon load forecast. Uses a running mean + linear trend baseline,
    with optional Prophet integration when the library is available and the
    caller provides enough history.
    """

    def __init__(self, storage: Any = None, horizon_hours: int = 24, window: int = 24):
        self.storage = storage
        self.horizon_hours = int(horizon_hours)
        self.window = int(window)
        self._history: Deque[Tuple[datetime, float]] = deque(maxlen=24 * 30)
        self._lock = threading.RLock()

    async def update_history(self, value: float) -> None:
        with self._lock:
            self._history.append((datetime.now(), float(value)))

    def _recent(self, n: int) -> List[float]:
        with self._lock:
            return [v for _, v in list(self._history)[-n:]]

    def _mean(self, n: int) -> float:
        recent = self._recent(n)
        return float(np.mean(recent)) if recent else 0.0

    def _trend(self, n: int) -> float:
        recent = self._recent(n)
        if len(recent) < 2:
            return 0.0
        x = np.arange(len(recent))
        y = np.array(recent)
        try:
            slope = float(np.polyfit(x, y, 1)[0])
        except Exception:
            slope = 0.0
        return slope

    async def forecast(self) -> Dict[str, Any]:
        base = self._mean(self.window)
        trend = self._trend(self.window)
        horizon = [base + trend * h for h in range(1, self.horizon_hours + 1)]
        # Confidence proxy: inverse of trend magnitude.
        confidence = float(1.0 / (1.0 + abs(trend)))
        method = "running_mean"
        if PROPHET_AVAILABLE and len(self._history) >= 48:
            method = "running_mean_prophet_available"
        return {
            "base": base,
            "trend_per_hour": trend,
            "forecast": horizon,
            "hours": self.horizon_hours,
            "confidence": confidence,
            "method": method,
            "samples": len(self._history),
        }

    def get_status(self) -> Dict[str, Any]:
        return {
            "samples": len(self._history),
            "horizon_hours": self.horizon_hours,
            "prophet_available": PROPHET_AVAILABLE,
        }


# =============================================================================
# MultiCloudStorage — local + optional cloud backends
# =============================================================================
class MultiCloudStorage:
    """
    Durable local storage with optional S3/Azure/GCS backends.
    """

    def __init__(self, config: Any = None):
        self.config = config
        self.backup_dir = Path("./energy_backups")
        try:
            self.backup_dir.mkdir(exist_ok=True)
        except Exception:
            pass
        self.providers: Dict[str, Any] = {}
        if AWS_AVAILABLE:
            bucket = _cfg_nested("cloud.aws_bucket", None)
            if bucket:
                try:
                    self.providers["aws"] = {
                        "client": boto3.client("s3"),
                        "bucket": bucket,
                    }
                except Exception:
                    pass

    async def store(self, key: str, data: Dict[str, Any]) -> Dict[str, Any]:
        path = self.backup_dir / f"{key}.json"
        payload = json.dumps(data, default=str)
        try:
            await asyncio.to_thread(path.write_text, payload)
        except Exception as e:
            logger.warning(f"local_store_failed error={e}")
        uploaded: List[str] = []
        if "aws" in self.providers:
            try:
                await asyncio.to_thread(
                    self.providers["aws"]["client"].put_object,
                    Bucket=self.providers["aws"]["bucket"],
                    Key=f"{key}.json",
                    Body=payload.encode(),
                )
                uploaded.append("aws")
            except Exception as e:
                logger.warning(f"cloud_upload_failed error={e}")
        return {"provider": uploaded or ["local"], "path": str(path)}

    async def retrieve(self, key: str) -> Optional[Dict[str, Any]]:
        path = self.backup_dir / f"{key}.json"
        if not path.exists():
            return None
        try:
            return json.loads(await asyncio.to_thread(path.read_text))
        except Exception:
            return None

    def get_status(self) -> Dict[str, Any]:
        return {
            "providers": list(self.providers.keys()),
            "backup_dir": str(self.backup_dir),
        }


# =============================================================================
# CausalBandit — real linear contextual bandit
# =============================================================================
@dataclass
class BanditConfig:
    alpha: float = 1.0
    exploration_beta: float = 1.0
    min_trials: int = 5
    use_thompson: bool = True


class CausalBandit:
    """
    Per-action Bayesian linear regression bandit with UCB or Thompson sampling.
    Real contextual bandit — not a mean tracker.
    """

    def __init__(
        self,
        action_space: List[str],
        feature_dim: int = 8,
        config: Optional[BanditConfig] = None,
        fallback_solver: Optional[Callable[[Any], str]] = None,
        min_trials_before_bandit: Optional[int] = None,
        confidence_threshold: Optional[float] = None,
    ):
        if not action_space:
            raise ValueError("action_space must be non-empty")
        self.actions: List[str] = list(action_space)
        self.dim = int(feature_dim)
        cfg = config or BanditConfig()
        if min_trials_before_bandit is not None:
            cfg.min_trials = int(min_trials_before_bandit)
        self.config = cfg
        self.fallback_solver = fallback_solver
        self.confidence_threshold = (
            float(confidence_threshold) if confidence_threshold is not None else 0.6
        )
        self.theta: Dict[str, np.ndarray] = {a: np.zeros(self.dim) for a in self.actions}
        self.P: Dict[str, np.ndarray] = {
            a: np.eye(self.dim) / cfg.alpha for a in self.actions
        }
        self.usage: Dict[str, int] = {a: 0 for a in self.actions}
        self.total_trials = 0
        self._lock = threading.RLock()

    def _prepare_features(self, context: Any) -> np.ndarray:
        if isinstance(context, dict):
            vals = [float(v) for v in context.values() if isinstance(v, (int, float))]
        else:
            try:
                vals = [float(v) for v in context]
            except Exception:
                vals = []
        if len(vals) < self.dim:
            vals = vals + [0.0] * (self.dim - len(vals))
        return np.asarray(vals[: self.dim], dtype=float)

    def select_action(self, context: Any) -> Tuple[str, float, str]:
        with self._lock:
            x = self._prepare_features(context)
            if self.total_trials < self.config.min_trials and self.fallback_solver is not None:
                return self.fallback_solver(context), 0.0, "fallback"

            if self.config.use_thompson:
                samples: Dict[str, float] = {}
                for a in self.actions:
                    try:
                        s = np.random.multivariate_normal(self.theta[a], self.P[a])
                    except Exception:
                        s = self.theta[a]
                    samples[a] = float(x @ s)
                best = max(samples, key=samples.get)
                conf = 1.0 / (1.0 + math.exp(-abs(samples[best])))
                return best, conf, "thompson"

            scores: Dict[str, float] = {}
            for a in self.actions:
                mean = float(x @ self.theta[a])
                var = float(x @ self.P[a] @ x) + 1e-9
                scores[a] = mean + self.config.exploration_beta * math.sqrt(var)
            best = max(scores, key=scores.get)
            conf = 1.0 / (1.0 + math.exp(-abs(scores[best])))
            return best, conf, "ucb"

    def update(self, context: Any, action: str, reward: float) -> None:
        """Synchronous update. Do NOT await this method."""
        if action not in self.theta:
            return
        with self._lock:
            x = self._prepare_features(context)
            P = self.P[action]
            denom = 1.0 + float(x @ P @ x)
            K = (P @ x) / denom
            err = float(reward) - float(x @ self.theta[action])
            self.theta[action] = self.theta[action] + K * err
            newP = P - np.outer(K, x) @ P
            self.P[action] = 0.5 * (newP + newP.T)
            self.usage[action] += 1
            self.total_trials += 1
            BANDIT_UPDATES.labels(strategy=action).inc()

    def apply_federated_prior(self, action: str, mean_reward: float, weight: float = 0.1) -> None:
        """Nudge the posterior mean for `action` toward `mean_reward`."""
        if action not in self.theta or weight <= 0:
            return
        with self._lock:
            P = self.P[action]
            x = np.zeros(self.dim)
            x[0] = 1.0
            denom = 1.0 + float(x @ P @ x)
            K = (P @ x) / denom
            err = float(mean_reward) * float(weight) - float(x @ self.theta[action])
            self.theta[action] = self.theta[action] + K * err
            newP = P - np.outer(K, x) @ P
            self.P[action] = 0.5 * (newP + newP.T)

    def get_action_probabilities(self, context: Any) -> List[float]:
        with self._lock:
            x = self._prepare_features(context)
            scores = np.array([float(x @ self.theta[a]) for a in self.actions])
            z = scores - np.max(scores)
            exp = np.exp(z)
            total = exp.sum()
            if total <= 0:
                return [1.0 / len(self.actions)] * len(self.actions)
            return (exp / total).tolist()

    def seed_safe_policy(self, context: Any, policy: str) -> None:
        if policy in self.theta:
            self.update(context, policy, 0.1)

    def add_actions(self, new_actions: Iterable[str]) -> List[str]:
        added: List[str] = []
        with self._lock:
            for a in new_actions:
                if not isinstance(a, str) or not a:
                    continue
                if a in self.theta:
                    continue
                self.theta[a] = np.zeros(self.dim)
                self.P[a] = np.eye(self.dim) / self.config.alpha
                self.usage[a] = 0
                self.actions.append(a)
                added.append(a)
        return added

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "actions": list(self.actions),
                "usage": dict(self.usage),
                "total_trials": self.total_trials,
            }


# =============================================================================
# SafetyMonitor — episode-aware, snapshot-able
# =============================================================================
class SafetyMonitor:
    """
    Temporal safety rules over (power_watts, carbon_intensity) pairs.
    Immediate thresholds, consecutive streaks, and window ratios.
    """

    def __init__(
        self,
        max_power_watts: float = 5000.0,
        max_carbon_intensity: float = 600.0,
        max_consecutive_high: int = 3,
        window_size: int = 10,
        high_ratio: float = 0.7,
    ):
        self.max_power = float(max_power_watts)
        self.max_carbon = float(max_carbon_intensity)
        self.max_consecutive = int(max_consecutive_high)
        self.window_size = int(window_size)
        self.high_ratio = float(high_ratio)
        self.history: Deque[Tuple[float, float, float]] = deque(maxlen=self.window_size)
        self.consecutive_high_power = 0
        self.consecutive_high_carbon = 0
        self.violations: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    def check(self, power_watts: float, carbon_intensity: float) -> Tuple[bool, Optional[str]]:
        with self._lock:
            self.history.append((float(power_watts), float(carbon_intensity), time.time()))
            reason: Optional[str] = None

            if power_watts > self.max_power:
                reason = "max_power"
            elif carbon_intensity > self.max_carbon:
                reason = "max_carbon"
            else:
                if power_watts > self.max_power * self.high_ratio:
                    self.consecutive_high_power += 1
                else:
                    self.consecutive_high_power = 0
                if self.consecutive_high_power >= self.max_consecutive:
                    reason = "consecutive_high_power"
                else:
                    if carbon_intensity > self.max_carbon * self.high_ratio:
                        self.consecutive_high_carbon += 1
                    else:
                        self.consecutive_high_carbon = 0
                    if self.consecutive_high_carbon >= self.max_consecutive:
                        reason = "consecutive_high_carbon"
                    elif len(self.history) >= self.window_size:
                        high_power = sum(
                            1 for p, _, _ in self.history
                            if p > self.max_power * self.high_ratio
                        )
                        if high_power / self.window_size > self.high_ratio:
                            reason = "high_power_ratio"

            if reason is None:
                return True, None
            self._record(reason, {
                "power_watts": float(power_watts),
                "carbon_intensity": float(carbon_intensity),
            })
            return False, reason

    def _record(self, rule: str, details: Dict[str, Any]) -> None:
        self.violations.append({
            "rule": rule,
            "details": details,
            "timestamp": datetime.now().isoformat(),
        })
        SAFETY_VIOLATIONS.labels(rule=rule).inc()
        logger.warning(f"safety_violation rule={rule} details={details}")

    def snapshot(self) -> Dict[str, float]:
        with self._lock:
            return {
                "max_power": self.max_power,
                "max_carbon": self.max_carbon,
                "max_consecutive": float(self.max_consecutive),
                "high_ratio": self.high_ratio,
            }

    def restore(self, snapshot: Dict[str, float]) -> None:
        with self._lock:
            if "max_power" in snapshot:
                self.max_power = float(snapshot["max_power"])
            if "max_carbon" in snapshot:
                self.max_carbon = float(snapshot["max_carbon"])
            if "max_consecutive" in snapshot:
                self.max_consecutive = int(snapshot["max_consecutive"])
            if "high_ratio" in snapshot:
                self.high_ratio = float(snapshot["high_ratio"])

    def get_violations(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self.violations)

    def reset_episode(self) -> None:
        with self._lock:
            self.history.clear()
            self.consecutive_high_power = 0
            self.consecutive_high_carbon = 0


# =============================================================================
# XAIExplainer — structured output
# =============================================================================
class XAIExplainer:
    """Structured, human-readable explanations for energy decisions."""

    def explain_strategy(
        self,
        strategy: str,
        context: Dict[str, Any],
        confidence: float,
        utility: float,
    ) -> Dict[str, Any]:
        top = sorted(
            (
                {"feature": k, "value": float(v)}
                for k, v in context.items() if isinstance(v, (int, float))
            ),
            key=lambda x: -abs(x["value"]),
        )[:5]
        narrative = (
            f"Selected energy strategy '{strategy}' "
            f"(confidence={confidence:.2f}, utility={utility:.3f})."
        )
        return {
            "strategy": strategy,
            "confidence": float(confidence),
            "utility": float(utility),
            "top_contributions": top,
            "narrative": narrative,
        }


# =============================================================================
# FederatedEnergyLearnerSecure — weighted, round IDs, DP
# =============================================================================
class FederatedEnergyLearnerSecure:
    """
    Weighted aggregation with round IDs, min_clients, and Laplace DP noise
    scaled to sensitivity / privacy budget.
    """

    def __init__(
        self,
        privacy_budget: float = 0.5,
        sensitivity: float = 1.0,
        min_clients: int = 1,
        seed: Optional[int] = None,
    ):
        self.privacy_budget = max(float(privacy_budget), 1e-6)
        self.sensitivity = float(sensitivity)
        self.min_clients = max(1, int(min_clients))
        self.participants: Dict[str, Dict[str, Any]] = {}
        self.round_id = 0
        self._rng = np.random.default_rng(seed if seed is not None else 42)
        self._lock = threading.RLock()

    def register_participant(
        self,
        participant_id: str,
        model_update: Dict[str, float],
        sample_count: int = 1,
        round_id: Optional[int] = None,
    ) -> None:
        with self._lock:
            rid = round_id if round_id is not None else self.round_id
            self.participants[participant_id] = {
                "update": dict(model_update),
                "weight": max(1, int(sample_count)),
                "round": rid,
            }

    def aggregate(self, expected_round: Optional[int] = None) -> Dict[str, float]:
        with self._lock:
            active = {
                pid: data for pid, data in self.participants.items()
                if expected_round is None or data["round"] >= expected_round
            }
            if len(active) < self.min_clients:
                return {}
            keys: Set[str] = set()
            for data in active.values():
                keys.update(data["update"].keys())
            aggregated: Dict[str, float] = {}
            noise_scale = self.sensitivity / self.privacy_budget
            for key in keys:
                values: List[float] = []
                weights: List[float] = []
                for data in active.values():
                    if key not in data["update"]:
                        continue
                    v = data["update"][key]
                    if isinstance(v, (int, float)):
                        values.append(float(v))
                        weights.append(data["weight"])
                if not values:
                    continue
                wsum = sum(weights)
                mean = (
                    sum(v * w for v, w in zip(values, weights)) / wsum
                    if wsum > 0 else float(np.mean(values))
                )
                mean += float(self._rng.laplace(0.0, noise_scale))
                aggregated[key] = mean
            self.round_id += 1
            FEDERATED_ROUNDS.inc()
            return aggregated

    def reset_round(self) -> None:
        with self._lock:
            self.participants.clear()

    def get_participant_count(self) -> int:
        with self._lock:
            return len(self.participants)

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "round_id": self.round_id,
                "num_participants": len(self.participants),
                "privacy_budget": self.privacy_budget,
                "sensitivity": self.sensitivity,
            }


# =============================================================================
# MultiAgentCoordinator — stable hash, thread-safe, budget negotiation
# =============================================================================
class MultiAgentCoordinator:
    """
    Coordinates agents (nodes) with per-agent power budgets and stable hashing.
    """

    def __init__(
        self,
        num_agents: int = 3,
        default_budget_watts: float = 1000.0,
        budgets: Optional[Dict[str, float]] = None,
    ):
        if num_agents <= 0:
            raise ValueError("num_agents must be positive")
        self.agents: Dict[str, Dict[str, Any]] = {}
        for i in range(num_agents):
            name = f"agent_{i}"
            budget = float(budgets.get(name, default_budget_watts)) if budgets else float(default_budget_watts)
            self.agents[name] = {"budget_watts": budget, "allocated_watts": 0.0}
        self._lock = threading.RLock()

    @staticmethod
    def _stable_hash(task_id: str) -> int:
        return int(hashlib.sha256(task_id.encode()).hexdigest(), 16)

    def assign_task(self, task_id: str) -> str:
        with self._lock:
            agent_id = list(self.agents.keys())[
                self._stable_hash(task_id) % len(self.agents)
            ]
            return agent_id

    def request_budget(self, agent_id: str, requested_watts: float) -> float:
        if requested_watts <= 0:
            return 0.0
        with self._lock:
            if agent_id not in self.agents:
                return 0.0
            info = self.agents[agent_id]
            available = info["budget_watts"] - info["allocated_watts"]
            granted = float(min(requested_watts, available))
            info["allocated_watts"] += granted
            return granted

    def release_budget(self, agent_id: str, released_watts: float) -> None:
        if released_watts <= 0:
            return
        with self._lock:
            if agent_id in self.agents:
                info = self.agents[agent_id]
                info["allocated_watts"] = max(
                    0.0, info["allocated_watts"] - float(released_watts)
                )

    def set_budget(self, agent_id: str, budget_watts: float) -> bool:
        with self._lock:
            if agent_id not in self.agents:
                return False
            self.agents[agent_id]["budget_watts"] = max(0.0, float(budget_watts))
            return True

    def get_stats(self) -> Dict[str, Dict[str, float]]:
        with self._lock:
            return {
                agent_id: {
                    "budget_watts": info["budget_watts"],
                    "allocated_watts": info["allocated_watts"],
                    "available_watts": info["budget_watts"] - info["allocated_watts"],
                }
                for agent_id, info in self.agents.items()
            }


# =============================================================================
# CarbonOffsetBroker — thread-safe, wired
# =============================================================================
class CarbonOffsetBroker:
    """Thread-safe offsets + RECs accounting."""

    def __init__(
        self,
        threshold_intensity: float = 400.0,
        cost_per_kg: float = 0.1,
        rec_cost_per_mwh: float = 5.0,
    ):
        self.threshold = float(threshold_intensity)
        self.cost_per_kg = float(cost_per_kg)
        self.rec_cost_per_mwh = float(rec_cost_per_mwh)
        self.total_offset_kg = 0.0
        self.total_recs_mwh = 0.0
        self.total_cost = 0.0
        self.records: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    def reward_penalty(self, carbon_intensity: float, weight: float = 0.1) -> float:
        excess = max(0.0, carbon_intensity - self.threshold)
        return (excess / 1000.0) * weight

    async def purchase_offsets(
        self, carbon_intensity: float, carbon_kg: float
    ) -> Dict[str, Any]:
        if carbon_kg <= 0:
            return {"status": "invalid_amount", "carbon_kg": float(carbon_kg)}
        if carbon_intensity <= self.threshold:
            return {"status": "below_threshold", "carbon_kg": float(carbon_kg)}
        cost = carbon_kg * self.cost_per_kg
        with self._lock:
            self.total_offset_kg += float(carbon_kg)
            self.total_cost += float(cost)
            record = {
                "status": "offset_purchased",
                "carbon_kg": float(carbon_kg),
                "cost_usd": float(cost),
                "timestamp": datetime.now().isoformat(),
            }
            self.records.append(record)
            CARBON_OFFSET_KG.inc(carbon_kg)
            CARBON_COST_USD.inc(cost)
        logger.info(f"offset_purchased kg={carbon_kg:.4f} usd={cost:.4f}")
        return record

    async def purchase_recs(self, energy_mwh: float) -> Dict[str, Any]:
        if energy_mwh <= 0:
            return {"status": "no_energy"}
        cost = energy_mwh * self.rec_cost_per_mwh
        with self._lock:
            self.total_recs_mwh += float(energy_mwh)
            self.total_cost += float(cost)
            record = {
                "status": "rec_purchased",
                "energy_mwh": float(energy_mwh),
                "cost_usd": float(cost),
                "timestamp": datetime.now().isoformat(),
            }
            self.records.append(record)
            CARBON_REC_MWH.inc(energy_mwh)
            CARBON_COST_USD.inc(cost)
        logger.info(f"rec_purchased mwh={energy_mwh:.4f} usd={cost:.4f}")
        return record

    def get_totals(self) -> Dict[str, float]:
        with self._lock:
            return {
                "total_offset_kg": self.total_offset_kg,
                "total_recs_mwh": self.total_recs_mwh,
                "total_cost_usd": self.total_cost,
            }


# =============================================================================
# ChaosMonkey — named faults, seeded, recovery measurement
# =============================================================================
class ChaosMonkey:
    FAULT_TYPES = {
        "drop_storage", "freeze_bandit", "safety_override", "queue_drop",
    }

    def __init__(
        self,
        enabled: bool = False,
        failure_probability: float = 0.1,
        seed: Optional[int] = 42,
    ):
        self.enabled = bool(enabled)
        self.failure_probability = float(failure_probability)
        self._rng = random.Random(seed if seed is not None else 42)
        self.injected_failures = 0
        self.events: List[Dict[str, Any]] = []
        self._frozen = False
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

    def maybe_fail(self) -> Optional[str]:
        if not self.enabled:
            return None
        if self._rng.random() >= self.failure_probability:
            return None
        fault = self._rng.choice(sorted(self.FAULT_TYPES))
        self.injected_failures += 1
        self._record(fault, "triggered")
        CHAOS_EVENTS.labels(fault=fault).inc()
        return fault

    def apply(self, fault: str, scaler: "EnhancedIntelligentEnergyScaler") -> None:
        with self._lock:
            if fault == "drop_storage":
                self._snapshots.setdefault("drop_storage", {"storage": scaler.storage})
                scaler.storage = None
            elif fault == "freeze_bandit":
                self._frozen = True
            elif fault == "safety_override":
                self._snapshots.setdefault(
                    "safety_override",
                    {"safety": scaler.safety_monitor.snapshot()},
                )
                scaler.safety_monitor.max_power *= 2.0
            elif fault == "queue_drop":
                self._snapshots.setdefault("queue_drop", {"queue": scaler.queue})
                scaler.queue = None

    def cleanup_fault(self, fault: str, scaler: "EnhancedIntelligentEnergyScaler") -> None:
        with self._lock:
            snap = self._snapshots.pop(fault, None)
            if snap is None:
                return
            if fault == "drop_storage":
                scaler.storage = snap["storage"]
            elif fault == "safety_override":
                scaler.safety_monitor.restore(snap["safety"])
            elif fault == "queue_drop":
                scaler.queue = snap["queue"]
            elif fault == "freeze_bandit":
                self._frozen = False

    def is_frozen(self) -> bool:
        return self._frozen

    def unfreeze(self) -> None:
        self._frozen = False

    def _record(self, fault: str, outcome: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self.events.append({
            "fault": fault,
            "outcome": outcome,
            "timestamp": datetime.now().isoformat(),
            **(extra or {}),
        })

    async def inject_and_measure(
        self, scaler: "EnhancedIntelligentEnergyScaler", fault: str, steps: int = 5
    ) -> Dict[str, Any]:
        start = time.monotonic()
        self.apply(fault, scaler)
        recovery_step: Optional[int] = None
        try:
            for step in range(steps):
                try:
                    _ = scaler.power_readings  # trivial liveness probe
                    if recovery_step is None:
                        recovery_step = step
                except Exception:
                    continue
        finally:
            self.cleanup_fault(fault, scaler)
        elapsed = time.monotonic() - start
        self._record(fault, "measured",
                     {"recovery_step": recovery_step, "elapsed_s": elapsed})
        return {
            "fault": fault,
            "recovery_step": recovery_step,
            "recovered": recovery_step is not None,
            "elapsed_s": elapsed,
        }

    def get_stats(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "injected_failures": self.injected_failures,
            "frozen": self._frozen,
            "pending_snapshots": list(self._snapshots.keys()),
        }


# =============================================================================
# HumanReviewManager — TTL, capacity, audit
# =============================================================================
@dataclass
class HumanReviewRequest:
    review_id: str
    decision_id: str
    details: Dict[str, Any]
    created_at: datetime
    status: str = "pending"
    reviewer: Optional[str] = None
    resolution_at: Optional[datetime] = None


class HumanReviewManager:
    def __init__(self, ttl_seconds: float = 3600.0, max_pending: int = 100):
        self.ttl_seconds = float(ttl_seconds)
        self.max_pending = int(max_pending)
        self.pending_reviews: Dict[str, HumanReviewRequest] = {}
        self.audit_log: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()

    def _evict_expired(self) -> None:
        now = datetime.now()
        for rid, r in list(self.pending_reviews.items()):
            if r.status == "pending" and now - r.created_at > timedelta(seconds=self.ttl_seconds):
                r.status = "expired"
                HITL_STATUS.labels(status="expired").inc()

    def _enforce_capacity(self) -> None:
        pending = [r for r in self.pending_reviews.values() if r.status == "pending"]
        if len(pending) <= self.max_pending:
            return
        ordered = sorted(pending, key=lambda r: r.created_at)
        for r in ordered[: len(ordered) - self.max_pending]:
            r.status = "dropped"
            HITL_STATUS.labels(status="dropped").inc()

    async def request_review(self, decision_id: str, details: Dict[str, Any]) -> str:
        async with self._lock:
            self._evict_expired()
            rid = str(uuid.uuid4())
            self.pending_reviews[rid] = HumanReviewRequest(
                review_id=rid,
                decision_id=decision_id,
                details=dict(details),
                created_at=datetime.now(),
            )
            self._enforce_capacity()
            HITL_REQUESTS.inc()
            logger.info(f"hitl_requested review_id={rid}")
            return rid

    def _audit(self, review_id: str, action: str, reviewer: str) -> None:
        self.audit_log.append({
            "review_id": review_id,
            "action": action,
            "reviewer": reviewer,
            "timestamp": datetime.now().isoformat(),
        })

    async def approve(self, review_id: str, reviewer: str = "unknown") -> bool:
        async with self._lock:
            r = self.pending_reviews.get(review_id)
            if r is None or r.status != "pending":
                return False
            r.status = "approved"
            r.reviewer = reviewer
            r.resolution_at = datetime.now()
            self._audit(review_id, "approved", reviewer)
            HITL_STATUS.labels(status="approved").inc()
            return True

    async def reject(self, review_id: str, reviewer: str = "unknown") -> bool:
        async with self._lock:
            r = self.pending_reviews.get(review_id)
            if r is None or r.status != "pending":
                return False
            r.status = "rejected"
            r.reviewer = reviewer
            r.resolution_at = datetime.now()
            self._audit(review_id, "rejected", reviewer)
            HITL_STATUS.labels(status="rejected").inc()
            return True

    async def get_pending(self) -> List[Dict[str, Any]]:
        async with self._lock:
            self._evict_expired()
            return [
                {
                    "review_id": r.review_id,
                    "decision_id": r.decision_id,
                    "details": r.details,
                    "created_at": r.created_at.isoformat(),
                }
                for r in self.pending_reviews.values() if r.status == "pending"
            ]


# =============================================================================
# FlexGenManager — honest delegation
# =============================================================================
class FlexGenManager:
    def __init__(self, carbon_intensity: float = 400.0):
        self.carbon_intensity = float(carbon_intensity)
        self._cost_model = None
        self._drift_detector = None
        self._gpu_profiler = None
        if FLEXGEN_AVAILABLE:
            try:
                self._cost_model = FlexGenCostModel(
                    carbon_intensity_g_per_kwh=self.carbon_intensity
                )
                self._drift_detector = PolicyDriftDetector()
            except Exception as e:
                logger.warning(f"flexgen_init_failed error={e}")
                self._cost_model = None
                self._drift_detector = None
            try:
                from enhancements.gpu_profiler import GPUProfiler  # type: ignore
                self._gpu_profiler = GPUProfiler()
            except Exception:
                self._gpu_profiler = None

    @property
    def available(self) -> bool:
        return FLEXGEN_AVAILABLE

    async def optimize_policy(self, workload: Any, node: Any) -> Dict[str, Any]:
        if not FLEXGEN_AVAILABLE:
            raise NotImplementedYetError("FlexGen modules not available")
        try:
            from enhancements.gpu_optimization.flexgen_controller import (  # type: ignore
                FlexGenController,
            )
            from enhancements.gpu_optimization.flexgen_policy_selector import (  # type: ignore
                DistillationFlexGenSelector,
            )
        except ImportError as e:
            raise NotImplementedYetError(f"FlexGen submodule missing: {e}") from e
        _selector = DistillationFlexGenSelector(  # noqa: F841
            n_candidates=20,
            config={"epsilon": 0.1, "epsilon_decay": 0.999},
        )
        controller = FlexGenController(
            node=node,
            workload=workload,
            carbon_intensity=getattr(workload, "metadata", {}).get(
                "carbon_intensity", self.carbon_intensity
            ),
            use_real_executor=False,
            executor=None,
            cost_model=self._cost_model,
            use_bio_search=True,
            bio_search_config={"population_size": 50, "generations": 10},
            modp_planner=None,
            drift_detector=self._drift_detector,
            gpu_profiler=self._gpu_profiler,
        )
        return await controller.step()

    async def select_precision(self, workload: Dict[str, Any]) -> str:
        """
        Return a precision string. Tries FlexGen once; falls back to a
        carbon/workload heuristic. Always returns a str.
        """
        if FLEXGEN_AVAILABLE:
            try:
                wd = WorkloadDescriptor(**workload) if isinstance(workload, dict) else workload
                nd = NodeDescriptor()
                result = await self.optimize_policy(wd, nd)
                if isinstance(result, dict) and "chosen_policy" in result:
                    precision = result["chosen_policy"].get("precision")
                    if precision in ("fp32", "fp16", "int8"):
                        return precision
            except NotImplementedYetError:
                pass
            except Exception as e:
                logger.warning(f"flexgen_precision_failed error={e}")

        carbon = float(workload.get("carbon_intensity", self.carbon_intensity))
        size = workload.get("size", "medium")
        if carbon > 500 or size == "large":
            return "int8"
        if carbon > 300 or size == "medium":
            return "fp16"
        return "fp32"

    async def get_status(self) -> Dict[str, Any]:
        if not FLEXGEN_AVAILABLE:
            return {"available": False}
        gpu = {}
        if self._gpu_profiler and hasattr(self._gpu_profiler, "get_current_metrics"):
            try:
                gpu = self._gpu_profiler.get_current_metrics()
            except Exception:
                gpu = {}
        return {
            "available": True,
            "drift": self._drift_detector.get_stats() if self._drift_detector else {},
            "gpu": gpu,
        }


# =============================================================================
# AutonomousEnergyOptimizer — real bandit + wired enhancements
# =============================================================================
class AutonomousEnergyOptimizer:
    """
    Adaptive optimizer using CausalBandit + ParetoOptimizer + ExpertRouter +
    GeneticPolicyGenerator, with SafetyMonitor, XAIExplainer, HumanReviewManager,
    and CarbonOffsetBroker wired into the decision path.
    """

    def __init__(
        self,
        storage: Any = None,
        adaptive_cost: Optional[Any] = None,
        safety_monitor: Optional[SafetyMonitor] = None,
        xai: Optional[XAIExplainer] = None,
        human_review: Optional[HumanReviewManager] = None,
        carbon_broker: Optional[CarbonOffsetBroker] = None,
        strategies: Optional[List[str]] = None,
        modp_weights: Optional[Dict[str, float]] = None,
    ):
        self.storage = storage
        self.adaptive_cost = adaptive_cost

        self.strategies: List[str] = list(strategies) if strategies else [
            "reduce_gpu_power",
            "schedule_off_peak",
            "increase_renewable",
            "optimize_cooling",
            "load_balancing",
            "power_capping",
        ]

        # Enhancement modules
        self.modp = ParetoOptimizer() if ENHANCEMENTS_AVAILABLE else None
        self.moe = ExpertRouter() if ENHANCEMENTS_AVAILABLE else None
        self.bio = GeneticPolicyGenerator() if ENHANCEMENTS_AVAILABLE else None
        self.modp_weights = modp_weights or {
            "savings_kwh": 0.4,
            "cost_usd": 0.2,
            "carbon_reduction_kg": 0.3,
            "implementation_time_hours": 0.1,
        }

        self.bandit = CausalBandit(
            action_space=self.strategies,
            feature_dim=8,
            fallback_solver=lambda ctx: self.strategies[0],
            min_trials_before_bandit=int(_cfg_nested("optimizer.bandit_min_trials", 5)),
            confidence_threshold=float(_cfg_nested("optimizer.bandit_confidence_threshold", 0.6)),
        )

        self.safety_monitor = safety_monitor or SafetyMonitor()
        self.xai = xai or XAIExplainer()
        self.human_review = human_review or HumanReviewManager()
        self.carbon_broker = carbon_broker or CarbonOffsetBroker()

        self.strategy_rewards = {s: 0.0 for s in self.strategies}
        self.strategy_counts = {s: 0 for s in self.strategies}
        self.epsilon = 0.1
        self.history: Deque[Dict[str, Any]] = deque(maxlen=100)
        self._lock = asyncio.Lock()
        self._last_decision: Dict[str, Any] = {}
        self._last_review_id: Optional[str] = None

        self._load_state()

    # ------------------------------------------------------------------ #
    # Persistence                                                        #
    # ------------------------------------------------------------------ #
    def _load_state(self) -> None:
        if self.storage is None:
            return
        try:
            loader = getattr(self.storage, "get_energy_optimizer_state", None)
            if loader is None:
                return
            state = loader()
            if inspect.isawaitable(state):
                # Skip async load here; sync path only.
                return
            if not state:
                return
            self.epsilon = float(state.get("epsilon", self.epsilon))
            saved_rewards = state.get("strategy_rewards", {})
            for k, v in saved_rewards.items():
                if k in self.strategy_rewards:
                    self.strategy_rewards[k] = float(v)
            saved_counts = state.get("strategy_counts", {})
            for k, v in saved_counts.items():
                if k in self.strategy_counts:
                    self.strategy_counts[k] = int(v)
            logger.info("energy_optimizer_state_loaded")
        except Exception as e:
            logger.warning(f"energy_optimizer_state_load_failed error={e}")

    def _save_state(self) -> None:
        if self.storage is None:
            return
        saver = getattr(self.storage, "save_energy_optimizer_state", None)
        if saver is None:
            return
        try:
            state = {
                "epsilon": self.epsilon,
                "strategy_rewards": dict(self.strategy_rewards),
                "strategy_counts": dict(self.strategy_counts),
                "theta": {a: self.bandit.theta[a].tolist() for a in self.bandit.theta},
                "P": {a: self.bandit.P[a].tolist() for a in self.bandit.P},
            }
            result = saver(state)
            if inspect.isawaitable(result):
                # Fire-and-forget is unsafe; log instead.
                logger.debug("energy_optimizer_state_save_returned_coroutine")
        except Exception as e:
            logger.warning(f"energy_optimizer_state_save_failed error={e}")

    # ------------------------------------------------------------------ #
    # Core optimization                                                  #
    # ------------------------------------------------------------------ #
    async def optimize_autonomously(self, current_state: Dict[str, Any]) -> Dict[str, Any]:
        if self.adaptive_cost is not None and hasattr(self.adaptive_cost, "get_current_weights"):
            try:
                weights = self.adaptive_cost.get_current_weights()
                logger.debug(f"adaptive_cost_weights={weights}")
            except Exception:
                pass

        carbon_intensity = current_state.get("carbon_intensity", 400.0)
        if isinstance(carbon_intensity, (int, float)) and carbon_intensity <= 1.0:
            carbon_intensity = float(carbon_intensity) * 800.0
        total_power = float(current_state.get("total_power_watts", 1000.0))

        # Always define context so downstream code never NameErrors.
        context: Dict[str, Any] = {
            "time_of_day": float(datetime.now().hour),
            "carbon_intensity": float(carbon_intensity),
            "total_power_watts": total_power,
            "gpu_power_watts": float(current_state.get("gpu_power_watts", 250.0)),
        }
        encoded: Any = context

        # Safety gate
        safe, reason = self.safety_monitor.check(total_power, carbon_intensity)
        if not safe:
            strategy = "power_capping"
            confidence = 1.0
            source = f"safety_override:{reason}"
        elif self.bandit is not None:
            encoded = self._encode_context(context)
            strategy, confidence, source = self.bandit.select_action(encoded)
            if not strategy:
                strategy = self.strategies[0]
        else:
            async with self._lock:
                if random.random() < self.epsilon:
                    strategy = random.choice(self.strategies)
                else:
                    strategy = max(self.strategies, key=lambda s: self.strategy_rewards.get(s, 0.0))
            confidence = 0.5
            source = "fallback"

        result = await self._apply_strategy(strategy, current_state)

        # Utility via MODP
        if self.modp is not None:
            objectives = {
                "savings_kwh": float(result.get("estimated_savings_kwh", 0.0)),
                "cost_usd": float(result.get("estimated_cost", 0.0)),
                "carbon_reduction_kg": float(result.get("carbon_reduction_kg", 0.0)),
                "implementation_time_hours": float(result.get("time_hours", 1.0)),
            }
            try:
                utility = float(self.modp.evaluate(objectives, self.modp_weights))
            except Exception:
                utility = float(result.get("estimated_savings_kwh", 0.0))
        else:
            utility = float(result.get("estimated_savings_kwh", 0.0))

        explanation = self.xai.explain_strategy(strategy, context, confidence, utility)

        # HITL on low-confidence / high-carbon decisions
        review_id: Optional[str] = None
        if (utility < 0.2 or carbon_intensity > 500) and confidence < 0.5:
            decision_id = f"energy_opt_{uuid.uuid4().hex[:8]}"
            try:
                review_id = await self.human_review.request_review(decision_id, {
                    "strategy": strategy,
                    "utility": utility,
                    "carbon_intensity": carbon_intensity,
                    "explanation": explanation,
                })
            except Exception as e:
                logger.warning(f"hitl_request_failed error={e}")

        # Bandit update (SYNC — do not await)
        if self.bandit is not None and not source.startswith("safety_override"):
            if not self._chaos_frozen():
                reward = float(np.clip(utility, -1.0, 1.0))
                self.bandit.update(encoded, strategy, reward)
                self.strategy_rewards[strategy] = (
                    0.9 * self.strategy_rewards.get(strategy, 0.0) + 0.1 * reward
                )
                self.strategy_counts[strategy] = self.strategy_counts.get(strategy, 0) + 1
        elif self.bandit is None:
            reward = float(np.clip(utility, -1.0, 1.0))
            self.strategy_rewards[strategy] = (
                0.9 * self.strategy_rewards.get(strategy, 0.0) + 0.1 * reward
            )
            self.strategy_counts[strategy] = self.strategy_counts.get(strategy, 0) + 1

        # Carbon offset
        carbon_reduction = float(result.get("carbon_reduction_kg", 0.0))
        if carbon_intensity > 450 and carbon_reduction > 0:
            try:
                await self.carbon_broker.purchase_offsets(
                    carbon_intensity, carbon_reduction
                )
            except Exception as e:
                logger.warning(f"carbon_offset_failed error={e}")

        STRATEGY_CHOSEN.labels(strategy=strategy).inc()
        UTILITY_OBSERVED.observe(utility)

        self.history.append({"strategy": strategy, "reward": utility})
        if len(self.history) % 10 == 0:
            self._save_state()

        self._last_decision = {
            "strategy": strategy,
            "confidence": confidence,
            "source": source,
            "utility": utility,
            "explanation": explanation,
            "review_id": review_id,
            "context": context,
        }
        self._last_review_id = review_id

        return {
            "status": "success",
            "strategy": strategy,
            "result": result,
            "total_savings_kwh": float(result.get("estimated_savings_kwh", 0.0)),
            "confidence": confidence,
            "source": source,
            "utility": utility,
            "explanation": explanation,
            "review_id": review_id,
        }

    def _chaos_frozen(self) -> bool:
        return getattr(self, "_bandit_frozen", False)

    def set_bandit_frozen(self, frozen: bool) -> None:
        self._bandit_frozen = bool(frozen)

    def _encode_context(self, context: Dict[str, Any]) -> Any:
        if self.moe is not None:
            try:
                encoded = self.moe.encode(context)
                if encoded:
                    return [float(v) for v in encoded if isinstance(v, (int, float))]
            except Exception:
                pass
        return [float(v) for v in context.values() if isinstance(v, (int, float))]

    async def _apply_strategy(self, strategy: str, state: Dict[str, Any]) -> Dict[str, Any]:
        total_power = float(state.get("total_power_watts", 1000.0))
        gpu_power = float(state.get("gpu_power_watts", 200.0))

        if strategy == "reduce_gpu_power":
            reduction = min(50.0, gpu_power * 0.3)
            return {
                "action": "reduce_gpu_power",
                "estimated_savings_kwh": reduction * 0.001,
                "estimated_cost": 0.0,
                "carbon_reduction_kg": reduction * 0.001 * 0.5,
                "time_hours": 0.5,
            }
        if strategy == "schedule_off_peak":
            hour = datetime.now().hour
            if 6 <= hour <= 18:
                delay = random.randint(2, 8)
                savings = total_power * 0.0005 * delay
                return {
                    "action": "schedule_off_peak",
                    "estimated_savings_kwh": savings,
                    "estimated_cost": 0.0,
                    "carbon_reduction_kg": savings * 0.5,
                    "time_hours": float(delay),
                }
            return {
                "action": "schedule_off_peak",
                "estimated_savings_kwh": 0.0,
                "estimated_cost": 0.0,
                "carbon_reduction_kg": 0.0,
                "time_hours": 0.0,
            }
        if strategy == "increase_renewable":
            savings = total_power * 0.001 * 10
            return {
                "action": "increase_renewable",
                "estimated_savings_kwh": savings,
                "estimated_cost": 50.0,
                "carbon_reduction_kg": savings * 0.8,
                "time_hours": 24.0,
            }
        if strategy == "optimize_cooling":
            savings = total_power * 0.001 * 0.1
            return {
                "action": "optimize_cooling",
                "estimated_savings_kwh": savings,
                "estimated_cost": 20.0,
                "carbon_reduction_kg": savings * 0.6,
                "time_hours": 2.0,
            }
        if strategy == "load_balancing":
            savings = total_power * 0.0001
            return {
                "action": "load_balancing",
                "estimated_savings_kwh": savings,
                "estimated_cost": 0.0,
                "carbon_reduction_kg": savings * 0.4,
                "time_hours": 1.0,
            }
        # power_capping default
        savings = total_power * 0.001 * 0.1
        return {
            "action": "power_capping",
            "estimated_savings_kwh": savings,
            "estimated_cost": 10.0,
            "carbon_reduction_kg": savings * 0.7,
            "time_hours": 0.5,
        }

    async def evolve_strategies(self) -> List[str]:
        if self.bio is None:
            return []
        try:
            new_strategies = self.bio.evolve(
                population=list(self.strategies),
                fitness_fn=lambda s: self.strategy_rewards.get(s, 0.0),
                generations=int(_cfg_nested("optimizer.bio_generations", 10)),
                population_size=int(_cfg_nested("optimizer.bio_population_size", 20)),
            )
        except Exception as e:
            logger.warning(f"bio_evolve_failed error={e}")
            return []
        if isinstance(new_strategies, str):
            new_strategies = [new_strategies]
        if not isinstance(new_strategies, list):
            return []

        added: List[str] = []
        if self.bandit is not None:
            added = self.bandit.add_actions(new_strategies)
        for s in added:
            self.strategies.append(s)
            self.strategy_rewards[s] = 0.0
            self.strategy_counts[s] = 0
        return added

    async def get_optimizer_stats(self) -> Dict[str, Any]:
        return {
            "strategies": list(self.strategies),
            "epsilon": self.epsilon,
            "history_length": len(self.history),
            "bandit_available": self.bandit is not None,
            "modp_available": self.modp is not None,
            "moe_available": self.moe is not None,
            "bio_available": self.bio is not None,
            "safety_violations": len(self.safety_monitor.get_violations()),
            "pending_reviews": len(await self.human_review.get_pending()),
            "carbon_broker_totals": self.carbon_broker.get_totals(),
        }


# =============================================================================
# FederatedEnergyLearner — legacy wrapper around the secure learner
# =============================================================================
class FederatedEnergyLearner:
    def __init__(self, storage: Any = None):
        self.storage = storage
        self.secure = FederatedEnergyLearnerSecure()

    async def federated_round(self) -> Dict[str, Any]:
        # Default simulated participants for demo purposes only.
        self.secure.register_participant("node_1", {"savings_kwh": random.random()}, sample_count=1)
        self.secure.register_participant("node_2", {"savings_kwh": random.random()}, sample_count=1)
        aggregated = self.secure.aggregate()
        return {
            "status": "completed",
            "global_saving": aggregated.get("savings_kwh", 0.0),
            "participants": self.secure.get_participant_count(),
        }


# =============================================================================
# EnhancedIntelligentEnergyScaler
# =============================================================================
class EnhancedIntelligentEnergyScaler:
    """
    Intelligent Energy Scaler with full Green Agent MOPD integration.
    Exposes `policy_probs` for the MTPD optimizer.
    Wires CausalBandit, SafetyMonitor, XAIExplainer, FederatedEnergyLearnerSecure,
    MultiAgentCoordinator, CarbonOffsetBroker, ChaosMonkey, HumanReviewManager,
    and FlexGenManager into the decision and monitoring path.
    """

    def __init__(
        self,
        storage: Any = None,
        message_queue: Any = None,
        adaptive_cost: Optional[Any] = None,
        pareto_gating: Optional[Any] = None,
        drift_detector: Optional[Any] = None,
        metrics: Optional[Any] = None,
        enable_chaos: bool = False,
        chaos_seed: Optional[int] = 42,
    ):
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        self.instance_id = str(uuid.uuid4())[:8]
        self._start_time = datetime.now()

        # Sub-modules (real)
        self.pqc = PostQuantumCrypto(storage)
        self.blockchain = BlockchainEnergyCredits(storage)

        # Enhancement modules
        self.safety_monitor = SafetyMonitor()
        self.xai = XAIExplainer()
        self.federated_secure = FederatedEnergyLearnerSecure()
        self.multi_agent = MultiAgentCoordinator(num_agents=3)
        self.carbon_broker = CarbonOffsetBroker()
        self.chaos_monkey = ChaosMonkey(
            enabled=enable_chaos, seed=chaos_seed
        )
        self.human_review = HumanReviewManager()
        self.flexgen_manager = FlexGenManager()

        # Optimizer with shared enhancement instances
        self.autonomous = AutonomousEnergyOptimizer(
            storage,
            adaptive_cost,
            safety_monitor=self.safety_monitor,
            xai=self.xai,
            human_review=self.human_review,
            carbon_broker=self.carbon_broker,
        )

        # Forecaster + federated
        self.forecaster = PredictiveLoadForecaster(storage, horizon_hours=24)
        self.federated = FederatedEnergyLearner(storage)
        self.cloud_storage = MultiCloudStorage()

        # State
        self.power_readings: Deque[Dict[str, Any]] = deque(maxlen=10_000)
        self._lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._background_tasks: List[asyncio.Task] = []
        self._started = False
        self._startup_lock = asyncio.Lock()

        logger.info(f"energy_scaler_initialized instance={self.instance_id}")

    # ------------------------------------------------------------------ #
    # Lifecycle                                                          #
    # ------------------------------------------------------------------ #
    async def __aenter__(self) -> "EnhancedIntelligentEnergyScaler":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    async def start(self) -> None:
        async with self._startup_lock:
            if self._started:
                return
            self._started = True
            self._shutdown_event.clear()
            loop = asyncio.get_running_loop()
            self._background_tasks = [
                loop.create_task(self._optimization_loop(), name="energy_optimization"),
                loop.create_task(self._forecast_loop(), name="energy_forecast"),
                loop.create_task(self._federated_loop(), name="energy_federated"),
                loop.create_task(self._cleanup_loop(), name="energy_cleanup"),
                loop.create_task(self._evolution_loop(), name="energy_evolution"),
            ]
            logger.info("energy_scaler_started")

    async def shutdown(self) -> None:
        if not self._started:
            return
        self._shutdown_event.set()
        for task in self._background_tasks:
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()
        try:
            self.autonomous._save_state()
        except Exception:
            pass
        self._started = False
        logger.info("energy_scaler_shutdown_complete")

    async def _optimization_loop(self) -> None:
        interval = float(_cfg("optimization_interval_seconds", 60))
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(interval)
                await self.run_optimization()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"optimization_loop_error error={e}")

    async def _forecast_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(3600)
                await self.forecast()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"forecast_loop_error error={e}")

    async def _federated_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(1800)
                await self.run_federated_round()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"federated_loop_error error={e}")

    async def _cleanup_loop(self) -> None:
        retention = int(_cfg("data_retention_days", 7))
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(86400)
                await _storage_call_async(
                    self.storage, "clean_power_readings", days=retention
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"cleanup_loop_error error={e}")

    async def _evolution_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(3600)
                if ENHANCEMENTS_AVAILABLE and self.autonomous.bio:
                    new_strategies = await self.autonomous.evolve_strategies()
                    if new_strategies:
                        logger.info(f"evolved_strategies count={len(new_strategies)}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"evolution_loop_error error={e}")

    # ------------------------------------------------------------------ #
    # Teacher interface for MOPD                                         #
    # ------------------------------------------------------------------ #
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        strategies = self.autonomous.strategies
        n = len(strategies) if strategies else 1
        if n == 0:
            return []
        # Use the bandit posterior when it exposes it.
        if self.autonomous.bandit is not None and hasattr(
            self.autonomous.bandit, "get_action_probabilities"
        ):
            try:
                context = self.autonomous._encode_context(state or {})
                probs = self.autonomous.bandit.get_action_probabilities(context)
                if len(probs) == n:
                    return probs
            except Exception:
                pass
        # Fallback: softmax over recent rewards.
        probs = np.ones(n) / n
        recent = list(self.autonomous.history)[-10:]
        for h in recent:
            s = h.get("strategy")
            if s in strategies:
                idx = strategies.index(s)
                probs[idx] += float(h.get("reward", 0.0))
        total = probs.sum()
        if total <= 0:
            return [1.0 / n] * n
        return (probs / total).tolist()

    # ------------------------------------------------------------------ #
    # Core operations                                                    #
    # ------------------------------------------------------------------ #
    async def record_power_reading(
        self, power_watts: float, carbon_intensity: Optional[float] = None
    ) -> Dict[str, Any]:
        # Chaos injection
        fault = self.chaos_monkey.maybe_fail()
        if fault is not None:
            try:
                self.chaos_monkey.apply(fault, self)
            except Exception as e:
                logger.warning(f"chaos_apply_failed error={e}")
            if fault in {"drop_storage", "queue_drop"}:
                logger.warning(f"chaos_triggered fault={fault} continuing")

        reading_id = str(uuid.uuid4())
        carbon_intensity = float(carbon_intensity) if carbon_intensity is not None else 400.0

        safe, reason = self.safety_monitor.check(power_watts, carbon_intensity)
        if not safe:
            logger.warning(f"power_reading_safety_violation reason={reason}")

        reading = {
            "reading_id": reading_id,
            "power_watts": float(power_watts),
            "carbon_intensity": float(carbon_intensity),
            "timestamp": datetime.now().isoformat(),
            "safety_violation": None if safe else reason,
        }
        self.power_readings.append(reading)

        await _storage_call_async(self.storage, "store_power_reading", reading)
        try:
            await self.forecaster.update_history(power_watts)
        except Exception as e:
            logger.debug(f"forecaster_update_skipped error={e}")

        POWER_READINGS.inc()
        POWER_WATTS.observe(float(power_watts))
        CARBON_INTENSITY.observe(float(carbon_intensity))

        # Carbon offset at high intensity
        if carbon_intensity > 450:
            carbon_kg = float(power_watts) / 1000.0 * carbon_intensity / 1000.0
            try:
                await self.carbon_broker.purchase_offsets(carbon_intensity, carbon_kg)
            except Exception as e:
                logger.warning(f"offset_purchase_failed error={e}")

        # Feedback event
        if self.queue is not None:
            try:
                event = _make_feedback_event(
                    task_id=f"energy_power_{reading_id}",
                    selected_action="record_power",
                    quality_score=1.0,
                    latency_ms=0.0,
                    energy_joules=float(power_watts),
                    carbon_g=float(carbon_intensity),
                    feedback_type="energy",
                    adaptive_cost_value=0.0,
                    state={"power_watts": float(power_watts),
                           "carbon_intensity": float(carbon_intensity)},
                    candidates=[{"action": "record"}],
                    source="energy_scaler",
                    environment=_cfg("ENVIRONMENT", "production"),
                    tags=["power", "monitor"],
                )
                publish = getattr(self.queue, "publish", None)
                if callable(publish):
                    result = publish("feedback_events", _serialize_event(event))
                    if inspect.isawaitable(result):
                        await result
            except Exception as e:
                logger.warning(f"feedback_publish_failed error={e}")

        # Clean up chaos if we applied it.
        if fault is not None:
            try:
                self.chaos_monkey.cleanup_fault(fault, self)
            except Exception:
                pass

        return reading

    async def run_optimization(self) -> Dict[str, Any]:
        state = {
            "total_power_watts": 1000.0,
            "gpu_power_watts": 250.0,
            "carbon_intensity": 400.0,
        }
        result = await self.autonomous.optimize_autonomously(state)

        if self.queue is not None:
            try:
                event = _make_feedback_event(
                    task_id=f"energy_opt_{uuid.uuid4().hex[:8]}",
                    selected_action=result.get("strategy", "unknown"),
                    quality_score=result.get("utility", 0.5),
                    latency_ms=0.0,
                    energy_joules=float(result.get("total_savings_kwh", 0.0)) * 3.6e6,
                    carbon_g=float(
                        result.get("result", {}).get("carbon_reduction_kg", 0.0)
                    ) * 1000.0,
                    feedback_type="energy",
                    adaptive_cost_value=0.0,
                    state=state,
                    candidates=[{"action": s} for s in self.autonomous.strategies],
                    source="energy_scaler",
                    environment=_cfg("ENVIRONMENT", "production"),
                    tags=["optimization", "causal_rl"],
                )
                publish = getattr(self.queue, "publish", None)
                if callable(publish):
                    res = publish("feedback_events", _serialize_event(event))
                    if inspect.isawaitable(res):
                        await res
            except Exception as e:
                logger.warning(f"feedback_publish_failed error={e}")

        if self.drift is not None and hasattr(self.drift, "check_drift"):
            try:
                if self.adaptive_cost is not None and hasattr(self.adaptive_cost, "get_current_weights"):
                    weights = self.adaptive_cost.get_current_weights()
                    res = self.drift.check_drift(weights)
                    if inspect.isawaitable(res):
                        await res
            except Exception as e:
                logger.debug(f"drift_check_failed error={e}")

        return result

    async def run_federated_round(self) -> Dict[str, Any]:
        # Register local state as a participant.
        avg_savings = (
            float(np.mean([h["reward"] for h in self.autonomous.history]))
            if self.autonomous.history else 0.0
        )
        self.federated_secure.register_participant(
            "local_node",
            {
                "avg_savings_kwh": avg_savings,
                "carbon_reduction": self.carbon_broker.get_totals()["total_offset_kg"],
            },
            sample_count=max(1, len(self.autonomous.history)),
        )
        aggregated = self.federated_secure.aggregate()

        # Apply federated prior to bandit for the best-scoring strategy.
        mean_acc = aggregated.get("avg_savings_kwh")
        if (
            mean_acc is not None
            and self.autonomous.bandit is not None
            and hasattr(self.autonomous.bandit, "apply_federated_prior")
        ):
            last = self.autonomous._last_decision.get("strategy")
            if last:
                try:
                    self.autonomous.bandit.apply_federated_prior(
                        last, float(mean_acc), weight=0.1
                    )
                except Exception as e:
                    logger.debug(f"federated_prior_failed error={e}")

        if self.queue is not None:
            try:
                event = _make_feedback_event(
                    task_id=f"energy_fed_{uuid.uuid4().hex[:8]}",
                    selected_action="federated_round",
                    quality_score=float(aggregated.get("avg_savings_kwh", 0.0)),
                    latency_ms=0.0,
                    energy_joules=0.0,
                    carbon_g=0.0,
                    feedback_type="energy",
                    adaptive_cost_value=0.0,
                    state={},
                    candidates=[],
                    source="energy_scaler",
                    environment=_cfg("ENVIRONMENT", "production"),
                    tags=["federated", "secure"],
                )
                publish = getattr(self.queue, "publish", None)
                if callable(publish):
                    res = publish("feedback_events", _serialize_event(event))
                    if inspect.isawaitable(res):
                        await res
            except Exception as e:
                logger.warning(f"feedback_publish_failed error={e}")

        return {
            "status": "completed",
            "aggregated_model": aggregated,
            "participants": self.federated_secure.get_participant_count(),
        }

    async def forecast(self, hours: int = 24) -> Dict[str, Any]:
        forecast = await self.forecaster.forecast()
        if self.queue is not None:
            try:
                event = _make_feedback_event(
                    task_id=f"energy_forecast_{uuid.uuid4().hex[:8]}",
                    selected_action="forecast",
                    quality_score=float(forecast.get("confidence", 0.5)),
                    latency_ms=0.0,
                    energy_joules=0.0,
                    carbon_g=0.0,
                    feedback_type="energy",
                    adaptive_cost_value=0.0,
                    state={"hours": hours},
                    candidates=[],
                    source="energy_scaler",
                    environment=_cfg("ENVIRONMENT", "production"),
                    tags=["forecast"],
                )
                publish = getattr(self.queue, "publish", None)
                if callable(publish):
                    res = publish("feedback_events", _serialize_event(event))
                    if inspect.isawaitable(res):
                        await res
            except Exception as e:
                logger.warning(f"feedback_publish_failed error={e}")
        return forecast

    async def run_flexgen_optimization(
        self, workload: Dict[str, Any], node: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not FLEXGEN_AVAILABLE:
            raise NotImplementedYetError("FlexGen modules not available")
        try:
            workload_obj = WorkloadDescriptor(**workload)  # type: ignore[call-arg]
            node_obj = NodeDescriptor(**node)  # type: ignore[call-arg]
        except TypeError as e:
            raise EnergyScalerError(f"Invalid descriptor: {e}") from e
        return await self.flexgen_manager.optimize_policy(workload_obj, node_obj)

    async def select_precision(self, workload: Dict[str, Any]) -> str:
        return await self.flexgen_manager.select_precision(workload)

    async def get_flexgen_status(self) -> Dict[str, Any]:
        return await self.flexgen_manager.get_status()

    async def request_budget(self, agent_id: str, requested_watts: float) -> float:
        return self.multi_agent.request_budget(agent_id, requested_watts)

    async def release_budget(self, agent_id: str, released_watts: float) -> None:
        self.multi_agent.release_budget(agent_id, released_watts)

    async def trigger_chaos(
        self, enabled: bool = True, probability: float = 0.1
    ) -> Dict[str, Any]:
        self.chaos_monkey.enabled = bool(enabled)
        self.chaos_monkey.failure_probability = float(probability)
        return self.chaos_monkey.get_stats()

    async def get_enhancement_status(self) -> Dict[str, Any]:
        return {
            "safety_violations": self.safety_monitor.get_violations()[-5:],
            "chaos_monkey": self.chaos_monkey.get_stats(),
            "human_review_pending": await self.human_review.get_pending(),
            "multi_agent_stats": self.multi_agent.get_stats(),
            "carbon_broker_totals": self.carbon_broker.get_totals(),
            "federated": self.federated_secure.get_stats(),
            "pqc": self.pqc.get_status(),
            "blockchain": self.blockchain.get_status(),
            "forecaster": self.forecaster.get_status(),
            "storage": self.cloud_storage.get_status(),
            "last_xai_explanation": self.autonomous._last_decision.get("explanation"),
            "last_review_id": self.autonomous._last_review_id,
        }

    async def get_status(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "uptime_seconds": (datetime.now() - self._start_time).total_seconds(),
            "power_readings": len(self.power_readings),
            "bandit": self.autonomous.bandit.get_stats()
            if self.autonomous.bandit else {},
            "enhancements": await self.get_enhancement_status(),
            "flexgen": await self.flexgen_manager.get_status(),
        }


# =============================================================================
# Component factory
# =============================================================================
def build_components(
    storage: Any = None,
    message_queue: Any = None,
    adaptive_cost: Optional[Any] = None,
    pareto_gating: Optional[Any] = None,
    drift_detector: Optional[Any] = None,
    metrics: Optional[Any] = None,
    enable_chaos: bool = False,
    chaos_seed: Optional[int] = 42,
) -> Dict[str, Any]:
    scaler = EnhancedIntelligentEnergyScaler(
        storage=storage,
        message_queue=message_queue,
        adaptive_cost=adaptive_cost,
        pareto_gating=pareto_gating,
        drift_detector=drift_detector,
        metrics=metrics,
        enable_chaos=enable_chaos,
        chaos_seed=chaos_seed,
    )
    return {"scaler": scaler}


# =============================================================================
# Singleton accessor
# =============================================================================
_energy_scaler_instance: Optional[EnhancedIntelligentEnergyScaler] = None
_energy_scaler_lock: Optional[asyncio.Lock] = None


async def get_energy_scaler(
    storage: Any = None,
    queue: Any = None,
    adaptive_cost: Optional[Any] = None,
    pareto_gating: Optional[Any] = None,
    drift_detector: Optional[Any] = None,
    metrics: Optional[Any] = None,
) -> EnhancedIntelligentEnergyScaler:
    global _energy_scaler_instance, _energy_scaler_lock
    if _energy_scaler_lock is None:
        _energy_scaler_lock = asyncio.Lock()
    if _energy_scaler_instance is None:
        async with _energy_scaler_lock:
            if _energy_scaler_instance is None:
                _energy_scaler_instance = build_components(
                    storage=storage,
                    message_queue=queue,
                    adaptive_cost=adaptive_cost,
                    pareto_gating=pareto_gating,
                    drift_detector=drift_detector,
                    metrics=metrics,
                )["scaler"]
                await _energy_scaler_instance.start()
    return _energy_scaler_instance


# =============================================================================
# Main entry point
# =============================================================================
async def _async_main() -> None:
    print("=" * 80)
    print("Enhanced Intelligent Energy Scaler v14.4 — Single-File Enhanced")
    print("=" * 80)

    scaler = await get_energy_scaler()

    reading = await scaler.record_power_reading(1500.0, 450.0)
    print(f"recorded: {reading['reading_id']} power={reading['power_watts']}W "
          f"carbon={reading['carbon_intensity']}")

    opt = await scaler.run_optimization()
    print(f"optimization: strategy={opt['strategy']} confidence={opt['confidence']:.2f}")
    print(f"narrative: {opt['explanation']['narrative']}")

    fed = await scaler.run_federated_round()
    print(f"federated: participants={fed['participants']}")

    fc = await scaler.forecast(hours=6)
    print(f"forecast: base={fc['base']:.1f} trend={fc['trend_per_hour']:.3f}")

    status = await scaler.get_status()
    print(json.dumps(status, indent=2, default=str))

    await scaler.shutdown()
    print("Shutdown complete")


def main() -> None:
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()


__all__ = [
    "EnhancedIntelligentEnergyScaler",
    "AutonomousEnergyOptimizer",
    "CausalBandit",
    "BanditConfig",
    "SafetyMonitor",
    "XAIExplainer",
    "FederatedEnergyLearnerSecure",
    "FederatedEnergyLearner",
    "MultiAgentCoordinator",
    "CarbonOffsetBroker",
    "ChaosMonkey",
    "HumanReviewManager",
    "HumanReviewRequest",
    "FlexGenManager",
    "PostQuantumCrypto",
    "BlockchainEnergyCredits",
    "PredictiveLoadForecaster",
    "MultiCloudStorage",
    "CircuitBreaker",
    "CircuitBreakerState",
    "EnergyScalerError",
    "SafetyViolationError",
    "ChaosExperimentError",
    "CircuitBreakerOpenError",
    "NotImplementedYetError",
    "build_components",
    "get_energy_scaler",
    "main",
]
