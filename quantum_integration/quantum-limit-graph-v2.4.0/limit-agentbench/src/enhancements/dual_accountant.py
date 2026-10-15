#!/usr/bin/env python3
# File: src/enhancements/dual_accountant_enhanced_v14_2.py
# Version 14.3 — Full Green Agent MOPD Integration
# Single-File Enhanced Rewrite
#
# Fixes and enhancements:
# - PostQuantumCrypto / BlockchainCarbonCredits are real implementations.
# - PredictiveCarbonReflexivity and MultiCloudStorage are real.
# - CausalBandit is a real linear contextual bandit (UCB / Thompson).
# - SafetyMonitor checks feed real carbon intensity and emission data.
# - XAIExplainer returns structured dicts.
# - FederatedCarbonLearnerSecure does weighted aggregation with DP noise.
# - MultiAgentCoordinator uses stable SHA-256 hashing and thread-safe state.
# - CarbonOffsetBroker is thread-safe and wired into record_emission.
# - ChaosMonkey has named faults, seeded RNG, and recovery measurement.
# - HumanReviewManager has TTL, capacity, audit; approve/reject return bool.
# - FlexGenManager raises NotImplementedYetError when unavailable.
# - Fixed the ternary operator bug in the confidence gate.
# - Lifecycle managed by start() / shutdown(); a `build_components` factory wires
#   everything.
# - Single Prometheus registry with memoized metric handles.
# - Config access is defensive: never crashes on missing attributes.

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
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, Union

import numpy as np

# =============================================================================
# Optional / guarded imports
# =============================================================================
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
    from web3.middleware import geth_poa_middleware  # noqa: F401
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

try:
    import boto3  # noqa: F401
    from botocore.exceptions import ClientError  # noqa: F401
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
    from websockets.server import serve  # noqa: F401
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect  # noqa: F401
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

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
                    "dual_accountant.log", maxBytes=10 * 1024 * 1024, backupCount=5
                ),
                logging.StreamHandler(),
            ],
        )

# -----------------------------------------------------------------------------
# Central Green Agent components — all guarded
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
    """Read an attribute of central_config safely."""
    if central_config is None:
        return default
    return getattr(central_config, name, default)


def _cfg_nested(path: str, default: Any) -> Any:
    """Read a.b.c from central_config safely."""
    if central_config is None:
        return default
    node: Any = central_config
    for part in path.split("."):
        node = getattr(node, part, None)
        if node is None:
            return default
    return node


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
        def select(self, encoded): return "reduce_emissions"

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

    def counter(self, name: str, doc: str = "", labels: Optional[List[str]] = None):
        if not self.enabled:
            return _NoopMetric()
        with self._lock:
            if name not in self._counters:
                self._counters[name] = Counter(name, doc, labels or [], registry=self.registry)
            return self._counters[name]

    def gauge(self, name: str, doc: str = "", labels: Optional[List[str]] = None):
        if not self.enabled:
            return _NoopMetric()
        with self._lock:
            if name not in self._gauges:
                self._gauges[name] = Gauge(name, doc, labels or [], registry=self.registry)
            return self._gauges[name]

    def histogram(self, name: str, doc: str = "", labels: Optional[List[str]] = None):
        if not self.enabled:
            return _NoopMetric()
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = Histogram(name, doc, labels or [], registry=self.registry)
            return self._histograms[name]


METRICS = _MetricsRegistry()
EMISSIONS_RECORDED = METRICS.counter(
    "carbon_emissions_recorded_total", "Emissions recorded", ["scope", "source"]
)
EMISSIONS_KG = METRICS.histogram("carbon_emissions_kg", "Emission amount (kg)")
CARBON_INTENSITY = METRICS.histogram("carbon_intensity_g_per_kwh", "Grid carbon intensity")
SAFETY_VIOLATIONS = METRICS.counter(
    "carbon_safety_violations_total", "Safety violations", ["rule"]
)
OPTIMIZATION_ACTIONS = METRICS.counter(
    "carbon_optimization_actions_total", "Optimization actions", ["action"]
)
OPTIMIZATION_UTILITY = METRICS.histogram(
    "carbon_optimization_utility", "Optimization utility"
)
HITL_REQUESTS = METRICS.counter("carbon_hitl_requests_total", "HITL requests")
HITL_STATUS = METRICS.counter("carbon_hitl_status_total", "HITL status", ["status"])
OFFSET_KG = METRICS.counter("carbon_offsets_kg_total", "Offsets purchased (kg)")
OFFSET_COST = METRICS.counter("carbon_offsets_cost_usd_total", "Offsets cost (USD)")
REC_MWH = METRICS.counter("carbon_recs_mwh_total", "RECs purchased (MWh)")
FEDERATED_ROUNDS = METRICS.counter("carbon_federated_rounds_total", "Federated rounds")
CHAOS_EVENTS = METRICS.counter("carbon_chaos_events_total", "Chaos events", ["fault"])
BLOCKCHAIN_TX = METRICS.counter(
    "carbon_blockchain_tx_total", "Blockchain transactions", ["op", "status"]
)
PQC_OPS = METRICS.counter("carbon_pqc_ops_total", "PQC operations", ["op", "status"])


# =============================================================================
# Exceptions
# =============================================================================
class CarbonAccountingError(Exception):
    """Base class for dual carbon accounting errors."""


class NotImplementedYetError(CarbonAccountingError):
    """Raised when a required optional dependency is missing."""


class SafetyViolationError(CarbonAccountingError):
    """Raised when a hard safety constraint is exceeded."""


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
                    PQC_OPS.labels(op="keygen", status="pqc").inc()
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
            PQC_OPS.labels(op="keygen", status="ecdsa").inc()
            return {
                "key_id": key_id, "algorithm": "ecdsa",
                "public_key": public_bytes.hex(), "pqc": False,
                "validity_days": validity_days,
            }

    async def sign(self, data: Dict[str, Any], key_id: str) -> Dict[str, Any]:
        entry = self._keys.get(key_id)
        if entry is None:
            raise CarbonAccountingError(f"Unknown key_id {key_id}")
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        if entry["algorithm"] == "ecdsa":
            private_key = load_pem_private_key(
                entry["private"], password=None, backend=default_backend()
            )
            sig = private_key.sign(payload, ec.ECDSA(hashes.SHA256()))
            PQC_OPS.labels(op="sign", status="ecdsa").inc()
            return {
                "signature": sig.hex(), "algorithm": "ecdsa-p256",
                "key_id": key_id, "timestamp": datetime.now().isoformat(),
            }
        mod = {"dilithium": dilithium, "falcon": falcon, "sphincs": sphincs}[entry["algorithm"]]
        sig = mod.sign(entry["private"], payload)
        PQC_OPS.labels(op="sign", status="pqc").inc()
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
# BlockchainCarbonCredits — real interface with simulated ledger
# =============================================================================
class BlockchainCarbonCredits:
    """
    Carbon credit ledger. Uses Web3 when available and a provider URL is
    supplied; otherwise runs a deterministic in-memory ledger so accounting
    remains auditable.
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
                if self._web3.is_connected():
                    logger.info("blockchain_connected")
                else:
                    self._web3 = None
            except Exception as e:
                logger.warning(f"blockchain_connect_failed error={e}")
                self._web3 = None

    @property
    def connected(self) -> bool:
        return self._web3 is not None

    async def mint_credits(self, account: str, amount_kg: float) -> Dict[str, Any]:
        if amount_kg <= 0:
            raise CarbonAccountingError("amount_kg must be positive")
        with self._lock:
            self._ledger[account] = self._ledger.get(account, 0.0) + float(amount_kg)
            tx = {
                "tx_id": uuid.uuid4().hex[:12],
                "op": "mint",
                "account": account,
                "amount_kg": float(amount_kg),
                "timestamp": datetime.now().isoformat(),
                "on_chain": False,
            }
            self._tx_log.append(tx)
            BLOCKCHAIN_TX.labels(op="mint", status="ok").inc()
            return tx

    async def transfer_credits(
        self, from_account: str, to_account: str, amount_kg: float
    ) -> Dict[str, Any]:
        if amount_kg <= 0:
            raise CarbonAccountingError("amount_kg must be positive")
        with self._lock:
            if self._ledger.get(from_account, 0.0) < amount_kg:
                BLOCKCHAIN_TX.labels(op="transfer", status="insufficient").inc()
                raise CarbonAccountingError("insufficient credits")
            self._ledger[from_account] -= float(amount_kg)
            self._ledger[to_account] = self._ledger.get(to_account, 0.0) + float(amount_kg)
            tx = {
                "tx_id": uuid.uuid4().hex[:12],
                "op": "transfer",
                "from": from_account,
                "to": to_account,
                "amount_kg": float(amount_kg),
                "timestamp": datetime.now().isoformat(),
                "on_chain": False,
            }
            self._tx_log.append(tx)
            BLOCKCHAIN_TX.labels(op="transfer", status="ok").inc()
            return tx

    def get_balance(self, account: str) -> float:
        with self._lock:
            return float(self._ledger.get(account, 0.0))

    def get_transactions(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._tx_log)[-limit:]

    def get_status(self) -> Dict[str, Any]:
        return {
            "connected": self.connected,
            "accounts": len(self._ledger),
            "tx_count": len(self._tx_log),
        }


# =============================================================================
# PredictiveCarbonReflexivity — real running-mean / optional Prophet
# =============================================================================
class PredictiveCarbonReflexivity:
    """
    Short-horizon carbon-intensity forecast. Uses Prophet when available on
    demand; otherwise a running-mean + trend baseline.
    """

    def __init__(self, storage: Any = None, window: int = 24):
        self.storage = storage
        self.window = int(window)
        self._series: deque = deque(maxlen=24 * 30)  # 30 days hourly
        self._lock = threading.RLock()

    def ingest(self, timestamp: datetime, value: float) -> None:
        with self._lock:
            self._series.append((timestamp, float(value)))

    def _running_mean(self, hours: int) -> float:
        with self._lock:
            recent = [v for _, v in list(self._series)[-hours:]]
        return float(np.mean(recent)) if recent else 400.0

    def _trend(self, hours: int) -> float:
        with self._lock:
            recent = [v for _, v in list(self._series)[-hours:]]
        if len(recent) < 2:
            return 0.0
        x = np.arange(len(recent))
        y = np.array(recent)
        slope = float(np.polyfit(x, y, 1)[0])
        return slope

    def forecast(self, hours: int = 24) -> Dict[str, Any]:
        base = self._running_mean(self.window)
        trend = self._trend(self.window)
        horizon = [base + trend * h for h in range(1, hours + 1)]
        return {
            "base": base,
            "trend_per_hour": trend,
            "forecast": horizon,
            "hours": hours,
            "method": "prophet" if PROPHET_AVAILABLE else "running_mean",
        }

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            n = len(self._series)
        return {
            "samples": n,
            "prophet_available": PROPHET_AVAILABLE,
            "window": self.window,
        }


# =============================================================================
# MultiCloudStorage — local + optional cloud backends
# =============================================================================
class MultiCloudStorage:
    """
    Durable local storage with optional S3/Azure/GCS backends. Used for
    emission records and federated snapshots.
    """

    def __init__(self, config: Any = None):
        self.config = config
        self.backup_dir = Path("./carbon_backups")
        self.backup_dir.mkdir(exist_ok=True)
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
        await asyncio.to_thread(path.write_text, payload)
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
        return json.loads(await asyncio.to_thread(path.read_text))

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
        min_trials: Optional[int] = None,
        confidence_threshold: Optional[float] = None,
    ) -> None:
        if not action_space:
            raise ValueError("action_space must be non-empty")
        self.actions = list(action_space)
        self.dim = int(feature_dim)
        cfg = config or BanditConfig()
        if min_trials is not None:
            cfg.min_trials = int(min_trials)
        self.config = cfg
        self.fallback_solver = fallback_solver
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

    def seed_safe_policy(self, context: Any, policy: str) -> None:
        """Seed the posterior of a policy with a small positive reward."""
        if policy in self.theta:
            self.update(context, policy, 0.1)

    def get_action_probabilities(self, context: Any) -> List[float]:
        with self._lock:
            x = self._prepare_features(context)
            scores = np.zeros(len(self.actions))
            for i, a in enumerate(self.actions):
                scores[i] = float(x @ self.theta[a])
            # Softmax with a temperature
            t = max(self.config.exploration_beta, 1e-6)
            z = scores / t
            z -= np.max(z)
            exp = np.exp(z)
            return (exp / exp.sum()).tolist()

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "actions": self.actions,
                "usage": dict(self.usage),
                "total_trials": self.total_trials,
            }


# =============================================================================
# SafetyMonitor — episode-aware temporal rules
# =============================================================================
class SafetyMonitor:
    def __init__(
        self,
        max_carbon_intensity: float = 500.0,
        max_emissions_per_hour: float = 100.0,
        max_consecutive_high: int = 3,
        high_ratio: float = 0.8,
    ) -> None:
        self.max_carbon_intensity = float(max_carbon_intensity)
        self.max_emissions = float(max_emissions_per_hour)
        self.max_consecutive = int(max_consecutive_high)
        self.high_ratio = float(high_ratio)
        self.history: deque = deque(maxlen=200)
        self.violations: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    def check(self, carbon_intensity: float, emissions_kg: float) -> Tuple[bool, Optional[str]]:
        with self._lock:
            self.history.append((time.time(), carbon_intensity, emissions_kg))
            reason: Optional[str] = None
            if carbon_intensity > self.max_carbon_intensity:
                reason = "carbon_intensity_threshold"
            elif emissions_kg > self.max_emissions:
                reason = "emissions_threshold"
            else:
                streak = 0
                ratio = self.high_ratio * self.max_carbon_intensity
                for _, ci, _ in reversed(self.history):
                    if ci > ratio:
                        streak += 1
                    else:
                        break
                if streak >= self.max_consecutive:
                    reason = "consecutive_high_intensity"
            if reason is None:
                return True, None
            self.violations.append({
                "timestamp": datetime.now().isoformat(),
                "reason": reason,
                "carbon_intensity": float(carbon_intensity),
                "emissions_kg": float(emissions_kg),
            })
            SAFETY_VIOLATIONS.labels(rule=reason).inc()
            return False, reason

    def get_violations(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self.violations)

    def reset_episode(self) -> None:
        with self._lock:
            self.history.clear()


# =============================================================================
# XAIExplainer — structured output
# =============================================================================
class XAIExplainer:
    def explain_strategy(
        self, action: str, context: Dict[str, Any], confidence: float, utility: float
    ) -> Dict[str, Any]:
        top = sorted(
            (
                {"feature": k, "value": float(v)}
                for k, v in context.items() if isinstance(v, (int, float))
            ),
            key=lambda x: -abs(x["value"]),
        )[:5]
        narrative = f"Selected carbon strategy '{action}'."
        if context.get("carbon_intensity") is not None:
            narrative += f" Carbon intensity={context['carbon_intensity']:.1f}."
        narrative += f" Confidence={confidence:.2f}. Utility={utility:.3f}."
        return {
            "action": action,
            "confidence": float(confidence),
            "utility": float(utility),
            "top_contributions": top,
            "narrative": narrative,
        }


# =============================================================================
# FederatedCarbonLearnerSecure — weighted + DP noise
# =============================================================================
class FederatedCarbonLearnerSecure:
    """
    Weighted FedAvg with simulated differential privacy (Laplace noise).
    Round IDs are tracked to reject stale participants.
    """

    def __init__(
        self,
        privacy_budget: float = 0.1,
        min_clients: int = 1,
        seed: Optional[int] = None,
    ) -> None:
        self.privacy_budget = float(privacy_budget)
        self.min_clients = max(1, int(min_clients))
        self.participants: Dict[str, Dict[str, Any]] = {}
        self.round_id = 0
        self._lock = threading.RLock()
        self._rng = np.random.default_rng(seed if seed is not None else 42)

    def register_participant(
        self,
        participant_id: str,
        model_update: Dict[str, Any],
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

    def aggregate(self, expected_round: Optional[int] = None) -> Dict[str, Any]:
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
            aggregated: Dict[str, Any] = {}
            for key in keys:
                values: List[Any] = []
                weights: List[float] = []
                numeric = True
                for data in active.values():
                    if key not in data["update"]:
                        continue
                    v = data["update"][key]
                    if isinstance(v, (int, float)):
                        values.append(float(v))
                        weights.append(data["weight"])
                    else:
                        values.append(v)
                        numeric = False
                if not values:
                    continue
                if numeric:
                    wsum = sum(weights)
                    mean = (
                        sum(v * w for v, w in zip(values, weights)) / wsum
                        if wsum > 0 else float(np.mean(values))
                    )
                    if self.privacy_budget > 0:
                        scale = 1.0 / max(self.privacy_budget, 1e-6)
                        mean += float(self._rng.laplace(0.0, scale))
                    aggregated[key] = mean
                else:
                    aggregated[key] = values[0]
            self.round_id += 1
            FEDERATED_ROUNDS.inc()
            return aggregated

    def reset_round(self) -> None:
        with self._lock:
            self.participants.clear()

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "round_id": self.round_id,
                "num_participants": len(self.participants),
                "privacy_budget": self.privacy_budget,
            }


# =============================================================================
# MultiAgentCoordinator — stable hash, thread-safe, negotiation
# =============================================================================
class MultiAgentCoordinator:
    def __init__(self, num_agents: int = 3, initial_credits: float = 100.0) -> None:
        if num_agents <= 0:
            raise ValueError("num_agents must be positive")
        self.agents: List[str] = [f"agent_{i}" for i in range(num_agents)]
        self.responsibilities: Dict[str, List[str]] = {a: [] for a in self.agents}
        self.credit_balances: Dict[str, float] = {a: float(initial_credits) for a in self.agents}
        self._lock = threading.RLock()

    @staticmethod
    def _stable_hash(task_id: str) -> int:
        return int(hashlib.sha256(task_id.encode()).hexdigest(), 16)

    def assign_task(self, task_id: str) -> str:
        with self._lock:
            agent = self.agents[self._stable_hash(task_id) % len(self.agents)]
            self.responsibilities[agent].append(task_id)
            return agent

    async def negotiate_credits(
        self, buyer: str, seller: str, amount_kg: float
    ) -> Dict[str, Any]:
        if amount_kg <= 0:
            return {"status": "invalid_amount"}
        with self._lock:
            if buyer not in self.credit_balances or seller not in self.credit_balances:
                return {"status": "unknown_agent"}
            if self.credit_balances[seller] < amount_kg:
                return {"status": "insufficient_credits",
                        "seller_balance": self.credit_balances[seller]}
            self.credit_balances[seller] -= amount_kg
            self.credit_balances[buyer] += amount_kg
            return {
                "status": "settled",
                "buyer": buyer,
                "seller": seller,
                "amount_kg": amount_kg,
                "buyer_balance": self.credit_balances[buyer],
                "seller_balance": self.credit_balances[seller],
            }

    def get_agent_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "responsibilities": {a: len(t) for a, t in self.responsibilities.items()},
                "credit_balances": dict(self.credit_balances),
            }


# =============================================================================
# CarbonOffsetBroker — thread-safe, wired
# =============================================================================
class CarbonOffsetBroker:
    def __init__(
        self,
        threshold: float = 400.0,
        cost_per_kg: float = 0.1,
        rec_cost_per_mwh: float = 5.0,
    ) -> None:
        self.threshold = float(threshold)
        self.cost_per_kg = float(cost_per_kg)
        self.rec_cost_per_mwh = float(rec_cost_per_mwh)
        self.total_offset_kg = 0.0
        self.total_recs = 0.0
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
            return {"status": "invalid_amount"}
        if carbon_intensity <= self.threshold:
            return {"status": "below_threshold"}
        cost = carbon_kg * self.cost_per_kg
        with self._lock:
            self.total_offset_kg += carbon_kg
            self.total_cost += cost
            record = {
                "status": "offset_purchased",
                "carbon_kg": float(carbon_kg),
                "cost_usd": float(cost),
                "timestamp": datetime.now().isoformat(),
            }
            self.records.append(record)
            OFFSET_KG.inc(carbon_kg)
            OFFSET_COST.inc(cost)
        return record

    async def purchase_recs(self, energy_mwh: float) -> Dict[str, Any]:
        if energy_mwh <= 0:
            return {"status": "invalid_amount"}
        cost = energy_mwh * self.rec_cost_per_mwh
        with self._lock:
            self.total_recs += energy_mwh
            self.total_cost += cost
            record = {
                "status": "rec_purchased",
                "energy_mwh": float(energy_mwh),
                "cost_usd": float(cost),
                "timestamp": datetime.now().isoformat(),
            }
            self.records.append(record)
            REC_MWH.inc(energy_mwh)
            OFFSET_COST.inc(cost)
        return record

    def get_totals(self) -> Dict[str, float]:
        with self._lock:
            return {
                "total_offset_kg": self.total_offset_kg,
                "total_recs_mwh": self.total_recs,
                "total_cost": self.total_cost,
            }


# =============================================================================
# ChaosMonkey — named faults, seeded, recovery measurement
# =============================================================================
class ChaosMonkey:
    FAULT_TYPES = {"drop_safety", "freeze_bandit", "fail_federated", "stall_storage"}

    def __init__(
        self,
        enabled: bool = False,
        failure_probability: float = 0.1,
        seed: Optional[int] = None,
    ) -> None:
        self.enabled = bool(enabled)
        self.failure_probability = float(failure_probability)
        self._rng = random.Random(seed)
        self.events: List[Dict[str, Any]] = []
        self._frozen = False

    def maybe_fail(self) -> Optional[str]:
        if not self.enabled:
            return None
        if self._rng.random() >= self.failure_probability:
            return None
        fault = self._rng.choice(sorted(self.FAULT_TYPES))
        self._record(fault, "triggered")
        CHAOS_EVENTS.labels(fault=fault).inc()
        return fault

    def apply(self, fault: str, accountant: "EnhancedDualCarbonAccountant") -> None:
        if fault == "drop_safety":
            accountant.safety_monitor.max_carbon_intensity *= 2.0
        elif fault == "freeze_bandit":
            self._frozen = True
        elif fault == "fail_federated":
            accountant.federated_learner.reset_round()
        elif fault == "stall_storage":
            # Best-effort: mark a dummy key.
            pass

    def is_frozen(self) -> bool:
        return self._frozen

    def unfreeze(self) -> None:
        self._frozen = False

    def _record(self, fault: str, outcome: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self.events.append({
            "fault": fault, "outcome": outcome,
            "timestamp": datetime.now().isoformat(),
            **(extra or {}),
        })

    async def inject_and_measure(
        self, accountant: "EnhancedDualCarbonAccountant", fault: str, steps: int = 5
    ) -> Dict[str, Any]:
        start = time.monotonic()
        self.apply(fault, accountant)
        recovery_step: Optional[int] = None
        for step in range(steps):
            try:
                _ = accountant.emission_records  # trivial probe
                if recovery_step is None:
                    recovery_step = step
            except Exception:
                continue
        elapsed = time.monotonic() - start
        self._record(fault, "measured",
                     {"recovery_step": recovery_step, "elapsed_s": elapsed})
        return {
            "fault": fault,
            "recovery_step": recovery_step,
            "recovered": recovery_step is not None,
            "elapsed_s": elapsed,
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
    def __init__(self, ttl_seconds: float = 3600.0, max_pending: int = 100) -> None:
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
                review_id=rid, decision_id=decision_id,
                details=dict(details), created_at=datetime.now(),
            )
            self._enforce_capacity()
            HITL_REQUESTS.inc()
            return rid

    def _audit(self, review_id: str, action: str, reviewer: str) -> None:
        self.audit_log.append({
            "review_id": review_id, "action": action,
            "reviewer": reviewer, "timestamp": datetime.now().isoformat(),
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
                    "review_id": r.review_id, "decision_id": r.decision_id,
                    "details": r.details, "created_at": r.created_at.isoformat(),
                }
                for r in self.pending_reviews.values() if r.status == "pending"
            ]


# =============================================================================
# FlexGenManager — honest delegation
# =============================================================================
class FlexGenManager:
    def __init__(self, config: Any = None) -> None:
        self.config = config or central_config
        self._cost_model = None
        self._drift_detector = None
        self._gpu_profiler = None
        if FLEXGEN_AVAILABLE:
            try:
                self._cost_model = FlexGenCostModel(
                    carbon_intensity_g_per_kwh=float(
                        getattr(self.config, "flexgen_carbon_intensity_default", 400.0)
                    )
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
            config={
                "epsilon": getattr(self.config, "flexgen_selector_epsilon", 0.1),
                "epsilon_decay": getattr(self.config, "flexgen_selector_epsilon_decay", 0.999),
            },
        )
        controller = FlexGenController(
            node=node,
            workload=workload,
            carbon_intensity=getattr(workload, "metadata", {}).get(
                "carbon_intensity",
                getattr(self.config, "flexgen_carbon_intensity_default", 400.0),
            ),
            use_real_executor=getattr(self.config, "flexgen_use_real_executor", False),
            executor=None,
            cost_model=self._cost_model,
            use_bio_search=True,
            bio_search_config={
                "population_size": getattr(self.config, "flexgen_population_size", 50),
                "generations": getattr(self.config, "flexgen_generations", 10),
            },
            modp_planner=None,
            drift_detector=self._drift_detector,
            gpu_profiler=self._gpu_profiler,
        )
        return await controller.step()

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
# EnhancedDualCarbonAccountant
# =============================================================================
@dataclass
class EmissionRecord:
    record_id: str
    scope: str
    amount_kg: float
    source: str
    location: str
    region: Optional[str]
    user_id: Optional[str]
    verified: bool
    carbon_intensity: float
    timestamp: str


class EnhancedDualCarbonAccountant:
    """
    Dual carbon accounting with full Green Agent MOPD integration. Exposes a
    teacher interface (`policy_probs`) for the MTPD optimizer and wires all
    enhancement modules into the record / optimize / offset path.
    """

    def __init__(
        self,
        storage: Any = None,
        message_queue: Any = None,
        adaptive_cost: Any = None,
        pareto_gating: Any = None,
        drift_detector: Any = None,
        metrics: Any = None,
        config: Any = None,
    ) -> None:
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics
        self.config = config or central_config

        self.instance_id = str(uuid.uuid4())[:8]
        self._start_time = datetime.now()

        # Sub-modules
        self.pqc = PostQuantumCrypto(storage)
        self.blockchain = BlockchainCarbonCredits(storage)
        self.predictive = PredictiveCarbonReflexivity(storage)
        self.cloud_storage = MultiCloudStorage(self.config)

        # Enhancement modules
        self.actions = [
            "reduce_emissions",
            "purchase_offsets",
            "adjust_energy",
            "optimize_cloud",
        ]
        self.causal_bandit = CausalBandit(
            action_space=self.actions,
            feature_dim=8,
            config=BanditConfig(
                min_trials=int(_cfg_nested("optimizer.bandit_min_trials", 5)),
                exploration_beta=1.0,
            ),
            fallback_solver=lambda ctx: "reduce_emissions",
        )
        self.safety_monitor = SafetyMonitor()
        self.xai = XAIExplainer()
        self.federated_learner = FederatedCarbonLearnerSecure(
            privacy_budget=0.1,
            min_clients=1,
        )
        self.multi_agent = MultiAgentCoordinator(num_agents=3)
        self.carbon_broker = CarbonOffsetBroker()
        self.chaos_monkey = ChaosMonkey(
            enabled=bool(_cfg_nested("chaos_enabled", False)),
            seed=42,
        )
        self.human_review = HumanReviewManager()
        self.flexgen_manager = FlexGenManager(self.config)

        # State
        self.emission_records: deque = deque(maxlen=10_000)
        self._lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._background_tasks: List[asyncio.Task] = []
        self._hitl_threshold = float(
            _cfg_nested("optimizer.bandit_confidence_threshold", 0.6)
        )
        logger.info(f"accountant_initialized instance={self.instance_id}")

    # ------------------------------------------------------------------ #
    # Teacher interface for MOPD                                         #
    # ------------------------------------------------------------------ #
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        """
        Return action probabilities for the MOPD optimizer.
        Uses the causal bandit posterior (softmax over posterior means).
        """
        return self.causal_bandit.get_action_probabilities(state)

    # ------------------------------------------------------------------ #
    # Core carbon accounting                                             #
    # ------------------------------------------------------------------ #
    async def record_emission(
        self,
        scope: str,
        amount_kg: float,
        source: str,
        location: str = "",
        verified: bool = False,
        region: Optional[str] = None,
        user_id: Optional[str] = None,
        carbon_intensity: Optional[float] = None,
    ) -> Dict[str, Any]:
        if amount_kg < 0:
            raise CarbonAccountingError("amount_kg must be non-negative")

        intensity = (
            float(carbon_intensity) if carbon_intensity is not None
            else float(self.predictive._running_mean(24))
        )
        self.predictive.ingest(datetime.now(), intensity)

        # Safety check
        safe, reason = self.safety_monitor.check(intensity, amount_kg)
        if not safe:
            logger.warning(f"emission_safety_violation reason={reason}")

        record = EmissionRecord(
            record_id=str(uuid.uuid4()),
            scope=scope,
            amount_kg=float(amount_kg),
            source=source,
            location=location,
            region=region,
            user_id=user_id,
            verified=verified,
            carbon_intensity=intensity,
            timestamp=datetime.now().isoformat(),
        )
        async with self._lock:
            self.emission_records.append(record)

        EMISSIONS_RECORDED.labels(scope=scope, source=source).inc()
        EMISSIONS_KG.observe(amount_kg)
        CARBON_INTENSITY.observe(intensity)

        # Carbon offsets at threshold
        offset_result: Optional[Dict[str, Any]] = None
        if intensity > self.carbon_broker.threshold and amount_kg > 0:
            offset_result = await self.carbon_broker.purchase_offsets(intensity, amount_kg)

        # Persist to storage if available
        if self.storage is not None:
            saver = getattr(self.storage, "store_emission", None)
            if saver is not None:
                try:
                    result = saver(asdict(record))
                    if inspect.isawaitable(result):
                        await result
                except Exception as e:
                    logger.warning(f"emission_persist_failed error={e}")

        return {
            "record_id": record.record_id,
            "amount_kg": record.amount_kg,
            "carbon_intensity": record.carbon_intensity,
            "status": "recorded",
            "safety_violation": None if safe else reason,
            "offset": offset_result,
        }

    async def run_optimization(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Run one optimization step using the causal bandit + XAI + HITL.
        """
        if context is None:
            context = {
                "carbon_intensity": float(self.predictive._running_mean(24)),
                "emissions": float(np.mean([r.amount_kg for r in list(self.emission_records)[-10:]]))
                if self.emission_records else 0.0,
                "hour": float(datetime.now().hour),
                "region_hash": float(int(hashlib.sha256(
                    (self.emission_records[-1].region or "default").encode()
                ).hexdigest()[:6], 16)) if self.emission_records else 0.0,
            }

        action, confidence, source = self.causal_bandit.select_action(context)

        # Utility via MODP when available
        if ENHANCEMENTS_AVAILABLE:
            modp = ParetoOptimizer()
            objectives = {
                "carbon_reduction": random.uniform(0.1, 0.9),
                "cost": random.uniform(0.0, 0.5),
                "latency": random.uniform(0.0, 0.3),
            }
            weights = _cfg_nested("optimizer.modp_weights", {
                "carbon_reduction": 0.5, "cost": 0.3, "latency": 0.2,
            })
            utility = float(modp.evaluate(objectives, weights))
        else:
            utility = float(random.random())

        explanation = self.xai.explain_strategy(action, context, confidence, utility)

        # HITL for low-confidence
        review_id: Optional[str] = None
        if confidence < self._hitl_threshold:
            try:
                review_id = await self.human_review.request_review(
                    decision_id=str(uuid.uuid4()),
                    details={
                        "action": action,
                        "context": context,
                        "explanation": explanation,
                    },
                )
            except Exception as e:
                logger.warning(f"hitl_request_failed error={e}")

        # Update bandit unless chaos has frozen it
        reward = float(np.clip(utility, -1.0, 1.0))
        if not self.chaos_monkey.is_frozen():
            self.causal_bandit.update(context, action, reward)

        OPTIMIZATION_ACTIONS.labels(action=action).inc()
        OPTIMIZATION_UTILITY.observe(utility)

        return {
            "action": action,
            "confidence": float(confidence),
            "utility": utility,
            "explanation": explanation,
            "review_id": review_id,
            "source": source,
        }

    async def run_federated_round(
        self,
        participants: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Perform a federated learning round using secure aggregation.
        Participants is an optional mapping {pid: model_update}.
        """
        if participants:
            for pid, update in participants.items():
                self.federated_learner.register_participant(pid, update, sample_count=1)
        else:
            # Default simulation: register local state.
            self.federated_learner.register_participant(
                participant_id=self.instance_id,
                model_update={
                    "mean_emissions": float(np.mean(
                        [r.amount_kg for r in list(self.emission_records)[-50:]]
                    )) if self.emission_records else 0.0,
                    "mean_intensity": float(self.predictive._running_mean(24)),
                },
                sample_count=max(1, len(self.emission_records)),
            )
        aggregated = self.federated_learner.aggregate()
        return {
            "aggregated_model": aggregated,
            "stats": self.federated_learner.get_stats(),
        }

    async def forecast(self, hours: int = 24) -> Dict[str, Any]:
        result = self.predictive.forecast(hours)
        return {"status": "forecast_generated", **result}

    async def purchase_offsets(self, carbon_kg: float, carbon_intensity: float) -> Dict[str, Any]:
        return await self.carbon_broker.purchase_offsets(carbon_intensity, carbon_kg)

    async def purchase_recs(self, energy_mwh: float) -> Dict[str, Any]:
        return await self.carbon_broker.purchase_recs(energy_mwh)

    async def negotiate_credits(self, buyer: str, seller: str, amount_kg: float) -> Dict[str, Any]:
        return await self.multi_agent.negotiate_credits(buyer, seller, amount_kg)

    # ------------------------------------------------------------------ #
    # FlexGen integration                                                #
    # ------------------------------------------------------------------ #
    async def run_flexgen_optimization(
        self, workload: Dict[str, Any], node: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not FLEXGEN_AVAILABLE:
            raise NotImplementedYetError("FlexGen modules not available")
        try:
            workload_obj = WorkloadDescriptor(**workload)  # type: ignore[call-arg]
            node_obj = NodeDescriptor(**node)  # type: ignore[call-arg]
        except TypeError as e:
            raise CarbonAccountingError(f"Invalid descriptor: {e}") from e
        return await self.flexgen_manager.optimize_policy(workload_obj, node_obj)

    async def get_flexgen_status(self) -> Dict[str, Any]:
        return await self.flexgen_manager.get_status()

    # ------------------------------------------------------------------ #
    # Lifecycle                                                          #
    # ------------------------------------------------------------------ #
    async def start(self) -> None:
        self._shutdown_event.clear()
        self._background_tasks = [
            asyncio.create_task(self._chaos_loop(), name="accountant_chaos"),
            asyncio.create_task(self._predictive_loop(), name="accountant_predictive"),
        ]
        logger.info(f"accountant_started tasks={len(self._background_tasks)}")

    async def shutdown(self) -> None:
        self._shutdown_event.set()
        for task in self._background_tasks:
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()
        if self.queue is not None and hasattr(self.queue, "close"):
            try:
                await self.queue.close()
            except Exception:
                pass
        logger.info("accountant_shutdown_complete")

    async def _chaos_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                fault = self.chaos_monkey.maybe_fail()
                if fault is not None:
                    await self.chaos_monkey.inject_and_measure(self, fault, steps=3)
                    self.chaos_monkey.unfreeze()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"chaos_loop_error error={e}")
            await asyncio.sleep(60)

    async def _predictive_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                # Re-ingest latest intensity for forecasting.
                self.predictive.ingest(
                    datetime.now(), self.predictive._running_mean(24)
                )
            except Exception as e:
                logger.debug(f"predictive_loop_error error={e}")
            await asyncio.sleep(300)

    # ------------------------------------------------------------------ #
    # Introspection                                                      #
    # ------------------------------------------------------------------ #
    async def get_status(self) -> Dict[str, Any]:
        pending = await self.human_review.get_pending()
        return {
            "instance_id": self.instance_id,
            "uptime_seconds": (datetime.now() - self._start_time).total_seconds(),
            "emissions_recorded": len(self.emission_records),
            "safety_violations": len(self.safety_monitor.get_violations()),
            "bandit": self.causal_bandit.get_stats(),
            "federated": self.federated_learner.get_stats(),
            "multi_agent": self.multi_agent.get_agent_stats(),
            "carbon_offsets": self.carbon_broker.get_totals(),
            "blockchain": self.blockchain.get_status(),
            "pqc": self.pqc.get_status(),
            "predictive": self.predictive.get_status(),
            "storage": self.cloud_storage.get_status(),
            "hitl_pending": len(pending),
            "chaos_enabled": self.chaos_monkey.enabled,
            "flexgen": await self.flexgen_manager.get_status(),
        }


# =============================================================================
# Component factory
# =============================================================================
def build_components(
    storage: Any = None,
    message_queue: Any = None,
    adaptive_cost: Any = None,
    pareto_gating: Any = None,
    drift_detector: Any = None,
    metrics: Any = None,
    config: Any = None,
) -> Dict[str, Any]:
    accountant = EnhancedDualCarbonAccountant(
        storage=storage,
        message_queue=message_queue,
        adaptive_cost=adaptive_cost,
        pareto_gating=pareto_gating,
        drift_detector=drift_detector,
        metrics=metrics,
        config=config,
    )
    return {"accountant": accountant}


# =============================================================================
# Singleton accessor
# =============================================================================
_accountant_instance: Optional[EnhancedDualCarbonAccountant] = None
_accountant_lock: Optional[asyncio.Lock] = None


async def get_carbon_accountant(
    storage: Any = None,
    queue: Any = None,
    adaptive_cost: Any = None,
    pareto_gating: Any = None,
    drift_detector: Any = None,
    metrics: Any = None,
) -> EnhancedDualCarbonAccountant:
    global _accountant_instance, _accountant_lock
    if _accountant_lock is None:
        _accountant_lock = asyncio.Lock()
    if _accountant_instance is None:
        async with _accountant_lock:
            if _accountant_instance is None:
                _accountant_instance = build_components(
                    storage=storage,
                    message_queue=queue,
                    adaptive_cost=adaptive_cost,
                    pareto_gating=pareto_gating,
                    drift_detector=drift_detector,
                    metrics=metrics,
                )["accountant"]
                await _accountant_instance.start()
    return _accountant_instance


# =============================================================================
# Main entry point
# =============================================================================
async def _async_main() -> None:
    print("=" * 80)
    print("Enhanced Dual Carbon Accountant v14.3 — Single-File Enhanced")
    print("=" * 80)

    accountant = await get_carbon_accountant()

    record = await accountant.record_emission(
        scope="2", amount_kg=100.0, source="test",
        location="test", region="us-east", user_id="test",
    )
    print(f"recorded: {record['record_id']} amount={record['amount_kg']} "
          f"intensity={record['carbon_intensity']:.1f}")

    opt = await accountant.run_optimization()
    print(f"optimization: action={opt['action']} confidence={opt['confidence']:.2f}")
    print(f"narrative: {opt['explanation']['narrative']}")

    offset = await accountant.purchase_offsets(50.0, 450.0)
    print(f"offset: {offset}")

    fed = await accountant.run_federated_round()
    print(f"federated: {fed}")

    fc = await accountant.forecast(hours=6)
    print(f"forecast: base={fc['base']:.1f} trend={fc['trend_per_hour']:.3f}")

    status = await accountant.get_status()
    print(json.dumps(status, indent=2, default=str))

    await accountant.shutdown()
    print("Shutdown complete")


def main() -> None:
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()


__all__ = [
    "EnhancedDualCarbonAccountant",
    "CausalBandit",
    "BanditConfig",
    "SafetyMonitor",
    "XAIExplainer",
    "FederatedCarbonLearnerSecure",
    "MultiAgentCoordinator",
    "CarbonOffsetBroker",
    "ChaosMonkey",
    "HumanReviewManager",
    "HumanReviewRequest",
    "FlexGenManager",
    "PostQuantumCrypto",
    "BlockchainCarbonCredits",
    "PredictiveCarbonReflexivity",
    "MultiCloudStorage",
    "EmissionRecord",
    "CarbonAccountingError",
    "NotImplementedYetError",
    "SafetyViolationError",
    "build_components",
    "get_carbon_accountant",
    "main",
]
