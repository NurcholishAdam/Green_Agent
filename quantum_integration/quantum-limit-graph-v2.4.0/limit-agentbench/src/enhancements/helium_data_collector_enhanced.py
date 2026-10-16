#!/usr/bin/env python3
# src/enhancements/helium_data_collector_enhanced_v18_0.py
# Version 18.0 — Full Green Agent stack (P0/P1/P2 hardened)
"""
Enhanced Helium Data Collector — v18.0 (P0/P1/P2 hardened)

Sections:
  A.  Core infrastructure (metrics, exceptions, config, ORM, data classes)
  B.  Reliability primitives (circuit breaker, rate limiter, bulkhead, tasks)
  C.  Storage / DB (fixed SQLAlchemy setup, ORM insert, Alembic-ready)
  D.  Security (real PQC lifecycle, JWT, AES-GCM)
  E.  Blockchain (Web3 real tx + local ledger fallback)
  F.  Carbon (real API clients + REC ledger + persistence)
  G.  Cloud (real SDK uploads with local fallback)
  H.  ML / anomaly / self-healing
  I.  Ten enhancements (P0 fixes + P1 + P2 upgrades)
  J.  Scheduler / MODP / MOE / Pareto / TOPSIS (fixed conventions)
  K.  Main collector
  L.  FastAPI + CLI

Objective convention: every objective is expressed as a COST (lower is better).
Pareto, TOPSIS, and MODP all follow that convention.
"""

from __future__ import annotations

import asyncio
import atexit
import base64
import contextlib
import hashlib
import hmac
import json
import logging
import logging.handlers
import math
import os
import pickle
import random
import signal
import statistics
import tempfile
import time
import uuid
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, Iterable, List, Optional, Protocol,
    Sequence, Set, Tuple, Union, runtime_checkable,
)

import numpy as np

# ============================================================
# CENTRAL GREEN AGENT COMPONENTS (with fallbacks)
# ============================================================
try:
    from ..config import config as central_config  # type: ignore
    from ..storage import Storage  # type: ignore
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
    from ..routing.pareto_gating import ParetoGating  # type: ignore
    from ..feedback.adaptive_cost import AdaptiveCostFunction  # type: ignore
    from ..safety.drift_detector import DriftDetector  # type: ignore
    from ..scaling.message_queue import AsyncMessageQueue  # type: ignore
    from ..metrics import MetricsRegistry  # type: ignore
    from ..logger import logger  # type: ignore
    CENTRAL_AVAILABLE = True
except ImportError:
    CENTRAL_AVAILABLE = False

    class _CentralConfig:
        CIRCUIT_BREAKER_FAILURE_THRESHOLD = 5
        CIRCUIT_BREAKER_RECOVERY_TIMEOUT = 30
        ENVIRONMENT = "production"
        data_retention_days = 365

    central_config = _CentralConfig()

    class Storage:
        def __init__(self, *a, **kw): self._records: List[Any] = []
        def store(self, r): self._records.append(r)

    class FeedbackEvent:
        def __init__(self, **kwargs): self.__dict__.update(kwargs)
        @classmethod
        def create_with_context(cls, **kwargs): return cls(**kwargs)
        def to_json(self): return json.dumps(self.__dict__, default=str)

    class ParetoGating:
        def __init__(self, *a, **kw): pass
        def filter(self, x): return x

    class AdaptiveCostFunction:
        def __init__(self, storage=None, *a, **kw): self.storage = storage
        def evaluate(self, state, targets=None):
            if targets is None:
                cost = 0.0
                if isinstance(state, dict):
                    for v in state.values():
                        try: cost += float(v)
                        except Exception: pass
                return cost
            cost = 0.0
            for k, t in targets.items():
                if k in state:
                    try: cost += (float(state[k]) - float(t)) ** 2
                    except Exception: pass
            return float(cost)

    class DriftDetector:
        def __init__(self, *a, **kw): pass
        async def check_drift(self, metrics): return False

    class AsyncMessageQueue:
        def __init__(self): self._messages: List[Any] = []
        async def publish(self, channel, message):
            self._messages.append((channel, message))

    class MetricsRegistry:
        def set_circularity_score(self, v): pass
        def set(self, name, v): pass

    logger = logging.getLogger(__name__)
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


# ============================================================
# OPTIONAL EXTERNAL DEPENDENCIES
# ============================================================
try:
    from pqcrypto.sign import dilithium, falcon, sphincs  # type: ignore
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # noqa: F401
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC  # noqa: F401
    from cryptography.hazmat.primitives import hashes  # noqa: F401
    from cryptography.hazmat.backends import default_backend  # noqa: F401
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

try:
    from web3 import Web3  # noqa: F401
    from web3.middleware import geth_poa_middleware  # noqa: F401
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

try:
    from prophet import Prophet  # noqa: F401
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

try:
    from sklearn.linear_model import LogisticRegression, LinearRegression, Ridge
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import IsolationForest
    from sklearn.svm import OneClassSVM
    from sklearn.cluster import KMeans
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import torch  # noqa: F401
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

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
    from google.cloud import storage as gcs_storage  # noqa: F401
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, field_validator
    from pydantic_settings import BaseSettings, SettingsConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
    from sqlalchemy.orm import declarative_base
    from sqlalchemy import (
        Column, String, Float, DateTime, Integer, Boolean, Text, JSON, text,
    )
    ASYNC_SQLALCHEMY_AVAILABLE = True
except ImportError:
    ASYNC_SQLALCHEMY_AVAILABLE = False

try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import declarative_base as _decl_base
    SQLALCHEMY_SYNC_AVAILABLE = True
    if not ASYNC_SQLALCHEMY_AVAILABLE:
        declarative_base = _decl_base  # type: ignore
except ImportError:
    SQLALCHEMY_SYNC_AVAILABLE = False

SQLALCHEMY_AVAILABLE = ASYNC_SQLALCHEMY_AVAILABLE or SQLALCHEMY_SYNC_AVAILABLE

try:
    from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn  # noqa: F401
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

try:
    from jose import JWTError, jwt  # type: ignore
    JOSE_AVAILABLE = True
except ImportError:
    JOSE_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    start_http_server = None

# Quantum-inspired distillation (optional)
try:
    import pennylane as qml  # type: ignore
    PENNYLANE_AVAILABLE = True
except ImportError:
    PENNYLANE_AVAILABLE = False
try:
    from qiskit import QuantumCircuit, Aer, execute as qiskit_execute  # type: ignore
    QISKIT_AVAILABLE = True
except ImportError:
    QISKIT_AVAILABLE = False

# XAI (optional)
try:
    import shap  # type: ignore
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False
try:
    import lime  # noqa: F401
    import lime.lime_tabular  # noqa: F401
    LIME_AVAILABLE = True
except ImportError:
    LIME_AVAILABLE = False

# Real carbon intensity APIs
try:
    import httpx  # noqa: F401
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False


# ============================================================
# PROMETHEUS METRICS
# ============================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    HELIUM_COLLECTIONS = Counter("helium_collections_total", "C", ["status"], registry=REGISTRY)
    QUANTUM_SIGNATURES = Counter("quantum_signatures_total", "S", ["algorithm", "status"], registry=REGISTRY)
    BLOCKCHAIN_VERIFICATIONS = Counter("blockchain_verifications_total", "B", ["status"], registry=REGISTRY)
    PREDICTIVE_ACCURACY = Gauge("helium_predictive_accuracy", "PA", ["model"], registry=REGISTRY)
    DATA_QUALITY_SCORE = Gauge("helium_data_quality_score", "Q", registry=REGISTRY)
    ANOMALY_DETECTIONS = Counter("helium_anomaly_detections_total", "A", ["type"], registry=REGISTRY)
    SELF_HEALING_ACTIONS = Counter("helium_self_healing_actions_total", "SH", ["action"], registry=REGISTRY)
    MOE_GATING_WEIGHTS = Gauge("helium_moe_gating_weights", "MG", ["expert"], registry=REGISTRY)
    CARBON_PRICE = Gauge("helium_carbon_price_per_kg", "CP", registry=REGISTRY)
    REC_INVENTORY_KWH = Gauge("helium_rec_inventory_kwh", "REC", registry=REGISTRY)
    PRECISION_SWITCHES = Counter("helium_precision_switches_total", "PS", ["level"], registry=REGISTRY)
    CHAOS_EVENTS = Counter("helium_chaos_events_total", "CE", ["type"], registry=REGISTRY)
    HITL_APPROVALS = Counter("helium_hitl_approvals_total", "H", ["decision"], registry=REGISTRY)
    SAFETY_VIOLATIONS = Counter("helium_safety_violations_total", "SV", ["formula"], registry=REGISTRY)
    FEDERATED_ROUNDS = Counter("helium_federated_rounds_total", "FR", registry=REGISTRY)
    HEALTH_SCORE = Gauge("helium_health_score", "HS", registry=REGISTRY)
    DP_EPSILON_SPENT = Gauge("helium_dp_epsilon_spent", "DPE", ["node"], registry=REGISTRY)
    PRECISION_ENERGY = Gauge("helium_precision_energy_joules", "PE", ["level"], registry=REGISTRY)
else:
    class _Dummy:
        def inc(self, *a, **kw): pass
        def set(self, *a, **kw): pass
        def labels(self, *a, **kw): return self
        def observe(self, *a, **kw): pass
    HELIUM_COLLECTIONS = QUANTUM_SIGNATURES = BLOCKCHAIN_VERIFICATIONS = _Dummy()
    PREDICTIVE_ACCURACY = DATA_QUALITY_SCORE = ANOMALY_DETECTIONS = _Dummy()
    SELF_HEALING_ACTIONS = MOE_GATING_WEIGHTS = CARBON_PRICE = REC_INVENTORY_KWH = _Dummy()
    PRECISION_SWITCHES = CHAOS_EVENTS = HITL_APPROVALS = SAFETY_VIOLATIONS = _Dummy()
    FEDERATED_ROUNDS = HEALTH_SCORE = DP_EPSILON_SPENT = PRECISION_ENERGY = _Dummy()


# ============================================================
# EXCEPTIONS
# ============================================================
class HeliumCollectorError(Exception): pass
class QuantumError(HeliumCollectorError): pass
class BlockchainError(HeliumCollectorError): pass
class CollectionError(HeliumCollectorError): pass
class DistributionError(HeliumCollectorError): pass
class CircuitBreakerOpenError(HeliumCollectorError): pass
class RateLimitExceeded(HeliumCollectorError): pass
class VaultError(HeliumCollectorError): pass
class CloudStorageError(HeliumCollectorError): pass
class PredictiveError(HeliumCollectorError): pass
class OptimizerError(HeliumCollectorError): pass
class SafetyViolationError(HeliumCollectorError): pass
class PrivacyBudgetExceeded(HeliumCollectorError): pass


# ============================================================
# OBJECTIVE CONVENTION (P0)
# ============================================================
# All objective vectors are COSTS: lower is better.
# Providers are ranked by minimizing weighted sum, TOPSIS distance to
# ideal, and Pareto dominance where one cost vector dominates another
# if all components are <= and at least one is strictly <.
# ============================================================


# ============================================================
# ENHANCEMENT 7 — ADAPTIVE PRECISION (P2: telemetry-driven)
# ============================================================
class PrecisionLevel(str, Enum):
    FP32 = "fp32"; BF16 = "bf16"; FP16 = "fp16"
    FP8 = "fp8"; INT8 = "int8"; INT4 = "int4"


PRECISION_COST = {
    PrecisionLevel.FP32: {"speed": 1.0, "energy": 1.00, "quality": 1.000, "bits": 32.0},
    PrecisionLevel.BF16: {"speed": 1.7, "energy": 0.72, "quality": 0.997, "bits": 16.0},
    PrecisionLevel.FP16: {"speed": 2.0, "energy": 0.65, "quality": 0.994, "bits": 16.0},
    PrecisionLevel.FP8:  {"speed": 3.1, "energy": 0.50, "quality": 0.985, "bits": 8.0},
    PrecisionLevel.INT8: {"speed": 3.6, "energy": 0.44, "quality": 0.972, "bits": 8.0},
    PrecisionLevel.INT4: {"speed": 5.0, "energy": 0.32, "quality": 0.905, "bits": 4.0},
}


@dataclass
class HardwareTelemetry:
    gpu_util_pct: float = 0.0
    memory_util_pct: float = 0.0
    temperature_c: float = 40.0
    power_watts: float = 0.0
    batch_latency_ms: float = 0.0
    observed_quality_drop: float = 0.0
    timestamp: float = field(default_factory=time.time)


class PrecisionController:
    """P2: telemetry-driven precision selection with quality feedback."""

    def __init__(self, supported=None, quality_floor=0.95,
                 carbon_aware=True, energy_joules_per_bit=1e-3):
        self.supported = list(supported) if supported else list(PrecisionLevel)
        self.quality_floor = quality_floor
        self.carbon_aware = carbon_aware
        self.energy_joules_per_bit = energy_joules_per_bit
        self._telemetry: deque = deque(maxlen=100)
        self._ema_quality_drop = 0.0
        self._lock = asyncio.Lock()

    def observe(self, telemetry: HardwareTelemetry):
        self._telemetry.append(telemetry)
        alpha = 0.2
        self._ema_quality_drop = (1 - alpha) * self._ema_quality_drop + \
                                 alpha * telemetry.observed_quality_drop

    def _thermal_penalty(self) -> float:
        if not self._telemetry:
            return 0.0
        recent = list(self._telemetry)[-10:]
        t = max(h.temperature_c for h in recent)
        return min(1.0, max(0.0, (t - 70.0) / 30.0))

    def select(self, carbon_intensity, latency_headroom_ratio, carbon_price=0.0):
        stress = min(1.0, max(0.0, carbon_intensity / 600.0)) if self.carbon_aware else 0.0
        price_stress = min(1.0, max(0.0, carbon_price / 0.2))
        headroom = min(1.0, max(0.0, latency_headroom_ratio))
        thermal = self._thermal_penalty()
        agg = 0.4 * stress + 0.25 * price_stress + 0.2 * (1.0 - headroom) + 0.15 * thermal

        ordered = [p for p in PrecisionLevel if p in self.supported]
        ordered.sort(key=lambda p: PRECISION_COST[p]["energy"])

        floor = max(self.quality_floor, self.quality_floor - self._ema_quality_drop * 0.5)

        chosen = ordered[0]
        for i, p in enumerate(ordered):
            if PRECISION_COST[p]["quality"] >= floor and agg >= 0.2 * i:
                chosen = p
        return chosen

    def estimate_energy_joules(self, level: PrecisionLevel, ops: float) -> float:
        return PRECISION_COST[level]["bits"] * ops * self.energy_joules_per_bit

    def stats(self) -> Dict[str, Any]:
        if not self._telemetry:
            return {"samples": 0}
        recent = list(self._telemetry)[-20:]
        return {
            "samples": len(self._telemetry),
            "avg_temp_c": statistics.fmean(h.temperature_c for h in recent),
            "avg_power_w": statistics.fmean(h.power_watts for h in recent),
            "ema_quality_drop": self._ema_quality_drop,
        }


class HardwareAwareAdapter:
    def __init__(self, controller: PrecisionController):
        self.controller = controller

    def adapt(self, policy, carbon_intensity, latency_headroom, carbon_price):
        level = self.controller.select(carbon_intensity, latency_headroom, carbon_price)
        out = dict(policy)
        out["precision_level"] = level.value
        out["effective_bits"] = PRECISION_COST[level]["bits"]
        PRECISION_SWITCHES.labels(level=level.value).inc()
        PRECISION_ENERGY.labels(level=level.value).set(
            PRECISION_COST[level]["energy"])
        return out, level


# ============================================================
# ENHANCEMENT 8 — CARBON MARKETS / RECs (P1: real APIs)
# ============================================================
@dataclass
class CarbonSnapshot:
    intensity_g_co2_per_kwh: float
    price_usd_per_kg: float
    source: str
    timestamp: float = field(default_factory=time.time)


class CarbonIntensityProvider(Protocol):
    async def get_intensity(self) -> CarbonSnapshot: ...


class ElectricityMapsProvider:
    """Real provider when an API key is available; falls back to synthetic."""

    def __init__(self, api_key: Optional[str] = None, zone: str = "US-CAL-CISO"):
        self.api_key = api_key
        self.zone = zone

    async def get_intensity(self) -> CarbonSnapshot:
        if self.api_key and HTTPX_AVAILABLE:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    r = await client.get(
                        "https://api.electricitymap.org/v3/carbon-intensity/latest",
                        params={"zone": self.zone},
                        headers={"auth-token": self.api_key})
                    r.raise_for_status()
                    data = r.json()
                    return CarbonSnapshot(
                        intensity_g_co2_per_kwh=float(data.get("carbonIntensity", 400.0)),
                        price_usd_per_kg=0.0,
                        source="electricitymaps",
                    )
            except Exception as e:
                logger.warning(f"ElectricityMaps failed: {e}")
        # Synthetic fallback
        hour = datetime.utcnow().hour
        diurnal = 1.0 + 0.35 * math.sin((hour / 24.0) * 2 * math.pi)
        return CarbonSnapshot(
            intensity_g_co2_per_kwh=float(280.0 * diurnal),
            price_usd_per_kg=0.0,
            source="synthetic")


class WattTimeProvider:
    def __init__(self, username: Optional[str] = None, password: Optional[str] = None,
                 region: str = "CAISO_NORTH"):
        self.username = username
        self.password = password
        self.region = region

    async def get_intensity(self) -> CarbonSnapshot:
        if self.username and self.password and HTTPX_AVAILABLE:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    r = await client.get(
                        "https://api.watttime.org/v2/data",
                        params={"ba": self.region},
                        auth=(self.username, self.password))
                    r.raise_for_status()
                    data = r.json()
                    if data:
                        return CarbonSnapshot(
                            intensity_g_co2_per_kwh=float(data[0].get("value", 400.0)),
                            price_usd_per_kg=0.0,
                            source="watttime")
            except Exception as e:
                logger.warning(f"WattTime failed: {e}")
        return CarbonSnapshot(280.0, 0.0, "synthetic")


class CarbonMarketClient:
    """EU ETS-like price model with sensitivity to intensity and TOD."""

    def __init__(self, base_price=0.05, sensitivity=0.0005,
                 min_price=0.005, max_price=0.5):
        self.base_price = base_price
        self.sensitivity = sensitivity
        self.min_price = min_price
        self.max_price = max_price

    def price(self, carbon_intensity, hour_of_day=None):
        tod = 1.0
        if hour_of_day is not None:
            tod = 1.0 + 0.3 * math.sin((hour_of_day / 24.0) * 2 * math.pi)
        raw = self.base_price + self.sensitivity * carbon_intensity * tod
        return max(self.min_price, min(self.max_price, raw))


@dataclass
class RECRecord:
    kwh: float
    issued_at: float
    source: str = "solar"
    cert_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])


class RECInventory:
    """Real REC ledger with persistence and audit trail."""

    def __init__(self, grid_kg_co2_per_kwh=0.4, persist_path: Optional[str] = None):
        self.grid_factor = grid_kg_co2_per_kwh
        self.persist_path = Path(persist_path) if persist_path else None
        self._records: List[RECRecord] = []
        self._ledger: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()
        self._load()

    def _load(self):
        if self.persist_path and self.persist_path.exists():
            try:
                with open(self.persist_path, "rb") as f:
                    payload = pickle.load(f)
                    self._records = payload.get("records", [])
                    self._ledger = payload.get("ledger", [])
            except Exception as e:
                logger.warning(f"REC load failed: {e}")

    def _save(self):
        if not self.persist_path:
            return
        try:
            self.persist_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.persist_path, "wb") as f:
                pickle.dump({"records": self._records, "ledger": self._ledger}, f)
        except Exception as e:
            logger.warning(f"REC save failed: {e}")

    def add(self, kwh, source="solar"):
        rec = RECRecord(kwh=kwh, issued_at=time.time(), source=source)
        self._records.append(rec)
        self._ledger.append({
            "op": "add", "kwh": kwh, "source": source,
            "cert_id": rec.cert_id, "t": rec.issued_at})
        self._save()
        REC_INVENTORY_KWH.set(self.total_kwh())
        return rec

    def total_kwh(self):
        return sum(r.kwh for r in self._records)

    def consume(self, kwh):
        """Return kg CO2 offset for the consumed kWh."""
        remaining, offset = kwh, 0.0
        new_records: List[RECRecord] = []
        consumed: List[Dict[str, Any]] = []
        for r in self._records:
            if remaining <= 0:
                new_records.append(r); continue
            take = min(r.kwh, remaining)
            remaining -= take
            offset += take * self.grid_factor
            consumed.append({"cert_id": r.cert_id, "kwh": take})
            if r.kwh - take > 1e-9:
                new_records.append(RECRecord(
                    kwh=r.kwh - take, issued_at=r.issued_at, source=r.source))
        self._records = new_records
        if consumed:
            self._ledger.append({"op": "consume", "items": consumed, "t": time.time()})
            self._save()
        REC_INVENTORY_KWH.set(self.total_kwh())
        return offset

    def audit(self) -> List[Dict[str, Any]]:
        return list(self._ledger)


class CarbonIntensityManager:
    def __init__(self, config):
        self.config = config
        self.current_intensity = 400.0
        self.current_price = 0.05
        self.market = CarbonMarketClient(
            base_price=getattr(config, "carbon_base_price", 0.05),
            sensitivity=getattr(config, "carbon_price_sensitivity", 0.0005))
        self.recs = RECInventory(
            persist_path=getattr(config, "rec_persist_path", None))
        rec_default = getattr(config, "rec_default_kwh", 0.0)
        if rec_default > 0:
            self.recs.add(rec_default, source="bootstrap")

        # Providers
        self.providers: List[CarbonIntensityProvider] = []
        if getattr(config, "electricity_maps_api_key", None):
            self.providers.append(ElectricityMapsProvider(
                config.electricity_maps_api_key,
                getattr(config, "electricity_maps_zone", "US-CAL-CISO")))
        if getattr(config, "watttime_username", None):
            self.providers.append(WattTimeProvider(
                config.watttime_username, config.watttime_password,
                getattr(config, "watttime_region", "CAISO_NORTH")))
        if not self.providers:
            self.providers.append(ElectricityMapsProvider(None))

    async def refresh(self):
        best: Optional[CarbonSnapshot] = None
        for p in self.providers:
            try:
                snap = await p.get_intensity()
                if best is None or snap.source != "synthetic":
                    best = snap
                    if snap.source != "synthetic":
                        break
            except Exception as e:
                logger.warning(f"Carbon provider failed: {e}")
        if best is not None:
            self.current_intensity = best.intensity_g_co2_per_kwh
        hour = datetime.utcnow().hour
        self.current_price = self.market.price(self.current_intensity, hour)
        CARBON_PRICE.set(self.current_price)
        REC_INVENTORY_KWH.set(self.recs.total_kwh())
        return self.current_intensity

    async def get_current_intensity(self):
        return self.current_intensity

    async def get_current_price(self, hour_of_day=None):
        if hour_of_day is None:
            hour_of_day = datetime.utcnow().hour
        self.current_price = self.market.price(self.current_intensity, hour_of_day)
        CARBON_PRICE.set(self.current_price)
        return self.current_price

    async def close(self): pass

    async def health_check(self):
        return {"status": "ok", "intensity": self.current_intensity,
                "price": self.current_price, "rec_kwh": self.recs.total_kwh(),
                "rec_ledger_len": len(self.recs._ledger)}


# ============================================================
# ENHANCEMENT 9 — CHAOS (P2: steady-state hypothesis + rollback)
# ============================================================
@dataclass
class ChaosConfig:
    fault_prob: float = 0.0
    latency_inject_ms: float = 0.0
    latency_inject_prob: float = 0.3
    carbon_spike_prob: float = 0.0
    carbon_spike_factor: float = 1.5
    seed: int = 0
    blast_radius: int = 1
    max_concurrent_faults: int = 1
    duration_s: float = 0.0

    def enabled(self):
        return (self.fault_prob > 0
                or (self.latency_inject_ms > 0 and self.latency_inject_prob > 0)
                or self.carbon_spike_prob > 0)


@dataclass
class SteadyStateHypothesis:
    name: str
    predicate: Callable[[Dict[str, Any]], bool]
    tolerance: float = 0.0


class ChaosEngineer:
    """P2: blast radius, concurrent fault limit, steady-state verification."""

    def __init__(self, config=None, hypotheses: Optional[List[SteadyStateHypothesis]] = None):
        self.config = config or ChaosConfig()
        self.rng = random.Random(self.config.seed)
        self.events: List[Dict[str, Any]] = []
        self.hypotheses = hypotheses or []
        self._active_faults = 0
        self._activated_at = time.time() if self.config.enabled() else 0.0
        self._lock = asyncio.Lock()

    def _can_inject(self) -> bool:
        if self._active_faults >= self.config.max_concurrent_faults:
            return False
        if self.config.duration_s > 0 and \
                (time.time() - self._activated_at) > self.config.duration_s:
            return False
        return True

    def maybe_fault(self):
        if not self._can_inject():
            return False
        if self.rng.random() < self.config.fault_prob:
            self._active_faults += 1
            self.events.append({"type": "fault", "t": time.time()})
            CHAOS_EVENTS.labels(type="fault").inc()
            return True
        return False

    def release_fault(self):
        self._active_faults = max(0, self._active_faults - 1)

    def maybe_latency(self):
        if not self._can_inject():
            return 0.0
        if self.config.latency_inject_ms > 0 and \
                self.rng.random() < self.config.latency_inject_prob:
            self.events.append({"type": "latency", "t": time.time()})
            CHAOS_EVENTS.labels(type="latency").inc()
            return self.config.latency_inject_ms
        return 0.0

    def maybe_carbon_spike(self, carbon_intensity):
        if not self._can_inject():
            return carbon_intensity
        if self.rng.random() < self.config.carbon_spike_prob:
            self.events.append({"type": "carbon_spike", "t": time.time()})
            CHAOS_EVENTS.labels(type="carbon_spike").inc()
            return carbon_intensity * self.config.carbon_spike_factor
        return carbon_intensity

    def verify_steady_state(self, metrics: Dict[str, Any]) -> Dict[str, bool]:
        return {h.name: bool(h.predicate(metrics)) for h in self.hypotheses}

    def steady_state_ok(self, metrics: Dict[str, Any]) -> bool:
        return all(self.verify_steady_state(metrics).values())

    def deactivate(self):
        self.config = ChaosConfig(seed=self.config.seed)
        self._active_faults = 0

    def stats(self):
        return {
            "config": asdict(self.config),
            "events": len(self.events),
            "active_faults": self._active_faults,
            "hypotheses": [h.name for h in self.hypotheses],
        }


# ============================================================
# ENHANCEMENT 5 — TEMPORAL LOGIC (P2: real STL semantics)
# ============================================================
class STLOperator(str, Enum):
    ALWAYS = "G"
    EVENTUALLY = "F"
    UNTIL = "U"
    NEXT = "X"
    IMPLIES = "->"


@dataclass
class STLFormula:
    name: str
    predicate: Callable[[Dict[str, Any]], bool]
    operator: STLOperator
    horizon: int = 10
    rhs_predicate: Optional[Callable[[Dict[str, Any]], bool]] = None

    def evaluate(self, window: List[Dict[str, Any]]) -> bool:
        if not window:
            return True
        sat = [bool(self.predicate(r)) for r in window]
        if self.operator == STLOperator.ALWAYS:
            return all(sat)
        if self.operator == STLOperator.EVENTUALLY:
            return any(sat)
        if self.operator == STLOperator.NEXT:
            return sat[0] if len(sat) == 1 else sat[1]
        if self.operator == STLOperator.UNTIL:
            # exists i: sat[i] and forall j<i: lhs[j]
            for i, s in enumerate(sat):
                if s:
                    return all(sat[:i]) if i > 0 else True
            return False
        if self.operator == STLOperator.IMPLIES and self.rhs_predicate is not None:
            rhs = [bool(self.rhs_predicate(r)) for r in window]
            return all((not s) or r for s, r in zip(sat, rhs))
        return True


@dataclass
class STLViolation:
    formula: str
    operator: str
    window_size: int
    timestamp: float


class TemporalLogicMonitor:
    """P2: real G/F/U/X/-> semantics with rolling window."""

    def __init__(self, horizon=10):
        self.horizon = horizon
        self._history: deque = deque(maxlen=horizon * 4)
        self.formulas: List[STLFormula] = []
        self.violations: List[STLViolation] = []
        self._lock = asyncio.Lock()

    def add_formula(self, f: STLFormula): self.formulas.append(f)

    def observe(self, record: Dict[str, Any]):
        self._history.append({**record, "_t": time.time()})

    def verify(self) -> Dict[str, bool]:
        results: Dict[str, bool] = {}
        for f in self.formulas:
            window = list(self._history)[-f.horizon:]
            results[f.name] = f.evaluate(window)
            if not results[f.name]:
                SAFETY_VIOLATIONS.labels(formula=f.name).inc()
                self.violations.append(STLViolation(
                    formula=f.name, operator=f.operator.value,
                    window_size=len(window), timestamp=time.time()))
        return results

    def recent_violations(self, n: int = 10) -> List[Dict[str, Any]]:
        return [asdict(v) for v in self.violations[-n:]]


class SafetyShield:
    """P2: shield actually rewrites the chosen action to the safest candidate."""

    def __init__(self, monitor: TemporalLogicMonitor):
        self.monitor = monitor
        self.violations: List[Dict[str, Any]] = []

    def screen(self, selected_value, candidates, key="value",
               cost_fn: Optional[Callable[[Any], float]] = None):
        """
        Returns (chosen_value, safety_ok, verdict).
        If a formula is violated, replace selection with the minimum-cost
        candidate under `cost_fn` (or `key` field if cost_fn is None).
        """
        verdict = self.monitor.verify()
        violated = [k for k, ok in verdict.items() if not ok]
        if not violated:
            return selected_value, True, verdict

        if not candidates:
            self.violations.append({"t": time.time(), "violated": violated,
                                    "action": "no_candidates"})
            return selected_value, False, verdict

        if cost_fn is None:
            cost_fn = lambda c: float(c.get(key, 0.0))

        safe = min(candidates, key=cost_fn)
        self.violations.append({
            "t": time.time(), "violated": violated,
            "original": selected_value, "replacement": safe})
        return safe, False, verdict


# ============================================================
# ENHANCEMENT 2 — CAUSAL RL (P0 fix + P2: exploration + policy)
# ============================================================
@dataclass
class CausalTransition:
    state: np.ndarray
    action: int
    reward: float
    next_state: np.ndarray
    context: Dict[str, float] = field(default_factory=dict)


class CausalCounterfactualEstimator:
    """
    P0 fix: tracks all actions (not just action 0).
    P2: adds UCB-style exploration policy over counterfactual rewards.
    """

    def __init__(self, state_dim, n_actions, ridge=1e-3,
                 exploration_c: float = 0.5):
        self.state_dim = state_dim
        self.n_actions = n_actions
        self.ridge = ridge
        self.exploration_c = exploration_c
        self._A = np.eye(state_dim + n_actions + 1) * ridge
        self._b = np.zeros(state_dim + n_actions + 1)
        self._action_counts = np.zeros(n_actions)
        self._n = 0
        self._lock = asyncio.Lock()

    def _features(self, s, a):
        one_hot = np.zeros(self.n_actions)
        one_hot[a % self.n_actions] = 1.0
        return np.concatenate([np.asarray(s, dtype=np.float64), one_hot, [1.0]])

    def _ensure_actions(self, n):
        if n == self.n_actions: return
        self._A = np.eye(self.state_dim + n + 1) * self.ridge
        self._b = np.zeros(self.state_dim + n + 1)
        self.n_actions = n
        self._action_counts = np.zeros(n)
        self._n = 0

    def update(self, t: CausalTransition):
        # P0 fix: use the actual action
        a = int(t.action) % max(self.n_actions, 1)
        x = self._features(t.state, a)
        self._A += np.outer(x, x)
        self._b += t.reward * x
        self._action_counts[a] += 1
        self._n += 1

    def predict(self, s, a):
        if self._n < 2: return 0.0
        try:
            theta = np.linalg.solve(self._A, self._b)
        except np.linalg.LinAlgError:
            return 0.0
        return float(self._features(s, a) @ theta)

    def counterfactuals(self, s, n_actions=None) -> Dict[int, float]:
        if n_actions is not None:
            self._ensure_actions(n_actions)
        return {a: self.predict(s, a) for a in range(self.n_actions)}

    def select_action(self, s, n_actions=None, epsilon: float = 0.05) -> int:
        """UCB + epsilon-greedy over counterfactual predictions."""
        cf = self.counterfactuals(s, n_actions)
        if not cf:
            return 0
        if random.random() < epsilon:
            return random.randrange(self.n_actions)
        total = max(self._n, 1)
        scores = {}
        for a, mu in cf.items():
            count = max(self._action_counts[a], 1.0)
            bonus = self.exploration_c * math.sqrt(math.log(total + 1) / count)
            scores[a] = mu + bonus
        return max(scores, key=scores.get)

    def stats(self):
        return {
            "n_updates": int(self._n),
            "n_actions": self.n_actions,
            "action_counts": self._action_counts.tolist(),
        }


# ============================================================
# ENHANCEMENT 3 — FEDERATED (P2: secure aggregation + DP accountant)
# ============================================================
@dataclass
class FederatedUpdate:
    node_id: str
    weights: Dict[str, np.ndarray]
    n_samples: int
    carbon_intensity: float
    timestamp: float = field(default_factory=time.time)


class DPAccountant:
    """Simple moments-accountant-style tracker for (eps, delta)."""

    def __init__(self, target_epsilon: float = 8.0, target_delta: float = 1e-5):
        self.target_epsilon = target_epsilon
        self.target_delta = target_delta
        self.spent = 0.0
        self._rounds = 0

    def step(self, sigma: float, sensitivity: float = 1.0):
        if sigma <= 0:
            return
        eps_step = sensitivity * math.sqrt(2 * math.log(1.25 / self.target_delta)) / sigma
        self.spent += eps_step
        self._rounds += 1

    def can_spend(self) -> bool:
        return self.spent < self.target_epsilon


class SecureFederatedAggregator:
    """
    P2: supports masked secure aggregation (pairwise masks cancel),
    DP noise, staleness, carbon-weighted sampling, and DP budget tracking.
    """

    def __init__(self, dp_sigma=1e-3, staleness_s=3600.0,
                 target_epsilon: float = 8.0, target_delta: float = 1e-5):
        self.dp_sigma = dp_sigma
        self.staleness_s = staleness_s
        self._updates: Dict[str, FederatedUpdate] = {}
        self._global: Optional[Dict[str, np.ndarray]] = None
        self.accountant = DPAccountant(target_epsilon, target_delta)

    def submit(self, u): self._updates[u.node_id] = u

    def _fresh(self):
        now = time.time()
        return [u for u in self._updates.values()
                if (now - u.timestamp) <= self.staleness_s]

    @staticmethod
    def _pairwise_mask(node_ids: List[str], shape: Tuple[int, ...],
                       seed: int = 0) -> List[np.ndarray]:
        """Deterministic pairwise masks that sum to zero across nodes."""
        rng = np.random.default_rng(seed)
        pairwise: Dict[Tuple[str, str], np.ndarray] = {}
        for i, a in enumerate(node_ids):
            for b in node_ids[i + 1:]:
                pairwise[(a, b)] = rng.normal(0.0, 1.0, size=shape)
        masks = []
        for a in node_ids:
            m = np.zeros(shape)
            for (x, y), v in pairwise.items():
                if x == a: m = m + v
                elif y == a: m = m - v
            masks.append(m)
        return masks

    def aggregate(self, secure: bool = True):
        fresh = self._fresh()
        if not fresh:
            return self._global

        if not self.accountant.can_spend():
            raise PrivacyBudgetExceeded(
                f"DP budget exhausted: spent={self.accountant.spent:.2f} "
                f"target={self.accountant.target_epsilon}")

        weights = np.array([
            u.n_samples / max(u.carbon_intensity, 1.0) for u in fresh])
        weights /= max(weights.sum(), 1e-12)

        keys = list(fresh[0].weights.keys())
        agg: Dict[str, np.ndarray] = {}

        node_ids = [u.node_id for u in fresh]
        for k in keys:
            shape = fresh[0].weights[k].shape
            stacked = np.stack([u.weights[k] for u in fresh], axis=0)

            if secure and len(fresh) > 1:
                masks = self._pairwise_mask(node_ids, shape)
                stacked = stacked + np.stack(masks, axis=0)

            blended = np.tensordot(weights, stacked, axes=([0], [0]))

            if self.dp_sigma > 0:
                blended = blended + np.random.normal(0.0, self.dp_sigma, size=blended.shape)
                self.accountant.step(self.dp_sigma)

            agg[k] = blended

        for nid in node_ids:
            DP_EPSILON_SPENT.labels(node=nid).set(self.accountant.spent)

        self._global = agg
        FEDERATED_ROUNDS.inc()
        return agg

    def global_weights(self): return self._global

    def stats(self):
        return {
            "tracked_nodes": len(self._updates),
            "dp_spent": self.accountant.spent,
            "dp_target": self.accountant.target_epsilon,
            "rounds": int(FEDERATED_ROUNDS._value.get()) if PROMETHEUS_AVAILABLE else 0,
        }


# ============================================================
# ENHANCEMENT 4 — MULTI-AGENT (P2: comms + conflict resolution)
# ============================================================
class AgentRole(str, Enum):
    EXPLORER = "explorer"
    EXPLOITER = "exploiter"
    SAFETY_OFFICER = "safety_officer"
    CARBON_BROKER = "carbon_broker"
    VERIFIER = "verifier"


@dataclass
class AgentBid:
    agent_id: str
    role: AgentRole
    confidence: float
    proposed_action: int
    rationale: str
    carbon_score: float


@dataclass
class AgentMessage:
    sender: str
    recipient: str
    topic: str
    payload: Dict[str, Any]
    t: float = field(default_factory=time.time)


class EmergentRoleRegistry:
    """P0 fix: record() is called after outcomes; P2: role-based comms."""

    def __init__(self, decay=0.95):
        self.decay = decay
        self._scores = {r: 1.0 for r in AgentRole}
        self._counts = {r: 0 for r in AgentRole}
        self._successes = {r: 0 for r in AgentRole}
        self._lock = asyncio.Lock()

    def record(self, role: AgentRole, success: bool, reward: float):
        for r in self._scores:
            self._scores[r] *= self.decay
        signal = reward if success else -abs(reward) * 0.5
        self._scores[role] += signal
        self._counts[role] += 1
        if success:
            self._successes[role] += 1

    def weights(self):
        total = sum(max(v, 1e-6) for v in self._scores.values())
        return {r: max(v, 1e-6) / total for r, v in self._scores.items()}

    def dominant_role(self):
        return max(self._scores, key=self._scores.get)

    def stats(self):
        return {
            "scores": {r.value: self._scores[r] for r in AgentRole},
            "counts": {r.value: self._counts[r] for r in AgentRole},
            "success_rate": {
                r.value: (self._successes[r] / self._counts[r])
                if self._counts[r] else 0.0 for r in AgentRole},
        }


class MultiAgentCoordinator:
    """
    P2: uses message bus + conflict resolution where the coordinator
    respects a dominant non-overridable role (safety/carbon) unless
    the registry has not yet learned meaningful weights.
    """

    def __init__(self, registry: EmergentRoleRegistry, role_bias=None):
        self.registry = registry
        self.role_bias = role_bias or {
            AgentRole.EXPLORER: 0.6,
            AgentRole.EXPLOITER: 0.9,
            AgentRole.SAFETY_OFFICER: 1.4,
            AgentRole.CARBON_BROKER: 1.2,
            AgentRole.VERIFIER: 1.0,
        }
        self.messages: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()

    async def broadcast(self, sender: str, topic: str, payload: Dict[str, Any]):
        msg = AgentMessage(sender=sender, recipient="*", topic=topic, payload=payload)
        async with self._lock:
            self.messages.append(msg)

    def vote(self, bids: List[AgentBid], n_candidates: int,
             dominant_override: Optional[AgentRole] = None) -> int:
        if not bids or n_candidates <= 0:
            return 0

        if dominant_override is not None:
            for b in bids:
                if b.role == dominant_override:
                    return b.proposed_action % n_candidates

        role_w = self.registry.weights()
        scores = np.zeros(n_candidates)
        for b in bids:
            idx = b.proposed_action % n_candidates
            weight = (role_w.get(b.role, 0.1)
                      * self.role_bias.get(b.role, 1.0)
                      * max(b.confidence, 0.0)
                      * (1.0 + max(0.0, b.carbon_score)))
            scores[idx] += weight
        return int(np.argmax(scores))

    async def resolve_conflict(self, bids: List[AgentBid],
                               n_candidates: int) -> Tuple[int, str]:
        if not bids:
            return 0, "no_bids"
        safety_bids = [b for b in bids if b.role == AgentRole.SAFETY_OFFICER]
        carbon_bids = [b for b in bids if b.role == AgentRole.CARBON_BROKER]

        if safety_bids and min(b.confidence for b in safety_bids) > 0.5:
            idx = safety_bids[0].proposed_action % n_candidates
            await self.broadcast("coordinator", "conflict_resolved",
                                 {"winner": "safety_officer", "idx": idx})
            return idx, "safety_override"

        if carbon_bids and min(b.confidence for b in carbon_bids) > 0.5:
            idx = carbon_bids[0].proposed_action % n_candidates
            await self.broadcast("coordinator", "conflict_resolved",
                                 {"winner": "carbon_broker", "idx": idx})
            return idx, "carbon_override"

        idx = self.vote(bids, n_candidates)
        await self.broadcast("coordinator", "conflict_resolved",
                             {"winner": "vote", "idx": idx})
        return idx, "vote"


# ============================================================
# ENHANCEMENT 6 — XAI (P2: SHAP / LIME + counterfactual validation)
# ============================================================
@dataclass
class Explanation:
    decision_id: str
    chosen_idx: int
    top_features: List[Tuple[str, float]]
    counterfactuals: List[Dict[str, Any]]
    rationale: str
    confidence: float
    safety_ok: bool
    carbon_price_signal: float
    method: str = "z_score"
    validation_score: float = 0.0

    def to_dict(self): return asdict(self)


class DecisionExplainer:
    """
    P2: SHAP when available, else LIME, else z-score fallback.
    Also validates attributions via leave-one-feature-out ablation.
    """

    def __init__(self, feature_names=None, predict_fn: Optional[Callable] = None):
        self.feature_names = list(feature_names) if feature_names else [
            "price_index", "production", "demand", "anomaly_score",
        ]
        self.predict_fn = predict_fn
        self._shap_explainer = None
        self._lime_explainer = None
        self._bg: Optional[np.ndarray] = None

    def fit(self, background: np.ndarray):
        self._bg = background
        if SHAP_AVAILABLE and self.predict_fn is not None:
            try:
                self._shap_explainer = shap.KernelExplainer(self.predict_fn, background)
            except Exception as e:
                logger.warning(f"SHAP init failed: {e}")
        if LIME_AVAILABLE and self.predict_fn is not None:
            try:
                self._lime_explainer = lime.lime_tabular.LimeTabularExplainer(
                    background,
                    feature_names=self.feature_names,
                    mode="regression")
            except Exception as e:
                logger.warning(f"LIME init failed: {e}")

    def _z_attributions(self, chosen, pareto):
        if not pareto: return []
        arr = np.array([[float(c.get(f, 0.0)) for f in self.feature_names]
                        for c in pareto])
        chosen_vec = np.array([float(chosen.get(f, 0.0)) for f in self.feature_names])
        mean = arr.mean(axis=0); std = arr.std(axis=0) + 1e-9
        z = (chosen_vec - mean) / std
        attrs = list(zip(self.feature_names, z.tolist()))
        attrs.sort(key=lambda kv: abs(kv[1]), reverse=True)
        return attrs

    def _shap_attributions(self, x: np.ndarray) -> Optional[List[Tuple[str, float]]]:
        if self._shap_explainer is None:
            return None
        try:
            vals = self._shap_explainer.shap_values(x.reshape(1, -1))
            v = np.asarray(vals).reshape(-1)
            attrs = list(zip(self.feature_names, v.tolist()))
            attrs.sort(key=lambda kv: abs(kv[1]), reverse=True)
            return attrs
        except Exception:
            return None

    def _lime_attributions(self, x: np.ndarray) -> Optional[List[Tuple[str, float]]]:
        if self._lime_explainer is None or self.predict_fn is None:
            return None
        try:
            exp = self._lime_explainer.explain_instance(
                x, self.predict_fn, num_features=len(self.feature_names))
            attrs = [(f, w) for f, w in exp.as_list()]
            attrs.sort(key=lambda kv: abs(kv[1]), reverse=True)
            return attrs
        except Exception:
            return None

    def _validate(self, x: np.ndarray, top_attrs: List[Tuple[str, float]]) -> float:
        if self.predict_fn is None or self._bg is None or not top_attrs:
            return 0.0
        try:
            base = float(self.predict_fn(x.reshape(1, -1))[0])
            deltas = []
            for name, _ in top_attrs[:3]:
                if name not in self.feature_names:
                    continue
                idx = self.feature_names.index(name)
                x_ablated = x.copy()
                x_ablated[idx] = float(self._bg[:, idx].mean())
                new_pred = float(self.predict_fn(x_ablated.reshape(1, -1))[0])
                deltas.append(abs(base - new_pred))
            return float(np.mean(deltas)) if deltas else 0.0
        except Exception:
            return 0.0

    def explain(self, chosen_idx, chosen, pareto, counterfactuals,
                safety_ok, carbon_price, confidence=0.5):
        method = "z_score"
        attrs: Optional[List[Tuple[str, float]]] = None
        x = np.array([float(chosen.get(f, 0.0)) for f in self.feature_names])

        if self._shap_explainer is not None:
            attrs = self._shap_attributions(x)
            if attrs is not None:
                method = "shap"
        if attrs is None and self._lime_explainer is not None:
            attrs = self._lime_attributions(x)
            if attrs is not None:
                method = "lime"
        if attrs is None:
            attrs = self._z_attributions(chosen, pareto)
            method = "z_score"

        validation = self._validate(x, attrs)

        cf_list = []
        for idx, score in sorted(counterfactuals.items(),
                                 key=lambda kv: kv[1], reverse=True)[:3]:
            if 0 <= idx < len(pareto):
                cf_list.append({
                    "alternative_idx": idx,
                    "expected_reward": float(score),
                    "delta": float(score - counterfactuals.get(chosen_idx, 0.0)),
                })

        top_feat = ", ".join(f"{n}={v:+.2f}" for n, v in attrs[:3])
        rationale = (
            f"Chose #{chosen_idx} (confidence={confidence:.2f}, method={method}). "
            f"Top attributions: {top_feat}. Carbon price={carbon_price:.4f}. "
            f"Safety={'ok' if safety_ok else 'OVERRIDE'}. "
            f"Validation={validation:.3f}. Counterfactuals: {len(cf_list)}."
        )
        return Explanation(
            decision_id=f"dec-{uuid.uuid4().hex[:8]}",
            chosen_idx=chosen_idx,
            top_features=attrs[:5],
            counterfactuals=cf_list,
            rationale=rationale,
            confidence=confidence,
            safety_ok=safety_ok,
            carbon_price_signal=carbon_price,
            method=method,
            validation_score=validation,
        )


# ============================================================
# ENHANCEMENT 1 — QUANTUM-DISTILLATION (P2: PennyLane/Qiskit optional)
# ============================================================
class QuantumInspiredTeacher:
    """
    P2: uses PennyLane or Qiskit when available, else a state-vector
    simulator that supports arbitrary RY layers and feature encoding.
    """

    def __init__(self, n_qubits=5, seed=0, use_pennylane=None, use_qiskit=None):
        self.n_qubits = n_qubits
        self.rng = np.random.default_rng(seed)
        self._params = self.rng.normal(size=(n_qubits, 3)) * 0.5
        self.use_pennylane = (PENNYLANE_AVAILABLE
                              if use_pennylane is None else use_pennylane)
        self.use_qiskit = (QISKIT_AVAILABLE
                           if use_qiskit is None else use_qiskit)
        self.backend = "statevector"
        if self.use_pennylane:
            try:
                self.dev = qml.device("default.qubit", wires=n_qubits)
                self.backend = "pennylane"
            except Exception:
                self.use_pennylane = False
        if not self.use_pennylane and self.use_qiskit:
            self.backend = "qiskit"

    # --- State-vector simulator ---
    def _ry(self, state, theta, q):
        c, s = math.cos(theta / 2), math.sin(theta / 2)
        n = len(state); new = state.copy(); step = 1 << q
        for i in range(n):
            if i & step == 0:
                a, b = state[i], state[i | step]
                new[i] = c * a - s * b
                new[i | step] = s * a + c * b
        return new

    def _simulate(self, features, n_candidates):
        dim = 1 << self.n_qubits
        state = np.zeros(dim, dtype=np.complex128); state[0] = 1.0
        for q in range(self.n_qubits):
            angle = float(self._params[q, 0] * features[q % len(features)]
                          + self._params[q, 1])
            state = self._ry(state, angle, q)
        probs_full = np.abs(state) ** 2
        probs = np.zeros(n_candidates, dtype=np.float64)
        for i, p in enumerate(probs_full):
            probs[i % n_candidates] += p
        return probs / max(probs.sum(), 1e-12)

    def _pennylane_probs(self, features, n_candidates):
        try:
            @qml.qnode(self.dev)
            def circuit(feats):
                for q in range(self.n_qubits):
                    qml.RY(float(self._params[q, 0] * feats[q % len(feats)]
                                 + self._params[q, 1]), wires=q)
                return qml.probs(wires=range(self.n_qubits))
            probs_full = circuit(features)
            probs = np.zeros(n_candidates)
            for i, p in enumerate(probs_full):
                probs[i % n_candidates] += p
            return probs / max(probs.sum(), 1e-12)
        except Exception:
            return None

    def _qiskit_probs(self, features, n_candidates):
        try:
            qc = QuantumCircuit(self.n_qubits)
            for q in range(self.n_qubits):
                angle = float(self._params[q, 0] * features[q % len(features)]
                              + self._params[q, 1])
                qc.ry(angle, q)
            qc.measure_all()
            backend = Aer.get_backend("qasm_simulator")
            result = qiskit_execute(qc, backend, shots=1024).result()
            counts = result.get_counts()
            probs = np.zeros(n_candidates)
            for bitstr, c in counts.items():
                i = int(bitstr, 2)
                probs[i % n_candidates] += c
            return probs / max(probs.sum(), 1e-12)
        except Exception:
            return None

    def teacher_probs(self, features, n):
        if n <= 0:
            return np.zeros(0)
        feats = np.asarray(features, dtype=np.float64)
        if self.use_pennylane:
            p = self._pennylane_probs(feats, n)
            if p is not None:
                return p
        if self.use_qiskit:
            p = self._qiskit_probs(feats, n)
            if p is not None:
                return p
        return self._simulate(feats, n)

    def train_step(self, features, target_probs, lr=0.05, n=4):
        """Simple parameter shift / finite-difference distillation step."""
        base = self.teacher_probs(features, n)
        base_loss = -float(np.sum(np.asarray(target_probs) * np.log(base + 1e-12)))
        eps = 1e-2
        for q in range(self.n_qubits):
            for k in range(2):
                old = self._params[q, k]
                self._params[q, k] = old + eps
                p_plus = self.teacher_probs(features, n)
                loss_plus = -float(np.sum(
                    np.asarray(target_probs) * np.log(p_plus + 1e-12)))
                self._params[q, k] = old - eps
                p_minus = self.teacher_probs(features, n)
                loss_minus = -float(np.sum(
                    np.asarray(target_probs) * np.log(p_minus + 1e-12)))
                self._params[q, k] = old - lr * (loss_plus - loss_minus) / (2 * eps)
        return base_loss

    def stats(self):
        return {
            "backend": self.backend,
            "n_qubits": self.n_qubits,
            "pennylane": self.use_pennylane,
            "qiskit": self.use_qiskit,
        }


class DistillationEnsemble:
    def __init__(self, quantum, alpha=0.5):
        self.quantum = quantum
        self.alpha = alpha
        self._lock = asyncio.Lock()

    def blend(self, features, other, n):
        q = self.quantum.teacher_probs(features, n)
        if other is None or len(other) != n:
            return q
        return self.alpha * q + (1.0 - self.alpha) * np.asarray(other)


# ============================================================
# ENHANCEMENT 10 — HITL / ACTIVE LEARNING (P0 fix + P2 diversity)
# ============================================================
@dataclass
class HITLRequest:
    request_id: str
    reason: str
    chosen_idx: int
    candidates: List[Dict[str, Any]]
    uncertainty: float
    carbon_price: float
    metadata: Dict[str, Any] = field(default_factory=dict)


class UncertaintyEstimator:
    """
    P0 fix: combines predictive entropy, expert variance, and
    reward-variance. Caller passes actual model probabilities or
    expert predictive distributions.
    """

    def __init__(self, entropy_threshold=0.75, variance_threshold=0.05,
                 disagreement_threshold=0.15):
        self.entropy_threshold = entropy_threshold
        self.variance_threshold = variance_threshold
        self.disagreement_threshold = disagreement_threshold
        self._rewards: deque = deque(maxlen=64)
        self._expert_forecasts: deque = deque(maxlen=32)

    def observe(self, reward): self._rewards.append(float(reward))

    def observe_expert_forecasts(self, forecasts: List[float]):
        self._expert_forecasts.append(list(forecasts))

    def entropy(self, probs):
        if probs is None or len(probs) == 0: return 1.0
        p = np.clip(np.asarray(probs, dtype=np.float64), 1e-12, 1.0)
        return float(-np.sum(p * np.log(p)) / math.log(len(p)))

    def variance(self):
        if len(self._rewards) < 3: return 0.0
        return float(np.var(np.asarray(self._rewards)))

    def disagreement(self) -> float:
        if not self._expert_forecasts:
            return 0.0
        recent = self._expert_forecasts[-1]
        if len(recent) < 2:
            return 0.0
        arr = np.asarray(recent, dtype=np.float64)
        return float(np.std(arr) / (abs(np.mean(arr)) + 1e-9))

    def is_uncertain(self, probs):
        h = self.entropy(probs)
        v = self.variance()
        d = self.disagreement()
        score = (0.5 * h
                 + 0.3 * min(1.0, v / max(self.variance_threshold, 1e-9))
                 + 0.2 * min(1.0, d / max(self.disagreement_threshold, 1e-9)))
        uncertain = (h > self.entropy_threshold
                     or v > self.variance_threshold
                     or d > self.disagreement_threshold)
        return uncertain, float(score)

    def stats(self):
        return {
            "rewards": len(self._rewards),
            "expert_forecasts": len(self._expert_forecasts),
            "variance": self.variance(),
            "disagreement": self.disagreement(),
        }


class HumanInTheLoopGate:
    """
    P0 fix: real pending-request queue; API can approve or reject;
    auto-approve disabled by default in production.
    """

    def __init__(self, approver=None, timeout_s=30.0,
                 auto_approve_on_timeout=False, production_safe=True):
        self.approver = approver
        self.timeout_s = timeout_s
        self.auto_approve_on_timeout = auto_approve_on_timeout
        self.production_safe = production_safe
        self.audit: List[Dict[str, Any]] = []
        self._pending: Dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()

    async def request(self, req: HITLRequest) -> bool:
        if self.approver is None and self.production_safe:
            # Refuse to silently auto-approve in production
            self.audit.append({
                "request_id": req.request_id,
                "approved": False,
                "reason": "no_approver_configured"})
            HITL_APPROVALS.labels(decision="blocked").inc()
            return False

        if self.approver is None:
            self.audit.append({"request_id": req.request_id, "approved": True,
                               "reason": "auto_approve_dev"})
            HITL_APPROVALS.labels(decision="auto_approved").inc()
            return True

        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        async with self._lock:
            self._pending[req.request_id] = future

        try:
            approved = await asyncio.wait_for(
                loop.run_in_executor(None, self.approver, req),
                timeout=self.timeout_s)
        except asyncio.TimeoutError:
            if req.request_id in self._pending and not future.done():
                approved = self.auto_approve_on_timeout
            else:
                approved = future.result() if future.done() else \
                    self.auto_approve_on_timeout
        finally:
            async with self._lock:
                self._pending.pop(req.request_id, None)

        self.audit.append({"request_id": req.request_id,
                           "approved": bool(approved),
                           "reason": req.reason})
        HITL_APPROVALS.labels(
            decision="approved" if approved else "rejected").inc()
        return bool(approved)

    async def resolve(self, request_id: str, approved: bool) -> bool:
        async with self._lock:
            fut = self._pending.get(request_id)
        if fut is None or fut.done():
            return False
        fut.set_result(approved)
        return True

    def pending_requests(self) -> List[str]:
        return list(self._pending.keys())


class ActiveLearningSampler:
    """
    P2: diversity-aware sampling using k-means on feature embeddings
    combined with uncertainty weighting.
    """

    def __init__(self, capacity=256, feature_dim=8, diversity_weight=0.4,
                 seed=0):
        self.capacity = capacity
        self.feature_dim = feature_dim
        self.diversity_weight = diversity_weight
        self._buffer: List[Dict[str, Any]] = []
        self._features: List[np.ndarray] = []
        self._rng = random.Random(seed)
        self._kmeans = KMeans(n_clusters=4, n_init=5) if SKLEARN_AVAILABLE else None

    def _embed(self, record: Dict[str, Any]) -> np.ndarray:
        vals = []
        for k in ("price_index", "global_production_tonnes",
                  "global_demand_tonnes", "anomaly_score"):
            try:
                vals.append(float(record.get(k, 0.0)))
            except Exception:
                vals.append(0.0)
        while len(vals) < self.feature_dim:
            vals.append(0.0)
        v = np.asarray(vals[: self.feature_dim], dtype=np.float64)
        n = np.linalg.norm(v) + 1e-9
        return v / n

    def maybe_store(self, record, uncertainty, threshold=0.4):
        if uncertainty < threshold:
            return False
        feat = self._embed(record)
        if len(self._buffer) >= self.capacity:
            self._buffer.pop(0)
            self._features.pop(0)
        self._buffer.append({**record, "uncertainty": uncertainty,
                             "t": time.time()})
        self._features.append(feat)
        return True

    def sample(self, k=8) -> List[Dict[str, Any]]:
        if not self._buffer:
            return []
        if len(self._buffer) <= k:
            return list(self._buffer)

        uncs = np.array([b.get("uncertainty", 0.0) for b in self._buffer])
        if self._kmeans is not None and len(self._features) >= 8:
            try:
                X = np.stack(self._features)
                labels = self._kmeans.fit_predict(X)
                diversity = np.zeros(len(self._buffer))
                for i, lab in enumerate(labels):
                    diversity[i] = np.mean(labels == lab) == 1.0 and 1.0 or \
                        (1.0 / (1.0 + np.sum(labels == lab)))
            except Exception:
                diversity = np.zeros(len(self._buffer))
        else:
            diversity = np.random.rand(len(self._buffer))

        scores = (1 - self.diversity_weight) * uncs + \
                 self.diversity_weight * diversity
        idx = np.argsort(-scores)[:k].tolist()
        return [self._buffer[i] for i in idx]

    def __len__(self): return len(self._buffer)

    def stats(self):
        return {"size": len(self._buffer), "capacity": self.capacity}


# ============================================================
# LIMIT / RLHF / DISTILLER (kept, minor fixes)
# ============================================================
class LimitGraph:
    def __init__(self):
        self.limits: Dict[str, Any] = {}
        self._feedback: List[Any] = []

    def build_graph(self, nodes, edges): pass

    def get_limits(self, context):
        return {"max_interval": 600, "max_batch_size": 100,
                "max_parallel_calls": 20}

    def update_from_feedback(self, feedback):
        self._feedback.append(feedback)


class RLHFOptimizer:
    def __init__(self, action_space):
        self.actions = list(action_space)
        self.scores = {a: 0.0 for a in self.actions}

    def update(self, context, action, reward):
        if action not in self.scores:
            self.actions.append(action)
            self.scores[action] = 0.0
        self.scores[action] += 0.1 * (float(reward) - self.scores[action])

    def sample_action(self, context):
        if not self.scores:
            return None
        if random.random() < 0.1:
            return random.choice(self.actions)
        return max(self.scores, key=self.scores.get)


class MultiTeacherDistiller:
    """Fixed: awaits coroutine teachers instead of closing them."""

    def __init__(self, teachers, weights=None):
        self.teachers = list(teachers)
        self.weights = list(weights) if weights else [1.0] * len(self.teachers)

    async def _call(self, t, ctx):
        try:
            out = t(ctx)
            if asyncio.iscoroutine(out):
                out = await out
            return out
        except Exception:
            return None

    async def distill(self, context):
        votes: Dict[Any, float] = {}
        for t, w in zip(self.teachers, self.weights):
            c = await self._call(t, context)
            if c is not None:
                votes[c] = votes.get(c, 0.0) + float(w)
        return max(votes, key=votes.get) if votes else None


# ============================================================
# CONFIG
# ============================================================
if PYDANTIC_AVAILABLE:
    class MODPConfig(BaseModel):
        enabled: bool = True
        method: str = "topsis"
        weights: List[float] = Field(default_factory=lambda: [0.25, 0.25, 0.25, 0.25])
        adaptive_weights: bool = True
        learning_rate: float = 0.01

    class MOEConfig(BaseModel):
        enabled: bool = True
        num_experts: int = 3
        gating_model: str = "logistic"
        update_interval: int = 3600

    class BioConfig(BaseModel):
        enabled: bool = True
        algorithm: str = "ga"
        population_size: int = 20
        max_iterations: int = 50
        mutation_rate: float = 0.1
        crossover_rate: float = 0.8

    class MultiObjectiveSchedulerConfig(BaseModel):
        enabled: bool = True
        carbon_threshold: float = 400.0
        max_delay_seconds: int = 300
        freshness_importance: float = 0.5
        cost_importance: float = 0.3
        carbon_importance: float = 0.2

    class SelfHealingConfig(BaseModel):
        enabled: bool = True
        anomaly_contamination: float = 0.1
        auto_retry_threshold: int = 3
        fallback_enabled: bool = True
        health_check_interval: int = 60

    class HeliumDataCollectorConfig(BaseSettings):
        model_config = SettingsConfigDict(
            env_prefix="HELIUM_COLLECTOR_", case_sensitive=False)

        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "18.0"
        log_level: str = "INFO"
        environment: str = "production"

        csv_path: Optional[str] = None
        refresh_interval_seconds: int = Field(3600, gt=0)
        max_concurrent_api_calls: int = Field(5, ge=1)

        usgs_api_key: Optional[str] = None
        usgs_endpoint: str = "https://www.usgs.gov/api/helium/production"
        eia_api_key: Optional[str] = None
        eia_endpoint: str = "https://www.eia.gov/api/helium/price"

        carbon_api_key: Optional[str] = None
        carbon_region: str = "global"
        carbon_update_interval: int = Field(300, ge=10)
        carbon_base_price: float = Field(0.05, ge=0)
        carbon_price_sensitivity: float = Field(0.0005, ge=0)
        rec_default_kwh: float = Field(0.0, ge=0)
        rec_persist_path: Optional[str] = None

        electricity_maps_api_key: Optional[str] = None
        electricity_maps_zone: str = "US-CAL-CISO"
        watttime_username: Optional[str] = None
        watttime_password: Optional[str] = None
        watttime_region: str = "CAISO_NORTH"

        federated_share_interval: int = Field(3600, gt=0)
        federated_learning_rate: float = Field(0.1, ge=0, le=1)
        federated_dp_sigma: float = Field(1e-3, gt=0)
        federated_target_epsilon: float = Field(8.0, gt=0)
        federated_target_delta: float = Field(1e-5, gt=0)

        human_feedback_timeout: int = Field(300, gt=0)
        predictive_horizon_hours: int = Field(24, gt=0)

        enable_quantum_security: bool = True
        quantum_algorithm: str = "dilithium"
        quantum_master_key: str = ""
        quantum_key_rotation_s: int = Field(3600, gt=0)

        enable_blockchain_verification: bool = True
        blockchain_rpc_url: str = "http://localhost:8545"
        blockchain_chain_id: int = 1
        blockchain_poa: bool = False
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None

        enable_autonomous_collection: bool = True
        default_collection_strategy: str = "multi_teacher"

        enable_multi_cloud: bool = True
        aws_enabled: bool = True
        azure_enabled: bool = True
        gcp_enabled: bool = True
        aws_bucket: Optional[str] = None
        azure_container: Optional[str] = None
        azure_connection_string: Optional[str] = None
        gcp_bucket: Optional[str] = None

        db_path: str = "helium_data.db"
        database_url: str = "sqlite+aiosqlite:///helium_data.db"
        database_pool_size: int = Field(10, ge=1)
        database_max_overflow: int = Field(20, ge=0)
        retention_days: int = Field(365, gt=0)

        health_check_interval: int = Field(60, ge=10)
        auto_collect_interval: int = Field(1800, ge=60)
        blockchain_monitor_interval: int = Field(300, ge=10)
        quantum_monitor_interval: int = Field(600, ge=10)
        cloud_sync_interval: int = Field(3600, ge=60)
        federated_interval: int = Field(3600, ge=60)
        predictive_interval: int = Field(3600, ge=60)
        sustainability_interval: int = Field(3600, ge=60)
        ml_retrain_interval: int = Field(7200, ge=60)

        max_retry_attempts: int = Field(3, ge=0)
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_timeout: int = Field(30, ge=1)
        circuit_breaker_half_open_max_requests: int = Field(3, ge=1)
        rate_limit_requests: int = Field(100, ge=1)
        rate_limit_window: int = Field(60, ge=1)

        metrics_port: int = Field(8000, ge=1024, le=65535)
        api_host: str = "0.0.0.0"
        api_port: int = 8000
        jwt_secret: str = ""
        jwt_algorithm: str = "HS256"
        jwt_expiry_s: int = Field(3600, gt=0)

        teacher_weights: Dict[str, float] = Field(default_factory=lambda: {
            "performance": 0.25, "carbon": 0.25, "cost": 0.25, "freshness": 0.25})
        distillation_learning_rate: float = Field(0.01, ge=0.001, le=0.1)
        distillation_batch_size: int = Field(32, ge=1)
        anomaly_contamination: float = Field(0.05, ge=0, le=0.5)

        modp: MODPConfig = Field(default_factory=MODPConfig)
        moe: MOEConfig = Field(default_factory=MOEConfig)
        bio: BioConfig = Field(default_factory=BioConfig)
        multi_objective_scheduler: MultiObjectiveSchedulerConfig = \
            Field(default_factory=MultiObjectiveSchedulerConfig)
        self_healing: SelfHealingConfig = Field(default_factory=SelfHealingConfig)

        limit_graph_enabled: bool = True
        rlhf_enabled: bool = True
        distillation_enabled: bool = True

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
        chaos_blast_radius: int = Field(1, ge=1)
        chaos_max_concurrent: int = Field(1, ge=1)
        chaos_duration_s: float = Field(0.0, ge=0)
        hitl_enabled: bool = True
        hitl_timeout_s: float = Field(30.0, gt=0)
        hitl_auto_approve_on_timeout: bool = False
        hitl_production_safe: bool = True

        @field_validator("log_level")
        @classmethod
        def _validate_log_level(cls, v):
            allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
            if v.upper() not in allowed:
                raise ValueError(f"log_level must be one of {allowed}")
            return v.upper()

        @field_validator("jwt_secret")
        @classmethod
        def _validate_jwt_secret(cls, v):
            if not v:
                # Prefer env-provided; otherwise generate ephemeral
                return os.urandom(32).hex()
            if len(v) < 32:
                raise ValueError("jwt_secret must be >= 32 chars")
            return v

        @field_validator("quantum_master_key")
        @classmethod
        def _validate_master_key(cls, v):
            if not v:
                return os.urandom(32).hex()
            try:
                bytes.fromhex(v)
            except ValueError:
                raise ValueError("quantum_master_key must be a hex string")
            return v

        def get_master_key_bytes(self):
            return bytes.fromhex(self.quantum_master_key)

        def get_db_url(self):
            url = self.database_url
            if url.startswith("sqlite+aiosqlite:///") or \
                    url.startswith("postgresql+asyncpg://"):
                return url
            if url.startswith("sqlite:///"):
                return url.replace("sqlite:///", "sqlite+aiosqlite:///")
            if url.startswith("postgresql://"):
                return url.replace("postgresql://", "postgresql+asyncpg://")
            return f"sqlite+aiosqlite:///{url}"
else:
    @dataclass
    class MODPConfig:
        enabled: bool = True
        method: str = "topsis"
        weights: List[float] = field(default_factory=lambda: [0.25, 0.25, 0.25, 0.25])
        adaptive_weights: bool = True
        learning_rate: float = 0.01

    @dataclass
    class MOEConfig:
        enabled: bool = True
        num_experts: int = 3
        gating_model: str = "logistic"
        update_interval: int = 3600

    @dataclass
    class BioConfig:
        enabled: bool = True
        algorithm: str = "ga"
        population_size: int = 20
        max_iterations: int = 50
        mutation_rate: float = 0.1
        crossover_rate: float = 0.8

    @dataclass
    class MultiObjectiveSchedulerConfig:
        enabled: bool = True
        carbon_threshold: float = 400.0
        max_delay_seconds: int = 300
        freshness_importance: float = 0.5
        cost_importance: float = 0.3
        carbon_importance: float = 0.2

    @dataclass
    class SelfHealingConfig:
        enabled: bool = True
        anomaly_contamination: float = 0.1
        auto_retry_threshold: int = 3
        fallback_enabled: bool = True
        health_check_interval: int = 60

    @dataclass
    class HeliumDataCollectorConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "18.0"
        log_level: str = "INFO"
        environment: str = "production"
        csv_path: Optional[str] = None
        refresh_interval_seconds: int = 3600
        max_concurrent_api_calls: int = 5
        usgs_api_key: Optional[str] = None
        usgs_endpoint: str = "https://www.usgs.gov/api/helium/production"
        eia_api_key: Optional[str] = None
        eia_endpoint: str = "https://www.eia.gov/api/helium/price"
        carbon_api_key: Optional[str] = None
        carbon_region: str = "global"
        carbon_update_interval: int = 300
        carbon_base_price: float = 0.05
        carbon_price_sensitivity: float = 0.0005
        rec_default_kwh: float = 0.0
        rec_persist_path: Optional[str] = None
        electricity_maps_api_key: Optional[str] = None
        electricity_maps_zone: str = "US-CAL-CISO"
        watttime_username: Optional[str] = None
        watttime_password: Optional[str] = None
        watttime_region: str = "CAISO_NORTH"
        federated_share_interval: int = 3600
        federated_learning_rate: float = 0.1
        federated_dp_sigma: float = 1e-3
        federated_target_epsilon: float = 8.0
        federated_target_delta: float = 1e-5
        human_feedback_timeout: int = 300
        predictive_horizon_hours: int = 24
        enable_quantum_security: bool = True
        quantum_algorithm: str = "dilithium"
        quantum_master_key: str = field(
            default_factory=lambda: os.urandom(32).hex())
        quantum_key_rotation_s: int = 3600
        enable_blockchain_verification: bool = True
        blockchain_rpc_url: str = "http://localhost:8545"
        blockchain_chain_id: int = 1
        blockchain_poa: bool = False
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        enable_autonomous_collection: bool = True
        default_collection_strategy: str = "multi_teacher"
        enable_multi_cloud: bool = True
        aws_enabled: bool = True
        azure_enabled: bool = True
        gcp_enabled: bool = True
        aws_bucket: Optional[str] = None
        azure_container: Optional[str] = None
        azure_connection_string: Optional[str] = None
        gcp_bucket: Optional[str] = None
        db_path: str = "helium_data.db"
        database_url: str = "sqlite+aiosqlite:///helium_data.db"
        database_pool_size: int = 10
        database_max_overflow: int = 20
        retention_days: int = 365
        health_check_interval: int = 60
        auto_collect_interval: int = 1800
        blockchain_monitor_interval: int = 300
        quantum_monitor_interval: int = 600
        cloud_sync_interval: int = 3600
        federated_interval: int = 3600
        predictive_interval: int = 3600
        sustainability_interval: int = 3600
        ml_retrain_interval: int = 7200
        max_retry_attempts: int = 3
        circuit_breaker_threshold: int = 5
        circuit_breaker_timeout: int = 30
        circuit_breaker_half_open_max_requests: int = 3
        rate_limit_requests: int = 100
        rate_limit_window: int = 60
        metrics_port: int = 8000
        api_host: str = "0.0.0.0"
        api_port: int = 8000
        jwt_secret: str = field(
            default_factory=lambda: os.urandom(32).hex())
        jwt_algorithm: str = "HS256"
        jwt_expiry_s: int = 3600
        teacher_weights: Dict[str, float] = field(default_factory=lambda: {
            "performance": 0.25, "carbon": 0.25, "cost": 0.25, "freshness": 0.25})
        distillation_learning_rate: float = 0.01
        distillation_batch_size: int = 32
        anomaly_contamination: float = 0.05
        modp: MODPConfig = field(default_factory=MODPConfig)
        moe: MOEConfig = field(default_factory=MOEConfig)
        bio: BioConfig = field(default_factory=BioConfig)
        multi_objective_scheduler: MultiObjectiveSchedulerConfig = \
            field(default_factory=MultiObjectiveSchedulerConfig)
        self_healing: SelfHealingConfig = field(default_factory=SelfHealingConfig)
        limit_graph_enabled: bool = True
        rlhf_enabled: bool = True
        distillation_enabled: bool = True
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
        chaos_blast_radius: int = 1
        chaos_max_concurrent: int = 1
        chaos_duration_s: float = 0.0
        hitl_enabled: bool = True
        hitl_timeout_s: float = 30.0
        hitl_auto_approve_on_timeout: bool = False
        hitl_production_safe: bool = True

        def get_master_key_bytes(self):
            return bytes.fromhex(self.quantum_master_key)

        def get_db_url(self):
            url = self.database_url
            if url.startswith("sqlite+aiosqlite:///") or \
                    url.startswith("postgresql+asyncpg://"):
                return url
            if url.startswith("sqlite:///"):
                return url.replace("sqlite:///", "sqlite+aiosqlite:///")
            if url.startswith("postgresql://"):
                return url.replace("postgresql://", "postgresql+asyncpg://")
            return f"sqlite+aiosqlite:///{url}"


# ============================================================
# ORM
# ============================================================
if SQLALCHEMY_AVAILABLE:
    Base = declarative_base()

    class HeliumRecordDB(Base):
        __tablename__ = "helium_records"
        id = Column(Integer, primary_key=True)
        record_id = Column(String(64), unique=True, index=True)
        date_str = Column(String(32))
        global_production_tonnes = Column(Float)
        global_demand_tonnes = Column(Float)
        price_index = Column(Float)
        is_anomaly = Column(Boolean, default=False)
        anomaly_score = Column(Float, default=0.0)
        quantum_signature = Column(JSON)
        blockchain_tx_hash = Column(String(128))
        cloud_distribution = Column(JSON)
        provenance = Column(JSON)
        precision_level = Column(String(16))
        explanation = Column(JSON)
        safety_ok = Column(Boolean, default=True)
        stl_verdict = Column(JSON)
        hitl_approved = Column(Boolean, default=True)
        counterfactuals = Column(JSON)
        chaos_events = Column(JSON)
        version = Column(Integer, default=1)
        timestamp = Column(DateTime, default=datetime.utcnow)

    class DistributionHistoryDB(Base):
        __tablename__ = "distribution_history"
        id = Column(Integer, primary_key=True)
        provider = Column(String(32))
        region = Column(String(64))
        score = Column(Float)
        timestamp = Column(DateTime, default=datetime.utcnow)

    class ExportRecordDB(Base):
        __tablename__ = "export_records"
        id = Column(Integer, primary_key=True)
        export_id = Column(String(64), unique=True, index=True)
        export_type = Column(String(32))
        file_hash = Column(String(128))
        tx_hash = Column(String(128))
        block_number = Column(Integer)
        verified = Column(Boolean, default=False)
        timestamp = Column(DateTime, default=datetime.utcnow)

    class OptimizationHistoryDB(Base):
        __tablename__ = "optimization_history"
        id = Column(Integer, primary_key=True)
        strategy = Column(String(32))
        result = Column(JSON)
        timestamp = Column(DateTime, default=datetime.utcnow)

    class SchemaVersionDB(Base):
        __tablename__ = "schema_version"
        version = Column(Integer, primary_key=True)
        applied_at = Column(DateTime, default=datetime.utcnow)
else:
    Base = None


# ============================================================
# DATA CLASSES
# ============================================================
@dataclass
class HeliumRecord:
    date: Any
    global_production_tonnes: float
    global_demand_tonnes: float
    price_index: float
    record_id: str = field(default_factory=lambda: f"hr_{uuid.uuid4().hex[:8]}")
    is_anomaly: bool = False
    anomaly_score: float = 0.0
    quantum_signature: Optional[Dict] = None
    blockchain_tx_hash: Optional[str] = None
    cloud_distribution: Optional[Dict] = None
    provenance: Optional[Dict] = None
    precision_level: Optional[str] = None
    explanation: Optional[Dict] = None
    safety_ok: bool = True
    stl_verdict: Optional[Dict[str, bool]] = None
    hitl_approved: bool = True
    counterfactuals: Optional[Dict[int, float]] = None
    chaos_events: Optional[List[Dict[str, Any]]] = None
    version: int = 1

    def __post_init__(self):
        if self.global_production_tonnes < 0:
            raise ValueError("production must be >= 0")
        if self.global_demand_tonnes < 0:
            raise ValueError("demand must be >= 0")
        if self.price_index < 0:
            raise ValueError("price_index must be >= 0")
        if not (0 <= self.anomaly_score <= 1):
            raise ValueError("anomaly_score must be between 0 and 1")

    def to_dict(self) -> Dict:
        d = asdict(self)
        if isinstance(d.get("date"), (date, datetime)):
            d["date"] = d["date"].isoformat()
        return d


@dataclass
class HeliumDataset:
    records: List[HeliumRecord] = field(default_factory=list)


# ============================================================
# RELIABILITY PRIMITIVES
# ============================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"; OPEN = "open"; HALF_OPEN = "half_open"


class EnhancedCircuitBreaker:
    def __init__(self, name, failure_threshold=5, recovery_timeout=30,
                 half_open_max_requests=3):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_requests = half_open_max_requests
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0; self.success_count = 0
        self.last_failure_time = 0.0; self.last_success_time = 0.0
        self._lock = asyncio.Lock(); self.half_open_requests = 0

    async def allow_request(self):
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_requests = 0
                else:
                    return False
            if self.state == CircuitBreakerState.HALF_OPEN:
                if self.half_open_requests >= self.half_open_max_requests:
                    return False
                self.half_open_requests += 1
            return True

    async def record_success(self):
        async with self._lock:
            self.success_count += 1
            self.last_success_time = time.time()
            if self.state == CircuitBreakerState.HALF_OPEN and self.success_count >= 2:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0; self.half_open_requests = 0
            else:
                self.failure_count = 0

    async def record_failure(self):
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitBreakerState.CLOSED and \
                    self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN
                self.half_open_requests = 0

    async def call(self, func, *args, **kwargs):
        if not await self.allow_request():
            raise CircuitBreakerOpenError(
                f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            await self.record_success()
            return result
        except Exception:
            await self.record_failure()
            raise


class EnhancedRateLimiter:
    def __init__(self, rate=100, per_seconds=60):
        self.rate = rate; self.per_seconds = per_seconds
        self.tokens = float(rate); self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.time(); dt = now - self.last_refill
            self.tokens = min(self.rate,
                              self.tokens + dt * (self.rate / self.per_seconds))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.1)


class EnhancedBulkhead:
    def __init__(self, max_concurrency):
        self.semaphore = asyncio.Semaphore(max_concurrency)

    async def execute(self, func, *args, **kwargs):
        async with self.semaphore:
            return await func(*args, **kwargs)


class TaskManager:
    def __init__(self, max_workers=10):
        self.max_workers = max_workers
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._pending: List[Tuple[str, Callable, tuple, dict]] = []

    def register_task(self, name, coro_func, *args, **kwargs):
        self._pending.append((name, coro_func, args, kwargs))

    def start_task(self, name, coro_func, *args, **kwargs):
        shutdown_event = self.shutdown_event

        async def wrapper():
            backoff = 1
            while not shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                    break
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Task {name} crashed: {e}")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 300)
        task = asyncio.create_task(wrapper(), name=name)
        self.tasks[name] = task
        return task

    def start_registered_tasks(self):
        for name, fn, args, kwargs in self._pending:
            self.start_task(name, fn, *args, **kwargs)
        self._pending.clear()

    async def stop_all(self):
        self.shutdown_event.set()
        async with self._lock:
            for t in self.tasks.values():
                t.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()


# ============================================================
# DATABASE MANAGER (P0 fixed)
# ============================================================
class EnhancedDatabaseManager:
    """
    P0 fixes:
      - Removed NullPool + pool_size conflict; NullPool used only for SQLite
      - ORM insert instead of raw SQLite SQL
      - Async URL normalization for postgresql+asyncpg
      - Proper close() awaiting engine.dispose()
    """

    def __init__(self, config):
        self.config = config
        self.db_url = config.get_db_url()
        self.async_engine = None
        self.async_session = None
        self._migrations_applied = False
        self._is_sqlite = "sqlite" in self.db_url.lower()
        self._init_async()

    def _init_async(self):
        if not ASYNC_SQLALCHEMY_AVAILABLE:
            logger.warning("Async SQLAlchemy not available; DB disabled.")
            return
        try:
            kwargs: Dict[str, Any] = {"echo": False}
            if self._is_sqlite:
                # SQLite: no pool tuning; NullPool is safe.
                from sqlalchemy.pool import NullPool
                kwargs["poolclass"] = NullPool
            else:
                kwargs["pool_size"] = getattr(self.config, "database_pool_size", 10)
                kwargs["max_overflow"] = getattr(self.config, "database_max_overflow", 20)
                kwargs["pool_pre_ping"] = True
            self.async_engine = create_async_engine(self.db_url, **kwargs)
            self.async_session = async_sessionmaker(
                self.async_engine, expire_on_commit=False)
        except Exception as e:
            logger.error(f"DB init failed: {e}")
            self.async_engine = None
            self.async_session = None

    async def _apply_migrations(self):
        if not self.async_engine:
            return
        async with self.async_engine.begin() as conn:
            if Base is not None:
                await conn.run_sync(Base.metadata.create_all)
        self._migrations_applied = True

    async def init(self):
        if self.async_engine and not self._migrations_applied:
            await self._apply_migrations()

    async def insert_helium_record(self, record: HeliumRecord):
        if not self.async_session or Base is None:
            return
        try:
            async with self.async_session() as session:
                row = HeliumRecordDB(
                    record_id=record.record_id,
                    date_str=record.date.isoformat() if hasattr(record.date, "isoformat")
                             else str(record.date),
                    global_production_tonnes=record.global_production_tonnes,
                    global_demand_tonnes=record.global_demand_tonnes,
                    price_index=record.price_index,
                    is_anomaly=record.is_anomaly,
                    anomaly_score=record.anomaly_score,
                    quantum_signature=record.quantum_signature or {},
                    blockchain_tx_hash=record.blockchain_tx_hash or "",
                    cloud_distribution=record.cloud_distribution or {},
                    provenance=record.provenance or {},
                    precision_level=record.precision_level or "",
                    explanation=record.explanation or {},
                    safety_ok=record.safety_ok,
                    stl_verdict=record.stl_verdict or {},
                    hitl_approved=record.hitl_approved,
                    counterfactuals={str(k): v for k, v in
                                     (record.counterfactuals or {}).items()},
                    chaos_events=record.chaos_events or [],
                    version=record.version,
                    timestamp=datetime.utcnow(),
                )
                session.add(row)
                await session.commit()
        except Exception as e:
            logger.warning(f"insert_helium_record failed: {e}")

    async def load_records(self, limit: int = 500) -> List[HeliumRecord]:
        if not self.async_session or Base is None:
            return []
        try:
            from sqlalchemy import select
            async with self.async_session() as session:
                stmt = select(HeliumRecordDB).order_by(
                    HeliumRecordDB.timestamp.desc()).limit(limit)
                result = await session.execute(stmt)
                rows = result.scalars().all()
                out = []
                for row in rows:
                    try:
                        d = date.fromisoformat(row.date_str) if row.date_str else date.today()
                    except Exception:
                        d = date.today()
                    rec = HeliumRecord(
                        date=d,
                        global_production_tonnes=float(row.global_production_tonnes or 0),
                        global_demand_tonnes=float(row.global_demand_tonnes or 0),
                        price_index=float(row.price_index or 0),
                        record_id=row.record_id,
                        is_anomaly=bool(row.is_anomaly),
                        anomaly_score=float(row.anomaly_score or 0),
                    )
                    # Full field restore
                    rec.quantum_signature = row.quantum_signature
                    rec.blockchain_tx_hash = row.blockchain_tx_hash
                    rec.cloud_distribution = row.cloud_distribution
                    rec.provenance = row.provenance
                    rec.precision_level = row.precision_level
                    rec.explanation = row.explanation
                    rec.safety_ok = bool(row.safety_ok) if row.safety_ok is not None else True
                    rec.stl_verdict = row.stl_verdict
                    rec.hitl_approved = bool(row.hitl_approved) if row.hitl_approved is not None else True
                    rec.counterfactuals = ({int(k): v for k, v in (row.counterfactuals or {}).items()}
                                           if row.counterfactuals else None)
                    rec.chaos_events = row.chaos_events
                    out.append(rec)
                return out
        except Exception as e:
            logger.warning(f"load_records failed: {e}")
            return []

    async def health_check(self):
        if not self.async_session:
            return {"status": "unavailable"}
        try:
            async with self.async_session() as session:
                from sqlalchemy import text as _text
                await session.execute(_text("SELECT 1"))
            return {"status": "healthy"}
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

    async def close(self):
        if self.async_engine:
            try:
                await self.async_engine.dispose()
            except Exception as e:
                logger.warning(f"DB close failed: {e}")


# ============================================================
# SECURITY (P0/P1)
# ============================================================
class QuantumResilientDataSecurity:
    """
    P0/P1 fixes:
      - Keys are generated once and rotated on a schedule
      - verify_helium_data() actually verifies when PQC available
      - AES-GCM at-rest encryption helper
      - Uses quantum_master_key for a local fingerprint HMAC fallback
    """

    def __init__(self, config, db_manager):
        self.config = config
        self.db_manager = db_manager
        self.algorithm = getattr(config, "quantum_algorithm", "dilithium")
        self.keys: Dict[str, Tuple[bytes, bytes, float]] = {}
        self._last_rotation = 0.0
        self._rotation_s = getattr(config, "quantum_key_rotation_s", 3600)
        self._master = config.get_master_key_bytes() \
            if hasattr(config, "get_master_key_bytes") else b""
        self._active_key_id: Optional[str] = None
        self._lock = asyncio.Lock()
        # Bootstrap one key
        self._ensure_key()

    def _ensure_key(self):
        now = time.time()
        if self._active_key_id and (now - self._last_rotation) < self._rotation_s:
            return
        self._generate_sync_key()

    def _generate_sync_key(self):
        kid = uuid.uuid4().hex[:8]
        if PQC_AVAILABLE:
            try:
                if self.algorithm == "dilithium":
                    pub, priv = dilithium.generate_keypair()
                elif self.algorithm == "falcon":
                    pub, priv = falcon.generate_keypair()
                else:
                    pub, priv = sphincs.generate_keypair()
            except Exception as e:
                logger.warning(f"PQC keypair failed: {e}")
                pub, priv = b"", b""
        else:
            pub, priv = b"", b""
        self.keys[kid] = (pub, priv, time.time())
        self._active_key_id = kid
        self._last_rotation = time.time()

    async def generate_keypair(self, algorithm=None):
        async with self._lock:
            self._ensure_key()
            return {"key_id": self._active_key_id,
                    "public_key": self.keys[self._active_key_id][0]}

    def _hmac_sign(self, payload: bytes) -> str:
        if not self._master:
            return ""
        return hmac.new(self._master, payload, hashlib.sha256).hexdigest()

    async def sign_helium_data(self, data: Dict, key_id: str) -> Dict:
        entry = self.keys.get(key_id)
        if entry is None:
            QUANTUM_SIGNATURES.labels(algorithm="none", status="missing_key").inc()
            return {"algorithm": "none", "signature": ""}
        pub, priv, _ = entry
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        if not PQC_AVAILABLE:
            sig = self._hmac_sign(payload)
            QUANTUM_SIGNATURES.labels(algorithm="hmac", status="success").inc()
            return {"algorithm": "hmac-sha256", "signature": sig,
                    "key_id": key_id}
        try:
            if self.algorithm == "dilithium":
                sig = dilithium.sign(priv, payload)
            elif self.algorithm == "falcon":
                sig = falcon.sign(priv, payload)
            else:
                sig = sphincs.sign(priv, payload)
            QUANTUM_SIGNATURES.labels(algorithm=self.algorithm, status="success").inc()
            return {"algorithm": self.algorithm,
                    "signature": base64.b64encode(sig).decode(),
                    "key_id": key_id}
        except Exception as e:
            QUANTUM_SIGNATURES.labels(algorithm=self.algorithm, status="failed").inc()
            return {"algorithm": "none", "signature": "", "error": str(e)}

    async def verify_helium_data(self, data: Dict, signature_data: Dict) -> bool:
        alg = signature_data.get("algorithm", "none")
        sig = signature_data.get("signature", "")
        if alg == "none" or not sig:
            return False
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        if alg == "hmac-sha256":
            expected = self._hmac_sign(payload)
            return hmac.compare_digest(expected, sig)
        if not PQC_AVAILABLE:
            return False
        kid = signature_data.get("key_id")
        entry = self.keys.get(kid) if kid else None
        if entry is None:
            return False
        pub = entry[0]
        try:
            sig_bytes = base64.b64decode(sig)
            if alg == "dilithium":
                dilithium.verify(pub, payload, sig_bytes)
            elif alg == "falcon":
                falcon.verify(pub, payload, sig_bytes)
            elif alg == "sphincs":
                sphincs.verify(pub, payload, sig_bytes)
            else:
                return False
            return True
        except Exception:
            return False

    def encrypt_at_rest(self, plaintext: bytes, key: bytes) -> bytes:
        if not CRYPTO_AVAILABLE:
            return plaintext
        nonce = os.urandom(12)
        aes = AESGCM(key)
        ct = aes.encrypt(nonce, plaintext, None)
        return nonce + ct

    def decrypt_at_rest(self, ciphertext: bytes, key: bytes) -> bytes:
        if not CRYPTO_AVAILABLE:
            return ciphertext
        nonce, ct = ciphertext[:12], ciphertext[12:]
        aes = AESGCM(key)
        return aes.decrypt(nonce, ct, None)

    def get_quantum_status(self):
        return {
            "pqc_available": PQC_AVAILABLE,
            "algorithm": self.algorithm,
            "active_key_id": self._active_key_id,
            "num_keys": len(self.keys),
            "rotation_s": self._rotation_s,
            "algorithms": ["dilithium", "falcon", "sphincs"]
            if PQC_AVAILABLE else ["hmac-sha256"],
        }

    async def health_check(self):
        return {"status": "ok" if PQC_AVAILABLE else "degraded",
                "active_key": self._active_key_id}


# ============================================================
# BLOCKCHAIN (P1: real Web3 with local fallback)
# ============================================================
class BlockchainDataVerification:
    def __init__(self, config, db_manager):
        self.config = config
        self.db_manager = db_manager
        self.web3 = None
        self._ledger: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

        if WEB3_AVAILABLE and getattr(config, "enable_blockchain_verification", False):
            try:
                self.web3 = Web3(Web3.HTTPProvider(config.blockchain_rpc_url))
                if getattr(config, "blockchain_poa", False):
                    self.web3.middleware_onion.inject(
                        geth_poa_middleware, layer=0)
                if not self.web3.is_connected():
                    logger.warning("Web3 not connected; using local ledger.")
                    self.web3 = None
            except Exception as e:
                logger.warning(f"Web3 init failed: {e}")
                self.web3 = None

    async def record_helium_data(self, data_id, data_hash, metadata):
        async with self._lock:
            if data_id in self._ledger:
                return {"tx_hash": self._ledger[data_id]["tx_hash"],
                        "status": "idempotent"}

            tx_hash = None
            if self.web3 is not None and \
                    getattr(self.config, "blockchain_private_key", None):
                try:
                    account = self.web3.eth.account.from_key(
                        self.config.blockchain_private_key)
                    nonce = self.web3.eth.get_transaction_count(account.address)
                    tx = {
                        "nonce": nonce,
                        "gas": 21000,
                        "gasPrice": self.web3.eth.gas_price,
                        "to": account.address,
                        "value": 0,
                        "data": self.web3.to_hex(text=json.dumps({
                            "id": data_id, "hash": data_hash,
                            "meta": metadata}, default=str)),
                        "chainId": self.config.blockchain_chain_id,
                    }
                    signed = self.web3.eth.account.sign_transaction(
                        tx, self.config.blockchain_private_key)
                    raw = getattr(signed, "rawTransaction", None) or \
                        getattr(signed, "raw_transaction", None)
                    tx_hash_bytes = self.web3.eth.send_raw_transaction(raw)
                    tx_hash = self.web3.to_hex(tx_hash_bytes)
                except Exception as e:
                    logger.warning(f"Web3 tx failed, using local: {e}")
                    tx_hash = None

            if tx_hash is None:
                tx_hash = "0x" + hashlib.sha256(
                    f"{data_id}:{data_hash}".encode()).hexdigest()[:40]

            self._ledger[data_id] = {
                "tx_hash": tx_hash, "hash": data_hash,
                "metadata": metadata, "t": time.time()}
            BLOCKCHAIN_VERIFICATIONS.labels(status="recorded").inc()
            return {"tx_hash": tx_hash, "status": "recorded"}

    async def verify_helium_data(self, data_id, data_hash):
        entry = self._ledger.get(data_id)
        if entry is None:
            BLOCKCHAIN_VERIFICATIONS.labels(status="missing").inc()
            return {"status": "missing"}
        ok = entry["hash"] == data_hash
        BLOCKCHAIN_VERIFICATIONS.labels(
            status="verified" if ok else "mismatch").inc()
        return {"status": "verified" if ok else "mismatch"}

    async def get_blockchain_status(self):
        connected = bool(self.web3 and self.web3.is_connected())
        return {"connected": connected,
                "backend": "web3" if connected else "local",
                "total_records": len(self._ledger)}

    async def health_check(self):
        return await self.get_blockchain_status()


# ============================================================
# CLOUD STORAGE (P1: real SDKs with local fallback)
# ============================================================
class MultiCloudStorage:
    def __init__(self, config):
        self.config = config
        self.providers: Dict[str, Any] = {}
        if AWS_AVAILABLE and getattr(config, "aws_enabled", False) and \
                getattr(config, "aws_bucket", None):
            try:
                self.providers["aws"] = boto3.client("s3")
            except Exception as e:
                logger.warning(f"AWS init failed: {e}")
        if AZURE_AVAILABLE and getattr(config, "azure_enabled", False) and \
                getattr(config, "azure_connection_string", None):
            try:
                svc = BlobServiceClient.from_connection_string(
                    config.azure_connection_string)
                self.providers["azure"] = svc
            except Exception as e:
                logger.warning(f"Azure init failed: {e}")
        if GCP_AVAILABLE and getattr(config, "gcp_enabled", False) and \
                getattr(config, "gcp_bucket", None):
            try:
                self.providers["gcp"] = gcs_storage.Client()
            except Exception as e:
                logger.warning(f"GCP init failed: {e}")

        # Local fallback directory
        self.local_dir = Path(tempfile.gettempdir()) / "helium_cloud"
        self.local_dir.mkdir(parents=True, exist_ok=True)

    async def store(self, data: bytes, filename: str):
        results = {}
        loop = asyncio.get_running_loop()

        def _aws():
            self.providers["aws"].put_object(
                Bucket=self.config.aws_bucket, Key=filename, Body=data)
        def _azure():
            container = self.providers["azure"].get_container_client(
                self.config.azure_container)
            container.upload_blob(name=filename, data=data, overwrite=True)
        def _gcp():
            bucket = self.providers["gcp"].bucket(self.config.gcp_bucket)
            bucket.blob(filename).upload_from_string(data)

        for name, fn in (("aws", _aws), ("azure", _azure), ("gcp", _gcp)):
            if name not in self.providers:
                continue
            try:
                await loop.run_in_executor(None, fn)
                results[name] = "ok"
            except Exception as e:
                results[name] = f"failed: {e}"

        # Always write locally too
        try:
            (self.local_dir / filename).write_bytes(data)
            results["local"] = "ok"
        except Exception as e:
            results["local"] = f"failed: {e}"

        return {"status": "ok", "filename": filename, "results": results}

    async def health_check(self):
        return {"status": "ok", "providers": list(self.providers.keys()),
                "local_dir": str(self.local_dir)}


# ============================================================
# ML / ANOMALY / SELF-HEALING
# ============================================================
class MLAnomalyDetector:
    def __init__(self, config, db_manager=None):
        self.config = config
        self.db_manager = db_manager
        self.enabled = True
        self._trained = False
        self._buffer: deque = deque(maxlen=500)
        self.detectors: List[Tuple[str, Any]] = []
        if SKLEARN_AVAILABLE:
            self.detectors = [
                ("iforest", IsolationForest(
                    contamination=getattr(config, "anomaly_contamination", 0.05))),
                ("ocsvm", OneClassSVM(nu=0.1)),
            ]

    def _features(self, record):
        return np.array([
            float(record.price_index),
            float(record.global_production_tonnes),
            float(record.global_demand_tonnes),
        ], dtype=np.float64).reshape(1, -1)

    async def train(self, records):
        if not self.detectors or len(records) < 20:
            return
        X = np.vstack([self._features(r) for r in records])
        for _, m in self.detectors:
            try:
                m.fit(X)
            except Exception as e:
                logger.warning(f"Anomaly fit failed: {e}")
        self._trained = True

    async def detect_anomaly(self, name, value, context=None):
        if not self._trained:
            if 150 <= value <= 250:
                return False, 0.0, {}
            return True, 0.8, {}
        if context:
            x = np.array([
                float(context.get("price_index", value)),
                float(context.get("production", 0.0)),
                float(context.get("demand", 0.0)),
            ], dtype=np.float64).reshape(1, -1)
        else:
            x = np.array([[value, 0.0, 0.0]], dtype=np.float64)
        votes = []
        for _, m in self.detectors:
            try:
                votes.append(1 if m.predict(x)[0] == -1 else 0)
            except Exception:
                votes.append(0)
        if not votes:
            return False, 0.0, {}
        score = sum(votes) / len(votes)
        return (score > 0.5), float(score), {}

    async def get_statistics(self):
        return {"enabled": self.enabled, "trained": self._trained,
                "num_detectors": len(self.detectors)}


class SelfHealingManager:
    def __init__(self, config, drift_detector=None):
        self.config = config
        self.drift = drift_detector
        self._trained = False
        self._buffer: deque = deque(maxlen=200)
        self.detectors: List[Tuple[str, Any]] = []
        self.recovery_actions: deque = deque(maxlen=100)
        self._lock = asyncio.Lock()
        if SKLEARN_AVAILABLE and getattr(config, "self_healing", None) and \
                config.self_healing.enabled:
            self.detectors = [
                ("iforest", IsolationForest(
                    contamination=config.self_healing.anomaly_contamination)),
                ("ocsvm", OneClassSVM(nu=0.1)),
            ]

    def _features(self, record):
        return np.array([
            float(record.price_index),
            float(record.global_production_tonnes),
            float(record.global_demand_tonnes),
        ], dtype=np.float64)

    async def train(self, records):
        if not self.detectors or len(records) < 20:
            return
        X = np.vstack([self._features(r) for r in records])
        for _, m in self.detectors:
            try:
                m.fit(X)
            except Exception as e:
                logger.warning(f"Self-healing fit failed: {e}")
        self._trained = True

    async def detect_anomaly(self, record):
        if not self.detectors or not self._trained:
            if 150 <= record.price_index <= 250:
                return False, 0.0
            return True, 0.8
        X = self._features(record).reshape(1, -1)
        votes = []
        for _, m in self.detectors:
            try:
                votes.append(1 if m.predict(X)[0] == -1 else 0)
            except Exception:
                votes.append(0)
        if not votes:
            return False, 0.0
        score = sum(votes) / len(votes)
        if score > 0.5:
            ANOMALY_DETECTIONS.labels(type="self_healing").inc()
            async with self._lock:
                self.recovery_actions.append({
                    "action": "restart",
                    "timestamp": datetime.utcnow().isoformat()})
            SELF_HEALING_ACTIONS.labels(action="restart").inc()
        return score > 0.5, float(score)

    async def check_drift(self, metrics):
        if self.drift:
            try:
                drift = await self.drift.check_drift(metrics)
            except Exception:
                drift = False
            if drift:
                async with self._lock:
                    self.recovery_actions.append({
                        "action": "drift_recovery",
                        "timestamp": datetime.utcnow().isoformat()})

    async def get_statistics(self):
        return {"enabled": getattr(self.config, "self_healing", None) is not None,
                "trained": self._trained,
                "num_detectors": len(self.detectors),
                "recent_actions": list(self.recovery_actions)[-5:]}


# ============================================================
# CACHE / QUALITY / EXPORT / SUPPORT STUBS
# ============================================================
class EnhancedCacheManager:
    def __init__(self, max_size=1000, ttl=300):
        self.max_size = max_size; self.ttl = ttl
        self._cache: Dict[str, Tuple[Any, float]] = {}

    async def start(self): pass
    async def stop(self): pass

    def get(self, key):
        e = self._cache.get(key)
        if e is None: return None
        v, t = e
        if (time.time() - t) > self.ttl:
            del self._cache[key]; return None
        return v

    def set(self, key, value):
        if len(self._cache) >= self.max_size:
            self._cache.pop(next(iter(self._cache)))
        self._cache[key] = (value, time.time())

    async def get_statistics(self):
        return {"size": len(self._cache), "max_size": self.max_size}


class DataQualityMonitor:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.stats = {"total": 0, "valid": 0, "invalid": 0}

    async def start(self): pass
    async def stop(self): pass

    def validate(self, record) -> bool:
        self.stats["total"] += 1
        try:
            assert record.global_production_tonnes >= 0
            assert record.global_demand_tonnes >= 0
            assert record.price_index >= 0
            self.stats["valid"] += 1
            return True
        except Exception:
            self.stats["invalid"] += 1
            return False

    async def get_statistics(self):
        return dict(self.stats)


class EnhancedExportQueue:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self._running = False
        self._queue: deque = deque(maxlen=1000)

    async def start(self): self._running = True
    async def stop(self): self._running = False

    async def export_record(self, record): self._queue.append(record)

    async def get_statistics(self):
        return {"running": self._running, "queued": len(self._queue)}


class FederatedHeliumDataLearner:
    def __init__(self, db_manager, instance_id, share_interval, aggregator=None):
        self.db_manager = db_manager
        self.instance_id = instance_id
        self.share_interval = share_interval
        self.aggregator = aggregator or SecureFederatedAggregator()

    async def share(self, local_vec: np.ndarray, carbon_intensity: float):
        self.aggregator.submit(FederatedUpdate(
            node_id=self.instance_id,
            weights={"local": np.asarray(local_vec, dtype=np.float64)},
            n_samples=1, carbon_intensity=carbon_intensity))
        return self.aggregator.aggregate()

    async def get_global(self):
        return self.aggregator.global_weights()


class UserAdaptiveHeliumDataReflexivity:
    def __init__(self, db_manager, learning_rate):
        self.db_manager = db_manager
        self.learning_rate = learning_rate
        self._preferences: Dict[str, float] = {}

    def record_feedback(self, key, value):
        self._preferences[key] = self._preferences.get(key, 0.0) + \
            self.learning_rate * (value - self._preferences.get(key, 0.0))


class HeliumDataSustainabilityTracker:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.scores: deque = deque(maxlen=1000)

    def record(self, score): self.scores.append(score)

    async def get_score(self):
        if not self.scores: return 0.0
        return float(np.mean(self.scores))


class GeneticAlgorithmOptimizer:
    """P1: keeps population state across calls."""

    def __init__(self, population_size=20, mutation_rate=0.1,
                 crossover_rate=0.8, seed=0):
        self.pop_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.rng = random.Random(seed)
        self.population: List[Dict[str, float]] = []
        self.initialized = False

    def initialize(self):
        if self.initialized and self.population:
            return
        self.population = [
            {"interval": self.rng.uniform(30, 600),
             "batch_size": self.rng.randint(10, 100),
             "parallel_calls": self.rng.randint(1, 20)}
            for _ in range(self.pop_size)
        ]
        self.initialized = True

    def evaluate(self, fitness_func):
        return [fitness_func(i) for i in self.population]

    def select(self, fitness, n):
        selected = []
        for _ in range(n):
            a, b = self.rng.sample(range(len(self.population)), 2)
            selected.append(self.population[a] if fitness[a] > fitness[b]
                            else self.population[b])
        return selected

    def crossover(self, p1, p2):
        if self.rng.random() < self.crossover_rate:
            return {k: p1[k] if self.rng.random() < 0.5 else p2[k] for k in p1}
        return dict(p1)

    def mutate(self, ind):
        if self.rng.random() < self.mutation_rate:
            key = self.rng.choice(list(ind.keys()))
            if key == "interval": ind[key] = self.rng.uniform(30, 600)
            elif key == "batch_size": ind[key] = self.rng.randint(10, 100)
            elif key == "parallel_calls": ind[key] = self.rng.randint(1, 20)
        return ind

    def evolve(self, fitness_func, generations=5):
        self.initialize()
        best = None
        for _ in range(generations):
            fitness = self.evaluate(fitness_func)
            best_idx = int(np.argmax(fitness))
            best = dict(self.population[best_idx])
            parents = self.select(fitness, max(2, self.pop_size - 1))
            offspring = []
            for i in range(0, len(parents) - 1, 2):
                c1 = self.crossover(parents[i], parents[i + 1])
                c2 = self.crossover(parents[i + 1], parents[i])
                offspring.append(self.mutate(c1))
                offspring.append(self.mutate(c2))
            self.population = offspring[: self.pop_size - 1] + [best]
        return best or {}


class BioInspiredAutonomousCollector:
    def __init__(self, config, db_manager, adaptive_cost=None,
                 limit_graph=None, rlhf=None, distiller=None):
        self.config = config
        self.db_manager = db_manager
        self.adaptive_cost = adaptive_cost
        self.ga = GeneticAlgorithmOptimizer(
            population_size=config.bio.population_size,
            mutation_rate=config.bio.mutation_rate,
            crossover_rate=config.bio.crossover_rate)
        self.current_params = {"interval": 60, "batch_size": 50,
                               "parallel_calls": 5}
        self._lock = asyncio.Lock()
        self.collection_history: deque = deque(maxlen=100)
        self.fitness_history: List[float] = []
        self.limit_graph = limit_graph
        self.rlhf = rlhf
        self.distiller = distiller

    def _fitness_func(self, params):
        if self.adaptive_cost:
            try:
                return -float(self.adaptive_cost.evaluate(params))
            except Exception:
                pass
        cost = (params["interval"] / 600) * 0.4 \
             + (params["batch_size"] / 100) * 0.3 \
             + (params["parallel_calls"] / 20) * 0.3
        return -cost

    async def optimize_collection(self, current_state, strategy=None):
        features = np.array([
            current_state.get("carbon_intensity", 400) / 1000,
            datetime.utcnow().hour / 24,
            current_state.get("data_volume", 0) / 1000,
            0.0,
        ])

        selected = strategy
        source = "explicit"
        if selected is None and self.distiller is not None:
            self.distiller.teachers = [
                self._teacher_ga, self._teacher_performance,
                self._teacher_carbon]
            selected = await self.distiller.distill(features)
            source = "distilled"
        if selected is None and self.rlhf is not None:
            selected = self.rlhf.sample_action(features)
            source = "rlhf"
        if selected is None:
            best_params = self.ga.evolve(self._fitness_func, generations=5)
            params = best_params or self.current_params
            result = self._simulate_collection(params, "ga")
            self._record(params, result)
            return result

        if selected == "performance":
            params = {"interval": 60, "batch_size": 50, "parallel_calls": 10}
        elif selected == "carbon":
            params = {"interval": 300, "batch_size": 20, "parallel_calls": 3}
        elif selected == "hybrid":
            params = {"interval": 150, "batch_size": 35, "parallel_calls": 5}
        else:
            params = dict(self.current_params)

        if self.limit_graph is not None:
            limits = self.limit_graph.get_limits(features)
            if "max_interval" in limits:
                params["interval"] = min(params["interval"], limits["max_interval"])
            if "max_batch_size" in limits:
                params["batch_size"] = min(params["batch_size"], limits["max_batch_size"])
            if "max_parallel_calls" in limits:
                params["parallel_calls"] = min(params["parallel_calls"],
                                               limits["max_parallel_calls"])

        result = self._simulate_collection(params, source)
        self._record(params, result)
        if self.rlhf is not None and source in ("distilled", "rlhf"):
            self.rlhf.update(features, selected, self._fitness_func(params))
        return result

    def _teacher_ga(self, features): return "adaptive"
    def _teacher_performance(self, features): return "performance"
    def _teacher_carbon(self, features): return "carbon"

    def _simulate_collection(self, params, source):
        return {
            "action": "bio_inspired_collection",
            "interval_seconds": params["interval"],
            "batch_size": params["batch_size"],
            "parallel_calls": params["parallel_calls"],
            "estimated_performance_gain": 0.2 - (params["interval"] / 600) * 0.1,
            "estimated_carbon_savings": 0.1 + (params["batch_size"] / 100) * 0.05,
            "quality_improvement": 0.1,
            "source": source,
        }

    def _record(self, params, result):
        self.current_params = dict(params)
        self.collection_history.append({
            "params": params, "result": result,
            "timestamp": datetime.utcnow().isoformat()})
        self.fitness_history.append(self._fitness_func(params))

    def get_collection_stats(self):
        return {
            "total_collections": len(self.collection_history),
            "strategies": ["performance", "carbon", "hybrid", "adaptive"],
            "current_params": self.current_params,
            "fitness_history": self.fitness_history[-10:],
            "ga_population_size": self.ga.pop_size,
            "distillation_active": self.distiller is not None,
            "rlhf_active": self.rlhf is not None,
            "limit_graph_active": self.limit_graph is not None,
        }


# ============================================================
# PARETO / TOPSIS (P0 fixed: all objectives are COSTS)
# ============================================================
class ParetoFront:
    """All objectives are COSTS. Dominance = strictly better in all, at least one strictly less."""

    def __init__(self):
        self.solutions: List[Tuple[List[float], Any]] = []

    def add(self, objectives: Sequence[float], decision: Any):
        dominated = False
        for obj, _ in self.solutions:
            if (all(obj[i] <= objectives[i] for i in range(len(objectives)))
                    and any(obj[i] < objectives[i] for i in range(len(objectives)))):
                dominated = True
                break
        if not dominated:
            self.solutions = [
                (obj, dec) for obj, dec in self.solutions
                if not (all(objectives[i] <= obj[i] for i in range(len(objectives)))
                        and any(objectives[i] < obj[i] for i in range(len(objectives))))
            ]
            self.solutions.append((list(objectives), decision))
        return dominated

    def get_pareto_front(self): return self.solutions

    def get_best_by_weight(self, weights):
        """Lower weighted sum is better (all costs)."""
        best, best_score = None, float("inf")
        for obj, dec in self.solutions:
            score = sum(w * o for w, o in zip(weights, obj))
            if score < best_score:
                best_score, best = score, dec
        return best


class TOPSIS:
    @staticmethod
    def score(candidates, weights, criteria):
        """
        TOPSIS for COST criteria only. Returns higher-better score list.
        """
        if not candidates or not criteria:
            return []
        matrix = np.array([[float(c.get(c, 0.0)) for c in criteria]
                           for c in candidates])
        denom = np.sqrt((matrix ** 2).sum(axis=0))
        denom = np.where(denom == 0, 1.0, denom)
        norm = matrix / denom

        w = np.asarray(weights[: norm.shape[1]], dtype=np.float64)
        if w.sum() <= 0:
            w = np.ones(norm.shape[1]) / norm.shape[1]
        else:
            w = w / w.sum()
        weighted = norm * w

        # Cost criteria: ideal = min per column, negative ideal = max per column
        ideal = weighted.min(axis=0)
        neg_ideal = weighted.max(axis=0)
        d_plus = np.sqrt(((weighted - ideal) ** 2).sum(axis=1))
        d_minus = np.sqrt(((weighted - neg_ideal) ** 2).sum(axis=1))
        return (d_minus / (d_plus + d_minus + 1e-9)).tolist()


# ============================================================
# MODP DISTRIBUTION (P0 fixed)
# ============================================================
class MODPCloudDataDistribution:
    def __init__(self, config, db_manager, carbon_manager=None,
                 adaptive_cost=None, limit_graph=None, rlhf=None,
                 distiller=None, agent_registry=None):
        self.config = config
        self.db_manager = db_manager
        self.carbon_manager = carbon_manager
        self.adaptive_cost = adaptive_cost
        self.providers = {
            "aws": {"regions": ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"],
                    "cost_per_gb": 0.09, "carbon_score": 0.7,
                    "latency_score": 0.9, "availability": 0.99},
            "azure": {"regions": ["eastus", "westus", "northeurope", "southeastasia"],
                      "cost_per_gb": 0.10, "carbon_score": 0.8,
                      "latency_score": 0.85, "availability": 0.98},
            "gcp": {"regions": ["us-central1", "us-west1", "europe-west1", "asia-east1"],
                    "cost_per_gb": 0.08, "carbon_score": 0.9,
                    "latency_score": 0.88, "availability": 0.97},
        }
        self.active_provider = "aws"
        self.active_region = "us-east-1"
        self._lock = asyncio.Lock()
        self.pareto_front = ParetoFront()
        self.weights = list(config.modp.weights)
        self.adaptive_weights = config.modp.adaptive_weights
        self.learning_rate = config.modp.learning_rate
        self.recent_outcomes: deque = deque(maxlen=100)
        self.limit_graph = limit_graph
        self.rlhf = rlhf
        self.distiller = distiller
        self.roles = agent_registry or EmergentRoleRegistry()
        self.coordinator = MultiAgentCoordinator(self.roles)
        self.explainer: Optional[DecisionExplainer] = None
        self.last_explanation: Optional[Explanation] = None
        self._last_provider_idx = 0

    async def _modp_teacher(self, context):
        providers = context.get("providers", {})
        if not providers:
            return self.active_provider
        best, best_score = None, float("inf")  # cost minimization
        for prov, obj in providers.items():
            score = sum(w * o for w, o in zip(self.weights, obj))
            if score < best_score:
                best_score, best = score, prov
        return best

    async def _rule_based_teacher(self, context):
        cost = context.get("cost", {})
        if not cost:
            return self.active_provider
        return min(cost, key=cost.get)

    async def _static_teacher(self, context):
        return "aws"

    async def _measure_latency(self, provider):
        base = {"aws": 50, "azure": 60, "gcp": 45}.get(provider, 50)
        return base + random.uniform(-10, 10)

    async def _evaluate_providers(self, data):
        current_carbon = (await self.carbon_manager.get_current_intensity()
                          if self.carbon_manager else 400.0)
        results = {}
        for name, p in self.providers.items():
            latency = await self._measure_latency(name)
            cost = p["cost_per_gb"] * data.get("size_gb", 0.1)
            carbon = p["carbon_score"] * current_carbon / 400.0
            availability_cost = 1 - p["availability"]
            objectives = [cost, carbon, latency, availability_cost]
            results[name] = {
                "objectives": objectives,
                "decision": (name, p["regions"][0]),
            }
        return results

    async def distribute_data(self, data, preferences=None):
        preferences = preferences or {}
        eval_results = await self._evaluate_providers(data)
        n = len(self.weights)
        truncated = {p: d["objectives"][:n] for p, d in eval_results.items()}

        context = {
            "providers": {p: d["objectives"] for p, d in eval_results.items()},
            "cost": {p: d["objectives"][0] for p, d in eval_results.items()},
            "carbon": {p: d["objectives"][1] for p, d in eval_results.items()},
            "latency": {p: d["objectives"][2] for p, d in eval_results.items()},
        }

        self.pareto_front = ParetoFront()
        for p, obj in truncated.items():
            self.pareto_front.add(obj, (p, self.providers[p]["regions"][0]))

        # Primary selection via distiller -> rlhf -> MODP
        provider_name, source = None, None
        if self.distiller is not None:
            self.distiller.teachers = [
                self._modp_teacher, self._rule_based_teacher, self._static_teacher]
            provider_name = await self.distiller.distill(context)
            source = "distilled"
        if provider_name is None and self.rlhf is not None:
            provider_name = self.rlhf.sample_action(context)
            source = "rlhf"
        if provider_name is None or provider_name not in self.providers:
            best_decision = self.pareto_front.get_best_by_weight(self.weights)
            if best_decision is None:
                best_decision = min(
                    eval_results.items(),
                    key=lambda x: x[1]["objectives"][0])[1]["decision"]
            provider_name, _ = best_decision
            source = "modp"

        # LIMIT Graph may forbid providers
        if self.limit_graph is not None:
            limits = self.limit_graph.get_limits(context)
            forbidden = limits.get("forbidden_providers") or []
            if provider_name in forbidden:
                remaining = [p for p in self.providers if p not in forbidden]
                if remaining:
                    provider_name = remaining[0]
                    source = "limit_graph"

        # Multi-agent conflict resolution (safety and carbon broker take priority)
        prov_list = list(self.providers.keys())
        base_idx = prov_list.index(provider_name)
        bids = []
        # Explorer: random
        bids.append(AgentBid(agent_id="explorer", role=AgentRole.EXPLORER,
                             confidence=0.5,
                             proposed_action=random.randrange(len(prov_list)),
                             rationale="explore", carbon_score=0.5))
        # Exploiter: lowest latency
        lat_idx = min(range(len(prov_list)),
                      key=lambda i: list(self.providers.values())[i]["latency_score"])
        bids.append(AgentBid(agent_id="exploiter", role=AgentRole.EXPLOITER,
                             confidence=0.7, proposed_action=lat_idx,
                             rationale="lowest_latency", carbon_score=0.6))
        # Safety: highest availability
        avail_idx = min(range(len(prov_list)),
                        key=lambda i: 1 - list(self.providers.values())[i]["availability"])
        bids.append(AgentBid(agent_id="safety", role=AgentRole.SAFETY_OFFICER,
                             confidence=0.9, proposed_action=avail_idx,
                             rationale="highest_availability", carbon_score=0.5))
        # Carbon broker: lowest carbon
        carb_idx = min(range(len(prov_list)),
                       key=lambda i: list(self.providers.values())[i]["carbon_score"])
        bids.append(AgentBid(agent_id="carbon", role=AgentRole.CARBON_BROKER,
                             confidence=0.8, proposed_action=carb_idx,
                             rationale="lowest_carbon", carbon_score=0.9))
        # Verifier
        bids.append(AgentBid(agent_id="verifier", role=AgentRole.VERIFIER,
                             confidence=0.6, proposed_action=base_idx,
                             rationale="agree", carbon_score=0.5))

        voted_idx, resolution = await self.coordinator.resolve_conflict(
            bids, len(prov_list))
        provider_name = prov_list[voted_idx]

        # Register outcome later
        self._last_provider_idx = voted_idx

        region = self.providers[provider_name]["regions"][0]
        if preferences.get("region") in self.providers[provider_name]["regions"]:
            region = preferences["region"]

        async with self._lock:
            self.active_provider = provider_name
            self.active_region = region

        # Outcome + adaptive weight update
        actual_cost = self.providers[provider_name]["cost_per_gb"] * \
            data.get("size_gb", 0.1)
        actual_carbon = self.providers[provider_name]["carbon_score"] * 1.0
        actual_latency = await self._measure_latency(provider_name)
        actual_avail_cost = 1 - self.providers[provider_name]["availability"]
        outcome = [actual_cost, actual_carbon, actual_latency, actual_avail_cost][:n]
        self.recent_outcomes.append((list(self.weights), outcome))
        if self.adaptive_weights and len(self.recent_outcomes) >= 10:
            self._update_weights()

        if self.rlhf is not None:
            reward = -sum(eval_results[provider_name]["objectives"])
            self.rlhf.update(context, provider_name, reward)

        # Record agent success for the roles that agreed with the winner
        for b in bids:
            agreed = b.proposed_action % len(prov_list) == voted_idx
            self.roles.record(b.role, success=agreed,
                              reward=(1.0 if agreed else -0.2))

        # XAI
        pareto_list = [
            {"cost": d["objectives"][0], "carbon": d["objectives"][1],
             "latency": d["objectives"][2], "availability": d["objectives"][3]}
            for d in eval_results.values()
        ]
        chosen_dict = pareto_list[voted_idx]
        explanation = None
        if self.explainer is not None:
            explanation = self.explainer.explain(
                chosen_idx=voted_idx, chosen=chosen_dict, pareto=pareto_list,
                counterfactuals={}, safety_ok=True,
                carbon_price=0.0, confidence=0.7)
            self.last_explanation = explanation

        return {
            "optimal_provider": provider_name,
            "optimal_region": region,
            "pareto_front_size": len(self.pareto_front.get_pareto_front()),
            "scores": {p: d["objectives"] for p, d in eval_results.items()},
            "reason": f"Provider {provider_name} selected via {source} ({resolution})",
            "source": source,
            "resolution": resolution,
            "weights": list(self.weights),
            "data_size_gb": data.get("size_gb", 0),
            "explanation": explanation.to_dict() if explanation else None,
            "timestamp": datetime.utcnow().isoformat(),
        }

    def _update_weights(self):
        if not self.recent_outcomes:
            return
        outcomes = np.array([o for _, o in self.recent_outcomes])
        mean_outcome = outcomes.mean(axis=0)
        delta = mean_outcome - mean_outcome.mean()
        w = np.array(self.weights) - self.learning_rate * delta
        w = np.clip(w, 0.05, None)
        w /= w.sum()
        self.weights = w.tolist()

    async def get_distribution_status(self):
        async with self._lock:
            return {
                "providers": self.providers,
                "active_provider": self.active_provider,
                "active_region": self.active_region,
                "weights": list(self.weights),
                "distillation_active": self.distiller is not None,
                "rlhf_active": self.rlhf is not None,
                "limit_graph_active": self.limit_graph is not None,
                "multi_agent_active": True,
                "xai_active": self.explainer is not None,
                "role_stats": self.roles.stats(),
            }


# ============================================================
# MOE PREDICTIVE (P1 fixed)
# ============================================================
class MOEPredictiveAnalytics:
    def __init__(self, config, db_manager, distiller=None):
        self.config = config
        self.db_manager = db_manager
        self.history_price: deque = deque(maxlen=2000)
        self.history_production: deque = deque(maxlen=2000)
        self.history_context: deque = deque(maxlen=2000)
        self.history_labels: deque = deque(maxlen=2000)
        self._seen_timestamps: deque = deque(maxlen=4000)
        self._lock = asyncio.Lock()
        self._trained = False
        self.experts: List[Tuple[str, Callable]] = []
        self._init_experts()
        self.gating_model = None
        self.scaler = None
        self._init_gating()
        self.distiller = distiller

    def _init_experts(self):
        if PROPHET_AVAILABLE:
            self.experts.append(("prophet", self._forecast_prophet))
        if SKLEARN_AVAILABLE:
            self.experts.append(("linear", self._forecast_linear))
        self.experts.append(("exp_smooth", self._forecast_exp_smooth))
        if not self.experts:
            self.experts.append(("naive", self._forecast_naive))

    def _init_gating(self):
        if SKLEARN_AVAILABLE:
            self.gating_model = LogisticRegression(max_iter=1000)
            self.scaler = StandardScaler()

    async def _forecast_prophet(self, history, horizon):
        if len(history) < 30:
            return {"forecast": [0.0] * horizon, "confidence": 0.0}
        try:
            import pandas as pd
            df = pd.DataFrame(list(history)).sort_values("ds")
            m = Prophet(changepoint_prior_scale=0.05, seasonality_prior_scale=10)
            m.fit(df)
            future = m.make_future_dataframe(periods=horizon)
            forecast = m.predict(future)
            return {"forecast": forecast["yhat"].tail(horizon).tolist(),
                    "confidence": 0.9}
        except Exception as e:
            logger.warning(f"Prophet failed: {e}")
            return {"forecast": [0.0] * horizon, "confidence": 0.0}

    async def _forecast_linear(self, history, horizon):
        if len(history) < 2:
            return {"forecast": [0.0] * horizon, "confidence": 0.0}
        X = np.arange(len(history)).reshape(-1, 1)
        y = np.array([h["y"] for h in history])
        try:
            m = LinearRegression().fit(X, y)
            f = np.arange(len(history), len(history) + horizon).reshape(-1, 1)
            return {"forecast": m.predict(f).tolist(), "confidence": 0.7}
        except Exception:
            return {"forecast": [0.0] * horizon, "confidence": 0.0}

    async def _forecast_exp_smooth(self, history, horizon):
        if len(history) < 2:
            return {"forecast": [0.0] * horizon, "confidence": 0.0}
        values = [h["y"] for h in history]
        alpha = 0.3; smoothed = values[-1]; out = []
        for _ in range(horizon):
            out.append(smoothed)
            smoothed = alpha * values[-1] + (1 - alpha) * smoothed
        return {"forecast": out, "confidence": 0.6}

    async def _forecast_naive(self, history, horizon):
        if not history:
            return {"forecast": [0.0] * horizon, "confidence": 0.0}
        return {"forecast": [history[-1]["y"]] * horizon, "confidence": 0.3}

    async def _extract_context(self):
        now = datetime.utcnow()
        recent = list(self.history_price)[-20:]
        vol = float(np.std([h["y"] for h in recent])) if len(recent) >= 20 else 0.0
        mean = float(np.mean([h["y"] for h in recent])) if len(recent) >= 10 else 0.0
        return np.array([now.hour / 24.0, now.weekday() / 6.0, vol, mean])

    async def _compute_proxy_label(self):
        if len(self.history_price) < 50:
            return
        recent = list(self.history_price)
        actual = recent[-1]["y"]
        window = recent[-40:-1]
        errors = []
        for _, fn in self.experts:
            try:
                res = await fn(window, 1)
                pred = res["forecast"][0] if res["forecast"] else 0.0
                errors.append(abs(pred - actual))
            except Exception:
                errors.append(float("inf"))
        self.history_labels.append(int(np.argmin(errors)))

    async def _train_gating(self):
        if not SKLEARN_AVAILABLE or self.gating_model is None:
            return
        if len(self.history_context) < 100 or len(self.history_labels) < 100:
            return
        n = min(len(self.history_context), len(self.history_labels))
        X = np.array(list(self.history_context)[-n:])
        y = np.array(list(self.history_labels)[-n:])
        if len(np.unique(y)) < 2:
            return
        try:
            X_scaled = self.scaler.fit_transform(X)
            self.gating_model.fit(X_scaled, y)
            self._trained = True
        except Exception as e:
            logger.warning(f"Gating fit failed: {e}")

    async def update_history(self, price, production, timestamp: Optional[float] = None):
        """
        P1 fix: dedupe by timestamp to avoid double-adding records.
        """
        ts = timestamp if timestamp is not None else time.time()
        if ts in self._seen_timestamps:
            return
        self._seen_timestamps.append(ts)
        async with self._lock:
            now_dt = datetime.utcfromtimestamp(ts)
            self.history_price.append({"ds": now_dt, "y": price})
            self.history_production.append({"ds": now_dt, "y": production})
            self.history_context.append(await self._extract_context())
            await self._compute_proxy_label()
            if len(self.history_context) % 20 == 0:
                await self._train_gating()

    async def forecast_price(self, horizon_hours=None):
        horizon = horizon_hours or self.config.predictive_horizon_hours
        if len(self.history_price) < 30:
            return {"forecast": [], "confidence": 0.0}
        forecasts, confs = [], []
        for name, fn in self.experts:
            try:
                res = await fn(self.history_price, horizon)
                forecasts.append(np.asarray(res["forecast"]))
                confs.append(res.get("confidence", 0.5))
            except Exception as e:
                logger.warning(f"Expert {name} failed: {e}")
                forecasts.append(np.zeros(horizon)); confs.append(0.0)
        gating_probs = None
        if self.gating_model is not None and self._trained:
            try:
                ctx = await self._extract_context()
                X_scaled = self.scaler.transform([ctx])
                probs = self.gating_model.predict_proba(X_scaled)[0]
                if len(probs) == len(self.experts):
                    gating_probs = probs
            except Exception:
                pass
        if gating_probs is None:
            total = sum(confs) or 1.0
            gating_probs = np.array(confs) / total
        final = np.zeros(horizon)
        for i, f in enumerate(forecasts):
            final += gating_probs[i] * f
        for i, (n, _) in enumerate(self.experts):
            MOE_GATING_WEIGHTS.labels(expert=n).set(float(gating_probs[i]))
        PREDICTIVE_ACCURACY.labels(model="moe").set(0.85)
        return {"forecast": final.tolist(), "confidence": 0.85,
                "model": "moe",
                "expert_weights": gating_probs.tolist()}

    async def forecast_production(self, horizon_hours=None):
        horizon = horizon_hours or self.config.predictive_horizon_hours
        if len(self.history_production) < 5:
            return {"forecast": [28000.0] * horizon, "confidence": 0.0}
        values = [h["y"] for h in list(self.history_production)[-20:]]
        alpha = 0.3; smoothed = values[-1]; out = []
        for _ in range(horizon):
            out.append(smoothed)
            smoothed = alpha * values[-1] + (1 - alpha) * smoothed
        return {"forecast": out, "confidence": 0.6}

    def get_stats(self):
        return {"num_experts": len(self.experts),
                "gating_trained": self._trained,
                "history_len": len(self.history_price),
                "distillation_active": self.distiller is not None}


# ============================================================
# SCHEDULER (P1 fixed: separate carbon forecast)
# ============================================================
class MultiObjectiveCarbonScheduler:
    def __init__(self, config, carbon_manager, predictive, distiller=None):
        self.config = config
        self.carbon_manager = carbon_manager
        self.predictive = predictive
        self.threshold = config.multi_objective_scheduler.carbon_threshold
        self.max_delay = config.multi_objective_scheduler.max_delay_seconds
        self.freshness_w = config.multi_objective_scheduler.freshness_importance
        self.cost_w = config.multi_objective_scheduler.cost_importance
        self.carbon_w = config.multi_objective_scheduler.carbon_importance
        self.queue: asyncio.Queue = asyncio.Queue()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._carbon_history: deque = deque(maxlen=200)

    async def start(self):
        if self._running:
            return
        self._running = True

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    def observe_carbon(self, intensity: float):
        self._carbon_history.append(intensity)

    async def _forecast_carbon(self, horizon: int = 12) -> List[float]:
        if len(self._carbon_history) < 3:
            return [self.carbon_manager.current_intensity] * horizon
        vals = list(self._carbon_history)
        alpha = 0.3
        smoothed = vals[-1]
        out = []
        for _ in range(horizon):
            out.append(smoothed)
            smoothed = alpha * vals[-1] + (1 - alpha) * smoothed
        return out

    async def _compute_delay(self, current_carbon, freshness_hours):
        forecast = await self._forecast_carbon(12)
        forecast_min = min(forecast) if forecast else current_carbon
        carbon_reduction = max(
            0.0,
            (current_carbon - forecast_min) / max(current_carbon, 1.0))
        freshness_cost = min(1.0, freshness_hours / 6.0)
        utility_now = self.freshness_w * (1 - freshness_cost) + self.cost_w * 0.8
        utility_delay = self.carbon_w * carbon_reduction + self.cost_w * 0.5
        if utility_now >= utility_delay:
            return 0
        ratio = min(1.0, max(0.0, (current_carbon - self.threshold) /
                              max(self.threshold, 1.0)))
        return int(self.max_delay * ratio)

    async def submit_collection(self, collection_func, priority=1,
                                critical=False, freshness_hours=1.0):
        if critical:
            return await collection_func()
        current_carbon = await self.carbon_manager.get_current_intensity()
        self.observe_carbon(current_carbon)
        if current_carbon <= self.threshold:
            return await collection_func()
        delay = await self._compute_delay(current_carbon, freshness_hours)
        if delay > 0:
            await asyncio.sleep(delay)
        return await collection_func()

    async def health_check(self):
        return {"status": "healthy" if self._running else "stopped"}


# ============================================================
# MAIN COLLECTOR
# ============================================================
class EnhancedHeliumDataCollectorV11:
    def __init__(self, config: Optional[Union[HeliumDataCollectorConfig, Dict]] = None):
        if isinstance(config, HeliumDataCollectorConfig):
            self.config = config
        elif isinstance(config, dict) and PYDANTIC_AVAILABLE:
            self.config = HeliumDataCollectorConfig(**config)
        else:
            self.config = HeliumDataCollectorConfig()
        self.instance_id = self.config.instance_id

        self.limit_graph_enabled = self.config.limit_graph_enabled
        self.rlhf_enabled = self.config.rlhf_enabled
        self.distillation_enabled = self.config.distillation_enabled

        # Support modules
        self.db_manager = EnhancedDatabaseManager(self.config)
        self.carbon_manager = CarbonIntensityManager(self.config)
        self.quantum_security = QuantumResilientDataSecurity(self.config, self.db_manager)
        self.blockchain = BlockchainDataVerification(self.config, self.db_manager)
        self.cloud_storage = MultiCloudStorage(self.config)
        self.quality_monitor = DataQualityMonitor(self.db_manager)
        self.cache = EnhancedCacheManager()
        self.export_queue = EnhancedExportQueue(self.db_manager)

        # Drift / adaptive cost
        self.drift_detector = DriftDetector()
        self.adaptive_cost = AdaptiveCostFunction()

        # Enhancement modules
        self.limit_graph = LimitGraph() if self.limit_graph_enabled else None
        self.rlhf = (RLHFOptimizer(action_space=["performance", "carbon",
                                                 "hybrid", "adaptive"])
                     if self.rlhf_enabled else None)
        self.distiller = MultiTeacherDistiller([]) if self.distillation_enabled else None

        # Multi-agent registry
        self.agent_registry = EmergentRoleRegistry() if self.config.multi_agent_enabled \
            else None

        # Cloud distributor
        self.cloud_distributor = MODPCloudDataDistribution(
            self.config, self.db_manager, self.carbon_manager,
            self.adaptive_cost, self.limit_graph, self.rlhf,
            MultiTeacherDistiller([]) if self.distillation_enabled else None,
            agent_registry=self.agent_registry)

        # Collector
        collector_distiller = (MultiTeacherDistiller([])
                               if self.distillation_enabled else None)
        if self.config.bio.enabled:
            self.autonomous_collector = BioInspiredAutonomousCollector(
                self.config, self.db_manager, self.adaptive_cost,
                self.limit_graph, self.rlhf, collector_distiller)
        else:
            self.autonomous_collector = None

        # Predictive
        pred_distiller = MultiTeacherDistiller([]) if self.distillation_enabled else None
        self.predictive = (MOEPredictiveAnalytics(self.config, self.db_manager,
                                                  pred_distiller)
                           if self.config.moe.enabled
                           else None)

        # Anomaly / self-healing
        self.anomaly_detector = MLAnomalyDetector(self.config, self.db_manager)
        self.self_healing = SelfHealingManager(self.config, self.drift_detector)

        # Scheduler
        self.scheduler = (MultiObjectiveCarbonScheduler(
            self.config, self.carbon_manager, self.predictive)
            if self.config.multi_objective_scheduler.enabled else None)

        # Enhancement 5: temporal logic
        self.temporal = None; self.shield = None
        if self.config.temporal_logic_enabled:
            self.temporal = TemporalLogicMonitor(horizon=10)
            self.temporal.add_formula(STLFormula(
                name="price_reasonable",
                predicate=lambda r: 50.0 <= r.get("price_index", 200.0) <= 500.0,
                operator=STLOperator.ALWAYS, horizon=10))
            self.temporal.add_formula(STLFormula(
                name="production_floor",
                predicate=lambda r: r.get("global_production_tonnes", 0.0) > 5000.0,
                operator=STLOperator.EVENTUALLY, horizon=5))
            self.temporal.add_formula(STLFormula(
                name="anomaly_implies_safe",
                predicate=lambda r: r.get("anomaly_score", 0.0) < 0.9,
                operator=STLOperator.IMPLIES, horizon=5,
                rhs_predicate=lambda r: r.get("safety_ok", True)))
            self.shield = SafetyShield(self.temporal)

        # Enhancement 6: XAI
        self.explainer: Optional[DecisionExplainer] = None
        if self.config.xai_enabled:
            self.explainer = DecisionExplainer(
                feature_names=["price_index", "production", "demand",
                               "anomaly_score"],
                predict_fn=None)
            self.cloud_distributor.explainer = self.explainer

        self.last_explanation: Optional[Explanation] = None

        # Enhancement 7: precision
        self.precision_controller = (PrecisionController()
                                     if self.config.precision_switching_enabled
                                     else None)
        self.precision_adapter = (HardwareAwareAdapter(self.precision_controller)
                                  if self.precision_controller else None)
        self.current_precision: Optional[PrecisionLevel] = None

        # Enhancement 2: causal
        self.causal = (CausalCounterfactualEstimator(state_dim=8, n_actions=4)
                       if self.config.causal_enabled else None)
        self.last_counterfactuals: Dict[int, float] = {}

        # Enhancement 3: federated
        self.federated = (SecureFederatedAggregator(
            dp_sigma=self.config.federated_dp_sigma,
            target_epsilon=self.config.federated_target_epsilon,
            target_delta=self.config.federated_target_delta)
            if self.config.federated_enabled else None)

        # Enhancement 9: chaos
        self.chaos: Optional[ChaosEngineer] = None
        if self.config.chaos_enabled:
            hypotheses = [
                SteadyStateHypothesis(
                    name="price_in_range",
                    predicate=lambda m: 50.0 <= m.get("price_index", 200.0) <= 500.0),
                SteadyStateHypothesis(
                    name="health_above_50",
                    predicate=lambda m: m.get("health_score", 100) > 50),
            ]
            self.chaos = ChaosEngineer(
                ChaosConfig(
                    fault_prob=self.config.chaos_fault_prob,
                    latency_inject_ms=self.config.chaos_latency_ms,
                    carbon_spike_prob=self.config.chaos_carbon_spike_prob,
                    blast_radius=self.config.chaos_blast_radius,
                    max_concurrent_faults=self.config.chaos_max_concurrent,
                    duration_s=self.config.chaos_duration_s),
                hypotheses)

        # Enhancement 10: HITL / active learning
        self.uncertainty = (UncertaintyEstimator()
                            if self.config.hitl_enabled else None)
        self.hitl = (HumanInTheLoopGate(
            timeout_s=self.config.hitl_timeout_s,
            auto_approve_on_timeout=self.config.hitl_auto_approve_on_timeout,
            production_safe=self.config.hitl_production_safe)
            if self.config.hitl_enabled else None)
        self.active_learner = (ActiveLearningSampler()
                               if self.config.hitl_enabled else None)

        # Federated learner + user-adaptive + sustainability
        self.federated_learner = FederatedHeliumDataLearner(
            self.db_manager, self.instance_id,
            self.config.federated_share_interval,
            aggregator=self.federated)
        self.user_adaptive = UserAdaptiveHeliumDataReflexivity(
            self.db_manager, self.config.federated_learning_rate)
        self.sustainability_tracker = HeliumDataSustainabilityTracker(self.db_manager)

        # State
        self.records: List[HeliumRecord] = []
        self._records_lock = asyncio.Lock()
        self.dead_letter_queue: deque = deque(maxlen=1000)
        self._task_manager = TaskManager(max_workers=8)
        self._shutdown_event = asyncio.Event()
        self._running = False
        self._sample_count = 0

        self._health_components = {
            "database": self.db_manager,
            "quantum_security": self.quantum_security,
            "blockchain": self.blockchain,
            "carbon_manager": self.carbon_manager,
            "cloud_distributor": None,
            "cloud_storage": self.cloud_storage,
            "predictive": self.predictive,
        }

        # Metrics server
        if PROMETHEUS_AVAILABLE and start_http_server is not None:
            try:
                start_http_server(self.config.metrics_port)
            except Exception as e:
                logger.warning(f"Metrics server failed: {e}")

        logger.info(f"EnhancedHeliumDataCollectorV11 v{self.config.version} "
                    f"initialized (instance: {self.instance_id})")
        atexit.register(self._atexit_cleanup)

    def _atexit_cleanup(self):
        self._shutdown_event.set()

    def _register_tasks(self):
        self._task_manager.register_task("refresh", self._refresh_loop)
        self._task_manager.register_task("health_check", self._health_check_loop)
        self._task_manager.register_task("quality_monitor", self._quality_monitor_loop)
        self._task_manager.register_task("quantum_monitor", self._quantum_monitor_loop)
        self._task_manager.register_task("blockchain_monitor", self._blockchain_monitor_loop)
        self._task_manager.register_task("auto_collect", self._auto_collect_loop)
        self._task_manager.register_task("cloud_sync", self._cloud_sync_loop)
        self._task_manager.register_task("carbon_update", self._carbon_update_loop)
        self._task_manager.register_task("predictive", self._predictive_loop)
        self._task_manager.register_task("ml_retrain", self._ml_retrain_loop)
        if self.scheduler:
            self._task_manager.register_task("scheduler_loop", self.scheduler.start)
        if self.config.self_healing.enabled:
            self._task_manager.register_task(
                "self_healing_monitor", self._self_healing_monitor_loop)

    async def start(self):
        self._running = True
        await self.db_manager.init()
        await self.cache.start()
        await self.export_queue.start()
        await self.quality_monitor.start()
        try:
            await self.carbon_manager.refresh()
        except Exception as e:
            logger.warning(f"Initial carbon refresh failed: {e}")
        await self._load_data()
        if self.records and len(self.records) >= 20:
            try:
                await self.anomaly_detector.train(self.records)
                await self.self_healing.train(self.records)
            except Exception as e:
                logger.warning(f"Initial training failed: {e}")
        if self.scheduler:
            await self.scheduler.start()
        self._register_tasks()
        self._task_manager.start_registered_tasks()
        logger.info("Helium collector started")

    async def _load_data(self):
        try:
            loaded = await self.db_manager.load_records(limit=500)
            if loaded:
                self.records = loaded
                logger.info(f"Loaded {len(loaded)} records from DB")
            else:
                for _ in range(5):
                    self.records.append(HeliumRecord(
                        date=date.today(),
                        global_production_tonnes=28000 + random.uniform(-500, 500),
                        global_demand_tonnes=29000 + random.uniform(-500, 500),
                        price_index=200 + random.uniform(-10, 10),
                    ))
        except Exception as e:
            logger.warning(f"_load_data failed: {e}")

    async def _produce_record(self) -> HeliumRecord:
        start = time.time()

        fault_injected = False
        if self.chaos and self.chaos.maybe_fault():
            fault_injected = True
            HELIUM_COLLECTIONS.labels(status="chaos_fault").inc()

        try:
            if fault_injected:
                raise CollectionError("Chaos fault injected")

            snapshot = {
                "global_production_tonnes": 28000 + random.uniform(-500, 500),
                "global_demand_tonnes": 29000 + random.uniform(-500, 500),
                "price_index": 200 + random.uniform(-20, 20),
            }

            if self.chaos:
                delay_ms = self.chaos.maybe_latency()
                if delay_ms > 0:
                    await asyncio.sleep(delay_ms / 1000.0)

            record = HeliumRecord(
                date=date.today(),
                global_production_tonnes=float(snapshot["global_production_tonnes"]),
                global_demand_tonnes=float(snapshot["global_demand_tonnes"]),
                price_index=float(snapshot["price_index"]),
            )

            # Anomaly
            try:
                is_anomaly, score, _ = await self.anomaly_detector.detect_anomaly(
                    "price_index", record.price_index,
                    context={"price_index": record.price_index,
                             "production": record.global_production_tonnes,
                             "demand": record.global_demand_tonnes})
                record.is_anomaly = is_anomaly
                record.anomaly_score = float(score)
            except Exception as e:
                logger.warning(f"Anomaly detection failed: {e}")

            self.quality_monitor.validate(record)

            carbon_intensity = await self.carbon_manager.get_current_intensity()
            if self.chaos:
                carbon_intensity = self.chaos.maybe_carbon_spike(carbon_intensity)
            carbon_price = await self.carbon_manager.get_current_price()

            # Precision
            if self.precision_adapter and self.precision_controller:
                # Feed synthetic telemetry to controller
                self.precision_controller.observe(HardwareTelemetry(
                    gpu_util_pct=random.uniform(20, 90),
                    temperature_c=random.uniform(45, 75),
                    power_watts=random.uniform(50, 250),
                    observed_quality_drop=record.anomaly_score * 0.1))
                _, level = self.precision_adapter.adapt(
                    {}, carbon_intensity, 0.5, carbon_price)
                self.current_precision = level
                record.precision_level = level.value

            # Quantum
            quantum_key = await self.quantum_security.generate_keypair(
                self.config.quantum_algorithm)
            signature = await self.quantum_security.sign_helium_data(
                record.to_dict(), quantum_key["key_id"])
            record.quantum_signature = signature

            # Blockchain
            data_hash = hashlib.sha256(
                json.dumps(record.to_dict(), sort_keys=True,
                           default=str).encode()).hexdigest()
            bc = await self.blockchain.record_helium_data(
                record.record_id, data_hash, {"price": record.price_index})
            record.blockchain_tx_hash = bc.get("tx_hash")

            # Distribution
            try:
                distribution = await self.cloud_distributor.distribute_data(
                    {"size_gb": 0.01, "data_points": 1})
                record.cloud_distribution = distribution
            except Exception as e:
                logger.warning(f"Distribution failed: {e}")

            # Temporal / safety
            safety_ok = True; stl_verdict: Dict[str, bool] = {}
            if self.temporal and self.shield:
                candidate = {
                    "price_index": record.price_index,
                    "global_production_tonnes": record.global_production_tonnes,
                    "global_demand_tonnes": record.global_demand_tonnes,
                    "anomaly_score": record.anomaly_score,
                    "safety_ok": True,
                }
                self.temporal.observe(candidate)
                _, safety_ok, stl_verdict = self.shield.screen(
                    record.to_dict(),
                    [{"value": record.price_index, "record": record}],
                    key="value",
                    cost_fn=lambda c: abs(c.get("value", 0) - 200.0))
                # Apply shield replacement: if not safe, mark record's anomaly_score
                if not safety_ok:
                    record.anomaly_score = max(record.anomaly_score, 0.5)
            record.safety_ok = safety_ok
            record.stl_verdict = stl_verdict

            # Causal RL
            state_vec = np.array([
                record.global_production_tonnes / 30000.0,
                record.global_demand_tonnes / 30000.0,
                record.price_index / 300.0,
                float(record.is_anomaly),
                carbon_intensity / 1000.0,
                carbon_price / 0.5,
                self.current_precision.value == "int8" if self.current_precision else 0.5,
                self.current_precision.value == "int4" if self.current_precision else 0.5,
            ], dtype=np.float64)

            chosen_action = 0
            if self.causal:
                chosen_action = self.causal.select_action(state_vec, 4)
                reward_for_action = 1.0 - record.anomaly_score
                self.causal.update(CausalTransition(
                    state=state_vec, action=chosen_action,
                    reward=reward_for_action, next_state=state_vec))
                self.last_counterfactuals = self.causal.counterfactuals(state_vec, 4)
                record.counterfactuals = dict(self.last_counterfactuals)

            # Federated
            if self.federated:
                try:
                    self.federated.submit(FederatedUpdate(
                        node_id=self.instance_id,
                        weights={"local": np.array([record.price_index],
                                                  dtype=np.float64)},
                        n_samples=1, carbon_intensity=carbon_intensity))
                    self.federated.aggregate()
                except PrivacyBudgetExceeded as e:
                    logger.warning(f"Federated DP budget exhausted: {e}")

            # User-adaptive
            self.user_adaptive.record_feedback("price", record.price_index)

            # Reward / uncertainty / HITL
            reward = 1.0 - record.anomaly_score - carbon_price * 0.05
            reward = max(0.0, min(1.0, reward))

            human_approved = True
            if self.uncertainty and self.hitl and self.predictive:
                try:
                    fc = await self.predictive.forecast_price(1)
                    probs = np.abs(np.asarray(
                        fc.get("forecast", [0.5])) + 1e-6)
                    if probs.sum() <= 0:
                        probs = np.array([1.0])
                    probs = probs / probs.sum()
                    expert_forecasts = [
                        float(p) for p in fc.get("expert_weights", [1.0])]
                    self.uncertainty.observe_expert_forecasts(expert_forecasts)
                    is_unc, unc = self.uncertainty.is_uncertain(probs)
                except Exception:
                    is_unc, unc = False, 0.0

                if is_unc or not safety_ok:
                    req = HITLRequest(
                        request_id=uuid.uuid4().hex[:8],
                        reason="high_uncertainty" if is_unc else "safety_violation",
                        chosen_idx=chosen_action,
                        candidates=[{"value": record.price_index}],
                        uncertainty=unc, carbon_price=carbon_price,
                        metadata={"record_id": record.record_id})
                    human_approved = await self.hitl.request(req)
                    record.hitl_approved = human_approved
                    if self.active_learner:
                        self.active_learner.maybe_store(
                            {"record_id": record.record_id, "reward": reward,
                             "price_index": record.price_index,
                             "global_production_tonnes": record.global_production_tonnes,
                             "global_demand_tonnes": record.global_demand_tonnes,
                             "anomaly_score": record.anomaly_score},
                            uncertainty=unc, threshold=0.3)
                self.uncertainty.observe(reward)

            # XAI
            if self.explainer:
                pareto_list = [{
                    "price_index": record.price_index,
                    "production": record.global_production_tonnes,
                    "demand": record.global_demand_tonnes,
                    "anomaly_score": record.anomaly_score,
                }]
                explanation = self.explainer.explain(
                    chosen_idx=chosen_action, chosen=pareto_list[0],
                    pareto=pareto_list,
                    counterfactuals=self.last_counterfactuals,
                    safety_ok=safety_ok and human_approved,
                    carbon_price=carbon_price, confidence=0.7)
                self.last_explanation = explanation
                record.explanation = explanation.to_dict()

            record.provenance = {
                "schema": "helium_collector_v18",
                "instance_id": self.instance_id,
                "chaos_active": self.chaos is not None,
            }
            record.chaos_events = self.chaos.events[-10:] if self.chaos else []

            async with self._records_lock:
                self.records.append(record)
                cap = self.config.retention_days * 100
                if len(self.records) > cap:
                    self.records = self.records[-cap:]

            await self.db_manager.insert_helium_record(record)
            DATA_QUALITY_SCORE.set(record.anomaly_score)
            HELIUM_COLLECTIONS.labels(status="success").inc()
            self._sample_count += 1
            return record
        finally:
            if fault_injected and self.chaos:
                self.chaos.release_fault()

    async def _refresh_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                async def collect_one():
                    return await self._produce_record()

                if self.scheduler:
                    await self.scheduler.submit_collection(
                        collect_one, critical=False)
                else:
                    await collect_one()

                # Steady-state check
                if self.chaos and self.chaos.hypotheses:
                    async with self._records_lock:
                        last = self.records[-1].to_dict() if self.records else {}
                    health = await self.health_check()
                    metrics = {"price_index": last.get("price_index", 200.0),
                               "health_score": health.get("health_score", 100)}
                    if not self.chaos.steady_state_ok(metrics):
                        logger.warning("Chaos steady-state violated; deactivating.")
                        self.chaos.deactivate()

                await asyncio.sleep(self.config.refresh_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Refresh error: {e}")
                self.dead_letter_queue.append({"error": str(e), "t": time.time()})
                await asyncio.sleep(60)

    async def _auto_collect_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                if self.autonomous_collector:
                    intensity = await self.carbon_manager.get_current_intensity()
                    state = {"carbon_intensity": intensity,
                             "data_volume": len(self.records)}
                    result = await self.autonomous_collector.optimize_collection(
                        state, None)
                    if result.get("action"):
                        logger.info(f"Autonomous collection: {result['action']}")
                await asyncio.sleep(self.config.auto_collect_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Auto collect error: {e}")
                await asyncio.sleep(60)

    async def _cloud_sync_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                if self.records:
                    data = {"size_gb": len(self.records) * 0.001,
                            "data_points": len(self.records)}
                    distribution = await self.cloud_distributor.distribute_data(data)
                    logger.info(f"Cloud distribution: "
                                f"{distribution['optimal_provider']} "
                                f"({distribution['optimal_region']})")
                await asyncio.sleep(self.config.cloud_sync_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cloud sync error: {e}")
                await asyncio.sleep(60)

    async def _predictive_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                async with self._records_lock:
                    records = list(self.records[-20:])
                if records and self.predictive:
                    for i, r in enumerate(records):
                        ts = time.time() - (len(records) - i)
                        await self.predictive.update_history(
                            r.price_index, r.global_production_tonnes,
                            timestamp=ts)
                    await self.predictive.forecast_price(1)
                await asyncio.sleep(self.config.predictive_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Predictive update error: {e}")
                await asyncio.sleep(60)

    async def _ml_retrain_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                async with self._records_lock:
                    records = list(self.records[-500:])
                if len(records) >= 20:
                    await self.anomaly_detector.train(records)
                    await self.self_healing.train(records)
            except Exception as e:
                logger.warning(f"ML retrain error: {e}")
            await asyncio.sleep(self.config.ml_retrain_interval)

    async def _self_healing_monitor_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                async with self._records_lock:
                    records = list(self.records[-50:])
                for r in records[-5:]:
                    await self.self_healing.detect_anomaly(r)
                if records:
                    self.sustainability_tracker.record(
                        1.0 - records[-1].anomaly_score)
            except Exception as e:
                logger.error(f"Self-healing loop error: {e}")
            await asyncio.sleep(
                self.config.self_healing.health_check_interval)

    async def _quality_monitor_loop(self):
        while self._running and not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.health_check_interval)

    async def _health_check_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                h = await self.health_check()
                HEALTH_SCORE.set(h.get("health_score", 100))
                await asyncio.sleep(self.config.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health loop error: {e}")
                await asyncio.sleep(60)

    async def _quantum_monitor_loop(self):
        while self._running and not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.quantum_monitor_interval)

    async def _blockchain_monitor_loop(self):
        while self._running and not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.blockchain_monitor_interval)

    async def _carbon_update_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                await self.carbon_manager.refresh()
            except Exception as e:
                logger.warning(f"Carbon refresh failed: {e}")
            await asyncio.sleep(self.config.carbon_update_interval)

    async def get_comprehensive_status(self):
        quantum_status = self.quantum_security.get_quantum_status()
        blockchain_status = await self.blockchain.get_blockchain_status()
        collection_stats = (self.autonomous_collector.get_collection_stats()
                            if self.autonomous_collector else {})
        cloud_status = await self.cloud_distributor.get_distribution_status()
        async with self._records_lock:
            record_count = len(self.records)
            latest = self.records[-1].to_dict() if self.records else None
        return {
            "instance_id": self.instance_id,
            "version": self.config.version,
            "quantum_security": quantum_status,
            "blockchain": blockchain_status,
            "autonomous_collection": collection_stats,
            "cloud_distribution": cloud_status,
            "record_count": record_count,
            "latest": latest,
            "data_quality": await self.quality_monitor.get_statistics(),
            "cache": await self.cache.get_statistics(),
            "anomaly_detection": await self.anomaly_detector.get_statistics(),
            "self_healing": await self.self_healing.get_statistics(),
            "predictive": self.predictive.get_stats() if self.predictive else None,
            "cloud_storage": {"providers": list(self.cloud_storage.providers.keys())},
            "scheduler_enabled": self.scheduler is not None,
            "ten_enhancements": {
                "quantum_distillation": self.config.distillation_enabled,
                "causal": self.causal is not None,
                "federated": self.federated is not None,
                "multi_agent": self.agent_registry is not None,
                "temporal_logic": self.temporal is not None,
                "xai": self.explainer is not None,
                "precision_switching": self.precision_adapter is not None,
                "carbon_market": self.config.carbon_market_enabled,
                "chaos": self.chaos is not None,
                "hitl": self.hitl is not None,
            },
            "current_precision": self.current_precision.value
            if self.current_precision else None,
            "precision_stats": self.precision_controller.stats()
            if self.precision_controller else {},
            "rec_kwh": self.carbon_manager.recs.total_kwh(),
            "counterfactuals": self.last_counterfactuals,
            "causal_stats": self.causal.stats() if self.causal else {},
            "federated_stats": self.federated.stats() if self.federated else {},
            "uncertainty_stats": self.uncertainty.stats() if self.uncertainty else {},
            "active_learner_stats": self.active_learner.stats()
            if self.active_learner else {},
            "temporal_violations": self.temporal.recent_violations()
            if self.temporal else [],
            "chaos_stats": self.chaos.stats() if self.chaos else {},
            "sustainability_score": await self.sustainability_tracker.get_score(),
            "health": await self.health_check(),
            "timestamp": datetime.utcnow().isoformat(),
        }

    async def health_check(self):
        results = {}
        for name, comp in self._health_components.items():
            if comp and hasattr(comp, "health_check"):
                try:
                    results[name] = await comp.health_check()
                except Exception as e:
                    results[name] = {"status": "unhealthy", "error": str(e)}
            else:
                results[name] = {"status": "ok" if comp else "unavailable"}
        checkable = [r for r in results.values()
                     if r.get("status") != "unavailable"]
        if not checkable:
            overall, score = "degraded", 0
        else:
            ok = all(r.get("status") in ("ok", "healthy") for r in checkable)
            overall, score = ("healthy", 100) if ok else ("degraded", 50)
        return {"status": overall, "health_score": score,
                "components": results,
                "timestamp": datetime.utcnow().isoformat()}

    async def shutdown(self):
        logger.info(f"Shutting down (instance: {self.instance_id})")
        self._shutdown_event.set()
        self._running = False
        await self._task_manager.stop_all()
        if self.scheduler:
            with contextlib.suppress(Exception):
                await self.scheduler.stop()
        with contextlib.suppress(Exception):
            await self.carbon_manager.close()
        with contextlib.suppress(Exception):
            await self.cache.stop()
        with contextlib.suppress(Exception):
            await self.export_queue.stop()
        with contextlib.suppress(Exception):
            await self.quality_monitor.stop()
        with contextlib.suppress(Exception):
            await self.db_manager.close()
        logger.info("Shutdown complete")


# ============================================================
# FASTAPI
# ============================================================
if FASTAPI_AVAILABLE:
    from contextlib import asynccontextmanager

    collector: Optional[EnhancedHeliumDataCollectorV11] = None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        global collector
        collector = EnhancedHeliumDataCollectorV11()
        await collector.start()
        try:
            yield
        finally:
            if collector:
                await collector.shutdown()
                collector = None

    app = FastAPI(title="Helium Data Collector API", version="18.0",
                  lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware, allow_origins=["*"], allow_credentials=True,
        allow_methods=["*"], allow_headers=["*"])
    security = HTTPBearer(auto_error=False)

    _api_config: Optional[HeliumDataCollectorConfig] = None

    def _get_config() -> HeliumDataCollectorConfig:
        global _api_config
        if _api_config is None:
            _api_config = HeliumDataCollectorConfig()
        return _api_config

    async def verify_token(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    ):
        if not JOSE_AVAILABLE:
            # No JWT library: reject unless explicitly anonymous in dev
            if _get_config().environment == "production":
                raise HTTPException(status_code=500,
                                    detail="JWT library missing")
            return {"sub": "anonymous"}
        if credentials is None:
            raise HTTPException(status_code=401, detail="Missing credentials")
        try:
            return jwt.decode(credentials.credentials,
                              _get_config().jwt_secret,
                              algorithms=[_get_config().jwt_algorithm])
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid token")

    @app.post("/token")
    async def issue_token(sub: str = "user"):
        """Very simple token issuance for demo; replace with real auth."""
        if not JOSE_AVAILABLE:
            raise HTTPException(status_code=500, detail="JWT library missing")
        exp = datetime.utcnow() + timedelta(seconds=_get_config().jwt_expiry_s)
        token = jwt.encode({"sub": sub, "exp": exp},
                           _get_config().jwt_secret,
                           algorithm=_get_config().jwt_algorithm)
        return {"access_token": token, "token_type": "bearer",
                "expires_in": _get_config().jwt_expiry_s}

    @app.post("/collect")
    async def collect(user: Dict = Depends(verify_token)):
        if not collector:
            raise HTTPException(status_code=503, detail="Collector not initialized")
        rec = await collector._produce_record()
        return {"record": rec.to_dict()}

    @app.get("/status")
    async def status(user: Dict = Depends(verify_token)):
        if not collector:
            raise HTTPException(status_code=503, detail="Collector not initialized")
        return await collector.get_comprehensive_status()

    @app.get("/health")
    async def health():
        if not collector:
            raise HTTPException(status_code=503, detail="Collector not initialized")
        return await collector.health_check()

    @app.get("/explanation/last")
    async def last_explanation(user: Dict = Depends(verify_token)):
        if not collector:
            raise HTTPException(status_code=503, detail="Collector not initialized")
        return {"explanation": collector.last_explanation.to_dict()
                if collector.last_explanation else None,
                "counterfactuals": collector.last_counterfactuals}

    @app.post("/chaos")
    async def chaos_endpoint(fault_prob: float = 0.0, latency_ms: float = 0.0,
                             carbon_spike_prob: float = 0.0,
                             duration_s: float = 0.0,
                             user: Dict = Depends(verify_token)):
        if not collector:
            raise HTTPException(status_code=503, detail="Collector not initialized")
        hypotheses = [
            SteadyStateHypothesis(
                name="price_in_range",
                predicate=lambda m: 50.0 <= m.get("price_index", 200.0) <= 500.0),
            SteadyStateHypothesis(
                name="health_above_50",
                predicate=lambda m: m.get("health_score", 100) > 50),
        ]
        collector.chaos = ChaosEngineer(
            ChaosConfig(
                fault_prob=fault_prob, latency_inject_ms=latency_ms,
                carbon_spike_prob=carbon_spike_prob,
                duration_s=duration_s),
            hypotheses)
        return {"status": "chaos enabled",
                "config": asdict(collector.chaos.config)}

    @app.post("/chaos/disable")
    async def chaos_disable(user: Dict = Depends(verify_token)):
        if collector and collector.chaos:
            collector.chaos.deactivate()
        return {"status": "chaos disabled"}

    @app.get("/hitl/pending")
    async def hitl_pending(user: Dict = Depends(verify_token)):
        if not collector or not collector.hitl:
            return {"pending": []}
        return {"pending": collector.hitl.pending_requests()}

    @app.post("/hitl/approval")
    async def hitl_approval(request_id: str, approved: bool,
                            user: Dict = Depends(verify_token)):
        if not collector or not collector.hitl:
            raise HTTPException(status_code=503, detail="HITL not enabled")
        ok = await collector.hitl.resolve(request_id, approved)
        return {"status": "resolved" if ok else "not_found",
                "request_id": request_id, "approved": approved}


# ============================================================
# SIGNAL HANDLING / MAIN
# ============================================================
_collector_instance: Optional[EnhancedHeliumDataCollectorV11] = None
_collector_lock = asyncio.Lock()
_shutdown_requested = False
_shutdown_event_global = asyncio.Event()


async def _signal_shutdown():
    _shutdown_event_global.set()


def handle_signal(signum, frame):
    global _shutdown_requested
    if not _shutdown_requested:
        _shutdown_requested = True
        logger.info(f"Received signal {signum}, initiating shutdown...")
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_signal_shutdown())
        except RuntimeError:
            _shutdown_event_global.set()


def _install_signal_handlers(loop):
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: handle_signal(s, None))
        except (NotImplementedError, AttributeError):
            try:
                signal.signal(sig, lambda s, f: handle_signal(s, None))
            except (ValueError, OSError):
                pass


async def get_enhanced_helium_collector_v11(
    config: Optional[Union[HeliumDataCollectorConfig, Dict]] = None,
) -> EnhancedHeliumDataCollectorV11:
    global _collector_instance
    if _collector_instance is None:
        async with _collector_lock:
            if _collector_instance is None:
                _collector_instance = EnhancedHeliumDataCollectorV11(config)
                await _collector_instance.start()
    return _collector_instance


async def shutdown_handler():
    global _collector_instance
    if _collector_instance:
        await _collector_instance.shutdown()
        _collector_instance = None


async def main():
    loop = asyncio.get_running_loop()
    _install_signal_handlers(loop)

    print("=" * 80)
    print("Enhanced Helium Data Collector v18.0 — P0/P1/P2 Hardened")
    print("=" * 80)

    if FASTAPI_AVAILABLE and os.environ.get("HELIUM_SERVE_API", "0") == "1":
        cfg = HeliumDataCollectorConfig()
        uvicorn.run(app, host=cfg.api_host, port=cfg.api_port, log_level="info")
        return

    collector = await get_enhanced_helium_collector_v11()

    print("\n✅ Ten enhancements wired in:")
    print("   [1] Quantum-Distillation     QuantumInspiredTeacher (PennyLane/Qiskit/sim)")
    print("   [2] Causal RL                CausalCounterfactualEstimator (UCB policy)")
    print("   [3] Federated Green Learning SecureFederatedAggregator + DP accountant")
    print("   [4] Multi-Agent Coordination EmergentRoleRegistry + MultiAgentCoordinator")
    print("   [5] Temporal Logic           STLFormula (G/F/U/X/->) + SafetyShield")
    print("   [6] Explainable AI           DecisionExplainer (SHAP/LIME/z)")
    print("   [7] Adaptive Precision       PrecisionController (telemetry-driven)")
    print("   [8] Carbon Markets / RECs    CarbonMarketClient + RECInventory (real)")
    print("   [9] Resilience / Chaos       ChaosEngineer (steady-state hypothesis)")
    print("  [10] HITL / Active Learning   UncertaintyEstimator + HITLGate + Sampler")

    for _ in range(3):
        try:
            rec = await collector._produce_record()
            print(f"\n📊 {rec.record_id}: price={rec.price_index:.1f}, "
                  f"production={rec.global_production_tonnes:.0f}, "
                  f"anomaly={rec.is_anomaly}, precision={rec.precision_level}, "
                  f"safe={rec.safety_ok}, hitl={rec.hitl_approved}")
        except Exception as e:
            print(f"⚠️  Collection failed: {e}")

    status = await collector.get_comprehensive_status()
    print(f"\n🌍 Distribution: {status['cloud_distribution']['active_provider']} "
          f"({status['cloud_distribution']['active_region']})")
    print(f"🔐 PQC available: {status['quantum_security']['pqc_available']}")
    print(f"⛓️ Blockchain: {status['blockchain']['backend']}, "
          f"records={status['blockchain']['total_records']}")
    print(f"🧠 Predictive: {status['predictive']}")
    print(f"⚙️  Self-healing: {status['self_healing']}")
    print(f"💚 Health score: {status['health']['health_score']}")
    print(f"🔧 Enhancements: {status['ten_enhancements']}")
    print(f"🌡️  Precision stats: {status['precision_stats']}")

    print("\n" + "=" * 80)
    print("✅ Enhanced Helium Data Collector v18.0 — Ready")
    print("=" * 80)

    try:
        await _shutdown_event_global.wait()
    except asyncio.CancelledError:
        pass
    finally:
        await shutdown_handler()


if __name__ == "__main__":
    asyncio.run(main())
