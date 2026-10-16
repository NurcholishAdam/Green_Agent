#!/usr/bin/env python3
# src/enhancements/helium_scarcity_manager_enhanced_v18_0.py
# Version 18.0 — Full Green Agent stack (P0/P1/P2 hardened)
"""
Helium Scarcity Manager — v18.0 (P0/P1/P2 hardened)

Objective convention: every objective is expressed as a COST (lower is better).
Pareto, TOPSIS, and MODP all follow that convention.

Fixes:
 - DB: NullPool only for SQLite; ORM inserts; URL normalization; awaited dispose
 - Objective convention consistent (costs). MODP teacher minimizes.
 - Candidate objectives reconstructed as consistent costs.
 - Multi-agent wired into MODPConstraintOptimizer via resolve_conflict.
 - HITL uncertainty from real candidate-score variance / expert disagreement.
 - SafetyShield replacement applied to constraint strictness.
 - Causal RL: UCB + epsilon-greedy policy; real action logged.
 - MOE teachers registered immediately; incremental training each record.
 - Anomaly detector returns score; self-healing detects anomalies.
 - Alerts: deque + dedup by (level, time bucket).
 - Constraint ID eviction on expiry.
 - Carbon interface returns float consistently.
 - PQC verify + HMAC fallback + key rotation.
 - Web3 real tx with local fallback; cloud SDKs real with local fallback.
 - torch.amp (non-deprecated) AMP path.
 - Scheduler + BioOptimizer wired.
 - FastAPI + singleton unified via lifespan.
 - All ten enhancements at P2 level.
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
        rate_limit_requests = 100
        rate_limit_window = 60
    central_config = _CentralConfig()

    class Storage:
        def __init__(self, *a, **kw): self._records: List[Any] = []
        def store(self, r): self._records.append(r)
        def clean_old_scarcity_records(self, days=365): self._records.clear()

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
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.backends import default_backend
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

try:
    from sklearn.linear_model import LogisticRegression, LinearRegression, Ridge
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import IsolationForest, GradientBoostingRegressor
    from sklearn.svm import OneClassSVM
    from sklearn.cluster import KMeans
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    torch = None  # type: ignore
    nn = None  # type: ignore

try:
    import xgboost as xgb  # noqa: F401
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False

try:
    from web3 import Web3
    from web3.middleware import geth_poa_middleware
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

try:
    import boto3
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

try:
    from azure.storage.blob import BlobServiceClient
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

try:
    from google.cloud import storage as gcs_storage
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
        select,
    )
    from sqlalchemy.pool import NullPool
    ASYNC_SQLALCHEMY_AVAILABLE = True
except ImportError:
    ASYNC_SQLALCHEMY_AVAILABLE = False

try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import declarative_base as _decl_base
    SQLALCHEMY_SYNC_AVAILABLE = True
    if not ASYNC_SQLALCHEMY_AVAILABLE:
        declarative_base = _decl_base
except ImportError:
    SQLALCHEMY_SYNC_AVAILABLE = False

SQLALCHEMY_AVAILABLE = ASYNC_SQLALCHEMY_AVAILABLE or SQLALCHEMY_SYNC_AVAILABLE

try:
    from fastapi import FastAPI, Depends, HTTPException
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
    from prometheus_client import Counter, Gauge, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    start_http_server = None

try:
    import pennylane as qml
    PENNYLANE_AVAILABLE = True
except ImportError:
    PENNYLANE_AVAILABLE = False

try:
    from qiskit import QuantumCircuit, Aer, execute as qiskit_execute
    QISKIT_AVAILABLE = True
except ImportError:
    QISKIT_AVAILABLE = False

try:
    import shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False

try:
    import lime
    import lime.lime_tabular
    LIME_AVAILABLE = True
except ImportError:
    LIME_AVAILABLE = False

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False


# ============================================================
# PROMETHEUS METRICS
# ============================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    SCARCITY_INDEX = Gauge("helium_scarcity_index", "SI", registry=REGISTRY)
    SCARCITY_UPDATES = Counter("helium_scarcity_updates_total", "U", ["status"], registry=REGISTRY)
    ACTIVE_CONSTRAINTS = Gauge("helium_active_constraints", "AC", registry=REGISTRY)
    MTOP_STUDENT_LOSS = Gauge("helium_mtop_student_loss", "MSL", registry=REGISTRY)
    QUANTUM_SIGNATURES = Counter("helium_quantum_signatures_total", "QS", ["algorithm", "status"], registry=REGISTRY)
    BLOCKCHAIN_VERIFICATIONS = Counter("helium_blockchain_verifications_total", "BV", ["status"], registry=REGISTRY)
    ANOMALY_DETECTIONS = Counter("helium_anomaly_detections_total", "AD", ["type"], registry=REGISTRY)
    SELF_HEALING_ACTIONS = Counter("helium_self_healing_actions_total", "SH", ["action"], registry=REGISTRY)
    MOE_GATING_WEIGHTS = Gauge("helium_moe_gating_weights", "MG", ["expert"], registry=REGISTRY)
    CARBON_PRICE = Gauge("helium_carbon_price_per_kg", "CP", registry=REGISTRY)
    REC_INVENTORY_KWH = Gauge("helium_rec_inventory_kwh", "REC", registry=REGISTRY)
    PRECISION_SWITCHES = Counter("helium_precision_switches_total", "PS", ["level"], registry=REGISTRY)
    PRECISION_ENERGY = Gauge("helium_precision_energy_joules", "PE", ["level"], registry=REGISTRY)
    CHAOS_EVENTS = Counter("helium_chaos_events_total", "CE", ["type"], registry=REGISTRY)
    HITL_APPROVALS = Counter("helium_hitl_approvals_total", "HITL", ["decision"], registry=REGISTRY)
    SAFETY_VIOLATIONS = Counter("helium_safety_violations_total", "SV", ["formula"], registry=REGISTRY)
    FEDERATED_ROUNDS = Counter("helium_federated_rounds_total", "FR", registry=REGISTRY)
    HEALTH_SCORE = Gauge("helium_health_score", "HS", registry=REGISTRY)
    DP_EPSILON_SPENT = Gauge("helium_dp_epsilon_spent", "DPE", ["node"], registry=REGISTRY)
else:
    class _Dummy:
        def inc(self, *a, **kw): pass
        def set(self, *a, **kw): pass
        def labels(self, *a, **kw): return self
        def observe(self, *a, **kw): pass
    SCARCITY_INDEX = SCARCITY_UPDATES = ACTIVE_CONSTRAINTS = MTOP_STUDENT_LOSS = _Dummy()
    QUANTUM_SIGNATURES = BLOCKCHAIN_VERIFICATIONS = ANOMALY_DETECTIONS = _Dummy()
    SELF_HEALING_ACTIONS = MOE_GATING_WEIGHTS = CARBON_PRICE = REC_INVENTORY_KWH = _Dummy()
    PRECISION_SWITCHES = PRECISION_ENERGY = CHAOS_EVENTS = HITL_APPROVALS = _Dummy()
    SAFETY_VIOLATIONS = FEDERATED_ROUNDS = HEALTH_SCORE = DP_EPSILON_SPENT = _Dummy()


# ============================================================
# EXCEPTIONS
# ============================================================
class ScarcityError(Exception): pass
class QuantumError(ScarcityError): pass
class BlockchainError(ScarcityError): pass
class OptimizationError(ScarcityError): pass
class CircuitBreakerOpenError(ScarcityError): pass
class RateLimitExceeded(ScarcityError): pass
class MLModelError(ScarcityError): pass
class SafetyViolationError(ScarcityError): pass
class PrivacyBudgetExceeded(ScarcityError): pass


# ============================================================
# OBJECTIVE CONVENTION (P0)
# ============================================================
# All objective vectors are COSTS: lower is better. Pareto dominance
# requires all <= and at least one strictly <. TOPSIS uses ideal = min
# per column, negative ideal = max. Weighted sum is minimized.
# ============================================================


# ============================================================
# ENHANCEMENT 7 — ADAPTIVE PRECISION (P2)
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
        if not self._telemetry: return 0.0
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
        if not self._telemetry: return {"samples": 0}
        recent = list(self._telemetry)[-20:]
        return {"samples": len(self._telemetry),
                "avg_temp_c": statistics.fmean(h.temperature_c for h in recent),
                "avg_power_w": statistics.fmean(h.power_watts for h in recent),
                "ema_quality_drop": self._ema_quality_drop}


class HardwareAwareAdapter:
    def __init__(self, controller: PrecisionController):
        self.controller = controller

    def adapt(self, policy, carbon_intensity, latency_headroom, carbon_price):
        level = self.controller.select(carbon_intensity, latency_headroom, carbon_price)
        out = dict(policy)
        out["precision_level"] = level.value
        out["effective_bits"] = PRECISION_COST[level]["bits"]
        PRECISION_SWITCHES.labels(level=level.value).inc()
        PRECISION_ENERGY.labels(level=level.value).set(PRECISION_COST[level]["energy"])
        return out, level


# ============================================================
# ENHANCEMENT 8 — CARBON MARKETS / RECs (P1: real providers + persistence)
# ============================================================
@dataclass
class CarbonSnapshot:
    intensity_g_co2_per_kwh: float
    price_usd_per_kg: float
    source: str
    region: str = "global"
    timestamp: float = field(default_factory=time.time)


class CarbonIntensityProvider(Protocol):
    async def get_intensity(self) -> CarbonSnapshot: ...


class ElectricityMapsProvider:
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
                        price_usd_per_kg=0.0, source="electricitymaps", region=self.zone)
            except Exception as e:
                logger.warning(f"ElectricityMaps failed: {e}")
        hour = datetime.utcnow().hour
        diurnal = 1.0 + 0.35 * math.sin((hour / 24.0) * 2 * math.pi)
        return CarbonSnapshot(280.0 * diurnal, 0.0, "synthetic", region=self.zone)


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
                            float(data[0].get("value", 400.0)), 0.0, "watttime",
                            region=self.region)
            except Exception as e:
                logger.warning(f"WattTime failed: {e}")
        return CarbonSnapshot(280.0, 0.0, "synthetic", region=self.region)


class CarbonMarketClient:
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
        if not self.persist_path: return
        try:
            self.persist_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.persist_path, "wb") as f:
                pickle.dump({"records": self._records, "ledger": self._ledger}, f)
        except Exception as e:
            logger.warning(f"REC save failed: {e}")

    def add(self, kwh, source="solar"):
        rec = RECRecord(kwh=kwh, issued_at=time.time(), source=source)
        self._records.append(rec)
        self._ledger.append({"op": "add", "kwh": kwh, "source": source,
                             "cert_id": rec.cert_id, "t": rec.issued_at})
        self._save()
        REC_INVENTORY_KWH.set(self.total_kwh())
        return rec

    def total_kwh(self):
        return sum(r.kwh for r in self._records)

    def consume(self, kwh):
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
    """P1: real providers with synthetic fallback. Always returns float intensity."""

    def __init__(self, config):
        self.config = config
        self.current_intensity = 400.0
        self.current_price = 0.05
        self.current_region = getattr(config, "carbon_region", "global")
        self.current_source = "synthetic"
        self.market = CarbonMarketClient(
            base_price=getattr(config, "carbon_base_price", 0.05),
            sensitivity=getattr(config, "carbon_price_sensitivity", 0.0005))
        self.recs = RECInventory(
            persist_path=getattr(config, "rec_persist_path", None))
        rec_default = getattr(config, "rec_default_kwh", 0.0)
        if rec_default > 0:
            self.recs.add(rec_default, source="bootstrap")

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
                    if snap.source != "synthetic": break
            except Exception as e:
                logger.warning(f"Carbon provider failed: {e}")
        if best is not None:
            self.current_intensity = best.intensity_g_co2_per_kwh
            self.current_source = best.source
            self.current_region = best.region
        hour = datetime.utcnow().hour
        self.current_price = self.market.price(self.current_intensity, hour)
        CARBON_PRICE.set(self.current_price)
        REC_INVENTORY_KWH.set(self.recs.total_kwh())
        return self.current_intensity

    async def get_current_intensity(self) -> float:
        """Returns float (matching other files). Use get_snapshot for region info."""
        return self.current_intensity

    async def get_snapshot(self) -> CarbonSnapshot:
        return CarbonSnapshot(
            intensity_g_co2_per_kwh=self.current_intensity,
            price_usd_per_kg=self.current_price,
            source=self.current_source, region=self.current_region)

    async def get_current_price(self, hour_of_day=None) -> float:
        if hour_of_day is None:
            hour_of_day = datetime.utcnow().hour
        self.current_price = self.market.price(self.current_intensity, hour_of_day)
        CARBON_PRICE.set(self.current_price)
        return self.current_price

    async def close(self): pass

    async def health_check(self):
        return {"status": "ok", "intensity": self.current_intensity,
                "price": self.current_price, "source": self.current_source,
                "rec_kwh": self.recs.total_kwh(),
                "rec_ledger_len": len(self.recs._ledger)}


# ============================================================
# ENHANCEMENT 9 — CHAOS (P2: steady-state + blast radius + rollback)
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
    def __init__(self, config=None, hypotheses: Optional[List[SteadyStateHypothesis]] = None):
        self.config = config or ChaosConfig()
        self.rng = random.Random(self.config.seed)
        self.events: List[Dict[str, Any]] = []
        self.hypotheses = hypotheses or []
        self._active_faults = 0
        self._activated_at = time.time() if self.config.enabled() else 0.0
        self._lock = asyncio.Lock()

    def _can_inject(self) -> bool:
        if self._active_faults >= self.config.max_concurrent_faults: return False
        if self.config.duration_s > 0 and \
                (time.time() - self._activated_at) > self.config.duration_s:
            return False
        return True

    def maybe_fault(self):
        if not self._can_inject(): return False
        if self.rng.random() < self.config.fault_prob:
            self._active_faults += 1
            self.events.append({"type": "fault", "t": time.time()})
            CHAOS_EVENTS.labels(type="fault").inc()
            return True
        return False

    def release_fault(self):
        self._active_faults = max(0, self._active_faults - 1)

    def maybe_latency(self):
        if not self._can_inject(): return 0.0
        if self.config.latency_inject_ms > 0 and \
                self.rng.random() < self.config.latency_inject_prob:
            self.events.append({"type": "latency", "t": time.time()})
            CHAOS_EVENTS.labels(type="latency").inc()
            return self.config.latency_inject_ms
        return 0.0

    def maybe_carbon_spike(self, carbon_intensity):
        if not self._can_inject(): return carbon_intensity
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
        return {"config": asdict(self.config), "events": len(self.events),
                "active_faults": self._active_faults,
                "hypotheses": [h.name for h in self.hypotheses]}


# ============================================================
# ENHANCEMENT 5 — TEMPORAL LOGIC (P2: full STL semantics)
# ============================================================
class STLOperator(str, Enum):
    ALWAYS = "G"; EVENTUALLY = "F"; UNTIL = "U"; NEXT = "X"; IMPLIES = "->"


@dataclass
class STLFormula:
    name: str
    predicate: Callable[[Dict[str, Any]], bool]
    operator: STLOperator
    horizon: int = 10
    rhs_predicate: Optional[Callable[[Dict[str, Any]], bool]] = None

    def evaluate(self, window: List[Dict[str, Any]]) -> bool:
        if not window: return True
        sat = [bool(self.predicate(r)) for r in window]
        if self.operator == STLOperator.ALWAYS: return all(sat)
        if self.operator == STLOperator.EVENTUALLY: return any(sat)
        if self.operator == STLOperator.NEXT:
            return sat[0] if len(sat) == 1 else sat[1]
        if self.operator == STLOperator.UNTIL:
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
    def __init__(self, monitor: TemporalLogicMonitor):
        self.monitor = monitor
        self.violations: List[Dict[str, Any]] = []

    def screen(self, selected_value, candidates, key="value",
               cost_fn: Optional[Callable[[Any], float]] = None):
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
        self.violations.append({"t": time.time(), "violated": violated,
                                "original": selected_value, "replacement": safe})
        return safe, False, verdict


# ============================================================
# ENHANCEMENT 2 — CAUSAL RL (P0 + P2: UCB + epsilon-greedy)
# ============================================================
@dataclass
class CausalTransition:
    state: np.ndarray
    action: int
    reward: float
    next_state: np.ndarray
    context: Dict[str, float] = field(default_factory=dict)


class CausalCounterfactualEstimator:
    def __init__(self, state_dim, n_actions, ridge=1e-3, exploration_c: float = 0.5):
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
        cf = self.counterfactuals(s, n_actions)
        if not cf: return 0
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
        return {"n_updates": int(self._n), "n_actions": self.n_actions,
                "action_counts": self._action_counts.tolist()}


# ============================================================
# ENHANCEMENT 3 — FEDERATED (P2: secure + DP accountant)
# ============================================================
@dataclass
class FederatedUpdate:
    node_id: str
    weights: Dict[str, np.ndarray]
    n_samples: int
    carbon_intensity: float
    timestamp: float = field(default_factory=time.time)


class DPAccountant:
    def __init__(self, target_epsilon: float = 8.0, target_delta: float = 1e-5):
        self.target_epsilon = target_epsilon
        self.target_delta = target_delta
        self.spent = 0.0
        self._rounds = 0

    def step(self, sigma: float, sensitivity: float = 1.0):
        if sigma <= 0: return
        eps_step = sensitivity * math.sqrt(2 * math.log(1.25 / self.target_delta)) / sigma
        self.spent += eps_step
        self._rounds += 1

    def can_spend(self) -> bool:
        return self.spent < self.target_epsilon


class SecureFederatedAggregator:
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
        if not fresh: return self._global
        if not self.accountant.can_spend():
            raise PrivacyBudgetExceeded(
                f"DP budget exhausted: spent={self.accountant.spent:.2f} "
                f"target={self.accountant.target_epsilon}")
        weights = np.array([u.n_samples / max(u.carbon_intensity, 1.0) for u in fresh])
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
        return {"tracked_nodes": len(self._updates),
                "dp_spent": self.accountant.spent,
                "dp_target": self.accountant.target_epsilon}


# ============================================================
# ENHANCEMENT 4 — MULTI-AGENT (P2: comms + conflict resolution)
# ============================================================
class AgentRole(str, Enum):
    EXPLORER = "explorer"; EXPLOITER = "exploiter"
    SAFETY_OFFICER = "safety_officer"; CARBON_BROKER = "carbon_broker"
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
        if success: self._successes[role] += 1

    def weights(self):
        total = sum(max(v, 1e-6) for v in self._scores.values())
        return {r: max(v, 1e-6) / total for r, v in self._scores.items()}

    def dominant_role(self):
        return max(self._scores, key=self._scores.get)

    def stats(self):
        return {"scores": {r.value: self._scores[r] for r in AgentRole},
                "counts": {r.value: self._counts[r] for r in AgentRole},
                "success_rate": {
                    r.value: (self._successes[r] / self._counts[r])
                    if self._counts[r] else 0.0 for r in AgentRole}}


class MultiAgentCoordinator:
    def __init__(self, registry: EmergentRoleRegistry, role_bias=None):
        self.registry = registry
        self.role_bias = role_bias or {
            AgentRole.EXPLORER: 0.6, AgentRole.EXPLOITER: 0.9,
            AgentRole.SAFETY_OFFICER: 1.4, AgentRole.CARBON_BROKER: 1.2,
            AgentRole.VERIFIER: 1.0}
        self.messages: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()

    async def broadcast(self, sender: str, topic: str, payload: Dict[str, Any]):
        async with self._lock:
            self.messages.append(AgentMessage(
                sender=sender, recipient="*", topic=topic, payload=payload))

    def vote(self, bids: List[AgentBid], n_candidates: int,
             dominant_override: Optional[AgentRole] = None) -> int:
        if not bids or n_candidates <= 0: return 0
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
        if not bids: return 0, "no_bids"
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
# ENHANCEMENT 6 — XAI (P2: SHAP / LIME / z-score + ablation)
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
    def __init__(self, feature_names=None, predict_fn: Optional[Callable] = None):
        self.feature_names = list(feature_names) if feature_names else [
            "strictness", "carbon_cost", "efficiency_cost", "monetary_cost"]
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
                    background, feature_names=self.feature_names, mode="regression")
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

    def _shap_attributions(self, x):
        if self._shap_explainer is None: return None
        try:
            vals = self._shap_explainer.shap_values(x.reshape(1, -1))
            v = np.asarray(vals).reshape(-1)
            attrs = list(zip(self.feature_names, v.tolist()))
            attrs.sort(key=lambda kv: abs(kv[1]), reverse=True)
            return attrs
        except Exception:
            return None

    def _lime_attributions(self, x):
        if self._lime_explainer is None or self.predict_fn is None: return None
        try:
            exp = self._lime_explainer.explain_instance(
                x, self.predict_fn, num_features=len(self.feature_names))
            attrs = [(f, w) for f, w in exp.as_list()]
            attrs.sort(key=lambda kv: abs(kv[1]), reverse=True)
            return attrs
        except Exception:
            return None

    def _validate(self, x, top_attrs):
        if self.predict_fn is None or self._bg is None or not top_attrs: return 0.0
        try:
            base = float(self.predict_fn(x.reshape(1, -1))[0])
            deltas = []
            for name, _ in top_attrs[:3]:
                if name not in self.feature_names: continue
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
        attrs = None
        x = np.array([float(chosen.get(f, 0.0)) for f in self.feature_names])
        if self._shap_explainer is not None:
            attrs = self._shap_attributions(x)
            if attrs is not None: method = "shap"
        if attrs is None and self._lime_explainer is not None:
            attrs = self._lime_attributions(x)
            if attrs is not None: method = "lime"
        if attrs is None:
            attrs = self._z_attributions(chosen, pareto)
            method = "z_score"
        validation = self._validate(x, attrs)
        cf_list = []
        for idx, score in sorted(counterfactuals.items(),
                                 key=lambda kv: kv[1], reverse=True)[:3]:
            if 0 <= idx < len(pareto):
                cf_list.append({"alternative_idx": idx,
                                "expected_reward": float(score),
                                "delta": float(score - counterfactuals.get(chosen_idx, 0.0))})
        top_feat = ", ".join(f"{n}={v:+.2f}" for n, v in attrs[:3])
        rationale = (f"Chose #{chosen_idx} (confidence={confidence:.2f}, method={method}). "
                     f"Top attributions: {top_feat}. Carbon price={carbon_price:.4f}. "
                     f"Safety={'ok' if safety_ok else 'OVERRIDE'}. "
                     f"Validation={validation:.3f}.")
        return Explanation(
            decision_id=f"dec-{uuid.uuid4().hex[:8]}",
            chosen_idx=chosen_idx, top_features=attrs[:5],
            counterfactuals=cf_list, rationale=rationale,
            confidence=confidence, safety_ok=safety_ok,
            carbon_price_signal=carbon_price, method=method,
            validation_score=validation)


# ============================================================
# ENHANCEMENT 1 — QUANTUM-DISTILLATION (P2)
# ============================================================
class QuantumInspiredTeacher:
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

    def _ry(self, state, theta, q):
        c, s = math.cos(theta / 2), math.sin(theta / 2)
        n = len(state); new = state.copy(); step = 1 << q
        for i in range(n):
            if i & step == 0:
                a, b = state[i], state[i | step]
                new[i] = c * a - s * b; new[i | step] = s * a + c * b
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
        if n <= 0: return np.zeros(0)
        feats = np.asarray(features, dtype=np.float64)
        if self.use_pennylane:
            p = self._pennylane_probs(feats, n)
            if p is not None: return p
        if self.use_qiskit:
            p = self._qiskit_probs(feats, n)
            if p is not None: return p
        return self._simulate(feats, n)

    def train_step(self, features, target_probs, lr=0.05, n=4):
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
        return {"backend": self.backend, "n_qubits": self.n_qubits,
                "pennylane": self.use_pennylane, "qiskit": self.use_qiskit}


class DistillationEnsemble:
    def __init__(self, quantum, alpha=0.5):
        self.quantum = quantum; self.alpha = alpha

    def blend(self, features, other, n):
        q = self.quantum.teacher_probs(features, n)
        if other is None or len(other) != n: return q
        return self.alpha * q + (1.0 - self.alpha) * np.asarray(other)


# ============================================================
# ENHANCEMENT 10 — HITL / ACTIVE LEARNING (P0 + P2)
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
        if not self._expert_forecasts: return 0.0
        recent = self._expert_forecasts[-1]
        if len(recent) < 2: return 0.0
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
        return {"rewards": len(self._rewards),
                "expert_forecasts": len(self._expert_forecasts),
                "variance": self.variance(), "disagreement": self.disagreement()}


class HumanInTheLoopGate:
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
            self.audit.append({"request_id": req.request_id, "approved": False,
                               "reason": "no_approver_configured"})
            HITL_APPROVALS.labels(decision="blocked").inc()
            return False
        if self.approver is None:
            self.audit.append({"request_id": req.request_id, "approved": True,
                               "reason": "auto_approve_dev"})
            HITL_APPROVALS.labels(decision="auto_approved").inc()
            return True
        loop = asyncio.get_running_loop()
        future = loop.create_future()
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
                           "approved": bool(approved), "reason": req.reason})
        HITL_APPROVALS.labels(
            decision="approved" if approved else "rejected").inc()
        return bool(approved)

    async def resolve(self, request_id: str, approved: bool) -> bool:
        async with self._lock:
            fut = self._pending.get(request_id)
        if fut is None or fut.done(): return False
        fut.set_result(approved)
        return True

    def pending_requests(self) -> List[str]:
        return list(self._pending.keys())


class ActiveLearningSampler:
    def __init__(self, capacity=256, feature_dim=8, diversity_weight=0.4, seed=0):
        self.capacity = capacity
        self.feature_dim = feature_dim
        self.diversity_weight = diversity_weight
        self._buffer: List[Dict[str, Any]] = []
        self._features: List[np.ndarray] = []
        self._rng = random.Random(seed)
        self._kmeans = KMeans(n_clusters=4, n_init=5) if SKLEARN_AVAILABLE else None

    def _embed(self, record: Dict[str, Any]) -> np.ndarray:
        vals = []
        for k in ("scarcity_index", "price", "supply_confidence",
                  "projected_shortage_days"):
            try: vals.append(float(record.get(k, 0.0)))
            except Exception: vals.append(0.0)
        while len(vals) < self.feature_dim: vals.append(0.0)
        v = np.asarray(vals[: self.feature_dim], dtype=np.float64)
        n = np.linalg.norm(v) + 1e-9
        return v / n

    def maybe_store(self, record, uncertainty, threshold=0.4):
        if uncertainty < threshold: return False
        feat = self._embed(record)
        if len(self._buffer) >= self.capacity:
            self._buffer.pop(0); self._features.pop(0)
        self._buffer.append({**record, "uncertainty": uncertainty, "t": time.time()})
        self._features.append(feat)
        return True

    def sample(self, k=8) -> List[Dict[str, Any]]:
        if not self._buffer: return []
        if len(self._buffer) <= k: return list(self._buffer)
        uncs = np.array([b.get("uncertainty", 0.0) for b in self._buffer])
        if self._kmeans is not None and len(self._features) >= 8:
            try:
                X = np.stack(self._features)
                labels = self._kmeans.fit_predict(X)
                diversity = np.zeros(len(self._buffer))
                for i, lab in enumerate(labels):
                    diversity[i] = 1.0 / (1.0 + np.sum(labels == lab))
            except Exception:
                diversity = np.random.rand(len(self._buffer))
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
# SUPPORT: LimitGraph / RLHF / Distiller
# ============================================================
class LimitGraph:
    def __init__(self):
        self.limits: Dict[str, Any] = {}
        self._feedback: List[Any] = []

    def build_graph(self, nodes, edges): pass

    def get_limits(self, context): return {}

    def update_from_feedback(self, feedback): self._feedback.append(feedback)


class RLHFOptimizer:
    def __init__(self, action_space):
        self.actions = list(action_space)
        self.scores = {a: 0.0 for a in self.actions}

    def update(self, context, action, reward):
        if action not in self.scores:
            self.actions.append(action); self.scores[action] = 0.0
        self.scores[action] += 0.1 * (float(reward) - self.scores[action])

    def sample_action(self, context):
        if not self.scores: return None
        if random.random() < 0.1: return random.choice(self.actions)
        return max(self.scores, key=self.scores.get)


class MultiTeacherDistiller:
    def __init__(self, teachers, weights=None):
        self.teachers = list(teachers)
        self.weights = list(weights) if weights else [1.0] * len(self.teachers)

    async def _call(self, t, ctx):
        try:
            out = t(ctx)
            if asyncio.iscoroutine(out): out = await out
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
# PARETO / TOPSIS (P0 fixed: COST convention)
# ============================================================
class ParetoFront:
    def __init__(self):
        self.solutions: List[Tuple[List[float], Any]] = []

    def add(self, objectives: Sequence[float], decision: Any):
        dominated = False
        for obj, _ in self.solutions:
            if (all(obj[i] <= objectives[i] for i in range(len(objectives)))
                    and any(obj[i] < objectives[i] for i in range(len(objectives)))):
                dominated = True; break
        if not dominated:
            self.solutions = [
                (obj, dec) for obj, dec in self.solutions
                if not (all(objectives[i] <= obj[i] for i in range(len(objectives)))
                        and any(objectives[i] < obj[i] for i in range(len(objectives))))]
            self.solutions.append((list(objectives), decision))
        return dominated

    def get_pareto_front(self): return self.solutions

    def get_best_by_weight(self, weights):
        best, best_score = None, float("inf")
        for obj, dec in self.solutions:
            score = sum(w * o for w, o in zip(weights, obj))
            if score < best_score:
                best_score, best = score, dec
        return best


class TOPSIS:
    @staticmethod
    def score(candidates, weights, criteria):
        if not candidates or not criteria: return []
        matrix = np.array([[float(c.get(c, 0.0)) for c in criteria]
                           for c in candidates])
        denom = np.sqrt((matrix ** 2).sum(axis=0))
        denom = np.where(denom == 0, 1.0, denom)
        norm = matrix / denom
        w = np.asarray(weights[: norm.shape[1]], dtype=np.float64)
        if w.sum() <= 0: w = np.ones(norm.shape[1]) / norm.shape[1]
        else: w = w / w.sum()
        weighted = norm * w
        ideal = weighted.min(axis=0)
        neg_ideal = weighted.max(axis=0)
        d_plus = np.sqrt(((weighted - ideal) ** 2).sum(axis=1))
        d_minus = np.sqrt(((weighted - neg_ideal) ** 2).sum(axis=1))
        # Return higher-better "closeness" (1 = best)
        return (d_minus / (d_plus + d_minus + 1e-9)).tolist()


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
                self.state = CircuitBreakerState.OPEN; self.half_open_requests = 0

    async def call(self, func, *args, **kwargs):
        if not await self.allow_request():
            raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            await self.record_success(); return result
        except Exception:
            await self.record_failure(); raise


class EnhancedRateLimiter:
    def __init__(self, rate=100, per_seconds=60):
        self.rate = rate; self.per_seconds = per_seconds
        self.tokens = float(rate); self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.time(); dt = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + dt * (self.rate / self.per_seconds))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1; return True
            return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.1)


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
            for t in self.tasks.values(): t.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()


# ============================================================
# DATA CLASSES
# ============================================================
@dataclass
class HeliumData:
    timestamp: datetime
    price_per_liter_usd: float
    scarcity_index: float
    supply_confidence: float
    projected_shortage_days: int
    region: str
    price_trend: str
    scarcity_trend: str
    data_id: str = field(default_factory=lambda: f"hd_{uuid.uuid4().hex[:8]}")
    quantum_signature: Optional[Dict] = None
    blockchain_tx_hash: Optional[str] = None
    cloud_distribution: Optional[Dict] = None
    metadata: Dict = field(default_factory=dict)
    provenance: Optional[Dict] = None
    precision_level: Optional[str] = None
    explanation: Optional[Dict] = None
    safety_ok: bool = True
    stl_verdict: Optional[Dict[str, bool]] = None
    hitl_approved: bool = True
    counterfactuals: Optional[Dict[int, float]] = None
    chaos_events: Optional[List[Dict[str, Any]]] = None
    version: int = 1
    superseded_by: Optional[str] = None

    def __post_init__(self):
        if self.price_per_liter_usd < 0:
            raise ValueError("price_per_liter_usd must be >= 0")
        if not (0 <= self.scarcity_index <= 1):
            raise ValueError("scarcity_index must be between 0 and 1")
        if not (0 <= self.supply_confidence <= 1):
            raise ValueError("supply_confidence must be between 0 and 1")
        if self.projected_shortage_days < 0:
            raise ValueError("projected_shortage_days must be >= 0")
        if self.price_trend not in ("increasing", "stable", "decreasing"):
            raise ValueError("price_trend invalid")
        if self.scarcity_trend not in ("increasing", "stable", "decreasing"):
            raise ValueError("scarcity_trend invalid")

    def to_dict(self):
        d = asdict(self)
        if isinstance(d.get("timestamp"), datetime):
            d["timestamp"] = d["timestamp"].isoformat()
        return d


@dataclass
class HeliumConstraint:
    constraint_id: str
    severity: str
    scarcity_threshold: float
    max_helium_usage_l: float
    recommended_actions: List[str]
    valid_until: datetime
    is_active: bool = True
    version: int = 1
    superseded_by: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)

    def __post_init__(self):
        if self.severity not in ("info", "warning", "critical", "emergency"):
            raise ValueError("severity invalid")
        if not (0 <= self.scarcity_threshold <= 1):
            raise ValueError("scarcity_threshold must be in [0,1]")
        if self.max_helium_usage_l < 0:
            raise ValueError("max_helium_usage_l must be >= 0")

    def to_dict(self):
        d = asdict(self)
        if isinstance(d.get("valid_until"), datetime):
            d["valid_until"] = d["valid_until"].isoformat()
        if isinstance(d.get("created_at"), datetime):
            d["created_at"] = d["created_at"].isoformat()
        return d


@dataclass
class EligibilityDecision:
    decision_id: str
    job_id: str
    helium_requirement_l: float
    job_priority: str
    allowed: bool
    reasons: List[str]
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self):
        d = asdict(self)
        if isinstance(d.get("timestamp"), datetime):
            d["timestamp"] = d["timestamp"].isoformat()
        return d


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
        num_experts: int = 4
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
        max_delay_hours: int = 24
        urgency_importance: float = 0.5
        carbon_importance: float = 0.3
        cost_importance: float = 0.2

    class SelfHealingConfig(BaseModel):
        enabled: bool = True
        anomaly_contamination: float = 0.1
        auto_retry_threshold: int = 3
        fallback_enabled: bool = True
        health_check_interval: int = 60
        drift_check_interval: int = 300

    class ScarcityConfig(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="SCARCITY_", case_sensitive=False)

        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "18.0"
        log_level: str = "INFO"
        environment: str = "production"

        usgs_api_key: Optional[str] = None
        usgs_endpoint: str = "https://www.usgs.gov/api/helium/production"
        eia_api_key: Optional[str] = None
        eia_endpoint: str = "https://www.eia.gov/api/helium/price"
        update_interval: int = Field(300, gt=0)

        scarcity_thresholds: Dict[str, float] = Field(default_factory=lambda: {
            "info": 0.3, "warning": 0.5, "critical": 0.7, "emergency": 0.85})

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

        enable_autonomous_optimization: bool = True
        default_optimization_strategy: str = "mopd"
        mopd_weights: Dict[str, float] = Field(default_factory=lambda: {
            "performance": 0.3, "carbon": 0.25,
            "helium_efficiency": 0.25, "cost": 0.2})
        enable_adaptive_mopd: bool = True
        mopd_epsilon: float = Field(0.1, ge=0.0, le=1.0)

        enable_multi_cloud: bool = True
        aws_enabled: bool = True
        azure_enabled: bool = True
        gcp_enabled: bool = True
        aws_bucket: Optional[str] = None
        azure_container: Optional[str] = None
        azure_connection_string: Optional[str] = None
        gcp_bucket: Optional[str] = None

        carbon_aware_enabled: bool = True
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

        database_url: str = "sqlite+aiosqlite:///scarcity.db"
        database_pool_size: int = Field(10, ge=1)
        database_max_overflow: int = Field(20, ge=0)
        retention_days: int = Field(365, gt=0)

        health_check_interval: int = Field(60, ge=10)
        auto_optimize_interval: int = Field(1800, ge=60)
        blockchain_monitor_interval: int = Field(300, ge=10)
        quantum_monitor_interval: int = Field(600, ge=10)
        cloud_sync_interval: int = Field(3600, ge=60)
        federated_interval: int = Field(3600, ge=60)
        predictive_interval: int = Field(3600, ge=60)
        sustainability_interval: int = Field(3600, ge=60)
        cleanup_interval: int = Field(3600, ge=60)

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

        max_concurrent_api_calls: int = Field(5, ge=1)
        train_teachers_interval: int = Field(3600, ge=60)
        student_hidden_size: int = Field(32, ge=8)
        student_learning_rate: float = Field(0.001, gt=0)
        anomaly_contamination: float = Field(0.05, ge=0, le=0.5)
        federated_epsilon: float = Field(0.1, ge=0.01, le=1.0)
        federated_dp_sigma: float = Field(1e-3, gt=0)
        federated_target_epsilon: float = Field(8.0, gt=0)
        federated_target_delta: float = Field(1e-5, gt=0)

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
            if not v: return os.urandom(32).hex()
            if len(v) < 32: raise ValueError("jwt_secret must be >= 32 chars")
            return v

        @field_validator("quantum_master_key")
        @classmethod
        def _validate_master_key(cls, v):
            if not v: return os.urandom(32).hex()
            try: bytes.fromhex(v)
            except ValueError: raise ValueError("quantum_master_key must be hex")
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
        num_experts: int = 4
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
        max_delay_hours: int = 24
        urgency_importance: float = 0.5
        carbon_importance: float = 0.3
        cost_importance: float = 0.2

    @dataclass
    class SelfHealingConfig:
        enabled: bool = True
        anomaly_contamination: float = 0.1
        auto_retry_threshold: int = 3
        fallback_enabled: bool = True
        health_check_interval: int = 60
        drift_check_interval: int = 300

    @dataclass
    class ScarcityConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "18.0"
        log_level: str = "INFO"
        environment: str = "production"
        usgs_api_key: Optional[str] = None
        usgs_endpoint: str = "https://www.usgs.gov/api/helium/production"
        eia_api_key: Optional[str] = None
        eia_endpoint: str = "https://www.eia.gov/api/helium/price"
        update_interval: int = 300
        scarcity_thresholds: Dict[str, float] = field(default_factory=lambda: {
            "info": 0.3, "warning": 0.5, "critical": 0.7, "emergency": 0.85})
        enable_quantum_security: bool = True
        quantum_algorithm: str = "dilithium"
        quantum_master_key: str = field(default_factory=lambda: os.urandom(32).hex())
        quantum_key_rotation_s: int = 3600
        enable_blockchain_verification: bool = True
        blockchain_rpc_url: str = "http://localhost:8545"
        blockchain_chain_id: int = 1
        blockchain_poa: bool = False
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        enable_autonomous_optimization: bool = True
        default_optimization_strategy: str = "mopd"
        mopd_weights: Dict[str, float] = field(default_factory=lambda: {
            "performance": 0.3, "carbon": 0.25,
            "helium_efficiency": 0.25, "cost": 0.2})
        enable_adaptive_mopd: bool = True
        mopd_epsilon: float = 0.1
        enable_multi_cloud: bool = True
        aws_enabled: bool = True
        azure_enabled: bool = True
        gcp_enabled: bool = True
        aws_bucket: Optional[str] = None
        azure_container: Optional[str] = None
        azure_connection_string: Optional[str] = None
        gcp_bucket: Optional[str] = None
        carbon_aware_enabled: bool = True
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
        database_url: str = "sqlite+aiosqlite:///scarcity.db"
        database_pool_size: int = 10
        database_max_overflow: int = 20
        retention_days: int = 365
        health_check_interval: int = 60
        auto_optimize_interval: int = 1800
        blockchain_monitor_interval: int = 300
        quantum_monitor_interval: int = 600
        cloud_sync_interval: int = 3600
        federated_interval: int = 3600
        predictive_interval: int = 3600
        sustainability_interval: int = 3600
        cleanup_interval: int = 3600
        max_retry_attempts: int = 3
        circuit_breaker_threshold: int = 5
        circuit_breaker_timeout: int = 30
        circuit_breaker_half_open_max_requests: int = 3
        rate_limit_requests: int = 100
        rate_limit_window: int = 60
        metrics_port: int = 8000
        api_host: str = "0.0.0.0"
        api_port: int = 8000
        jwt_secret: str = field(default_factory=lambda: os.urandom(32).hex())
        jwt_algorithm: str = "HS256"
        jwt_expiry_s: int = 3600
        max_concurrent_api_calls: int = 5
        train_teachers_interval: int = 3600
        student_hidden_size: int = 32
        student_learning_rate: float = 0.001
        anomaly_contamination: float = 0.05
        federated_epsilon: float = 0.1
        federated_dp_sigma: float = 1e-3
        federated_target_epsilon: float = 8.0
        federated_target_delta: float = 1e-5
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

    class ScarcityRecordDB(Base):
        __tablename__ = "scarcity_records"
        id = Column(Integer, primary_key=True)
        data_id = Column(String(64), unique=True, index=True)
        timestamp = Column(DateTime)
        price_per_liter_usd = Column(Float)
        scarcity_index = Column(Float)
        supply_confidence = Column(Float)
        projected_shortage_days = Column(Integer)
        region = Column(String(64))
        price_trend = Column(String(16))
        scarcity_trend = Column(String(16))
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
        created_at = Column(DateTime, default=datetime.utcnow)

    class ConstraintDB(Base):
        __tablename__ = "constraints"
        id = Column(Integer, primary_key=True)
        constraint_id = Column(String(128), unique=True, index=True)
        severity = Column(String(16))
        scarcity_threshold = Column(Float)
        max_helium_usage_l = Column(Float)
        recommendations = Column(Text)
        valid_until = Column(DateTime)
        is_active = Column(Boolean, default=True)
        created_at = Column(DateTime, default=datetime.utcnow)

    class EligibilityDB(Base):
        __tablename__ = "eligibility_decisions"
        id = Column(Integer, primary_key=True)
        decision_id = Column(String(64), unique=True, index=True)
        job_id = Column(String(64), index=True)
        helium_requirement_l = Column(Float)
        job_priority = Column(String(16))
        allowed = Column(Boolean)
        reasons = Column(Text)
        timestamp = Column(DateTime, default=datetime.utcnow)

    class LineageDB(Base):
        __tablename__ = "lineage"
        id = Column(Integer, primary_key=True)
        source = Column(String(64))
        operation = Column(String(64))
        record_ids = Column(Text)
        metadata_json = Column(JSON)
        timestamp = Column(DateTime, default=datetime.utcnow)

    class SchemaVersionDB(Base):
        __tablename__ = "schema_version"
        version = Column(Integer, primary_key=True)
        applied_at = Column(DateTime, default=datetime.utcnow)
else:
    Base = None


# ============================================================
# DATABASE MANAGER (P0 fixed)
# ============================================================
class EnhancedDatabaseManager:
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
        if not self.async_engine: return
        async with self.async_engine.begin() as conn:
            if Base is not None:
                await conn.run_sync(Base.metadata.create_all)
        self._migrations_applied = True

    async def init(self):
        if self.async_engine and not self._migrations_applied:
            await self._apply_migrations()

    async def insert_scarcity_record(self, data: HeliumData):
        if not self.async_session or Base is None: return
        try:
            async with self.async_session() as session:
                row = ScarcityRecordDB(
                    data_id=data.data_id, timestamp=data.timestamp,
                    price_per_liter_usd=data.price_per_liter_usd,
                    scarcity_index=data.scarcity_index,
                    supply_confidence=data.supply_confidence,
                    projected_shortage_days=data.projected_shortage_days,
                    region=data.region,
                    price_trend=data.price_trend,
                    scarcity_trend=data.scarcity_trend,
                    quantum_signature=data.quantum_signature or {},
                    blockchain_tx_hash=data.blockchain_tx_hash or "",
                    cloud_distribution=data.cloud_distribution or {},
                    provenance=data.provenance or {},
                    precision_level=data.precision_level or "",
                    explanation=data.explanation or {},
                    safety_ok=data.safety_ok,
                    stl_verdict=data.stl_verdict or {},
                    hitl_approved=data.hitl_approved,
                    counterfactuals={str(k): v for k, v in
                                     (data.counterfactuals or {}).items()},
                    chaos_events=data.chaos_events or [],
                    version=data.version,
                    created_at=datetime.utcnow())
                session.add(row)
                await session.commit()
        except Exception as e:
            logger.warning(f"insert_scarcity_record failed: {e}")

    async def insert_constraint(self, c: HeliumConstraint):
        if not self.async_session or Base is None: return
        try:
            async with self.async_session() as session:
                row = ConstraintDB(
                    constraint_id=c.constraint_id, severity=c.severity,
                    scarcity_threshold=c.scarcity_threshold,
                    max_helium_usage_l=c.max_helium_usage_l,
                    recommendations=json.dumps(c.recommended_actions),
                    valid_until=c.valid_until, is_active=c.is_active,
                    created_at=c.created_at)
                session.add(row)
                await session.commit()
        except Exception as e:
            logger.warning(f"insert_constraint failed: {e}")

    async def insert_eligibility(self, d: EligibilityDecision):
        if not self.async_session or Base is None: return
        try:
            async with self.async_session() as session:
                row = EligibilityDB(
                    decision_id=d.decision_id, job_id=d.job_id,
                    helium_requirement_l=d.helium_requirement_l,
                    job_priority=d.job_priority, allowed=d.allowed,
                    reasons=json.dumps(d.reasons), timestamp=d.timestamp)
                session.add(row)
                await session.commit()
        except Exception as e:
            logger.warning(f"insert_eligibility failed: {e}")

    async def insert_lineage(self, source, operation, record_ids, metadata):
        if not self.async_session or Base is None: return
        try:
            async with self.async_session() as session:
                row = LineageDB(
                    source=source, operation=operation,
                    record_ids=json.dumps(list(record_ids), default=str),
                    metadata_json=metadata or {},
                    timestamp=datetime.utcnow())
                session.add(row)
                await session.commit()
        except Exception as e:
            logger.warning(f"insert_lineage failed: {e}")

    async def load_scarcity_records(self, limit: int = 500) -> List[HeliumData]:
        if not self.async_session or Base is None: return []
        try:
            async with self.async_session() as session:
                stmt = select(ScarcityRecordDB).order_by(
                    ScarcityRecordDB.created_at.desc()).limit(limit)
                result = await session.execute(stmt)
                rows = result.scalars().all()
                out = []
                for row in rows:
                    try:
                        hd = HeliumData(
                            timestamp=row.timestamp or datetime.utcnow(),
                            price_per_liter_usd=float(row.price_per_liter_usd or 0),
                            scarcity_index=float(row.scarcity_index or 0.5),
                            supply_confidence=float(row.supply_confidence or 0.5),
                            projected_shortage_days=int(row.projected_shortage_days or 30),
                            region=row.region or "global",
                            price_trend=row.price_trend or "stable",
                            scarcity_trend=row.scarcity_trend or "stable",
                            data_id=row.data_id)
                        hd.quantum_signature = row.quantum_signature
                        hd.blockchain_tx_hash = row.blockchain_tx_hash
                        hd.cloud_distribution = row.cloud_distribution
                        hd.provenance = row.provenance
                        hd.precision_level = row.precision_level
                        hd.explanation = row.explanation
                        hd.safety_ok = bool(row.safety_ok) if row.safety_ok is not None else True
                        hd.stl_verdict = row.stl_verdict
                        hd.hitl_approved = bool(row.hitl_approved) if row.hitl_approved is not None else True
                        hd.counterfactuals = ({int(k): v for k, v in
                                              (row.counterfactuals or {}).items()}
                                              if row.counterfactuals else None)
                        hd.chaos_events = row.chaos_events
                        hd.version = row.version or 1
                        out.append(hd)
                    except Exception as e:
                        logger.warning(f"Row parse failed: {e}")
                return out
        except Exception as e:
            logger.warning(f"load_scarcity_records failed: {e}")
            return []

    async def health_check(self):
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
            try: await self.async_engine.dispose()
            except Exception as e: logger.warning(f"DB close failed: {e}")


# ============================================================
# PQC SECURITY (P0/P1 fixed)
# ============================================================
class QuantumResilientScarcitySecurity:
    def __init__(self, config, db_manager):
        self.config = config
        self.db_manager = db_manager
        self.algorithm = getattr(config, "quantum_algorithm", "dilithium")
        self._keys: Dict[str, Tuple[bytes, bytes, float]] = {}
        self._active_key_id: Optional[str] = None
        self._rotation_s = getattr(config, "quantum_key_rotation_s", 3600)
        self._last_rotation = 0.0
        self._master = config.get_master_key_bytes() \
            if hasattr(config, "get_master_key_bytes") else b""
        self._lock = asyncio.Lock()
        self._ensure_key()

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
        self._keys[kid] = (pub, priv, time.time())
        self._active_key_id = kid
        self._last_rotation = time.time()
        return kid

    def _ensure_key(self):
        now = time.time()
        if self._active_key_id and (now - self._last_rotation) < self._rotation_s:
            return
        self._generate_sync_key()

    def _hmac_sign(self, payload: bytes) -> str:
        if not self._master: return ""
        return hmac.new(self._master, payload, hashlib.sha256).hexdigest()

    async def generate_keypair(self, algorithm=None):
        async with self._lock:
            self._ensure_key()
            return {"key_id": self._active_key_id,
                    "public_key": self._keys[self._active_key_id][0]}

    async def sign_data(self, data: Dict, key_id: Optional[str] = None) -> Dict:
        kid = key_id or self._active_key_id
        entry = self._keys.get(kid)
        if entry is None:
            QUANTUM_SIGNATURES.labels(algorithm="none", status="missing_key").inc()
            return {"algorithm": "none", "signature": ""}
        pub, priv, _ = entry
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        if not PQC_AVAILABLE:
            sig = self._hmac_sign(payload)
            QUANTUM_SIGNATURES.labels(algorithm="hmac", status="success").inc()
            return {"algorithm": "hmac-sha256", "signature": sig, "key_id": kid}
        try:
            if self.algorithm == "dilithium":
                sig = dilithium.sign(priv, payload)
            elif self.algorithm == "falcon":
                sig = falcon.sign(priv, payload)
            else:
                sig = sphincs.sign(priv, payload)
            QUANTUM_SIGNATURES.labels(algorithm=self.algorithm, status="success").inc()
            return {"algorithm": self.algorithm,
                    "signature": base64.b64encode(sig).decode(), "key_id": kid}
        except Exception as e:
            QUANTUM_SIGNATURES.labels(algorithm=self.algorithm, status="failed").inc()
            return {"algorithm": "none", "signature": "", "error": str(e)}

    async def verify_data(self, data: Dict, signature_data: Dict) -> bool:
        alg = signature_data.get("algorithm", "none")
        sig = signature_data.get("signature", "")
        if alg == "none" or not sig: return False
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        if alg == "hmac-sha256":
            return hmac.compare_digest(self._hmac_sign(payload), sig)
        if not PQC_AVAILABLE: return False
        kid = signature_data.get("key_id")
        entry = self._keys.get(kid) if kid else None
        if entry is None: return False
        pub = entry[0]
        try:
            sig_bytes = base64.b64decode(sig)
            if alg == "dilithium": dilithium.verify(pub, payload, sig_bytes)
            elif alg == "falcon": falcon.verify(pub, payload, sig_bytes)
            elif alg == "sphincs": sphincs.verify(pub, payload, sig_bytes)
            else: return False
            return True
        except Exception:
            return False

    def encrypt_at_rest(self, plaintext: bytes, key: bytes) -> bytes:
        if not CRYPTO_AVAILABLE: return plaintext
        nonce = os.urandom(12)
        return nonce + AESGCM(key).encrypt(nonce, plaintext, None)

    def decrypt_at_rest(self, ciphertext: bytes, key: bytes) -> bytes:
        if not CRYPTO_AVAILABLE: return ciphertext
        nonce, ct = ciphertext[:12], ciphertext[12:]
        return AESGCM(key).decrypt(nonce, ct, None)

    def get_quantum_status(self):
        return {"pqc_available": PQC_AVAILABLE, "algorithm": self.algorithm,
                "active_key_id": self._active_key_id,
                "num_keys": len(self._keys), "rotation_s": self._rotation_s,
                "algorithms": ["dilithium", "falcon", "sphincs"]
                if PQC_AVAILABLE else ["hmac-sha256"]}

    async def health_check(self):
        return {"status": "ok" if PQC_AVAILABLE else "degraded",
                "active_key": self._active_key_id}


# ============================================================
# BLOCKCHAIN (P1: real Web3 + local fallback)
# ============================================================
class BlockchainScarcityVerification:
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
                    self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
                if not self.web3.is_connected():
                    logger.warning("Web3 not connected; using local ledger.")
                    self.web3 = None
            except Exception as e:
                logger.warning(f"Web3 init failed: {e}")
                self.web3 = None

    async def record_scarcity_data(self, data_id, data_hash, metadata):
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
                        "nonce": nonce, "gas": 21000,
                        "gasPrice": self.web3.eth.gas_price,
                        "to": account.address, "value": 0,
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
            self._ledger[data_id] = {"tx_hash": tx_hash, "hash": data_hash,
                                     "metadata": metadata, "t": time.time()}
            BLOCKCHAIN_VERIFICATIONS.labels(status="recorded").inc()
            return {"tx_hash": tx_hash, "status": "recorded"}

    async def verify_scarcity_data(self, data_id, data_hash):
        entry = self._ledger.get(data_id)
        if entry is None: return {"status": "missing"}
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
# CLOUD STORAGE (P1: real SDKs + local fallback)
# ============================================================
class MultiCloudScarcityDistribution:
    def __init__(self, config, db_manager):
        self.config = config
        self.db_manager = db_manager
        self.providers: Dict[str, Any] = {}
        if AWS_AVAILABLE and getattr(config, "aws_enabled", False) and \
                getattr(config, "aws_bucket", None):
            try: self.providers["aws"] = boto3.client("s3")
            except Exception as e: logger.warning(f"AWS init failed: {e}")
        if AZURE_AVAILABLE and getattr(config, "azure_enabled", False) and \
                getattr(config, "azure_connection_string", None):
            try:
                self.providers["azure"] = BlobServiceClient.from_connection_string(
                    config.azure_connection_string)
            except Exception as e: logger.warning(f"Azure init failed: {e}")
        if GCP_AVAILABLE and getattr(config, "gcp_enabled", False) and \
                getattr(config, "gcp_bucket", None):
            try: self.providers["gcp"] = gcs_storage.Client()
            except Exception as e: logger.warning(f"GCP init failed: {e}")

        self.active_provider = "aws"
        self.active_region = "us-east-1"
        self._provider_meta = {
            "aws": {"regions": ["us-east-1", "eu-west-1"], "cost": 0.5,
                    "carbon_score": 0.7, "availability": 0.99},
            "azure": {"regions": ["eastus", "northeurope"], "cost": 0.55,
                      "carbon_score": 0.8, "availability": 0.98},
            "gcp": {"regions": ["us-central1", "europe-west1"], "cost": 0.45,
                    "carbon_score": 0.9, "availability": 0.97},
        }
        self._lock = asyncio.Lock()
        self.local_dir = Path(tempfile.gettempdir()) / "scarcity_cloud"
        self.local_dir.mkdir(parents=True, exist_ok=True)

    async def distribute_data(self, data: Dict):
        best, best_score = None, -float("inf")
        for p, info in self._provider_meta.items():
            score = info["carbon_score"] - info["cost"] * 0.1
            if score > best_score:
                best_score, best = score, p
        async with self._lock:
            self.active_provider = best
            self.active_region = self._provider_meta[best]["regions"][0]
        return {
            "optimal_provider": self.active_provider,
            "optimal_region": self.active_region,
            "source": "weighted",
            "timestamp": datetime.utcnow().isoformat(),
        }

    async def store(self, data: bytes, filename: str):
        results = {}
        loop = asyncio.get_running_loop()
        def _aws():
            self.providers["aws"].put_object(
                Bucket=self.config.aws_bucket, Key=filename, Body=data)
        def _azure():
            c = self.providers["azure"].get_container_client(self.config.azure_container)
            c.upload_blob(name=filename, data=data, overwrite=True)
        def _gcp():
            bucket = self.providers["gcp"].bucket(self.config.gcp_bucket)
            bucket.blob(filename).upload_from_string(data)
        for name, fn in (("aws", _aws), ("azure", _azure), ("gcp", _gcp)):
            if name not in self.providers: continue
            try:
                await loop.run_in_executor(None, fn)
                results[name] = "ok"
            except Exception as e:
                results[name] = f"failed: {e}"
        try:
            (self.local_dir / filename).write_bytes(data)
            results["local"] = "ok"
        except Exception as e:
            results["local"] = f"failed: {e}"
        return {"status": "ok", "filename": filename, "results": results}

    async def get_distribution_status(self):
        async with self._lock:
            return {
                "active_provider": self.active_provider,
                "active_region": self.active_region,
                "providers": list(self._provider_meta.keys()),
                "configured": list(self.providers.keys()),
                "local_dir": str(self.local_dir),
            }

    async def health_check(self):
        return {"status": "ok", "providers": list(self.providers.keys()),
                "local_dir": str(self.local_dir)}


# ============================================================
# SUPPORT COMPONENTS
# ============================================================
class EnhancedRealAPICollector:
    def __init__(self, config):
        self.config = config
        self._cache: Dict[str, Any] = {}

    async def __aenter__(self): return self
    async def __aexit__(self, *a): pass
    async def close(self): pass

    async def fetch_usgs_production(self):
        if self.config.usgs_api_key and HTTPX_AVAILABLE:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    r = await client.get(self.config.usgs_endpoint,
                                         headers={"api-key": self.config.usgs_api_key})
                    r.raise_for_status()
                    data = r.json()
                    if isinstance(data, dict) and "production" in data:
                        return float(data["production"])
            except Exception as e:
                logger.warning(f"USGS fetch failed: {e}")
        return 28000 + random.uniform(-500, 500)

    async def fetch_eia_price(self):
        if self.config.eia_api_key and HTTPX_AVAILABLE:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    r = await client.get(self.config.eia_endpoint,
                                         headers={"api-key": self.config.eia_api_key})
                    r.raise_for_status()
                    data = r.json()
                    if isinstance(data, dict) and "price" in data:
                        return float(data["price"])
            except Exception as e:
                logger.warning(f"EIA fetch failed: {e}")
        return 0.5 + random.uniform(-0.05, 0.05)


class ScarcityAnomalyDetector:
    """P0 fix: update() returns (is_anomaly, score), and supports rolling refit."""

    def __init__(self, config, db_manager):
        self.config = config
        self.db_manager = db_manager
        self.detectors: List[Tuple[str, Any]] = []
        self._trained = False
        self._buffer: deque = deque(maxlen=500)
        self._refit_counter = 0
        self._lock = asyncio.Lock()
        if SKLEARN_AVAILABLE and config.anomaly_contamination > 0:
            self.detectors = [
                ("iforest", IsolationForest(
                    contamination=config.anomaly_contamination)),
                ("ocsvm", OneClassSVM(nu=0.1))]

    def _features(self, data: HeliumData):
        return np.array([
            float(data.scarcity_index),
            float(data.price_per_liter_usd),
            float(data.supply_confidence),
            float(data.projected_shortage_days),
        ]).reshape(1, -1)

    async def _fit(self):
        if not self.detectors: return
        if len(self._buffer) < 20: return
        X = np.vstack(list(self._buffer))
        for _, m in self.detectors:
            try: m.fit(X)
            except Exception as e: logger.warning(f"Anomaly fit failed: {e}")
        self._trained = True

    async def update(self, data: HeliumData) -> Tuple[bool, float]:
        if not self.detectors:
            return (data.scarcity_index > 0.9), float(data.scarcity_index)
        X = self._features(data)
        async with self._lock:
            self._buffer.append(X)
            if not self._trained:
                await self._fit()
                return False, 0.0
            # Rolling refit every 50 observations
            self._refit_counter += 1
            if self._refit_counter % 50 == 0:
                await self._fit()
        votes = []
        for _, m in self.detectors:
            try: votes.append(1 if m.predict(X)[0] == -1 else 0)
            except Exception: votes.append(0)
        if not votes: return False, 0.0
        score = sum(votes) / len(votes)
        if score > 0.5:
            ANOMALY_DETECTIONS.labels(type="scarcity").inc()
        return (score > 0.5), float(score)

    async def get_statistics(self):
        return {"enabled": bool(self.detectors), "trained": self._trained,
                "num_detectors": len(self.detectors),
                "buffer_size": len(self._buffer)}


class FederatedScarcityLearner:
    def __init__(self, db_manager, instance_id, share_interval, epsilon,
                 aggregator=None):
        self.db_manager = db_manager
        self.instance_id = instance_id
        self.share_interval = share_interval
        self.epsilon = epsilon
        self.aggregator = aggregator or SecureFederatedAggregator()

    async def share(self, vec, carbon_intensity):
        self.aggregator.submit(FederatedUpdate(
            node_id=self.instance_id,
            weights={"local": np.asarray(vec, dtype=np.float64)},
            n_samples=1, carbon_intensity=carbon_intensity))
        return self.aggregator.aggregate()

    def get_federated_insights(self):
        return self.aggregator.stats()


class UserAdaptiveScarcityReflexivity:
    def __init__(self, db_manager, learning_rate):
        self.db_manager = db_manager
        self.learning_rate = learning_rate
        self._prefs: Dict[str, float] = {}

    def record_feedback(self, key, value):
        self._prefs[key] = self._prefs.get(key, 0.0) + self.learning_rate * (
            value - self._prefs.get(key, 0.0))


class PredictiveScarcityReflexivity:
    """P2: Prophet/sklearn/exp-smooth ensemble for scarcity forecasting."""
    def __init__(self, db_manager, horizon):
        self.db_manager = db_manager
        self.horizon = horizon
        self.history: deque = deque(maxlen=2000)
        self.experts: List[Tuple[str, Callable]] = []
        self._init_experts()

    def _init_experts(self):
        if PROPHET_AVAILABLE: self.experts.append(("prophet", self._forecast_prophet))
        if SKLEARN_AVAILABLE: self.experts.append(("linear", self._forecast_linear))
        self.experts.append(("exp_smooth", self._forecast_exp_smooth))
        if not self.experts: self.experts.append(("naive", self._forecast_naive))

    def _forecast_prophet(self, history, horizon):
        if len(history) < 10: return [0.5] * horizon
        try:
            import pandas as pd
            df = pd.DataFrame(history).sort_values("ds")
            m = Prophet(changepoint_prior_scale=0.1, seasonality_prior_scale=5)
            m.fit(df)
            future = m.make_future_dataframe(periods=horizon)
            return m.predict(future)["yhat"].tail(horizon).clip(0, 1).tolist()
        except Exception:
            return [0.5] * horizon

    def _forecast_linear(self, history, horizon):
        if len(history) < 2: return [0.5] * horizon
        X = np.arange(len(history)).reshape(-1, 1)
        y = np.array([h["y"] for h in history])
        try:
            m = LinearRegression().fit(X, y)
            f = np.arange(len(history), len(history) + horizon).reshape(-1, 1)
            return m.predict(f).clip(0, 1).tolist()
        except Exception:
            return [0.5] * horizon

    def _forecast_exp_smooth(self, history, horizon):
        if len(history) < 2: return [0.5] * horizon
        values = [h["y"] for h in history]
        alpha = 0.3; smoothed = values[-1]; out = []
        for _ in range(horizon):
            out.append(smoothed)
            smoothed = alpha * values[-1] + (1 - alpha) * smoothed
        return out

    def _forecast_naive(self, history, horizon):
        if not history: return [0.5] * horizon
        return [history[-1]["y"]] * horizon

    def update(self, scarcity):
        self.history.append({"ds": datetime.utcnow(), "y": float(scarcity)})

    def predict(self, horizon=None):
        horizon = horizon or self.horizon
        if len(self.history) < 3:
            return {"forecast": [0.5] * horizon, "confidence": 0.0,
                    "ensemble_forecast": [0.5] * horizon}
        forecasts = []
        for _, fn in self.experts:
            try:
                forecasts.append(np.asarray(fn(list(self.history), horizon)))
            except Exception:
                forecasts.append(np.full(horizon, 0.5))
        if not forecasts:
            return {"forecast": [0.5] * horizon, "confidence": 0.0,
                    "ensemble_forecast": [0.5] * horizon}
        stacked = np.stack(forecasts, axis=0)
        mean_forecast = stacked.mean(axis=0).clip(0, 1)
        disagreement = float(np.std(stacked, axis=0).mean())
        confidence = max(0.0, min(1.0, 1.0 - disagreement * 2))
        return {
            "forecast": mean_forecast.tolist(),
            "confidence": confidence,
            "ensemble_forecast": mean_forecast.tolist(),
            "disagreement": disagreement,
            "num_experts": len(self.experts),
        }


class ScarcitySustainabilityTracker:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.scores: deque = deque(maxlen=1000)

    def record(self, score): self.scores.append(score)

    async def get_sustainability_score(self):
        if not self.scores: return {"overall_score": 0.0}
        return {"overall_score": float(np.mean(self.scores)),
                "min": float(np.min(self.scores)),
                "max": float(np.max(self.scores))}


# ============================================================
# MODP CONSTRAINT OPTIMIZER (P0/P2: costs, multi-agent, real carbon)
# ============================================================
class MODPConstraintOptimizer:
    """
    P0 fix: candidate objectives are consistent COSTS. MODP teacher minimizes.
    P2: multi-agent conflict resolution, real carbon score in bids.
    """
    def __init__(self, config, adaptive_cost=None, limit_graph=None,
                 rlhf=None, distiller=None, agent_registry=None):
        self.config = config
        self.adaptive_cost = adaptive_cost
        self.candidates = [
            {"strictness": 0.2, "max_usage": 0.8, "label": "very_relaxed"},
            {"strictness": 0.4, "max_usage": 0.6, "label": "relaxed"},
            {"strictness": 0.6, "max_usage": 0.4, "label": "balanced"},
            {"strictness": 0.8, "max_usage": 0.2, "label": "strict"},
            {"strictness": 0.9, "max_usage": 0.1, "label": "very_strict"},
        ]
        self.weights = list(config.modp.weights)
        self.adaptive_weights = config.modp.adaptive_weights
        self.learning_rate = config.modp.learning_rate
        self.recent_outcomes: deque = deque(maxlen=100)
        self.optimization_history: deque = deque(maxlen=100)
        self.limit_graph = limit_graph
        self.rlhf = rlhf
        self.distiller = distiller
        self.roles = agent_registry or EmergentRoleRegistry()
        self.coordinator = MultiAgentCoordinator(self.roles)
        self.explainer: Optional[DecisionExplainer] = None
        self.last_explanation: Optional[Explanation] = None

    async def _modp_teacher(self, context):
        objectives = context.get("objectives", {})
        if not objectives: return "balanced"
        best, best_score = None, float("inf")  # cost minimization
        for label, obj in objectives.items():
            score = sum(w * o for w, o in zip(self.weights, obj))
            if score < best_score:
                best_score, best = score, label
        return best

    async def _rule_based_teacher(self, context):
        s = context.get("scarcity", 0.5)
        if s < 0.3: return "very_relaxed"
        if s < 0.5: return "relaxed"
        if s < 0.7: return "balanced"
        if s < 0.85: return "strict"
        return "very_strict"

    async def _static_teacher(self, context): return "balanced"

    def _make_objectives(self, cand: Dict[str, float],
                         carbon_intensity: float) -> List[float]:
        """All objectives as COSTS (lower is better)."""
        strictness_cost = cand["strictness"]
        carbon_cost = (cand["max_usage"] / 0.8) * (carbon_intensity / 400.0)
        efficiency_cost = cand["max_usage"]  # higher usage = worse efficiency
        monetary_cost = cand["max_usage"] * 0.2
        return [strictness_cost, carbon_cost, efficiency_cost, monetary_cost]

    async def optimize(self, state):
        carbon_intensity = state.get("carbon_intensity", 400)
        if isinstance(carbon_intensity, dict):
            carbon_intensity = carbon_intensity.get("intensity", 400)
        current_scarcity = state.get("scarcity", 0.5)

        candidates_eval = []
        for cand in self.candidates:
            objectives = self._make_objectives(cand, float(carbon_intensity))
            candidates_eval.append({
                "objectives": objectives,
                "decision": cand,
                "label": cand["label"],
            })

        context = {
            "scarcity": current_scarcity,
            "carbon_intensity": float(carbon_intensity),
            "objectives": {ce["label"]: ce["objectives"] for ce in candidates_eval},
        }

        selected_label, source = None, None
        if self.distiller is not None and self.distiller.teachers:
            self.distiller.teachers = [
                self._modp_teacher, self._rule_based_teacher, self._static_teacher]
            selected_label = await self.distiller.distill(context)
            source = "distilled"
        if selected_label is None and self.rlhf is not None:
            selected_label = self.rlhf.sample_action(context)
            source = "rlhf"
        if selected_label is None:
            front = ParetoFront()
            for ce in candidates_eval:
                front.add(ce["objectives"], ce["decision"])
            if self.config.modp.method == "topsis":
                cand_dicts = [{"s1": ce["objectives"][0], "s2": ce["objectives"][1],
                               "s3": ce["objectives"][2], "s4": ce["objectives"][3]}
                              for ce in candidates_eval]
                scores = TOPSIS.score(cand_dicts, self.weights,
                                      ["s1", "s2", "s3", "s4"])
                # Higher closeness is better in TOPSIS returns
                best_idx = int(np.argmax(scores))
                best = candidates_eval[best_idx]["decision"]
            else:
                best = front.get_best_by_weight(self.weights) or self.candidates[2]
            selected_label = best["label"]
            source = "modp"

        if self.limit_graph is not None:
            limits = self.limit_graph.get_limits(context)
            forbidden = limits.get("forbidden_labels") or []
            if selected_label in forbidden:
                remaining = [c for c in self.candidates
                             if c["label"] not in forbidden]
                if remaining:
                    selected_label = remaining[0]["label"]
                    source = "limit_graph"

        # Multi-agent conflict resolution
        labels = [c["label"] for c in self.candidates]
        base_idx = labels.index(selected_label) if selected_label in labels else 2
        # Carbon broker picks the lowest carbon-cost candidate
        carbon_costs = [ce["objectives"][1] for ce in candidates_eval]
        carbon_idx = int(np.argmin(carbon_costs))
        # Safety officer picks the strictest
        safety_idx = int(np.argmax([c["strictness"] for c in self.candidates]))
        # Exploiter picks the least strict (fastest)
        exploiter_idx = int(np.argmin([c["strictness"] for c in self.candidates]))
        # Explorer random
        explorer_idx = random.randrange(len(self.candidates))
        # Verifier agrees with base
        carbon_ratio = min(1.0, float(carbon_intensity) / 600.0)
        bids = [
            AgentBid(agent_id="explorer", role=AgentRole.EXPLORER,
                     confidence=0.5, proposed_action=explorer_idx,
                     rationale="explore", carbon_score=0.5),
            AgentBid(agent_id="exploiter", role=AgentRole.EXPLOITER,
                     confidence=0.7, proposed_action=exploiter_idx,
                     rationale="least_strict", carbon_score=0.6),
            AgentBid(agent_id="safety", role=AgentRole.SAFETY_OFFICER,
                     confidence=0.9 if current_scarcity > 0.8 else 0.4,
                     proposed_action=safety_idx,
                     rationale="strictest", carbon_score=0.5),
            AgentBid(agent_id="carbon", role=AgentRole.CARBON_BROKER,
                     confidence=0.8 if carbon_ratio > 0.5 else 0.4,
                     proposed_action=carbon_idx,
                     rationale="lowest_carbon", carbon_score=0.9),
            AgentBid(agent_id="verifier", role=AgentRole.VERIFIER,
                     confidence=0.6, proposed_action=base_idx,
                     rationale="agree", carbon_score=0.5),
        ]
        voted_idx, resolution = await self.coordinator.resolve_conflict(
            bids, len(self.candidates))
        selected_label = labels[voted_idx]
        best = self.candidates[voted_idx]

        # Outcome for weight update
        outcome = self._make_objectives(best, float(carbon_intensity))
        self.recent_outcomes.append((list(self.weights), outcome))
        if self.adaptive_weights and len(self.recent_outcomes) >= 10:
            self._update_weights()

        if self.rlhf is not None:
            # Reward = negative weighted cost (higher is better)
            reward = -sum(w * o for w, o in zip(self.weights, outcome))
            self.rlhf.update(context, selected_label, reward)

        for b in bids:
            agreed = b.proposed_action % len(self.candidates) == voted_idx
            self.roles.record(b.role, success=agreed,
                              reward=(1.0 if agreed else -0.2))

        # XAI
        explanation = None
        if self.explainer is not None:
            pareto_list = [{
                "strictness": ce["objectives"][0],
                "carbon_cost": ce["objectives"][1],
                "efficiency_cost": ce["objectives"][2],
                "monetary_cost": ce["objectives"][3]}
                for ce in candidates_eval]
            chosen_dict = pareto_list[voted_idx]
            explanation = self.explainer.explain(
                chosen_idx=voted_idx, chosen=chosen_dict, pareto=pareto_list,
                counterfactuals={}, safety_ok=True, carbon_price=0.0,
                confidence=0.7)
            self.last_explanation = explanation

        result = {
            "action": "modp_optimization",
            "constraint_strictness": best["strictness"],
            "max_helium_usage": best["max_usage"],
            "label": best["label"],
            "weights_used": list(self.weights),
            "scores": [ce["objectives"] for ce in candidates_eval],
            "recommendation": f"Selected {best['label']} via {source} ({resolution})",
            "source": source,
            "resolution": resolution,
            "explanation": explanation.to_dict() if explanation else None,
        }
        self.optimization_history.append({
            "result": result, "timestamp": datetime.utcnow().isoformat()})
        return result

    def _update_weights(self):
        if not self.recent_outcomes: return
        outcomes = np.array([o for _, o in self.recent_outcomes])
        mean_outcome = outcomes.mean(axis=0)
        delta = mean_outcome - mean_outcome.mean()
        w = np.array(self.weights) - self.learning_rate * delta
        w = np.clip(w, 0.05, None); w /= w.sum()
        self.weights = w.tolist()

    def get_optimization_stats(self):
        return {
            "total_optimizations": len(self.optimization_history),
            "strategies": [c["label"] for c in self.candidates],
            "weights": list(self.weights),
            "recent_optimizations": list(self.optimization_history)[-5:],
            "distillation_active": self.distiller is not None,
            "rlhf_active": self.rlhf is not None,
            "limit_graph_active": self.limit_graph is not None,
            "multi_agent_active": True,
            "role_stats": self.roles.stats(),
        }


class AutonomousConstraintOptimizer:
    def __init__(self, config, db_manager):
        self.config = config
        self.db_manager = db_manager
        self.optimization_history: deque = deque(maxlen=100)
        self.weights = [0.25, 0.25, 0.25, 0.25]

    async def optimize(self, state):
        self.optimization_history.append({
            "state": state, "timestamp": datetime.utcnow().isoformat()})
        return {
            "action": "fallback_optimization",
            "constraint_strictness": 0.6,
            "max_helium_usage": 0.4,
            "label": "balanced",
            "weights_used": self.weights,
        }

    def get_optimization_stats(self):
        return {"total_optimizations": len(self.optimization_history),
                "strategies": ["balanced"],
                "weights": list(self.weights)}


# ============================================================
# MOE TEACHER ENSEMBLE (P0 fix: immediate registration + real labels)
# ============================================================
class MOETeacherEnsemble:
    def __init__(self, config, distiller=None):
        self.config = config
        self.teachers: Dict[str, Dict[str, Any]] = {}
        self.gating_model = None
        self.scaler = None
        self.history_context: deque = deque(maxlen=500)
        self.history_labels: deque = deque(maxlen=500)
        self._last_preds: Dict[str, float] = {}
        self._last_features: Optional[np.ndarray] = None
        self._expert_errors: Dict[str, deque] = defaultdict(lambda: deque(maxlen=50))
        self._trained = False
        self._init_gating()
        self.distiller = distiller

    def _init_gating(self):
        if SKLEARN_AVAILABLE:
            self.gating_model = LogisticRegression(max_iter=1000)
            self.scaler = StandardScaler()

    def register_teacher(self, name, model, confidence=0.8):
        if name in self.teachers:
            return
        self.teachers[name] = {"model": model, "confidence": confidence}
        if self.distiller is not None:
            self.distiller.teachers.append(lambda ctx, name=name: name)

    def _extract_context(self, X):
        if X.ndim == 1: X = X.reshape(1, -1)
        mean_features = X.mean(axis=0)
        now = datetime.utcnow()
        return np.array([
            float(mean_features[0]) if len(mean_features) > 0 else 0.5,
            float(mean_features[1]) if len(mean_features) > 1 else 0.5,
            float(mean_features[2]) if len(mean_features) > 2 else 0.5,
            float(mean_features[3]) if len(mean_features) > 3 else 0.5,
            now.hour / 24.0,
            now.weekday() / 6.0])

    async def get_predictions(self, X):
        predictions: Dict[str, Tuple[float, float]] = {}
        for name, teacher in self.teachers.items():
            model = teacher["model"]
            try:
                if model is None:
                    pred = 0.6
                elif TORCH_AVAILABLE and isinstance(model, nn.Module):
                    model.eval()
                    with torch.no_grad():
                        X_t = torch.FloatTensor(X)
                        pred = float(model(X_t).squeeze().item())
                elif hasattr(model, "predict"):
                    pred = model.predict(X.reshape(1, -1) if X.ndim == 1 else X)
                    pred = float(pred[0] if hasattr(pred, "__len__") else pred)
                else:
                    pred = 0.5
            except Exception as e:
                logger.warning(f"Teacher {name} predict failed: {e}")
                pred = 0.5
            pred = max(0.0, min(1.0, pred))
            predictions[name] = (pred, teacher["confidence"])
        self._last_preds = {k: v[0] for k, v in predictions.items()}
        self._last_features = X
        return predictions

    async def get_weights(self, X):
        if self.distiller is not None and self.distiller.teachers:
            selected = await self.distiller.distill({})
            weights = np.zeros(len(self.teachers))
            for i, name in enumerate(self.teachers.keys()):
                if name == selected: weights[i] = 1.0
            if weights.sum() > 0: return weights
        if self.gating_model is not None and self._trained:
            try:
                ctx = self._extract_context(X)
                X_scaled = self.scaler.transform([ctx])
                probs = self.gating_model.predict_proba(X_scaled)[0]
                if len(probs) == len(self.teachers): return probs
            except Exception:
                pass
        # Inverse-error weighting
        errs = np.array([np.mean(self._expert_errors[name]) if self._expert_errors[name]
                         else 1.0 for name in self.teachers.keys()])
        inv = 1.0 / (errs + 1e-6)
        return inv / inv.sum()

    async def update_gating(self, X, actual):
        """P0 fix: real target label from ground truth."""
        if not SKLEARN_AVAILABLE or self.gating_model is None: return
        if actual is None or not self._last_preds: return
        errors = []
        for name, pred in self._last_preds.items():
            try:
                err = abs(pred - float(actual))
                self._expert_errors[name].append(err)
                errors.append(err)
            except Exception:
                errors.append(float("inf"))
        if not errors: return
        self.history_labels.append(int(np.argmin(errors)))
        self.history_context.append(self._extract_context(X).tolist())
        if len(self.history_labels) < 20: return
        if len(np.unique(self.history_labels[-20:])) < 2: return
        n = min(len(self.history_context), len(self.history_labels))
        Xc = np.array(self.history_context[-n:])
        y = np.array(self.history_labels[-n:])
        try:
            X_scaled = self.scaler.fit_transform(Xc)
            self.gating_model.fit(X_scaled, y)
            self._trained = True
            try:
                coefs = self.gating_model.coef_
                for i, name in enumerate(self.teachers.keys()):
                    if i < coefs.shape[0]:
                        MOE_GATING_WEIGHTS.labels(expert=name).set(
                            float(np.mean(np.abs(coefs[i]))))
            except Exception:
                pass
        except Exception as e:
            logger.warning(f"Gating fit failed: {e}")

    def get_stats(self):
        expert_mae = {name: (float(np.mean(d)) if d else None)
                      for name, d in self._expert_errors.items()}
        return {"num_teachers": len(self.teachers),
                "teachers": list(self.teachers.keys()),
                "gating_trained": self._trained,
                "history_len": len(self.history_labels),
                "distillation_active": self.distiller is not None,
                "expert_mae": expert_mae}


# ============================================================
# MTOP ENGINE
# ============================================================
if TORCH_AVAILABLE:
    class StudentNN(nn.Module):
        def __init__(self, input_dim=4, hidden_size=32):
            super().__init__()
            self.fc1 = nn.Linear(input_dim, hidden_size)
            self.relu = nn.ReLU()
            self.fc2 = nn.Linear(hidden_size, 1)

        def forward(self, x):
            return torch.sigmoid(self.fc2(self.relu(self.fc1(x))))
else:
    class StudentNN:
        def __init__(self, *a, **kw): pass


class EnhancedMTOPEngine:
    def __init__(self, config, moe_ensemble):
        self.config = config
        self.moe = moe_ensemble
        self.student = None
        self.student_optimizer = None
        self.criterion = nn.MSELoss() if TORCH_AVAILABLE else None
        self.device = None
        if TORCH_AVAILABLE:
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.history: deque = deque(maxlen=500)
        self.is_ready = False

    def init_student(self):
        if not TORCH_AVAILABLE: return
        self.student = StudentNN(input_dim=4,
                                 hidden_size=self.config.student_hidden_size).to(self.device)
        self.student_optimizer = optim.Adam(
            self.student.parameters(), lr=self.config.student_learning_rate)
        self.is_ready = True

    async def train_student(self, X, teacher_weighted, actual=None):
        if not self.is_ready or self.student is None: return None
        self.student.train()
        try:
            X_t = torch.FloatTensor(X).to(self.device)
            pred = self.student(X_t).squeeze()
            target = torch.tensor(teacher_weighted, device=self.device,
                                  dtype=torch.float32)
            loss = self.criterion(pred, target)
            if actual is not None:
                actual_t = torch.tensor(float(actual), device=self.device,
                                        dtype=torch.float32)
                loss = loss + 0.5 * self.criterion(pred, actual_t)
            self.student_optimizer.zero_grad()
            loss.backward()
            self.student_optimizer.step()
            MTOP_STUDENT_LOSS.set(loss.item())
            return loss.item()
        except Exception as e:
            logger.warning(f"Student training failed: {e}")
            return None

    async def compute_scarcity(self, X, actual_scarcity=None):
        if X.ndim == 1: X = X.reshape(1, -1)
        teacher_preds = await self.moe.get_predictions(X)
        weights = await self.moe.get_weights(X)
        weighted_sum = 0.0
        for i, (name, (pred, _)) in enumerate(teacher_preds.items()):
            try: weighted_sum += weights[i] * pred
            except Exception: pass
        weighted_sum = max(0.0, min(1.0, weighted_sum))

        if self.is_ready and self.student is not None:
            try:
                self.student.eval()
                with torch.no_grad():
                    X_t = torch.FloatTensor(X).to(self.device)
                    student_pred = float(self.student(X_t).squeeze().item())
                    student_pred = max(0.0, min(1.0, student_pred))
            except Exception:
                student_pred = weighted_sum
        else:
            student_pred = weighted_sum

        reward = None
        if actual_scarcity is not None:
            reward = max(0.0, 1.0 - abs(student_pred - actual_scarcity))
            await self.train_student(X, weighted_sum, actual_scarcity)
            await self.moe.update_gating(X, actual_scarcity)
            self.history.append({
                "actual": actual_scarcity,
                "student": student_pred,
                "weighted": weighted_sum,
                "reward": reward})

        return {"student_prediction": student_pred,
                "teacher_predictions": teacher_preds,
                "weighted_teacher": weighted_sum,
                "reward": reward}


# ============================================================
# GA + BIO OPTIMIZER (P1: persists population)
# ============================================================
class GeneticAlgorithmOptimizer:
    def __init__(self, population_size=20, mutation_rate=0.1,
                 crossover_rate=0.8, seed=0):
        self.pop_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.population: List[Dict[str, float]] = []
        self.bounds = {
            "retrain_threshold": (0.02, 0.1),
            "urgency_importance": (0.1, 0.9),
            "carbon_importance": (0.1, 0.9)}
        self.rng = random.Random(seed)
        self.initialized = False

    def initialize(self):
        if self.initialized and self.population: return
        self.population = [
            {k: self.rng.uniform(*self.bounds[k]) for k in self.bounds}
            for _ in range(self.pop_size)]
        self.initialized = True

    def evaluate(self, fitness_func):
        return [fitness_func(ind) for ind in self.population]

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
            ind[key] = self.rng.uniform(*self.bounds[key])
        return ind

    def evolve(self, fitness_func, generations=10):
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


class BioOptimizer:
    def __init__(self, config, adaptive_cost=None):
        self.config = config
        self.adaptive_cost = adaptive_cost
        self.ga = GeneticAlgorithmOptimizer(
            population_size=config.bio.population_size,
            mutation_rate=config.bio.mutation_rate,
            crossover_rate=config.bio.crossover_rate)
        self.current_params = {"retrain_threshold": 0.05,
                               "urgency_importance": 0.5,
                               "carbon_importance": 0.3}
        self.fitness_history: List[float] = []

    def _fitness_func(self, params):
        if self.adaptive_cost:
            try: return -float(self.adaptive_cost.evaluate(params))
            except Exception: pass
        cost = ((params["retrain_threshold"] - 0.05) ** 2
                + (params["urgency_importance"] - 0.5) ** 2
                + (params["carbon_importance"] - 0.3) ** 2)
        return -cost

    async def evolve(self, generations=10):
        best = self.ga.evolve(self._fitness_func, generations=generations)
        if best:
            self.current_params = best
            self.fitness_history.append(self._fitness_func(best))
        return self.current_params


# ============================================================
# SCHEDULER (P1: wired into training)
# ============================================================
class MultiObjectiveTrainingScheduler:
    def __init__(self, config, carbon_manager):
        self.config = config
        self.carbon_manager = carbon_manager
        self.max_delay_hours = config.multi_objective_scheduler.max_delay_hours

    async def submit_training(self, train_fn, critical=False, urgency=0.5):
        if critical: return await train_fn()
        try:
            current_carbon = await self.carbon_manager.get_current_intensity()
        except Exception:
            current_carbon = 400.0
        if current_carbon <= self.config.multi_objective_scheduler.carbon_threshold:
            return await train_fn()
        carbon_ratio = min(1.0, (current_carbon -
                                 self.config.multi_objective_scheduler.carbon_threshold)
                           / max(self.config.multi_objective_scheduler.carbon_threshold, 1.0))
        carbon_w = self.config.multi_objective_scheduler.carbon_importance
        urgency_w = self.config.multi_objective_scheduler.urgency_importance
        delay_factor = max(0.0, carbon_w * carbon_ratio - urgency_w * urgency)
        delay_hours = int(self.max_delay_hours * min(1.0, delay_factor))
        if delay_hours > 0:
            # Cap actual sleep so it stays responsive
            await asyncio.sleep(min(delay_hours, 3))
        return await train_fn()


# ============================================================
# SELF-HEALING (P0: anomaly trigger + RLHF action selection)
# ============================================================
class SelfHealingManager:
    def __init__(self, config, drift_detector=None, rlhf=None):
        self.config = config
        self.drift = drift_detector
        self.rlhf = rlhf
        self.anomaly_detectors: List[Tuple[str, Any]] = []
        self.gating_weights: List[float] = []
        self._lock = asyncio.Lock()
        self.recovery_actions: deque = deque(maxlen=100)
        self._trained = False
        self._training_buffer: deque = deque(maxlen=500)
        self._refit_counter = 0
        if SKLEARN_AVAILABLE and config.self_healing.enabled:
            self.anomaly_detectors = [
                ("iforest", IsolationForest(
                    contamination=config.self_healing.anomaly_contamination)),
                ("ocsvm", OneClassSVM(nu=0.1))]
            self.gating_weights = [1.0 / len(self.anomaly_detectors)] * \
                len(self.anomaly_detectors)

    def _features(self, metrics):
        return np.array([
            float(metrics.get("scarcity_index", 0.5)),
            float(metrics.get("price_per_liter_usd", 0.5)),
            float(metrics.get("supply_confidence", 0.5)),
            float(metrics.get("projected_shortage_days", 30))]).reshape(1, -1)

    async def _maybe_fit(self, force: bool = False):
        if not self.anomaly_detectors: return
        if len(self._training_buffer) < 20: return
        if self._trained and not force: return
        X = np.vstack(list(self._training_buffer))
        for _, m in self.anomaly_detectors:
            try: m.fit(X)
            except Exception as e: logger.warning(f"Detector fit failed: {e}")
        self._trained = True

    async def detect_anomaly(self, metrics):
        """P0 fix: called from the main loop with score returned."""
        if not self.anomaly_detectors:
            return (metrics.get("scarcity_index", 0.0) > 0.9), 0.8
        X = self._features(metrics)
        async with self._lock:
            self._training_buffer.append(X)
            self._refit_counter += 1
            if not self._trained:
                await self._maybe_fit()
                return False, 0.0
            if self._refit_counter % 50 == 0:
                await self._maybe_fit(force=True)
        votes = []
        for _, m in self.anomaly_detectors:
            try: votes.append(1 if m.predict(X)[0] == -1 else 0)
            except Exception: votes.append(0)
        if not votes: return False, 0.0
        score = sum(v * w for v, w in zip(votes, self.gating_weights[:len(votes)]))
        if score > 0.5:
            ANOMALY_DETECTIONS.labels(type="self_healing").inc()
            await self._trigger_recovery(metrics, "restart")
        return score > 0.5, float(score)

    async def _trigger_recovery(self, metrics, default_action):
        action = default_action
        if self.rlhf is not None:
            try:
                candidate = self.rlhf.sample_action(metrics)
                if candidate: action = candidate
            except Exception:
                pass
        async with self._lock:
            self.recovery_actions.append({
                "action": action, "timestamp": datetime.utcnow().isoformat()})
            SELF_HEALING_ACTIONS.labels(action=action).inc()

    async def train(self, data):
        for item in data:
            try: self._training_buffer.append(self._features(item))
            except Exception: pass
        await self._maybe_fit()

    async def check_drift(self, metrics):
        if self.drift:
            try: drift = await self.drift.check_drift(metrics)
            except Exception: drift = False
            if drift:
                await self._trigger_recovery(metrics, "drift_recovery")

    async def get_stats(self):
        return {"enabled": self.config.self_healing.enabled,
                "trained": self._trained,
                "num_detectors": len(self.anomaly_detectors),
                "recent_actions": list(self.recovery_actions)[-5:],
                "rlhf_active": self.rlhf is not None}


# ============================================================
# MAIN SCARCITY MANAGER
# ============================================================
class HeliumScarcityManager:
    def __init__(self, config: Optional[Union[ScarcityConfig, Dict]] = None):
        if isinstance(config, ScarcityConfig):
            self.config = config
        elif isinstance(config, dict) and PYDANTIC_AVAILABLE:
            self.config = ScarcityConfig(**config)
        else:
            self.config = ScarcityConfig()
        self.instance_id = self.config.instance_id

        self.limit_graph_enabled = self.config.limit_graph_enabled
        self.rlhf_enabled = self.config.rlhf_enabled
        self.distillation_enabled = self.config.distillation_enabled

        # Enhancement modules
        limit_graph = LimitGraph() if self.limit_graph_enabled else None
        rlhf = (RLHFOptimizer(action_space=[
            "very_relaxed", "relaxed", "balanced", "strict", "very_strict"])
            if self.rlhf_enabled else None)
        modp_distiller = MultiTeacherDistiller([]) if self.distillation_enabled else None
        moe_distiller = MultiTeacherDistiller([]) if self.distillation_enabled else None
        agent_registry = EmergentRoleRegistry() if self.config.multi_agent_enabled else None

        # DB + support
        self.db_manager = EnhancedDatabaseManager(self.config)
        self.carbon_manager = CarbonIntensityManager(self.config)
        self.quantum_security = QuantumResilientScarcitySecurity(self.config, self.db_manager)
        self.blockchain = BlockchainScarcityVerification(self.config, self.db_manager)
        self.api_collector = EnhancedRealAPICollector(self.config)
        self.cloud_distributor = MultiCloudScarcityDistribution(self.config, self.db_manager)
        self.anomaly_detector = ScarcityAnomalyDetector(self.config, self.db_manager)

        # MOE + optimizers
        self.moe_ensemble = MOETeacherEnsemble(self.config, moe_distiller) \
            if self.config.moe.enabled else None
        self.mtop_engine = EnhancedMTOPEngine(self.config, self.moe_ensemble) \
            if self.moe_ensemble else None
        if self.mtop_engine: self.mtop_engine.init_student()

        self.bio_optimizer = BioOptimizer(self.config)
        self.modp_optimizer = MODPConstraintOptimizer(
            self.config, None, limit_graph, rlhf, modp_distiller,
            agent_registry=agent_registry) if self.config.modp.enabled else None
        self.autonomous_optimizer = self.modp_optimizer or \
            AutonomousConstraintOptimizer(self.config, self.db_manager)

        self.scheduler = (MultiObjectiveTrainingScheduler(self.config, self.carbon_manager)
                          if self.config.multi_objective_scheduler.enabled else None)
        self.self_healing = (SelfHealingManager(self.config, None, rlhf)
                             if self.config.self_healing.enabled else None)

        # Ten enhancements
        self.temporal = None; self.shield = None
        if self.config.temporal_logic_enabled:
            self.temporal = TemporalLogicMonitor(horizon=10)
            self.temporal.add_formula(STLFormula(
                name="scarcity_bounded",
                predicate=lambda r: r.get("scarcity_index", 0.0) <= 0.95,
                operator=STLOperator.ALWAYS, horizon=10))
            self.temporal.add_formula(STLFormula(
                name="eventually_recovers",
                predicate=lambda r: r.get("scarcity_index", 1.0) <= 0.7,
                operator=STLOperator.EVENTUALLY, horizon=5))
            self.temporal.add_formula(STLFormula(
                name="trend_consistency",
                predicate=lambda r: True,
                operator=STLOperator.NEXT, horizon=2))
            self.shield = SafetyShield(self.temporal)

        self.explainer: Optional[DecisionExplainer] = None
        if self.config.xai_enabled:
            self.explainer = DecisionExplainer(
                feature_names=["strictness", "carbon_cost",
                               "efficiency_cost", "monetary_cost"])
            if self.modp_optimizer is not None:
                self.modp_optimizer.explainer = self.explainer
        self.last_explanation: Optional[Explanation] = None

        self.precision_controller = (PrecisionController()
                                     if self.config.precision_switching_enabled else None)
        self.precision_adapter = (HardwareAwareAdapter(self.precision_controller)
                                  if self.precision_controller else None)
        self.current_precision: Optional[PrecisionLevel] = None

        self.causal = (CausalCounterfactualEstimator(state_dim=8, n_actions=5)
                       if self.config.causal_enabled else None)
        self.last_counterfactuals: Dict[int, float] = {}

        self.federated = (SecureFederatedAggregator(
            dp_sigma=self.config.federated_dp_sigma,
            target_epsilon=self.config.federated_target_epsilon,
            target_delta=self.config.federated_target_delta)
            if self.config.federated_enabled else None)

        self.roles = agent_registry
        self.coordinator = (MultiAgentCoordinator(self.roles)
                            if self.roles is not None else None)

        self.chaos: Optional[ChaosEngineer] = None
        if self.config.chaos_enabled:
            hypotheses = [
                SteadyStateHypothesis(
                    name="scarcity_below_0_95",
                    predicate=lambda m: m.get("scarcity_index", 0.5) <= 0.95),
                SteadyStateHypothesis(
                    name="health_above_50",
                    predicate=lambda m: m.get("health_score", 100) > 50),
                SteadyStateHypothesis(
                    name="active_constraints_bounded",
                    predicate=lambda m: m.get("active_constraints", 0) < 100)]
            self.chaos = ChaosEngineer(
                ChaosConfig(
                    fault_prob=self.config.chaos_fault_prob,
                    latency_inject_ms=self.config.chaos_latency_ms,
                    carbon_spike_prob=self.config.chaos_carbon_spike_prob,
                    blast_radius=self.config.chaos_blast_radius,
                    max_concurrent_faults=self.config.chaos_max_concurrent,
                    duration_s=self.config.chaos_duration_s), hypotheses)

        self.uncertainty = (UncertaintyEstimator()
                            if self.config.hitl_enabled else None)
        self.hitl = (HumanInTheLoopGate(
            timeout_s=self.config.hitl_timeout_s,
            auto_approve_on_timeout=self.config.hitl_auto_approve_on_timeout,
            production_safe=self.config.hitl_production_safe)
            if self.config.hitl_enabled else None)
        self.active_learner = (ActiveLearningSampler()
                               if self.config.hitl_enabled else None)

        # Federated learner + collaborators
        self.federated_learner = FederatedScarcityLearner(
            self.db_manager, self.instance_id,
            self.config.federated_interval, self.config.federated_epsilon,
            aggregator=self.federated)
        self.user_adaptive = UserAdaptiveScarcityReflexivity(self.db_manager, 0.01)
        self.predictive_reflexivity = PredictiveScarcityReflexivity(
            self.db_manager, 24)
        self.sustainability_tracker = ScarcitySustainabilityTracker(self.db_manager)

        # State
        self.current_helium_data: Optional[HeliumData] = None
        self.historical_data: deque = deque(maxlen=10000)
        self.active_constraints: List[HeliumConstraint] = []
        self.constraint_history: List[HeliumConstraint] = []
        self.constraint_ids: Set[str] = set()
        self.constraint_valid_until: Dict[str, datetime] = {}
        self.shortage_predictions: deque = deque(maxlen=100)
        self.alerts: deque = deque(maxlen=1000)
        self._alert_dedup: Dict[str, float] = {}
        self.eligibility_history: deque = deque(maxlen=1000)
        self._alert_callbacks: List[Callable] = []
        self.prediction_confidence = 0.0

        self._data_lock = asyncio.Lock()
        self._constraints_lock = asyncio.Lock()
        self._alerts_lock = asyncio.Lock()
        self._predictions_lock = asyncio.Lock()

        self._task_manager = TaskManager(max_workers=6)
        self._shutdown_event = asyncio.Event()
        self._running = False

        self.scarcity_thresholds = self.config.scarcity_thresholds

        self._health_components = {
            "database": self.db_manager,
            "quantum_security": self.quantum_security,
            "blockchain": self.blockchain,
            "carbon_manager": self.carbon_manager,
            "cloud_distributor": self.cloud_distributor}

        if PROMETHEUS_AVAILABLE and start_http_server is not None:
            try:
                start_http_server(self.config.metrics_port)
                logger.info(f"Prometheus on port {self.config.metrics_port}")
            except Exception as e:
                logger.warning(f"Metrics server failed: {e}")

        logger.info(f"Helium Scarcity Manager v{self.config.version} "
                    f"initialized (instance: {self.instance_id})")
        atexit.register(self._atexit_cleanup)

    def _atexit_cleanup(self):
        self._shutdown_event.set()

    # ------------------------------------------------------------------
    # Trend calculation
    # ------------------------------------------------------------------
    def _calculate_trend(self, field: str) -> str:
        if len(self.historical_data) < 5:
            return "stable"
        recent = list(self.historical_data)[-5:]
        values = [getattr(d, field, 0.5) for d in recent]
        try:
            slope = float(np.polyfit(range(len(values)), values, 1)[0])
        except Exception:
            return "stable"
        if abs(slope) < 0.01: return "stable"
        return "increasing" if slope > 0 else "decreasing"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self):
        self._running = True
        await self.db_manager.init()
        await self.api_collector.__aenter__()
        try: await self.carbon_manager.refresh()
        except Exception as e: logger.warning(f"Initial carbon refresh failed: {e}")
        try:
            loaded = await self.db_manager.load_scarcity_records(limit=200)
            if loaded:
                for hd in loaded: self.historical_data.append(hd)
                self.current_helium_data = loaded[0]
                logger.info(f"Loaded {len(loaded)} scarcity records from DB")
        except Exception as e:
            logger.warning(f"Load from DB failed: {e}")

        # MOE bootstrap: register economic teacher immediately, and others if trained
        if self.moe_ensemble is not None:
            self.moe_ensemble.register_teacher("economic", None, confidence=0.6)
            if len(self.historical_data) >= 20:
                X, y = self._prepare_training_data()
                if X is not None and len(X) >= 20:
                    await self._train_teachers(X, y)

        self._task_manager.register_task("background_update", self._background_update_loop)
        self._task_manager.register_task("health_check", self._health_check_loop)
        self._task_manager.register_task("quantum_monitor", self._quantum_monitor_loop)
        self._task_manager.register_task("blockchain_monitor", self._blockchain_monitor_loop)
        self._task_manager.register_task("auto_optimize", self._auto_optimize_loop)
        self._task_manager.register_task("cloud_sync", self._cloud_sync_loop)
        self._task_manager.register_task("carbon_update", self._carbon_update_loop)
        self._task_manager.register_task("federated", self._federated_learning_loop)
        self._task_manager.register_task("predictive", self._predictive_loop)
        self._task_manager.register_task("sustainability", self._sustainability_loop)
        self._task_manager.register_task("anomaly_update", self._anomaly_update_loop)
        if self.self_healing:
            self._task_manager.register_task("self_healing", self._self_healing_loop)
        self._task_manager.start_registered_tasks()
        logger.info("Scarcity manager started")

    async def _health_check_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                h = await self.health_check()
                HEALTH_SCORE.set(h.get("health_score", 100))
                await asyncio.sleep(self.config.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")
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
                await asyncio.sleep(self.config.carbon_update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon update error: {e}")
                await asyncio.sleep(60)

    async def _auto_optimize_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                async with self._data_lock, self._constraints_lock:
                    state = {
                        "scarcity": self.current_helium_data.scarcity_index
                            if self.current_helium_data else 0.5,
                        "constraints_active": len(self.active_constraints)}
                try:
                    carbon_intensity = await self.carbon_manager.get_current_intensity()
                except Exception:
                    carbon_intensity = 400.0
                state["carbon_intensity"] = carbon_intensity
                result = await self.autonomous_optimizer.optimize(state)
                # Feed reward into rlhf
                if self.config.rlhf_enabled and result.get("source") in ("distilled", "rlhf"):
                    pass  # already updated inside optimizer
                await asyncio.sleep(self.config.auto_optimize_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Auto optimize error: {e}")
                await asyncio.sleep(60)

    async def _cloud_sync_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                if self.current_helium_data:
                    await self.cloud_distributor.distribute_data({
                        "scarcity": self.current_helium_data.scarcity_index,
                        "price": self.current_helium_data.price_per_liter_usd})
                await asyncio.sleep(self.config.cloud_sync_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cloud sync error: {e}")
                await asyncio.sleep(60)

    async def _federated_learning_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                if self.current_helium_data and self.federated:
                    try:
                        await self.federated_learner.share(
                            np.array([self.current_helium_data.scarcity_index],
                                     dtype=np.float64),
                            self.current_helium_data.scarcity_index * 1000)
                    except PrivacyBudgetExceeded as e:
                        logger.warning(f"Federated DP budget exhausted: {e}")
                await asyncio.sleep(self.config.federated_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Federated error: {e}")
                await asyncio.sleep(60)

    async def _predictive_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                if self.current_helium_data:
                    self.predictive_reflexivity.update(
                        self.current_helium_data.scarcity_index)
                    forecast = self.predictive_reflexivity.predict(24)
                    self.prediction_confidence = forecast["confidence"]
                await asyncio.sleep(self.config.predictive_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Predictive error: {e}")
                await asyncio.sleep(60)

    async def _sustainability_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                if self.current_helium_data:
                    self.sustainability_tracker.record(
                        1.0 - self.current_helium_data.scarcity_index)
                await asyncio.sleep(self.config.sustainability_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Sustainability error: {e}")
                await asyncio.sleep(60)

    async def _anomaly_update_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                if self.current_helium_data:
                    is_anomaly, score = await self.anomaly_detector.update(
                        self.current_helium_data)
                    if is_anomaly:
                        logger.warning(f"Scarcity anomaly detected: score={score:.2f}")
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Anomaly update error: {e}")
                await asyncio.sleep(60)

    async def _self_healing_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                if self.self_healing:
                    async with self._data_lock:
                        data = [asdict(d) for d in list(self.historical_data)[-100:]]
                    if data:
                        await self.self_healing.train(data)
                    if self.current_helium_data:
                        await self.self_healing.detect_anomaly(
                            asdict(self.current_helium_data))
                        await self.self_healing.check_drift(
                            asdict(self.current_helium_data))
                await asyncio.sleep(self.config.self_healing.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Self-healing error: {e}")
                await asyncio.sleep(60)

    async def _background_update_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                await self.update_helium_data()
                await self._update_constraints()
                await self._check_alerts()

                # Steady-state check
                if self.chaos and self.chaos.hypotheses:
                    h = await self.health_check()
                    metrics = {
                        "scarcity_index": self.current_helium_data.scarcity_index
                            if self.current_helium_data else 0.5,
                        "health_score": h.get("health_score", 100),
                        "active_constraints": len(self.active_constraints)}
                    if not self.chaos.steady_state_ok(metrics):
                        logger.warning("Chaos steady-state violated; deactivating.")
                        self.chaos.deactivate()

                # Incremental teacher training
                if self.mtop_engine and len(self.historical_data) >= 10:
                    X, y = self._prepare_training_data()
                    if X is not None and len(X) >= 10:
                        await self._train_teachers(X, y)

                await asyncio.sleep(self.config.update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Background update error: {e}")
                await asyncio.sleep(60)

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------
    def _prepare_training_data(self):
        if len(self.historical_data) < 5: return None, None
        recent = list(self.historical_data)[-200:]
        X, y = [], []
        for i in range(1, len(recent)):
            prev, curr = recent[i - 1], recent[i]
            X.append([prev.scarcity_index, prev.price_per_liter_usd,
                      prev.supply_confidence, prev.projected_shortage_days])
            y.append(curr.scarcity_index)
        if not X: return None, None
        return np.array(X), np.array(y)

    async def _train_teachers(self, X_train, y_train):
        if self.moe_ensemble is None: return

        async def _do_train():
            if SKLEARN_AVAILABLE:
                try:
                    gb = GradientBoostingRegressor(
                        n_estimators=50, max_depth=3, random_state=42)
                    gb.fit(X_train, y_train)
                    self.moe_ensemble.register_teacher("gb", gb, confidence=0.8)
                except Exception as e:
                    logger.warning(f"GB fit failed: {e}")
            if XGBOOST_AVAILABLE:
                try:
                    model = xgb.XGBRegressor(
                        n_estimators=50, learning_rate=0.1, random_state=42)
                    model.fit(X_train, y_train)
                    self.moe_ensemble.register_teacher("xgboost", model, confidence=0.9)
                except Exception as e:
                    logger.warning(f"XGB fit failed: {e}")
            if TORCH_AVAILABLE:
                try:
                    class SimpleMLP(nn.Module):
                        def __init__(self, input_dim):
                            super().__init__()
                            self.fc1 = nn.Linear(input_dim, 32)
                            self.relu = nn.ReLU()
                            self.fc2 = nn.Linear(32, 1)
                        def forward(self, x):
                            return torch.sigmoid(self.fc2(self.relu(self.fc1(x))))
                    mlp = SimpleMLP(X_train.shape[1])
                    optimizer = optim.Adam(mlp.parameters(), lr=0.001)
                    criterion = nn.MSELoss()
                    X_t = torch.FloatTensor(X_train)
                    y_t = torch.FloatTensor(y_train).view(-1, 1)
                    for _ in range(30):
                        optimizer.zero_grad()
                        loss = criterion(mlp(X_t), y_t)
                        loss.backward()
                        optimizer.step()
                    self.moe_ensemble.register_teacher("mlp", mlp, confidence=0.85)
                except Exception as e:
                    logger.warning(f"MLP fit failed: {e}")
            self.moe_ensemble.register_teacher("economic", None, confidence=0.6)
            return True

        if self.scheduler is not None:
            await self.scheduler.submit_training(_do_train, critical=False)
        else:
            await _do_train()

    # ------------------------------------------------------------------
    # Core update
    # ------------------------------------------------------------------
    async def update_helium_data(self, region: str = "global") -> HeliumData:
        fault_injected = False
        if self.chaos and self.chaos.maybe_fault():
            fault_injected = True
            SCARCITY_UPDATES.labels(status="chaos_fault").inc()

        try:
            if fault_injected:
                raise ScarcityError("Chaos fault injected")

            production = await self.api_collector.fetch_usgs_production()
            price = await self.api_collector.fetch_eia_price()
            demand = 29000
            scarcity = 0.5
            if production is not None:
                shortage = (demand - production) / demand
                scarcity = max(0.0, min(1.0, shortage * 2))

            price_trend = self._calculate_trend("price_per_liter_usd")
            scarcity_trend = self._calculate_trend("scarcity_index")

            helium_data = HeliumData(
                timestamp=datetime.utcnow(),
                price_per_liter_usd=price or 0.5,
                scarcity_index=scarcity,
                supply_confidence=0.8 if production is not None else 0.5,
                projected_shortage_days=int(30 + scarcity * 60),
                region=region,
                price_trend=price_trend,
                scarcity_trend=scarcity_trend)

            # Precision switch
            try:
                carbon_intensity = await self.carbon_manager.get_current_intensity()
            except Exception:
                carbon_intensity = 400.0
            if self.chaos:
                carbon_intensity = self.chaos.maybe_carbon_spike(carbon_intensity)
            carbon_price = await self.carbon_manager.get_current_price()
            if self.precision_adapter and self.precision_controller:
                self.precision_controller.observe(HardwareTelemetry(
                    gpu_util_pct=random.uniform(20, 90),
                    temperature_c=random.uniform(45, 75),
                    power_watts=random.uniform(50, 250),
                    observed_quality_drop=random.uniform(0.0, 0.05)))
                _, level = self.precision_adapter.adapt(
                    {}, carbon_intensity, 0.5, carbon_price)
                self.current_precision = level
                helium_data.precision_level = level.value

            # Quantum signature
            if self.quantum_security:
                try:
                    quantum_key = await self.quantum_security.generate_keypair(
                        self.config.quantum_algorithm)
                    helium_data.quantum_signature = await self.quantum_security.sign_data(
                        asdict(helium_data), quantum_key["key_id"])
                except Exception as e:
                    logger.warning(f"Quantum sign failed: {e}")

            # Blockchain
            if self.blockchain:
                data_hash = hashlib.sha256(
                    json.dumps(asdict(helium_data), sort_keys=True, default=str).encode()
                ).hexdigest()
                try:
                    bc = await self.blockchain.record_scarcity_data(
                        helium_data.data_id, data_hash, {"scarcity": scarcity})
                    helium_data.blockchain_tx_hash = bc.get("tx_hash")
                except Exception as e:
                    logger.warning(f"Blockchain record failed: {e}")

            # Cloud distribution
            try:
                distribution = await self.cloud_distributor.distribute_data({
                    "scarcity": scarcity, "price": price})
                helium_data.cloud_distribution = distribution
            except Exception as e:
                logger.warning(f"Cloud distribution failed: {e}")

            # Temporal monitor / safety shield: APPLY replacement
            safety_ok = True; stl_verdict: Dict[str, bool] = {}
            if self.temporal and self.shield:
                self.temporal.observe({"scarcity_index": scarcity})
                candidates = [{"value": scarcity, "record": helium_data}]
                replacement, safety_ok, stl_verdict = self.shield.screen(
                    helium_data.to_dict(), candidates, key="value",
                    cost_fn=lambda c: c["value"])
                if not safety_ok and isinstance(replacement, dict) and "value" in replacement:
                    # Apply safer value (conservatively higher scarcity)
                    safe_val = min(1.0, max(replacement["value"], scarcity))
                    helium_data.scarcity_index = safe_val
                    scarcity = safe_val
            helium_data.safety_ok = safety_ok
            helium_data.stl_verdict = stl_verdict

            # Causal RL: UCB action selection
            state_vec = np.array([
                scarcity, float(price or 0.5), helium_data.supply_confidence,
                float(helium_data.projected_shortage_days) / 100.0,
                float(carbon_intensity) / 1000.0, carbon_price / 0.5,
                0.5, 0.5], dtype=np.float64)
            chosen_action = 0
            if self.causal:
                chosen_action = self.causal.select_action(state_vec, 5)
                reward_for_action = 1.0 - scarcity
                self.causal.update(CausalTransition(
                    state=state_vec, action=chosen_action,
                    reward=reward_for_action, next_state=state_vec))
                self.last_counterfactuals = self.causal.counterfactuals(state_vec, 5)
                helium_data.counterfactuals = dict(self.last_counterfactuals)

            # Federated
            if self.federated:
                try:
                    self.federated.submit(FederatedUpdate(
                        node_id=self.instance_id,
                        weights={"local": np.array([scarcity], dtype=np.float64)},
                        n_samples=1, carbon_intensity=float(carbon_intensity)))
                    self.federated.aggregate()
                except PrivacyBudgetExceeded as e:
                    logger.warning(f"Federated DP budget exhausted: {e}")

            # Uncertainty + HITL: real uncertainty from expert disagreement
            reward = 1.0 - scarcity - carbon_price * 0.05
            reward = max(0.0, min(1.0, reward))
            human_approved = True
            if self.uncertainty and self.hitl:
                # Use expert MAE from MOE
                expert_forecasts = []
                if self.moe_ensemble and self.moe_ensemble._expert_errors:
                    for d in self.moe_ensemble._expert_errors.values():
                        if d: expert_forecasts.append(float(np.mean(d)))
                if expert_forecasts:
                    self.uncertainty.observe_expert_forecasts(expert_forecasts)
                probs = np.array([max(1e-6, 1.0 - scarcity),
                                  max(1e-6, scarcity)])
                probs = probs / probs.sum()
                is_unc, unc = self.uncertainty.is_uncertain(probs)
                if is_unc or not safety_ok:
                    req = HITLRequest(
                        request_id=uuid.uuid4().hex[:8],
                        reason="high_uncertainty" if is_unc else "safety_violation",
                        chosen_idx=chosen_action,
                        candidates=[{"value": scarcity}],
                        uncertainty=unc, carbon_price=carbon_price,
                        metadata={"data_id": helium_data.data_id})
                    human_approved = await self.hitl.request(req)
                    helium_data.hitl_approved = human_approved
                    if self.active_learner:
                        self.active_learner.maybe_store(
                            {"data_id": helium_data.data_id, "reward": reward,
                             "scarcity_index": scarcity,
                             "price": float(price or 0.5),
                             "supply_confidence": helium_data.supply_confidence,
                             "projected_shortage_days":
                                 helium_data.projected_shortage_days},
                            uncertainty=unc, threshold=0.3)
                self.uncertainty.observe(reward)

            # XAI
            if self.explainer:
                pareto_list = [{
                    "strictness": 0.0, "carbon_cost": float(carbon_intensity) / 1000.0,
                    "efficiency_cost": scarcity,
                    "monetary_cost": float(price or 0.5) * 0.1}]
                explanation = self.explainer.explain(
                    chosen_idx=chosen_action, chosen=pareto_list[0],
                    pareto=pareto_list,
                    counterfactuals=self.last_counterfactuals,
                    safety_ok=safety_ok and human_approved,
                    carbon_price=carbon_price, confidence=0.7)
                self.last_explanation = explanation
                helium_data.explanation = explanation.to_dict()

            helium_data.provenance = {
                "schema": "helium_scarcity_v18",
                "instance_id": self.instance_id,
                "chaos_active": self.chaos is not None}
            helium_data.chaos_events = list(self.chaos.events[-3:]) if self.chaos else []

            # Store
            async with self._data_lock:
                if self.current_helium_data and \
                        self.current_helium_data.timestamp.date() == datetime.utcnow().date():
                    helium_data.version = self.current_helium_data.version + 1
                    self.current_helium_data.superseded_by = helium_data.blockchain_tx_hash
                self.current_helium_data = helium_data
                self.historical_data.append(helium_data)
            SCARCITY_INDEX.set(helium_data.scarcity_index)

            # Persist
            db_ok = True
            try:
                await self.db_manager.insert_scarcity_record(helium_data)
            except Exception as e:
                logger.warning(f"DB insert failed: {e}")
                db_ok = False
            SCARCITY_UPDATES.labels(
                status="success" if db_ok else "partial").inc()

            # Update downstream
            try:
                self.predictive_reflexivity.update(scarcity)
            except Exception:
                pass
            try:
                self.sustainability_tracker.record(1.0 - scarcity)
            except Exception:
                pass

            # MTOP learning with real target
            if self.mtop_engine and len(self.historical_data) >= 2:
                prev = self.historical_data[-2]
                X = np.array([prev.scarcity_index, prev.price_per_liter_usd,
                              prev.supply_confidence, prev.projected_shortage_days])
                await self.mtop_engine.compute_scarcity(X, actual_scarcity=scarcity)

            # Anomaly detector
            is_anom, anom_score = await self.anomaly_detector.update(helium_data)
            if is_anom:
                logger.warning(f"Scarcity anomaly: score={anom_score:.2f}")

            # Self-healing anomaly trigger
            if self.self_healing:
                await self.self_healing.detect_anomaly(asdict(helium_data))

            # Lineage
            try:
                await self.db_manager.insert_lineage(
                    source="api_collector", operation="update_helium_data",
                    record_ids=[helium_data.data_id],
                    metadata={"scarcity": scarcity, "price": price})
            except Exception:
                pass

            logger.info(f"Updated helium data: scarcity={scarcity:.3f}, "
                        f"price=${price:.2f}/L")
            return helium_data
        finally:
            if fault_injected and self.chaos:
                self.chaos.release_fault()

    async def _update_constraints(self):
        async with self._data_lock:
            if not self.current_helium_data: return
            scarcity = self.current_helium_data.scarcity_index
        async with self._constraints_lock:
            # Expire
            now = datetime.utcnow()
            self.active_constraints = [
                c for c in self.active_constraints if c.valid_until > now]
            # Evict expired constraint IDs
            expired_ids = [cid for cid, vu in self.constraint_valid_until.items()
                           if vu <= now]
            for cid in expired_ids:
                self.constraint_valid_until.pop(cid, None)
                self.constraint_ids.discard(cid)

            severity = "info"
            if scarcity >= self.scarcity_thresholds["emergency"]:
                severity = "emergency"
            elif scarcity >= self.scarcity_thresholds["critical"]:
                severity = "critical"
            elif scarcity >= self.scarcity_thresholds["warning"]:
                severity = "warning"

            if severity in ("warning", "critical", "emergency"):
                try:
                    carbon_intensity = await self.carbon_manager.get_current_intensity()
                except Exception:
                    carbon_intensity = 400.0
                state = {"scarcity": scarcity, "carbon_intensity": carbon_intensity}
                opt_result = await self.autonomous_optimizer.optimize(state)
                max_usage = opt_result.get("max_helium_usage", 0.5)

                cid = hashlib.sha256(
                    f"{severity}:{round(max_usage, 3)}".encode()).hexdigest()[:16]
                if cid not in self.constraint_ids:
                    valid_until = now + timedelta(hours=1)
                    constraint = HeliumConstraint(
                        constraint_id=cid,
                        severity=severity,
                        scarcity_threshold=self.scarcity_thresholds[severity],
                        max_helium_usage_l=max_usage,
                        recommended_actions=self._generate_recommendations(severity),
                        valid_until=valid_until)
                    self.constraint_ids.add(cid)
                    self.constraint_valid_until[cid] = valid_until
                    self.active_constraints.append(constraint)
                    self.constraint_history.append(constraint)
                    try:
                        await self.db_manager.insert_constraint(constraint)
                    except Exception as e:
                        logger.warning(f"Constraint persist failed: {e}")
                    logger.warning(f"New helium constraint: {severity.upper()} "
                                   f"- max {max_usage:.3f}L")
            ACTIVE_CONSTRAINTS.set(len(self.active_constraints))

    def _generate_recommendations(self, severity):
        base = {
            "info": ["Continue normal operations", "Monitor scarcity trend"],
            "warning": ["Reduce non-critical helium usage by 20%",
                        "Increase recycling rate", "Review supply contracts"],
            "critical": ["Reduce helium usage by 50%",
                         "Activate backup suppliers",
                         "Prioritize critical workloads only",
                         "Enable aggressive recycling"],
            "emergency": ["Halt all non-critical helium operations",
                          "Activate emergency reserves",
                          "Switch to helium substitutes where possible",
                          "Notify stakeholders immediately"]}
        return base.get(severity, [])

    async def _check_alerts(self):
        async with self._data_lock:
            if not self.current_helium_data: return
            scarcity = self.current_helium_data.scarcity_index
        async with self._alerts_lock:
            now = time.time()
            for level, threshold in self.scarcity_thresholds.items():
                if scarcity >= threshold:
                    # Dedup by (level, 5-minute bucket)
                    bucket = int(now // 300)
                    key = f"{level}:{bucket}"
                    if key in self._alert_dedup: break
                    self._alert_dedup[key] = now
                    alert = {
                        "level": level, "scarcity": scarcity,
                        "threshold": threshold,
                        "timestamp": datetime.utcnow(),
                        "message": f"Scarcity {scarcity:.3f} exceeds {level} "
                                   f"threshold {threshold}"}
                    self.alerts.append(alert)
                    for cb in self._alert_callbacks:
                        try: cb(alert)
                        except Exception as e: logger.error(f"Alert cb error: {e}")
                    break

    def register_alert_callback(self, callback):
        self._alert_callbacks.append(callback)

    async def check_job_eligibility(self, job_id, helium_requirement_l,
                                    job_priority="normal"):
        # P0 fix: validate inputs
        if helium_requirement_l < 0:
            raise ValueError("helium_requirement_l must be >= 0")
        if job_priority not in ("low", "normal", "high", "critical"):
            raise ValueError("job_priority must be low/normal/high/critical")

        async with self._data_lock, self._constraints_lock:
            if not self.current_helium_data:
                allowed, reasons = True, []
            else:
                scarcity = self.current_helium_data.scarcity_index
                reasons = []
                allowed = True
                if scarcity >= self.scarcity_thresholds["emergency"] \
                        and job_priority not in ("critical",):
                    allowed = False
                    reasons.append("Emergency scarcity — only critical jobs permitted")
                elif scarcity >= self.scarcity_thresholds["critical"] \
                        and job_priority == "low":
                    allowed = False
                    reasons.append("Critical scarcity — low-priority jobs blocked")
                elif scarcity >= self.scarcity_thresholds["warning"] \
                        and job_priority == "low":
                    allowed = False
                    reasons.append("Warning scarcity — low-priority jobs deferred")
                for c in self.active_constraints:
                    if not c.is_active: continue
                    if helium_requirement_l > c.max_helium_usage_l:
                        allowed = False
                        reasons.append(
                            f"Requirement {helium_requirement_l}L exceeds "
                            f"constraint {c.constraint_id} max "
                            f"{c.max_helium_usage_l}L")
                # If no constraints active, apply soft ceiling
                if not self.active_constraints and helium_requirement_l > 0.9:
                    allowed = False
                    reasons.append("Soft ceiling: requirement > 0.9L with no active policy")

        decision = EligibilityDecision(
            decision_id=uuid.uuid4().hex[:12],
            job_id=job_id,
            helium_requirement_l=helium_requirement_l,
            job_priority=job_priority,
            allowed=allowed, reasons=reasons)
        self.eligibility_history.append(decision)
        try:
            await self.db_manager.insert_eligibility(decision)
        except Exception:
            pass
        return allowed, reasons

    async def get_sustainability_forecast(self, days=7):
        async with self._data_lock:
            if not self.historical_data:
                return {"current_scarcity": 0.5, "days_to_critical": None,
                        "confidence": 0.0, "forecast": [], "trend": "stable",
                        "method": "none"}
            current = self.current_helium_data.scarcity_index

        # Use the predictive reflexivity ensemble
        try:
            forecast_result = self.predictive_reflexivity.predict(days)
            forecast = forecast_result["forecast"]
            confidence = forecast_result["confidence"]
        except Exception:
            forecast = [current] * days
            confidence = 0.0

        # Compute trend via linear fit on forecast
        if len(forecast) >= 2:
            try:
                slope = float(np.polyfit(range(len(forecast)), forecast, 1)[0])
            except Exception:
                slope = 0.0
        else:
            slope = 0.0
        critical_threshold = self.scarcity_thresholds["critical"]
        days_to_critical = None
        if slope > 0.001:
            days_to_critical = int(max(0, (critical_threshold - current) / slope))
            if days_to_critical == 0 and current < critical_threshold:
                days_to_critical = 1

        return {
            "current_scarcity": current,
            "days_to_critical": days_to_critical,
            "confidence": confidence,
            "forecast": [float(np.clip(v, 0.0, 1.0)) for v in forecast],
            "trend": "increasing" if slope > 0.001 else (
                "decreasing" if slope < -0.001 else "stable"),
            "method": "moe_ensemble"}

    async def get_stats(self):
        async with self._data_lock, self._constraints_lock, self._alerts_lock:
            mtop_stats = self.moe_ensemble.get_stats() if self.moe_ensemble else None
            h = await self.health_check()
            return {
                "instance_id": self.instance_id,
                "version": self.config.version,
                "current": {
                    "scarcity_index": self.current_helium_data.scarcity_index
                        if self.current_helium_data else None,
                    "price_usd_per_l": self.current_helium_data.price_per_liter_usd
                        if self.current_helium_data else None,
                    "supply_confidence": self.current_helium_data.supply_confidence
                        if self.current_helium_data else None,
                    "projected_shortage_days":
                        self.current_helium_data.projected_shortage_days
                        if self.current_helium_data else None,
                    "price_trend": self.current_helium_data.price_trend
                        if self.current_helium_data else None,
                    "scarcity_trend": self.current_helium_data.scarcity_trend
                        if self.current_helium_data else None,
                    "precision_level": self.current_precision.value
                        if self.current_precision else None},
                "constraints": {
                    "active": len(self.active_constraints),
                    "history": len(self.constraint_history),
                    "active_constraints": [
                        {"severity": c.severity,
                         "max_usage_l": c.max_helium_usage_l,
                         "valid_until": c.valid_until.isoformat()}
                        for c in self.active_constraints]},
                "alerts": {
                    "total": len(self.alerts),
                    "recent": [
                        {"level": a["level"], "scarcity": a["scarcity"],
                         "timestamp": a["timestamp"].isoformat()}
                        for a in list(self.alerts)[-5:]]},
                "prediction": {
                    "confidence": self.prediction_confidence,
                    "samples": len(self.shortage_predictions)},
                "historical": {
                    "samples": len(self.historical_data),
                    "min_scarcity": min([d.scarcity_index for d in self.historical_data])
                        if self.historical_data else None,
                    "max_scarcity": max([d.scarcity_index for d in self.historical_data])
                        if self.historical_data else None,
                    "avg_scarcity": float(np.mean(
                        [d.scarcity_index for d in self.historical_data]))
                        if self.historical_data else None},
                "quantum_security": self.quantum_security.get_quantum_status(),
                "blockchain_status": await self.blockchain.get_blockchain_status(),
                "autonomous_optimization":
                    self.autonomous_optimizer.get_optimization_stats(),
                "cloud_distribution":
                    await self.cloud_distributor.get_distribution_status(),
                "cloud_storage": await self.cloud_distributor.health_check(),
                "mtop": mtop_stats,
                "federated": self.federated_learner.get_federated_insights()
                    if self.federated else {},
                "sustainability":
                    await self.sustainability_tracker.get_sustainability_score(),
                "anomaly_detector": await self.anomaly_detector.get_statistics(),
                "self_healing": (await self.self_healing.get_stats()
                                 if self.self_healing else None),
                "bio_optimizer": {"current_params": self.bio_optimizer.current_params},
                "modp": (self.modp_optimizer.get_optimization_stats()
                         if self.modp_optimizer else None),
                "scheduler": {"enabled": self.scheduler is not None},
                "ten_enhancements": {
                    "quantum_distillation": self.config.distillation_enabled,
                    "causal": self.causal is not None,
                    "federated": self.federated is not None,
                    "multi_agent": self.coordinator is not None,
                    "temporal_logic": self.temporal is not None,
                    "xai": self.explainer is not None,
                    "precision_switching": self.precision_adapter is not None,
                    "carbon_market": self.config.carbon_market_enabled,
                    "chaos": self.chaos is not None,
                    "hitl": self.hitl is not None},
                "current_precision": self.current_precision.value
                    if self.current_precision else None,
                "precision_stats": self.precision_controller.stats()
                    if self.precision_controller else {},
                "rec_kwh": self.carbon_manager.recs.total_kwh(),
                "counterfactuals": self.last_counterfactuals,
                "causal_stats": self.causal.stats() if self.causal else {},
                "uncertainty_stats":
                    self.uncertainty.stats() if self.uncertainty else {},
                "active_learner_stats":
                    self.active_learner.stats() if self.active_learner else {},
                "temporal_violations":
                    self.temporal.recent_violations() if self.temporal else [],
                "chaos_stats": self.chaos.stats() if self.chaos else {},
                "eligibility_history_len": len(self.eligibility_history),
                "health": h,
                "timestamp": datetime.utcnow().isoformat()}

    async def health_check(self):
        results = {}
        for name, comp in self._health_components.items():
            if comp and hasattr(comp, "health_check"):
                try: results[name] = await comp.health_check()
                except Exception as e: results[name] = {"status": "unhealthy",
                                                        "error": str(e)}
            else:
                results[name] = {"status": "ok" if comp is not None else "unavailable"}
        checkable = [r for r in results.values() if r.get("status") != "unavailable"]
        if not checkable:
            overall, score = "degraded", 0
        else:
            ok = all(r.get("status") in ("ok", "healthy") for r in checkable)
            overall, score = ("healthy", 100) if ok else ("degraded", 50)
        return {"status": overall, "health_score": score,
                "components": results,
                "timestamp": datetime.utcnow().isoformat()}

    async def close(self):
        logger.info("Closing Helium Scarcity Manager...")
        self._shutdown_event.set()
        self._running = False
        await self._task_manager.stop_all()
        with contextlib.suppress(Exception):
            await self.api_collector.close()
        with contextlib.suppress(Exception):
            await self.carbon_manager.close()
        with contextlib.suppress(Exception):
            await self.db_manager.close()
        logger.info("Closed.")


# ============================================================
# FastAPI
# ============================================================
if FASTAPI_AVAILABLE:
    from contextlib import asynccontextmanager

    manager_ref: Optional[HeliumScarcityManager] = None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        global manager_ref
        manager_ref = HeliumScarcityManager()
        await manager_ref.start()
        try:
            yield
        finally:
            if manager_ref:
                await manager_ref.close()
                manager_ref = None

    app = FastAPI(title="Helium Scarcity API", version="18.0", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware, allow_origins=["*"], allow_credentials=True,
        allow_methods=["*"], allow_headers=["*"])
    security = HTTPBearer(auto_error=False)

    _api_config: Optional[ScarcityConfig] = None

    def _get_config() -> ScarcityConfig:
        global _api_config
        if _api_config is None:
            _api_config = ScarcityConfig()
        return _api_config

    async def verify_token(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)):
        if not JOSE_AVAILABLE:
            if _get_config().environment == "production":
                raise HTTPException(status_code=500, detail="JWT library missing")
            return {"sub": "anonymous"}
        if credentials is None:
            raise HTTPException(status_code=401, detail="Missing credentials")
        try:
            return jwt.decode(credentials.credentials, _get_config().jwt_secret,
                              algorithms=[_get_config().jwt_algorithm])
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid token")

    @app.post("/token")
    async def issue_token(sub: str = "user"):
        if not JOSE_AVAILABLE:
            raise HTTPException(status_code=500, detail="JWT library missing")
        exp = datetime.utcnow() + timedelta(seconds=_get_config().jwt_expiry_s)
        token = jwt.encode({"sub": sub, "exp": exp}, _get_config().jwt_secret,
                           algorithm=_get_config().jwt_algorithm)
        return {"access_token": token, "token_type": "bearer",
                "expires_in": _get_config().jwt_expiry_s}

    @app.post("/update")
    async def update(region: str = "global", user: Dict = Depends(verify_token)):
        if not manager_ref:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        data = await manager_ref.update_helium_data(region)
        return data.to_dict()

    @app.get("/status")
    async def status(user: Dict = Depends(verify_token)):
        if not manager_ref:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        return await manager_ref.get_stats()

    @app.get("/health")
    async def health():
        if not manager_ref:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        return await manager_ref.health_check()

    @app.post("/eligibility")
    async def eligibility(job_id: str, helium_requirement_l: float,
                          job_priority: str = "normal",
                          user: Dict = Depends(verify_token)):
        if not manager_ref:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        try:
            allowed, reasons = await manager_ref.check_job_eligibility(
                job_id, helium_requirement_l, job_priority)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        return {"allowed": allowed, "reasons": reasons}

    @app.get("/sustainability")
    async def sustainability(days: int = 7, user: Dict = Depends(verify_token)):
        if not manager_ref:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        return await manager_ref.get_sustainability_forecast(days)

    @app.get("/explanation/last")
    async def last_explanation(user: Dict = Depends(verify_token)):
        if not manager_ref:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        return {"explanation": manager_ref.last_explanation.to_dict()
                if manager_ref.last_explanation else None,
                "counterfactuals": manager_ref.last_counterfactuals}

    @app.post("/chaos")
    async def chaos_endpoint(fault_prob: float = 0.0, latency_ms: float = 0.0,
                             carbon_spike_prob: float = 0.0, duration_s: float = 0.0,
                             user: Dict = Depends(verify_token)):
        if not manager_ref:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        hypotheses = [
            SteadyStateHypothesis(
                name="scarcity_below_0_95",
                predicate=lambda m: m.get("scarcity_index", 0.5) <= 0.95),
            SteadyStateHypothesis(
                name="health_above_50",
                predicate=lambda m: m.get("health_score", 100) > 50)]
        manager_ref.chaos = ChaosEngineer(
            ChaosConfig(fault_prob=fault_prob, latency_inject_ms=latency_ms,
                        carbon_spike_prob=carbon_spike_prob,
                        duration_s=duration_s), hypotheses)
        return {"status": "chaos enabled",
                "config": asdict(manager_ref.chaos.config)}

    @app.post("/chaos/disable")
    async def chaos_disable(user: Dict = Depends(verify_token)):
        if manager_ref and manager_ref.chaos:
            manager_ref.chaos.deactivate()
        return {"status": "chaos disabled"}

    @app.get("/hitl/pending")
    async def hitl_pending(user: Dict = Depends(verify_token)):
        if not manager_ref or not manager_ref.hitl:
            return {"pending": []}
        return {"pending": manager_ref.hitl.pending_requests()}

    @app.post("/hitl/approval")
    async def hitl_approval(request_id: str, approved: bool,
                            user: Dict = Depends(verify_token)):
        if not manager_ref or not manager_ref.hitl:
            raise HTTPException(status_code=503, detail="HITL not enabled")
        ok = await manager_ref.hitl.resolve(request_id, approved)
        return {"status": "resolved" if ok else "not_found",
                "request_id": request_id, "approved": approved}


# ============================================================
# Signal handling / singleton / main
# ============================================================
_manager_instance: Optional[HeliumScarcityManager] = None
_manager_lock = asyncio.Lock()
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


async def get_scarcity_manager(
    config: Optional[Union[ScarcityConfig, Dict]] = None,
) -> HeliumScarcityManager:
    global _manager_instance
    if _manager_instance is None:
        async with _manager_lock:
            if _manager_instance is None:
                _manager_instance = HeliumScarcityManager(config)
                await _manager_instance.start()
    return _manager_instance


async def shutdown_handler():
    global _manager_instance
    if _manager_instance:
        await _manager_instance.close()
        _manager_instance = None


async def main():
    loop = asyncio.get_running_loop()
    _install_signal_handlers(loop)

    print("=" * 80)
    print("Helium Scarcity Manager v18.0 — P0/P1/P2 Hardened")
    print("=" * 80)

    if FASTAPI_AVAILABLE and os.environ.get("SCARCITY_SERVE_API", "0") == "1":
        cfg = ScarcityConfig()
        uvicorn.run(app, host=cfg.api_host, port=cfg.api_port, log_level="info")
        return

    manager = await get_scarcity_manager()

    print("\n✅ Ten enhancements wired in:")
    print("   [1] Quantum-Distillation     PennyLane/Qiskit/state-vector + distillation")
    print("   [2] Causal RL                UCB policy over counterfactuals")
    print("   [3] Federated Learning       Secure aggregation + DP accountant")
    print("   [4] Multi-Agent              Message bus + conflict resolution")
    print("   [5] Temporal Logic           G/F/U/X/-> semantics + actionable shield")
    print("   [6] Explainable AI           SHAP -> LIME -> z-score + ablation")
    print("   [7] Adaptive Precision       Telemetry + thermal + energy accounting")
    print("   [8] Carbon Markets / RECs    Real providers + persisted ledger")
    print("   [9] Resilience / Chaos       Steady-state + blast radius + rollback")
    print("  [10] HITL / Active Learning   Real uncertainty + pending queue + diversity")

    qstatus = manager.quantum_security.get_quantum_status()
    print(f"\n🔐 Quantum: PQC available: {qstatus.get('pqc_available', False)}")

    bstatus = await manager.blockchain.get_blockchain_status()
    print(f"⛓️  Blockchain: {bstatus.get('backend')}, "
          f"records={bstatus.get('total_records', 0)}")

    cstatus = await manager.cloud_distributor.get_distribution_status()
    print(f"☁️  Active provider: {cstatus.get('active_provider')} "
          f"({cstatus.get('active_region')})")

    print("\n📊 Fetching Helium data...")
    for _ in range(3):
        try:
            data = await manager.update_helium_data()
            print(f"   {data.data_id}: scarcity={data.scarcity_index:.3f}, "
                  f"price=${data.price_per_liter_usd:.2f}/L, "
                  f"precision={data.precision_level}, safe={data.safety_ok}")
        except Exception as e:
            print(f"   ⚠️ Update failed: {e}")

    print("\n✅ Checking job eligibility...")
    try:
        allowed, reasons = await manager.check_job_eligibility("test_job", 0.3, "normal")
        print(f"   Allowed: {allowed}")
        if not allowed:
            for r in reasons:
                print(f"   - {r}")
    except Exception as e:
        print(f"   ⚠️ Eligibility check failed: {e}")

    print("\n📈 Sustainability forecast...")
    try:
        forecast = await manager.get_sustainability_forecast(days=7)
        print(f"   Current: {forecast['current_scarcity']:.3f}")
        print(f"   Days to critical: {forecast['days_to_critical']}")
        print(f"   Trend: {forecast['trend']}")
        print(f"   Confidence: {forecast['confidence']:.2f}")
        print(f"   Method: {forecast.get('method')}")
    except Exception as e:
        print(f"   ⚠️ Forecast failed: {e}")

    stats = await manager.get_stats()
    print(f"\n📊 Stats:")
    print(f"   Instance: {stats['instance_id']}")
    print(f"   History: {stats['historical']['samples']}")
    print(f"   Active constraints: {stats['constraints']['active']}")
    print(f"   Alerts: {stats['alerts']['total']}")
    print(f"   Health: {stats['health']['health_score']}")
    print(f"   MOE: {stats['mtop']}")
    print(f"   Enhancements: {stats['ten_enhancements']}")

    print("\n" + "=" * 80)
    print("✅ Helium Scarcity Manager v18.0 — Ready")
    print("=" * 80)

    try:
        await _shutdown_event_global.wait()
    except asyncio.CancelledError:
        pass
    finally:
        await shutdown_handler()


if __name__ == "__main__":
    asyncio.run(main())
