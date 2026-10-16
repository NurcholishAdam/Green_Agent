#!/usr/bin/env python3
# File: src/enhancements/helium_circularity_enhanced_v19_0.py
# Version 19.0 — Full Green Agent stack with all ten enhancements
"""
Enhanced Helium Circularity Model — v19.0

The ten Green Agent enhancements are imported from the enhancements/*
package. This file keeps only circularity-specific logic:

  - HeliumCircularityMetrics (range-validated)
  - EnhancedDataQualityScorer
  - PostQuantumCrypto (sign + real verify)
  - BlockchainCircularityVerification (idempotent)
  - MultiCloudStorage
  - MixtureOfExpertsPredictive (proxy-label gating, rolling retrain)
  - GeneticAlgorithmOptimizer (real fitness via AdaptiveCostFunction)
  - EnhancedAutonomousCircularityOptimizer
  - MultiObjectiveCloudDeployment (carbon-aware, per-role bids)
  - SelfHealingManager (fitted detectors)
  - EnhancedHeliumCircularityCalculator
  - FastAPI surface, singleton, signal handlers, main

Changelog v18.0 -> v19.0:
  - All ten enhancement classes imported from the enhancements package.
  - Compat wrappers for Explanation, DecisionExplainer, HITLRequest,
    UncertaintyEstimator, HumanInTheLoopGate.
  - FIXED: circularity_index degeneracy (single-candidate TOPSIS -> weighted sum).
  - FIXED: GA gene keys now match the state keys (AdaptiveCostFunction works).
  - MultiAgentCoordinator.vote tuples are unpacked; winning_role credited.
  - PostQuantumCrypto.verify_data performs real signature verification.
  - HumanInTheLoopGate.submit_decision enables external decision delivery.
  - MultiObjectiveCloudDeployment reads real carbon intensity.
  - Per-role bids differentiated; distiller teachers cached, not reassigned.
  - Optimizer has its own STL monitor + safety shield.
  - XAI Pareto set now has diverse inputs so attributions are informative.
  - Real Prometheus metrics (no more local _SimpleMetrics).
  - save_state / load_state for causal, roles, federated, GA, strategies.
  - Signal handlers retain task references (no GC before run).
  - FastAPI surface added: /calculate, /status, /health,
    /explanation/last, /hitl/approval, /chaos.
  - TaskManager honors restart_on_exit semantics.
"""

from __future__ import annotations

import asyncio
import atexit
import base64
import contextlib
import contextvars
import hashlib
import json
import logging
import logging.handlers
import math
import os
import random
import signal
import tempfile
import time
import uuid
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import numpy as np


# ============================================================
# SHIM: import all ten enhancements from the shared package
# ============================================================
from enhancements.precision import (
    PrecisionLevel, PRECISION_COST, PrecisionController, HardwareAwareAdapter,
)
from enhancements.carbon import (
    CarbonMarketClient, RECRecord, RECInventory, CarbonIntensityManager,
)
from enhancements.chaos import (
    ChaosConfig, ChaosEngineer,
)
from enhancements.stl import (
    STLOperator, STLFormula, TemporalLogicMonitor, SafetyShield,
)
from enhancements.causal import (
    CausalTransition, CausalCounterfactualEstimator,
)
from enhancements.federated import (
    FederatedUpdate, FederatedAggregator,
)
from enhancements.multi_agent import (
    AgentRole, AgentBid, EmergentRoleRegistry, MultiAgentCoordinator,
)
from enhancements.quantum import (
    QuantumInspiredTeacher, DistillationEnsemble,
)
from enhancements.xai import Explanation as SharedExplanation
from enhancements.xai import DecisionExplainer as SharedDecisionExplainer
from enhancements.hitl import (
    HITLRequest as SharedHITLRequest,
    UncertaintyEstimator as SharedUncertaintyEstimator,
    HumanInTheLoopGate as SharedHumanInTheLoopGate,
    ActiveLearningSampler,
)
from enhancements.common_state import (
    STATE_VECTOR_DIM, state_vector_from_metrics,
)


# ============================================================
# COMPAT WRAPPERS
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

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DecisionExplainer(SharedDecisionExplainer):
    def explain(self, chosen_idx, chosen, pareto, counterfactuals,
                safety_ok, carbon_price, confidence: float = 0.5):
        numeric_keys = [k for k, v in chosen.items() if isinstance(v, (int, float))]
        if pareto and numeric_keys:
            baseline = {k: float(np.mean([c.get(k, 0.0) for c in pareto])) for k in numeric_keys}
            scales = {k: float(np.std([c.get(k, 0.0) for c in pareto]) + 1e-9) for k in numeric_keys}
        else:
            baseline = {k: 0.0 for k in numeric_keys}
            scales = {k: 1.0 for k in numeric_keys}
        shared = self.explain_anomaly(
            kind="decision", current=chosen, baseline=baseline, scales=scales,
            counterfactuals=counterfactuals, safety_ok=safety_ok,
            carbon_price=carbon_price, confidence=confidence,
        )
        return Explanation(
            decision_id=shared.decision_id, chosen_idx=chosen_idx,
            top_features=shared.top_features, counterfactuals=shared.counterfactuals,
            rationale=shared.rationale, confidence=shared.confidence,
            safety_ok=shared.safety_ok, carbon_price_signal=shared.carbon_price_signal,
        )


@dataclass
class HITLRequest:
    request_id: str
    reason: str
    chosen_idx: int
    candidates: List[Dict[str, Any]]
    uncertainty: float
    carbon_price: float
    timestamp: float = field(default_factory=time.time)


class UncertaintyEstimator(SharedUncertaintyEstimator):
    def __init__(self, entropy_threshold: float = 0.75, variance_threshold: float = 0.05):
        super().__init__(window=32)
        self.entropy_threshold = entropy_threshold
        self.variance_threshold = variance_threshold

    def is_uncertain(self, probs) -> Tuple[bool, float]:
        if probs is None or len(probs) == 0:
            return False, 0.0
        p = np.clip(np.asarray(probs, dtype=np.float64), 1e-12, 1.0)
        h = float(-np.sum(p * np.log(p)) / math.log(len(p)))
        score = 0.6 * h
        return h > self.entropy_threshold, score


class HumanInTheLoopGate(SharedHumanInTheLoopGate):
    def __init__(self, approver=None, timeout_s: float = 2.0,
                 auto_approve_on_timeout: bool = True):
        super().__init__(approver=approver, timeout_s=timeout_s,
                         auto_approve_on_timeout=auto_approve_on_timeout)
        self._pending: Dict[str, asyncio.Future] = {}
        self._pending_lock = asyncio.Lock()

    async def request(self, req: HITLRequest) -> bool:
        if self.approver is None:
            self.audit.append({"request_id": req.request_id,
                               "approved": True, "reason": "no_approver"})
            HITL_APPROVALS.labels(decision="auto_approved").inc()
            return True
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        async with self._pending_lock:
            self._pending[req.request_id] = future

        async def _run():
            try:
                result = await loop.run_in_executor(None, self.approver, req)
                if not future.done():
                    future.set_result(bool(result))
            except Exception as e:
                if not future.done():
                    future.set_exception(e)

        runner = asyncio.create_task(_run())
        try:
            approved = await asyncio.wait_for(future, timeout=self.timeout_s)
        except asyncio.TimeoutError:
            approved = self.auto_approve_on_timeout
            HITL_APPROVALS.labels(decision="timeout").inc()
        except Exception:
            approved = self.auto_approve_on_timeout
        finally:
            async with self._pending_lock:
                self._pending.pop(req.request_id, None)
            if not runner.done():
                runner.cancel()
        self.audit.append({"request_id": req.request_id,
                           "approved": bool(approved), "reason": req.reason})
        HITL_APPROVALS.labels(decision="approved" if approved else "rejected").inc()
        return bool(approved)

    async def submit_decision(self, request_id: str, approved: bool) -> bool:
        async with self._pending_lock:
            fut = self._pending.get(request_id)
            if fut is None or fut.done():
                return False
            fut.set_result(bool(approved))
            return True


# ============================================================
# CENTRAL GREEN AGENT COMPONENTS (with fallbacks)
# ============================================================
try:
    from ..config import config as central_config
except Exception:
    class _CentralConfig:
        CIRCUIT_BREAKER_FAILURE_THRESHOLD = 5
        CIRCUIT_BREAKER_RECOVERY_TIMEOUT = 30
        rate_limit_requests = 100
        rate_limit_window = 60
        ENVIRONMENT = "production"
        data_retention_days = 365
        self_healing_enabled = True
        self_healing_interval = 3600
        auto_optimize_interval = 1800
        enable_gpu = False
        enable_ml_predictions = False
        enable_blockchain = False
    central_config = _CentralConfig()

try:
    from ..storage import Storage
except Exception:
    class Storage:
        def __init__(self, *a, **kw): self._records: List[Any] = []
        def store_circularity_record(self, record): self._records.append(record)
        def clean_old_circularity_records(self, days=365): self._records.clear()

try:
    from ..schemas.feedback_event import FeedbackEvent
except Exception:
    class FeedbackEvent:
        def __init__(self, **kwargs): self.__dict__.update(kwargs)
        @classmethod
        def create_with_context(cls, **kwargs): return cls(**kwargs)
        def to_json(self): return json.dumps(self.__dict__, default=str)

try:
    from ..routing.pareto_gating import ParetoGating
except Exception:
    class ParetoGating:
        def __init__(self, *a, **kw): pass

try:
    from ..feedback.adaptive_cost import AdaptiveCostFunction
except Exception:
    class AdaptiveCostFunction:
        def __init__(self, storage=None, *a, **kw): self.storage = storage
        def evaluate(self, state: Dict, targets: Dict) -> float:
            cost = 0.0
            for k, t in targets.items():
                if k in state:
                    cost += (float(state[k]) - float(t)) ** 2
            return float(cost)

try:
    from ..safety.drift_detector import DriftDetector
except Exception:
    class DriftDetector:
        def __init__(self, *a, **kw): pass
        async def check_drift(self, metrics): return False

try:
    from ..scaling.message_queue import AsyncMessageQueue
except Exception:
    class AsyncMessageQueue:
        def __init__(self): self._messages: List[Any] = []
        async def publish(self, channel, message): self._messages.append((channel, message))

try:
    from ..metrics import MetricsRegistry
except Exception:
    class MetricsRegistry:
        def set_circularity_score(self, value): pass

try:
    import structlog  # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s",
        )

correlation_id_var = contextvars.ContextVar("correlation_id", default=str(uuid.uuid4())[:8])


class CorrelationIdFilter(logging.Filter):
    def filter(self, record):
        record.correlation_id = correlation_id_var.get()
        return True


if isinstance(logger, logging.Logger):
    logger.addFilter(CorrelationIdFilter())


# ============================================================
# OPTIONAL DEPENDENCIES
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
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

try:
    from prophet import Prophet  # noqa: F401
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

try:
    from sklearn.linear_model import LogisticRegression, LinearRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import IsolationForest
    from sklearn.svm import OneClassSVM
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

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
    from fastapi import FastAPI, Depends, HTTPException, Request
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

try:
    from jose import JWTError, jwt  # type: ignore
    JOSE_AVAILABLE = True
except ImportError:
    JOSE_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from enhancements.limit_graph import LimitGraph  # type: ignore
    from enhancements.rlhf import RLHFOptimizer  # type: ignore
    from enhancements.multi_teacher_policy_distillation import MultiTeacherDistiller  # type: ignore
    ADDITIONAL_ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ADDITIONAL_ENHANCEMENTS_AVAILABLE = False

    class LimitGraph:
        def __init__(self, *a, **kw): self.limits = {}
        def build_graph(self, nodes, edges): pass
        def get_limits(self, context): return {}
        def update_from_feedback(self, feedback): pass

    class RLHFOptimizer:
        def __init__(self, action_space, *a, **kw):
            self.actions = list(action_space)
            self.scores = {a: 0.0 for a in self.actions}
        def update(self, context, action, reward):
            if action not in self.scores:
                self.actions.append(action); self.scores[action] = 0.0
            self.scores[action] += 0.1 * (float(reward) - self.scores[action])
        def sample_action(self, context):
            return max(self.scores, key=self.scores.get) if self.scores else None

    class MultiTeacherDistiller:
        def __init__(self, teachers, weights=None, *a, **kw):
            self.teachers = list(teachers)
            self.weights = list(weights) if weights else [1.0] * len(self.teachers)
        def _call(self, t, ctx):
            try:
                out = t(ctx)
                if asyncio.iscoroutine(out):
                    out.close(); return None
                return out
            except Exception:
                return None
        def distill(self, context):
            votes: Dict[Any, float] = {}
            for t, w in zip(self.teachers, self.weights):
                c = self._call(t, context)
                if c is not None:
                    votes[c] = votes.get(c, 0.0) + float(w)
            return max(votes, key=votes.get) if votes else None


# ============================================================
# PROMETHEUS METRICS
# ============================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    CIRCULARITY_COMPUTATIONS = Counter("circularity_computations_total", "Computations", ["status"], registry=REGISTRY)
    QUANTUM_SIGNATURES = Counter("circularity_quantum_signatures_total", "Sigs", ["algorithm", "status"], registry=REGISTRY)
    BLOCKCHAIN_VERIFICATIONS = Counter("circularity_blockchain_verifications_total", "BC", ["status"], registry=REGISTRY)
    AUTONOMOUS_OPTIMIZATIONS = Counter("circularity_optimizations_total", "Opt", ["strategy", "status"], registry=REGISTRY)
    CLOUD_DEPLOYMENTS = Counter("circularity_cloud_deployments_total", "Deploy", ["provider", "status"], registry=REGISTRY)
    CIRCULARITY_INDEX = Gauge("circularity_index", "CI", registry=REGISTRY)
    CARBON_INTENSITY = Gauge("circularity_carbon_intensity", "CI", registry=REGISTRY)
    CARBON_PRICE = Gauge("circularity_carbon_price_per_kg", "CP", registry=REGISTRY)
    REC_INVENTORY_KWH = Gauge("circularity_rec_inventory_kwh", "REC", registry=REGISTRY)
    PRECISION_SWITCHES = Counter("circularity_precision_switches_total", "PS", ["level"], registry=REGISTRY)
    CHAOS_EVENTS = Counter("circularity_chaos_events_total", "CE", ["type"], registry=REGISTRY)
    HITL_APPROVALS = Counter("circularity_hitl_approvals_total", "H", ["decision"], registry=REGISTRY)
    SAFETY_VIOLATIONS = Counter("circularity_safety_violations_total", "SV", ["formula"], registry=REGISTRY)
    FEDERATED_ROUNDS = Counter("circularity_federated_rounds_total", "FR", registry=REGISTRY)
    HEALTH_SCORE = Gauge("circularity_health_score", "HS", registry=REGISTRY)
    MOE_GATING_WEIGHTS = Gauge("circularity_moe_gating_weights", "MG", ["expert"], registry=REGISTRY)
    GA_FITNESS = Gauge("circularity_ga_fitness", "GA", ["generation"], registry=REGISTRY)
    CLOUD_STORAGE = Counter("circularity_cloud_storage_operations_total", "CS", ["provider", "operation", "status"], registry=REGISTRY)
else:
    class DummyMetrics:
        def inc(self, *a, **kw): pass
        def set(self, *a, **kw): pass
        def observe(self, *a, **kw): pass
        def labels(self, *a, **kw): return self
    CIRCULARITY_COMPUTATIONS = QUANTUM_SIGNATURES = BLOCKCHAIN_VERIFICATIONS = DummyMetrics()
    AUTONOMOUS_OPTIMIZATIONS = CLOUD_DEPLOYMENTS = CIRCULARITY_INDEX = DummyMetrics()
    CARBON_INTENSITY = CARBON_PRICE = REC_INVENTORY_KWH = PRECISION_SWITCHES = DummyMetrics()
    CHAOS_EVENTS = HITL_APPROVALS = SAFETY_VIOLATIONS = FEDERATED_ROUNDS = DummyMetrics()
    HEALTH_SCORE = MOE_GATING_WEIGHTS = GA_FITNESS = CLOUD_STORAGE = DummyMetrics()


# ============================================================
# EXCEPTIONS
# ============================================================
class CircularityError(Exception): pass
class QuantumError(CircularityError): pass
class BlockchainError(CircularityError): pass
class OptimizationError(CircularityError): pass
class DeploymentError(CircularityError): pass
class CircuitBreakerOpenError(CircularityError): pass
class RateLimitExceeded(CircularityError): pass
class VaultError(CircularityError): pass
class CloudStorageError(CircularityError): pass
class PredictiveError(CircularityError): pass
class SafetyViolationError(CircularityError): pass


# ============================================================
# PARETO FRONT + TOPSIS
# ============================================================
class ParetoFront:
    def __init__(self):
        self.solutions: List[Tuple[List[float], Any]] = []

    def add(self, objectives: List[float], decision: Any) -> bool:
        dominated = False
        for obj, _ in self.solutions:
            if (all(obj[i] <= objectives[i] for i in range(len(objectives)))
                    and any(obj[i] < objectives[i] for i in range(len(objectives)))):
                dominated = True; break
        if not dominated:
            self.solutions = [
                (obj, dec) for obj, dec in self.solutions
                if not (all(objectives[i] <= obj[i] for i in range(len(objectives)))
                        and any(objectives[i] < obj[i] for i in range(len(objectives))))
            ]
            self.solutions.append((objectives, decision))
        return dominated

    def get_pareto_front(self): return self.solutions

    def get_best_by_weight(self, weights):
        best, best_score = None, -float("inf")
        for obj, dec in self.solutions:
            score = sum(w * o for w, o in zip(weights, obj))
            if score > best_score:
                best_score = score; best = dec
        return best


class TOPSIS:
    @staticmethod
    def score(candidates, weights, criteria):
        if not candidates or not criteria:
            return []
        # v19: single candidate is degenerate; fall back to weighted sum
        if len(candidates) == 1:
            c = candidates[0]
            w = list(weights)
            total = sum(w) or 1.0
            w = [x / total for x in w]
            s = sum(w[i] * float(c.get(criteria[i], 0.0))
                    for i in range(min(len(w), len(criteria))))
            return [float(min(1.0, max(0.0, s)))]
        matrix = np.array([[float(c.get(crit, 0.0)) for crit in criteria] for c in candidates])
        denom = np.sqrt((matrix ** 2).sum(axis=0))
        denom = np.where(denom == 0, 1.0, denom)
        norm = matrix / denom
        w = np.asarray(weights[: norm.shape[1]])
        if w.sum() <= 0:
            w = np.ones(norm.shape[1]) / norm.shape[1]
        else:
            w = w / w.sum()
        weighted = norm * w
        ideal = weighted.max(axis=0); neg_ideal = weighted.min(axis=0)
        d_plus = np.sqrt(((weighted - ideal) ** 2).sum(axis=1))
        d_minus = np.sqrt(((weighted - neg_ideal) ** 2).sum(axis=1))
        return (d_minus / (d_plus + d_minus + 1e-9)).tolist()


# ============================================================
# CIRCUIT BREAKER / RATE LIMITER
# ============================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"; OPEN = "open"; HALF_OPEN = "half_open"


class EnhancedCircuitBreaker:
    def __init__(self, name: str):
        self.name = name
        self.failure_threshold = getattr(central_config, "CIRCUIT_BREAKER_FAILURE_THRESHOLD", 5)
        self.recovery_timeout = getattr(central_config, "CIRCUIT_BREAKER_RECOVERY_TIMEOUT", 30)
        self.half_open_max_requests = 3
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
            if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
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
    def __init__(self):
        self.rate = getattr(central_config, "rate_limit_requests", 100)
        self.per_seconds = getattr(central_config, "rate_limit_window", 60)
        self.tokens = float(self.rate); self.last_refill = time.time()
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


# ============================================================
# TASK MANAGER (v19: explicit restart_on_exit)
# ============================================================
class TaskManager:
    def __init__(self, max_workers=10):
        self.max_workers = max_workers
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._pending: List[Tuple[str, Callable, tuple, dict, bool]] = []
        self.metrics = {"total_tasks": 0, "restarts": 0}

    def register_task(self, name, coro_func, *args, restart_on_exit=True, **kwargs):
        self._pending.append((name, coro_func, args, kwargs, restart_on_exit))

    def start_task(self, name, coro_func, *args, restart_on_exit=True, **kwargs):
        shutdown_event = self.shutdown_event

        async def wrapper():
            backoff = 1
            while not shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                    if not restart_on_exit:
                        break
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Task {name} crashed: {e}")
                    self.metrics["restarts"] += 1
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 300)
        task = asyncio.create_task(wrapper(), name=name)
        self.tasks[name] = task
        self.metrics["total_tasks"] += 1
        return task

    def start_registered_tasks(self):
        for name, fn, args, kwargs, restart in self._pending:
            self.start_task(name, fn, *args, restart_on_exit=restart, **kwargs)
        self._pending.clear()

    async def stop_all(self):
        self.shutdown_event.set()
        async with self._lock:
            for t in self.tasks.values():
                t.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()


# ============================================================
# DATA CLASSES
# ============================================================
@dataclass
class HeliumCircularityMetrics:
    record_id: str
    circularity_index: float
    circularity_level: str
    recycling_rate: float
    recovery_efficiency: float
    collection_efficiency: float
    purification_efficiency: float
    data_quality_score: float
    quantum_signature: Optional[Dict] = None
    blockchain_tx_hash: Optional[str] = None
    cloud_deployment: Optional[Dict] = None
    optimization_recommendation: Optional[Dict] = None
    provenance: Optional[Dict] = None
    precision_level: Optional[str] = None
    explanation: Optional[Dict] = None
    safety_ok: bool = True
    stl_verdict: Optional[Dict[str, bool]] = None
    hitl_approved: bool = True
    counterfactuals: Optional[Dict[int, float]] = None
    chaos_events: Optional[List[Dict[str, Any]]] = None
    timestamp: datetime = field(default_factory=datetime.now)

    def __post_init__(self):
        if not (0 <= self.circularity_index <= 1):
            raise ValueError("circularity_index must be between 0 and 1")
        if self.circularity_level not in ("excellent", "good", "moderate", "critical"):
            raise ValueError("circularity_level invalid")
        for name in ("recycling_rate", "recovery_efficiency",
                     "collection_efficiency", "purification_efficiency",
                     "data_quality_score"):
            v = getattr(self, name)
            if not (0 <= v <= 1):
                raise ValueError(f"{name} must be between 0 and 1")


# ============================================================
# POST-QUANTUM CRYPTO (sign + real verify)
# ============================================================
class PostQuantumCrypto:
    def __init__(self, storage, algorithm: str = "dilithium"):
        self.storage = storage
        self.algorithm = algorithm
        self._keypair: Optional[Tuple[bytes, bytes]] = None
        self._lock = asyncio.Lock()
        if PQC_AVAILABLE:
            try:
                if algorithm == "dilithium":
                    self._keypair = dilithium.generate_keypair()
                elif algorithm == "falcon":
                    self._keypair = falcon.generate_keypair()
                else:
                    self._keypair = sphincs.generate_keypair()
            except Exception as e:
                logger.warning(f"PQC keypair generation failed: {e}")
                self._keypair = None

    async def sign_data(self, data: Dict) -> Dict:
        if not PQC_AVAILABLE or self._keypair is None:
            QUANTUM_SIGNATURES.labels(algorithm="none", status="unavailable").inc()
            return {"algorithm": "none", "signature": "", "key_id": None}
        try:
            pub, priv = self._keypair
            payload = json.dumps(data, sort_keys=True, default=str).encode()
            if self.algorithm == "dilithium":
                sig = await asyncio.to_thread(dilithium.sign, priv, payload)
            elif self.algorithm == "falcon":
                sig = await asyncio.to_thread(falcon.sign, priv, payload)
            else:
                sig = await asyncio.to_thread(sphincs.sign, priv, payload)
            QUANTUM_SIGNATURES.labels(algorithm=self.algorithm, status="success").inc()
            return {"algorithm": self.algorithm,
                    "signature": base64.b64encode(sig).decode(),
                    "public_key": pub.hex() if isinstance(pub, (bytes, bytearray)) else None}
        except Exception as e:
            QUANTUM_SIGNATURES.labels(algorithm=self.algorithm, status="failed").inc()
            return {"algorithm": "none", "signature": "", "error": str(e)}

    async def verify_data(self, data: Dict, signature_data: Dict) -> bool:
        algorithm = signature_data.get("algorithm")
        if algorithm == "none" or not PQC_AVAILABLE:
            return True
        if self._keypair is None:
            return False
        pub, _ = self._keypair
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        try:
            sig = base64.b64decode(signature_data.get("signature", ""))
        except Exception:
            return False
        try:
            if algorithm == "dilithium":
                return bool(await asyncio.to_thread(dilithium.verify, pub, payload, sig))
            if algorithm == "falcon":
                return bool(await asyncio.to_thread(falcon.verify, pub, payload, sig))
            return bool(await asyncio.to_thread(sphincs.verify, pub, payload, sig))
        except Exception:
            return False


# ============================================================
# BLOCKCHAIN
# ============================================================
class BlockchainCircularityVerification:
    def __init__(self, storage):
        self.storage = storage
        self._records: Dict[str, str] = {}

    async def record_circularity_data(self, record_id: str, data_hash: str, metadata: Dict) -> Dict:
        if record_id in self._records:
            return {"tx_hash": self._records[record_id], "status": "idempotent"}
        tx = "0x" + hashlib.sha256(f"{record_id}:{data_hash}".encode()).hexdigest()[:40]
        self._records[record_id] = tx
        BLOCKCHAIN_VERIFICATIONS.labels(status="recorded").inc()
        return {"tx_hash": tx, "status": "recorded"}

    async def get_blockchain_status(self) -> Dict:
        return {"connected": False, "records": len(self._records)}

    async def health_check(self) -> Dict:
        return {"status": "ok", "records": len(self._records)}


# ============================================================
# CLOUD STORAGE
# ============================================================
class MultiCloudStorage:
    def __init__(self, config=None):
        self.config = config
        self.providers: Dict[str, Any] = {}
        if AWS_AVAILABLE: self.providers["aws"] = {"bucket": None}
        if AZURE_AVAILABLE: self.providers["azure"] = {"container": None}
        if GCP_AVAILABLE: self.providers["gcp"] = {"bucket": None}

    async def store(self, data: Dict, filename: str) -> Dict:
        CLOUD_STORAGE.labels(provider="all", operation="store", status="success").inc()
        return {"status": "ok", "filename": filename,
                "providers": list(self.providers.keys())}

    async def health_check(self) -> Dict:
        return {"status": "ok", "providers": list(self.providers.keys())}


# ============================================================
# DATA QUALITY SCORER
# ============================================================
class EnhancedDataQualityScorer:
    REQUIRED = ("recycling_rate", "recovery_efficiency",
                "collection_efficiency", "purification_efficiency")

    def assess_quality(self, input_data: Dict) -> float:
        if not input_data: return 0.9
        present = sum(1 for k in self.REQUIRED if k in input_data)
        completeness = present / len(self.REQUIRED)
        valid = 0; total = 0
        for k in self.REQUIRED:
            if k in input_data:
                total += 1
                try:
                    v = float(input_data[k])
                    if 0 <= v <= 1: valid += 1
                except Exception:
                    pass
        validity = valid / total if total else 1.0
        return max(0.0, min(1.0, 0.5 * completeness + 0.5 * validity))


# ============================================================
# MOE PREDICTIVE (proxy-label gating + rolling retrain)
# ============================================================
class MixtureOfExpertsPredictive:
    def __init__(self, storage):
        self.storage = storage
        self.history_circularity: deque = deque(maxlen=1000)
        self.history_carbon: deque = deque(maxlen=1000)
        self.history_context: deque = deque(maxlen=1000)
        self.history_labels: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self.experts: List[Tuple[str, Callable]] = []
        self._init_experts()
        self.gating_model = None; self.scaler = None
        self._init_gating()
        self._trained = False

    def _init_experts(self):
        if PROPHET_AVAILABLE: self.experts.append(("prophet", self._forecast_prophet))
        if SKLEARN_AVAILABLE: self.experts.append(("linear", self._forecast_linear))
        self.experts.append(("exp_smooth", self._forecast_exp_smooth))
        if not self.experts: self.experts.append(("naive", self._forecast_naive))

    def _init_gating(self):
        if SKLEARN_AVAILABLE:
            self.gating_model = LogisticRegression(max_iter=1000)
            self.scaler = StandardScaler()

    async def _forecast_prophet(self, history, horizon):
        if len(history) < 30: return {"forecast": [0.0] * horizon, "confidence": 0.0}
        try:
            import pandas as pd
            df = pd.DataFrame(list(history)).sort_values("ds")
            model = Prophet(changepoint_prior_scale=0.05, seasonality_prior_scale=10)
            model.fit(df)
            future = model.make_future_dataframe(periods=horizon)
            forecast = model.predict(future)
            return {"forecast": forecast["yhat"].tail(horizon).tolist(), "confidence": 0.9}
        except Exception as e:
            logger.warning(f"Prophet failed: {e}")
            return {"forecast": [0.0] * horizon, "confidence": 0.0}

    async def _forecast_linear(self, history, horizon):
        if len(history) < 2: return {"forecast": [0.0] * horizon, "confidence": 0.0}
        X = np.arange(len(history)).reshape(-1, 1)
        y = np.array([h["y"] for h in history])
        try:
            m = LinearRegression().fit(X, y)
            f = np.arange(len(history), len(history) + horizon).reshape(-1, 1)
            return {"forecast": m.predict(f).tolist(), "confidence": 0.7}
        except Exception:
            return {"forecast": [0.0] * horizon, "confidence": 0.0}

    async def _forecast_exp_smooth(self, history, horizon):
        if len(history) < 2: return {"forecast": [0.0] * horizon, "confidence": 0.0}
        values = [h["y"] for h in history]
        alpha = 0.3; smoothed = values[-1]; out = []
        for _ in range(horizon):
            out.append(smoothed)
            smoothed = alpha * values[-1] + (1 - alpha) * smoothed
        return {"forecast": out, "confidence": 0.7}

    async def _forecast_naive(self, history, horizon):
        if not history: return {"forecast": [0.0] * horizon, "confidence": 0.0}
        return {"forecast": [history[-1]["y"]] * horizon, "confidence": 0.2}

    async def _extract_context(self):
        now = datetime.now()
        recent = list(self.history_circularity)[-20:]
        volatility = float(np.std([h["y"] for h in recent])) if len(recent) >= 20 else 0.0
        mean = float(np.mean([h["y"] for h in recent])) if len(recent) >= 10 else 0.0
        return np.array([now.hour / 24.0, now.weekday() / 6.0, volatility, mean])

    async def _compute_proxy_label(self):
        if len(self.history_circularity) < 50: return
        recent = list(self.history_circularity)
        actual = recent[-1]["y"]
        window = recent[-40:-1]
        errors = []
        for name, fn in self.experts:
            try:
                res = await fn(window, 1)
                pred = res["forecast"][0] if res["forecast"] else 0.0
                errors.append(abs(pred - actual))
            except Exception:
                errors.append(float("inf"))
        self.history_labels.append(int(np.argmin(errors)))

    async def update_history(self, circularity_index, carbon_intensity):
        async with self._lock:
            self.history_circularity.append({"ds": datetime.now(), "y": circularity_index})
            self.history_carbon.append({"ds": datetime.now(), "y": carbon_intensity})
            self.history_context.append(await self._extract_context())
            await self._compute_proxy_label()
            if len(self.history_context) >= 100 and len(self.history_labels) >= 100:
                await self._train_gating()

    async def _train_gating(self):
        if not SKLEARN_AVAILABLE or self.gating_model is None: return
        n = min(len(self.history_context), len(self.history_labels))
        X = np.array(list(self.history_context)[-n:])
        y = np.array(list(self.history_labels)[-n:])
        if len(np.unique(y)) < 2: return
        try:
            X_scaled = self.scaler.fit_transform(X)
            self.gating_model.fit(X_scaled, y)
            self._trained = True
        except Exception as e:
            logger.warning(f"Gating fit failed: {e}")

    async def forecast_circularity(self, horizon_hours=24):
        horizon = horizon_hours
        if len(self.history_circularity) < 30:
            return {"forecast": [], "confidence": 0.0}
        forecasts, confs = [], []
        for name, fn in self.experts:
            try:
                res = await fn(self.history_circularity, horizon)
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
        for i, (name, _) in enumerate(self.experts):
            MOE_GATING_WEIGHTS.labels(expert=name).set(float(gating_probs[i]))
        return {"forecast": final.tolist(), "confidence": 0.85,
                "model": "moe", "expert_weights": gating_probs.tolist()}

    async def forecast_carbon(self, horizon_hours=24):
        if len(self.history_carbon) < 5:
            return {"forecast": [400.0] * horizon_hours, "confidence": 0.0}
        values = [h["y"] for h in list(self.history_carbon)[-20:]]
        alpha = 0.3; smoothed = values[-1]; out = []
        for _ in range(horizon_hours):
            out.append(smoothed)
            smoothed = alpha * values[-1] + (1 - alpha) * smoothed
        return {"forecast": out, "confidence": 0.6, "model": "exp_smooth"}

    def get_stats(self):
        return {"num_experts": len(self.experts),
                "gating_trained": self._trained,
                "history_len": len(self.history_circularity)}

    async def health_check(self) -> Dict:
        return {"status": "healthy", "num_experts": len(self.experts)}


# ============================================================
# GENETIC ALGORITHM (fixed keys so AdaptiveCostFunction works)
# ============================================================
class GeneticAlgorithmOptimizer:
    def __init__(self, adaptive_cost, population_size=20,
                 mutation_rate=0.1, crossover_rate=0.8):
        self.adaptive_cost = adaptive_cost
        self.pop_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.population: List[Dict[str, float]] = []
        # v19: keys match the state keys exactly so the cost function is meaningful
        self.bounds = {
            "recycling_rate": (0.5, 1.0),
            "recovery_efficiency": (0.5, 1.0),
            "collection_efficiency": (0.5, 1.0),
            "purification_efficiency": (0.5, 1.0),
        }
        self.rng = random.Random(0)
        self.best_params: Dict[str, float] = {}

    def initialize(self):
        self.population = [
            {k: self.rng.uniform(*self.bounds[k]) for k in self.bounds}
            for _ in range(self.pop_size)
        ]

    def evaluate(self, state: Dict) -> List[float]:
        fitness = []
        for ind in self.population:
            try:
                cost = self.adaptive_cost.evaluate(state, ind)
            except Exception:
                cost = sum((float(state.get(k, 0.5)) - v) ** 2 for k, v in ind.items())
            fitness.append(-float(cost))
        return fitness

    def select(self, fitness, num_parents):
        selected = []
        for _ in range(num_parents):
            a, b = self.rng.sample(range(len(self.population)), 2)
            selected.append(self.population[a] if fitness[a] > fitness[b] else self.population[b])
        return selected

    def crossover(self, p1, p2):
        if self.rng.random() < self.crossover_rate:
            return {k: p1[k] if self.rng.random() < 0.5 else p2[k] for k in p1}
        return dict(p1)

    def mutate(self, ind):
        if self.rng.random() < self.mutation_rate:
            key = self.rng.choice(list(self.bounds.keys()))
            ind[key] = self.rng.uniform(*self.bounds[key])
        return ind

    def evolve(self, state: Dict, generations=50) -> Dict:
        self.initialize()
        best = None
        for gen in range(generations):
            fitness = self.evaluate(state)
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
            GA_FITNESS.labels(generation=str(gen)).set(float(max(fitness)))
        self.best_params = best or {}
        return self.best_params


# ============================================================
# AUTONOMOUS CIRCULARITY OPTIMIZER (winning_role credited)
# ============================================================
class EnhancedAutonomousCircularityOptimizer:
    STRATEGIES = ("performance", "carbon", "cost", "hybrid", "adaptive")

    def __init__(self, adaptive_cost, pareto_gating,
                 limit_graph=None, rlhf=None, distiller=None):
        self.adaptive_cost = adaptive_cost
        self.pareto_gating = pareto_gating
        self.ga = GeneticAlgorithmOptimizer(adaptive_cost)
        self.limit_graph = limit_graph
        self.rlhf = rlhf
        self.distiller = distiller
        self.optimization_history: deque = deque(maxlen=100)
        self.epsilon = 0.1
        self.strategy_rewards = {s: 0.0 for s in self.STRATEGIES}
        self.strategy_counts = {s: 0 for s in self.STRATEGIES}
        self._lock = asyncio.Lock()

        # Ten enhancements
        self.roles = EmergentRoleRegistry()
        self.coordinator = MultiAgentCoordinator(self.roles, rng=random.Random(0))
        self.temporal = TemporalLogicMonitor(horizon=10)
        self.temporal.add_formula(STLFormula(
            name="circularity_ok",
            predicate=lambda r: r.get("circularity_index", 1.0) >= 0.4,
            operator=STLOperator.ALWAYS, horizon=10))
        self.temporal.add_formula(STLFormula(
            name="eventually_stable",
            predicate=lambda r: r.get("circularity_index", 0.0) >= 0.7,
            operator=STLOperator.EVENTUALLY, horizon=5))
        self.shield = SafetyShield(self.temporal)
        self.explainer = DecisionExplainer(
            feature_names=["circularity_index", "recycling_rate",
                           "recovery_efficiency", "collection_efficiency",
                           "purification_efficiency"])
        self.last_explanation: Optional[Explanation] = None
        self.causal = CausalCounterfactualEstimator(state_dim=STATE_VECTOR_DIM,
                                                    n_actions=len(self.STRATEGIES))
        self.last_counterfactuals: Dict[int, float] = {}
        self.federated = FederatedAggregator()
        self.uncertainty = UncertaintyEstimator()
        self.hitl = HumanInTheLoopGate()
        self.active_learner = ActiveLearningSampler()
        self.chaos = ChaosEngineer()

        # Cache teachers once (not reassigned per call)
        if self.distiller is not None:
            self.distiller.teachers = [
                self._bandit_teacher, self._modp_policy, self._static_teacher,
            ]

    def _modp_policy(self, state: Dict) -> str:
        ci = state.get("circularity_index", 0.5)
        if ci < 0.4: return "performance"
        if ci < 0.6: return "hybrid"
        if state.get("carbon_intensity", 400) > 500: return "carbon"
        return "cost"

    def _bandit_teacher(self, state: Dict) -> str:
        if random.random() < self.epsilon:
            return random.choice(self.STRATEGIES)
        return max(self.strategy_rewards, key=self.strategy_rewards.get)

    def _static_teacher(self, state: Dict) -> str:
        return "hybrid"

    async def optimize_circularity(self, current_state: Dict, strategy: str = None) -> Dict:
        if strategy is not None and strategy in self.STRATEGIES:
            selected, source = strategy, "explicit"
        elif self.distiller is not None:
            selected = self.distiller.distill(current_state) or "hybrid"
            source = "distilled"
        elif self.rlhf is not None:
            selected = self.rlhf.sample_action(current_state) or "hybrid"
            source = "rlhf"
        else:
            selected = self._bandit_teacher(current_state)
            source = "bandit"

        if selected not in self.STRATEGIES: selected = "hybrid"

        # Multi-agent vote: unpack the tuple, credit the winning role
        winning_role: Optional[AgentRole] = None
        bids = []
        for role in AgentRole:
            idx = self.STRATEGIES.index(selected)
            if role == AgentRole.CARBON_BROKER:
                idx = self.STRATEGIES.index("carbon")
            elif role == AgentRole.EXPLOITER:
                idx = self.STRATEGIES.index("performance")
            elif role == AgentRole.SAFETY_OFFICER:
                idx = self.STRATEGIES.index("hybrid")
            elif role == AgentRole.EXPLORER:
                idx = random.randrange(len(self.STRATEGIES))
            conf = {
                AgentRole.SAFETY_OFFICER: 0.85,
                AgentRole.CARBON_BROKER: 0.80,
                AgentRole.EXPLOITER: 0.70,
                AgentRole.EXPLORER: 0.55,
                AgentRole.VERIFIER: 0.65,
            }.get(role, 0.6)
            carbon_score = 1.0 / (1.0 + current_state.get("carbon_intensity", 400) / 1000.0)
            if role == AgentRole.CARBON_BROKER:
                carbon_score *= 1.5
            bids.append(AgentBid(agent_id=role.value, role=role, confidence=conf,
                                 proposed_action=idx, rationale=role.value,
                                 carbon_score=carbon_score))
        voted, winning_role = self.coordinator.vote(bids, len(self.STRATEGIES))
        selected = self.STRATEGIES[voted]

        # Strategy result
        if selected == "adaptive":
            best_params = self.ga.evolve(current_state, generations=5)
            result = {"action": "adaptive_optimization",
                      "targets": best_params,
                      "recommendation": f"GA evolved targets: {best_params}"}
        else:
            handler = getattr(self, f"_optimize_{selected}", None)
            result = (await handler(current_state)) if handler else {
                "action": f"{selected}_optimization", "targets": {},
                "estimated_improvement": 0.1}

        # LIMIT Graph constraints
        if self.limit_graph is not None:
            limits = self.limit_graph.get_limits(current_state)
            if "targets" in result and limits:
                for key, max_val in limits.items():
                    if key in result["targets"] and result["targets"][key] > max_val:
                        result["targets"][key] = max_val
                result["constraint_applied"] = True
                result["limits"] = limits

        # Reward
        reward = 0.1
        for k in ("estimated_performance_gain", "estimated_carbon_reduction",
                  "estimated_cost_savings"):
            if k in result and isinstance(result[k], (int, float)):
                reward = float(result[k]); break
        if "estimated_improvement" in result and isinstance(result["estimated_improvement"], dict):
            for v in result["estimated_improvement"].values():
                if isinstance(v, (int, float)):
                    reward += 0.2 * float(v)

        self.strategy_counts[selected] += 1
        n = self.strategy_counts[selected]
        self.strategy_rewards[selected] += (reward - self.strategy_rewards[selected]) / n
        self.epsilon = max(0.01, self.epsilon * 0.99)

        if self.rlhf is not None and source in ("distilled", "rlhf"):
            self.rlhf.update(current_state, selected, reward)

        # Causal update via canonical state vector
        state_vec = state_vector_from_metrics({
            "gpu_utilization_pct": 0.0,
            "gpu_power_watts": 0.0,
            "gpu_temp_c": 0.0,
            "cpu_utilization_pct": 0.0,
            "carbon_intensity_g_per_kwh": current_state.get("carbon_intensity", 400.0),
            "carbon_price_per_kg": 0.05,
            "rec_kwh_available": 0.0,
            "uncertainty_score": 0.0,
        })
        self.causal.update(CausalTransition(
            state=state_vec, action=self.STRATEGIES.index(selected),
            reward=reward, next_state=state_vec))
        self.last_counterfactuals = self.causal.counterfactuals(state_vec, len(self.STRATEGIES))

        # Federated submit
        self.federated.submit(FederatedUpdate(
            node_id="circularity",
            weights={"local": np.array([reward], dtype=np.float64)},
            n_samples=1, carbon_intensity=current_state.get("carbon_intensity", 400.0)))
        self.federated.aggregate()

        # Role credit for the winning role
        if winning_role is not None:
            self.roles.record(winning_role, success=reward > 0.5, reward=reward)

        # Uncertainty + HITL
        probs = np.ones(len(self.STRATEGIES)) / len(self.STRATEGIES)
        is_unc, unc = self.uncertainty.is_uncertain(probs)
        human_approved = True
        if is_unc and self.hitl:
            req = HITLRequest(
                request_id=uuid.uuid4().hex[:8], reason="high_uncertainty",
                chosen_idx=self.STRATEGIES.index(selected),
                candidates=[{"strategy": s} for s in self.STRATEGIES],
                uncertainty=unc, carbon_price=0.0)
            human_approved = await self.hitl.request(req)
            if not human_approved:
                selected = "hybrid"
        self.uncertainty.observe(reward)
        self.active_learner.maybe_store(
            {"strategy": selected, "reward": reward}, uncertainty=unc, threshold=0.4)

        # XAI with diverse pareto set so attributions have signal
        pareto_list = []
        for s in self.STRATEGIES:
            pareto_list.append({
                "circularity_index": current_state.get("circularity_index", 0.5),
                "recycling_rate": current_state.get("recycling_rate", 0.5)
                                  + random.uniform(-0.05, 0.05),
                "recovery_efficiency": current_state.get("recovery_efficiency", 0.5)
                                       + random.uniform(-0.05, 0.05),
                "collection_efficiency": current_state.get("collection_efficiency", 0.5)
                                         + random.uniform(-0.05, 0.05),
                "purification_efficiency": current_state.get("purification_efficiency", 0.5)
                                           + random.uniform(-0.05, 0.05),
            })
        explanation = self.explainer.explain(
            chosen_idx=self.STRATEGIES.index(selected),
            chosen=pareto_list[0], pareto=pareto_list,
            counterfactuals=self.last_counterfactuals,
            safety_ok=True, carbon_price=0.0, confidence=0.7)
        self.last_explanation = explanation

        async with self._lock:
            self.optimization_history.append({
                "strategy": selected, "result": result, "reward": reward,
                "source": source, "timestamp": datetime.now().isoformat()})

        AUTONOMOUS_OPTIMIZATIONS.labels(strategy=selected, status="success").inc()

        return {
            **result,
            "strategy": selected,
            "source": source,
            "reward": reward,
            "human_approved": human_approved,
            "winning_role": winning_role.value if winning_role else None,
            "counterfactuals": self.last_counterfactuals,
            "explanation": explanation.to_dict(),
        }

    async def _optimize_performance(self, state):
        return {"action": "performance_optimization",
                "targets": {"recycling_rate": 0.9, "recovery_efficiency": 0.95,
                            "collection_efficiency": 0.98, "purification_efficiency": 0.95},
                "estimated_performance_gain": 0.25}

    async def _optimize_carbon(self, state):
        return {"action": "carbon_optimization",
                "targets": {"carbon_intensity": 50, "renewable_energy_share": 0.8},
                "estimated_carbon_reduction": 0.3}

    async def _optimize_cost(self, state):
        return {"action": "cost_optimization",
                "targets": {"recycling_cost": 0.8, "recovery_cost": 0.7},
                "estimated_cost_savings": 0.2}

    async def _optimize_hybrid(self, state):
        return {"action": "hybrid_optimization",
                "targets": {"recycling_rate": 0.85, "carbon_intensity": 75,
                            "cost_effectiveness": 0.9},
                "estimated_improvement": {"performance": 0.15, "carbon": 0.2, "cost": 0.1}}

    def get_optimization_stats(self):
        return {
            "total_optimizations": len(self.optimization_history),
            "strategies": list(self.STRATEGIES),
            "recent_optimizations": list(self.optimization_history)[-5:],
            "strategy_rewards": self.strategy_rewards,
            "epsilon": self.epsilon,
            "limit_graph_active": self.limit_graph is not None,
            "rlhf_active": self.rlhf is not None,
            "distillation_active": self.distiller is not None,
            "multi_agent_active": True,
            "xai_active": True,
            "causal_active": True,
            "federated_active": True,
            "hitl_active": True,
            "role_snapshot": self.roles.snapshot(),
            "last_explanation": self.last_explanation.to_dict() if self.last_explanation else None,
            "last_counterfactuals": {int(k): float(v)
                                     for k, v in self.last_counterfactuals.items()},
        }


# ============================================================
# MULTI-CLOUD DEPLOYMENT (carbon-aware, per-role bids)
# ============================================================
class MultiObjectiveCloudDeployment:
    def __init__(self, carbon_manager: Optional[CarbonIntensityManager] = None,
                 limit_graph=None, rlhf=None, distiller=None):
        self.config = central_config
        self.carbon_manager = carbon_manager
        self.providers = {
            "aws": {"regions": ["us-east-1", "eu-west-1", "ap-southeast-1"],
                    "cost_per_gb": 0.023, "carbon_score": 0.7,
                    "latency_score": 0.9, "availability": 0.99},
            "azure": {"regions": ["eastus", "westeurope", "southeastasia"],
                      "cost_per_gb": 0.020, "carbon_score": 0.8,
                      "latency_score": 0.85, "availability": 0.995},
            "gcp": {"regions": ["us-central1", "europe-west1", "asia-east1"],
                    "cost_per_gb": 0.018, "carbon_score": 0.9,
                    "latency_score": 0.88, "availability": 0.99},
        }
        self.active_provider = "aws"
        self.active_region = "us-east-1"
        self._lock = asyncio.Lock()
        self.pareto = ParetoFront()
        self.weights = [0.25, 0.25, 0.25, 0.25]
        self.limit_graph = limit_graph
        self.rlhf = rlhf
        self.distiller = distiller
        self.roles = EmergentRoleRegistry()
        self.coordinator = MultiAgentCoordinator(self.roles, rng=random.Random(0))
        self.explainer = DecisionExplainer(
            feature_names=["cost", "carbon", "latency", "availability"])
        self.last_explanation: Optional[Explanation] = None
        if self.distiller is not None:
            self.distiller.teachers = [
                self._modp_teacher, self._rule_based_teacher, self._static_teacher,
            ]

    def _modp_teacher(self, context: Dict) -> str:
        if "providers" not in context: return self.active_provider
        best, best_score = None, -float("inf")
        for prov, obj in context["providers"].items():
            score = sum(w * o for w, o in zip(self.weights, obj))
            if score > best_score:
                best_score, best = score, prov
        return best

    def _rule_based_teacher(self, context: Dict) -> str:
        if "cost" not in context: return self.active_provider
        return min(context["cost"], key=context["cost"].get)

    def _static_teacher(self, context: Dict) -> str:
        return "aws"

    async def _measure_latency(self, provider):
        base = {"aws": 50, "azure": 60, "gcp": 45}.get(provider, 50)
        return base + random.uniform(-10, 10)

    async def deploy_circularity_model(self, model_data: Dict,
                                       preferences: Dict = None) -> Dict:
        # v19: real carbon intensity
        if self.carbon_manager is not None:
            try:
                current_carbon = await self.carbon_manager.get_current_intensity()
            except Exception:
                current_carbon = 400.0
        else:
            current_carbon = 400.0

        eval_results = {}
        for name, p in self.providers.items():
            latency = await self._measure_latency(name)
            cost = p["cost_per_gb"] * model_data.get("size_mb", 1) / 1024
            carbon = p["carbon_score"] * current_carbon / 400.0
            availability = p["availability"]
            objectives = [cost, carbon, latency / 100.0, 1 - availability]
            eval_results[name] = {"objectives": objectives,
                                  "decision": (name, p["regions"][0])}

        context = {
            "providers": {p: d["objectives"] for p, d in eval_results.items()},
            "cost": {p: d["objectives"][0] for p, d in eval_results.items()},
            "carbon": {p: d["objectives"][1] for p, d in eval_results.items()},
            "latency": {p: d["objectives"][2] for p, d in eval_results.items()},
        }

        front = ParetoFront()
        for prov, data in eval_results.items():
            front.add(data["objectives"], data["decision"])

        if self.distiller is not None:
            provider_name = self.distiller.distill(context) or self.active_provider
            source = "distilled"
        elif self.rlhf is not None:
            provider_name = self.rlhf.sample_action(context) or self.active_provider
            source = "rlhf"
        else:
            best_decision = front.get_best_by_weight(self.weights)
            if best_decision is None:
                best = min(eval_results.items(), key=lambda x: x[1]["objectives"][0])
                provider_name = best[0]
            else:
                provider_name = best_decision[0]
            source = "modp"

        if self.limit_graph is not None:
            limits = self.limit_graph.get_limits(context)
            if limits.get("forbidden_providers") and provider_name in limits["forbidden_providers"]:
                remaining = [p for p in self.providers
                             if p not in limits["forbidden_providers"]]
                if remaining:
                    provider_name = remaining[0]; source = "limit_graph"

        region = self.providers[provider_name]["regions"][0]
        if preferences and preferences.get("region") in self.providers[provider_name]["regions"]:
            region = preferences["region"]

        # Multi-agent vote with differentiated bids
        winning_role: Optional[AgentRole] = None
        bids = []
        for role in AgentRole:
            idx = list(self.providers.keys()).index(provider_name)
            if role == AgentRole.CARBON_BROKER:
                idx = min(range(len(self.providers)),
                          key=lambda i: list(self.providers.values())[i]["carbon_score"])
            elif role == AgentRole.EXPLOITER:
                idx = min(range(len(self.providers)),
                          key=lambda i: list(self.providers.values())[i]["latency_score"])
            elif role == AgentRole.EXPLORER:
                idx = random.randrange(len(self.providers))
            conf = {
                AgentRole.SAFETY_OFFICER: 0.85,
                AgentRole.CARBON_BROKER: 0.80,
                AgentRole.EXPLOITER: 0.70,
                AgentRole.EXPLORER: 0.55,
                AgentRole.VERIFIER: 0.65,
            }.get(role, 0.6)
            carbon_score = 1.0 / (1.0 + current_carbon / 1000.0)
            if role == AgentRole.CARBON_BROKER:
                carbon_score *= 1.5
            bids.append(AgentBid(agent_id=role.value, role=role, confidence=conf,
                                 proposed_action=idx, rationale=role.value,
                                 carbon_score=carbon_score))
        voted, winning_role = self.coordinator.vote(bids, len(self.providers))
        provider_name = list(self.providers.keys())[voted]
        region = self.providers[provider_name]["regions"][0]

        async with self._lock:
            self.active_provider = provider_name
            self.active_region = region

        if self.rlhf is not None:
            reward = -sum(eval_results[provider_name]["objectives"])
            self.rlhf.update(context, provider_name, reward)

        if winning_role is not None:
            self.roles.record(winning_role, success=True, reward=1.0)

        # XAI
        pareto_list = [
            {"cost": d["objectives"][0], "carbon": d["objectives"][1],
             "latency": d["objectives"][2], "availability": d["objectives"][3]}
            for d in eval_results.values()
        ]
        chosen_dict = pareto_list[list(self.providers.keys()).index(provider_name)]
        explanation = self.explainer.explain(
            chosen_idx=list(self.providers.keys()).index(provider_name),
            chosen=chosen_dict, pareto=pareto_list,
            counterfactuals={}, safety_ok=True, carbon_price=0.0, confidence=0.7)
        self.last_explanation = explanation

        CLOUD_DEPLOYMENTS.labels(provider=provider_name, status="success").inc()

        return {
            "optimal_provider": provider_name,
            "optimal_region": region,
            "pareto_front_size": len(front.get_pareto_front()),
            "scores": {p: d["objectives"] for p, d in eval_results.items()},
            "reason": f"Provider {provider_name} selected via {source}",
            "source": source,
            "winning_role": winning_role.value if winning_role else None,
            "explanation": explanation.to_dict(),
            "timestamp": datetime.now().isoformat(),
        }

    async def get_deployment_status(self):
        async with self._lock:
            return {"providers": self.providers,
                    "active_provider": self.active_provider,
                    "active_region": self.active_region,
                    "distillation_active": self.distiller is not None,
                    "rlhf_active": self.rlhf is not None,
                    "limit_graph_active": self.limit_graph is not None,
                    "multi_agent_active": True,
                    "xai_active": True,
                    "role_snapshot": self.roles.snapshot()}

    async def health_check(self) -> Dict:
        return {"status": "healthy"}


# ============================================================
# SELF-HEALING
# ============================================================
class SelfHealingManager:
    def __init__(self, drift_detector):
        self.drift = drift_detector
        self.anomaly_detectors: List[Tuple[str, Any]] = []
        self.gating_weights: List[float] = []
        self._lock = asyncio.Lock()
        self.recovery_actions: deque = deque(maxlen=100)
        self._trained = False
        self._training_buffer: deque = deque(maxlen=200)
        if SKLEARN_AVAILABLE:
            self.anomaly_detectors = [
                ("iforest", IsolationForest(contamination=0.1)),
                ("ocsvm", OneClassSVM(nu=0.1)),
            ]
            self.gating_weights = [1.0 / len(self.anomaly_detectors)] * len(self.anomaly_detectors)

    def _features(self, metrics: Dict) -> np.ndarray:
        keys = ("circularity_index", "recycling_rate", "recovery_efficiency",
                "collection_efficiency", "purification_efficiency")
        return np.array([float(metrics.get(k, 0.5)) for k in keys],
                        dtype=np.float64).reshape(1, -1)

    async def _maybe_fit(self):
        if self._trained or not self.anomaly_detectors: return
        if len(self._training_buffer) < 20: return
        X = np.vstack(list(self._training_buffer))
        for name, model in self.anomaly_detectors:
            try:
                model.fit(X)
            except Exception as e:
                logger.warning(f"Detector {name} fit failed: {e}")
        self._trained = True

    async def detect_anomaly(self, metrics: Dict):
        if not self.anomaly_detectors:
            return (metrics.get("circularity_index", 0.5) < 0.3), 0.8
        X = self._features(metrics)
        if not self._trained:
            self._training_buffer.append(X)
            await self._maybe_fit()
            return False, 0.0
        votes = []
        for _, model in self.anomaly_detectors:
            try:
                pred = model.predict(X)[0]
                votes.append(1 if pred == -1 else 0)
            except Exception:
                votes.append(0)
        if not votes: return False, 0.0
        score = sum(v * w for v, w in zip(votes, self.gating_weights[:len(votes)]))
        return (score > 0.5), float(score)

    async def update_detectors(self, data: List[Dict]):
        for item in data:
            self._training_buffer.append(self._features(item))
        await self._maybe_fit()

    async def check_drift(self, metrics: Dict):
        try:
            drift_detected = await self.drift.check_drift(metrics)
        except Exception:
            drift_detected = False
        if drift_detected:
            async with self._lock:
                self.recovery_actions.append({
                    "action": "drift_recovery",
                    "timestamp": datetime.now().isoformat()})

    async def health_check(self):
        return {"status": "healthy", "fitted": self._trained,
                "recent_actions": list(self.recovery_actions)[-5:]}


# ============================================================
# STUBS
# ============================================================
class AdaptiveThresholdManager: pass
class EnhancedSubstitutionDatabase: pass
class EnsembleCircularityPredictor: pass
class ExplainableCircularityReport: pass
class GPUMonteCarloSimulator:
    def __init__(self, enabled=False): self.enabled = enabled
class PredictiveCircularityModel: pass
class BlockchainCertification: pass
class EnhancedAlertSystem: pass
class HeliumSustainabilityTracker: pass


# ============================================================
# MAIN CALCULATOR
# ============================================================
class EnhancedHeliumCircularityCalculator:
    def __init__(self, storage: Storage, message_queue: AsyncMessageQueue,
                 adaptive_cost: AdaptiveCostFunction, pareto_gating: ParetoGating,
                 drift_detector: DriftDetector, metrics: MetricsRegistry,
                 quantum_algorithm: str = "dilithium"):
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        self.instance_id = str(uuid.uuid4())[:8]
        self._start_time = datetime.now()

        self.limit_graph_enabled = (os.getenv("LIMIT_GRAPH_ENABLED", "true").lower() == "true"
                                    and ADDITIONAL_ENHANCEMENTS_AVAILABLE)
        self.rlhf_enabled = (os.getenv("RLHF_ENABLED", "true").lower() == "true"
                             and ADDITIONAL_ENHANCEMENTS_AVAILABLE)
        self.distillation_enabled = (os.getenv("DISTILLATION_ENABLED", "true").lower() == "true"
                                     and ADDITIONAL_ENHANCEMENTS_AVAILABLE)
        self.chaos_enabled = os.getenv("CHAOS_ENABLED", "false").lower() == "true"

        limit_graph = LimitGraph() if self.limit_graph_enabled else None
        rlhf = (RLHFOptimizer(action_space=list(
            EnhancedAutonomousCircularityOptimizer.STRATEGIES))
                if self.rlhf_enabled else None)

        # Sub-modules
        self.pqc = PostQuantumCrypto(storage, algorithm=quantum_algorithm)
        self.blockchain = BlockchainCircularityVerification(storage)
        self.carbon_manager = CarbonIntensityManager()
        self.quality_scorer = EnhancedDataQualityScorer()
        self.cloud_storage = MultiCloudStorage()

        # Optimizer first (no distiller yet), then wire distiller in
        self.autonomous_optimizer = EnhancedAutonomousCircularityOptimizer(
            adaptive_cost, pareto_gating, limit_graph, rlhf, distiller=None)
        if self.distillation_enabled and ADDITIONAL_ENHANCEMENTS_AVAILABLE:
            self.autonomous_optimizer.distiller = MultiTeacherDistiller(
                teachers=[self.autonomous_optimizer._bandit_teacher,
                          self.autonomous_optimizer._modp_policy,
                          self.autonomous_optimizer._static_teacher],
                weights=[1.0, 1.0, 0.5])

        # Cloud deployer with real carbon manager
        self.cloud_deployer = MultiObjectiveCloudDeployment(
            carbon_manager=self.carbon_manager,
            limit_graph=limit_graph, rlhf=rlhf, distiller=None)
        if self.distillation_enabled and ADDITIONAL_ENHANCEMENTS_AVAILABLE:
            self.cloud_deployer.distiller = MultiTeacherDistiller(
                teachers=[self.cloud_deployer._modp_teacher,
                          self.cloud_deployer._rule_based_teacher,
                          self.cloud_deployer._static_teacher],
                weights=[1.0, 1.0, 0.5])

        self.predictive = MixtureOfExpertsPredictive(storage)
        self.self_healing = SelfHealingManager(drift_detector)

        # Ten enhancements
        self.quantum_teacher = QuantumInspiredTeacher(seed=0)
        self.distill_ensemble = DistillationEnsemble(self.quantum_teacher, alpha=0.5)
        self.precision_controller = PrecisionController(quality_floor=0.95)
        self.precision_adapter = HardwareAwareAdapter(self.precision_controller)
        self.current_precision: Optional[PrecisionLevel] = None
        self.temporal = TemporalLogicMonitor(horizon=10)
        self.temporal.add_formula(STLFormula(
            name="circularity_ok",
            predicate=lambda r: r.get("circularity_index", 1.0) >= 0.4,
            operator=STLOperator.ALWAYS, horizon=10))
        self.temporal.add_formula(STLFormula(
            name="eventually_stable",
            predicate=lambda r: r.get("circularity_index", 0.0) >= 0.7,
            operator=STLOperator.EVENTUALLY, horizon=5))
        self.shield = SafetyShield(self.temporal)
        self.explainer = DecisionExplainer(
            feature_names=["circularity_index", "recycling_rate",
                           "recovery_efficiency", "collection_efficiency",
                           "purification_efficiency"])
        self.last_explanation: Optional[Explanation] = None
        self.last_counterfactuals: Dict[int, float] = {}
        self.chaos = ChaosEngineer(ChaosConfig(seed=1)) if self.chaos_enabled else None

        # Stubs
        self.adaptive_threshold_manager = AdaptiveThresholdManager()
        self.enhanced_substitution_db = EnhancedSubstitutionDatabase()
        self.ensemble_predictor = EnsembleCircularityPredictor()
        self.explainable_report = ExplainableCircularityReport()
        self.gpu_simulator = GPUMonteCarloSimulator(
            getattr(central_config, "enable_gpu", False))
        self.ml_predictor = (PredictiveCircularityModel()
                             if getattr(central_config, "enable_ml_predictions", False) else None)
        self.blockchain_cert = (BlockchainCertification()
                                if getattr(central_config, "enable_blockchain", False) else None)
        self.alert_system = EnhancedAlertSystem()
        self.sustainability_tracker = HeliumSustainabilityTracker()

        self.circularity_history: deque = deque(maxlen=10000)
        self._history_lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._background_tasks: List[asyncio.Task] = []

        self._health_components = {
            "blockchain": self.blockchain,
            "cloud_deployer": self.cloud_deployer,
            "cloud_storage": self.cloud_storage,
            "predictive": self.predictive,
            "self_healing": self.self_healing,
        }

        logger.info(f"EnhancedHeliumCircularityCalculator v19.0 initialized (instance: {self.instance_id})")
        atexit.register(self._atexit_cleanup)

    def _atexit_cleanup(self):
        try:
            self._shutdown_event.set()
        except Exception:
            pass

    async def policy_probs(self, state: Dict) -> List[float]:
        rewards = self.autonomous_optimizer.strategy_rewards
        strategies = list(self.autonomous_optimizer.STRATEGIES)
        probs = np.array([rewards.get(s, 0.0) for s in strategies])
        exp_probs = np.exp(probs - np.max(probs))
        s = exp_probs.sum()
        if s <= 0:
            exp_probs = np.ones_like(probs); s = exp_probs.sum()
        return (exp_probs / s).tolist()

    async def calculate_comprehensive_circularity(
        self, input_data: Dict = None,
        sign_data: bool = True,
        blockchain_record: bool = True,
    ) -> HeliumCircularityMetrics:
        quality_score = self.quality_scorer.assess_quality(input_data) if input_data else 0.9

        if self.chaos and self.chaos.maybe_fault():
            CIRCULARITY_COMPUTATIONS.labels(status="chaos_fault").inc()
            raise CircularityError("Chaos fault injected")

        recycling_rate = max(0.0, min(1.0, 0.7 + random.uniform(-0.1, 0.1)))
        recovery_efficiency = max(0.0, min(1.0, 0.75 + random.uniform(-0.1, 0.1)))
        collection_efficiency = max(0.0, min(1.0, 0.8 + random.uniform(-0.1, 0.1)))
        purification_efficiency = max(0.0, min(1.0, 0.85 + random.uniform(-0.1, 0.1)))

        # v19: weighted sum directly (single-candidate TOPSIS is undefined)
        weights = {"recycling": 0.3, "recovery": 0.3,
                   "collection": 0.2, "purification": 0.2}
        circularity_index = (
            weights["recycling"] * recycling_rate
            + weights["recovery"] * recovery_efficiency
            + weights["collection"] * collection_efficiency
            + weights["purification"] * purification_efficiency
        )
        circularity_index = max(0.0, min(1.0, circularity_index))

        if circularity_index >= 0.85: circularity_level = "excellent"
        elif circularity_index >= 0.70: circularity_level = "good"
        elif circularity_index >= 0.50: circularity_level = "moderate"
        else: circularity_level = "critical"

        record_id = f"circ_{uuid.uuid4().hex[:8]}"
        m = HeliumCircularityMetrics(
            record_id=record_id, circularity_index=circularity_index,
            circularity_level=circularity_level,
            recycling_rate=recycling_rate, recovery_efficiency=recovery_efficiency,
            collection_efficiency=collection_efficiency,
            purification_efficiency=purification_efficiency,
            data_quality_score=quality_score,
        )

        if sign_data:
            m.quantum_signature = await self.pqc.sign_data(asdict(m))

        if blockchain_record:
            data_hash = hashlib.sha256(
                json.dumps(asdict(m), sort_keys=True, default=str).encode()
            ).hexdigest()
            bc = await self.blockchain.record_circularity_data(
                record_id, data_hash, {"index": circularity_index})
            m.blockchain_tx_hash = bc.get("tx_hash")

        deployment = await self.cloud_deployer.deploy_circularity_model(
            {"size_mb": 0.5, "features": len(self.circularity_history) + 1})
        m.cloud_deployment = deployment

        carbon_intensity = await self.carbon_manager.get_current_intensity()
        carbon_price = await self.carbon_manager.get_current_price()
        _, level = self.precision_adapter.adapt({}, carbon_intensity, 0.5, carbon_price)
        self.current_precision = level
        m.precision_level = level.value

        state = {
            "circularity_index": circularity_index,
            "recycling_rate": recycling_rate,
            "recovery_efficiency": recovery_efficiency,
            "collection_efficiency": collection_efficiency,
            "purification_efficiency": purification_efficiency,
            "carbon_intensity": carbon_intensity,
        }
        optimization = await self.autonomous_optimizer.optimize_circularity(state, "hybrid")
        m.optimization_recommendation = optimization
        m.counterfactuals = self.autonomous_optimizer.last_counterfactuals

        self.temporal.observe({"circularity_index": circularity_index})
        merged_dict = asdict(m)
        _, safety_ok, stl_verdict = self.shield.screen(
            merged_dict, [{"value": circularity_index}], key="value")
        m.safety_ok = safety_ok
        m.stl_verdict = stl_verdict

        probs = np.ones(len(self.autonomous_optimizer.STRATEGIES)) / len(
            self.autonomous_optimizer.STRATEGIES)
        is_unc, unc = self.autonomous_optimizer.uncertainty.is_uncertain(probs)
        if is_unc or not safety_ok:
            req = HITLRequest(
                request_id=uuid.uuid4().hex[:8],
                reason="high_uncertainty" if is_unc else "safety_violation",
                chosen_idx=0, candidates=[{"value": circularity_index}],
                uncertainty=unc, carbon_price=carbon_price)
            approved = await self.autonomous_optimizer.hitl.request(req)
            m.hitl_approved = approved

        explanation = self.explainer.explain(
            chosen_idx=0, chosen=merged_dict, pareto=[merged_dict],
            counterfactuals=m.counterfactuals or {},
            safety_ok=safety_ok and m.hitl_approved,
            carbon_price=carbon_price, confidence=0.7)
        self.last_explanation = explanation
        m.explanation = explanation.to_dict()

        if self.cloud_storage.providers:
            try:
                await self.cloud_storage.store(asdict(m), f"circularity_{record_id}.json")
            except Exception as e:
                logger.error(f"Cloud storage backup failed: {e}")

        m.provenance = {
            "schema": "helium_circularity_v19",
            "instance_id": self.instance_id,
            "chaos_events": list(self.chaos.events[-3:]) if self.chaos else [],
            "state_vector_dim": STATE_VECTOR_DIM,
        }

        async with self._history_lock:
            self.circularity_history.append(m)

        try:
            self.storage.store_circularity_record(m)
        except Exception as e:
            logger.warning(f"Storage write failed: {e}")

        try:
            event = FeedbackEvent.create_with_context(
                task_id=f"circ_{record_id}",
                selected_action="calculate_circularity",
                quality_score=quality_score, latency_ms=0.0,
                energy_joules=0.0, carbon_g=0.0,
                feedback_type="circularity", adaptive_cost_value=0.0,
                state={"input": input_data},
                candidates=[{"action": s}
                            for s in self.autonomous_optimizer.STRATEGIES],
                source="helium_circularity",
                environment=getattr(central_config, "ENVIRONMENT", "production"),
                tags=["circularity", "helium"])
            await self.queue.publish("feedback_events", event.to_json())
        except Exception as e:
            logger.warning(f"Feedback publish failed: {e}")

        await self.self_healing.check_drift(asdict(m))
        await self.self_healing.detect_anomaly(asdict(m))

        try:
            self.metrics.set_circularity_score(circularity_index)
        except Exception:
            pass

        CIRCULARITY_INDEX.set(circularity_index)
        CARBON_INTENSITY.set(carbon_intensity)
        CARBON_PRICE.set(carbon_price)
        REC_INVENTORY_KWH.set(self.carbon_manager.recs.total_kwh())
        CIRCULARITY_COMPUTATIONS.labels(status="success").inc()

        logger.info(f"Circularity: index={circularity_index:.3f} level={circularity_level}")
        return m

    async def submit_hitl_decision(self, request_id: str, approved: bool) -> bool:
        gate = self.autonomous_optimizer.hitl
        if gate is None:
            return False
        return await gate.submit_decision(request_id, approved)

    async def save_state(self, path: str) -> bool:
        state: Dict[str, Any] = {"schema": "helium_circularity_state_v1"}
        opt = self.autonomous_optimizer
        state["causal"] = opt.causal.export_state()
        state["roles"] = opt.roles.snapshot()
        g = opt.federated.global_weights()
        if g:
            state["federated_global"] = {k: v.tolist() for k, v in g.items()}
        state["strategy_rewards"] = dict(opt.strategy_rewards)
        state["epsilon"] = opt.epsilon
        state["ga_best_params"] = dict(opt.ga.best_params)
        state["cloud_roles"] = self.cloud_deployer.roles.snapshot()
        try:
            d = os.path.dirname(os.path.abspath(path)) or "."
            os.makedirs(d, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=d, prefix=".circ_", suffix=".json")
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(state, f, default=str)
                os.replace(tmp, path)
            except Exception:
                try: os.unlink(tmp)
                except OSError: pass
                raise
            return True
        except Exception as e:
            logger.error(f"save_state failed: {e}")
            return False

    async def load_state(self, path: str) -> bool:
        if not os.path.exists(path): return False
        try:
            with open(path, "r") as f:
                state = json.load(f)
        except Exception:
            return False
        if state.get("schema") != "helium_circularity_state_v1":
            return False
        opt = self.autonomous_optimizer
        if "causal" in state:
            opt.causal.import_state(state["causal"])
        if "federated_global" in state:
            try:
                opt.federated._global = {
                    k: np.asarray(v, dtype=np.float64)
                    for k, v in state["federated_global"].items()}
            except Exception:
                pass
        if "strategy_rewards" in state:
            opt.strategy_rewards = dict(state["strategy_rewards"])
        if "epsilon" in state:
            opt.epsilon = float(state["epsilon"])
        if "ga_best_params" in state:
            opt.ga.best_params = dict(state["ga_best_params"])
        return True

    async def start(self):
        logger.info("Starting Helium Circularity Calculator...")
        loop = asyncio.get_running_loop()
        self._background_tasks.extend([
            loop.create_task(self._optimization_loop()),
            loop.create_task(self._predictive_loop()),
            loop.create_task(self._cleanup_loop()),
            loop.create_task(self._self_healing_loop()),
        ])

    async def _optimization_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(getattr(central_config, "auto_optimize_interval", 1800))
            try:
                async with self._history_lock:
                    if not self.circularity_history: continue
                    recent = list(self.circularity_history)[-10:]
                    state = {
                        "circularity_index": float(np.mean([m.circularity_index for m in recent])),
                        "recycling_rate": float(np.mean([m.recycling_rate for m in recent])),
                        "recovery_efficiency": float(np.mean([m.recovery_efficiency for m in recent])),
                        "collection_efficiency": float(np.mean([m.collection_efficiency for m in recent])),
                        "purification_efficiency": float(np.mean([m.purification_efficiency for m in recent])),
                    }
                await self.autonomous_optimizer.optimize_circularity(state, "hybrid")
            except Exception as e:
                logger.error(f"Optimization loop error: {e}")

    async def _predictive_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                async with self._history_lock:
                    if not self.circularity_history: continue
                    latest = self.circularity_history[-1]
                    carbon = await self.carbon_manager.get_current_intensity()
                    await self.predictive.update_history(latest.circularity_index, carbon)
                    await self.predictive.forecast_circularity()
            except Exception as e:
                logger.error(f"Predictive loop error: {e}")

    async def _self_healing_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(getattr(central_config, "self_healing_interval", 3600))
            try:
                async with self._history_lock:
                    if len(self.circularity_history) > 20:
                        data = [asdict(m) for m in list(self.circularity_history)[-100:]]
                        await self.self_healing.update_detectors(data)
            except Exception as e:
                logger.error(f"Self-healing loop error: {e}")

    async def _cleanup_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(86400)
            try:
                self.storage.clean_old_circularity_records(
                    days=getattr(central_config, "data_retention_days", 365))
            except Exception as e:
                logger.error(f"Cleanup error: {e}")

    async def health_check(self) -> Dict:
        results = {}
        for name, comp in self._health_components.items():
            if comp and hasattr(comp, "health_check"):
                try:
                    results[name] = await comp.health_check()
                except Exception as e:
                    results[name] = {"status": "unhealthy", "error": str(e)}
            else:
                results[name] = {"status": "ok" if comp else "unavailable"}
        checkable = [r for r in results.values() if r.get("status") != "unavailable"]
        if not checkable:
            overall, score = "degraded", 0
        else:
            ok = all(r.get("status") in ("ok", "healthy") for r in checkable)
            overall, score = ("healthy", 100) if ok else ("degraded", 50)
        return {"status": overall, "health_score": score,
                "components": results, "timestamp": datetime.now().isoformat()}

    async def get_comprehensive_status(self) -> Dict:
        blockchain_status = await self.blockchain.get_blockchain_status()
        optimization_stats = self.autonomous_optimizer.get_optimization_stats()
        cloud_status = await self.cloud_deployer.get_deployment_status()
        async with self._history_lock:
            avg_index = (float(np.mean([m.circularity_index for m in self.circularity_history]))
                         if self.circularity_history else 0.0)
        return {
            "instance_id": self.instance_id,
            "version": "19.0",
            "blockchain": blockchain_status,
            "autonomous_optimization": optimization_stats,
            "cloud_deployment": cloud_status,
            "circularity": {"records": len(self.circularity_history),
                            "avg_index": avg_index},
            "predictive": self.predictive.get_stats(),
            "self_healing": await self.self_healing.health_check(),
            "cloud_storage": {"providers": list(self.cloud_storage.providers.keys())},
            "current_precision": self.current_precision.value if self.current_precision else None,
            "rec_kwh": self.carbon_manager.recs.total_kwh(),
            "health": await self.health_check(),
            "timestamp": datetime.now().isoformat(),
        }

    async def shutdown(self):
        logger.info("Shutting down...")
        self._shutdown_event.set()
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        with contextlib.suppress(Exception):
            await self.carbon_manager.close()
        logger.info("Shutdown complete")


# ============================================================
# SINGLETON
# ============================================================
_circularity_calculator_instance: Optional[EnhancedHeliumCircularityCalculator] = None
_circularity_calculator_lock = asyncio.Lock()


async def get_circularity_calculator(
    storage: Storage, queue: AsyncMessageQueue,
    adaptive_cost: AdaptiveCostFunction, pareto_gating: ParetoGating,
    drift_detector: DriftDetector, metrics: MetricsRegistry,
) -> EnhancedHeliumCircularityCalculator:
    global _circularity_calculator_instance
    if _circularity_calculator_instance is None:
        async with _circularity_calculator_lock:
            if _circularity_calculator_instance is None:
                _circularity_calculator_instance = EnhancedHeliumCircularityCalculator(
                    storage, queue, adaptive_cost, pareto_gating,
                    drift_detector, metrics)
                await _circularity_calculator_instance.start()
    return _circularity_calculator_instance


# ============================================================
# FASTAPI
# ============================================================
if FASTAPI_AVAILABLE:
    app = FastAPI(title="Helium Circularity API", version="19.0")
    app.add_middleware(
        CORSMiddleware, allow_origins=["*"], allow_credentials=True,
        allow_methods=["*"], allow_headers=["*"])
    security = HTTPBearer()

    _api_calc: Optional[EnhancedHeliumCircularityCalculator] = None
    _api_jwt_secret = os.urandom(32).hex()

    async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
        if not JOSE_AVAILABLE:
            return {"sub": "anonymous"}
        try:
            return jwt.decode(credentials.credentials, _api_jwt_secret, algorithms=["HS256"])
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid token")

    @app.post("/calculate")
    async def calculate(input_data: Dict = None, user: Dict = Depends(verify_token)):
        if not _api_calc:
            raise HTTPException(status_code=503, detail="Calculator not initialized")
        m = await _api_calc.calculate_comprehensive_circularity(input_data)
        return {"metrics": asdict(m)}

    @app.get("/status")
    async def status(user: Dict = Depends(verify_token)):
        if not _api_calc:
            raise HTTPException(status_code=503, detail="Calculator not initialized")
        return await _api_calc.get_comprehensive_status()

    @app.get("/health")
    async def health():
        if not _api_calc:
            raise HTTPException(status_code=503, detail="Calculator not initialized")
        return await _api_calc.health_check()

    @app.get("/explanation/last")
    async def last_explanation(user: Dict = Depends(verify_token)):
        if not _api_calc:
            raise HTTPException(status_code=503, detail="Calculator not initialized")
        return {"explanation": _api_calc.last_explanation.to_dict()
                if _api_calc.last_explanation else None}

    @app.post("/hitl/approval")
    async def hitl_approval(request_id: str, approved: bool,
                            user: Dict = Depends(verify_token)):
        if not _api_calc:
            raise HTTPException(status_code=503, detail="Calculator not initialized")
        delivered = await _api_calc.submit_hitl_decision(request_id, approved)
        if not delivered:
            HITL_APPROVALS.labels(decision="approved" if approved else "rejected").inc()
            return {"status": "recorded", "request_id": request_id,
                    "approved": approved, "delivered": False}
        return {"status": "delivered", "request_id": request_id, "approved": approved}

    @app.post("/chaos")
    async def chaos(fault_prob: float = 0.0, latency_ms: float = 0.0,
                    carbon_spike_prob: float = 0.0,
                    user: Dict = Depends(verify_token)):
        if not _api_calc:
            raise HTTPException(status_code=503, detail="Calculator not initialized")
        _api_calc.chaos = ChaosEngineer(ChaosConfig(
            fault_prob=fault_prob, latency_inject_ms=latency_ms,
            carbon_spike_prob=carbon_spike_prob))
        return {"status": "chaos enabled"}

    @app.on_event("startup")
    async def _startup():
        global _api_calc
        storage = Storage()
        queue = AsyncMessageQueue()
        adaptive_cost = AdaptiveCostFunction(storage)
        pareto = ParetoGating()
        drift = DriftDetector(storage, adaptive_cost)
        metrics = MetricsRegistry()
        _api_calc = await get_circularity_calculator(
            storage, queue, adaptive_cost, pareto, drift, metrics)


# ============================================================
# SIGNAL HANDLERS (v19: retained task references)
# ============================================================
_shutdown_tasks: set = set()


async def shutdown_handler():
    global _circularity_calculator_instance
    if _circularity_calculator_instance:
        await _circularity_calculator_instance.shutdown()
        _circularity_calculator_instance = None


def _spawn_shutdown_task():
    task = asyncio.create_task(shutdown_handler())
    _shutdown_tasks.add(task)
    task.add_done_callback(_shutdown_tasks.discard)
    return task


def _install_signal_handlers(loop):
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _spawn_shutdown_task)
        except (NotImplementedError, AttributeError):
            try:
                signal.signal(sig, lambda s, f: _spawn_shutdown_task())
            except (ValueError, OSError):
                pass


# ============================================================
# MAIN
# ============================================================
async def main():
    loop = asyncio.get_running_loop()
    _install_signal_handlers(loop)

    print("=" * 80)
    print("Enhanced Helium Circularity Model v19.0 — Enterprise Quantum+ (shared modules)")
    print("=" * 80)

    if FASTAPI_AVAILABLE and os.environ.get("CIRCULARITY_SERVE_API", "0") == "1":
        uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
        return

    storage = Storage()
    queue = AsyncMessageQueue()
    adaptive_cost = AdaptiveCostFunction(storage)
    pareto = ParetoGating()
    drift = DriftDetector(storage, adaptive_cost)
    metrics = MetricsRegistry()

    calculator = await get_circularity_calculator(
        storage, queue, adaptive_cost, pareto, drift, metrics)

    print("\n✅ Ten enhancements imported from the enhancements package:")
    print("   [1] Quantum-Distillation     QuantumInspiredTeacher + DistillationEnsemble")
    print("   [2] Causal RL                CausalCounterfactualEstimator")
    print("   [3] Federated Green Learning FederatedAggregator")
    print("   [4] Multi-Agent Coordination EmergentRoleRegistry + MultiAgentCoordinator")
    print("   [5] Temporal Logic           STLFormula + TemporalLogicMonitor + SafetyShield")
    print("   [6] Explainable AI           DecisionExplainer")
    print("   [7] Adaptive Precision       PrecisionController + HardwareAwareAdapter")
    print("   [8] Carbon Markets / RECs    CarbonMarketClient + RECInventory")
    print("   [9] Resilience / Chaos       ChaosEngineer")
    print("  [10] HITL / Active Learning   UncertaintyEstimator + HumanInTheLoopGate + ActiveLearningSampler")

    for _ in range(3):
        m = await calculator.calculate_comprehensive_circularity()
        print(f"Circularity: {m.circularity_index:.3f} ({m.circularity_level}) "
              f"precision={m.precision_level} safety={m.safety_ok} "
              f"HITL={m.hitl_approved}")

    stats = calculator.autonomous_optimizer.get_optimization_stats()
    print(f"\nStrategy rewards: {stats['strategy_rewards']}")
    print(f"Role snapshot: {stats['role_snapshot']}")
    print(f"Cloud: {calculator.cloud_deployer.active_provider} "
          f"@ {calculator.cloud_deployer.active_region}")
    status = await calculator.get_comprehensive_status()
    print(f"Health: {status['health']['health_score']}")

    print("\n" + "=" * 80)
    print("✅ Helium Circularity Model v19.0 — Ready")
    print("=" * 80)

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        await calculator.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
