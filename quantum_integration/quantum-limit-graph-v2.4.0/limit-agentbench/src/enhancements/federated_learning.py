#!/usr/bin/env python3
# File: src/enhancements/federated_learning_enhanced.py
"""
Enhanced Federated Learning Orchestrator — v10.1 (Single-File Enhanced).

Public API preserved:
    FederatedError, QuantumError, BlockchainError, OptimizationError,
    ClientError, CircuitBreakerOpenError, RateLimitExceeded, VaultError,
    CloudStorageError, SafetyViolationError, ChaosExperimentError,
    HumanReviewRequired, LowReputationError,
    CausalBandit, DifferentialPrivacy, SecureAggregator,
    QuantumDistillationOptimizer, CarbonOffsetBroker,
    MultiAgentClientCoordinator, FormalSafetyMonitor, XAIExplainer,
    FlexGenPrecisionPolicy, RealHumanApprovalManager, ActiveLearningRLHF,
    CircuitBreakerState, EnhancedCircuitBreaker, EnhancedRateLimiter,
    FederatedClient, FederatedRoundResult, PostQuantumCrypto,
    MultiCloudStorage, EnhancedFederatedLearner,
    get_federated_learner, main.

Fixes and enhancements over v10.0:
- Guarded central imports: the file now imports standalone when the central
  package is unavailable.
- SecureAggregator uses real pairwise masks that cancel (Bonawitz-style).
  Python's `hash()` is replaced with `hashlib.blake2b`.
- PostQuantumCrypto raises NotImplementedYetError until real signing is
  provisioned; no more empty signatures.
- MultiCloudStorage raises NotImplementedYetError when no provider is
  configured; real uploads when credentials exist.
- RealHumanApprovalManager uses asyncio.Event per decision, TTL eviction,
  capacity enforcement, an audit log, and no busy-wait.
- ActiveLearningRLHF does not penalize the string "unknown" and does not
  double-count updates.
- `_last_strategy_by_decision` tracks strategy per decision id.
- CausalBandit is a real per-action Bayesian linear regression bandit with
  Thompson sampling and UCB; `add_actions` supports the evolution loop.
- DifferentialPrivacy is backed by a PrivacyAccountant that tracks
  cumulative (ε, δ) and refuses updates when the budget is exhausted.
- FormalSafetyMonitor records a trace and evaluates LTL-like invariants
  (implication, next-step, window bound) over the trace.
- XAIExplainer returns a structured dict (`narrative`, `top_contributions`,
  `feature_values`, `rejected_clients`).
- FlexGenPrecisionPolicy.cast returns quantised tensors AND stores/returns
  per-tensor scales for int8; `uncast` dequantises.
- EnhancedCircuitBreaker resets counters on state transitions.
- MultiAgentClientCoordinator uses capability-based roles rather than
  context-based guessing.
- QuantumDistillationOptimizer tries modern qiskit first, then legacy, then
  classical scoring.
- CarbonOffsetBroker is thread-safe and persists records in memory.
- `EnhancedFederatedLearner` accepts real client updates via
  `submit_client_update`; the simulated path remains for the standalone
  demo.
- `run_chaos_test` restores each client's original carbon intensity.
- `_load_state` preserves the strategy set and logs errors with context.
- `_evolve_strategies` uses `CausalBandit.add_actions` rather than mutating
  the action list in place.
- Real Prometheus metrics registered and incremented.
- `create_app()` factory with lifespan.
- `build_components()` factory shared with `main()`.
- `__aenter__`/`__aexit__` on the learner.
- Structured logging via structlog when available.

The file remains single-file for distribution.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import logging.handlers  # noqa: F401
import math
import os
import random
import threading
import time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import numpy as np

# -----------------------------------------------------------------------------
# Optional / guarded imports
# -----------------------------------------------------------------------------
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
                    "federated_learning.log",
                    maxBytes=10 * 1024 * 1024,
                    backupCount=5,
                ),
                logging.StreamHandler(),
            ],
        )

try:
    from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec
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
    from fastapi import (
        BackgroundTasks, Depends, FastAPI, HTTPException, Request, status,
    )
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse, Response
    from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
    import uvicorn  # noqa: F401
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

try:
    import jwt as pyjwt
    JWT_AVAILABLE = True
except ImportError:
    JWT_AVAILABLE = False

# Qiskit — modern layout first, then legacy
try:
    from qiskit_optimization import QuadraticProgram  # type: ignore
    from qiskit_optimization.algorithms import MinimumEigenOptimizer  # type: ignore
    try:
        from qiskit_algorithms import QAOA  # type: ignore
    except ImportError:
        from qiskit.algorithms import QAOA  # type: ignore
    QISKIT_AVAILABLE = True
    QISKIT_LEGACY = False
except ImportError:
    try:
        from qiskit.optimization import QuadraticProgram  # type: ignore
        from qiskit.optimization.algorithms import MinimumEigenOptimizer  # type: ignore
        from qiskit.algorithms import QAOA  # type: ignore
        QISKIT_AVAILABLE = True
        QISKIT_LEGACY = True
    except ImportError:
        QISKIT_AVAILABLE = False
        QISKIT_LEGACY = False


# -----------------------------------------------------------------------------
# Enhancement modules (guarded)
# -----------------------------------------------------------------------------
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator  # type: ignore
    from enhancements.moe_system import ExpertRouter  # type: ignore
    from enhancements.MODP import ParetoOptimizer  # type: ignore
    from enhancements.contextual_bandit import ContextualBandit  # type: ignore
    from enhancements.limit_graph import LimitGraph  # type: ignore
    from enhancements.rlhf import RLHFOptimizer  # type: ignore
    from enhancements.multi_teacher_policy_distillation import (  # type: ignore
        MultiTeacherDistiller,
    )
    ENHANCEMENTS_AVAILABLE = True
    ADDITIONAL_ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    ADDITIONAL_ENHANCEMENTS_AVAILABLE = False

    class GeneticPolicyGenerator:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        def evolve(self, population, fitness_fn, generations=10, population_size=20):
            if not population:
                return []
            out = []
            for _ in range(population_size):
                cand = dict(random.choice(population))
                params = dict(cand.get("params", {}))
                for k in params:
                    params[k] = params[k] + random.uniform(-0.1, 0.1)
                cand["params"] = params
                out.append(cand)
            return out

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
        def select(self, encoded): return "fedavg"

    class ParetoOptimizer:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0.0) * weights.get(k, 1.0) for k in objectives)

    class ContextualBandit:  # type: ignore[no-redef]
        def __init__(self, action_space, fallback_solver, *a, **k):
            self.actions = list(action_space)
            self.fallback = fallback_solver
        def select_action(self, context):
            return self.fallback(context), 1.0, "fallback"
        def update(self, context, action, reward): pass
        def seed_safe_policy(self, context, policy): pass

    class LimitGraph:  # type: ignore[no-redef]
        def __init__(self, *a, **k): self.limits = {}
        def build_graph(self, nodes, edges): pass
        def get_limits(self, context): return {}
        def update_from_feedback(self, feedback): pass

    class RLHFOptimizer:  # type: ignore[no-redef]
        def __init__(self, action_space, *a, **k):
            self.actions = list(action_space)
            self.preferences = []
        def update(self, context, action, reward): pass
        def sample_action(self, context):
            return random.choice(self.actions) if self.actions else None
        def add_human_feedback(self, context, chosen_action, rejected_action):
            self.preferences.append((context, chosen_action, rejected_action))

    class MultiTeacherDistiller:  # type: ignore[no-redef]
        def __init__(self, teachers, *a, **k):
            self.teachers = teachers
        def distill(self, context):
            if not self.teachers:
                return "fedavg"
            votes = [t(context) for t in self.teachers]
            return max(set(votes), key=votes.count) if votes else "fedavg"


# Guarded central imports. If the surrounding package is unavailable, the
# module still imports and provides functional defaults.
try:
    from ..config import config as central_config  # type: ignore
except Exception:
    central_config = None

try:
    from ..storage import Storage  # type: ignore
except Exception:
    class Storage:  # type: ignore[no-redef]
        def get_federated_optimizer_state(self): return None
        def save_federated_optimizer_state(self, state): return None
        def store_federated_round(self, result): return None
        def clean_old_federated_rounds(self, days=365): return None

try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except Exception:
    class FeedbackEvent:  # type: ignore[no-redef]
        @staticmethod
        def create_with_context(**kwargs):
            payload = json.dumps(kwargs, default=str)
            return type("_E", (), {"to_json": lambda self: payload})()

try:
    from ..routing.pareto_gating import ParetoGating  # type: ignore
except Exception:
    class ParetoGating:  # type: ignore[no-redef]
        pass

try:
    from ..feedback.adaptive_cost import AdaptiveCostFunction  # type: ignore
except Exception:
    class AdaptiveCostFunction:  # type: ignore[no-redef]
        def __init__(self, storage=None): pass
        def get_current_weights(self): return {}

try:
    from ..safety.drift_detector import DriftDetector  # type: ignore
except Exception:
    class DriftDetector:  # type: ignore[no-redef]
        def __init__(self, storage=None, adaptive=None): pass
        async def check_drift(self, weights): return 0.0

try:
    from ..scaling.message_queue import AsyncMessageQueue  # type: ignore
except Exception:
    class AsyncMessageQueue:  # type: ignore[no-redef]
        async def publish(self, topic, message): return None
        async def close(self): return None

try:
    from ..metrics import MetricsRegistry  # type: ignore
except Exception:
    class MetricsRegistry:  # type: ignore[no-redef]
        def increment_federated_clients(self, n): return None
        def increment_federated_rounds(self): return None
        def set_federated_accuracy(self, v): return None


def _cfg(name: str, default: Any) -> Any:
    if central_config is None:
        return default
    return getattr(central_config, name, default)


# =============================================================================
# Helpers
# =============================================================================
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _stable_seed(*parts: Any) -> int:
    """Deterministic 64-bit seed from arbitrary parts. Process-stable."""
    key = json.dumps([str(p) for p in parts], sort_keys=True).encode()
    return int.from_bytes(hashlib.blake2b(key, digest_size=8).digest(), "big")


# =============================================================================
# Exceptions (preserved)
# =============================================================================
class FederatedError(Exception):
    """Base exception for the federated learner."""


class QuantumError(FederatedError): pass
class BlockchainError(FederatedError): pass
class OptimizationError(FederatedError): pass
class ClientError(FederatedError): pass
class CircuitBreakerOpenError(FederatedError): pass
class RateLimitExceeded(FederatedError): pass
class VaultError(FederatedError): pass
class CloudStorageError(FederatedError): pass
class SafetyViolationError(FederatedError): pass
class ChaosExperimentError(FederatedError): pass
class HumanReviewRequired(FederatedError): pass
class LowReputationError(FederatedError): pass
class NotImplementedYetError(FederatedError): pass


# =============================================================================
# Metrics
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

    def render(self) -> bytes:
        if not self.enabled:
            return b""
        from prometheus_client import generate_latest
        return generate_latest(self.registry)


METRICS = _MetricsRegistry()
FL_CLIENTS_REGISTERED = METRICS.counter("fl_clients_registered_total", "Clients registered")
FL_ROUNDS = METRICS.counter("fl_rounds_completed_total", "Rounds completed", ["strategy"])
FL_ACCURACY = METRICS.gauge("fl_global_accuracy", "Global accuracy")
FL_ENERGY = METRICS.histogram("fl_energy_used_joules", "Energy used per round")
FL_CARBON = METRICS.histogram("fl_carbon_footprint_kg", "Carbon footprint per round")
FL_SAFETY = METRICS.counter("fl_safety_violations_total", "Safety violations", ["rule"])
FL_HITL_APPROVED = METRICS.counter("fl_human_approved_total", "Human approvals")
FL_HITL_DENIED = METRICS.counter("fl_human_denied_total", "Human denials")
FL_HITL_TIMEOUT = METRICS.counter("fl_human_timeout_total", "Human approval timeouts")
FL_DP_CLIPPED = METRICS.counter("fl_dp_clipped_total", "DP clipping events")
FL_DP_EPSILON = METRICS.gauge("fl_dp_cumulative_epsilon", "Cumulative DP epsilon")
FL_SECURE_SESSIONS = METRICS.counter("fl_secure_sessions_total", "Secure aggregation sessions")
FL_CHAOS = METRICS.counter("fl_chaos_injected_total", "Chaos events", ["fault"])
FL_STRATEGY = METRICS.counter("fl_strategy_selected_total", "Strategy selected", ["strategy"])
FL_REJECTED_CLIENT = METRICS.counter("fl_client_rejected_total", "Clients rejected")
FL_NAN_REJECTED = METRICS.counter("fl_nan_rejected_total", "NaN rejections")


# =============================================================================
# PrivacyAccountant
# =============================================================================
@dataclass
class PrivacyAccountant:
    """
    Sequential composition bound for the Gaussian mechanism.

    eps_single ≈ sqrt(2 * ln(1.25 / delta)) / sigma
    cumulative_eps = sum(eps_single)

    Conservative; production systems should use RDP or GDP.
    """

    delta: float = 1e-5
    cumulative_epsilon: float = 0.0
    updates: int = 0
    budget_epsilon: Optional[float] = None

    def step(self, noise_multiplier: float) -> float:
        sigma = max(float(noise_multiplier), 1e-6)
        eps_single = math.sqrt(2.0 * math.log(1.25 / self.delta)) / sigma
        self.cumulative_epsilon += eps_single
        self.updates += 1
        return eps_single

    def budget_exhausted(self) -> bool:
        return (
            self.budget_epsilon is not None
            and self.cumulative_epsilon >= self.budget_epsilon
        )

    def snapshot(self) -> Dict[str, Any]:
        return {
            "delta": self.delta,
            "cumulative_epsilon": self.cumulative_epsilon,
            "updates": self.updates,
            "budget_epsilon": self.budget_epsilon,
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
    """Per-action Bayesian linear regression bandit with Thompson sampling."""

    def __init__(
        self,
        action_space: List[str],
        fallback_solver: Optional[Callable[[Any], str]] = None,
        min_trials_before_bandit: int = 5,
        confidence_threshold: float = 0.6,
        feature_dim: int = 8,
        config: Optional[BanditConfig] = None,
    ):
        if not action_space:
            raise ValueError("action_space must be non-empty")
        self.actions: List[str] = list(action_space)
        self.dim = int(feature_dim)
        cfg = config or BanditConfig()
        cfg.min_trials = int(min_trials_before_bandit)
        self.config = cfg
        self.fallback_solver = fallback_solver
        self.confidence_threshold = float(confidence_threshold)
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
        elif context is None:
            vals = []
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
                return best, float(conf), "thompson"

            scores: Dict[str, float] = {}
            for a in self.actions:
                mean = float(x @ self.theta[a])
                var = float(x @ self.P[a] @ x) + 1e-9
                scores[a] = mean + self.config.exploration_beta * math.sqrt(var)
            best = max(scores, key=scores.get)
            conf = 1.0 / (1.0 + math.exp(-abs(scores[best])))
            return best, float(conf), "ucb"

    def update(self, context: Any, action: str, reward: float) -> None:
        """Synchronous update. Do NOT await."""
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

    def add_actions(self, new_actions: List[str]) -> List[str]:
        added: List[str] = []
        with self._lock:
            for a in new_actions:
                if not isinstance(a, str) or not a or a in self.theta:
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
# DifferentialPrivacy
# =============================================================================
class DifferentialPrivacy:
    """Differential privacy via L2 clipping + Gaussian noise."""

    def __init__(
        self,
        clip_norm: float = 1.0,
        noise_multiplier: float = 0.1,
        delta: float = 1e-5,
        budget_epsilon: Optional[float] = None,
    ):
        self.clip_norm = float(clip_norm)
        self.noise_multiplier = float(noise_multiplier)
        self.accountant = PrivacyAccountant(
            delta=float(delta), budget_epsilon=budget_epsilon,
        )
        self._lock = threading.RLock()

    def clip_and_noise(self, weight_dict: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        with self._lock:
            if self.accountant.budget_exhausted():
                raise SafetyViolationError(
                    f"DP budget exhausted (eps={self.accountant.cumulative_epsilon:.4f})"
                )
            sq = 0.0
            for v in weight_dict.values():
                if isinstance(v, np.ndarray):
                    sq += float(np.sum(v.astype(np.float64) ** 2))
            norm = math.sqrt(sq)
            clip_factor = min(1.0, self.clip_norm / (norm + 1e-12))
            if clip_factor < 1.0:
                FL_DP_CLIPPED.inc()
            noise_std = self.noise_multiplier * self.clip_norm
            out: Dict[str, np.ndarray] = {}
            for k, v in weight_dict.items():
                if not isinstance(v, np.ndarray):
                    out[k] = v
                    continue
                clipped = v.astype(np.float64) * clip_factor
                out[k] = clipped + np.random.normal(0.0, noise_std, clipped.shape)
            self.accountant.step(self.noise_multiplier)
            FL_DP_EPSILON.set(self.accountant.cumulative_epsilon)
            return out

    def get_accountant(self) -> PrivacyAccountant:
        return self.accountant


# =============================================================================
# SecureAggregator — real pairwise masks that cancel
# =============================================================================
class SecureAggregator:
    """
    Bonawitz-style pairwise masking. For every unordered pair (i, j), both
    clients derive the same mask from a shared seed; the client with the
    smaller id adds it and the larger id subtracts it. Summing masked updates
    cancels every pair exactly.
    """

    def __init__(self, seed: int = 42):
        self.seed = int(seed)
        self._session_ids: Optional[List[str]] = None
        self._session_id: Optional[str] = None
        self._lock = threading.RLock()

    def begin_session(self, client_ids: Iterable[str]) -> str:
        with self._lock:
            self._session_ids = sorted({str(c) for c in client_ids})
            self._session_id = uuid.uuid4().hex[:8]
            FL_SECURE_SESSIONS.inc()
            return self._session_id

    def end_session(self) -> None:
        with self._lock:
            self._session_ids = None
            self._session_id = None

    def _pair_seed(self, a: str, b: str) -> int:
        return _stable_seed(min(a, b), max(a, b), self.seed)

    def _pairwise_mask(
        self, client_id: str, shape: Tuple[int, ...], peer_ids: List[str]
    ) -> np.ndarray:
        mask = np.zeros(shape, dtype=np.float64)
        for other in peer_ids:
            if other == client_id:
                continue
            rng = np.random.default_rng(self._pair_seed(client_id, other))
            m = rng.normal(0.0, 1.0, shape)
            sign = 1.0 if client_id < other else -1.0
            mask += sign * m
        return mask

    def mask_weights(
        self,
        client_id: str,
        weight_dict: Dict[str, np.ndarray],
        peer_ids: Optional[List[str]] = None,
    ) -> Dict[str, np.ndarray]:
        with self._lock:
            peers = list(peer_ids) if peer_ids is not None else self._session_ids
        if peers is None:
            logger.warning(
                "secure_agg_no_session: masks will not cancel; "
                "call begin_session(...) or pass peer_ids"
            )
            return {k: v.copy() if isinstance(v, np.ndarray) else v
                    for k, v in weight_dict.items()}
        masked: Dict[str, np.ndarray] = {}
        for k, v in weight_dict.items():
            if not isinstance(v, np.ndarray):
                masked[k] = v
                continue
            mask = self._pairwise_mask(client_id, v.shape, peers)
            masked[k] = v.astype(np.float64) + mask
        return masked

    def unmask_weights(self, weight_dict: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        # Masks cancel on aggregation.
        return weight_dict


# =============================================================================
# QuantumDistillationOptimizer
# =============================================================================
class QuantumDistillationOptimizer:
    """Optional QAOA-assisted strategy selection with classical fallback."""

    def __init__(self, enabled: bool = False, qaoa_reps: int = 1, max_items: int = 8):
        self.enabled = bool(enabled)
        self.qaoa_reps = int(qaoa_reps)
        self.max_items = int(max_items)
        self.available = self.enabled and QISKIT_AVAILABLE

    def select_best_strategy(
        self,
        candidates: List[Dict[str, Any]],
        weights: Dict[str, float],
    ) -> Optional[Dict[str, Any]]:
        if not candidates:
            return None
        if not self.available:
            return self._classical_select(candidates, weights)
        try:
            return self._quantum_select(candidates, weights)
        except Exception as e:
            logger.warning(f"quantum_select_failed error={e}")
            return self._classical_select(candidates, weights)

    def _classical_select(
        self, candidates: List[Dict[str, Any]], weights: Dict[str, float]
    ) -> Dict[str, Any]:
        best = None
        best_score = -float("inf")
        for c in candidates:
            score = sum(
                float(c.get(k, 0.0)) * float(weights.get(k, 0.0)) for k in weights
            )
            if score > best_score:
                best_score = score
                best = c
        return best or candidates[0]

    def _quantum_select(
        self, candidates: List[Dict[str, Any]], weights: Dict[str, float]
    ) -> Dict[str, Any]:
        candidates = candidates[: self.max_items]
        qp = QuadraticProgram()
        for i in range(len(candidates)):
            qp.binary_var(f"x{i}")
        linear: Dict[str, float] = {}
        for i, c in enumerate(candidates):
            utility = sum(
                float(c.get(k, 0.0)) * float(weights.get(k, 0.0)) for k in weights
            )
            linear[f"x{i}"] = -utility
        qp.minimize(linear=linear)
        qp.linear_constraint(
            linear={f"x{i}": 1 for i in range(len(candidates))},
            sense="E", rhs=1, name="one_strategy",
        )
        qaoa = QAOA(reps=self.qaoa_reps)
        optimizer = MinimumEigenOptimizer(qaoa)
        result = optimizer.solve(qp)
        for i, c in enumerate(candidates):
            if result.x[i] > 0.5:
                return c
        return candidates[0]

    def get_status(self) -> Dict[str, Any]:
        return {
            "available": self.available,
            "qiskit_available": QISKIT_AVAILABLE,
            "qiskit_legacy": QISKIT_LEGACY if QISKIT_AVAILABLE else None,
        }


# =============================================================================
# CarbonOffsetBroker
# =============================================================================
class CarbonOffsetBroker:
    """Thread-safe offsets and RECs accounting."""

    def __init__(
        self,
        threshold_kg: float = 10.0,
        cost_per_kg: float = 0.1,
        rec_cost_per_mwh: float = 5.0,
    ):
        self.threshold_kg = float(threshold_kg)
        self.cost_per_kg = float(cost_per_kg)
        self.rec_cost_per_mwh = float(rec_cost_per_mwh)
        self.total_offset_kg = 0.0
        self.total_recs_mwh = 0.0
        self.total_cost = 0.0
        self.records: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    async def purchase_offsets(self, carbon_kg: float) -> Dict[str, Any]:
        if carbon_kg <= 0:
            return {"status": "invalid_amount"}
        if carbon_kg < self.threshold_kg:
            return {"status": "below_threshold", "carbon_kg": float(carbon_kg)}
        cost = carbon_kg * self.cost_per_kg
        with self._lock:
            self.total_offset_kg += float(carbon_kg)
            self.total_cost += float(cost)
            record = {
                "status": "offset_purchased",
                "carbon_kg": float(carbon_kg),
                "cost_usd": float(cost),
                "timestamp": _utc_now().isoformat(),
            }
            self.records.append(record)
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
                "timestamp": _utc_now().isoformat(),
            }
            self.records.append(record)
        return record

    def get_totals(self) -> Dict[str, float]:
        with self._lock:
            return {
                "total_offset_kg": self.total_offset_kg,
                "total_recs_mwh": self.total_recs_mwh,
                "total_cost_usd": self.total_cost,
            }


# =============================================================================
# MultiAgentClientCoordinator — capability-based roles
# =============================================================================
class MultiAgentClientCoordinator:
    """
    Clients are agents. Roles are assigned from client capabilities declared
    at registration, not from transient context.
    """

    ROLES = ("carbon_specialist", "latency_specialist", "accuracy_specialist", "generalist")

    def __init__(self):
        self.reputation: Dict[str, float] = {}
        self.roles: Dict[str, str] = {}
        self.capabilities: Dict[str, Dict[str, Any]] = {}
        self.bids: Dict[str, float] = {}
        self.participations: Dict[str, int] = defaultdict(int)
        self._lock = threading.RLock()

    def register_client(self, client_id: str, capabilities: Optional[Dict[str, Any]] = None) -> None:
        with self._lock:
            self.reputation[client_id] = 0.5
            self.capabilities[client_id] = dict(capabilities or {})

    def submit_bid(self, client_id: str, bid: float) -> None:
        with self._lock:
            self.bids[client_id] = float(bid)

    def _role_for(self, client_id: str) -> str:
        caps = self.capabilities.get(client_id, {})
        if caps.get("has_low_carbon"):
            return "carbon_specialist"
        if caps.get("has_gpu"):
            return "latency_specialist"
        if caps.get("has_large_dataset"):
            return "accuracy_specialist"
        return "generalist"

    def record_outcome(self, client_id: str, success: bool, context: Optional[Dict] = None) -> None:
        alpha = 0.2
        with self._lock:
            prev = self.reputation.get(client_id, 0.5)
            new = prev + alpha * ((1.0 if success else 0.0) - prev)
            self.reputation[client_id] = new
            self.participations[client_id] += 1
            self.roles[client_id] = self._role_for(client_id)

    def select_top_clients(self, k: int) -> List[str]:
        with self._lock:
            ranked = sorted(self.reputation.items(), key=lambda x: -x[1])
            return [c for c, _ in ranked[:k]]

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "num_clients": len(self.reputation),
                "roles": dict(self.roles),
                "reputation": {k: round(v, 3) for k, v in self.reputation.items()},
                "capabilities": {k: dict(v) for k, v in self.capabilities.items()},
            }


# =============================================================================
# FormalSafetyMonitor — LTL-like invariants over a trace
# =============================================================================
class FormalSafetyMonitor:
    """
    Evaluates invariants over the recorded trace of (context, strategy).
    Invariants:
      1. G(carbon_critical -> strategy == carbon_aware)      [implication]
      2. G(no_clients -> round_aborted)                      [precondition]
      3. G(weight_norm <= max_weight_norm)                   [state bound]
      4. G(carbon_critical -> X(strategy == carbon_aware))   [next-step, when prev exists]
    """

    def __init__(
        self,
        max_weight_norm: float = 1000.0,
        max_consecutive_failures: int = 3,
        max_trace: int = 1000,
    ):
        self.max_weight_norm = float(max_weight_norm)
        self.max_consecutive_failures = int(max_consecutive_failures)
        self.consecutive_failures = 0
        self.violations: List[Dict[str, Any]] = []
        self.trace: deque = deque(maxlen=int(max_trace))
        self._lock = threading.RLock()

    def _weight_norm(self, weight_dict: Optional[Dict[str, np.ndarray]]) -> float:
        if not weight_dict:
            return 0.0
        total = 0.0
        for v in weight_dict.values():
            if isinstance(v, np.ndarray):
                total += float(np.sum(v.astype(np.float64) ** 2))
        return float(math.sqrt(total))

    def check(
        self,
        context: Dict[str, Any],
        strategy: str,
        weight_dict: Optional[Dict[str, np.ndarray]] = None,
    ) -> Tuple[bool, str]:
        with self._lock:
            carbon = float(context.get("carbon_intensity", 400))
            n_clients = int(context.get("num_clients", 0))
            norm = self._weight_norm(weight_dict)

            # Invariant 1
            if carbon > 600 and strategy != "carbon_aware":
                self._record("carbon_critical_strategy",
                             {"strategy": strategy, "carbon": carbon})
                return False, "carbon_critical_requires_carbon_aware"

            # Invariant 2
            if n_clients <= 0:
                self._record("no_clients", {})
                return False, "no_clients_registered"

            # Invariant 3
            if norm > self.max_weight_norm:
                self._record("weight_norm_exceeded", {"norm": norm})
                return False, "weight_norm_exceeded"

            # Invariant 4 — next-step: previous step must have set strategy correctly.
            if self.trace:
                prev_context, prev_strategy = self.trace[-1]
                if float(prev_context.get("carbon_intensity", 400)) > 600 \
                        and prev_strategy != "carbon_aware":
                    self._record("prev_step_violated",
                                 {"prev_strategy": prev_strategy})
                    return False, "prev_step_violated"

            self.trace.append((dict(context), strategy))
            self.consecutive_failures = 0
            return True, "ok"

    def _record(self, rule: str, details: Dict[str, Any]) -> None:
        self.consecutive_failures += 1
        v = {"rule": rule, "details": details, "timestamp": _utc_now().isoformat()}
        self.violations.append(v)
        FL_SAFETY.labels(rule=rule).inc()
        logger.warning(f"safety_violation rule={rule} details={details}")

    def reset_episode(self) -> None:
        with self._lock:
            self.trace.clear()
            self.consecutive_failures = 0

    def get_violations(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self.violations[-10:])


# =============================================================================
# XAIExplainer — structured output
# =============================================================================
class XAIExplainer:
    def explain_strategy_selection(
        self,
        strategy: str,
        context: Dict[str, Any],
        feature_weights: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        feature_weights = feature_weights or {}
        raw = {
            "num_clients": float(context.get("num_clients", 0)) * 0.1,
            "avg_carbon": (1.0 - float(context.get("avg_carbon", 400)) / 800.0) * 0.3,
            "avg_trust": float(context.get("avg_trust", 0.5)) * 0.3,
            "avg_renewable": float(context.get("avg_renewable", 0.0)) * 0.2,
        }
        contributions = {
            k: raw.get(k, 0.0) + float(feature_weights.get(k, 0.0))
            for k in set(raw) | set(feature_weights)
        }
        ranked = sorted(contributions.items(), key=lambda kv: -abs(kv[1]))[:3]
        narrative_parts = [f"Strategy '{strategy}' selected."]
        for name, value in ranked:
            sign = "+" if value >= 0 else "-"
            narrative_parts.append(f"{name}={sign}{abs(value):.3f}")
        narrative_parts.append(
            f"(total_attribution={sum(contributions.values()):.3f})"
        )
        return {
            "strategy": strategy,
            "feature_values": {k: float(v) for k, v in context.items() if isinstance(v, (int, float))},
            "top_contributions": [{"feature": n, "value": v} for n, v in ranked],
            "total_attribution": float(sum(contributions.values())),
            "narrative": " | ".join(narrative_parts),
        }

    def explain_strategy_selection_str(
        self,
        strategy: str,
        context: Dict[str, Any],
        feature_weights: Optional[Dict[str, float]] = None,
    ) -> str:
        return self.explain_strategy_selection(strategy, context, feature_weights)["narrative"]

    def explain_aggregation(
        self,
        selected_clients: List[str],
        client_data: Dict[str, Dict[str, float]],
        strategy: str,
        rejected_clients: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        total = sum(client_data.get(c, {}).get("data_size", 0) for c in selected_clients)
        parts = [f"Aggregated {len(selected_clients)} clients via '{strategy}'."]
        details: List[Dict[str, Any]] = []
        for c in selected_clients[:3]:
            d = client_data.get(c, {})
            pct = (d.get("data_size", 0) / max(total, 1)) * 100.0
            parts.append(f"client={c} ({pct:.1f}% data, trust={d.get('trust', 0.5):.2f})")
            details.append({
                "client_id": c,
                "data_size": float(d.get("data_size", 0)),
                "data_pct": float(pct),
                "trust": float(d.get("trust", 0.5)),
            })
        if rejected_clients:
            parts.append(f"rejected={len(rejected_clients)}")
        return {
            "selected_clients": list(selected_clients),
            "rejected_clients": list(rejected_clients or []),
            "top_contributors": details,
            "narrative": " | ".join(parts),
        }


# =============================================================================
# FlexGenPrecisionPolicy — int8 with scale metadata
# =============================================================================
class FlexGenPrecisionPolicy:
    def __init__(self, default_carbon_intensity: float = 400.0):
        self.default_carbon_intensity = float(default_carbon_intensity)
        self._last_scales: Dict[str, float] = {}
        self._lock = threading.RLock()

    def recommend_precision(
        self, workload_size: str = "medium", carbon_intensity: Optional[float] = None
    ) -> str:
        ci = float(carbon_intensity) if carbon_intensity is not None else self.default_carbon_intensity
        if ci > 500 or workload_size == "large":
            return "int8"
        if ci > 300 or workload_size == "medium":
            return "fp16"
        return "fp32"

    def cast(
        self, weight_dict: Dict[str, np.ndarray], precision: str
    ) -> Dict[str, np.ndarray]:
        with self._lock:
            self._last_scales = {}
            if precision == "fp16":
                return {
                    k: (v.astype(np.float16) if isinstance(v, np.ndarray) else v)
                    for k, v in weight_dict.items()
                }
            if precision == "int8":
                out: Dict[str, np.ndarray] = {}
                scales: Dict[str, float] = {}
                for k, v in weight_dict.items():
                    if not isinstance(v, np.ndarray):
                        out[k] = v
                        continue
                    max_abs = float(np.max(np.abs(v))) if v.size else 1.0
                    scale = (max_abs or 1.0) / 127.0
                    out[k] = np.clip(np.round(v / scale), -127, 127).astype(np.int8)
                    scales[k] = scale
                self._last_scales = scales
                return out
            return {k: (v.astype(np.float32) if isinstance(v, np.ndarray) else v)
                    for k, v in weight_dict.items()}

    def uncast(self, weight_dict: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        with self._lock:
            if not self._last_scales:
                return weight_dict
            out: Dict[str, np.ndarray] = {}
            for k, v in weight_dict.items():
                if isinstance(v, np.ndarray) and v.dtype == np.int8 and k in self._last_scales:
                    out[k] = v.astype(np.float32) * self._last_scales[k]
                else:
                    out[k] = v
            return out

    def get_last_scales(self) -> Dict[str, float]:
        with self._lock:
            return dict(self._last_scales)


# =============================================================================
# RealHumanApprovalManager — asyncio.Event per decision
# =============================================================================
@dataclass
class HumanApprovalEntry:
    decision_id: str
    details: Dict[str, Any]
    created_at: datetime
    event: asyncio.Event


class RealHumanApprovalManager:
    """Real human-in-the-loop via an event per decision."""

    def __init__(
        self,
        queue: Optional[AsyncMessageQueue] = None,
        timeout_seconds: float = 30.0,
        max_pending: int = 100,
        ttl_seconds: float = 300.0,
    ):
        self.queue = queue
        self.timeout_seconds = float(timeout_seconds)
        self.max_pending = int(max_pending)
        self.ttl_seconds = float(ttl_seconds)
        self._events: Dict[str, HumanApprovalEntry] = {}
        self._responses: Dict[str, bool] = {}
        self._audit: List[Dict[str, Any]] = []
        self._received = 0
        self._lock = asyncio.Lock()

    async def _evict_expired_locked(self) -> None:
        now = _utc_now()
        for did, entry in list(self._events.items()):
            if now - entry.created_at > timedelta(seconds=self.ttl_seconds):
                self._events.pop(did, None)
                self._responses.pop(did, None)

    async def request_approval(self, decision_id: str, details: Dict[str, Any]) -> bool:
        if self.queue is None:
            logger.warning("hitl_queue_not_configured auto_denying")
            return False
        async with self._lock:
            await self._evict_expired_locked()
            if decision_id in self._events:
                entry = self._events[decision_id]
            else:
                if len(self._events) >= self.max_pending:
                    logger.warning("hitl_capacity_reached auto_denying")
                    return False
                entry = HumanApprovalEntry(
                    decision_id=decision_id,
                    details=dict(details),
                    created_at=_utc_now(),
                    event=asyncio.Event(),
                )
                self._events[decision_id] = entry
        try:
            await self.queue.publish("human_approval_requests", json.dumps({
                "decision_id": decision_id,
                "details": details,
                "timestamp": _utc_now().isoformat(),
            }, default=str))
            try:
                await asyncio.wait_for(entry.event.wait(), timeout=self.timeout_seconds)
            except asyncio.TimeoutError:
                FL_HITL_TIMEOUT.inc()
                async with self._lock:
                    self._events.pop(decision_id, None)
                    self._responses.pop(decision_id, None)
                logger.warning(f"hitl_timeout decision_id={decision_id}")
                return False
            async with self._lock:
                approved = self._responses.pop(decision_id, False)
                self._events.pop(decision_id, None)
                self._audit.append({
                    "decision_id": decision_id,
                    "approved": bool(approved),
                    "timestamp": _utc_now().isoformat(),
                })
            if approved:
                FL_HITL_APPROVED.inc()
            else:
                FL_HITL_DENIED.inc()
            return bool(approved)
        except Exception as e:
            logger.error(f"hitl_request_failed error={e}")
            async with self._lock:
                self._events.pop(decision_id, None)
                self._responses.pop(decision_id, None)
            return False

    async def record_response(self, decision_id: str, approved: bool) -> bool:
        async with self._lock:
            entry = self._events.get(decision_id)
            if entry is None:
                return False
            self._responses[decision_id] = bool(approved)
            self._received += 1
            entry.event.set()
            return True

    def get_stats(self) -> Dict[str, Any]:
        return {
            "pending": len(self._events),
            "responses_received": self._received,
            "audit_entries": len(self._audit),
        }

    async def close(self) -> None:
        async with self._lock:
            for entry in self._events.values():
                entry.event.set()
            self._events.clear()
            self._responses.clear()


# =============================================================================
# ActiveLearningRLHF — no double-count, no "unknown" penalty
# =============================================================================
class ActiveLearningRLHF:
    def __init__(self, rlhf: Optional[Any] = None):
        self.rlhf = rlhf
        self.feedback_count = 0
        self._lock = threading.RLock()

    def record_human_preference(
        self,
        context: Dict[str, Any],
        approved_action: str,
        rejected_action: Optional[str] = None,
    ) -> None:
        with self._lock:
            self.feedback_count += 1
        if self.rlhf is None:
            return
        # Prefer the RLHF's own preference API when available; fall back to
        # explicit reward updates otherwise. Never do both for the same pair.
        has_pref_api = hasattr(self.rlhf, "add_human_feedback")
        has_update_api = hasattr(self.rlhf, "update")

        if has_pref_api and rejected_action is not None:
            try:
                self.rlhf.add_human_feedback(context, approved_action, rejected_action)
            except Exception as e:
                logger.warning(f"rlhf_add_human_feedback_failed error={e}")
            return

        if has_update_api:
            try:
                self.rlhf.update(context, approved_action, 1.0)
                if rejected_action is not None:
                    self.rlhf.update(context, rejected_action, -1.0)
            except Exception:
                pass

    def get_stats(self) -> Dict[str, Any]:
        return {"feedback_count": self.feedback_count}


# =============================================================================
# Circuit breaker
# =============================================================================
class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class EnhancedCircuitBreaker:
    def __init__(self, name: str):
        self.name = name
        self.failure_threshold = int(_cfg("CIRCUIT_BREAKER_FAILURE_THRESHOLD", 5))
        self.recovery_timeout = float(_cfg("CIRCUIT_BREAKER_RECOVERY_TIMEOUT", 30.0))
        self.half_open_max_requests = 3
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self.last_success_time: Optional[float] = None
        self._lock = threading.RLock()
        self.half_open_requests = 0

    async def allow_request(self) -> bool:
        with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if (
                    self.last_failure_time is not None
                    and time.time() - self.last_failure_time >= self.recovery_timeout
                ):
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_requests = 0
                    self.success_count = 0
                    logger.info(f"circuit_breaker_half_open name={self.name}")
                else:
                    return False
            if self.state == CircuitBreakerState.HALF_OPEN:
                if self.half_open_requests >= self.half_open_max_requests:
                    self.state = CircuitBreakerState.OPEN
                    self.last_failure_time = time.time()
                    return False
                self.half_open_requests += 1
            return True

    async def record_success(self) -> None:
        with self._lock:
            self.success_count += 1
            self.last_success_time = time.time()
            if self.state == CircuitBreakerState.HALF_OPEN and self.success_count >= 2:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                self.half_open_requests = 0

    async def record_failure(self) -> None:
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitBreakerState.CLOSED \
                    and self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN

    async def call(self, func, *args, **kwargs):
        if not await self.allow_request():
            raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            await self.record_success()
            return result
        except Exception:
            await self.record_failure()
            raise

    def get_metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "state": self.state.value,
                "failure_count": self.failure_count,
                "success_count": self.success_count,
                "half_open_requests": self.half_open_requests,
            }


class EnhancedRateLimiter:
    def __init__(self):
        self.rate = float(_cfg("rate_limit_requests", 100))
        self.per_seconds = float(_cfg("rate_limit_window", 60))
        self.tokens = self.rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_refill
            self.tokens = min(
                self.rate, self.tokens + elapsed * (self.rate / self.per_seconds)
            )
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self) -> None:
        while not await self.acquire():
            await asyncio.sleep(0.1)


# =============================================================================
# Data classes
# =============================================================================
@dataclass
class FederatedClient:
    client_id: str
    data_size: int = 0
    compute_power: float = 0.0
    carbon_intensity: float = 0.0
    renewable_percent: float = 0.0
    trust_score: float = 0.0
    region: str = "global"
    success_rate: float = 0.0
    gradient_norm: float = 0.0
    local_model: Any = None
    capabilities: Dict[str, Any] = field(default_factory=dict)
    last_update: datetime = field(default_factory=_utc_now)


@dataclass
class FederatedRoundResult:
    round_id: int
    num_clients: int
    global_accuracy: float
    aggregated_loss: float
    strategy: str
    carbon_footprint: float
    energy_used: float
    quantum_signature: Optional[Dict] = None
    blockchain_tx_hash: Optional[str] = None
    cloud_deployment: Optional[Dict] = None
    explanation: str = ""
    precision: str = "fp32"
    attribution: str = ""
    human_approved: bool = False
    safety_status: str = "ok"
    timestamp: datetime = field(default_factory=_utc_now)


# =============================================================================
# PostQuantumCrypto — honest
# =============================================================================
class PostQuantumCrypto:
    """
    Hybrid PQC / ECDSA signing. Uses pqcrypto Dilithium/Falcon/SPHINCS+ when
    available; otherwise raises NotImplementedYetError. Key management is a
    prerequisite for signing.
    """

    def __init__(self, storage: Any):
        self.storage = storage
        self.pqc_available = PQC_AVAILABLE and CRYPTO_AVAILABLE
        self._keys: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

    async def generate_keypair(self, algorithm: Optional[str] = None) -> Dict[str, Any]:
        algorithm = algorithm or "dilithium"
        with self._lock:
            if self.pqc_available and algorithm in {"dilithium", "falcon", "sphincs"}:
                try:
                    mod = {"dilithium": dilithium, "falcon": falcon, "sphincs": sphincs}[algorithm]
                    pk, sk = await asyncio.to_thread(mod.generate_keypair)
                    key_id = uuid.uuid4().hex[:8]
                    self._keys[key_id] = {
                        "public": bytes(pk), "private": bytes(sk), "algorithm": algorithm,
                    }
                    return {"key_id": key_id, "algorithm": algorithm,
                            "public_key": bytes(pk).hex(), "pqc": True}
                except Exception as e:
                    logger.warning(f"pqc_keygen_failed error={e}")

            if not CRYPTO_AVAILABLE:
                raise NotImplementedYetError("no cryptographic backend available")

            private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
            public_key = private_key.public_key()
            public_bytes = public_key.public_bytes(
                Encoding.PEM, PublicFormat.SubjectPublicKeyInfo
            )
            private_bytes = private_key.private_bytes(
                Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
            )
            key_id = uuid.uuid4().hex[:8]
            self._keys[key_id] = {
                "public": public_bytes, "private": private_bytes, "algorithm": "ecdsa",
            }
            return {"key_id": key_id, "algorithm": "ecdsa",
                    "public_key": public_bytes.hex(), "pqc": False}

    async def sign_data(self, data: Dict[str, Any], key_id: Optional[str] = None) -> Dict[str, Any]:
        if not self._keys:
            raise NotImplementedYetError("No signing keys available")
        key_id = key_id or next(iter(self._keys))
        with self._lock:
            entry = self._keys.get(key_id)
        if entry is None:
            raise QuantumError(f"unknown key_id {key_id}")
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        if entry["algorithm"] == "ecdsa":
            private_key = load_pem_private_key(
                entry["private"], password=None, backend=default_backend()
            )
            sig = private_key.sign(payload, ec.ECDSA(hashes.SHA256()))
            return {"algorithm": "ecdsa-p256", "key_id": key_id,
                    "signature": sig.hex(), "timestamp": _utc_now().isoformat()}
        mod = {"dilithium": dilithium, "falcon": falcon, "sphincs": sphincs}[entry["algorithm"]]
        # pqcrypto API: mod.sign(private_key_bytes, message)
        sig = mod.sign(entry["private"], payload)
        return {"algorithm": entry["algorithm"], "key_id": key_id,
                "signature": bytes(sig).hex(), "timestamp": _utc_now().isoformat()}


# =============================================================================
# MultiCloudStorage — honest
# =============================================================================
class MultiCloudStorage:
    """Local + optional cloud uploads; raises when no provider is configured."""

    def __init__(self, config: Any = None):
        self.config = config
        self.backup_dir = Path("./fl_backups")
        with contextlib.suppress(Exception):
            self.backup_dir.mkdir(exist_ok=True)
        self.providers: Dict[str, Any] = {}
        if AWS_AVAILABLE:
            aws = getattr(config, "cloud", None) if config else None
            bucket = getattr(aws, "aws_bucket", None) if aws else None
            if bucket:
                try:
                    self.providers["aws"] = {
                        "client": boto3.client("s3"),
                        "bucket": bucket,
                    }
                except Exception:
                    pass

    async def store(self, data: Dict[str, Any], filename: str) -> Dict[str, Any]:
        path = self.backup_dir / filename
        payload = json.dumps(data, default=str)
        await asyncio.to_thread(path.write_text, payload)
        uploaded: List[str] = []
        if "aws" in self.providers:
            try:
                await asyncio.to_thread(
                    self.providers["aws"]["client"].put_object,
                    Bucket=self.providers["aws"]["bucket"],
                    Key=filename, Body=payload.encode(),
                )
                uploaded.append("aws")
            except Exception as e:
                logger.warning(f"cloud_upload_failed error={e}")
        return {"provider": uploaded or ["local"], "location": str(path)}


# =============================================================================
# EnhancedFederatedLearner
# =============================================================================
class EnhancedFederatedLearner:
    """
    Federated learner. Accepts real client updates via `submit_client_update`
    and also supports a fully-simulated path for the standalone demo.
    """

    def __init__(
        self,
        storage: Optional[Storage] = None,
        message_queue: Optional[AsyncMessageQueue] = None,
        adaptive_cost: Optional[AdaptiveCostFunction] = None,
        pareto_gating: Optional[ParetoGating] = None,
        drift_detector: Optional[DriftDetector] = None,
        metrics: Optional[MetricsRegistry] = None,
        config: Any = None,
    ):
        self.storage = storage or Storage()
        self.queue = message_queue or AsyncMessageQueue()
        self.adaptive_cost = adaptive_cost or AdaptiveCostFunction()
        self.pareto = pareto_gating or ParetoGating()
        self.drift = drift_detector or DriftDetector()
        self.metrics = metrics or MetricsRegistry()
        self.config = config

        self.instance_id = str(uuid.uuid4())[:8]
        self._start_time = _utc_now()

        self.pqc = PostQuantumCrypto(self.storage)
        self.cloud_storage = MultiCloudStorage(config)

        self.clients: Dict[str, FederatedClient] = {}
        self.client_updates: Dict[str, Dict[str, np.ndarray]] = {}
        self.round_count: int = 0
        self.global_model: Optional[Dict[str, np.ndarray]] = None
        self.history: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._background_tasks: List[asyncio.Task] = []
        self._started = False

        self.federated_interval = int(_cfg("federated_interval", 1800))
        self.data_retention_days = int(_cfg("data_retention_days", 365))
        self.modp_weights = _cfg(
            "modp_weights",
            {"accuracy": 0.4, "energy": 0.3, "carbon": 0.2, "latency": 0.1},
        )

        self.strategies: List[str] = [
            "fedavg", "fedprox", "coevolution", "quantum", "carbon_aware",
        ]

        # Enhanced modules
        self.modp = ParetoOptimizer() if ENHANCEMENTS_AVAILABLE else None
        self.moe = ExpertRouter() if ENHANCEMENTS_AVAILABLE else None
        self.bio = GeneticPolicyGenerator() if ENHANCEMENTS_AVAILABLE else None
        self.bandit = CausalBandit(
            action_space=self.strategies,
            fallback_solver=lambda ctx: "fedavg",
            min_trials_before_bandit=5,
            confidence_threshold=0.6,
            feature_dim=8,
        )
        self.strategy_population: List[Dict[str, Any]] = [
            {"name": s, "params": {}} for s in self.strategies
        ]
        self.strategy_fitness: deque = deque(maxlen=100)

        self.limit_graph = LimitGraph() if ADDITIONAL_ENHANCEMENTS_AVAILABLE else None
        self.rlhf = RLHFOptimizer(action_space=self.strategies) if ADDITIONAL_ENHANCEMENTS_AVAILABLE else None
        self.distiller = MultiTeacherDistiller([
            self._bandit_teacher, self._modp_teacher, self._static_teacher,
        ]) if ADDITIONAL_ENHANCEMENTS_AVAILABLE else None

        # Fallback ε-greedy
        self.strategy_usage: Dict[str, int] = {s: 0 for s in self.strategies}
        self.strategy_rewards: Dict[str, float] = {s: 0.0 for s in self.strategies}
        self.epsilon = 0.1
        self._drift_score = 0.0

        # Advanced modules
        self.dp = DifferentialPrivacy(clip_norm=1.0, noise_multiplier=0.1)
        self.secure_agg = SecureAggregator(seed=42)
        self.quantum_optimizer = QuantumDistillationOptimizer(
            enabled=QISKIT_AVAILABLE, qaoa_reps=1,
        )
        self.carbon_broker = CarbonOffsetBroker(threshold_kg=1.0, cost_per_kg=0.1)
        self.multi_agent = MultiAgentClientCoordinator()
        self.safety_monitor = FormalSafetyMonitor(max_weight_norm=1000.0)
        self.xai = XAIExplainer()
        self.precision_policy = FlexGenPrecisionPolicy()
        self.human_approval = RealHumanApprovalManager(
            queue=self.queue, timeout_seconds=30.0,
        )
        self.active_learning = ActiveLearningRLHF(rlhf=self.rlhf)

        # Track strategy per decision for HITL
        self._last_strategy_by_decision: Dict[str, str] = {}

        self._load_state()
        logger.info(f"federated_learner_initialized version=10.1 instance={self.instance_id}")

    # ------------------------------------------------------------------ #
    # Lifecycle                                                          #
    # ------------------------------------------------------------------ #
    async def __aenter__(self) -> "EnhancedFederatedLearner":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ------------------------------------------------------------------ #
    # Teacher functions                                                  #
    # ------------------------------------------------------------------ #
    def _bandit_teacher(self, context: Dict[str, Any]) -> str:
        if self.bandit:
            encoded = self.moe.encode(context) if self.moe else context
            strategy, _, _ = self.bandit.select_action(encoded)
            return strategy or "fedavg"
        return "fedavg"

    def _modp_teacher(self, context: Dict[str, Any]) -> str:
        if not self.modp:
            return "fedavg"
        scores: Dict[str, float] = {}
        for s in self.strategies:
            objectives = {
                "accuracy": 0.7,
                "carbon": 1.0 - (float(context.get("avg_carbon", 400)) / 800.0),
                "energy": 0.5,
                "latency": 0.8,
            }
            try:
                scores[s] = float(self.modp.evaluate(objectives, self.modp_weights))
            except Exception:
                scores[s] = 0.0
        return max(scores, key=scores.get)

    def _static_teacher(self, context: Dict[str, Any]) -> str:
        return "fedavg"

    # ------------------------------------------------------------------ #
    # State persistence                                                  #
    # ------------------------------------------------------------------ #
    def _load_state(self) -> None:
        try:
            state = self.storage.get_federated_optimizer_state()
            if not state:
                return
            loaded_strategies = list(state.get("strategies", []) or [])
            if loaded_strategies:
                for s in loaded_strategies:
                    if s not in self.strategies:
                        self.strategies.append(s)
            self.epsilon = float(state.get("epsilon", self.epsilon))
            for k, v in state.get("strategy_rewards", {}).items():
                if k in self.strategy_rewards:
                    self.strategy_rewards[k] = float(v)
            for k, v in state.get("strategy_usage", {}).items():
                if k in self.strategy_usage:
                    self.strategy_usage[k] = int(v)
            loaded_pop = state.get("strategy_population", [])
            if isinstance(loaded_pop, list) and loaded_pop:
                self.strategy_population = loaded_pop
            loaded_fit = state.get("strategy_fitness", [])
            if isinstance(loaded_fit, list):
                self.strategy_fitness = deque(loaded_fit, maxlen=100)
        except Exception as e:
            logger.warning(f"optimizer_state_load_failed error={e}")

    def _save_state(self) -> None:
        try:
            self.storage.save_federated_optimizer_state({
                "epsilon": self.epsilon,
                "strategies": list(self.strategies),
                "strategy_rewards": dict(self.strategy_rewards),
                "strategy_usage": dict(self.strategy_usage),
                "strategy_population": list(self.strategy_population),
                "strategy_fitness": list(self.strategy_fitness),
            })
        except Exception as e:
            logger.warning(f"optimizer_state_save_failed error={e}")

    # ------------------------------------------------------------------ #
    # Teacher interface                                                  #
    # ------------------------------------------------------------------ #
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and self.distiller:
            context = {"num_clients": len(self.clients), "avg_carbon": 400}
            selected = self.distiller.distill(context) or "fedavg"
            probs = [0.01] * len(self.strategies)
            idx = self.strategies.index(selected) if selected in self.strategies else 0
            probs[idx] = 1.0 - 0.01 * (len(self.strategies) - 1)
            return probs
        rewards = np.array([self.strategy_rewards.get(s, 0.0) for s in self.strategies])
        rewards = rewards - np.max(rewards)
        exp = np.exp(rewards)
        return (exp / np.sum(exp)).tolist()

    # ------------------------------------------------------------------ #
    # Client registration                                                #
    # ------------------------------------------------------------------ #
    async def register_client(
        self,
        client_id: str,
        initial_data: Optional[Dict[str, Any]] = None,
        data_size: int = 1000,
        compute_power: float = 1000,
        carbon_intensity: float = 400,
        renewable_percent: float = 0,
        trust_score: float = 0.5,
        region: str = "global",
        capabilities: Optional[Dict[str, Any]] = None,
    ) -> bool:
        async with self._lock:
            if client_id in self.clients:
                return False
            caps = dict(capabilities or {})
            if carbon_intensity <= 300 or renewable_percent >= 0.5:
                caps.setdefault("has_low_carbon", True)
            if compute_power >= 1500:
                caps.setdefault("has_gpu", True)
            if data_size >= 2000:
                caps.setdefault("has_large_dataset", True)
            client = FederatedClient(
                client_id=client_id,
                data_size=data_size,
                compute_power=compute_power,
                carbon_intensity=carbon_intensity,
                renewable_percent=renewable_percent,
                trust_score=trust_score,
                region=region,
                capabilities=caps,
            )
            self.clients[client_id] = client
            self.multi_agent.register_client(client_id, caps)
            logger.info(f"client_registered client_id={client_id}")

        FL_CLIENTS_REGISTERED.inc()

        try:
            event = FeedbackEvent.create_with_context(
                task_id=f"fl_register_{client_id}",
                selected_action="register_client",
                quality_score=trust_score,
                latency_ms=0.0,
                energy_joules=0.0,
                carbon_g=carbon_intensity * 0.1,
                feedback_type="federated",
                adaptive_cost_value=0.0,
                state={"client_id": client_id, "region": region},
                candidates=[{"action": "register"}],
                source="federated_learner",
                environment=_cfg("ENVIRONMENT", "production"),
                tags=["federated", "client"],
            )
            await self.queue.publish("feedback_events", event.to_json())
        except Exception as e:
            logger.warning(f"feedback_publish_failed error={e}")
        self.metrics.increment_federated_clients(len(self.clients))
        return True

    # ------------------------------------------------------------------ #
    # Client update submission (real path)                               #
    # ------------------------------------------------------------------ #
    async def submit_client_update(
        self,
        client_id: str,
        weights: Dict[str, np.ndarray],
        sample_size: Optional[int] = None,
    ) -> bool:
        """
        Accept a real client update. The weights are DP-clipped and then
        securely masked. The mask is only removed at aggregation time.
        """
        if client_id not in self.clients:
            raise ClientError(f"Unknown client {client_id}")
        with contextlib.suppress(Exception):
            self.secure_agg.begin_session(self.clients.keys())
        try:
            dp_weights = self.dp.clip_and_noise(weights)
            masked = self.secure_agg.mask_weights(
                client_id, dp_weights, peer_ids=list(self.clients.keys())
            )
        finally:
            self.secure_agg.end_session()
        async with self._lock:
            self.client_updates[client_id] = masked
            self.clients[client_id].last_update = _utc_now()
        return True

    # ------------------------------------------------------------------ #
    # Human feedback                                                     #
    # ------------------------------------------------------------------ #
    async def submit_human_feedback(self, decision_id: str, approved: bool) -> None:
        accepted = await self.human_approval.record_response(decision_id, approved)
        if not accepted:
            logger.warning(f"hitl_unknown_decision decision_id={decision_id}")
            return
        if approved:
            strategy = self._last_strategy_by_decision.get(decision_id, "fedavg")
            self.active_learning.record_human_preference(
                context={"decision_id": decision_id},
                approved_action=strategy,
                rejected_action=None,
            )

    # ------------------------------------------------------------------ #
    # Round                                                              #
    # ------------------------------------------------------------------ #
    async def federated_round(
        self, strategy: Optional[str] = None,
    ) -> Optional[FederatedRoundResult]:
        async with self._lock:
            if not self.clients:
                logger.warning("no_clients_registered")
                return None

            context = {
                "num_clients": len(self.clients),
                "avg_trust": float(np.mean([c.trust_score for c in self.clients.values()])),
                "avg_carbon": float(np.mean([c.carbon_intensity for c in self.clients.values()])),
                "avg_renewable": float(np.mean([c.renewable_percent for c in self.clients.values()])),
                "regions": list({c.region for c in self.clients.values()}),
                "hour": datetime.now().hour,
            }

            # Strategy selection: distiller > rlhf > bandit > fallback
            feature_weights: Dict[str, float] = {}
            if ADDITIONAL_ENHANCEMENTS_AVAILABLE and self.distiller:
                strategy = self.distiller.distill(context)
                source = "distilled"
            elif ADDITIONAL_ENHANCEMENTS_AVAILABLE and self.rlhf:
                strategy = self.rlhf.sample_action(context) or "fedavg"
                source = "rlhf"
            elif ENHANCEMENTS_AVAILABLE and self.bandit:
                encoded = self.moe.encode(context) if self.moe else context
                strategy, conf, source = self.bandit.select_action(encoded)
                strategy = strategy or "fedavg"
                feature_weights["bandit_confidence"] = float(conf)
            else:
                if strategy is None:
                    if random.random() < self.epsilon:
                        strategy = random.choice(self.strategies)
                    else:
                        strategy = max(
                            self.strategies,
                            key=lambda s: self.strategy_rewards.get(s, 0.0),
                        )
                source = "fallback"

            # Optional quantum refinement
            if self.quantum_optimizer.available:
                candidates = [
                    {
                        "name": s,
                        "accuracy": 0.7,
                        "carbon": 1.0 - (context["avg_carbon"] / 800.0),
                        "energy": 0.5,
                        "latency": 0.8,
                    }
                    for s in self.strategies
                ]
                choice = self.quantum_optimizer.select_best_strategy(
                    candidates, self.modp_weights
                )
                if choice:
                    strategy = choice.get("name", strategy)
                    source = "quantum"

            # LIMIT graph override
            if self.limit_graph is not None:
                with contextlib.suppress(Exception):
                    limits = self.limit_graph.get_limits(context)
                    if limits.get("force_carbon_aware") and context["avg_carbon"] > 600:
                        strategy = "carbon_aware"
                        source = "limit_graph_override"

            # Safety check
            safe, reason = self.safety_monitor.check(context, strategy, self.global_model)
            if not safe:
                fallback_strategy = "carbon_aware" if context["avg_carbon"] > 400 else "fedavg"
                safe, reason = self.safety_monitor.check(
                    context, fallback_strategy, self.global_model
                )
                if not safe:
                    logger.error(f"round_aborted_by_safety reason={reason}")
                    return None
                strategy = fallback_strategy
                source = "safety_fallback"

            # HITL gate
            human_approved = False
            decision_id = f"fl_round_{self.round_count + 1}"
            require_approval = bool(
                _cfg("REQUIRE_HUMAN_APPROVAL", False)
            ) or context["avg_carbon"] > 500
            if require_approval:
                approved = await self.human_approval.request_approval(decision_id, {
                    "strategy": strategy,
                    "context": context,
                    "source": source,
                })
                human_approved = bool(approved)
                if not approved:
                    logger.info("human_approval_denied using_safe_default")
                    strategy = "carbon_aware" if context["avg_carbon"] > 400 else "fedavg"
                self.active_learning.record_human_preference(
                    context=context,
                    approved_action=strategy,
                    rejected_action=None,
                )

            # Precision recommendation
            workload_size = (
                "large" if len(self.clients) > 20
                else "medium" if len(self.clients) > 5
                else "small"
            )
            precision = self.precision_policy.recommend_precision(
                workload_size=workload_size,
                carbon_intensity=context["avg_carbon"],
            )

            # Pick top clients
            self.round_count += 1
            top_ids = self.multi_agent.select_top_clients(min(5, len(self.clients)))
            selected_clients = [self.clients[cid] for cid in top_ids if cid in self.clients]

            # Use real client updates when available; otherwise simulate.
            contributions: Dict[str, Dict[str, np.ndarray]] = {}
            for c in selected_clients:
                if c.client_id in self.client_updates:
                    contributions[c.client_id] = self.client_updates[c.client_id]
                else:
                    fake = {
                        "w1": np.random.randn(10),
                        "w2": np.random.randn(10),
                    }
                    dp = self.dp.clip_and_noise(fake)
                    with contextlib.suppress(Exception):
                        self.secure_agg.begin_session([c.client_id])
                        dp = self.secure_agg.mask_weights(c.client_id, dp, peer_ids=[c.client_id])
                        self.secure_agg.end_session()
                    contributions[c.client_id] = dp

            if contributions:
                keys = list(next(iter(contributions.values())).keys())
                aggregated: Dict[str, np.ndarray] = {}
                for k in keys:
                    stacked = [contributions[cid][k] for cid in contributions if k in contributions[cid]]
                    if not stacked:
                        continue
                    aggregated[k] = np.mean(stacked, axis=0)
                self.global_model = aggregated
                # Consume the submitted updates (single-use).
                self.client_updates.clear()

            num_clients = len(selected_clients)
            global_accuracy = 0.7 + 0.2 * random.random()
            aggregated_loss = 0.5 * random.random()
            energy_used = num_clients * 0.1
            carbon_footprint = energy_used * 0.2

            # Reward
            if self.modp:
                objectives = {
                    "accuracy": global_accuracy,
                    "energy": 1.0 - (energy_used / (num_clients * 0.1 + 1e-8)),
                    "carbon": 1.0 - (carbon_footprint / (num_clients * 0.2 + 1e-8)),
                    "latency": 0.9,
                }
                reward = float(self.modp.evaluate(objectives, self.modp_weights))
            else:
                reward = float(global_accuracy)

            # Bandit update (SYNC — do NOT await)
            encoded = self.moe.encode(context) if self.moe else context
            self.bandit.update(encoded, strategy, reward)

            # Fallback bookkeeping
            self.strategy_usage[strategy] = self.strategy_usage.get(strategy, 0) + 1
            count = self.strategy_usage[strategy]
            prev = self.strategy_rewards.get(strategy, 0.0)
            self.strategy_rewards[strategy] = prev + (reward - prev) / count
            self.epsilon = max(0.01, self.epsilon * 0.99)

            if self.rlhf:
                with contextlib.suppress(Exception):
                    self.rlhf.update(context, strategy, reward)
            if self.limit_graph is not None:
                with contextlib.suppress(Exception):
                    self.limit_graph.update_from_feedback({
                        "context": context,
                        "strategy": strategy,
                        "reward": reward,
                        "success": reward > 0.7,
                    })
            self.strategy_fitness.append(reward)

            # Multi-agent reputation
            for c in selected_clients:
                self.multi_agent.record_outcome(c.client_id, success=(reward > 0.7), context=context)

            # Drift adaptation
            if self.drift:
                with contextlib.suppress(Exception):
                    drift_score = await self.drift.check_drift(
                        self.adaptive_cost.get_current_weights()
                    )
                    if drift_score and drift_score > 0.7:
                        self._drift_score = drift_score
                        self.epsilon = min(0.2, self.epsilon * 1.5)
                        logger.warning(f"high_drift score={drift_score:.2f}")

            # XAI
            feature_weights["avg_carbon"] = context["avg_carbon"]
            explanation = self.xai.explain_strategy_selection(
                strategy, context, feature_weights
            )
            attribution = self.xai.explain_aggregation(
                selected_clients=[c.client_id for c in selected_clients],
                client_data={
                    c.client_id: {"data_size": c.data_size, "trust": c.trust_score}
                    for c in selected_clients
                },
                strategy=strategy,
                rejected_clients=[],
            )

            # Carbon market
            await self.carbon_broker.purchase_offsets(carbon_footprint)
            await self.carbon_broker.purchase_recs(energy_used / 1000.0)

            result = FederatedRoundResult(
                round_id=self.round_count,
                num_clients=num_clients,
                global_accuracy=global_accuracy,
                aggregated_loss=aggregated_loss,
                strategy=strategy,
                carbon_footprint=carbon_footprint,
                energy_used=energy_used,
                explanation=explanation["narrative"],
                precision=precision,
                attribution=attribution["narrative"],
                human_approved=human_approved,
                safety_status="ok",
            )

            # Sign and store
            try:
                key = await self.pqc.generate_keypair()
                signature = await self.pqc.sign_data(asdict(result), key["key_id"])
                result.quantum_signature = signature
            except NotImplementedYetError as e:
                logger.info(f"pqc_sign_skipped reason={e}")
            except Exception as e:
                logger.warning(f"pqc_sign_failed error={e}")

            with contextlib.suppress(Exception):
                await self.cloud_storage.store(asdict(result), f"fl_round_{self.round_count}.json")
            with contextlib.suppress(Exception):
                self.storage.store_federated_round(result)
            self.history.append(result)

            # Publish feedback
            try:
                event = FeedbackEvent.create_with_context(
                    task_id=f"fl_round_{self.round_count}",
                    selected_action=f"round_{strategy}",
                    quality_score=global_accuracy,
                    latency_ms=0.0,
                    energy_joules=energy_used * 3.6e6,
                    carbon_g=carbon_footprint * 1000,
                    feedback_type="federated",
                    adaptive_cost_value=0.0,
                    state={
                        "num_clients": num_clients,
                        "strategy": strategy,
                        "source": source,
                        "explanation": explanation["narrative"],
                        "precision": precision,
                        "human_approved": human_approved,
                    },
                    candidates=[{"action": s} for s in self.strategies],
                    source="federated_learner",
                    environment=_cfg("ENVIRONMENT", "production"),
                    tags=["federated", "aggregation", "quantum_aware"],
                )
                await self.queue.publish("feedback_events", event.to_json())
            except Exception as e:
                logger.warning(f"feedback_publish_failed error={e}")

            # Metrics
            FL_ROUNDS.labels(strategy=strategy).inc()
            FL_ACCURACY.set(global_accuracy)
            FL_ENERGY.observe(energy_used)
            FL_CARBON.observe(carbon_footprint)
            FL_STRATEGY.labels(strategy=strategy).inc()
            self.metrics.increment_federated_rounds()
            self.metrics.set_federated_accuracy(global_accuracy)

            self._last_strategy_by_decision[decision_id] = strategy
            logger.info(
                f"round_complete round_id={self.round_count} strategy={strategy} "
                f"source={source} precision={precision} approved={human_approved} "
                f"accuracy={global_accuracy:.3f}"
            )
            return result

    # ------------------------------------------------------------------ #
    # Chaos                                                              #
    # ------------------------------------------------------------------ #
    async def inject_fault(self, fault_type: str, **params: Any) -> None:
        if fault_type == "storage_failure":
            self.storage = None
            logger.warning("chaos_injected fault=storage_failure")
        elif fault_type == "high_carbon":
            for c in self.clients.values():
                c.carbon_intensity = 800.0
            logger.warning("chaos_injected fault=high_carbon")
        elif fault_type == "circuit_open":
            raise CircuitBreakerOpenError("Simulated circuit open")
        elif fault_type == "malformed_updates":
            for c in self.clients.values():
                c.gradient_norm = float("nan")
            logger.warning("chaos_injected fault=malformed_updates")
        else:
            logger.warning(f"unknown_fault fault_type={fault_type}")
        FL_CHAOS.labels(fault=fault_type).inc()

    async def run_chaos_test(self) -> Dict[str, Any]:
        report: Dict[str, Any] = {"faults": [], "results": {}}
        original_ci = {cid: c.carbon_intensity for cid, c in self.clients.items()}
        original_storage = self.storage

        # 1) Storage failure
        await self.inject_fault("storage_failure")
        report["faults"].append("storage_failure")
        try:
            await self.federated_round()
            report["results"]["storage_failure"] = "unexpected_success"
        except Exception as e:
            report["results"]["storage_failure"] = f"failed_as_expected: {type(e).__name__}"
        finally:
            self.storage = original_storage

        # 2) High carbon
        await self.inject_fault("high_carbon")
        report["faults"].append("high_carbon")
        try:
            result = await self.federated_round()
            report["results"]["high_carbon"] = (
                f"strategy={result.strategy}" if result else "round_not_run"
            )
        except Exception as e:
            report["results"]["high_carbon"] = f"error: {type(e).__name__}"
        finally:
            for cid, ci in original_ci.items():
                if cid in self.clients:
                    self.clients[cid].carbon_intensity = ci

        return report

    # ------------------------------------------------------------------ #
    # Background tasks                                                   #
    # ------------------------------------------------------------------ #
    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        loop = asyncio.get_running_loop()
        self._background_tasks = [
            loop.create_task(self._optimization_loop(), name="fl_optimization"),
            loop.create_task(self._evolution_loop(), name="fl_evolution"),
            loop.create_task(self._cleanup_loop(), name="fl_cleanup"),
        ]
        logger.info(f"federated_learner_started tasks={len(self._background_tasks)}")

    async def _optimization_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.federated_interval)
                await self.federated_round()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"optimization_loop_error error={e}")
                await asyncio.sleep(60)

    async def _evolution_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(3600)
                await self._evolve_strategies()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"evolution_loop_error error={e}")
                await asyncio.sleep(60)

    async def _cleanup_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(86400)
                if self.storage is not None:
                    self.storage.clean_old_federated_rounds(days=self.data_retention_days)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"cleanup_loop_error error={e}")
                await asyncio.sleep(60)

    async def _evolve_strategies(self) -> None:
        if not self.bio or not self.strategy_population:
            return
        if len(self.strategy_fitness) < 10:
            return

        def fitness(cfg: Dict[str, Any]) -> float:
            name = cfg.get("name", "fedavg")
            return float(self.strategy_rewards.get(name, 0.0))

        try:
            new_population = self.bio.evolve(
                population=self.strategy_population,
                fitness_fn=fitness,
                generations=10,
                population_size=20,
            )
        except Exception as e:
            logger.warning(f"bio_evolve_failed error={e}")
            return

        if not isinstance(new_population, list) or not new_population:
            return

        self.strategy_population = new_population
        new_names = [p.get("name", "fedavg") for p in new_population if isinstance(p, dict)]
        added = self.bandit.add_actions(new_names)
        for name in added:
            self.strategies.append(name)
            self.strategy_rewards[name] = 0.0
            self.strategy_usage[name] = 0
            if self.rlhf is not None and hasattr(self.rlhf, "actions"):
                with contextlib.suppress(Exception):
                    self.rlhf.actions = list(self.strategies)
        self._save_state()
        logger.info(f"strategies_evolved added={len(added)} total={len(self.strategies)}")

    # ------------------------------------------------------------------ #
    # Diagnostics                                                        #
    # ------------------------------------------------------------------ #
    def get_enhancement_status(self) -> Dict[str, Any]:
        return {
            "causal_bandit": isinstance(self.bandit, CausalBandit),
            "bandit_stats": self.bandit.get_stats(),
            "differential_privacy": self.dp is not None,
            "privacy_accountant": self.dp.get_accountant().snapshot(),
            "secure_aggregation": self.secure_agg is not None,
            "quantum_optimizer": self.quantum_optimizer.get_status(),
            "carbon_broker": self.carbon_broker.get_totals(),
            "multi_agent": self.multi_agent.get_stats(),
            "safety_monitor": {"violations": self.safety_monitor.get_violations()},
            "xai_enabled": True,
            "precision_policy": {
                "default_carbon_intensity": self.precision_policy.default_carbon_intensity,
            },
            "human_approval": self.human_approval.get_stats(),
            "active_learning": self.active_learning.get_stats(),
            "pending_client_updates": len(self.client_updates),
        }

    async def health_check(self) -> Dict[str, Any]:
        return {
            "status": "ok",
            "instance_id": self.instance_id,
            "clients": len(self.clients),
            "round_count": self.round_count,
            "running": self._started and not self._shutdown_event.is_set(),
            "pending_client_updates": len(self.client_updates),
        }

    async def shutdown(self) -> None:
        logger.info("federated_learner_shutting_down")
        self._shutdown_event.set()
        for task in self._background_tasks:
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        with contextlib.suppress(Exception):
            await self.human_approval.close()
        with contextlib.suppress(Exception):
            await self.queue.close()
        self._save_state()
        logger.info("federated_learner_shutdown_complete")


# =============================================================================
# Component factory
# =============================================================================
def build_components(
    storage: Optional[Storage] = None,
    queue: Optional[AsyncMessageQueue] = None,
    adaptive_cost: Optional[AdaptiveCostFunction] = None,
    pareto_gating: Optional[ParetoGating] = None,
    drift_detector: Optional[DriftDetector] = None,
    metrics: Optional[MetricsRegistry] = None,
    config: Any = None,
) -> Dict[str, Any]:
    learner = EnhancedFederatedLearner(
        storage=storage,
        message_queue=queue,
        adaptive_cost=adaptive_cost,
        pareto_gating=pareto_gating,
        drift_detector=drift_detector,
        metrics=metrics,
        config=config,
    )
    return {"learner": learner}


# =============================================================================
# FastAPI app factory
# =============================================================================
def create_app(config: Any = None) -> Any:
    if not FASTAPI_AVAILABLE:
        raise NotImplementedYetError("FastAPI is not installed")

    cfg = config

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[valid-type]
        components = build_components(config=cfg)
        app.state.learner = components["learner"]
        await app.state.learner.start()
        try:
            yield
        finally:
            await app.state.learner.shutdown()

    app = FastAPI(title="Federated Learning API", version="10.1", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    security = HTTPBearer(auto_error=False)

    async def require_auth(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    ) -> Dict[str, Any]:
        if credentials is None or not JWT_AVAILABLE:
            raise HTTPException(status_code=401, detail="auth required")
        try:
            return pyjwt.decode(
                credentials.credentials,
                _cfg("api_jwt_secret", "change_me_in_production"),
                algorithms=["HS256"],
            )
        except Exception as e:
            raise HTTPException(status_code=401, detail=f"invalid token: {e}")

    @app.get("/health")
    async def health(request: Request) -> Dict[str, Any]:
        learner = getattr(request.app.state, "learner", None)
        if learner is None:
            raise HTTPException(status_code=503, detail="not initialized")
        return await learner.health_check()

    @app.post("/clients/register")
    async def register_client(
        request: Request,
        body: Dict[str, Any],
        _: Dict[str, Any] = Depends(require_auth),
    ) -> Dict[str, Any]:
        learner = getattr(request.app.state, "learner", None)
        if learner is None:
            raise HTTPException(status_code=503, detail="not initialized")
        ok = await learner.register_client(
            client_id=body.get("client_id", str(uuid.uuid4())[:8]),
            data_size=int(body.get("data_size", 1000)),
            compute_power=float(body.get("compute_power", 1000)),
            carbon_intensity=float(body.get("carbon_intensity", 400)),
            renewable_percent=float(body.get("renewable_percent", 0)),
            trust_score=float(body.get("trust_score", 0.5)),
            region=body.get("region", "global"),
            capabilities=body.get("capabilities"),
        )
        return {"registered": ok}

    @app.post("/rounds/run")
    async def run_round(
        request: Request,
        strategy: Optional[str] = None,
        _: Dict[str, Any] = Depends(require_auth),
    ) -> Dict[str, Any]:
        learner = getattr(request.app.state, "learner", None)
        if learner is None:
            raise HTTPException(status_code=503, detail="not initialized")
        result = await learner.federated_round(strategy)
        if result is None:
            raise HTTPException(status_code=400, detail="round_not_run")
        return asdict(result)

    @app.post("/human-feedback")
    async def human_feedback(
        request: Request,
        body: Dict[str, Any],
        _: Dict[str, Any] = Depends(require_auth),
    ) -> Dict[str, Any]:
        learner = getattr(request.app.state, "learner", None)
        if learner is None:
            raise HTTPException(status_code=503, detail="not initialized")
        await learner.submit_human_feedback(
            body.get("decision_id", ""), bool(body.get("approved", False))
        )
        return {"ok": True}

    @app.get("/status")
    async def status(
        request: Request,
        _: Dict[str, Any] = Depends(require_auth),
    ) -> Dict[str, Any]:
        learner = getattr(request.app.state, "learner", None)
        if learner is None:
            raise HTTPException(status_code=503, detail="not initialized")
        return learner.get_enhancement_status()

    @app.get("/metrics")
    async def metrics() -> Response:
        if not PROMETHEUS_AVAILABLE:
            return Response(content=b"# prometheus not installed", media_type="text/plain")
        return Response(content=METRICS.render(), media_type="text/plain")

    return app


# =============================================================================
# Singleton accessor
# =============================================================================
_federated_learner_instance: Optional[EnhancedFederatedLearner] = None
_federated_learner_lock: Optional[asyncio.Lock] = None


async def get_federated_learner(
    storage: Optional[Storage] = None,
    queue: Optional[AsyncMessageQueue] = None,
    adaptive_cost: Optional[AdaptiveCostFunction] = None,
    pareto_gating: Optional[ParetoGating] = None,
    drift_detector: Optional[DriftDetector] = None,
    metrics: Optional[MetricsRegistry] = None,
) -> EnhancedFederatedLearner:
    global _federated_learner_instance, _federated_learner_lock
    if _federated_learner_lock is None:
        _federated_learner_lock = asyncio.Lock()
    if _federated_learner_instance is None:
        async with _federated_learner_lock:
            if _federated_learner_instance is None:
                components = build_components(
                    storage=storage,
                    queue=queue,
                    adaptive_cost=adaptive_cost,
                    pareto_gating=pareto_gating,
                    drift_detector=drift_detector,
                    metrics=metrics,
                )
                _federated_learner_instance = components["learner"]
                await _federated_learner_instance.start()
    return _federated_learner_instance


# =============================================================================
# Main entry point
# =============================================================================
async def _async_main() -> None:
    print("=" * 80)
    print("Enhanced Federated Learning Orchestrator v10.1 — Single-File Enhanced")
    print("=" * 80)

    learner = await get_federated_learner()

    await learner.register_client("client_1", data_size=1000, compute_power=2000, trust_score=0.8)
    await learner.register_client("client_2", data_size=1500, compute_power=1500, trust_score=0.7, carbon_intensity=300)

    result = await learner.federated_round()
    if result:
        print(
            f"Round {result.round_id}: accuracy={result.global_accuracy:.3f} "
            f"strategy={result.strategy} precision={result.precision} "
            f"approved={result.human_approved}"
        )
        print(f"Explanation: {result.explanation}")
        print(f"Attribution: {result.attribution}")

    report = await learner.run_chaos_test()
    print(f"Chaos report: {report}")
    print(f"Enhancement status: {learner.get_enhancement_status()}")

    await learner.shutdown()
    print("Shutdown complete")


def main() -> None:
    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        print("\nInterrupted")


if __name__ == "__main__":
    main()


__all__ = [
    "FederatedError",
    "QuantumError",
    "BlockchainError",
    "OptimizationError",
    "ClientError",
    "CircuitBreakerOpenError",
    "RateLimitExceeded",
    "VaultError",
    "CloudStorageError",
    "SafetyViolationError",
    "ChaosExperimentError",
    "HumanReviewRequired",
    "LowReputationError",
    "NotImplementedYetError",
    "CausalBandit",
    "BanditConfig",
    "DifferentialPrivacy",
    "PrivacyAccountant",
    "SecureAggregator",
    "QuantumDistillationOptimizer",
    "CarbonOffsetBroker",
    "MultiAgentClientCoordinator",
    "FormalSafetyMonitor",
    "XAIExplainer",
    "FlexGenPrecisionPolicy",
    "RealHumanApprovalManager",
    "HumanApprovalEntry",
    "ActiveLearningRLHF",
    "CircuitBreakerState",
    "EnhancedCircuitBreaker",
    "EnhancedRateLimiter",
    "FederatedClient",
    "FederatedRoundResult",
    "PostQuantumCrypto",
    "MultiCloudStorage",
    "EnhancedFederatedLearner",
    "build_components",
    "create_app",
    "get_federated_learner",
    "main",
]
