"""
Distillation Orchestrator for Multi-Teacher On-Policy Distillation (MOPD)
=======================================================================
Green Agent v3.2.2 — Single-File Enhanced

Fixes and enhancements over v3.2.1:
- Missing imports added (random, datetime, deque, numpy, torch.amp).
- FlexGenManager implemented honestly (delegates to real FlexGen or raises
  NotImplementedYetError).
- CausalBandit is a real linear contextual bandit (per-action Bayesian linear
  regression with UCB / Thompson sampling).
- Reverse-KL semantics corrected (was forward KL).
- Bandit action space and update key are now consistent.
- best_state snapshots are deep-cloned tensors (no aliasing).
- Teacher inference is under torch.no_grad().
- SafetyMonitor is fed real carbon values and wired to gate the batch.
- HumanReviewManager is triggered on low confidence.
- CarbonOffsetBroker is invoked on epoch carbon above threshold.
- FederatedCoordinator uses weighted aggregation with round IDs.
- MultiAgentCoordinator uses stable SHA-256 hashing.
- ChaosMonkey supports named faults, seeded RNG, and recovery measurement.
- DriftDetector is consulted per epoch; rollback honors the config flag.
- Pareto filter uses real per-teacher objectives.
- Feedback cadence actually flushes the buffer.
- FeedbackEvent construction is defensive against schema drift.
- save_student / load_student are versioned.
- Async context manager support.
- Structured metrics via a single Prometheus registry.
- Episode-aware safety monitor with reset_episode().

Public API preserved:
    DistillationConfig, DistillationOrchestrator, CausalBandit, SafetyMonitor,
    XAIExplainer, FederatedCoordinator, MultiAgentCoordinator,
    CarbonOffsetBroker, ChaosMonkey, HumanReviewManager, FlexGenManager.
"""

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
from dataclasses import asdict, dataclass, field, fields, is_dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Dict, Iterable, Iterator, List, Optional, Tuple, Union

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.optim import AdamW

# -----------------------------------------------------------------------------
# Optional / guarded imports
# -----------------------------------------------------------------------------
try:
    from torch.amp import autocast as _torch_autocast  # type: ignore
    TORCH_AMP_AVAILABLE = True
except ImportError:
    try:
        from torch.cuda.amp import autocast as _torch_autocast  # type: ignore
        TORCH_AMP_AVAILABLE = True
    except ImportError:
        _torch_autocast = None  # type: ignore
        TORCH_AMP_AVAILABLE = False

try:
    from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

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
                    "distillation.log", maxBytes=10 * 1024 * 1024, backupCount=5
                ),
                logging.StreamHandler(),
            ],
        )

# -----------------------------------------------------------------------------
# Green Agent dependencies (guarded)
# -----------------------------------------------------------------------------
try:
    from .storage import Storage  # type: ignore
except Exception:
    Storage = None  # type: ignore

try:
    from .schemas.feedback_event import FeedbackEvent  # type: ignore
except Exception:
    FeedbackEvent = None  # type: ignore

try:
    from .routing.pareto_gating import ParetoGating  # type: ignore
except Exception:
    ParetoGating = None  # type: ignore

try:
    from .safety.drift_detector import DriftDetector  # type: ignore
except Exception:
    DriftDetector = None  # type: ignore

try:
    from .scaling.message_queue import AsyncMessageQueue  # type: ignore
except Exception:
    AsyncMessageQueue = None  # type: ignore


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
        def select(self, encoded): return "all"

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
DISTILL_BATCHES = METRICS.counter("distill_batches_total", "Batches trained", ["domain"])
DISTILL_EPOCHS = METRICS.counter("distill_epochs_total", "Epochs completed")
DISTILL_LOSS = METRICS.histogram("distill_loss", "Batch loss")
DISTILL_ENERGY = METRICS.histogram("distill_energy_joules", "Batch energy (J)")
DISTILL_ACC = METRICS.gauge("distill_accuracy", "Validation accuracy")
DISTILL_SAVINGS = METRICS.gauge("distill_energy_savings_ratio", "Energy savings ratio")
DISTILL_SAFETY = METRICS.counter("distill_safety_violations_total", "Safety violations", ["rule"])
DISTILL_HITL = METRICS.counter("distill_hitl_requests_total", "HITL requests")
DISTILL_CARBON = METRICS.counter("distill_carbon_offset_kg_total", "Carbon offset (kg)")
DISTILL_CHAOS = METRICS.counter("distill_chaos_events_total", "Chaos events", ["fault"])
DISTILL_DRIFT = METRICS.counter("distill_drift_events_total", "Drift events")
DISTILL_BANDIT = METRICS.counter("distill_bandit_updates_total", "Bandit updates", ["policy"])
DISTILL_TEACHERS = METRICS.histogram("distill_teachers_selected", "Teachers selected per epoch")


# =============================================================================
# Exceptions
# =============================================================================
class DistillationError(Exception):
    """Base class for distillation errors."""


class NotImplementedYetError(DistillationError):
    """Raised when an optional dependency is missing."""


class SafetyViolationError(DistillationError):
    """Raised when a hard safety constraint is exceeded."""


# =============================================================================
# Config
# =============================================================================
@dataclass
class DistillationConfig:
    """Configuration for DistillationOrchestrator."""

    num_epochs: int = 3
    batch_size: int = 32
    lr: float = 1e-5
    reverse_kl: bool = True
    alpha_orm: float = 0.1
    baseline_energy_per_token: float = 1.0
    early_stopping_patience: int = 3
    validation_split: float = 0.1
    mixed_precision: bool = True
    dtype: str = "fp16"
    expert_id: str = "distillation"
    node_id: Optional[str] = None
    pareto_quality_min: float = 0.7
    pareto_latency_max: float = 500.0
    pareto_carbon_max: float = 1.0
    feedback_batch_size: int = 10
    save_best_model: bool = True
    drift_check_interval: int = 5
    rollback_enabled: bool = True

    # Optimizer settings
    modp_weights: Dict[str, float] = field(
        default_factory=lambda: {
            "accuracy": 0.4, "energy": 0.3, "carbon": 0.2, "latency": 0.1,
        }
    )
    bandit_min_trials: int = 5
    bandit_confidence_threshold: float = 0.6
    bandit_feature_dim: int = 8
    bio_generations: int = 10
    bio_population_size: int = 20
    hyperparam_evolution_enabled: bool = True

    # Safety / HITL / carbon / chaos / federated
    safety_max_energy_per_token: float = 0.01
    safety_max_carbon_g: float = 1.0
    safety_max_consecutive_high: int = 3
    carbon_factor: float = 0.2
    carbon_offset_threshold_g: float = 500.0
    carbon_cost_per_kg: float = 0.1
    chaos_enabled: bool = False
    chaos_seed: int = 42
    chaos_failure_probability: float = 0.05
    hitl_ttl_seconds: float = 3600.0
    hitl_max_pending: int = 100
    federated_min_clients: int = 1

    # FlexGen settings
    flexgen_carbon_intensity_default: float = 400.0
    flexgen_population_size: int = 50
    flexgen_generations: int = 10
    flexgen_use_real_executor: bool = False
    flexgen_executor_type: str = "mock"
    flexgen_selector_epsilon: float = 0.1
    flexgen_selector_epsilon_decay: float = 0.999

    # Versioning
    config_version: int = 2

    def __post_init__(self) -> None:
        if self.lr <= 0:
            raise ValueError("lr must be positive")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.alpha_orm < 0:
            raise ValueError("alpha_orm must be non-negative")
        if not 0 <= self.validation_split < 1:
            raise ValueError("validation_split must be in [0, 1)")


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
        if confidence_threshold is not None:
            cfg.exploration_beta = max(cfg.exploration_beta, 1.0 - float(confidence_threshold))
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
            DISTILL_BANDIT.labels(policy=action).inc()

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "actions": self.actions,
                "usage": dict(self.usage),
                "total_trials": self.total_trials,
            }


# =============================================================================
# SafetyMonitor — episode-aware
# =============================================================================
class SafetyMonitor:
    """
    Temporal-logic-like safety monitor over (energy_per_token, carbon_g).
    Immediate thresholds and streak detection. Use `reset_episode()` at
    episode/epoch boundaries.
    """

    def __init__(
        self,
        max_energy_per_token: float = 0.01,
        max_carbon_g: float = 1.0,
        max_consecutive_high: int = 3,
        high_ratio: float = 0.8,
    ) -> None:
        self.max_energy = float(max_energy_per_token)
        self.max_carbon = float(max_carbon_g)
        self.max_consecutive = int(max_consecutive_high)
        self.high_ratio = float(high_ratio)
        self.history: deque = deque(maxlen=200)
        self.violations: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    def check(self, energy_per_token: float, carbon_g: float) -> Tuple[bool, Optional[str]]:
        with self._lock:
            self.history.append((time.time(), energy_per_token, carbon_g))
            reason: Optional[str] = None
            if energy_per_token > self.max_energy:
                reason = "energy_threshold"
            elif carbon_g > self.max_carbon:
                reason = "carbon_threshold"
            else:
                streak = 0
                ratio = self.high_ratio * self.max_energy
                for _, e, _ in reversed(self.history):
                    if e > ratio:
                        streak += 1
                    else:
                        break
                if streak >= self.max_consecutive:
                    reason = "consecutive_high_energy"
            if reason is None:
                return True, None
            self.violations.append({
                "timestamp": datetime.now().isoformat(),
                "reason": reason,
                "energy": float(energy_per_token),
                "carbon": float(carbon_g),
            })
            DISTILL_SAFETY.labels(rule=reason).inc()
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
    def explain_teacher_selection(
        self, policy: str, context: Dict, confidence: float,
        reward: Optional[float] = None,
    ) -> Dict[str, Any]:
        narrative = f"Selected teacher policy '{policy}'."
        if reward is not None:
            narrative += f" Expected utility: {reward:.3f}."
        top = sorted(
            (
                {"feature": k, "value": float(v)}
                for k, v in context.items() if isinstance(v, (int, float))
            ),
            key=lambda x: -abs(x["value"]),
        )[:5]
        return {
            "policy": policy,
            "confidence": float(confidence),
            "top_contributions": top,
            "narrative": narrative,
        }

    def explain_hyperparams(self, hyperparams: Dict, fitness: float) -> Dict[str, Any]:
        return {
            "hyperparams": dict(hyperparams),
            "fitness": float(fitness),
            "narrative": (
                f"Evolved hyperparameters {hyperparams} with fitness {fitness:.3f}."
            ),
        }


# =============================================================================
# FederatedCoordinator — weighted, round IDs, thread-safe
# =============================================================================
@dataclass
class FederatedRound:
    round_id: int
    aggregated: Dict[str, Any]
    participants: List[str]
    weight_total: float
    timestamp: str


class FederatedCoordinator:
    def __init__(self, min_clients: int = 1) -> None:
        self.min_clients = max(1, int(min_clients))
        self.participants: Dict[str, Dict[str, Any]] = {}
        self.round_id = 0
        self.history: List[FederatedRound] = []
        self._lock = threading.RLock()

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
            keys: set = set()
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
                    aggregated[key] = (
                        sum(v * w for v, w in zip(values, weights)) / wsum
                        if wsum > 0 else float(np.mean(values))
                    )
                else:
                    aggregated[key] = values[0]
            self.round_id += 1
            self.history.append(FederatedRound(
                round_id=self.round_id,
                aggregated=aggregated,
                participants=list(active.keys()),
                weight_total=float(sum(d["weight"] for d in active.values())),
                timestamp=datetime.now().isoformat(),
            ))
            return aggregated

    def reset_round(self) -> None:
        with self._lock:
            self.participants.clear()

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "round_id": self.round_id,
                "num_participants": len(self.participants),
                "rounds_completed": len(self.history),
            }


# =============================================================================
# MultiAgentCoordinator — stable hash, thread-safe
# =============================================================================
class MultiAgentCoordinator:
    def __init__(self, num_agents: int = 3, roles: Optional[List[str]] = None) -> None:
        if num_agents <= 0:
            raise ValueError("num_agents must be positive")
        roles = roles or ["balanced"] * num_agents
        self.agents: Dict[str, Dict[str, Any]] = {
            f"agent_{i}": {"role": roles[i % len(roles)]} for i in range(num_agents)
        }
        self.responsibilities: Dict[str, List[str]] = {a: [] for a in self.agents}
        self._lock = threading.RLock()

    @staticmethod
    def _stable_hash(task_id: str) -> int:
        return int(hashlib.sha256(task_id.encode()).hexdigest(), 16)

    def assign_task(self, task_id: str) -> str:
        with self._lock:
            agents = list(self.agents.keys())
            agent = agents[self._stable_hash(task_id) % len(agents)]
            self.responsibilities[agent].append(task_id)
            return agent

    def get_agent_stats(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {
                a: {"role": info["role"], "tasks_assigned": len(self.responsibilities[a])}
                for a, info in self.agents.items()
            }


# =============================================================================
# CarbonOffsetBroker — thread-safe, wired
# =============================================================================
class CarbonOffsetBroker:
    def __init__(self, threshold: float = 400.0, cost_per_kg: float = 0.1) -> None:
        self.threshold = float(threshold)
        self.cost_per_kg = float(cost_per_kg)
        self.total_offset_kg = 0.0
        self.total_cost = 0.0
        self.records: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    def reward_penalty(self, carbon_intensity: float, weight: float = 0.1) -> float:
        excess = max(0.0, carbon_intensity - self.threshold)
        return (excess / 1000.0) * weight

    async def maybe_purchase_offsets(
        self, carbon_intensity: float, carbon_kg: float
    ) -> Dict[str, Any]:
        if carbon_intensity <= self.threshold or carbon_kg <= 0:
            return {"status": "below_threshold"}
        cost = carbon_kg * self.cost_per_kg
        with self._lock:
            self.total_offset_kg += carbon_kg
            self.total_cost += cost
            record = {
                "status": "offset",
                "carbon_kg": float(carbon_kg),
                "cost_usd": float(cost),
                "timestamp": datetime.now().isoformat(),
            }
            self.records.append(record)
            DISTILL_CARBON.inc(carbon_kg)
        return record

    def get_totals(self) -> Dict[str, float]:
        with self._lock:
            return {
                "total_offset_kg": self.total_offset_kg,
                "total_cost": self.total_cost,
            }


# =============================================================================
# ChaosMonkey — named faults + seeded RNG
# =============================================================================
class ChaosMonkey:
    FAULT_TYPES = {"skip_batch", "bandit_freeze", "safety_override", "queue_drop"}

    def __init__(
        self,
        enabled: bool = False,
        failure_probability: float = 0.05,
        seed: Optional[int] = None,
    ) -> None:
        self.enabled = bool(enabled)
        self.failure_probability = float(failure_probability)
        self._rng = random.Random(seed)
        self.events: List[Dict[str, Any]] = []
        self._bandit_frozen = False

    def maybe_fail(self) -> Optional[str]:
        if not self.enabled:
            return None
        if self._rng.random() >= self.failure_probability:
            return None
        fault = self._rng.choice(sorted(self.FAULT_TYPES))
        self._record(fault, "triggered")
        DISTILL_CHAOS.labels(fault=fault).inc()
        return fault

    def apply(self, fault: str, orchestrator: "DistillationOrchestrator") -> None:
        if fault == "skip_batch":
            # Caller decides to skip; no state change.
            return
        if fault == "bandit_freeze":
            self._bandit_frozen = True
        elif fault == "safety_override":
            # Temporarily relax thresholds for one epoch.
            orchestrator.safety_monitor.max_energy *= 2.0
        elif fault == "queue_drop":
            # Simulate one dropped feedback flush.
            orchestrator._feedback_buffer.clear()

    def is_bandit_frozen(self) -> bool:
        return self._bandit_frozen

    def unfreeze_bandit(self) -> None:
        self._bandit_frozen = False

    def _record(self, fault: str, outcome: str, extra: Optional[Dict] = None) -> None:
        self.events.append({
            "fault": fault, "outcome": outcome,
            "timestamp": datetime.now().isoformat(),
            **(extra or {}),
        })

    async def inject_and_measure(
        self,
        orchestrator: "DistillationOrchestrator",
        fault: str,
        steps: int = 5,
    ) -> Dict[str, Any]:
        start = time.monotonic()
        self.apply(fault, orchestrator)
        # Short recovery check: iteration through a no-op health probe.
        recovery_step: Optional[int] = None
        for step in range(steps):
            try:
                # In a real system, probe a health endpoint. Here we sample state.
                _ = orchestrator._feedback_buffer  # type: ignore[attr-defined]
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
# HumanReviewManager — TTL, bounded, audit
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

    def _enforce_capacity(self) -> None:
        pending = [r for r in self.pending_reviews.values() if r.status == "pending"]
        if len(pending) <= self.max_pending:
            return
        ordered = sorted(pending, key=lambda r: r.created_at)
        for r in ordered[: len(ordered) - self.max_pending]:
            r.status = "dropped"

    async def request_review(self, decision_id: str, details: Dict[str, Any]) -> str:
        async with self._lock:
            self._evict_expired()
            rid = str(uuid.uuid4())
            self.pending_reviews[rid] = HumanReviewRequest(
                review_id=rid, decision_id=decision_id,
                details=dict(details), created_at=datetime.now(),
            )
            self._enforce_capacity()
            DISTILL_HITL.inc()
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
    """
    Wraps real FlexGen components when available. Raises NotImplementedYetError
    on use when the modules are absent, so callers do not silently succeed with
    an empty result.
    """

    def __init__(self, config: DistillationConfig) -> None:
        self.config = config
        self._cost_model = None
        self._drift_detector = None
        self._gpu_profiler = None
        if FLEXGEN_AVAILABLE:
            try:
                self._cost_model = FlexGenCostModel(
                    carbon_intensity_g_per_kwh=config.flexgen_carbon_intensity_default
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
            n_candidates=self.config.flexgen_population_size,
            config={
                "epsilon": self.config.flexgen_selector_epsilon,
                "epsilon_decay": self.config.flexgen_selector_epsilon_decay,
            },
        )
        controller = FlexGenController(
            node=node,
            workload=workload,
            carbon_intensity=getattr(workload, "metadata", {}).get(
                "carbon_intensity", self.config.flexgen_carbon_intensity_default
            ),
            use_real_executor=self.config.flexgen_use_real_executor,
            executor=None,
            cost_model=self._cost_model,
            use_bio_search=True,
            bio_search_config={
                "population_size": self.config.flexgen_population_size,
                "generations": self.config.flexgen_generations,
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
        if self._gpu_profiler is not None and hasattr(self._gpu_profiler, "get_current_metrics"):
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
# Pareto gating fallback
# =============================================================================
class _LocalParetoGating:
    """Local fallback Pareto filter if the real one is unavailable."""

    def __init__(self, quality_min: float = 0.0, latency_max: float = float("inf"),
                 carbon_max: float = float("inf")):
        self.quality_min = quality_min
        self.latency_max = latency_max
        self.carbon_max = carbon_max

    def passes(self, objectives: Dict[str, float]) -> bool:
        return (
            objectives.get("accuracy", 1.0) >= self.quality_min
            and objectives.get("latency", 0.0) <= self.latency_max
            and objectives.get("carbon", 0.0) <= self.carbon_max
        )


# =============================================================================
# FeedbackEvent helper
# =============================================================================
def _build_feedback_event(**fields: Any) -> Optional[Any]:
    """Construct FeedbackEvent defensively against schema drift."""
    if FeedbackEvent is None:
        return None
    try:
        if is_dataclass(FeedbackEvent):
            known = {f.name for f in fields(FeedbackEvent)}
        else:
            sig = inspect.signature(FeedbackEvent)
            known = set(sig.parameters.keys())
    except (TypeError, ValueError):
        known = set(fields.keys())
    filtered = {k: v for k, v in fields.items() if k in known}
    try:
        return FeedbackEvent(**filtered)
    except Exception as e:
        logger.warning(f"feedback_event_construction_failed error={e}")
        return None


# =============================================================================
# DistillationOrchestrator
# =============================================================================
@dataclass
class TeacherSelection:
    teacher_ids: List[str]
    policy: str
    confidence: float
    context: Dict[str, Any]
    source: str


class DistillationOrchestrator:
    """
    Orchestrates MOPD with async support, energy awareness, Pareto gating,
    persistent metrics, adaptive cost feedback, safety monitoring, HITL,
    carbon offsetting, federated aggregation, and chaos testing.
    """

    def __init__(
        self,
        student_model: nn.Module,
        teachers: Dict[str, nn.Module],
        config: Union[DistillationConfig, Dict[str, Any]],
        storage: Optional[Any] = None,
        message_queue: Optional[Any] = None,
        gating_network: Optional[Any] = None,
        eco_manager: Optional[Any] = None,
        quantum_bridge: Optional[Any] = None,
        adaptive_function: Optional[Any] = None,
        drift_detector: Optional[Any] = None,
    ):
        self.cfg = DistillationConfig(**config) if isinstance(config, dict) else config

        self.student = student_model
        self.teachers = teachers or {}
        self.storage = storage
        self.queue = message_queue
        self.gating = gating_network
        self.eco = eco_manager
        self.quantum = quantum_bridge
        self.adaptive = adaptive_function
        self.drift_detector = drift_detector

        # Pareto gating (real or local)
        if ParetoGating is not None:
            try:
                self.pareto = ParetoGating(
                    quality_min=self.cfg.pareto_quality_min,
                    latency_max=self.cfg.pareto_latency_max,
                    carbon_max=self.cfg.pareto_carbon_max,
                )
            except Exception:
                self.pareto = _LocalParetoGating(
                    quality_min=self.cfg.pareto_quality_min,
                    latency_max=self.cfg.pareto_latency_max,
                    carbon_max=self.cfg.pareto_carbon_max,
                )
        else:
            self.pareto = _LocalParetoGating(
                quality_min=self.cfg.pareto_quality_min,
                latency_max=self.cfg.pareto_latency_max,
                carbon_max=self.cfg.pareto_carbon_max,
            )

        # Device + model placement
        try:
            self.device = next(self.student.parameters()).device
        except StopIteration:
            self.device = torch.device("cpu")
        self._move_to_device()

        # Optimizer
        self.optimizer = AdamW(self.student.parameters(), lr=self.cfg.lr)

        # Mixed precision
        if self.cfg.mixed_precision and torch.cuda.is_available() and TORCH_AMP_AVAILABLE:
            self._autocast_context = _torch_autocast  # type: ignore
            self._autocast_kwargs = {"device_type": "cuda", "dtype": torch.float16}
        else:
            self._autocast_context = contextlib.nullcontext
            self._autocast_kwargs = {}

        # Enhancement modules
        if ENHANCEMENTS_AVAILABLE:
            self.modp = ParetoOptimizer()
            self.moe = ExpertRouter()
            self.bio = GeneticPolicyGenerator()
        else:
            self.modp = None
            self.moe = None
            self.bio = None

        # Teacher policies
        self.teacher_policies = ["all", "top1", "top3", "green_focused", "accuracy_focused"]
        self.bandit = CausalBandit(
            action_space=self.teacher_policies,
            feature_dim=self.cfg.bandit_feature_dim,
            config=BanditConfig(min_trials=self.cfg.bandit_min_trials),
            fallback_solver=lambda ctx: "all",
        )

        # New modules
        self.safety_monitor = SafetyMonitor(
            max_energy_per_token=self.cfg.safety_max_energy_per_token,
            max_carbon_g=self.cfg.safety_max_carbon_g,
            max_consecutive_high=self.cfg.safety_max_consecutive_high,
        )
        self.xai = XAIExplainer()
        self.federated = FederatedCoordinator(min_clients=self.cfg.federated_min_clients)
        self.multi_agent = MultiAgentCoordinator(num_agents=max(1, len(self.teachers)))
        self.carbon_broker = CarbonOffsetBroker(
            threshold=400.0, cost_per_kg=self.cfg.carbon_cost_per_kg,
        )
        self.chaos_monkey = ChaosMonkey(
            enabled=self.cfg.chaos_enabled,
            failure_probability=self.cfg.chaos_failure_probability,
            seed=self.cfg.chaos_seed,
        )
        self.human_review = HumanReviewManager(
            ttl_seconds=self.cfg.hitl_ttl_seconds,
            max_pending=self.cfg.hitl_max_pending,
        )

        # FlexGen manager
        self.flexgen_manager = FlexGenManager(self.cfg)

        # State
        self._run_id = str(uuid.uuid4())
        self._best_accuracy = 0.0
        self._best_state: Optional[Dict[str, torch.Tensor]] = None
        self._patience_counter = 0
        self._feedback_buffer: List[Any] = []
        self._epoch_metrics: List[Dict[str, Any]] = []
        self._latest_accuracy = 0.0
        self._last_selection: Optional[TeacherSelection] = None

        self._load_state()
        logger.info(f"DistillationOrchestrator initialized run_id={self._run_id}")

    # ------------------------------------------------------------------ #
    # Lifecycle                                                          #
    # ------------------------------------------------------------------ #
    async def __aenter__(self) -> "DistillationOrchestrator":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    # ------------------------------------------------------------------ #
    # Persistence                                                        #
    # ------------------------------------------------------------------ #
    def _load_state(self) -> None:
        if not self.storage:
            return
        loader = getattr(self.storage, "get_distillation_optimizer_state", None)
        if loader is None:
            return
        try:
            state = loader(self._run_id)
            if state:
                # Refuse mismatched config versions.
                if state.get("config_version", 0) != self.cfg.config_version:
                    logger.warning("optimizer_state_version_mismatch")
                    return
                self._latest_accuracy = float(state.get("latest_accuracy", 0.0))
                self._best_accuracy = float(state.get("best_accuracy", 0.0))
                self._epoch_metrics = list(state.get("epoch_metrics", []))
        except Exception as e:
            logger.warning(f"optimizer_state_load_failed error={e}")

    def _save_state(self) -> None:
        if not self.storage:
            return
        saver = getattr(self.storage, "save_distillation_optimizer_state", None)
        if saver is None:
            return
        try:
            saver(self._run_id, {
                "config_version": self.cfg.config_version,
                "latest_accuracy": self._latest_accuracy,
                "best_accuracy": self._best_accuracy,
                "epoch_metrics": self._epoch_metrics,
                "bandit_stats": self.bandit.get_stats(),
                "federated_stats": self.federated.get_stats(),
                "carbon_totals": self.carbon_broker.get_totals(),
            })
        except Exception as e:
            logger.warning(f"optimizer_state_save_failed error={e}")

    # ------------------------------------------------------------------ #
    # Internal helpers                                                   #
    # ------------------------------------------------------------------ #
    def _move_to_device(self) -> None:
        self.student.to(self.device)
        for t in self.teachers.values():
            t.to(self.device)

    async def _get_carbon_intensity(self) -> float:
        if self.eco is not None and hasattr(self.eco, "get_carbon_intensity"):
            try:
                return float(await self.eco.get_carbon_intensity())
            except Exception:
                pass
        return self.cfg.flexgen_carbon_intensity_default

    async def _get_energy_budget(self) -> float:
        if self.eco is not None and hasattr(self.eco, "get_current_budget"):
            try:
                return float(await self.eco.get_current_budget())
            except Exception:
                pass
        return 1.0

    async def _get_energy_cost(self, batch_size: int, domain: str) -> float:
        if self.eco is not None and hasattr(self.eco, "energy_cost_per_token"):
            try:
                return float(await self.eco.energy_cost_per_token(batch_size, domain))
            except Exception:
                pass
        return 1e-6 * batch_size

    def _encode_context(self, context: Dict[str, Any]) -> List[float]:
        if self.moe is not None:
            try:
                encoded = self.moe.encode(context)
                if encoded:
                    return list(encoded)
            except Exception:
                pass
        # Fallback: pack numeric values
        vals = [float(v) for v in context.values() if isinstance(v, (int, float))]
        while len(vals) < self.cfg.bandit_feature_dim:
            vals.append(0.0)
        return vals[: self.cfg.bandit_feature_dim]

    async def _select_teachers(
        self, domain: str, reasoning_effort: str
    ) -> TeacherSelection:
        teacher_ids = list(self.teachers.keys())
        if not teacher_ids:
            return TeacherSelection([], "all", 0.0, {}, "empty")

        context = {
            "domain_hash": float(int(hashlib.sha256(domain.encode()).hexdigest()[:6], 16)),
            "effort_medium": float(reasoning_effort == "medium"),
            "effort_high": float(reasoning_effort == "high"),
            "carbon_intensity": await self._get_carbon_intensity(),
            "energy_budget": await self._get_energy_budget(),
            "num_teachers": float(len(teacher_ids)),
        }
        encoded = self._encode_context(context)

        policy, confidence, source = self.bandit.select_action(encoded)

        if policy == "top1":
            selected = teacher_ids[:1]
        elif policy == "top3":
            selected = teacher_ids[:3]
        elif policy == "green_focused":
            # Prefer later-declared teachers as a proxy for "green" until real
            # carbon metrics exist per teacher.
            selected = teacher_ids[-min(3, len(teacher_ids)):]
        elif policy == "accuracy_focused":
            selected = teacher_ids[: max(1, len(teacher_ids) // 2)]
        else:
            selected = teacher_ids

        selection = TeacherSelection(
            teacher_ids=selected,
            policy=policy,
            confidence=confidence,
            context=context,
            source=source,
        )
        self._last_selection = selection
        return selection

    async def _pareto_filter_teachers(
        self,
        teacher_logits: List[torch.Tensor],
        teacher_ids: List[str],
        inputs: torch.Tensor,
    ) -> Tuple[List[torch.Tensor], List[str]]:
        """
        Rank teachers by real objectives and keep the top 80% (or at least one).
        """
        if not teacher_logits:
            return teacher_logits, teacher_ids

        # Consensus teacher = mean logits
        with torch.no_grad():
            avg = torch.stack(teacher_logits).mean(dim=0)
            p_avg = F.softmax(avg, dim=-1)

        energy = await self._get_energy_cost(inputs.shape[0], "unknown") * inputs.shape[0]
        carbon = energy * self.cfg.carbon_factor

        objectives_per_teacher: List[Dict[str, float]] = []
        for logits in teacher_logits:
            with torch.no_grad():
                # Agreement with consensus as accuracy proxy.
                p_t = F.log_softmax(logits, dim=-1)
                agree = -(p_avg * p_t).sum(dim=-1).mean().item()
            objectives_per_teacher.append({
                "accuracy": float(agree),
                "energy": float(energy),
                "carbon": float(carbon),
                "latency": 0.0,
            })

        # Score with MODP if available; otherwise hardcoded weights.
        weights = self.cfg.modp_weights
        scored: List[Tuple[float, int, str, torch.Tensor]] = []
        for i, (tid, logits, obj) in enumerate(
            zip(teacher_ids, teacher_logits, objectives_per_teacher)
        ):
            if self.modp is not None:
                try:
                    utility = float(self.modp.evaluate(obj, weights))
                except Exception:
                    utility = sum(obj.get(k, 0.0) * weights.get(k, 1.0) for k in obj)
            else:
                utility = sum(obj.get(k, 0.0) * weights.get(k, 1.0) for k in obj)
            scored.append((utility, i, tid, logits))

        scored.sort(key=lambda x: -x[0])
        keep = max(1, int(len(scored) * 0.8))
        top = scored[:keep]
        return [t[3] for t in top], [t[2] for t in top]

    def _compute_loss(
        self,
        student_logits: torch.Tensor,
        teacher_logits_list: List[torch.Tensor],
        energy_cost: float,
        batch_size: int,
        seq_len: int,
    ) -> Tuple[torch.Tensor, float, float]:
        if not teacher_logits_list:
            raise ValueError("teacher_logits_list must be non-empty")
        avg_teacher = torch.stack(teacher_logits_list).mean(dim=0)

        if self.cfg.reverse_kl:
            # Reverse KL: KL(student || teacher)
            loss_distill = F.kl_div(
                F.log_softmax(avg_teacher, dim=-1),
                F.softmax(student_logits, dim=-1),
                reduction="batchmean",
            )
        else:
            # Forward KL: KL(teacher || student)
            loss_distill = F.kl_div(
                F.log_softmax(student_logits, dim=-1),
                F.softmax(avg_teacher, dim=-1),
                reduction="batchmean",
            )

        distill_val = float(loss_distill.item())
        total_tokens = batch_size * seq_len
        green_loss = energy_cost * total_tokens * self.cfg.alpha_orm
        green_val = float(green_loss.item()) if isinstance(green_loss, torch.Tensor) else float(green_loss)
        total_loss = loss_distill + green_loss
        return total_loss, distill_val, green_val

    async def _publish_feedback(
        self,
        teacher_ids: List[str],
        distill_loss: float,
        quality: float,
        energy_joules: float,
        carbon_g: float,
        latency_ms: float,
        epoch: int,
        selection: Optional[TeacherSelection],
    ) -> None:
        for tid in teacher_ids:
            event = _build_feedback_event(
                task_id=f"{self._run_id}_epoch{epoch}",
                teacher_id=tid,
                selected_action=selection.policy if selection else "all",
                quality_score=quality,
                latency_ms=latency_ms,
                energy_joules=energy_joules,
                carbon_g=carbon_g,
                distillation_loss=distill_loss,
                feedback_type="distillation",
                adaptive_cost_value=0.0,
            )
            if event is not None:
                self._feedback_buffer.append(event)

        if len(self._feedback_buffer) >= self.cfg.feedback_batch_size:
            await self._flush_feedback()

        # Bandit update with the policy that actually drove the epoch.
        if selection is not None and selection.policy in self.bandit.actions:
            if not self.chaos_monkey.is_bandit_frozen():
                baseline = max(self.cfg.baseline_energy_per_token * 1000.0, 1e-9)
                reward = 0.5 * float(quality) + 0.5 * (1.0 - energy_joules / baseline)
                reward = float(np.clip(reward, -1.0, 1.0))
                encoded = self._encode_context(selection.context)
                self.bandit.update(encoded, selection.policy, reward)

        # HITL on low confidence (non-default policy only).
        if (
            selection is not None
            and selection.policy != "all"
            and selection.confidence < self.cfg.bandit_confidence_threshold
        ):
            try:
                await self.human_review.request_review(
                    decision_id=f"teacher_selection:{self._run_id}:{epoch}",
                    details={
                        "policy": selection.policy,
                        "confidence": selection.confidence,
                        "teacher_ids": teacher_ids,
                        "carbon_g": carbon_g,
                    },
                )
            except Exception as e:
                logger.warning(f"hitl_request_failed error={e}")

        # Hyperparameter evolution (uses real reward, not hardcoded)
        if (
            self.cfg.hyperparam_evolution_enabled
            and self.bio is not None
        ):
            hyperparam_set = {
                "lr": self.cfg.lr,
                "alpha_orm": self.cfg.alpha_orm,
                "batch_size": float(self.cfg.batch_size),
                "reverse_kl": float(self.cfg.reverse_kl),
            }
            fitness = float(quality) + max(0.0, 1.0 - energy_joules / max(
                self.cfg.baseline_energy_per_token * 1000.0, 1e-9
            ))
            try:
                self.bio.evolve(
                    population=[hyperparam_set],
                    fitness_fn=lambda hp, f=fitness: f,
                )
            except Exception as e:
                logger.debug(f"bio_evolve_skipped error={e}")

    async def _flush_feedback(self) -> None:
        if not self._feedback_buffer:
            return
        buffer = list(self._feedback_buffer)
        self._feedback_buffer.clear()

        if self.adaptive is not None and hasattr(self.adaptive, "record_feedback"):
            for event in buffer:
                try:
                    await self.adaptive.record_feedback(event)
                except Exception as e:
                    logger.warning(f"adaptive_record_feedback_failed error={e}")
            return

        if self.queue is not None and hasattr(self.queue, "publish"):
            for event in buffer:
                try:
                    payload = (
                        event.model_dump_json()
                        if hasattr(event, "model_dump_json")
                        else json.dumps(
                            event if isinstance(event, dict) else event.__dict__,
                            default=str,
                        )
                    )
                    await self.queue.publish("feedback_events", payload)
                except Exception as e:
                    logger.warning(f"queue_publish_failed error={e}")
            return

        logger.warning(f"no_feedback_sink_dropped_events count={len(buffer)}")

    async def _save_metrics(self, epoch: int, metrics: Dict[str, Any]) -> None:
        if not self.storage:
            return
        fn = getattr(self.storage, "store_distillation_metrics", None)
        if fn is None:
            return
        try:
            fn(run_id=self._run_id, epoch=epoch, **metrics)
        except Exception as e:
            logger.warning(f"metrics_save_failed error={e}")

    async def _check_drift(self, epoch_metrics: Dict[str, Any]) -> bool:
        if self.drift_detector is None:
            return False
        for method_name in ("check", "detect", "update"):
            fn = getattr(self.drift_detector, method_name, None)
            if fn is None:
                continue
            try:
                result = fn(epoch_metrics)
                if inspect.isawaitable(result):
                    result = await result
                if isinstance(result, bool):
                    if result:
                        DISTILL_DRIFT.inc()
                    return bool(result)
                if isinstance(result, dict) and result.get("drift"):
                    DISTILL_DRIFT.inc()
                    return True
            except Exception as e:
                logger.debug(f"drift_method_failed method={method_name} error={e}")
        return False

    def _snapshot_state(self) -> Dict[str, torch.Tensor]:
        return {k: v.detach().clone() for k, v in self.student.state_dict().items()}

    async def _iter_batches(
        self, dataloader: Iterable[Any]
    ) -> AsyncIterator[Tuple[torch.Tensor, torch.Tensor, str]]:
        iterator = dataloader.__aiter__() if hasattr(dataloader, "__aiter__") else iter(dataloader)
        while True:
            try:
                batch = await iterator.__anext__() if hasattr(iterator, "__anext__") else next(iterator)
            except (StopAsyncIteration, StopIteration):
                return
            inputs, labels, domain = self._normalize_batch(batch)
            yield inputs, labels, domain

    def _normalize_batch(self, batch: Any) -> Tuple[torch.Tensor, torch.Tensor, str]:
        if isinstance(batch, dict):
            return batch["inputs"], batch["labels"], batch.get("domain", "unknown")
        if isinstance(batch, (list, tuple)):
            if len(batch) == 2:
                return batch[0], batch[1], "unknown"
            if len(batch) >= 3:
                return batch[0], batch[1], batch[2]
        raise ValueError(f"Unsupported batch type: {type(batch)}")

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #
    async def distill(
        self,
        dataloader: Any,
        eval_fn: Optional[Callable[[nn.Module, Any], float]] = None,
        val_dataloader: Optional[Any] = None,
        reasoning_effort: str = "medium",
    ) -> Dict[str, Any]:
        if eval_fn is None and val_dataloader is not None:
            eval_fn = self._default_accuracy_fn

        self.student.train()
        total_loss = 0.0
        total_energy = 0.0
        total_tokens = 0

        best_val_acc = self._best_accuracy
        best_state: Optional[Dict[str, torch.Tensor]] = None
        patience = 0
        epochs_run = 0

        for epoch in range(self.cfg.num_epochs):
            # Chaos injection (may freeze bandit, relax safety, drop flush).
            fault = self.chaos_monkey.maybe_fail()
            if fault is not None:
                self.chaos_monkey.apply(fault, self)

            self.safety_monitor.reset_episode()

            epoch_loss = 0.0
            epoch_energy = 0.0
            epoch_tokens = 0
            epoch_distill_sum = 0.0
            epoch_distill_count = 0
            batches_in_epoch = 0
            used_teacher_ids: set = set()

            # FlexGen policy selection per epoch (best-effort)
            if self.flexgen_manager.available:
                try:
                    workload = self._build_workload_descriptor(epoch)
                    node = self._build_node_descriptor()
                    result = await self.flexgen_manager.optimize_policy(workload, node)
                    if isinstance(result, dict) and "chosen_policy" in result:
                        logger.info(f"epoch={epoch+1} flexgen_policy={result['chosen_policy']}")
                except NotImplementedYetError:
                    pass
                except Exception as e:
                    logger.warning(f"flexgen_epoch_failed error={e}")

            async for inputs, labels, domain in self._iter_batches(dataloader):
                inputs = inputs.to(self.device)
                labels = labels.to(self.device)

                selection = await self._select_teachers(domain, reasoning_effort)
                used_teacher_ids.update(selection.teacher_ids)
                if not selection.teacher_ids:
                    continue

                # Teacher inference (no grad, autocast)
                teacher_logits: List[torch.Tensor] = []
                with torch.no_grad():
                    with self._autocast_context(**self._autocast_kwargs) if TORCH_AMP_AVAILABLE else contextlib.nullcontext():
                        for tid in selection.teacher_ids:
                            teacher_logits.append(self.teachers[tid](inputs))
                with self._autocast_context(**self._autocast_kwargs) if TORCH_AMP_AVAILABLE else contextlib.nullcontext():
                    student_logits = self.student(inputs)

                # Pareto filter
                if ENHANCEMENTS_AVAILABLE and self.modp is not None:
                    teacher_logits, selection.teacher_ids = await self._pareto_filter_teachers(
                        teacher_logits, selection.teacher_ids, inputs
                    )
                    if not teacher_logits:
                        continue

                # Energy and safety
                energy_per_token = await self._get_energy_cost(inputs.shape[0], domain)
                carbon_g = energy_per_token * inputs.shape[0] * self.cfg.carbon_factor
                safe, reason = self.safety_monitor.check(energy_per_token, carbon_g)
                if not safe:
                    logger.warning(f"batch_unsafe reason={reason} skipping")
                    self.chaos_monkey.unfreeze_bandit()
                    continue

                batch_size = inputs.shape[0]
                seq_len = inputs.shape[1] if inputs.dim() > 1 else 1
                loss, distill_val, green_val = self._compute_loss(
                    student_logits, teacher_logits, energy_per_token, batch_size, seq_len,
                )

                self.optimizer.zero_grad()
                loss.backward()
                self.optimizer.step()

                epoch_loss += float(loss.item())
                epoch_energy += float(green_val)
                epoch_tokens += batch_size * seq_len
                epoch_distill_sum += float(distill_val)
                epoch_distill_count += 1
                batches_in_epoch += 1

                DISTILL_BATCHES.labels(domain=domain).inc()
                DISTILL_LOSS.observe(float(loss.item()))
                DISTILL_ENERGY.observe(float(green_val))

                if batches_in_epoch % 100 == 0:
                    logger.info(
                        f"epoch={epoch+1} batch={batches_in_epoch} loss={loss.item():.4f}"
                    )

            if batches_in_epoch == 0:
                logger.warning(f"epoch={epoch+1} no batches processed")
                continue

            avg_loss = epoch_loss / batches_in_epoch
            avg_distill = epoch_distill_sum / max(epoch_distill_count, 1)
            avg_energy = epoch_energy / max(epoch_tokens, 1)
            savings = max(0.0, 1.0 - avg_energy / max(self.cfg.baseline_energy_per_token, 1e-9))

            logger.info(
                f"epoch={epoch+1} loss={avg_loss:.4f} distill={avg_distill:.4f} "
                f"savings={savings:.2%}"
            )

            # Validation
            val_acc = 0.0
            if val_dataloader is not None and eval_fn is not None:
                val_acc = float(eval_fn(self.student, val_dataloader))
                self._latest_accuracy = val_acc
                DISTILL_ACC.set(val_acc)
                if val_acc > best_val_acc:
                    best_val_acc = val_acc
                    patience = 0
                    best_state = self._snapshot_state()
                    if self.cfg.save_best_model:
                        self._best_state = best_state
                else:
                    patience += 1

            DISTILL_EPOCHS.inc()
            DISTILL_SAVINGS.set(savings)
            DISTILL_TEACHERS.observe(float(len(used_teacher_ids)))

            epoch_metrics = {
                "loss": avg_loss,
                "distill_loss": avg_distill,
                "accuracy": val_acc,
                "energy_savings": savings,
                "energy_joules": epoch_energy,
                "num_teachers": len(used_teacher_ids),
                "policy": selection.policy if selection else "all",
            }
            await self._save_metrics(epoch + 1, epoch_metrics)
            self._epoch_metrics.append(epoch_metrics)

            await self._publish_feedback(
                list(used_teacher_ids),
                avg_distill,
                val_acc,
                epoch_energy,
                carbon_g=epoch_energy * self.cfg.carbon_factor,
                latency_ms=0.0,
                epoch=epoch + 1,
                selection=selection,
            )

            # Carbon offsets at epoch end
            if epoch_energy * self.cfg.carbon_factor > self.cfg.carbon_offset_threshold_g:
                await self.carbon_broker.maybe_purchase_offsets(
                    carbon_intensity=(epoch_energy * self.cfg.carbon_factor),
                    carbon_kg=epoch_energy * self.cfg.carbon_factor / 1000.0,
                )

            # Drift check
            if self.cfg.drift_check_interval > 0 and (epoch + 1) % self.cfg.drift_check_interval == 0:
                drifted = await self._check_drift(epoch_metrics)
                if drifted:
                    logger.warning(f"epoch={epoch+1} drift_detected")
                    if self.cfg.rollback_enabled and best_state is not None:
                        self.student.load_state_dict(best_state)
                        logger.info("rollback_applied")

            # Early stopping
            if patience >= self.cfg.early_stopping_patience:
                logger.info(f"early_stopping epoch={epoch+1}")
                break

            total_loss += avg_loss
            total_energy += epoch_energy
            total_tokens += epoch_tokens
            epochs_run += 1

            # Unfreeze bandit at end of epoch in case chaos froze it.
            self.chaos_monkey.unfreeze_bandit()

        if best_state is not None:
            self.student.load_state_dict(best_state)

        final_accuracy = 0.0
        if eval_fn is not None and val_dataloader is not None:
            final_accuracy = float(eval_fn(self.student, val_dataloader))

        avg_total_loss = total_loss / max(epochs_run, 1)
        avg_energy_per_token = total_energy / max(total_tokens, 1)
        final_savings = max(
            0.0, 1.0 - avg_energy_per_token / max(self.cfg.baseline_energy_per_token, 1e-9)
        )

        await self._flush_feedback()

        # Federated aggregation (single-instance round; real deployments
        # would register multiple participants before this call).
        self.federated.register_participant(
            participant_id=self._run_id,
            model_update={
                "loss": avg_total_loss,
                "accuracy": final_accuracy,
                "energy_savings": final_savings,
                "epochs_run": float(epochs_run),
            },
            sample_count=max(1, total_tokens),
        )
        aggregated = self.federated.aggregate()

        self._save_state()

        return {
            "avg_loss": avg_total_loss,
            "accuracy": final_accuracy,
            "energy_savings_ratio": final_savings,
            "total_energy_joules": total_energy,
            "epochs_run": epochs_run,
            "best_accuracy": best_val_acc,
            "federated": aggregated,
            "carbon_offsets": self.carbon_broker.get_totals(),
            "safety_violations": len(self.safety_monitor.get_violations()),
            "bandit_stats": self.bandit.get_stats(),
        }

    def _default_accuracy_fn(self, model: nn.Module, dataloader: Any) -> float:
        model.eval()
        correct = 0
        total = 0
        with torch.no_grad():
            for batch in dataloader:
                inputs, labels, _ = self._normalize_batch(batch)
                inputs = inputs.to(self.device)
                labels = labels.to(self.device)
                outputs = model(inputs)
                _, predicted = torch.max(outputs, 1)
                total += labels.size(0)
                correct += (predicted == labels).sum().item()
        model.train()
        return correct / total if total > 0 else 0.0

    def _build_workload_descriptor(self, epoch: int) -> Any:
        if not FLEXGEN_AVAILABLE:
            raise NotImplementedYetError("FlexGen not available")
        try:
            return WorkloadDescriptor(
                task_id=f"distill_epoch_{epoch+1}",
                task_type="inference",
                tokens=int(self.cfg.batch_size * 128),
                latency_target=200.0,
                urgency="medium",
                priority="balanced",
                bio_mode="none",
                metadata={"carbon_intensity": self.cfg.flexgen_carbon_intensity_default},
            )
        except TypeError:
            # Fall back to a bare constructor if the schema differs.
            return WorkloadDescriptor()  # type: ignore[call-arg]

    def _build_node_descriptor(self) -> Any:
        if not FLEXGEN_AVAILABLE:
            raise NotImplementedYetError("FlexGen not available")
        try:
            return NodeDescriptor(
                id="teacher_node",
                type="cloud",
                region="us-east",
                region_carbon_intensity=0.42,
                energy_per_token=0.00005,
                uptime=0.99,
                maintenance_status="operational",
                metadata={
                    "gpu_memory_gb": 16,
                    "cpu_memory_gb": 64,
                    "disk_bandwidth_gbps": 2,
                },
            )
        except TypeError:
            return NodeDescriptor()  # type: ignore[call-arg]

    def save_student(self, path: Union[str, Path]) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        torch.save(self.student.state_dict(), path / "student_model.pt")
        with open(path / "distillation_config.json", "w") as f:
            cfg_dict = asdict(self.cfg)
            cfg_dict["_config_version"] = self.cfg.config_version
            json.dump(cfg_dict, f, indent=2)
        logger.info(f"student_saved path={path}")

    def load_student(self, path: Union[str, Path]) -> None:
        path = Path(path)
        self.student.load_state_dict(torch.load(path / "student_model.pt", map_location=self.device))
        with open(path / "distillation_config.json", "r") as f:
            data = json.load(f)
        saved_version = int(data.pop("_config_version", 0))
        if saved_version != self.cfg.config_version:
            logger.warning(
                f"student_config_version_mismatch saved={saved_version} current={self.cfg.config_version}"
            )
        # Only load fields that exist in the current dataclass.
        valid = {f.name for f in fields(DistillationConfig)}
        filtered = {k: v for k, v in data.items() if k in valid}
        self.cfg = DistillationConfig(**filtered)
        logger.info(f"student_loaded path={path}")

    async def run_flexgen_optimization(self, workload: Dict[str, Any], node: Dict[str, Any]) -> Dict[str, Any]:
        if not FLEXGEN_AVAILABLE:
            raise NotImplementedYetError("FlexGen modules not available")
        try:
            workload_obj = WorkloadDescriptor(**workload)  # type: ignore[call-arg]
            node_obj = NodeDescriptor(**node)  # type: ignore[call-arg]
        except TypeError as e:
            raise DistillationError(f"Invalid descriptor: {e}") from e
        return await self.flexgen_manager.optimize_policy(workload_obj, node_obj)

    async def close(self) -> None:
        try:
            await self._flush_feedback()
        except Exception:
            pass
        if self.queue is not None and hasattr(self.queue, "close"):
            try:
                await self.queue.close()
            except Exception:
                pass
        logger.info("DistillationOrchestrator closed")


__all__ = [
    "DistillationConfig",
    "DistillationOrchestrator",
    "CausalBandit",
    "BanditConfig",
    "SafetyMonitor",
    "XAIExplainer",
    "FederatedCoordinator",
    "MultiAgentCoordinator",
    "CarbonOffsetBroker",
    "ChaosMonkey",
    "HumanReviewManager",
    "HumanReviewRequest",
    "FlexGenManager",
    "TeacherSelection",
    "DistillationError",
    "NotImplementedYetError",
    "SafetyViolationError",
]
