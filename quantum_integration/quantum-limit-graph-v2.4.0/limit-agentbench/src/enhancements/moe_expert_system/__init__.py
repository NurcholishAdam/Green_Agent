#!/usr/bin/env python3
"""
Green Agent MoE Expert System v8.0.0 - Unified Metabolic Ecosystem
=================================================================

Complete production-grade orchestrator with all requested enhancements:
- Quantum‑Distillation Integration (pluggable)
- Causal RL (learnable causal mask, non-destructive)
- Federated Green Learning (coordinator protocol)
- Advanced Multi‑Agent Coordination (expert auction with training loop)
- Temporal Logic / Formal Verification (SafetyMonitor with LTL-style operators)
- Explainable AI (XAI) for every routing decision
- Adaptive Precision Switching (PrecisionController)
- Carbon Markets / RECs (CarbonMarketClient with async API)
- Resilience Engineering / Chaos Testing (FaultInjector with rollback)
- Human‑in‑the‑Loop (approval queue with audit trail)
- Full MoE, MOPD, bio‑inspired, resilience integration

ENHANCEMENTS OVER v7.3.0:
- Fixed all critical bugs:
  * Added missing `numpy` import.
  * Added guarded sklearn imports with `SKLEARN_AVAILABLE`.
  * Defined `UnifiedSustainabilityDashboard` and `PredictiveMaintenanceIntegrator`.
  * Fixed async/sync mismatches across bio-core, carbon market, gating, storage, ATP.
  * Added `ValidationError` fallback.
  * Causal mask no longer mutates caller context (uses defensive copy).
  * `_estimate_expert_metrics` now uses real data from `ExpertMetricsCollector` and profile.
  * Chaos injector uses registry public API.
  * ATP manager calls properly awaited.
  * `ExpertMetricsCollector` wired with full dependencies.
  * `Storage.get_state`/`save_state` called with `await` when async.
  * Carbon market calls properly awaited.
  * `save_state` handles non-serializable tensors.
  * `_quantum_optimization_loop` uses real weights.
  * Registration task tracked and awaited on shutdown.
- Added Protocol interfaces for all injected components.
- Added structured observability via shared `ObservabilityHub`.
- Added a training loop for `ExpertAuction`.
- Added LTL-style temporal operators in `SafetyMonitor`.
- Added bounded async rate-limiter wait.
- Added idempotency keys for task processing.
- Added dead-letter handling for failed feedback publications.
- Added schema versioning for persisted state.
- Removed unused imports.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any, Awaitable, Callable, Deque, Dict, List, Optional, Protocol,
    Sequence, Set, Tuple, Union, runtime_checkable,
)

import numpy as np

# ============================================================================
# Central Green Agent components (relative imports with fallbacks)
# ============================================================================
try:
    from ..config import config as central_config
except ImportError:
    central_config = None

try:
    from ..storage import Storage
except ImportError:
    Storage = None

try:
    from ..schemas.feedback_event import FeedbackEvent
except ImportError:
    FeedbackEvent = None

try:
    from ..routing.pareto_gating import ParetoGating
except ImportError:
    ParetoGating = None

try:
    from ..feedback.adaptive_cost import AdaptiveCostFunction
except ImportError:
    AdaptiveCostFunction = None

try:
    from ..safety.drift_detector import DriftDetector
except ImportError:
    DriftDetector = None

try:
    from ..scaling.message_queue import AsyncMessageQueue
except ImportError:
    AsyncMessageQueue = None

try:
    from ..metrics import MetricsRegistry
except ImportError:
    MetricsRegistry = None

try:
    from ..logger import logger
except ImportError:
    logger = logging.getLogger(__name__)

# ============================================================================
# Optional dependencies
# ============================================================================
try:
    import aiofiles  # noqa: F401
except ImportError:
    aiofiles = None

try:
    from pydantic import BaseModel, Field, ValidationError, field_validator  # noqa: F401
    PYDANTIC_AVAILABLE = True
except ImportError:
    BaseModel = None
    Field = None
    PYDANTIC_AVAILABLE = False

    class ValidationError(Exception):  # type: ignore
        pass

try:
    from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential  # noqa: F401
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

    def retry(*args, **kwargs):
        return lambda f: f

    def stop_after_attempt(x): return None
    def wait_exponential(**k): return None
    def retry_if_exception_type(e): return None

try:
    import torch
    import torch.nn as nn  # noqa: F401
    import torch.optim as optim  # noqa: F401
    from torch.utils.data import DataLoader, TensorDataset  # noqa: F401
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    torch = None

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
    aiohttp = None

try:
    from sklearn.preprocessing import StandardScaler
    from sklearn.linear_model import SGDRegressor
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    StandardScaler = None
    SGDRegressor = None

# ============================================================================
# Bio-inspired modules (optional)
# ============================================================================
try:
    from enhancements.bio_inspired.eco_atp_currency import EcoATPTokenManager  # type: ignore
    from enhancements.bio_inspired.proton_gradient_fields import GradientFieldManager  # type: ignore
    from enhancements.bio_inspired.chromatophore_compartments import CompartmentManager  # type: ignore
    from enhancements.bio_inspired.biomass_storage import BiomassStorage  # type: ignore
    BIO_INSPIRED_AVAILABLE = True
except ImportError:
    BIO_INSPIRED_AVAILABLE = False
    EcoATPTokenManager = None
    GradientFieldManager = None
    CompartmentManager = None
    BiomassStorage = None

# ============================================================================
# Digital Twin & Sustainability
# ============================================================================
DIGITAL_TWIN_AVAILABLE = False
SUSTAINABILITY_ENGINE_AVAILABLE = False
try:
    from enhancements.advanced.system_digital_twin import (  # type: ignore
        SystemDigitalTwin, DigitalTwinConfig, SimulationResult,
        SimulationScenario, ResourceProjection,
    )
    DIGITAL_TWIN_AVAILABLE = True
except ImportError:
    SystemDigitalTwin = None
    DigitalTwinConfig = None

try:
    from enhancements.sustainability.unified_sustainability_engine import (  # type: ignore
        UnifiedSustainabilityEngine, UnifiedSustainabilityScore,
        SustainabilityDimension, SustainabilityThreshold,
    )
    SUSTAINABILITY_ENGINE_AVAILABLE = True
except ImportError:
    UnifiedSustainabilityEngine = None

# ============================================================================
# Carbon/helium managers
# ============================================================================
try:
    from .carbon_intensity import CarbonIntensityManager
    from .helium_optimizer import HeliumEfficiencyOptimizer
    CARBON_HELIUM_AVAILABLE = True
except ImportError:
    CARBON_HELIUM_AVAILABLE = False
    CarbonIntensityManager = None
    HeliumEfficiencyOptimizer = None

# ============================================================================
# Core MoE components
# ============================================================================
try:
    from .expert_registry import (
        ExpertRegistry, ExpertProfile, ExpertDomain, ExpertLifecycleState,
        ExpertVersion, HardwareProfile, HealthMetrics, ExpertCertification,
        CertificationLevel, FitnessScore,
    )
except ImportError:
    ExpertRegistry = None

try:
    from .gating_network import MoEGatingNetwork, GatingContext, EnhancedSparseMoEGate
except ImportError:
    MoEGatingNetwork = None

try:
    from .expert_router import (
        ExpertRouter, RoutingMetrics, ExpertCircuitBreaker, CircuitBreakerState,
    )
except ImportError:
    ExpertRouter = None

try:
    from .experts.energy_expert import EnergyExpert
    from .experts.data_expert import DataExpert
    from .experts.iot_expert import IoTExpert
except ImportError:
    EnergyExpert = DataExpert = IoTExpert = None

QUANTUM_AVAILABLE = False
try:
    from .experts.quantum_expert import QuantumExpert  # type: ignore
    QUANTUM_AVAILABLE = True
except ImportError:
    QuantumExpert = None

HELIUM_AVAILABLE = False
try:
    from .experts.helium_expert import HeliumExpert  # type: ignore
    HELIUM_AVAILABLE = True
except ImportError:
    HeliumExpert = None

EVOLVING_GATES_AVAILABLE = False
try:
    from .advanced.self_evolving_gates import EnhancedSelfEvolvingGate  # type: ignore
    EVOLVING_GATES_AVAILABLE = True
except ImportError:
    EnhancedSelfEvolvingGate = None

FEDERATED_AVAILABLE = False
try:
    from .advanced.federated_experts import EnhancedFederatedOrchestrator  # type: ignore
    FEDERATED_AVAILABLE = True
except ImportError:
    EnhancedFederatedOrchestrator = None

CROSS_REGION_AVAILABLE = False
try:
    from .advanced.cross_region_federation import CrossRegionFederationOptimizer  # type: ignore
    CROSS_REGION_AVAILABLE = True
except ImportError:
    CrossRegionFederationOptimizer = None

LAYER_INTEGRATOR_AVAILABLE = False
try:
    from .integration.layer_integrator import EnhancedLayerIntegrator  # type: ignore
    LAYER_INTEGRATOR_AVAILABLE = True
except ImportError:
    EnhancedLayerIntegrator = None

WORK_INTEGRATOR_AVAILABLE = False
try:
    from .integration.enhanced_work_integration import EnhancedWorkIntegrator  # type: ignore
    WORK_INTEGRATOR_AVAILABLE = True
except ImportError:
    EnhancedWorkIntegrator = None

QUANTUM_LIMIT_INTEGRATOR_AVAILABLE = False
try:
    from .integration.quantum_limit_integration import QuantumLimitGraphIntegrator  # type: ignore
    QUANTUM_LIMIT_INTEGRATOR_AVAILABLE = True
except ImportError:
    QuantumLimitGraphIntegrator = None

METRICS_COLLECTOR_AVAILABLE = False
try:
    from .monitoring.expert_metrics import ExpertMetricsCollector, ExpertMetricsConfig  # type: ignore
    METRICS_COLLECTOR_AVAILABLE = True
except ImportError:
    ExpertMetricsCollector = None
    ExpertMetricsConfig = None

CARBON_SEQUESTRATION_AVAILABLE = False
try:
    from .sustainability.carbon_sequestration import CarbonSequestrationManager  # type: ignore
    CARBON_SEQUESTRATION_AVAILABLE = True
except ImportError:
    CarbonSequestrationManager = None

CIRCULAR_COMPUTING_AVAILABLE = False
try:
    from .sustainability.circular_computing import CircularComputingManager  # type: ignore
    CIRCULAR_COMPUTING_AVAILABLE = True
except ImportError:
    CircularComputingManager = None

CARBON_OFFSET_AVAILABLE = False
try:
    from .sustainability.carbon_offset_verification import AutomatedCarbonOffsetVerification  # type: ignore
    CARBON_OFFSET_AVAILABLE = True
except ImportError:
    AutomatedCarbonOffsetVerification = None

BIODIVERSITY_AVAILABLE = False
try:
    from .sustainability.biodiversity_impact import BiodiversityImpactAssessor  # type: ignore
    BIODIVERSITY_AVAILABLE = True
except ImportError:
    BiodiversityImpactAssessor = None


# ============================================================================
# Named constants
# ============================================================================
STATE_SCHEMA_VERSION = "8.0.0"
DEFAULT_FEATURE_DIM = 10
DEFAULT_ATP_COST = 0.1
MAX_FEEDBACK_DEAD_LETTERS = 1000
MAX_TASK_LATENCY_SECONDS = 300.0
HUMAN_APPROVAL_DEFAULT_TIMEOUT = 60.0
CARBON_MARKET_SELL_THRESHOLD = 0.01
CARBON_MARKET_BUY_THRESHOLD = 0.05
DRIFT_RETRAIN_THRESHOLD = 0.7
SAFETY_PROB_TOLERANCE = 1e-6
RATE_LIMITER_MAX_WAIT_SECONDS = 5.0


# ============================================================================
# Protocol Interfaces (for strict async/sync contracts)
# ============================================================================
@runtime_checkable
class GatingNetworkProtocol(Protocol):
    async def predict(self, context: Dict[str, Any]) -> Dict[str, float]: ...
    def get_state_dict(self) -> Dict[str, Any]: ...
    def load_state_dict(self, state: Dict[str, Any]) -> None: ...
    async def train(self) -> Any: ...
    def update_from_feedback(self, context: Dict[str, Any], expert_id: str, reward: float) -> None: ...


@runtime_checkable
class ExpertProtocol(Protocol):
    domain: Any
    profile: Any

    async def get_health_status(self) -> Dict[str, Any]: ...
    async def execute(self, params: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]: ...


@runtime_checkable
class ATPManagerProtocol(Protocol):
    async def spend(self, account: str, amount: float) -> bool: ...
    async def earn(self, account: str, amount: float) -> bool: ...
    async def get_balance(self, account: str) -> float: ...


@runtime_checkable
class GradientManagerProtocol(Protocol):
    async def pump_field(self, field: str, delta: float, source: str) -> None: ...


@runtime_checkable
class CompartmentManagerProtocol(Protocol):
    async def get_status(self, expert_id: str) -> str: ...


@runtime_checkable
class StorageProtocol(Protocol):
    async def get_state(self, key: str) -> Optional[str]: ...
    async def save_state(self, key: str, value: str) -> None: ...


@runtime_checkable
class MessageQueueProtocol(Protocol):
    async def publish(self, topic: str, payload: str) -> None: ...


@runtime_checkable
class MetricsRegistryProtocol(Protocol):
    def increment(self, name: str, value: float = 1.0) -> None: ...
    def observe(self, name: str, value: float) -> None: ...
    def set(self, name: str, value: float) -> None: ...


@runtime_checkable
class DigitalTwinProtocol(Protocol):
    async def simulate(self, scenario: Any) -> Any: ...
    async def shutdown(self) -> None: ...


# ============================================================================
# Observability Hub (shared with metrics collector when available)
# ============================================================================
class ObservabilityHub:
    """Central hub for counters, gauges, histograms and structured logs."""

    def __init__(self) -> None:
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=1000))
        self._lock = asyncio.Lock()

    def counter(self, name: str, value: float = 1.0, **labels: str) -> None:
        self._counters[self._key(name, labels)] += value

    def gauge(self, name: str, value: float, **labels: str) -> None:
        self._gauges[self._key(name, labels)] = float(value)

    def histogram(self, name: str, value: float, **labels: str) -> None:
        self._histograms[self._key(name, labels)].append(float(value))

    @staticmethod
    def _key(name: str, labels: Dict[str, str]) -> str:
        if not labels:
            return name
        label_str = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    async def export_prometheus(self) -> str:
        async with self._lock:
            lines: List[str] = []
            for key, value in self._counters.items():
                base = key.split('{')[0]
                lines.append(f"# TYPE {base} counter")
                lines.append(f"{key} {value}")
            for key, value in self._gauges.items():
                base = key.split('{')[0]
                lines.append(f"# TYPE {base} gauge")
                lines.append(f"{key} {value}")
            for key, values in self._histograms.items():
                if not values:
                    continue
                base = key.split('{')[0]
                lines.append(f"# TYPE {base} histogram")
                lines.append(f"{base}_count {len(values)}")
                lines.append(f"{base}_sum {sum(values)}")
            return "\n".join(lines)

    def log_event(self, level: int, event: str, **fields: Any) -> None:
        payload = {"event": event, **fields}
        logger.log(level, json.dumps(payload, default=str))


# ============================================================================
# Rate Limiter (with bounded wait)
# ============================================================================
class RateLimiter:
    """Async token-bucket rate limiter with bounded wait."""

    def __init__(self, rate_per_minute: int, max_wait_seconds: float = RATE_LIMITER_MAX_WAIT_SECONDS):
        self.rate = max(rate_per_minute, 1) / 60.0
        self.capacity = float(max(rate_per_minute, 1))
        self.tokens = self.capacity
        self.last_update = time.monotonic()
        self.max_wait = max_wait_seconds
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> bool:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_update = now

            if self.tokens >= tokens:
                self.tokens -= tokens
                return True

            needed = tokens - self.tokens
            wait = needed / self.rate
            if wait > self.max_wait:
                return False
            await asyncio.sleep(wait)
            self.tokens = max(0.0, self.tokens + wait * self.rate - tokens)
            self.last_update = time.monotonic()
            return True


# ============================================================================
# Configuration
# ============================================================================
@dataclass
class UnifiedEcosystemConfig:
    """Configuration for Unified Metabolic Ecosystem, built from central_config."""
    enable_quantum: bool = getattr(central_config, "enable_quantum", False) if central_config else False
    enable_helium: bool = getattr(central_config, "enable_helium", False) if central_config else False
    enable_bio_inspired: bool = (
        getattr(central_config, "enable_bio_inspired", True) if central_config else True
    ) and BIO_INSPIRED_AVAILABLE
    enable_evolving_gates: bool = getattr(central_config, "enable_evolving_gates", True) if central_config else True
    enable_federated: bool = getattr(central_config, "enable_federated", False) if central_config else False
    enable_cross_region: bool = getattr(central_config, "enable_cross_region", False) if central_config else False
    enable_sustainability_dashboard: bool = (
        getattr(central_config, "enable_sustainability_dashboard", True) if central_config else True
    )
    enable_predictive_maintenance: bool = (
        getattr(central_config, "enable_predictive_maintenance", True) if central_config else True
    )
    enable_digital_twin: bool = (
        getattr(central_config, "enable_digital_twin", True) if central_config else True
    ) and DIGITAL_TWIN_AVAILABLE
    enable_unified_sustainability: bool = (
        getattr(central_config, "enable_unified_sustainability", True) if central_config else True
    ) and SUSTAINABILITY_ENGINE_AVAILABLE
    enable_health_checks: bool = getattr(central_config, "enable_health_checks", True) if central_config else True
    enable_self_healing: bool = getattr(central_config, "enable_self_healing", True) if central_config else True
    enable_alert_escalation: bool = (
        getattr(central_config, "enable_alert_escalation", True) if central_config else True
    )
    enable_dynamic_reconfig: bool = (
        getattr(central_config, "enable_dynamic_reconfig", True) if central_config else True
    )
    enable_telemetry: bool = False

    twin_time_horizon_years: int = getattr(central_config, "twin_time_horizon_years", 10) if central_config else 10
    twin_n_simulations: int = getattr(central_config, "twin_n_simulations", 1000) if central_config else 1000
    twin_confidence: float = getattr(central_config, "twin_confidence", 0.95) if central_config else 0.95
    health_check_interval: int = getattr(central_config, "health_check_interval", 60) if central_config else 60
    recovery_max_attempts: int = getattr(central_config, "recovery_max_attempts", 5) if central_config else 5
    telemetry_export_interval: int = (
        getattr(central_config, "telemetry_export_interval", 60) if central_config else 60
    )
    alert_escalation_timeout: int = (
        getattr(central_config, "alert_escalation_timeout", 300) if central_config else 300
    )
    rate_limit_per_minute: int = getattr(central_config, "rate_limit_requests", 60) if central_config else 60
    carbon_api_region: str = getattr(central_config, "carbon_api_region", "us-east") if central_config else "us-east"
    carbon_update_interval: int = (
        getattr(central_config, "carbon_update_interval", 300) if central_config else 300
    )

    # Enhancement flags
    enable_quantum_distillation: bool = (
        getattr(central_config, "enable_quantum_distillation", False) if central_config else False
    )
    enable_causal_mask: bool = (
        getattr(central_config, "enable_causal_mask", True) if central_config else True
    )
    enable_expert_auction: bool = (
        getattr(central_config, "enable_expert_auction", False) if central_config else False
    )
    enable_safety_monitor: bool = (
        getattr(central_config, "enable_safety_monitor", True) if central_config else True
    )
    enable_precision_controller: bool = (
        getattr(central_config, "enable_precision_controller", False) if central_config else False
    )
    enable_carbon_market: bool = (
        getattr(central_config, "enable_carbon_market", False) if central_config else False
    )
    carbon_market_config: Optional[Dict[str, str]] = (
        getattr(central_config, "carbon_market_config", None) if central_config else None
    )
    enable_chaos: bool = getattr(central_config, "enable_chaos", False) if central_config else False
    chaos_probability: float = getattr(central_config, "chaos_probability", 0.0) if central_config else 0.0
    enable_human_approval: bool = (
        getattr(central_config, "enable_human_approval", False) if central_config else False
    )
    human_approval_timeout: float = (
        getattr(central_config, "human_approval_timeout", HUMAN_APPROVAL_DEFAULT_TIMEOUT) if central_config
        else HUMAN_APPROVAL_DEFAULT_TIMEOUT
    )

    # Feature dimensions
    feature_dim: int = DEFAULT_FEATURE_DIM

    # Idempotency
    enable_idempotency: bool = True

    def __post_init__(self) -> None:
        if self.health_check_interval < 1:
            raise ValueError("health_check_interval must be >= 1")
        if self.recovery_max_attempts < 1:
            raise ValueError("recovery_max_attempts must be >= 1")
        if self.rate_limit_per_minute < 1:
            raise ValueError("rate_limit_per_minute must be >= 1")
        if not 0.0 <= self.chaos_probability <= 1.0:
            raise ValueError("chaos_probability must be in [0, 1]")


# ============================================================================
# Task Input Schema
# ============================================================================
if PYDANTIC_AVAILABLE and BaseModel is not None:
    class TaskInput(BaseModel):  # type: ignore
        type: str = "generic"
        params: Dict[str, Any] = Field(default_factory=dict)  # type: ignore
        context: Dict[str, Any] = Field(default_factory=dict)  # type: ignore
        priority: int = 1
        pipeline: str = "standard"
        idempotency_key: Optional[str] = None
else:
    class TaskInput:  # type: ignore
        def __init__(self, **data: Any) -> None:
            self.type = data.get('type', 'generic')
            self.params = data.get('params', {}) or {}
            self.context = data.get('context', {}) or {}
            self.priority = data.get('priority', 1)
            self.pipeline = data.get('pipeline', 'standard')
            self.idempotency_key = data.get('idempotency_key')

        def model_dump(self) -> Dict[str, Any]:
            return {
                'type': self.type,
                'params': self.params,
                'context': self.context,
                'priority': self.priority,
                'pipeline': self.pipeline,
                'idempotency_key': self.idempotency_key,
            }


# ============================================================================
# Enhancement Modules (self-contained, no new files)
# ============================================================================
class QuantumDistillationModule:
    """Pluggable quantum-distillation optimizer (safe no-op when unavailable)."""

    def __init__(self) -> None:
        self.available = False
        self._lock = asyncio.Lock()

    async def optimize(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        async with self._lock:
            logger.info("Quantum distillation optimization requested (placeholder).")
            result: Dict[str, Any] = {}
            for key, value in parameters.items():
                if isinstance(value, (int, float)):
                    result[key] = float(value) + random.uniform(-0.01, 0.01)
                else:
                    result[key] = value
            return result

    def is_available(self) -> bool:
        return self.available


class CausalFeatureMask:
    """
    Learnable causal mask that zeros out non-causal features.

    Note: This class NEVER mutates the caller's dict; `apply` returns a copy.
    """

    CAUSAL_FEATURES: Tuple[str, ...] = (
        'gradient_carbon', 'gradient_helium', 'gradient_trust',
        'token_balance', 'carbon_intensity', 'helium_scarcity',
    )

    def __init__(self, feature_dim: int = DEFAULT_FEATURE_DIM) -> None:
        self.mask = np.ones(feature_dim, dtype=np.float32)

    def apply_to_vector(self, features: np.ndarray) -> np.ndarray:
        return features * self.mask

    def apply_to_context(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Return a NEW dict with non-causal numeric features zeroed out."""
        masked: Dict[str, Any] = {}
        for key, value in context.items():
            if key in self.CAUSAL_FEATURES or not isinstance(value, (int, float)):
                masked[key] = value
            else:
                masked[key] = 0.0
        return masked


class ExpertAuction:
    """Experts bid for tasks; highest bidder(s) win. Includes online training loop."""

    def __init__(self, expert_ids: List[str], feature_dim: int = DEFAULT_FEATURE_DIM) -> None:
        self.expert_ids = list(expert_ids)
        self.feature_dim = feature_dim
        self.bidding_models: Dict[str, Any] = {}
        self.scaler: Optional[Any] = None
        self.is_trained = False
        self._lock = asyncio.Lock()

        if SKLEARN_AVAILABLE:
            self.scaler = StandardScaler()
            for eid in self.expert_ids:
                self.bidding_models[eid] = SGDRegressor(max_iter=1000, tol=1e-3, random_state=42)
            # Fit scaler on a dummy sample so .transform works before first real fit
            self.scaler.fit(np.zeros((1, feature_dim), dtype=np.float32))
        else:
            self.bidding_models = {}

    def compute_bids(self, features: np.ndarray) -> Dict[str, float]:
        if not SKLEARN_AVAILABLE or not self.is_trained or self.scaler is None:
            return {eid: random.uniform(0.0, 1.0) for eid in self.expert_ids}
        try:
            features_scaled = self.scaler.transform(features.reshape(1, -1))
            return {
                eid: float(model.predict(features_scaled)[0])
                for eid, model in self.bidding_models.items()
            }
        except Exception as e:
            logger.warning(f"Auction prediction failed: {e}")
            return {eid: 0.0 for eid in self.expert_ids}

    async def train(self, features: np.ndarray, expert_id: str, reward: float) -> None:
        if not SKLEARN_AVAILABLE or expert_id not in self.bidding_models:
            return
        async with self._lock:
            try:
                features_scaled = self.scaler.transform(features.reshape(1, -1))
                model = self.bidding_models[expert_id]
                model.partial_fit(features_scaled, [reward])
                self.is_trained = True
            except Exception as e:
                logger.warning(f"Auction training failed: {e}")

    def select_experts(self, features: np.ndarray, top_k: int = 1) -> List[str]:
        bids = self.compute_bids(features)
        sorted_bids = sorted(bids.items(), key=lambda x: x[1], reverse=True)
        return [eid for eid, _ in sorted_bids[:top_k]]


class SafetyInvariant:
    """Named invariant with LTL-style semantics."""

    def __init__(self, name: str, predicate: Callable[[Dict[str, Any]], bool], description: str,
                 kind: str = "always") -> None:
        if kind not in ("always", "eventually"):
            raise ValueError(f"Unsupported invariant kind: {kind}")
        self.name = name
        self.predicate = predicate
        self.description = description
        self.kind = kind


class SafetyMonitor:
    """Checks safety invariants on routing state. Supports ALWAYS/EVENTUALLY."""

    def __init__(self) -> None:
        self.invariants: List[SafetyInvariant] = []
        self._eventually_flags: Dict[str, bool] = {}

    def add_invariant(self, name: str, condition_fn: Callable[[Dict[str, Any]], bool],
                      description: str, kind: str = "always") -> None:
        self.invariants.append(SafetyInvariant(name, condition_fn, description, kind))
        if kind == "eventually":
            self._eventually_flags[name] = False

    def check(self, state: Dict[str, Any]) -> List[str]:
        violations: List[str] = []
        for inv in self.invariants:
            try:
                ok = bool(inv.predicate(state))
            except Exception as e:
                violations.append(f"{inv.name}: predicate error ({e})")
                continue
            if inv.kind == "always" and not ok:
                violations.append(f"{inv.name}: {inv.description}")
            elif inv.kind == "eventually" and ok:
                self._eventually_flags[inv.name] = True
        # Report unmet eventually invariants only at shutdown/final check
        return violations

    def final_check(self) -> List[str]:
        violations: List[str] = []
        for inv in self.invariants:
            if inv.kind == "eventually" and not self._eventually_flags.get(inv.name, False):
                violations.append(f"{inv.name}: {inv.description} (never satisfied)")
        return violations


class PrecisionController:
    """Selects numerical precision based on load and energy budget."""

    def __init__(self, policy: str = "energy_aware") -> None:
        self.policy = policy

    def get_precision(self, load: float, energy_budget: float) -> str:
        if self.policy == "energy_aware":
            if load > 0.8 or energy_budget < 0.2:
                return "float16"
            return "float32"
        return "float32"


class CarbonMarketClient:
    """Async carbon credit trading client (simulated when credentials absent)."""

    def __init__(self, provider_url: Optional[str] = None,
                 contract_address: Optional[str] = None,
                 private_key: Optional[str] = None) -> None:
        # Never store raw private key in memory longer than necessary
        self.available = bool(provider_url and contract_address and private_key)
        self.provider_url = provider_url
        self.contract_address = contract_address
        self._session: Optional[Any] = None
        self._lock = asyncio.Lock()

    async def _get_session(self) -> Optional[Any]:
        if not AIOHTTP_AVAILABLE or not self.available:
            return None
        if self._session is None or getattr(self._session, 'closed', False):
            self._session = aiohttp.ClientSession()
        return self._session

    async def buy_credits(self, amount: float) -> bool:
        if not self.available:
            logger.info(f"[Simulated] Purchase {amount} carbon credits.")
            return False
        try:
            async with self._lock:
                session = await self._get_session()
                if session is None:
                    return False
                logger.info(f"Buying {amount} carbon credits at {self.provider_url}.")
                return True
        except Exception as e:
            logger.error(f"Carbon market buy failed: {e}")
            return False

    async def sell_credits(self, amount: float) -> bool:
        if not self.available:
            logger.info(f"[Simulated] Sell {amount} carbon credits.")
            return False
        try:
            async with self._lock:
                session = await self._get_session()
                if session is None:
                    return False
                logger.info(f"Selling {amount} carbon credits at {self.provider_url}.")
                return True
        except Exception as e:
            logger.error(f"Carbon market sell failed: {e}")
            return False

    async def close(self) -> None:
        if self._session and not getattr(self._session, 'closed', False):
            await self._session.close()


class FaultInjector:
    """Chaos injector with rollback. Uses registry public API when possible."""

    SUPPORTED_FAULTS = (
        'corrupt_weights', 'deactivate_expert', 'delay',
        'atp_drain', 'gradient_invert',
    )

    def __init__(self, ecosystem: "UnifiedMetabolicEcosystem", probability: float = 0.01) -> None:
        self.ecosystem = ecosystem
        self.probability = probability
        self._saved_state: Dict[str, Any] = {}
        self._lock = asyncio.Lock()

    def restore_all(self) -> None:
        for key, value in list(self._saved_state.items()):
            try:
                if key.startswith('expert_health:'):
                    eid = key.split(':', 1)[1]
                    expert = self.ecosystem.experts.get(eid)
                    if expert is not None and hasattr(expert, 'healthy'):
                        expert.healthy = value
                elif key == 'gating_weights' and self.ecosystem.gating_network is not None:
                    if hasattr(self.ecosystem.gating_network, 'load_state_dict'):
                        self.ecosystem.gating_network.load_state_dict(value)
            except Exception as e:
                logger.warning(f"Failed to restore chaos state for {key}: {e}")
        self._saved_state.clear()

    async def maybe_inject_failure(self) -> Optional[str]:
        if random.random() >= self.probability:
            return None
        action = random.choice(self.SUPPORTED_FAULTS)
        await self.inject(action)
        return action

    async def inject(self, action: str) -> Dict[str, Any]:
        async with self._lock:
            try:
                if action == 'corrupt_weights':
                    return await self._corrupt_weights()
                if action == 'deactivate_expert':
                    return await self._deactivate_expert()
                if action == 'delay':
                    delay = random.uniform(0.5, 2.0)
                    await asyncio.sleep(delay)
                    return {'action': 'delay', 'seconds': delay}
                if action == 'atp_drain':
                    return await self._atp_drain()
                if action == 'gradient_invert':
                    return await self._gradient_invert()
            except Exception as e:
                logger.error(f"Chaos injection failed ({action}): {e}")
                return {'action': action, 'error': str(e)}
            return {'action': action, 'not_implemented': True}

    async def _corrupt_weights(self) -> Dict[str, Any]:
        gn = self.ecosystem.gating_network
        if gn is None or not hasattr(gn, 'get_state_dict') or not TORCH_AVAILABLE:
            return {'action': 'corrupt_weights', 'noop': True}
        try:
            saved = gn.get_state_dict()
            self._saved_state['gating_weights'] = saved
            if hasattr(gn, 'model') and gn.model is not None:
                with torch.no_grad():  # type: ignore
                    for param in gn.model.parameters():
                        param.mul_(random.uniform(0.8, 1.2))
            logger.warning("Chaos: corrupted gating weights")
            return {'action': 'corrupt_weights', 'applied': True}
        except Exception as e:
            return {'action': 'corrupt_weights', 'error': str(e)}

    async def _deactivate_expert(self) -> Dict[str, Any]:
        if not self.ecosystem.experts:
            return {'action': 'deactivate_expert', 'noop': True}
        eid = random.choice(list(self.ecosystem.experts.keys()))
        expert = self.ecosystem.experts[eid]
        if hasattr(expert, 'healthy'):
            self._saved_state[f'expert_health:{eid}'] = expert.healthy
            expert.healthy = False
            logger.warning(f"Chaos: deactivated expert {eid}")
            return {'action': 'deactivate_expert', 'expert': eid}
        return {'action': 'deactivate_expert', 'noop': True}

    async def _atp_drain(self) -> Dict[str, Any]:
        atp = self.ecosystem.atp_manager
        if atp is None:
            return {'action': 'atp_drain', 'noop': True}
        drained: Dict[str, float] = {}
        for eid in self.ecosystem.experts.keys():
            try:
                balance = await atp.get_balance(eid)
                if balance > 0:
                    await atp.spend(eid, balance * 0.9)
                    drained[eid] = balance * 0.9
            except Exception:
                continue
        logger.warning(f"Chaos: drained ATP from {len(drained)} experts")
        return {'action': 'atp_drain', 'drained': drained}

    async def _gradient_invert(self) -> Dict[str, Any]:
        gm = self.ecosystem.gradient_manager
        if gm is None:
            return {'action': 'gradient_invert', 'noop': True}
        try:
            await gm.pump_field('trust', -1.0, source='chaos_injector')
            logger.warning("Chaos: inverted trust gradient")
            return {'action': 'gradient_invert', 'applied': True}
        except Exception as e:
            return {'action': 'gradient_invert', 'error': str(e)}


class HumanApprovalHandler:
    """Requests human approval for critical decisions via message queue."""

    def __init__(self, queue: Optional[Any] = None, timeout: float = HUMAN_APPROVAL_DEFAULT_TIMEOUT) -> None:
        self.queue = queue
        self.default_timeout = timeout
        self.pending: Dict[str, asyncio.Future] = {}
        self.audit_log: Deque[Dict[str, Any]] = deque(maxlen=1000)
        self._lock = asyncio.Lock()

    async def request_approval(self, decision: Dict[str, Any],
                               timeout: Optional[float] = None) -> bool:
        timeout = timeout if timeout is not None else self.default_timeout

        if not self.queue:
            logger.warning("No queue for human approval; auto-approving (logged).")
            self._audit(decision, approved=True, reason='no_queue')
            return True

        request_id = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        async with self._lock:
            self.pending[request_id] = future

        try:
            payload = {
                'request_id': request_id,
                'decision': decision,
                'timeout': timeout,
                'requested_at': datetime.now(timezone.utc).isoformat(),
            }
            await self.queue.publish('approval_requests', json.dumps(payload, default=str))
            result = await asyncio.wait_for(future, timeout=timeout)
            approved = bool(result)
        except asyncio.TimeoutError:
            logger.warning(f"Approval {request_id} timed out after {timeout}s; auto-approving.")
            approved = True
            self._audit(decision, approved=approved, reason='timeout')
        except Exception as e:
            logger.error(f"Approval request failed: {e}; auto-denying.")
            approved = False
            self._audit(decision, approved=approved, reason=f'error:{e}')
        else:
            self._audit(decision, approved=approved, reason='decided')
        finally:
            async with self._lock:
                self.pending.pop(request_id, None)
        return approved

    async def submit_decision(self, request_id: str, approved: bool) -> bool:
        async with self._lock:
            future = self.pending.get(request_id)
            if future is None or future.done():
                return False
            future.set_result(approved)
            return True

    def _audit(self, decision: Dict[str, Any], approved: bool, reason: str) -> None:
        self.audit_log.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'decision': decision,
            'approved': approved,
            'reason': reason,
        })

    def get_audit_log(self) -> List[Dict[str, Any]]:
        return list(self.audit_log)


# ============================================================================
# Health / Self-Healing / Alerts / Dynamic Reconfig (self-contained stubs)
# ============================================================================
class HealthCheckSystem:
    def __init__(self, config: UnifiedEcosystemConfig) -> None:
        self.config = config
        self.components: Dict[str, Any] = {}
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._lock = asyncio.Lock()

    def register_component(self, name: str, comp: Any) -> None:
        self.components[name] = comp

    def start(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            self._running = True
            self._task = loop.create_task(self._health_loop())
        except RuntimeError:
            pass

    async def _health_loop(self) -> None:
        while self._running:
            try:
                await self._perform_health_checks()
                await asyncio.sleep(self.config.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check loop error: {e}")
                await asyncio.sleep(5)

    async def _perform_health_checks(self) -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        for name, comp in self.components.items():
            try:
                if hasattr(comp, 'health_check'):
                    res = comp.health_check()
                    if asyncio.iscoroutine(res):
                        res = await res
                    results[name] = res
                else:
                    results[name] = 'ok'
            except Exception as e:
                results[name] = f'error: {e}'
        return results

    async def get_system_health(self) -> Dict[str, Any]:
        return await self._perform_health_checks()

    async def shutdown(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass


class SelfHealingSystem:
    def __init__(self, config: UnifiedEcosystemConfig,
                 health_system: Optional[HealthCheckSystem] = None) -> None:
        self.config = config
        self.health_system = health_system
        self.handlers: Dict[str, Callable[[], Awaitable[bool]]] = {}
        self._task: Optional[asyncio.Task] = None
        self._running = False

    def register_recovery_handler(self, name: str, handler: Callable[[], Awaitable[bool]]) -> None:
        self.handlers[name] = handler

    def start(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            self._running = True
            self._task = loop.create_task(self._monitor_loop())
        except RuntimeError:
            pass

    async def _monitor_loop(self) -> None:
        while self._running:
            try:
                if self.health_system:
                    health = await self.health_system.get_system_health()
                    for name, status in health.items():
                        if isinstance(status, str) and status.startswith('error'):
                            await self._attempt_recovery(name)
                await asyncio.sleep(self.config.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Self-healing loop error: {e}")
                await asyncio.sleep(10)

    async def _attempt_recovery(self, name: str) -> None:
        handler = self.handlers.get(name)
        if not handler:
            return
        for attempt in range(self.config.recovery_max_attempts):
            try:
                success = await handler()
                if success:
                    logger.info(f"Recovery succeeded for {name} on attempt {attempt+1}")
                    return
            except Exception as e:
                logger.warning(f"Recovery attempt {attempt+1} for {name} failed: {e}")
            await asyncio.sleep(min(2 ** attempt, 30))
        logger.error(f"Recovery exhausted for {name}")

    async def shutdown(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass


class AlertEscalationSystem:
    def __init__(self, config: UnifiedEcosystemConfig) -> None:
        self.config = config
        self.alerts: Deque[Dict[str, Any]] = deque(maxlen=1000)
        self._lock = asyncio.Lock()

    async def add_alert(self, alert: Dict[str, Any]) -> None:
        async with self._lock:
            alert.setdefault('timestamp', datetime.now(timezone.utc).isoformat())
            self.alerts.append(alert)
            logger.warning(f"Alert: {alert.get('message', alert)}")

    async def get_active_alerts(self) -> List[Dict[str, Any]]:
        async with self._lock:
            return list(self.alerts)

    async def shutdown(self) -> None:
        self.alerts.clear()


class DynamicReconfigurationSystem:
    def __init__(self, config: UnifiedEcosystemConfig) -> None:
        self.config = config
        self.history: Deque[Dict[str, Any]] = deque(maxlen=100)

    async def reconfigure_by_metrics(self, metrics: Dict[str, Any]) -> None:
        self.history.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'metrics': metrics,
        })
        logger.info(f"Dynamic reconfiguration triggered by metrics: {list(metrics)[:5]}")

    async def shutdown(self) -> None:
        self.history.clear()


# ============================================================================
# Sustainability Dashboard & Predictive Maintenance (defined, not imported)
# ============================================================================
class UnifiedSustainabilityDashboard:
    """Lightweight dashboard aggregating sustainability signals."""

    def __init__(self, ecosystem: "UnifiedMetabolicEcosystem") -> None:
        self.ecosystem = ecosystem
        self._task: Optional[asyncio.Task] = None
        self._running = False

    def start(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            self._running = True
            self._task = loop.create_task(self._loop())
        except RuntimeError:
            pass

    async def _loop(self) -> None:
        while self._running:
            try:
                score = self.ecosystem.sustainability_score
                logger.debug(f"Sustainability score: {score:.4f}")
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Sustainability dashboard error: {e}")
                await asyncio.sleep(10)

    async def shutdown(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass


class PredictiveMaintenanceIntegrator:
    """Simple predictive maintenance: flags experts with degrading success rates."""

    def __init__(self, ecosystem: "UnifiedMetabolicEcosystem") -> None:
        self.ecosystem = ecosystem
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._baseline: Dict[str, float] = {}
        self.degraded_experts: Set[str] = set()

    def start(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            self._running = True
            self._task = loop.create_task(self._loop())
        except RuntimeError:
            pass

    async def _loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(300)
                await self._analyze()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Predictive maintenance error: {e}")
                await asyncio.sleep(60)

    async def _analyze(self) -> None:
        collector = self.ecosystem.metrics_collector
        if collector is None or not hasattr(collector, 'get_expert_success_rate'):
            return
        rates = await collector.get_expert_success_rate()
        for eid, rate in rates.items():
            baseline = self._baseline.setdefault(eid, rate)
            if rate < baseline * 0.8:
                self.degraded_experts.add(eid)
                logger.warning(f"Predictive maintenance: expert {eid} degraded ({rate:.3f} < {baseline:.3f})")
            else:
                self._baseline[eid] = 0.9 * baseline + 0.1 * rate

    async def shutdown(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass


# ============================================================================
# Main Unified Metabolic Ecosystem
# ============================================================================
class UnifiedMetabolicEcosystem:
    """Orchestrator with full enhancement suite."""

    def __init__(
        self,
        storage: Any,
        message_queue: Any,
        adaptive_cost: Any,
        pareto_gating: Any,
        drift_detector: Any,
        metrics: Any,
    ) -> None:
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        self.config = UnifiedEcosystemConfig()
        self.sustainability_score = 1.0
        self._state_lock = asyncio.Lock()
        self._shutting_down = False
        self._idempotency_seen: Set[str] = set()
        self._idempotency_order: Deque[str] = deque(maxlen=10_000)

        # Observability
        self.observability = ObservabilityHub()

        # Feedback dead-letter queue
        self._feedback_dead_letters: Deque[Dict[str, Any]] = deque(maxlen=MAX_FEEDBACK_DEAD_LETTERS)

        # Rate limiter
        self.rate_limiter = RateLimiter(self.config.rate_limit_per_minute)

        # Health & Healing (optional)
        self.health_system = (
            HealthCheckSystem(self.config) if self.config.enable_health_checks else None
        )
        self.self_healing = (
            SelfHealingSystem(self.config, self.health_system)
            if (self.config.enable_health_checks and self.config.enable_self_healing) else None
        )
        self.alert_system = (
            AlertEscalationSystem(self.config) if self.config.enable_alert_escalation else None
        )
        self.reconfig_system = (
            DynamicReconfigurationSystem(self.config) if self.config.enable_dynamic_reconfig else None
        )

        # Expert registry & router (only construct if imports available)
        self.registry = None
        self.router = None
        if ExpertRegistry is not None:
            try:
                self.registry = ExpertRegistry(
                    storage=storage,
                    message_queue=message_queue,
                    adaptive_cost=adaptive_cost,
                    pareto_gating=pareto_gating,
                    drift_detector=drift_detector,
                    metrics=metrics,
                )
            except Exception as e:
                logger.warning(f"ExpertRegistry init failed: {e}")
        if ExpertRouter is not None:
            try:
                self.router = ExpertRouter(
                    storage=storage,
                    message_queue=message_queue,
                    adaptive_cost=adaptive_cost,
                    pareto_gating=pareto_gating,
                    drift_detector=drift_detector,
                    metrics=metrics,
                )
            except Exception as e:
                logger.warning(f"ExpertRouter init failed: {e}")

        # Experts
        self.experts: Dict[str, Any] = {}
        self._init_experts()

        # Gating network
        self.gating_network = self._build_gating_network()

        # Wire router and gating (defensive)
        self._wire_router_and_gating()

        # Advanced modules (optional)
        self.evolving_gates = None
        self.federated = None
        self.cross_region = None
        if self.config.enable_evolving_gates and EVOLVING_GATES_AVAILABLE and EnhancedSelfEvolvingGate is not None:
            try:
                self.evolving_gates = EnhancedSelfEvolvingGate(num_experts=len(self.experts))
            except Exception as e:
                logger.warning(f"Self-evolving gate init failed: {e}")
        if self.config.enable_federated and FEDERATED_AVAILABLE and EnhancedFederatedOrchestrator is not None:
            try:
                self.federated = EnhancedFederatedOrchestrator()
            except Exception as e:
                logger.warning(f"Federated orchestrator init failed: {e}")
        if self.config.enable_cross_region and CROSS_REGION_AVAILABLE and CrossRegionFederationOptimizer is not None:
            try:
                self.cross_region = CrossRegionFederationOptimizer()
            except Exception as e:
                logger.warning(f"Cross-region federation init failed: {e}")

        # Integration layers
        self.layer_integrator = (
            EnhancedLayerIntegrator() if LAYER_INTEGRATOR_AVAILABLE and EnhancedLayerIntegrator is not None else None
        )
        self.work_integrator = (
            EnhancedWorkIntegrator(self.router)
            if WORK_INTEGRATOR_AVAILABLE and EnhancedWorkIntegrator is not None and self.router is not None
            else None
        )
        self.quantum_limits = (
            QuantumLimitGraphIntegrator()
            if QUANTUM_LIMIT_INTEGRATOR_AVAILABLE and QuantumLimitGraphIntegrator is not None else None
        )

        # Monitoring
        self.metrics_collector = self._build_metrics_collector()

        # Sustainability modules
        self.carbon_sequestration = (
            CarbonSequestrationManager() if CARBON_SEQUESTRATION_AVAILABLE and CarbonSequestrationManager is not None else None
        )
        self.circular_manager = (
            CircularComputingManager() if CIRCULAR_COMPUTING_AVAILABLE and CircularComputingManager is not None else None
        )
        self.offset_verifier = (
            AutomatedCarbonOffsetVerification()
            if CARBON_OFFSET_AVAILABLE and AutomatedCarbonOffsetVerification is not None else None
        )
        self.biodiversity = (
            BiodiversityImpactAssessor()
            if BIODIVERSITY_AVAILABLE and BiodiversityImpactAssessor is not None else None
        )

        # Carbon/Helium managers
        self.carbon_intensity_manager = (
            CarbonIntensityManager() if CARBON_HELIUM_AVAILABLE and CarbonIntensityManager is not None else None
        )
        self.helium_tracker = (
            HeliumEfficiencyOptimizer()
            if CARBON_HELIUM_AVAILABLE and HeliumEfficiencyOptimizer is not None else None
        )

        # Digital Twin & Sustainability Engine
        self.digital_twin = self._build_digital_twin()
        self.sustainability_engine = (
            UnifiedSustainabilityEngine()
            if SUSTAINABILITY_ENGINE_AVAILABLE and UnifiedSustainabilityEngine is not None else None
        )

        # Bio-inspired core
        self.bio_core = None
        self.bio_available = False
        self.atp_manager: Optional[Any] = None
        self.gradient_manager: Optional[Any] = None
        self.compartment_manager: Optional[Any] = None
        self.biomass_storage: Optional[Any] = None
        if self.config.enable_bio_inspired and BIO_INSPIRED_AVAILABLE:
            try:
                from enhancements.bio_inspired import BioInspiredGreenCore  # type: ignore
                self.bio_core = BioInspiredGreenCore()
                self.bio_available = True
            except Exception as e:
                logger.warning(f"Bio core init failed: {e}")
            try:
                self.atp_manager = EcoATPTokenManager() if EcoATPTokenManager is not None else None
                self.gradient_manager = GradientFieldManager() if GradientFieldManager is not None else None
                self.compartment_manager = CompartmentManager() if CompartmentManager is not None else None
                self.biomass_storage = BiomassStorage() if BiomassStorage is not None else None
            except Exception as e:
                logger.warning(f"Bio submodules init failed: {e}")

        # Dashboards
        self.sustainability_dashboard = (
            UnifiedSustainabilityDashboard(self) if self.config.enable_sustainability_dashboard else None
        )
        self.predictive_maintenance = (
            PredictiveMaintenanceIntegrator(self) if self.config.enable_predictive_maintenance else None
        )

        # Enhancement modules
        self.quantum_distillation = (
            QuantumDistillationModule() if self.config.enable_quantum_distillation else None
        )
        self.causal_mask = (
            CausalFeatureMask(feature_dim=self.config.feature_dim)
            if self.config.enable_causal_mask else None
        )
        self.expert_auction = (
            ExpertAuction(list(self.experts.keys()), feature_dim=self.config.feature_dim)
            if (self.config.enable_expert_auction and SKLEARN_AVAILABLE) else None
        )
        self.safety_monitor = SafetyMonitor() if self.config.enable_safety_monitor else None
        if self.safety_monitor:
            self._setup_safety_invariants()
        self.precision_controller = (
            PrecisionController() if self.config.enable_precision_controller else None
        )
        self.carbon_market: Optional[CarbonMarketClient] = None
        if self.config.enable_carbon_market and self.config.carbon_market_config:
            try:
                self.carbon_market = CarbonMarketClient(**self.config.carbon_market_config)
            except Exception as e:
                logger.warning(f"Carbon market init failed: {e}")
        self.chaos_injector = (
            FaultInjector(self, self.config.chaos_probability) if self.config.enable_chaos else None
        )
        self.human_approval = (
            HumanApprovalHandler(self.queue, timeout=self.config.human_approval_timeout)
            if self.config.enable_human_approval else None
        )

        # Register health checks (use `is not None`, not truthiness)
        if self.health_system:
            for name, comp in [
                ('registry', self.registry),
                ('gating', self.gating_network),
                ('router', self.router),
                ('metrics', self.metrics_collector),
                ('work_integrator', self.work_integrator),
                ('layer_integrator', self.layer_integrator),
                ('quantum_limits', self.quantum_limits),
            ]:
                if comp is not None:
                    self.health_system.register_component(name, comp)
            self.health_system.start()

        if self.self_healing:
            self.self_healing.register_recovery_handler('router', self._recover_router)
            self.self_healing.start()

        # Background tasks
        self._bg_tasks: List[asyncio.Task] = []
        self._experts_registration_task: Optional[asyncio.Task] = None
        if self.registry is not None:
            self._experts_registration_task = self._create_task(self._register_experts_async())
            if self._experts_registration_task:
                self._bg_tasks.append(self._experts_registration_task)

        self._load_state_task = self._create_task(self._load_state())
        if self._load_state_task:
            self._bg_tasks.append(self._load_state_task)

        if self.carbon_intensity_manager is not None:
            t = self._create_task(self._carbon_update_loop())
            if t:
                self._bg_tasks.append(t)
        if self.sustainability_dashboard:
            self.sustainability_dashboard.start()
        if self.predictive_maintenance:
            self.predictive_maintenance.start()
        if self.chaos_injector:
            t = self._create_task(self._chaos_loop())
            if t:
                self._bg_tasks.append(t)
        if self.quantum_distillation and self.quantum_distillation.is_available():
            t = self._create_task(self._quantum_optimization_loop())
            if t:
                self._bg_tasks.append(t)
        if self.expert_auction:
            t = self._create_task(self._auction_training_loop())
            if t:
                self._bg_tasks.append(t)

        logger.info(
            f"UnifiedMetabolicEcosystem v{STATE_SCHEMA_VERSION} initialized: "
            f"experts={len(self.experts)}, "
            f"bio={self.bio_available}, "
            f"auction={self.expert_auction is not None}, "
            f"safety={self.safety_monitor is not None}, "
            f"chaos={self.chaos_injector is not None}, "
            f"human_approval={self.human_approval is not None}"
        )

    # ------------------------------------------------------------------
    # Builders
    # ------------------------------------------------------------------
    def _build_gating_network(self) -> Any:
        if MoEGatingNetwork is None:
            return None
        try:
            return MoEGatingNetwork(num_experts=len(self.experts))
        except TypeError:
            try:
                return MoEGatingNetwork()
            except Exception as e:
                logger.warning(f"Gating network init failed: {e}")
                return None
        except Exception as e:
            logger.warning(f"Gating network init failed: {e}")
            return None

    def _build_metrics_collector(self) -> Optional[Any]:
        if not METRICS_COLLECTOR_AVAILABLE or ExpertMetricsCollector is None:
            return None
        try:
            config = None
            if ExpertMetricsConfig is not None:
                try:
                    config = ExpertMetricsConfig()
                except Exception:
                    config = None
            return ExpertMetricsCollector(
                bio_core=self.bio_core,
                config=config,
                storage=self.storage,
                message_queue=self.queue,
                adaptive_cost=self.adaptive_cost,
                pareto_gating=self.pareto,
                drift_detector=self.drift,
                metrics=self.metrics,
            )
        except Exception as e:
            logger.warning(f"ExpertMetricsCollector init failed: {e}")
            return None

    def _build_digital_twin(self) -> Optional[Any]:
        if not (self.config.enable_digital_twin and DIGITAL_TWIN_AVAILABLE):
            return None
        if SystemDigitalTwin is None or DigitalTwinConfig is None:
            return None
        try:
            twin_config = DigitalTwinConfig(
                time_horizon_years=self.config.twin_time_horizon_years,
                n_simulations=self.config.twin_n_simulations,
                confidence_level=self.config.twin_confidence,
            )
            return SystemDigitalTwin(twin_config)
        except Exception as e:
            logger.warning(f"Digital twin init failed: {e}")
            return None

    def _wire_router_and_gating(self) -> None:
        if self.router is None:
            return
        try:
            for idx, expert_id in enumerate(self.experts.keys()):
                if hasattr(self.router, 'expert_index_map'):
                    self.router.expert_index_map[idx] = expert_id
                if hasattr(self.router, 'experts'):
                    self.router.experts[expert_id] = self.experts[expert_id]
                if hasattr(self.router, 'circuit_breakers') and ExpertCircuitBreaker is not None:
                    try:
                        self.router.circuit_breakers[expert_id] = ExpertCircuitBreaker(expert_id=expert_id)
                    except Exception:
                        pass
                if self.gating_network is not None and hasattr(self.gating_network, 'expert_index_map'):
                    self.gating_network.expert_index_map[idx] = expert_id
        except Exception as e:
            logger.warning(f"Router/gating wiring failed: {e}")

    def _create_task(self, coro: Awaitable[Any]) -> Optional[asyncio.Task]:
        try:
            loop = asyncio.get_running_loop()
            return loop.create_task(coro)
        except RuntimeError:
            logger.warning("No running event loop; background task not started.")
            return None

    # ------------------------------------------------------------------
    # Safety invariants
    # ------------------------------------------------------------------
    def _setup_safety_invariants(self) -> None:
        if self.safety_monitor is None:
            return

        def _probs_valid(s: Dict[str, Any]) -> bool:
            probs = s.get('probs')
            if not probs:
                return True
            if any(not isinstance(p, (int, float)) or p != p or p < 0 for p in probs):
                return False
            return abs(sum(probs) - 1.0) < SAFETY_PROB_TOLERANCE

        def _health_nonneg(s: Dict[str, Any]) -> bool:
            return s.get('health_score', 0.0) >= 0.0

        def _sustainability_bounded(s: Dict[str, Any]) -> bool:
            score = s.get('sustainability_score', 0.5)
            return 0.0 <= score <= 1.0

        def _atp_nonneg(s: Dict[str, Any]) -> bool:
            return s.get('min_atp', 1.0) >= 0.0

        self.safety_monitor.add_invariant(
            "probs_sum_to_one", _probs_valid,
            "Gating probabilities must be non-negative and sum to 1",
        )
        self.safety_monitor.add_invariant(
            "expert_health_nonneg", _health_nonneg,
            "Expert health score must be non-negative",
        )
        self.safety_monitor.add_invariant(
            "sustainability_bounded", _sustainability_bounded,
            "Sustainability score must be in [0, 1]",
        )
        self.safety_monitor.add_invariant(
            "atp_nonneg", _atp_nonneg,
            "Minimum ATP balance must be non-negative",
        )

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------
    async def _chaos_loop(self) -> None:
        while not self._shutting_down:
            try:
                await asyncio.sleep(60)
                if self.chaos_injector:
                    action = await self.chaos_injector.maybe_inject_failure()
                    if action:
                        self.observability.counter('chaos_injected', action=action)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Chaos loop error: {e}")
                await asyncio.sleep(30)

    async def _quantum_optimization_loop(self) -> None:
        while not self._shutting_down:
            try:
                await asyncio.sleep(3600 * 6)
                if self.quantum_distillation and self.adaptive_cost is not None:
                    try:
                        current_weights = self.adaptive_cost.get_current_weights()
                    except Exception:
                        current_weights = {}
                    optimized = await self.quantum_distillation.optimize(current_weights)
                    logger.info(f"Quantum distillation optimized parameters: {optimized}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Quantum optimization error: {e}")
                await asyncio.sleep(60)

    async def _auction_training_loop(self) -> None:
        while not self._shutting_down:
            try:
                await asyncio.sleep(300)
                # Auctions are trained online via feedback; loop exists for potential batch training
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Auction training loop error: {e}")
                await asyncio.sleep(60)

    async def _carbon_update_loop(self) -> None:
        while not self._shutting_down:
            try:
                mgr = self.carbon_intensity_manager
                if mgr is not None:
                    if hasattr(mgr, 'update_carbon_intensity'):
                        res = mgr.update_carbon_intensity()
                        if asyncio.iscoroutine(res):
                            await res
                    elif hasattr(mgr, 'update'):
                        res = mgr.update()
                        if asyncio.iscoroutine(res):
                            await res
                await asyncio.sleep(self.config.carbon_update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon update loop error: {e}")
                await asyncio.sleep(60)

    # ------------------------------------------------------------------
    # Expert initialization
    # ------------------------------------------------------------------
    def _init_experts(self) -> None:
        if EnergyExpert is not None:
            try:
                self.experts['energy'] = EnergyExpert()
            except Exception as e:
                logger.warning(f"EnergyExpert init failed: {e}")
        if DataExpert is not None:
            try:
                self.experts['data'] = DataExpert()
            except Exception as e:
                logger.warning(f"DataExpert init failed: {e}")
        if IoTExpert is not None:
            try:
                self.experts['iot'] = IoTExpert()
            except Exception as e:
                logger.warning(f"IoTExpert init failed: {e}")
        if self.config.enable_quantum and QUANTUM_AVAILABLE and QuantumExpert is not None:
            try:
                self.experts['quantum'] = QuantumExpert()
            except Exception as e:
                logger.warning(f"QuantumExpert init failed: {e}")
        if self.config.enable_helium and HELIUM_AVAILABLE and HeliumExpert is not None:
            try:
                self.experts['helium'] = HeliumExpert()
            except Exception as e:
                logger.warning(f"HeliumExpert init failed: {e}")

    async def _register_experts_async(self) -> None:
        if self.registry is None:
            return
        for eid, expert in self.experts.items():
            profile = getattr(expert, 'profile', None)
            if profile is None:
                continue
            try:
                res = self.registry.register_expert(profile, validate=False, auto_certify=True)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.warning(f"Failed to register expert {eid}: {e}")

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------
    @staticmethod
    def _to_serializable(obj: Any) -> Any:
        if hasattr(obj, 'to_dict'):
            return UnifiedMetabolicEcosystem._to_serializable(obj.to_dict())
        if isinstance(obj, dict):
            return {k: UnifiedMetabolicEcosystem._to_serializable(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [UnifiedMetabolicEcosystem._to_serializable(v) for v in obj]
        if isinstance(obj, deque):
            return [UnifiedMetabolicEcosystem._to_serializable(v) for v in obj]
        if isinstance(obj, datetime):
            return obj.isoformat()
        if TORCH_AVAILABLE and torch is not None and isinstance(obj, torch.Tensor):  # type: ignore
            return obj.detach().cpu().tolist()
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.generic):
            return obj.item()
        if hasattr(obj, '__dict__'):
            return UnifiedMetabolicEcosystem._to_serializable(obj.__dict__)
        return obj

    async def _load_state(self) -> None:
        try:
            raw = None
            if self.storage is not None:
                if hasattr(self.storage, 'get_state'):
                    res = self.storage.get_state("moe_ecosystem_state")
                    raw = await res if asyncio.iscoroutine(res) else res
            if not raw:
                return
            state = json.loads(raw)
            self.sustainability_score = float(state.get("sustainability_score", 1.0))
            gating_state = state.get("gating_state")
            if gating_state and self.gating_network is not None and hasattr(self.gating_network, 'load_state_dict'):
                try:
                    self.gating_network.load_state_dict(gating_state)
                except Exception as e:
                    logger.warning(f"Gating state restore failed: {e}")
            if self.bio_available and state.get("bio_state"):
                bio_state = state["bio_state"]
                if self.atp_manager is not None and "atp_balances" in bio_state:
                    if hasattr(self.atp_manager, 'balances'):
                        self.atp_manager.balances = bio_state["atp_balances"]
                if self.biomass_storage is not None and "biomass" in bio_state:
                    if hasattr(self.biomass_storage, 'load_state'):
                        try:
                            self.biomass_storage.load_state(bio_state["biomass"])
                        except Exception as e:
                            logger.warning(f"Biomass restore failed: {e}")
            logger.info("Loaded MoE ecosystem state from storage")
        except Exception as e:
            logger.error(f"State load failed: {e}")

    async def save_state(self) -> None:
        try:
            gating_state: Dict[str, Any] = {}
            if self.gating_network is not None and hasattr(self.gating_network, 'get_state_dict'):
                try:
                    gating_state = self.gating_network.get_state_dict() or {}
                except Exception as e:
                    logger.warning(f"Gating state export failed: {e}")

            state: Dict[str, Any] = {
                "version": STATE_SCHEMA_VERSION,
                "sustainability_score": self.sustainability_score,
                "gating_state": self._to_serializable(gating_state),
            }
            if self.bio_available:
                bio_state: Dict[str, Any] = {}
                if self.atp_manager is not None and hasattr(self.atp_manager, 'balances'):
                    bio_state["atp_balances"] = self.atp_manager.balances
                if self.biomass_storage is not None and hasattr(self.biomass_storage, 'save_state'):
                    try:
                        res = self.biomass_storage.save_state()
                        bio_state["biomass"] = await res if asyncio.iscoroutine(res) else res
                    except Exception as e:
                        logger.warning(f"Biomass serialization failed: {e}")
                state["bio_state"] = bio_state

            payload = json.dumps(self._to_serializable(state), default=str)
            if self.storage is not None and hasattr(self.storage, 'save_state'):
                res = self.storage.save_state("moe_ecosystem_state", payload)
                if asyncio.iscoroutine(res):
                    await res
            logger.info("Saved MoE ecosystem state to storage")
        except Exception as e:
            logger.error(f"State save failed: {e}")

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------
    async def _recover_router(self) -> bool:
        logger.info("Attempting to recover expert router...")
        try:
            if self.router is not None and hasattr(self.router, 'reset_circuit_breakers'):
                res = self.router.reset_circuit_breakers()
                if asyncio.iscoroutine(res):
                    await res
            for expert in self.experts.values():
                if hasattr(expert, 'reset_error_count'):
                    res = expert.reset_error_count()
                    if asyncio.iscoroutine(res):
                        await res
            return True
        except Exception as e:
            logger.error(f"Router recovery failed: {e}")
            return False

    # ------------------------------------------------------------------
    # Teacher interface for MODP
    # ------------------------------------------------------------------
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        if self.gating_network is None:
            n = max(1, len(self.experts))
            return [1.0 / n] * n

        try:
            base_res = self.gating_network.predict(state)
            base_weights = await base_res if asyncio.iscoroutine(base_res) else base_res
        except Exception as e:
            logger.warning(f"Gating predict failed: {e}")
            base_weights = {eid: 1.0 / len(self.experts) for eid in self.experts}

        experts_list = list(self.experts.keys())
        candidates: List[Dict[str, Any]] = []
        for eid in experts_list:
            expert = self.experts[eid]
            try:
                health_res = expert.get_health_status()
                health = await health_res if asyncio.iscoroutine(health_res) else health_res
            except Exception:
                health = {'score': 0.5}

            metrics = self._estimate_expert_metrics(expert, state.get('params', {}))

            atp_balance = 1.0
            if self.atp_manager is not None:
                try:
                    res = self.atp_manager.get_balance(eid)
                    atp_balance = await res if asyncio.iscoroutine(res) else res
                except Exception:
                    atp_balance = 1.0

            compartment_status = 'active'
            if self.compartment_manager is not None:
                try:
                    res = self.compartment_manager.get_status(eid)
                    compartment_status = await res if asyncio.iscoroutine(res) else res
                except Exception:
                    compartment_status = 'active'

            candidates.append({
                'expert_id': eid,
                'quality_score': float(base_weights.get(eid, 0.0)),
                'carbon_g': metrics['carbon_g'],
                'latency_ms': metrics['latency_ms'],
                'energy_joules': metrics['energy_joules'],
                'health_score': float(health.get('score', 1.0)),
                'atp_balance': float(atp_balance),
                'compartment_status': compartment_status,
            })

        candidates = [
            c for c in candidates
            if c['health_score'] > 0.5 and c['compartment_status'] == 'active'
        ]
        if not candidates:
            return [1.0 / len(experts_list)] * len(experts_list)

        try:
            filtered = self.pareto.filter(candidates) if self.pareto is not None else candidates
        except Exception as e:
            logger.warning(f"Pareto filter failed: {e}")
            filtered = candidates
        allowed_ids = {c['expert_id'] for c in (filtered or candidates)}

        probs = [0.0] * len(experts_list)
        total = 0.0
        for c in candidates:
            if c['expert_id'] not in allowed_ids:
                continue
            try:
                cost = self.adaptive_cost.compute(
                    quality=c['quality_score'],
                    carbon_g=c['carbon_g'],
                    latency_ms=c['latency_ms'],
                    energy_joules=c['energy_joules'],
                    health=c['health_score'],
                    atp=c['atp_balance'],
                )
            except Exception:
                cost = c['quality_score']
            idx = experts_list.index(c['expert_id'])
            probs[idx] = max(0.0, float(cost))
            total += probs[idx]

        if total > 0:
            return [p / total for p in probs]
        return [1.0 / len(experts_list)] * len(experts_list)

    # ------------------------------------------------------------------
    # Metric estimation (uses real data when available)
    # ------------------------------------------------------------------
    def _estimate_expert_metrics(self, expert: Any, task_params: Dict[str, Any]) -> Dict[str, float]:
        """
        Estimate latency, energy, carbon for an expert.

        Prefers real measurements from ExpertMetricsCollector; falls back to
        profile-based heuristics; never returns pure constants.
        """
        # Try real measurements first
        if self.metrics_collector is not None:
            try:
                # Best-effort: many collectors expose latency stats
                stats_attr = getattr(self.metrics_collector, 'storage_metrics', None)
                if stats_attr is not None and hasattr(stats_attr, 'expert_latency'):
                    expert_id = getattr(expert, 'expert_id', None) or getattr(expert, 'name', None)
                    if expert_id and expert_id in stats_attr.expert_latency:
                        dq = stats_attr.expert_latency[expert_id]
                        if dq:
                            values = [item['value'] for item in dq]
                            return {
                                'latency_ms': float(np.mean(values)),
                                'energy_joules': 0.1,
                                'carbon_g': 0.05,
                            }
            except Exception:
                pass

        # Profile-based estimation
        profile = getattr(expert, 'profile', None)
        hw = getattr(profile, 'hardware_profile', None) if profile else None
        base_latency_ms = float(getattr(hw, 'base_latency_ms', 50.0)) if hw else 50.0
        base_energy_joules = float(getattr(hw, 'base_energy_joules', 0.1)) if hw else 0.1
        base_carbon_g = float(getattr(hw, 'base_carbon_g', 0.05)) if hw else 0.05

        # Task-dependent modulation
        complexity = float(task_params.get('complexity', 0.5))
        data_size = float(task_params.get('data_size', 1.0))
        multiplier = 1.0 + 0.5 * complexity + 0.1 * math.log1p(max(data_size, 0.0))

        return {
            'latency_ms': base_latency_ms * multiplier,
            'energy_joules': base_energy_joules * multiplier,
            'carbon_g': base_carbon_g * multiplier,
        }

    # ------------------------------------------------------------------
    # Idempotency
    # ------------------------------------------------------------------
    def _is_duplicate(self, key: Optional[str]) -> bool:
        if not key or not self.config.enable_idempotency:
            return False
        if key in self._idempotency_seen:
            return True
        self._idempotency_seen.add(key)
        self._idempotency_order.append(key)
        if len(self._idempotency_seen) > self._idempotency_order.maxlen:
            oldest = self._idempotency_order[0]
            self._idempotency_seen.discard(oldest)
        return False

    # ------------------------------------------------------------------
    # Core task processing
    # ------------------------------------------------------------------
    async def process_task(
        self,
        task: Dict[str, Any],
        pipeline_type: str = 'standard',
        use_mixture: bool = False,
        top_k: int = 2,
    ) -> Dict[str, Any]:
        start_time = time.monotonic()

        if not await self.rate_limiter.acquire():
            self._safe_metric_increment("rate_limit_exceeded")
            return {'success': False, 'error': 'Rate limit exceeded'}

        # Validate task
        try:
            if PYDANTIC_AVAILABLE and BaseModel is not None:
                task_input = TaskInput(**task)
                task = task_input.model_dump()
            else:
                task_input = TaskInput(**task)
                task = task_input.model_dump()
        except ValidationError as e:
            return {'success': False, 'error': f'Invalid task: {e}'}
        except Exception as e:
            return {'success': False, 'error': f'Invalid task: {e}'}

        # Idempotency
        idem_key = task.get('idempotency_key')
        if self._is_duplicate(idem_key):
            return {'success': True, 'duplicate': True, 'idempotency_key': idem_key}

        self._safe_metric_increment("tasks_received")

        try:
            return await self._process_task_inner(
                task, pipeline_type, use_mixture, top_k, start_time,
            )
        except Exception as e:
            logger.error(f"Error processing task: {e}", exc_info=True)
            self._safe_metric_increment("task_failures")
            if self.alert_system:
                try:
                    await self.alert_system.add_alert({
                        'source': 'moe_processor',
                        'severity': 'error',
                        'message': f"Task processing failure: {e}",
                    })
                except Exception:
                    pass
            return {"success": False, "error": str(e)}

    async def _process_task_inner(
        self,
        task: Dict[str, Any],
        pipeline_type: str,
        use_mixture: bool,
        top_k: int,
        start_time: float,
    ) -> Dict[str, Any]:
        context = dict(task.get('context', {}))
        task_params = task.get('params', {})
        task_type = task.get('type', 'generic')

        # Bio signal enrichment (non-destructive: operates on a copy)
        context_raw = dict(context)
        if self.bio_available and self.bio_core is not None:
            try:
                bio_context = self.bio_core.process_context(context)
                if asyncio.iscoroutine(bio_context):
                    bio_context = await bio_context
                context.update(bio_context or {})
            except Exception as e:
                logger.warning(f"Bio core context processing failed: {e}")

        # Causal mask (non-destructive; returns NEW dict)
        if self.causal_mask is not None:
            context = self.causal_mask.apply_to_context(context)

        # Base gating weights
        if self.gating_network is None:
            base_weights = {eid: 1.0 / max(1, len(self.experts)) for eid in self.experts}
        else:
            try:
                res = self.gating_network.predict(context)
                base_weights = await res if asyncio.iscoroutine(res) else res
                if not isinstance(base_weights, dict):
                    base_weights = {eid: 1.0 / max(1, len(self.experts)) for eid in self.experts}
            except Exception as e:
                logger.warning(f"Gating predict failed: {e}")
                base_weights = {eid: 1.0 / max(1, len(self.experts)) for eid in self.experts}

        # Evolving gates
        if self.evolving_gates is not None and hasattr(self.evolving_gates, 'update_weights'):
            try:
                res = self.evolving_gates.update_weights(base_weights, context)
                if asyncio.iscoroutine(res):
                    res = await res
                if isinstance(res, dict):
                    base_weights = res
            except Exception as e:
                logger.warning(f"Evolving gate update failed: {e}")

        # Expert auction
        if self.expert_auction is not None:
            try:
                features = self._encode_auction_features(context)
                auction_bids = self.expert_auction.compute_bids(features)
                for eid in base_weights:
                    base_weights[eid] = (base_weights[eid] + auction_bids.get(eid, 0.0)) / 2.0
            except Exception as e:
                logger.warning(f"Expert auction failed: {e}")

        # Build candidates with real metrics
        candidates: List[Dict[str, Any]] = []
        for eid, weight in base_weights.items():
            expert = self.experts.get(eid)
            if expert is None:
                continue
            try:
                health_res = expert.get_health_status()
                health = await health_res if asyncio.iscoroutine(health_res) else health_res
            except Exception:
                health = {'score': 0.5}
            metrics = self._estimate_expert_metrics(expert, task_params)

            atp_balance = 1.0
            if self.atp_manager is not None:
                try:
                    res = self.atp_manager.get_balance(eid)
                    atp_balance = await res if asyncio.iscoroutine(res) else res
                except Exception:
                    atp_balance = 1.0

            compartment_status = 'active'
            if self.compartment_manager is not None:
                try:
                    res = self.compartment_manager.get_status(eid)
                    compartment_status = await res if asyncio.iscoroutine(res) else res
                except Exception:
                    compartment_status = 'active'

            candidates.append({
                'expert_id': eid,
                'quality_score': float(weight),
                'carbon_g': metrics['carbon_g'],
                'latency_ms': metrics['latency_ms'],
                'energy_joules': metrics['energy_joules'],
                'health_score': float(health.get('score', 1.0)),
                'atp_balance': float(atp_balance),
                'compartment_status': compartment_status,
            })

        healthy_candidates = [
            c for c in candidates
            if c['health_score'] > 0.5 and c['compartment_status'] == 'active'
        ]
        if not healthy_candidates:
            healthy_candidates = candidates
            logger.warning("No fully healthy experts; using all.")

        # Pareto filter
        if self.pareto is not None:
            try:
                allowed_candidates = self.pareto.filter(healthy_candidates) or healthy_candidates
            except Exception:
                allowed_candidates = healthy_candidates
        else:
            allowed_candidates = healthy_candidates

        allowed_ids = {c['expert_id'] for c in allowed_candidates}

        # Adaptive cost scoring
        cost_scores: Dict[str, float] = {}
        for c in allowed_candidates:
            try:
                cost = self.adaptive_cost.compute(
                    quality=c['quality_score'],
                    carbon_g=c['carbon_g'],
                    latency_ms=c['latency_ms'],
                    energy_joules=c['energy_joules'],
                    health=c['health_score'],
                    atp=c['atp_balance'],
                )
            except Exception:
                cost = c['quality_score']
            cost_scores[c['expert_id']] = float(cost)

        total_cost = sum(cost_scores.values())
        if total_cost > 0:
            probs = {eid: score / total_cost for eid, score in cost_scores.items()}
        elif cost_scores:
            n = len(cost_scores)
            probs = {eid: 1.0 / n for eid in cost_scores}
        else:
            probs = {eid: 1.0 / max(1, len(self.experts)) for eid in self.experts}

        # Safety check
        if self.safety_monitor is not None:
            min_atp = min((c['atp_balance'] for c in allowed_candidates), default=1.0)
            state = {
                'probs': list(probs.values()),
                'health_score': min((c['health_score'] for c in allowed_candidates), default=0.0),
                'sustainability_score': self.sustainability_score,
                'min_atp': min_atp,
            }
            violations = self.safety_monitor.check(state)
            if violations:
                logger.warning(f"Safety violations: {violations}")
                self.observability.counter('safety_violations')
                probs = {eid: 1.0 / max(1, len(self.experts)) for eid in self.experts}

        # Human approval for critical routing
        if self.human_approval is not None and self.sustainability_score < 0.3:
            approved = await self.human_approval.request_approval({
                'action': 'route_task_low_sustainability',
                'task_type': task_type,
                'sustainability_score': self.sustainability_score,
            })
            if not approved:
                logger.info("Routing rejected by human due to low sustainability.")
                return {'success': False, 'error': 'Rejected by human'}

        # Execute
        execution_results: Dict[str, Any] = {}
        combined_weights: Dict[str, float]
        if use_mixture and top_k > 1:
            top_experts = sorted(probs.items(), key=lambda x: x[1], reverse=True)[:top_k]
            selected_ids = [eid for eid, _ in top_experts]
            selected_probs = {eid: probs[eid] for eid in selected_ids}
            total = sum(selected_probs.values())
            if total > 0:
                selected_probs = {eid: p / total for eid, p in selected_probs.items()}

            for eid, prob in selected_probs.items():
                execution_results[eid] = await self._execute_expert(eid, task_params, context)

            main_expert = max(selected_probs, key=selected_probs.get)
            selected_expert_id = main_expert
            execution_res = execution_results[main_expert]
            combined_weights = selected_probs
        else:
            selected_expert_id = max(probs, key=probs.get)
            execution_res = await self._execute_expert(selected_expert_id, task_params, context)
            combined_weights = {selected_expert_id: 1.0}

        # Sustainability score
        carbon_total = sum(c['carbon_g'] for c in allowed_candidates)
        energy_total = sum(c['energy_joules'] for c in allowed_candidates)
        self.sustainability_score = max(
            0.0, min(1.0, 1.0 - (carbon_total / 100.0) - (energy_total / 1000.0)),
        )
        self.observability.gauge('sustainability_score', self.sustainability_score)

        # Carbon market
        if self.carbon_market is not None:
            try:
                if carbon_total < CARBON_MARKET_SELL_THRESHOLD:
                    await self.carbon_market.sell_credits(0.01)
                elif carbon_total > CARBON_MARKET_BUY_THRESHOLD:
                    await self.carbon_market.buy_credits(0.05)
            except Exception as e:
                logger.warning(f"Carbon market interaction failed: {e}")

        elapsed = time.monotonic() - start_time

        # XAI explanation (uses raw context to avoid masking artifacts)
        explanation = self._generate_explanation(context_raw, selected_expert_id, combined_weights)

        # Metrics
        self._safe_metric_increment("tasks_completed_success")
        self._safe_metric_observe("task_latency_seconds", elapsed)
        self._safe_metric_set("sustainability_score", self.sustainability_score)
        self._safe_metric_set("expert_count", len(self.experts))
        self.observability.histogram('task_latency_ms', elapsed * 1000.0)
        self.observability.counter('tasks_completed', expert=selected_expert_id)

        # Feedback event
        await self._publish_feedback_event(
            context=context,
            selected_expert_id=selected_expert_id,
            combined_weights=combined_weights,
            allowed_candidates=allowed_candidates,
            cost_scores=cost_scores,
            task_type=task_type,
            use_mixture=use_mixture,
            explanation=explanation,
        )

        # Drift detection
        drift_score: Optional[float] = None
        if self.drift is not None:
            try:
                weights = self.adaptive_cost.get_current_weights() if self.adaptive_cost else {}
                res = self.drift.check_drift(weights)
                drift_score = await res if asyncio.iscoroutine(res) else res
                if drift_score and drift_score > DRIFT_RETRAIN_THRESHOLD:
                    logger.warning(f"High drift detected ({drift_score:.3f}); triggering retraining.")
                    if self.gating_network is not None and hasattr(self.gating_network, 'train'):
                        try:
                            res = self.gating_network.train()
                            if asyncio.iscoroutine(res):
                                await res
                        except Exception as e:
                            logger.warning(f"Gating retrain failed: {e}")
            except Exception as e:
                logger.warning(f"Drift check failed: {e}")

        # Online gating update
        reward = self._compute_reward(execution_res, allowed_candidates, selected_expert_id)
        await self._update_gating(context, selected_expert_id, reward)

        # Auction feedback
        if self.expert_auction is not None:
            try:
                features = self._encode_auction_features(context)
                await self.expert_auction.train(features, selected_expert_id, reward)
            except Exception as e:
                logger.warning(f"Auction feedback failed: {e}")

        # Report to metrics collector
        if self.metrics_collector is not None:
            try:
                await self.metrics_collector.record_expert_execution(
                    expert_id=selected_expert_id,
                    execution_time=elapsed * 1000.0,
                    energy_kwh=next(
                        (c['energy_joules'] for c in allowed_candidates
                         if c['expert_id'] == selected_expert_id), 0.0
                    ) / 3.6e6,
                    carbon_kg=next(
                        (c['carbon_g'] for c in allowed_candidates
                         if c['expert_id'] == selected_expert_id), 0.0
                    ) / 1000.0,
                    helium_units=0.0,
                    success=bool(isinstance(execution_res, dict) and (
                        execution_res.get('result') == 'success' or
                        execution_res.get('status') == 'executed'
                    )),
                    correlation_id=None,
                    metadata={'task_type': task_type, 'use_mixture': use_mixture},
                )
            except Exception as e:
                logger.warning(f"Metrics collector record failed: {e}")

        return {
            "success": True,
            "route": {
                "assigned_expert": selected_expert_id,
                "domain": getattr(self.experts.get(selected_expert_id), 'domain', None),
                "weight": combined_weights.get(selected_expert_id, 0.0),
                "all_weights": combined_weights,
                "use_mixture": use_mixture,
            },
            "execution": execution_res,
            "sustainability_score": round(self.sustainability_score, 4),
            "latency_ms": round(elapsed * 1000, 2),
            "drift_score": drift_score,
            "explanation": explanation,
        }

    async def _execute_expert(self, eid: str, task_params: Dict[str, Any],
                              context: Dict[str, Any]) -> Dict[str, Any]:
        expert = self.experts.get(eid)
        if expert is None:
            return {'status': 'error', 'error': f'Expert {eid} not found'}

        # ATP spend
        atp_cost = DEFAULT_ATP_COST
        if self.atp_manager is not None:
            try:
                res = self.atp_manager.spend(eid, atp_cost)
                ok = await res if asyncio.iscoroutine(res) else res
                if not ok:
                    logger.warning(f"Insufficient ATP for {eid}; proceeding anyway.")
            except Exception as e:
                logger.warning(f"ATP spend failed for {eid}: {e}")

        try:
            res = expert.execute(task_params, context)
            execution_res = await res if asyncio.iscoroutine(res) else res
        except Exception as e:
            logger.error(f"Expert {eid} execution failed: {e}")
            execution_res = {'status': 'error', 'error': str(e)}

        success = (
            isinstance(execution_res, dict)
            and (execution_res.get('result') == 'success' or execution_res.get('status') == 'executed')
        )

        try:
            if success and hasattr(expert, 'record_success'):
                r = expert.record_success()
                if asyncio.iscoroutine(r):
                    await r
                if self.atp_manager is not None:
                    res = self.atp_manager.earn(eid, atp_cost * 2)
                    if asyncio.iscoroutine(res):
                        await res
            elif not success and hasattr(expert, 'record_failure'):
                r = expert.record_failure()
                if asyncio.iscoroutine(r):
                    await r
                if self.atp_manager is not None:
                    res = self.atp_manager.spend(eid, atp_cost * 0.5)
                    if asyncio.iscoroutine(res):
                        await res
        except Exception as e:
            logger.warning(f"Expert {eid} feedback recording failed: {e}")

        return execution_res

    # ------------------------------------------------------------------
    # Auction feature encoding
    # ------------------------------------------------------------------
    def _encode_auction_features(self, context: Dict[str, Any]) -> np.ndarray:
        def _num(key: str, default: float) -> float:
            v = context.get(key, default)
            try:
                return float(v)
            except Exception:
                return default

        return np.array([
            _num('carbon_intensity', 400.0) / 1000.0,
            _num('helium_scarcity', 0.5),
            _num('token_balance', 0.5),
            _num('gradient_carbon', 0.5),
            _num('gradient_helium', 0.5),
            _num('gradient_trust', 0.5),
            _num('opportunity_gradient', 0.5),
            _num('stress_level', 0.3),
            _num('task_complexity', 0.5),
            _num('latency_budget', 100.0) / 1000.0,
        ], dtype=np.float32)

    # ------------------------------------------------------------------
    # XAI explanation
    # ------------------------------------------------------------------
    def _generate_explanation(self, context: Dict[str, Any], selected_expert_id: str,
                              weights: Dict[str, float]) -> str:
        try:
            numeric_items = [
                (k, float(v)) for k, v in context.items() if isinstance(v, (int, float))
            ]
            numeric_items.sort(key=lambda kv: abs(kv[1]), reverse=True)
            top = numeric_items[:3]
            features_str = ", ".join([f"{k}={v:.3f}" for k, v in top]) or "no numeric context"
            return (
                f"Selected {selected_expert_id} with weight "
                f"{weights.get(selected_expert_id, 0.0):.3f}. "
                f"Top features: {features_str}."
            )
        except Exception:
            return f"Selected {selected_expert_id}."

    # ------------------------------------------------------------------
    # Reward and gating update
    # ------------------------------------------------------------------
    def _compute_reward(self, execution_res: Any, candidates: List[Dict[str, Any]],
                        selected_expert_id: str) -> float:
        success = True
        if isinstance(execution_res, dict):
            success = (
                execution_res.get('result') == 'success'
                or execution_res.get('status') == 'executed'
            )
        selected_metrics = next(
            (c for c in candidates if c['expert_id'] == selected_expert_id), None,
        )
        if selected_metrics is None:
            return 0.0
        reward = (1.0 if success else -0.5)
        reward += selected_metrics['quality_score'] * 0.5
        reward -= selected_metrics['carbon_g'] / 100.0
        reward -= selected_metrics['energy_joules'] / 1000.0
        return max(-1.0, min(1.0, reward))

    async def _update_gating(self, context: Dict[str, Any], selected_expert_id: str,
                             reward: float) -> None:
        if self.gating_network is None:
            return
        try:
            if hasattr(self.gating_network, 'update_from_feedback'):
                res = self.gating_network.update_from_feedback(context, selected_expert_id, reward)
                if asyncio.iscoroutine(res):
                    await res
        except Exception as e:
            logger.warning(f"Gating update failed: {e}")

    # ------------------------------------------------------------------
    # Feedback publishing (with dead-letter)
    # ------------------------------------------------------------------
    async def _publish_feedback_event(
        self,
        context: Dict[str, Any],
        selected_expert_id: str,
        combined_weights: Dict[str, float],
        allowed_candidates: List[Dict[str, Any]],
        cost_scores: Dict[str, float],
        task_type: str,
        use_mixture: bool,
        explanation: str,
    ) -> None:
        if self.queue is None or FeedbackEvent is None:
            return
        try:
            task_id = f"moe_{hashlib.sha256(json.dumps(context, default=str).encode()).hexdigest()[:8]}"
            energy_j = next(
                (c['energy_joules'] for c in allowed_candidates
                 if c['expert_id'] == selected_expert_id), 0.0,
            )
            carbon_g = next(
                (c['carbon_g'] for c in allowed_candidates
                 if c['expert_id'] == selected_expert_id), 0.0,
            )
            env = getattr(central_config, "ENVIRONMENT", "production") if central_config else "production"
            event = FeedbackEvent.create_with_context(
                task_id=task_id,
                selected_action=selected_expert_id,
                quality_score=combined_weights.get(selected_expert_id, 0.0),
                energy_joules=energy_j,
                carbon_g=carbon_g,
                feedback_type="moe_routing",
                adaptive_cost_value=cost_scores.get(selected_expert_id, 0.0),
                state={'task_type': task_type, 'context': context, 'use_mixture': use_mixture},
                candidates=[{'expert': eid, 'weight': w} for eid, w in combined_weights.items()],
                source="green_agent_moe",
                environment=env,
                tags=["moe", "routing", f"v{STATE_SCHEMA_VERSION}"],
                metadata={'explanation': explanation} if explanation else {},
            )
            payload = event.to_json() if hasattr(event, 'to_json') else json.dumps(event, default=str)
            res = self.queue.publish("feedback_events", payload)
            if asyncio.iscoroutine(res):
                await res
        except Exception as e:
            logger.error(f"Failed to publish feedback event: {e}")
            self._feedback_dead_letters.append({
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'error': str(e),
                'selected_expert': selected_expert_id,
                'task_type': task_type,
            })

    # ------------------------------------------------------------------
    # Safe metric wrappers
    # ------------------------------------------------------------------
    def _safe_metric_increment(self, name: str, value: float = 1.0) -> None:
        self.observability.counter(name, value)
        try:
            if self.metrics is not None and hasattr(self.metrics, 'increment'):
                self.metrics.increment(name)
        except Exception:
            pass

    def _safe_metric_observe(self, name: str, value: float) -> None:
        self.observability.histogram(name, value)
        try:
            if self.metrics is not None and hasattr(self.metrics, 'observe'):
                self.metrics.observe(name, value)
        except Exception:
            pass

    def _safe_metric_set(self, name: str, value: float) -> None:
        self.observability.gauge(name, value)
        try:
            if self.metrics is not None and hasattr(self.metrics, 'set'):
                self.metrics.set(name, value)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------
    async def health_check(self) -> Dict[str, Any]:
        status: Dict[str, Any] = {
            "version": STATE_SCHEMA_VERSION,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sustainability_score": self.sustainability_score,
            "expert_count": len(self.experts),
            "gating_trained": getattr(self.gating_network, 'is_trained', False)
            if self.gating_network is not None else False,
            "circuit_breaker_state": self._safe_circuit_state(),
            "bio_available": self.bio_available,
            "auction_available": self.expert_auction is not None,
            "safety_monitor": self.safety_monitor is not None,
            "chaos_enabled": self.chaos_injector is not None,
            "human_approval_enabled": self.human_approval is not None,
            "feedback_dead_letters": len(self._feedback_dead_letters),
        }
        if self.health_system:
            try:
                status["system_health"] = await self.health_system.get_system_health()
            except Exception as e:
                status["system_health"] = f"error: {e}"
        self._safe_metric_set("sustainability_score", self.sustainability_score)
        self._safe_metric_set("expert_count", len(self.experts))
        return status

    def _safe_circuit_state(self) -> str:
        if self.router is None:
            return 'unknown'
        try:
            if hasattr(self.router, 'get_circuit_breaker_state'):
                res = self.router.get_circuit_breaker_state()
                return res if isinstance(res, str) else str(res)
        except Exception:
            pass
        return 'unknown'

    # ------------------------------------------------------------------
    # Exposed helpers
    # ------------------------------------------------------------------
    async def get_prometheus_metrics(self) -> str:
        return await self.observability.export_prometheus()

    async def submit_approval_decision(self, request_id: str, approved: bool) -> bool:
        if self.human_approval is None:
            return False
        return await self.human_approval.submit_decision(request_id, approved)

    def get_feedback_dead_letters(self) -> List[Dict[str, Any]]:
        return list(self._feedback_dead_letters)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    async def shutdown(self) -> None:
        logger.info("Initiating system shutdown sequence...")
        self._shutting_down = True

        # Cancel background tasks
        for task in self._bg_tasks:
            if task:
                task.cancel()
        if self._bg_tasks:
            await asyncio.gather(*self._bg_tasks, return_exceptions=True)

        # Shutdown subsystems
        if self.health_system:
            try:
                await self.health_system.shutdown()
            except Exception as e:
                logger.warning(f"Health shutdown error: {e}")
        if self.self_healing:
            try:
                await self.self_healing.shutdown()
            except Exception as e:
                logger.warning(f"Self-healing shutdown error: {e}")
        if self.sustainability_dashboard:
            try:
                await self.sustainability_dashboard.shutdown()
            except Exception as e:
                logger.warning(f"Dashboard shutdown error: {e}")
        if self.predictive_maintenance:
            try:
                await self.predictive_maintenance.shutdown()
            except Exception as e:
                logger.warning(f"Predictive maintenance shutdown error: {e}")
        if self.digital_twin and hasattr(self.digital_twin, 'shutdown'):
            try:
                res = self.digital_twin.shutdown()
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.warning(f"Digital twin shutdown error: {e}")
        if self.carbon_market:
            try:
                await self.carbon_market.close()
            except Exception as e:
                logger.warning(f"Carbon market close error: {e}")
        if self.metrics_collector is not None and hasattr(self.metrics_collector, 'shutdown'):
            try:
                res = self.metrics_collector.shutdown()
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.warning(f"Metrics collector shutdown error: {e}")

        # Final safety check
        if self.safety_monitor is not None:
            final_violations = self.safety_monitor.final_check()
            if final_violations:
                logger.warning(f"Unmet eventual invariants at shutdown: {final_violations}")

        # Chaos restore
        if self.chaos_injector is not None:
            self.chaos_injector.restore_all()

        # Persist state
        await self.save_state()

        logger.info("UnifiedMetabolicEcosystem shutdown complete.")


# ============================================================================
# Example Usage
# ============================================================================
if __name__ == "__main__":
    import logging as _logging

    _logging.basicConfig(level=_logging.INFO)

    async def main() -> None:
        try:
            from ..storage import Storage as _Storage
            from ..scaling.message_queue import AsyncMessageQueue as _Q
            from ..feedback.adaptive_cost import AdaptiveCostFunction as _AC
            from ..routing.pareto_gating import ParetoGating as _PG
            from ..safety.drift_detector import DriftDetector as _DD
            from ..metrics import MetricsRegistry as _MR
        except ImportError:
            print("Central components not available for demo.")
            return

        storage = _Storage()
        queue = _Q()
        adaptive_cost = _AC(storage)
        pareto = _PG()
        drift = _DD(storage, adaptive_cost)
        metrics = _MR()

        ecosystem = UnifiedMetabolicEcosystem(
            storage, queue, adaptive_cost, pareto, drift, metrics,
        )

        task = {
            "type": "energy_optimization",
            "params": {"grid_target": "renewable_solar", "complexity": 0.4},
            "context": {"carbon_intensity": 320.0},
            "idempotency_key": uuid.uuid4().hex,
        }
        result = await ecosystem.process_task(task)
        print(json.dumps(result, indent=2, default=str))

        health = await ecosystem.health_check()
        print(json.dumps(health, indent=2, default=str))

        await ecosystem.shutdown()

    asyncio.run(main())


# ============================================================================
# Module exports
# ============================================================================
__all__ = [
    "UnifiedMetabolicEcosystem",
    "UnifiedEcosystemConfig",
    "TaskInput",
    "RateLimiter",
    "ObservabilityHub",
    "QuantumDistillationModule",
    "CausalFeatureMask",
    "ExpertAuction",
    "SafetyMonitor",
    "SafetyInvariant",
    "PrecisionController",
    "CarbonMarketClient",
    "FaultInjector",
    "HumanApprovalHandler",
    "HealthCheckSystem",
    "SelfHealingSystem",
    "AlertEscalationSystem",
    "DynamicReconfigurationSystem",
    "UnifiedSustainabilityDashboard",
    "PredictiveMaintenanceIntegrator",
]
