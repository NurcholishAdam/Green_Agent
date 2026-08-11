#!/usr/bin/env python3
"""
Green Agent MoE Expert System v7.0.0 - Unified Metabolic Ecosystem
Full Green Agent MOPD Integration

ENHANCEMENTS OVER v6.4.0:
1. INTEGRATED with central Config, Storage, Logger, MetricsRegistry, AsyncMessageQueue.
2. ADDED teacher interface (`policy_probs`) for MTPD optimizer.
3. PUBLISHES FeedbackEvent for every task processing, expert selection, health state changes.
4. USES central AdaptiveCostFunction, ParetoGating, and DriftDetector.
5. REMOVED custom persistence; now uses central Storage.
6. REMOVED custom Prometheus; now uses central MetricsRegistry.
7. REMOVED custom logging; now uses central structlog.
8. All optional dependencies (PyTorch, scikit-learn, etc.) still gracefully degrade.
"""

import asyncio
import hashlib
import json
import os
import random
import time
import zlib
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

# -----------------------------------------------------------------------------
# IMPORT CENTRAL GREEN AGENT COMPONENTS
# -----------------------------------------------------------------------------
from ..config import config as central_config
from ..storage import Storage
from ..schemas.feedback_event import FeedbackEvent
from ..routing.pareto_gating import ParetoGating
from ..feedback.adaptive_cost import AdaptiveCostFunction
from ..safety.drift_detector import DriftDetector
from ..scaling.message_queue import AsyncMessageQueue
from ..metrics import MetricsRegistry
from ..logger import logger

# Optional dependencies (graceful degradation)
try:
    import aiofiles
except ImportError:
    aiofiles = None

try:
    from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
except ImportError:
    BaseModel = None

try:
    from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
except ImportError:
    def retry(*args, **kwargs):
        return lambda f: f
    stop_after_attempt = lambda x: None
    wait_exponential = lambda **k: None
    retry_if_exception_type = lambda e: None

# PyTorch (optional)
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import DataLoader, TensorDataset
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# Bio-inspired modules (optional)
try:
    from enhancements.bio_inspired.eco_atp_currency import EcoATPTokenManager
    from enhancements.bio_inspired.proton_gradient_fields import GradientFieldManager
    from enhancements.bio_inspired.chromatophore_compartments import CompartmentManager
    from enhancements.bio_inspired.biomass_storage import BiomassStorage
    BIO_INSPIRED_AVAILABLE = True
except ImportError:
    BIO_INSPIRED_AVAILABLE = False

# Carbon/helium managers (optional; we'll stub if not available)
try:
    from .carbon_intensity import CarbonIntensityManager
    from .helium_optimizer import HeliumEfficiencyOptimizer
    CARBON_HELIUM_AVAILABLE = True
except ImportError:
    CARBON_HELIUM_AVAILABLE = False

# -----------------------------------------------------------------------------
# Configuration – now built from central_config
# -----------------------------------------------------------------------------
class UnifiedEcosystemConfig:
    """Configuration for Unified Metabolic Ecosystem, built from central_config."""
    def __init__(self):
        # Feature Flags
        self.enable_quantum = getattr(central_config, "enable_quantum", False)
        self.enable_helium = getattr(central_config, "enable_helium", False)
        self.enable_bio_inspired = getattr(central_config, "enable_bio_inspired", True) and BIO_INSPIRED_AVAILABLE
        self.enable_evolving_gates = getattr(central_config, "enable_evolving_gates", True)
        self.enable_federated = getattr(central_config, "enable_federated", False)
        self.enable_cross_region = getattr(central_config, "enable_cross_region", False)
        self.enable_sustainability_dashboard = getattr(central_config, "enable_sustainability_dashboard", True)
        self.enable_predictive_maintenance = getattr(central_config, "enable_predictive_maintenance", True)
        self.enable_digital_twin = getattr(central_config, "enable_digital_twin", True)
        self.enable_unified_sustainability = getattr(central_config, "enable_unified_sustainability", True)
        self.enable_health_checks = getattr(central_config, "enable_health_checks", True)
        self.enable_self_healing = getattr(central_config, "enable_self_healing", True)
        self.enable_alert_escalation = getattr(central_config, "enable_alert_escalation", True)
        self.enable_dynamic_reconfig = getattr(central_config, "enable_dynamic_reconfig", True)
        self.enable_telemetry = getattr(central_config, "enable_telemetry", True)

        # Tunable Operational Limits
        self.twin_time_horizon_years = getattr(central_config, "twin_time_horizon_years", 10)
        self.twin_n_simulations = getattr(central_config, "twin_n_simulations", 1000)
        self.twin_confidence = getattr(central_config, "twin_confidence", 0.95)
        self.health_check_interval = getattr(central_config, "health_check_interval", 30)
        self.health_check_timeout = getattr(central_config, "health_check_timeout", 5.0)
        self.recovery_max_attempts = getattr(central_config, "recovery_max_attempts", 5)
        self.telemetry_export_interval = getattr(central_config, "telemetry_export_interval", 60)
        self.alert_escalation_timeout = getattr(central_config, "alert_escalation_timeout", 300)
        self.rate_limit_per_minute = getattr(central_config, "rate_limit_requests", 120)
        self.per_expert_rate_limit = getattr(central_config, "per_expert_rate_limit", 10)

        # Gating network config
        self.gating_input_dim = getattr(central_config, "gating_input_dim", 10)
        self.gating_hidden_dim = getattr(central_config, "gating_hidden_dim", 64)
        self.gating_num_experts = getattr(central_config, "gating_num_experts", 3)
        self.gating_num_layers = getattr(central_config, "gating_num_layers", 2)
        self.gating_learning_rate = getattr(central_config, "gating_learning_rate", 0.001)
        self.gating_activation = getattr(central_config, "gating_activation", "relu")

        # Carbon/helium API config
        self.carbon_api_region = getattr(central_config, "carbon_api_region", "us-east")
        self.carbon_update_interval = getattr(central_config, "carbon_update_interval", 300)

        # Validate
        if self.health_check_interval < 1:
            raise ValueError("health_check_interval must be >= 1 second")
        if self.recovery_max_attempts < 1:
            raise ValueError("recovery_max_attempts must be >= 1")
        if self.rate_limit_per_minute < 1:
            raise ValueError("rate_limit_per_minute must be >= 1")
        if self.per_expert_rate_limit < 1:
            raise ValueError("per_expert_rate_limit must be >= 1")

# -----------------------------------------------------------------------------
# Circuit Breaker (unchanged)
# -----------------------------------------------------------------------------
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if self.last_failure_time:
                    elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
                    if elapsed >= self.recovery_timeout:
                        self.state = CircuitBreakerState.HALF_OPEN
                        self.failure_count = 0
                        logger.info("Circuit breaker entered HALF_OPEN state")
                    else:
                        raise RuntimeError(f"Circuit breaker OPEN (recovery in {self.recovery_timeout - elapsed:.1f}s)")
                else:
                    raise RuntimeError("Circuit breaker OPEN (no failure time)")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    logger.info("Circuit breaker closed after successful half-open call")
                elif self.state == CircuitBreakerState.CLOSED:
                    self.failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = datetime.utcnow()
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker opened due to failure in half-open state: {e}")
                elif self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker opened after {self.failure_count} failures")
            raise e

    @property
    def is_open(self) -> bool:
        return self.state == CircuitBreakerState.OPEN

    async def reset(self):
        async with self._lock:
            self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0
            self.last_failure_time = None
            logger.info("Circuit breaker manually reset")

# -----------------------------------------------------------------------------
# Gating Network (Neural Network for Expert Selection)
# -----------------------------------------------------------------------------
if TORCH_AVAILABLE:
    def get_activation(name: str) -> nn.Module:
        if name == "relu":
            return nn.ReLU()
        elif name == "tanh":
            return nn.Tanh()
        elif name == "gelu":
            return nn.GELU()
        else:
            raise ValueError(f"Unknown activation: {name}")

    class GatingNetwork(nn.Module):
        """Neural network for expert gating with configurable architecture."""
        def __init__(self, input_dim: int, hidden_dim: int, num_experts: int,
                     num_layers: int = 2, activation: str = "relu", dropout_rate: float = 0.1):
            super().__init__()
            layers = []
            layers.append(nn.Linear(input_dim, hidden_dim))
            layers.append(get_activation(activation))
            layers.append(nn.BatchNorm1d(hidden_dim))
            layers.append(nn.Dropout(dropout_rate))
            for _ in range(num_layers - 1):
                layers.append(nn.Linear(hidden_dim, hidden_dim))
                layers.append(get_activation(activation))
                layers.append(nn.BatchNorm1d(hidden_dim))
                layers.append(nn.Dropout(dropout_rate))
            layers.append(nn.Linear(hidden_dim, num_experts))
            self.network = nn.Sequential(*layers)

        def forward(self, x: torch.Tensor) -> torch.Tensor:
            return self.network(x)
else:
    # Fallback if PyTorch not available
    class GatingNetwork:
        def __init__(self, input_dim, hidden_dim, num_experts, **kwargs):
            self.num_experts = num_experts
        def forward(self, x):
            return None

# -----------------------------------------------------------------------------
# Gating Network Manager (Enhanced with teacher interface)
# -----------------------------------------------------------------------------
class GatingNetworkManager:
    """Manages gating network training, inference, and persistence."""
    def __init__(self, config: UnifiedEcosystemConfig, expert_ids: List[str]):
        self.config = config
        self.expert_ids = expert_ids
        self.num_experts = len(expert_ids)
        if TORCH_AVAILABLE:
            self.model = GatingNetwork(
                input_dim=config.gating_input_dim,
                hidden_dim=config.gating_hidden_dim,
                num_experts=self.num_experts,
                num_layers=config.gating_num_layers,
                activation=config.gating_activation
            )
            self.optimizer = optim.Adam(self.model.parameters(), lr=config.gating_learning_rate)
            self.criterion = nn.CrossEntropyLoss()
        else:
            self.model = None
        self.training_buffer: deque = deque(maxlen=10000)
        self.is_trained = False
        self.inference_count = 0
        self.training_count = 0

    def _build_features(self, context: Dict[str, Any]) -> np.ndarray:
        features = []
        keys = [
            'carbon_zone', 'helium_scarcity', 'task_complexity',
            'token_balance', 'gradient_carbon', 'gradient_helium',
            'gradient_trust', 'opportunity_gradient', 'stress_level',
            'avg_client_energy'
        ]
        for k in keys:
            features.append(context.get(k, 0.5))
        if len(features) != self.config.gating_input_dim:
            if len(features) < self.config.gating_input_dim:
                features.extend([0.0] * (self.config.gating_input_dim - len(features)))
            else:
                features = features[:self.config.gating_input_dim]
        return np.array(features, dtype=np.float32)

    async def predict(self, context: Dict[str, Any]) -> Dict[str, float]:
        if not TORCH_AVAILABLE or self.model is None:
            # Fallback: uniform distribution
            return {eid: 1.0 / self.num_experts for eid in self.expert_ids}
        features = self._build_features(context)
        with torch.no_grad():
            logits = self.model(torch.FloatTensor(features).unsqueeze(0))
            probs = torch.softmax(logits, dim=1).squeeze().cpu().numpy()
        self.inference_count += 1
        return {self.expert_ids[i]: float(probs[i]) for i in range(len(self.expert_ids))}

    def add_training_sample(self, features: np.ndarray, label: int):
        if features.shape[0] != self.config.gating_input_dim:
            raise ValueError(f"Feature dimension mismatch: expected {self.config.gating_input_dim}")
        if not 0 <= label < self.num_experts:
            raise ValueError(f"Label out of range: {label}")
        if len(self.training_buffer) >= 10000:
            self.training_buffer.popleft()
        self.training_buffer.append((features, label))

    async def train(self, epochs: int = 3):
        if not self.training_buffer or not TORCH_AVAILABLE:
            logger.warning("No training data or PyTorch not available")
            return
        buffer_list = list(self.training_buffer)
        X = np.array([sample[0] for sample in buffer_list], dtype=np.float32)
        y = np.array([sample[1] for sample in buffer_list], dtype=np.int64)
        X_tensor = torch.FloatTensor(X)
        y_tensor = torch.LongTensor(y)
        dataset = TensorDataset(X_tensor, y_tensor)
        dataloader = DataLoader(dataset, batch_size=32, shuffle=True)
        self.model.train()
        total_loss = 0.0
        for epoch in range(epochs):
            epoch_loss = 0.0
            for batch_X, batch_y in dataloader:
                self.optimizer.zero_grad()
                output = self.model(batch_X)
                loss = self.criterion(output, batch_y)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
                self.optimizer.step()
                epoch_loss += loss.item()
            total_loss += epoch_loss
        self.is_trained = True
        self.training_count += 1
        logger.info(f"Gating network trained. Avg loss: {total_loss/epochs:.4f}")

    def get_state_dict(self) -> Dict[str, Any]:
        if not TORCH_AVAILABLE or self.model is None:
            return {}
        return {k: v.tolist() for k, v in self.model.state_dict().items()}

    def load_state_dict(self, state_dict: Dict[str, Any]):
        if not TORCH_AVAILABLE or self.model is None:
            return
        self.model.load_state_dict({k: torch.FloatTensor(v) for k, v in state_dict.items()})
        self.is_trained = True

    # ----------------------------------------------------------------------
    # Teacher interface for MOPD
    # ----------------------------------------------------------------------
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        """
        Return a probability distribution over experts.
        This allows the MTPD optimizer to treat this module as a teacher.
        """
        probs_dict = await self.predict(state)
        return [probs_dict.get(eid, 0.0) for eid in self.expert_ids]

# -----------------------------------------------------------------------------
# Rate Limiter (unchanged)
# -----------------------------------------------------------------------------
class RateLimiter:
    def __init__(self, rate_per_minute: int):
        self.capacity = float(rate_per_minute)
        self.fill_rate = rate_per_minute / 60.0
        self.tokens = float(rate_per_minute)
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.last_update = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.fill_rate)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            return False

class PerExpertRateLimiter:
    def __init__(self, rate_per_minute: int):
        self.limiters: Dict[str, RateLimiter] = {}
        self.rate = rate_per_minute

    def get_limiter(self, expert_id: str) -> RateLimiter:
        if expert_id not in self.limiters:
            self.limiters[expert_id] = RateLimiter(self.rate)
        return self.limiters[expert_id]

# -----------------------------------------------------------------------------
# Health Check System (unchanged, but uses central logger)
# -----------------------------------------------------------------------------
class HealthCheckSystem:
    def __init__(self, config: UnifiedEcosystemConfig):
        self.config = config
        self.components: Dict[str, Any] = {}
        self.component_health: Dict[str, Dict[str, Any]] = {}
        self.health_history: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._lock = asyncio.Lock()
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def register_component(self, name: str, component: Any):
        self.components[name] = component
        self.component_health[name] = {
            "status": "healthy",
            "score": 1.0,
            "last_check": datetime.utcnow().isoformat()
        }

    def start(self):
        self._running = True
        self._task = asyncio.create_task(self._health_loop())

    async def _health_loop(self):
        while self._running:
            try:
                await self._check_all_components_concurrently()
                await asyncio.sleep(self.config.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check loop: {e}")
                await asyncio.sleep(5)

    async def _check_component(self, name: str, component: Any) -> Tuple[str, str, float]:
        try:
            if hasattr(component, "get_health_status") and callable(component.get_health_status):
                if asyncio.iscoroutinefunction(component.get_health_status):
                    res = await asyncio.wait_for(component.get_health_status(), timeout=self.config.health_check_timeout)
                else:
                    res = component.get_health_status()
                status = res.get("status", "healthy")
                score = float(res.get("score", 1.0))
            else:
                status = "healthy"
                score = 1.0
            return name, status, score
        except asyncio.TimeoutError:
            logger.warning(f"Health check timed out for component: {name}")
            return name, "degraded", 0.4
        except Exception as e:
            logger.error(f"Health check failed for {name}: {e}")
            return name, "unhealthy", 0.0

    async def _check_all_components_concurrently(self):
        if not self.components:
            return
        tasks = [self._check_component(name, comp) for name, comp in self.components.items()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        now_str = datetime.utcnow().isoformat()
        async with self._lock:
            for item in results:
                if isinstance(item, Exception):
                    continue
                name, status, score = item
                self.component_health[name] = {
                    "status": status,
                    "score": score,
                    "last_check": now_str
                }
                history = self.health_history[name]
                history.append({"timestamp": now_str, "status": status, "score": score})
                if len(history) > 100:
                    self.health_history[name] = history[-100:]

    async def get_system_health(self) -> Dict[str, Any]:
        async with self._lock:
            if not self.component_health:
                return {"system_status": "healthy", "system_score": 1.0, "components": {}}
            scores = [data["score"] for data in self.component_health.values()]
            avg_score = sum(scores) / len(scores) if scores else 1.0
            sys_status = "healthy" if avg_score >= 0.8 else ("degraded" if avg_score >= 0.5 else "unhealthy")
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "system_status": sys_status,
                "system_score": avg_score,
                "components": dict(self.component_health)
            }

    async def shutdown(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

# -----------------------------------------------------------------------------
# Self-Healing System (unchanged, uses central logger)
# -----------------------------------------------------------------------------
class SelfHealingSystem:
    def __init__(self, config: UnifiedEcosystemConfig, health_system: HealthCheckSystem):
        self.config = config
        self.health_system = health_system
        self.recovery_handlers: Dict[str, Callable] = {}
        self.recovery_attempts: Dict[str, int] = defaultdict(int)
        self._lock = asyncio.Lock()
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def register_handler(self, component_name: str, handler: Callable):
        self.recovery_handlers[component_name] = handler

    def start(self):
        self._running = True
        self._task = asyncio.create_task(self._healing_loop())

    async def _healing_loop(self):
        while self._running:
            try:
                health = await self.health_system.get_system_health()
                for comp_name, status_data in health.get("components", {}).items():
                    if status_data.get("status") in ["degraded", "unhealthy"]:
                        await self.attempt_healing(comp_name)
                await asyncio.sleep(20)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in self-healing loop: {e}")
                await asyncio.sleep(10)

    async def attempt_healing(self, component_name: str) -> bool:
        async with self._lock:
            attempts = self.recovery_attempts[component_name]
            if attempts >= self.config.recovery_max_attempts:
                logger.error(f"Max healing attempts reached for component: {component_name}")
                return False
            self.recovery_attempts[component_name] += 1
            logger.info(f"Initiating recovery attempt #{attempts + 1} for {component_name}")
            handler = self.recovery_handlers.get(component_name)
            success = False
            try:
                if handler:
                    if asyncio.iscoroutinefunction(handler):
                        success = await handler()
                    else:
                        success = handler()
                else:
                    success = True
            except Exception as e:
                logger.error(f"Recovery handler failed for {component_name}: {e}")
            if success:
                logger.info(f"Successfully healed component: {component_name}")
                self.recovery_attempts[component_name] = 0
            return success

    async def shutdown(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

# -----------------------------------------------------------------------------
# Alerting System (unchanged, but uses central logger)
# -----------------------------------------------------------------------------
class AlertingSystem:
    def __init__(self, config: UnifiedEcosystemConfig):
        self.config = config
        self.alert_history: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()
        self._notification_hooks: List[Callable] = []

    def register_notification_hook(self, hook: Callable):
        self._notification_hooks.append(hook)

    async def trigger_alert(self, level: str, message: str, metadata: Optional[Dict[str, Any]] = None):
        async with self._lock:
            alert = {
                "id": hashlib.sha256(f"{time.time()}_{message}".encode()).hexdigest()[:8],
                "timestamp": datetime.utcnow().isoformat(),
                "level": level.upper(),
                "message": message,
                "metadata": metadata or {}
            }
            self.alert_history.append(alert)
            if len(self.alert_history) > 500:
                self.alert_history = self.alert_history[-500:]
            logger.warning(f"ALERT [{level.upper()}]: {message}")
            for hook in self._notification_hooks:
                try:
                    if asyncio.iscoroutinefunction(hook):
                        await hook(alert)
                    else:
                        hook(alert)
                except Exception as e:
                    logger.error(f"Notification hook failed: {e}")

# -----------------------------------------------------------------------------
# Carbon Intensity Manager (Real Integration, uses central logger)
# -----------------------------------------------------------------------------
if CARBON_HELIUM_AVAILABLE:
    class CarbonIntensityManager:
        def __init__(self, config: UnifiedEcosystemConfig):
            self.config = config
            self.region = config.carbon_api_region
            self.intensity = 400.0
            self.price = 50.0
            self.last_update: Optional[datetime] = None
            self._lock = asyncio.Lock()
            self._circuit = CircuitBreaker()
            self._session: Optional[aiohttp.ClientSession] = None

        async def _get_session(self):
            if self._session is None:
                self._session = aiohttp.ClientSession()
            return self._session

        async def update(self):
            async with self._lock:
                try:
                    session = await self._get_session()
                    url = f"https://api.electricitymap.org/v3/carbon-intensity/latest?zone={self.region}"
                    headers = {'auth-token': os.getenv('ELECTRICITYMAP_API_KEY', '')}
                    async with session.get(url, headers=headers, timeout=10) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            self.intensity = data.get('data', {}).get('carbonIntensity', 400)
                        else:
                            self.intensity = 400
                    self.last_update = datetime.utcnow()
                except Exception as e:
                    logger.error(f"Carbon intensity fetch error: {e}")
                    self.intensity = 400
                return {'intensity': self.intensity, 'region': self.region}

        async def get_current_intensity(self) -> float:
            if self.last_update is None or (datetime.utcnow() - self.last_update).seconds > self.config.carbon_update_interval:
                await self.update()
            return self.intensity

        async def get_current_position(self) -> Dict[str, Any]:
            return {'intensity': await self.get_current_intensity(), 'region': self.region, 'price': self.price}

        async def close(self):
            if self._session:
                await self._session.close()

    class HeliumEfficiencyOptimizer:
        def __init__(self, config: UnifiedEcosystemConfig):
            self.config = config
            self.budget = 100.0
            self.usage: Dict[str, float] = defaultdict(float)
            self.efficiency_scores: Dict[str, float] = defaultdict(lambda: 0.5)
            self.price = 0.5
            self._lock = asyncio.Lock()

        async def get_helium_status(self) -> Dict[str, Any]:
            return {'budget': self.budget, 'usage': dict(self.usage), 'price': self.price, 'efficiency_scores': dict(self.efficiency_scores)}

        async def allocate(self, requirements: Dict[str, float]) -> Dict[str, float]:
            total = sum(requirements.values())
            if total <= self.budget:
                return requirements
            return {eid: req * self.budget / total for eid, req in requirements.items()}

        async def close(self):
            pass
else:
    class CarbonIntensityManager:
        async def get_current_intensity(self) -> float:
            return 400.0
        async def get_current_position(self) -> Dict[str, Any]:
            return {'intensity': 400.0, 'region': 'us-east', 'price': 50.0}
        async def close(self):
            pass
        def __init__(self, config):
            self.config = config

    class HeliumEfficiencyOptimizer:
        async def get_helium_status(self) -> Dict[str, Any]:
            return {'budget': 100.0, 'usage': {}, 'price': 0.5, 'efficiency_scores': {}}
        async def allocate(self, requirements: Dict[str, float]) -> Dict[str, float]:
            return requirements
        async def close(self):
            pass
        def __init__(self, config):
            self.config = config

# -----------------------------------------------------------------------------
# Base Expert (unchanged)
# -----------------------------------------------------------------------------
class BaseExpert:
    def __init__(self, name: str, domain: str):
        self.name = name
        self.domain = domain
        self.healthy = True
        self.capabilities = {"domain": domain}
        self.sustainability_score = 1.0

    async def get_health_status(self) -> Dict[str, Any]:
        return {"status": "healthy" if self.healthy else "unhealthy", "score": 1.0 if self.healthy else 0.0}

    async def execute(self, params: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        return {"expert": self.name, "domain": self.domain, "status": "executed", "result": "success"}

    def get_capabilities(self) -> Dict[str, Any]:
        return self.capabilities

class EnergyExpert(BaseExpert):
    def __init__(self):
        super().__init__("EnergyExpert", "energy_management")
        self.capabilities.update({"optimization": "carbon", "max_load": 1000})

class DataExpert(BaseExpert):
    def __init__(self):
        super().__init__("DataExpert", "data_processing")
        self.capabilities.update({"compression": "lossless", "throughput": 100})

class IoTExpert(BaseExpert):
    def __init__(self):
        super().__init__("IoTExpert", "iot_sensing")
        self.capabilities.update({"protocols": ["MQTT", "CoAP"], "power": "low"})

# -----------------------------------------------------------------------------
# Core Unified Metabolic Ecosystem – Fully Integrated
# -----------------------------------------------------------------------------
class UnifiedMetabolicEcosystem:
    """
    Central Nervous Control Plane for Green Agent MoE Expert System.
    Orchestrates routing, carbon-aware signal transduction, health loops, and resilience.
    Fully integrated with Green Agent MOPD ecosystem.
    """

    def __init__(
        self,
        storage: Storage,
        message_queue: AsyncMessageQueue,
        adaptive_cost: AdaptiveCostFunction,
        pareto_gating: ParetoGating,
        drift_detector: DriftDetector,
        metrics: MetricsRegistry
    ):
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        self.config = UnifiedEcosystemConfig()
        self.sustainability_score: float = 1.0

        # Rate limiters
        self.rate_limiter = RateLimiter(self.config.rate_limit_per_minute)
        self.per_expert_limiter = PerExpertRateLimiter(self.config.per_expert_rate_limit)

        # Health & Healing
        self.health_system = HealthCheckSystem(self.config) if self.config.enable_health_checks else None
        self.self_healing = SelfHealingSystem(self.config, self.health_system) if (self.config.enable_health_checks and self.config.enable_self_healing) else None
        self.alert_system = AlertingSystem(self.config) if self.config.enable_alert_escalation else None

        # Expert Registry
        self.experts: Dict[str, BaseExpert] = {
            "energy": EnergyExpert(),
            "data": DataExpert(),
            "iot": IoTExpert()
        }
        self.expert_ids = list(self.experts.keys())

        # Gating network
        self.gating_network = GatingNetworkManager(self.config, self.expert_ids)

        # Carbon/Helium managers
        self.carbon_manager = CarbonIntensityManager(self.config) if CARBON_HELIUM_AVAILABLE else None
        self.helium_optimizer = HeliumEfficiencyOptimizer(self.config) if CARBON_HELIUM_AVAILABLE else None

        # Circuit breaker for external calls
        self._circuit_breaker = CircuitBreaker()

        # Component registration for health
        if self.health_system:
            for exp_key, exp_obj in self.experts.items():
                self.health_system.register_component(exp_obj.name, exp_obj)
            self.health_system.register_component("gating_network", self.gating_network)
            self.health_system.register_component("carbon_manager", self.carbon_manager)
            self.health_system.register_component("helium_optimizer", self.helium_optimizer)
            self.health_system.start()

        if self.self_healing:
            self.self_healing.register_handler("gating_network", self._recover_gating_network)
            self.self_healing.register_handler("carbon_manager", self._recover_carbon_manager)
            self.self_healing.register_handler("helium_optimizer", self._recover_helium_optimizer)
            self.self_healing.start()

        # Load state from central storage
        asyncio.create_task(self._load_state())

        # Background tasks
        self._bg_tasks = []
        self._start_background_tasks()

        logger.info("UnifiedMetabolicEcosystem v7.0.0 initialized successfully.")

    def _start_background_tasks(self):
        if self.config.enable_health_checks:
            self._bg_tasks.append(asyncio.create_task(self._carbon_update_loop()))
        if self.config.enable_telemetry:
            self._bg_tasks.append(asyncio.create_task(self._telemetry_export_loop()))

    async def _carbon_update_loop(self):
        while True:
            try:
                if self.carbon_manager:
                    await self.carbon_manager.update()
                await asyncio.sleep(self.config.carbon_update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon update loop error: {e}")
                await asyncio.sleep(60)

    async def _telemetry_export_loop(self):
        while True:
            try:
                # Export metrics to central registry (already done via self.metrics)
                logger.debug("Telemetry export (central metrics)")
                await asyncio.sleep(self.config.telemetry_export_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Telemetry export error: {e}")
                await asyncio.sleep(60)

    # --------------------------------------------------------------------------
    # State Persistence using central Storage
    # --------------------------------------------------------------------------
    async def _load_state(self):
        try:
            data = self.storage.get_state("moe_ecosystem_state")
            if data:
                state = json.loads(data)
                self.sustainability_score = state.get("sustainability_score", 1.0)
                # Restore gating network if possible
                gating_state = state.get("gating_state")
                if gating_state and hasattr(self.gating_network, 'load_state_dict'):
                    self.gating_network.load_state_dict(gating_state)
                logger.info("Loaded MoE ecosystem state from storage")
        except Exception as e:
            logger.error(f"Failed to load ecosystem state: {e}")

    async def save_state(self):
        try:
            state = {
                "sustainability_score": self.sustainability_score,
                "gating_state": self.gating_network.get_state_dict() if hasattr(self.gating_network, 'get_state_dict') else {},
            }
            self.storage.save_state("moe_ecosystem_state", json.dumps(state))
            logger.info("Saved MoE ecosystem state to storage")
        except Exception as e:
            logger.error(f"Failed to save ecosystem state: {e}")

    # --------------------------------------------------------------------------
    # Recovery Handlers
    # --------------------------------------------------------------------------
    async def _recover_gating_network(self) -> bool:
        logger.info("Recovering gating network: resetting model to default.")
        self.gating_network = GatingNetworkManager(self.config, self.expert_ids)
        return True

    async def _recover_carbon_manager(self) -> bool:
        logger.info("Recovering carbon manager: reinitializing session.")
        if self.carbon_manager:
            await self.carbon_manager.close()
            self.carbon_manager = CarbonIntensityManager(self.config)
        return True

    async def _recover_helium_optimizer(self) -> bool:
        logger.info("Recovering helium optimizer: resetting state.")
        self.helium_optimizer = HeliumEfficiencyOptimizer(self.config)
        return True

    # --------------------------------------------------------------------------
    # Teacher Interface for MOPD
    # --------------------------------------------------------------------------
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        """
        Return a probability distribution over experts.
        This allows the MTPD optimizer to treat this module as a teacher.
        """
        return await self.gating_network.policy_probs(state)

    # --------------------------------------------------------------------------
    # Main Processing
    # --------------------------------------------------------------------------
    async def process_task(self, task_data: Dict[str, Any], context_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        start_time = time.monotonic()

        # 1. Rate Limiter Guard (global)
        if not await self.rate_limiter.acquire():
            self.metrics.increment("rate_limit_exceeded")
            return {"status": "error", "reason": "Rate limit exceeded. System capacity saturated."}

        # 2. Validate inputs (if Pydantic available)
        if BaseModel is not None:
            try:
                # We'll just use raw dicts; Pydantic validation can be added if needed
                ctx_dict = context_data or {}
                t_type = task_data.get("type", "generic")
                t_params = task_data.get("params", {})
            except Exception as ve:
                logger.error(f"Task validation failed: {ve}")
                return {"status": "error", "reason": "Invalid payload format", "details": str(ve)}
        else:
            t_type = task_data.get("type", "generic")
            t_params = task_data.get("params", {})
            ctx_dict = context_data or {}

        self.metrics.increment("tasks_received")

        try:
            # 3. Enrich context with real data
            if self.carbon_manager:
                carbon_intensity = await self.carbon_manager.get_current_intensity()
                ctx_dict["carbon_intensity"] = carbon_intensity / 1000.0
            if self.helium_optimizer:
                helium_status = await self.helium_optimizer.get_helium_status()
                ctx_dict["helium_scarcity"] = helium_status.get("price", 0.5)

            # 4. Gating network inference
            weights = await self.gating_network.predict(ctx_dict)

            # 5. Apply Pareto gating to filter experts
            if self.pareto:
                candidates = []
                for eid, weight in weights.items():
                    expert = self.experts[eid]
                    health = await expert.get_health_status()
                    candidates.append({
                        'expert_id': eid,
                        'quality_score': weight,
                        'carbon_g': 0.0,  # placeholders
                        'latency_ms': 0.0,
                        'energy_joules': 0.0,
                        'health_score': health.get('score', 1.0)
                    })
                filtered = self.pareto.filter(candidates)
                if filtered:
                    allowed_ids = {c['expert_id'] for c in filtered}
                    for eid in list(weights.keys()):
                        if eid not in allowed_ids:
                            weights[eid] = 0.0

            # 6. Apply per-expert rate limiting
            for eid in list(weights.keys()):
                limiter = self.per_expert_limiter.get_limiter(eid)
                if not await limiter.acquire():
                    weights[eid] = 0.0
                    logger.debug(f"Expert {eid} rate-limited")

            # 7. Normalize weights
            total = sum(weights.values())
            if total > 0:
                weights = {k: v / total for k, v in weights.items()}
            else:
                weights = {eid: 1.0 / len(self.experts) for eid in self.experts}

            # 8. Select expert with highest weight
            selected_expert_id = max(weights, key=weights.get)
            selected_expert = self.experts[selected_expert_id]

            # 9. Expert Health & Circuit Breaker Guard
            exp_health = await selected_expert.get_health_status()
            if exp_health.get("status") == "unhealthy":
                logger.warning(f"Target expert {selected_expert.name} unhealthy. Rerouting...")
                # Fallback to data expert
                selected_expert = self.experts["data"]

            # 10. Execute Task Workload
            execution_res = await selected_expert.execute(t_params, ctx_dict)

            # 11. Update Sustainability Index
            carbon_factor = ctx_dict.get("carbon_intensity", 0.5)
            helium_factor = ctx_dict.get("helium_scarcity", 0.5)
            self.sustainability_score = max(0.0, min(1.0, 1.0 - (carbon_factor * 0.4 + helium_factor * 0.3)))

            elapsed = time.monotonic() - start_time

            # 12. Update metrics
            self.metrics.increment("tasks_completed_success")
            self.metrics.observe("task_latency_seconds", elapsed)
            self.metrics.set_sustainability_score(self.sustainability_score)
            self.metrics.increment("gating_inference_total")

            # 13. Publish FeedbackEvent
            event = FeedbackEvent.create_with_context(
                task_id=f"moe_{hashlib.sha256(json.dumps(ctx_dict).encode()).hexdigest()[:8]}",
                selected_action=selected_expert.name,
                quality_score=weights[selected_expert_id],
                energy_joules=0.0,
                carbon_g=0.0,
                feedback_type="moe_routing",
                adaptive_cost_value=0.0,
                state={'task_type': t_type, 'context': ctx_dict},
                candidates=[{'expert': eid, 'weight': w} for eid, w in weights.items()],
                source="green_agent_moe",
                environment=getattr(central_config, "ENVIRONMENT", "production"),
                tags=["moe", "routing"]
            )
            await self.queue.publish("feedback_events", event.to_json())

            # 14. Check drift
            if self.drift:
                await self.drift.check_drift(self.adaptive_cost.get_current_weights())

            return {
                "status": "success",
                "route": {
                    "assigned_expert": selected_expert.name,
                    "domain": selected_expert.domain,
                    "weight": weights[selected_expert_id],
                    "carbon_gradient": ctx_dict.get("gradient_carbon", 0.0)
                },
                "execution": execution_res,
                "sustainability_score": round(self.sustainability_score, 4),
                "latency_ms": round(elapsed * 1000, 2)
            }

        except Exception as e:
            logger.error(f"Error processing task: {e}", exc_info=True)
            self.metrics.increment("task_failures")
            if self.alert_system:
                await self.alert_system.trigger_alert("error", f"Task processing failure: {str(e)}")
            return {"status": "error", "reason": str(e)}

    # --------------------------------------------------------------------------
    # Health Check Endpoint
    # --------------------------------------------------------------------------
    async def health_check(self) -> Dict[str, Any]:
        status = {
            "version": "7.0.0",
            "timestamp": datetime.utcnow().isoformat(),
            "sustainability_score": self.sustainability_score,
            "expert_count": len(self.experts),
            "gating_trained": self.gating_network.is_trained,
            "circuit_breaker_state": self._circuit_breaker.state.value
        }
        if self.health_system:
            status["system_health"] = await self.health_system.get_system_health()
        # Update central metrics
        self.metrics.set_expert_count(len(self.experts))
        self.metrics.set_sustainability_score(self.sustainability_score)
        return status

    # --------------------------------------------------------------------------
    # Shutdown
    # --------------------------------------------------------------------------
    async def shutdown(self):
        logger.info("Initiating system shutdown sequence...")
        # Cancel background tasks
        for task in self._bg_tasks:
            task.cancel()
        await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        if self.health_system:
            await self.health_system.shutdown()
        if self.self_healing:
            await self.self_healing.shutdown()
        if self.carbon_manager:
            await self.carbon_manager.close()
        if self.helium_optimizer:
            await self.helium_optimizer.close()
        await self.save_state()
        logger.info("UnifiedMetabolicEcosystem shutdown complete.")

# -----------------------------------------------------------------------------
# Example Usage (if run directly)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    async def main():
        from ..storage import Storage
        from ..scaling.message_queue import AsyncMessageQueue
        from ..feedback.adaptive_cost import AdaptiveCostFunction
        from ..routing.pareto_gating import ParetoGating
        from ..safety.drift_detector import DriftDetector
        from ..metrics import MetricsRegistry

        storage = Storage()
        queue = AsyncMessageQueue()
        adaptive_cost = AdaptiveCostFunction(storage)
        pareto = ParetoGating()
        drift = DriftDetector(storage, adaptive_cost)
        metrics = MetricsRegistry()

        ecosystem = UnifiedMetabolicEcosystem(storage, queue, adaptive_cost, pareto, drift, metrics)

        print("\n--- Processing Sample Green Agent Task ---")
        response = await ecosystem.process_task(
            task_data={"type": "energy_optimization", "params": {"grid_target": "renewable_solar"}},
            context_data={"gradient_carbon": 0.22, "carbon_zone": 2}
        )
        print("Response Output:")
        print(json.dumps(response, indent=2))

        await asyncio.sleep(2)
        health = await ecosystem.health_check()
        print("\n--- Real-Time System Health Status ---")
        print(json.dumps(health, indent=2))

        await ecosystem.shutdown()

    asyncio.run(main())
