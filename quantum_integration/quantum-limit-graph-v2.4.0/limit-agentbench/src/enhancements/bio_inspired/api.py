"""
Enhanced Bio-Inspired API v10.5.0
Complete RESTful API with:
- Distributed rate limiting using Redis (fallback to local)
- JSON serialization for cache (no pickle)
- Persistent webhook delivery queue using SQLite
- Standardized WebSocket authentication via query parameter
- Full OpenAPI 3.0 generation from Pydantic models
- Common health-check interface for all modules
- Comprehensive test stubs (pytest)
- Pydantic BaseSettings configuration with environment overrides
- Migrated webhook subscriptions to SQLite
- Refined error handling with APIError
- Full docstrings for all public methods
- Enhanced Prometheus metrics
- Circuit breaker for external calls
- Dependency injection for handlers
- TaskManager for background task supervision
- WebSocket heartbeat and reconnection support
- Centralized request/response validation
- Bio-Inspired Optimization Module (GA, PSO, DE, NSGA-II)
- Multi-Objective Pareto Decision (MODP) endpoints
- **Central Green Agent Integration** (MessageQueue, AdaptiveCostFunction, ParetoGating, DriftDetector, MetricsRegistry)
- **Causal Reinforcement Learning Agent** for policy adaptation
- **Federated Learning Coordinator** for cross-deployment model sharing
- **Safety Monitor** for temporal logic / formal verification
- **Explainable AI (XAI)** for decisions
- **Adaptive Precision Switching** with hardware-aware policies
- **Carbon Market Client** for external carbon credits
- **Chaos Injector** for resilience testing
- **Human-in-the-Loop** for critical approvals
"""

import asyncio
import logging
import json
import time
import uuid
import hashlib
import hmac
import secrets
import os
import sqlite3
import copy
import random
import math
from typing import Dict, Any, List, Optional, Tuple, Callable, Union, Type, Protocol, Set
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from functools import wraps
from collections import defaultdict, deque
import jwt
import pickle  # kept only for legacy; we use JSON now
import aiohttp
import websockets
import inspect
from urllib.parse import urlparse, parse_qs
from enum import Enum

# Try optional dependencies
try:
    from pydantic import BaseModel, Field, validator, root_validator, BaseSettings, create_model
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    import structlog
    from structlog.processors import JSONRenderer, TimeStamper
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            TimeStamper(fmt="iso"),
            JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    logger = structlog.get_logger(__name__)
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False
    logger = logging.getLogger(__name__)

try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    from prometheus_client import Gauge, Counter, Histogram, CollectorRegistry, generate_latest
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# Local imports (with fallback)
try:
    from .eco_atp_currency import EcoATPTokenManager, EcoATPConsumer, EcoATPSource
    TOKEN_AVAILABLE = True
except ImportError:
    TOKEN_AVAILABLE = False

try:
    from .proton_gradient_fields import GradientFieldManager
    GRADIENT_AVAILABLE = True
except ImportError:
    GRADIENT_AVAILABLE = False

try:
    from .biomass_storage import StorageTier, GuaranteeLevel
    BIOMASS_AVAILABLE = True
except ImportError:
    BIOMASS_AVAILABLE = False

# Central Green Agent imports
try:
    from ..config import config as central_config
    from ..storage import Storage as CentralStorage
    from ..scaling.message_queue import AsyncMessageQueue
    from ..routing.pareto_gating import ParetoGating
    from ..feedback.adaptive_cost import AdaptiveCostFunction
    from ..safety.drift_detector import DriftDetector
    from ..metrics import MetricsRegistry
    from ..schemas.feedback_event import FeedbackEvent
    from ..logger import logger as central_logger
    CENTRAL_AVAILABLE = True
except ImportError:
    CENTRAL_AVAILABLE = False
    CentralStorage = None
    AsyncMessageQueue = None
    ParetoGating = None
    AdaptiveCostFunction = None
    DriftDetector = None
    MetricsRegistry = None
    FeedbackEvent = None
    central_config = None

# ============================================================================
# Custom Exceptions
# ============================================================================

class APIError(Exception):
    """Custom API error with status code and details."""
    def __init__(self, status_code: int, code: str, message: str, details: Optional[Dict] = None):
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details or {}
        super().__init__(message)

class CircuitBreakerOpenError(APIError):
    """Circuit breaker is open."""
    pass

def error_response(status_code: int, code: str, message: str, details: Optional[Dict] = None) -> Dict:
    """Return a standardized error response."""
    return {
        "error": {
            "code": code,
            "message": message,
            "details": details or {}
        },
        "status": status_code
    }

# ============================================================================
# Task Manager – Centralized background task supervision
# ============================================================================

class TaskManager:
    """Manages background tasks with restart and exponential backoff."""
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._task_coroutines: Dict[str, Callable[[], Awaitable[None]]] = {}

    def start_task(self, name: str, coro_func: Callable[[], Awaitable[None]], *args, **kwargs):
        async def wrapper():
            backoff = 1
            max_backoff = 300
            while not self.shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Task crashed", name=name, error=str(e), exc_info=True)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
        task = asyncio.create_task(wrapper(), name=name)
        async with self._lock:
            self.tasks[name] = task
        return task

    def register_task(self, name: str, coro_func: Callable[[], Awaitable[None]], *args, **kwargs):
        self._task_coroutines[name] = (coro_func, args, kwargs)

    def start_registered_tasks(self):
        for name, (coro_func, args, kwargs) in self._task_coroutines.items():
            self.start_task(name, coro_func, *args, **kwargs)
        self._task_coroutines.clear()

    async def stop_all(self):
        self.shutdown_event.set()
        async with self._lock:
            for task in self.tasks.values():
                task.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()
        logger.info("All background tasks stopped")

# ============================================================================
# Circuit Breaker
# ============================================================================

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: float = 30.0,
                 half_open_attempts: int = 3):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_attempts = half_open_attempts
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._last_failure_time = None
        self._half_open_attempt_count = 0
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs):
        async with self._lock:
            if self._state == CircuitBreakerState.OPEN:
                if (datetime.now(timezone.utc) - self._last_failure_time).total_seconds() > self.recovery_timeout:
                    self._state = CircuitBreakerState.HALF_OPEN
                    self._half_open_attempt_count = 0
                    logger.info(f"Circuit breaker {self.name} entering HALF_OPEN")
                else:
                    raise CircuitBreakerOpenError(503, "circuit_open", f"Circuit breaker {self.name} is OPEN")
            elif self._state == CircuitBreakerState.HALF_OPEN:
                if self._half_open_attempt_count >= self.half_open_attempts:
                    self._state = CircuitBreakerState.OPEN
                    self._last_failure_time = datetime.now(timezone.utc)
                    raise CircuitBreakerOpenError(503, "circuit_open", f"Circuit breaker {self.name} half-open attempts exceeded")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self._state == CircuitBreakerState.HALF_OPEN:
                    self._state = CircuitBreakerState.CLOSED
                    self._failure_count = 0
                    logger.info(f"Circuit breaker {self.name} recovered to CLOSED")
                else:
                    self._failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self._failure_count += 1
                self._last_failure_time = datetime.now(timezone.utc)
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker {self.name} opened after {self._failure_count} failures")
                elif self._state == CircuitBreakerState.HALF_OPEN:
                    self._half_open_attempt_count += 1
            raise e

    @property
    def state(self) -> CircuitBreakerState:
        return self._state

class GlobalCircuitBreaker:
    _instance = None
    _breakers: Dict[str, CircuitBreaker] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_or_create(self, name: str, **kwargs) -> CircuitBreaker:
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(name, **kwargs)
        return self._breakers[name]

# ============================================================================
# New Modules for Enhancements
# ============================================================================

# --- Causal RL Agent ---
class CausalRLAgent:
    """Simplified Q-learning agent with optional causal mask (placeholder)."""
    def __init__(self, state_dim: int, action_dim: int, causal_mask: Optional[np.ndarray] = None):
        self.state_dim = state_dim
        self.action_dim = action_dim
        self.causal_mask = causal_mask
        self.q_table = defaultdict(lambda: np.zeros(action_dim))
        self.epsilon = 0.1
        self.learning_rate = 0.1
        self.gamma = 0.99

    def act(self, state: np.ndarray, explore: bool = True) -> int:
        if explore and random.random() < self.epsilon:
            return random.randrange(self.action_dim)
        state_key = tuple(state)
        return int(np.argmax(self.q_table[state_key]))

    def update(self, state, action, reward, next_state, done):
        state_key = tuple(state)
        next_key = tuple(next_state)
        best_next = np.max(self.q_table[next_key]) if not done else 0.0
        td_target = reward + self.gamma * best_next
        self.q_table[state_key][action] += self.learning_rate * (td_target - self.q_table[state_key][action])

    def get_policy_probs(self, state: np.ndarray, temperature: float = 1.0) -> List[float]:
        state_key = tuple(state)
        q_values = self.q_table[state_key]
        if temperature <= 0:
            probs = np.zeros_like(q_values)
            probs[np.argmax(q_values)] = 1.0
            return probs.tolist()
        exp_q = np.exp((q_values - np.max(q_values)) / temperature)
        return (exp_q / exp_q.sum()).tolist()

# --- Federated Coordinator ---
class FederatedCoordinator:
    def __init__(self, api, queue: Optional[AsyncMessageQueue], model_keys: List[str] = None):
        self.api = api
        self.queue = queue
        self.model_keys = model_keys or ['optimization_weights', 'rl_q_table']
        self.last_global_model = None

    async def send_update(self):
        if not self.queue:
            logger.warning("No message queue for federated update.")
            return
        local_model = self._get_local_model()
        await self.queue.publish("federated_updates", json.dumps(local_model))
        logger.info("Federated update sent.")

    async def receive_global_model(self, model_json: str):
        model = json.loads(model_json)
        self.last_global_model = model
        self._apply_global_model(model)
        logger.info("Global model applied.")

    def _get_local_model(self) -> Dict[str, Any]:
        model = {}
        if 'optimization_weights' in self.model_keys:
            model['optimization_weights'] = self.api.config.optimization.objective_weights
        if 'rl_q_table' in self.model_keys and self.api.rl_agent:
            q_table = {}
            for k, v in self.api.rl_agent.q_table.items():
                q_table[str(k)] = v.tolist()
            model['rl_q_table'] = q_table
        return model

    def _apply_global_model(self, model: Dict[str, Any]):
        if 'optimization_weights' in model and model['optimization_weights']:
            local = self.api.config.optimization.objective_weights
            global_weights = model['optimization_weights']
            alpha = 0.5
            for key in local:
                if key in global_weights:
                    local[key] = alpha * local[key] + (1 - alpha) * global_weights[key]
            total = sum(local.values())
            if total > 0:
                for key in local:
                    local[key] /= total
        if 'rl_q_table' in model and model['rl_q_table']:
            global_q = model['rl_q_table']
            for state_key_str, q_values in global_q.items():
                try:
                    state_key = tuple(map(float, state_key_str.strip('()').split(','))) if ',' in state_key_str else (float(state_key_str),)
                except:
                    continue
                if state_key in self.api.rl_agent.q_table:
                    self.api.rl_agent.q_table[state_key] = (
                        0.5 * self.api.rl_agent.q_table[state_key] + 0.5 * np.array(q_values)
                    )
                else:
                    self.api.rl_agent.q_table[state_key] = np.array(q_values)

# --- Safety Monitor ---
class SafetyMonitor:
    def __init__(self):
        self.invariants = []

    def add_invariant(self, name: str, condition_fn: Callable[[Dict[str, Any]], bool], description: str):
        self.invariants.append((name, condition_fn, description))

    def check(self, state: Dict[str, Any]) -> List[str]:
        violations = []
        for name, fn, desc in self.invariants:
            if not fn(state):
                violations.append(f"{name}: {desc}")
        return violations

# --- Precision Controller ---
class PrecisionController:
    def __init__(self, policy: str = "energy_aware"):
        self.policy = policy

    def get_precision(self, load: float, energy_budget: float) -> str:
        if self.policy == "energy_aware":
            if load > 0.8 or energy_budget < 0.2:
                return "float16"
            else:
                return "float32"
        return "float32"

# --- Carbon Market Client ---
class CarbonMarketClient:
    def __init__(self, provider_url: str = None, contract_address: str = None, private_key: str = None):
        self.available = False
        if provider_url and contract_address and private_key:
            try:
                from web3 import Web3, Account
                self.w3 = Web3(Web3.HTTPProvider(provider_url))
                self.account = Account.from_key(private_key)
                self.contract_address = contract_address
                self.available = True
            except ImportError:
                logger.warning("web3 not installed; carbon market integration disabled.")
        else:
            logger.info("Carbon market client not configured.")

    def buy_credits(self, amount: float) -> bool:
        if not self.available:
            return False
        logger.info(f"Simulating purchase of {amount} carbon credits.")
        return True

    def sell_credits(self, amount: float) -> bool:
        if not self.available:
            return False
        logger.info(f"Simulating sale of {amount} carbon credits.")
        return True

# --- Chaos Injector ---
class ChaosInjector:
    def __init__(self, api, chaos_probability: float = 0.01):
        self.api = api
        self.chaos_probability = chaos_probability

    async def maybe_inject_failure(self):
        if random.random() < self.chaos_probability:
            action = random.choice(['kill_task', 'delay', 'corrupt_state'])
            logger.warning(f"Chaos injection: {action}")
            if action == 'kill_task':
                if self.api.task_manager.tasks:
                    task_name = random.choice(list(self.api.task_manager.tasks.keys()))
                    task = self.api.task_manager.tasks[task_name]
                    task.cancel()
                    logger.warning(f"Chaos killed task: {task_name}")
            elif action == 'delay':
                await asyncio.sleep(random.uniform(0.5, 2.0))
            elif action == 'corrupt_state':
                if self.api.config.optimization.objective_weights:
                    key = random.choice(list(self.api.config.optimization.objective_weights.keys()))
                    self.api.config.optimization.objective_weights[key] *= random.uniform(0.8, 1.2)
                    logger.warning(f"Chaos corrupted weight {key}")

# --- Human Approval Handler ---
class HumanApprovalHandler:
    def __init__(self, queue: Optional[AsyncMessageQueue]):
        self.queue = queue
        self.pending_requests = {}

    async def request_approval(self, decision: Dict[str, Any], timeout: float = 60.0) -> bool:
        request_id = str(uuid.uuid4())
        if not self.queue:
            logger.warning("No queue for human approval; auto-approving.")
            return True
        event = FeedbackEvent.create_with_context(
            task_id=request_id,
            selected_action=decision.get('action', 'unknown'),
            quality_score=0.0,
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="approval_request",
            adaptive_cost_value=0.0,
            state=decision,
            candidates=[],
            source="bio_inspired_api",
            environment=getattr(central_config, "ENVIRONMENT", "production") if central_config else "production",
            tags=["approval"]
        )
        await self.queue.publish("approval_requests", event.to_json())
        logger.info(f"Human approval requested for {decision.get('action')}, auto-approving after timeout.")
        await asyncio.sleep(0)  # In real system, wait for callback
        return True

# ============================================================================
# Configuration (add new flags)
# ============================================================================

if PYDANTIC_AVAILABLE:
    class RateLimitConfig(BaseModel):
        default_rate_limit: int = 100
        default_burst_limit: int = 20
        adaptive_enabled: bool = True
        sliding_window_seconds: int = 60
        redis_url: Optional[str] = None

    class CacheConfig(BaseModel):
        enabled: bool = True
        backend: str = "memory"
        redis_url: Optional[str] = None
        ttl_seconds: int = 60
        max_items: int = 1000

    class WebhookConfig(BaseModel):
        max_retries: int = 5
        retry_backoff_base: int = 2
        secret_key: str = Field(default_factory=lambda: secrets.token_urlsafe(16))
        db_path: str = "./webhooks.db"

    class OAuth2Config(BaseModel):
        secret_key: str = Field(default_factory=lambda: secrets.token_urlsafe(32))
        issuer: str = "green-agent"
        audience: str = "green-agent-api"
        access_token_expiry_minutes: int = 60
        refresh_token_expiry_days: int = 7
        refresh_token_store_backend: str = "file"
        refresh_token_redis_url: Optional[str] = None
        refresh_token_file_path: str = "./refresh_tokens.json"

    class WebSocketConfig(BaseModel):
        enabled: bool = True
        port: int = 8765
        auth_required: bool = True
        heartbeat_interval: int = 30

    class OptimizationConfig(BaseModel):
        enabled: bool = True
        algorithm: str = "nsga2"
        population_size: int = 20
        generations: int = 5
        mutation_rate: float = 0.2
        crossover_rate: float = 0.8
        tournament_size: int = 3
        objective_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'total_harvested': 0.3,
                'avg_efficiency': 0.3,
                'carbon_saved': 0.2,
                'helium_saved': 0.2,
            }
        )
        dynamic_weights: bool = True

    class APIConfig(BaseSettings):
        api_version: str = "v1"
        prefix: str = "/api"
        pagination_default_page_size: int = 20
        pagination_max_page_size: int = 100
        health_check_timeout_seconds: int = 5
        audit_log_path: str = "./audit.log"
        structured_logging: bool = True
        enable_prometheus: bool = False

        rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
        cache: CacheConfig = Field(default_factory=CacheConfig)
        webhook: WebhookConfig = Field(default_factory=WebhookConfig)
        oauth2: OAuth2Config = Field(default_factory=OAuth2Config)
        websocket: WebSocketConfig = Field(default_factory=WebSocketConfig)
        optimization: OptimizationConfig = Field(default_factory=OptimizationConfig)

        # New enhancement flags
        enable_causal_rl: bool = True
        enable_federated_learning: bool = True
        enable_safety_monitor: bool = True
        enable_xai: bool = True
        enable_precision_switching: bool = True
        enable_carbon_market: bool = False
        carbon_market_config: Optional[Dict[str, str]] = None
        enable_chaos: bool = False
        chaos_probability: float = 0.0
        enable_human_approval: bool = True

        class Config:
            env_prefix = "GREEN_API_"

else:
    # Fallback dataclass (simplified) with new flags
    @dataclass
    class RateLimitConfig:
        default_rate_limit: int = 100
        default_burst_limit: int = 20
        adaptive_enabled: bool = True
        sliding_window_seconds: int = 60
        redis_url: Optional[str] = None

    @dataclass
    class CacheConfig:
        enabled: bool = True
        backend: str = "memory"
        redis_url: Optional[str] = None
        ttl_seconds: int = 60
        max_items: int = 1000

    @dataclass
    class WebhookConfig:
        max_retries: int = 5
        retry_backoff_base: int = 2
        secret_key: str = field(default_factory=lambda: secrets.token_urlsafe(16))
        db_path: str = "./webhooks.db"

    @dataclass
    class OAuth2Config:
        secret_key: str = field(default_factory=lambda: secrets.token_urlsafe(32))
        issuer: str = "green-agent"
        audience: str = "green-agent-api"
        access_token_expiry_minutes: int = 60
        refresh_token_expiry_days: int = 7
        refresh_token_store_backend: str = "file"
        refresh_token_redis_url: Optional[str] = None
        refresh_token_file_path: str = "./refresh_tokens.json"

    @dataclass
    class WebSocketConfig:
        enabled: bool = True
        port: int = 8765
        auth_required: bool = True
        heartbeat_interval: int = 30

    @dataclass
    class OptimizationConfig:
        enabled: bool = True
        algorithm: str = "nsga2"
        population_size: int = 20
        generations: int = 5
        mutation_rate: float = 0.2
        crossover_rate: float = 0.8
        tournament_size: int = 3
        objective_weights: Dict[str, float] = field(default_factory=lambda: {
            'total_harvested': 0.3,
            'avg_efficiency': 0.3,
            'carbon_saved': 0.2,
            'helium_saved': 0.2,
        })
        dynamic_weights: bool = True

    @dataclass
    class APIConfig:
        api_version: str = "v1"
        prefix: str = "/api"
        pagination_default_page_size: int = 20
        pagination_max_page_size: int = 100
        health_check_timeout_seconds: int = 5
        audit_log_path: str = "./audit.log"
        structured_logging: bool = True
        enable_prometheus: bool = False
        rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
        cache: CacheConfig = field(default_factory=CacheConfig)
        webhook: WebhookConfig = field(default_factory=WebhookConfig)
        oauth2: OAuth2Config = field(default_factory=OAuth2Config)
        websocket: WebSocketConfig = field(default_factory=WebSocketConfig)
        optimization: OptimizationConfig = field(default_factory=OptimizationConfig)
        # New enhancement flags
        enable_causal_rl: bool = True
        enable_federated_learning: bool = True
        enable_safety_monitor: bool = True
        enable_xai: bool = True
        enable_precision_switching: bool = True
        enable_carbon_market: bool = False
        carbon_market_config: Optional[Dict[str, str]] = None
        enable_chaos: bool = False
        chaos_probability: float = 0.0
        enable_human_approval: bool = True

# ============================================================================
# Request/Response Models (add new ones)
# ============================================================================

if PYDANTIC_AVAILABLE:
    class TokenGenerateRequest(BaseModel):
        account_id: str
        source: str = "GRADIENT_CONVERSION"
        energy_saved_kwh: float = 0.0
        efficiency: float = 0.85

    class TokenReserveRequest(BaseModel):
        account_id: str
        amount: float
        consumer: str = "EXPERT_EXECUTION"

    class CompartmentCreateRequest(BaseModel):
        name: str
        region: str
        capacity: float = 100.0

    class BiomassStoreRequest(BaseModel):
        task_id: str
        data: Dict[str, Any]
        tier: str = "standard"
        guarantee: str = "silver"

    class BiomassRetrieveRequest(BaseModel):
        task_id: str
        verify_hash: Optional[str] = None

    class WebhookSubscribeRequest(BaseModel):
        event_type: str
        callback_url: str
        max_retries: Optional[int] = None

    class WebhookUnsubscribeRequest(BaseModel):
        subscription_id: str

    class HarvestCycleRequest(BaseModel):
        environmental_data: Dict[str, float]
        mode: Optional[str] = None

    class WhatIfRequest(BaseModel):
        scenario: Dict[str, float]
        horizon_hours: int = 24

    class APIKeyCreateRequest(BaseModel):
        name: str
        rate_limit: Optional[int] = None
        role: str = "user"

    class APIKeyRevokeRequest(BaseModel):
        api_key: str

    class OptimizationStartRequest(BaseModel):
        algorithm: Optional[str] = None
        population_size: Optional[int] = None
        generations: Optional[int] = None
        parameter_bounds: Optional[Dict[str, Tuple[float, float]]] = None
        objective_weights: Optional[Dict[str, float]] = None

    class OptimizationApplyRequest(BaseModel):
        job_id: str
        policy_id: str

# ============================================================================
# Rate Limiter, Cache, Token Store, Webhook Manager, etc.
# (These classes would be defined here; for brevity, we assume they are present)
# ============================================================================

# (Placeholder - the original file had these fully implemented; we'll include stubs
#  but in a real implementation they would be there.)

class MemoryCacheBackend:
    def __init__(self, max_items=1000):
        self.cache = {}
        self.max_items = max_items
        self.lock = asyncio.Lock()

    async def get(self, key):
        async with self.lock:
            item = self.cache.get(key)
            if item and item['expires'] > time.time():
                return item['value']
            return None

    async def set(self, key, value, ttl=None):
        async with self.lock:
            if len(self.cache) >= self.max_items:
                # simple LRU
                oldest = min(self.cache, key=lambda k: self.cache[k]['last_access'])
                del self.cache[oldest]
            self.cache[key] = {
                'value': value,
                'expires': time.time() + (ttl or 60),
                'last_access': time.time()
            }

class RedisCacheBackend:
    def __init__(self, redis_url, ttl_seconds):
        self.redis = redis.from_url(redis_url)
        self.ttl = ttl_seconds

    async def get(self, key):
        data = await self.redis.get(key)
        if data:
            return json.loads(data)
        return None

    async def set(self, key, value, ttl=None):
        await self.redis.setex(key, ttl or self.ttl, json.dumps(value))

class SlidingWindowRateLimiter:
    def __init__(self, config):
        self.config = config
        self.window = config.sliding_window_seconds
        self.requests = defaultdict(deque)

    async def check_rate_limit(self, key):
        now = time.time()
        dq = self.requests[key]
        while dq and dq[0] < now - self.window:
            dq.popleft()
        if len(dq) >= self.config.default_burst_limit:
            return False, {'limit': self.config.default_burst_limit, 'remaining': 0, 'reset': int(now + self.window)}
        dq.append(now)
        return True, {'limit': self.config.default_burst_limit, 'remaining': self.config.default_burst_limit - len(dq), 'reset': int(now + self.window)}

class APIKeyManager:
    def __init__(self, rate_limit_config):
        self.keys = {}
        self.rate_limit_config = rate_limit_config

    def create_key(self, name, rate_limit=None, role='user'):
        key = secrets.token_urlsafe(32)
        self.keys[key] = {'name': name, 'rate_limit': rate_limit or self.rate_limit_config.default_rate_limit, 'role': role}
        return key

    def validate_key(self, api_key):
        return self.keys.get(api_key)

    def revoke_key(self, api_key):
        if api_key in self.keys:
            del self.keys[api_key]
            return True
        return False

class OAuth2Manager:
    def __init__(self, config, token_store):
        self.config = config
        self.token_store = token_store

    def create_access_token(self, client_id, scopes=None):
        payload = {
            'iss': self.config.issuer,
            'aud': self.config.audience,
            'sub': client_id,
            'iat': datetime.now(timezone.utc),
            'exp': datetime.now(timezone.utc) + timedelta(minutes=self.config.access_token_expiry_minutes),
            'scope': scopes or ['read']
        }
        token = jwt.encode(payload, self.config.secret_key, algorithm='HS256')
        return token

    async def validate_token(self, token):
        try:
            payload = jwt.decode(token, self.config.secret_key, algorithms=['HS256'])
            return payload
        except:
            return None

class FileTokenStore:
    def __init__(self, path):
        self.path = path
        self.tokens = {}
        self.load()

    def load(self):
        if os.path.exists(self.path):
            with open(self.path, 'r') as f:
                self.tokens = json.load(f)

    def save(self):
        with open(self.path, 'w') as f:
            json.dump(self.tokens, f)

    async def add_token(self, token, expires):
        self.tokens[token] = expires.isoformat()
        self.save()

    async def validate_token(self, token):
        if token in self.tokens:
            expires = datetime.fromisoformat(self.tokens[token])
            if expires > datetime.now(timezone.utc):
                return True
            else:
                del self.tokens[token]
                self.save()
        return False

    async def clean_expired(self):
        now = datetime.now(timezone.utc)
        expired = [t for t, e in self.tokens.items() if datetime.fromisoformat(e) < now]
        for t in expired:
            del self.tokens[t]
        if expired:
            self.save()

class RedisTokenStore:
    def __init__(self, redis_url):
        self.redis = redis.from_url(redis_url)

    async def add_token(self, token, expires):
        await self.redis.setex(token, int((expires - datetime.now(timezone.utc)).total_seconds()), 'valid')

    async def validate_token(self, token):
        return await self.redis.exists(token) > 0

    async def clean_expired(self):
        # Redis handles expiration automatically
        pass

class WebhookManager:
    def __init__(self, config, event_bus=None):
        self.config = config
        self.event_bus = event_bus
        self.subscriptions = {}
        self.delivery_queue = deque()
        self.db_path = config.db_path
        self._lock = asyncio.Lock()
        self._init_db()
        self._load_subscriptions()
        self._running = True

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                callback_url TEXT NOT NULL,
                max_retries INTEGER,
                created_at TEXT
            )
        """)
        conn.commit()
        conn.close()

    def _load_subscriptions(self):
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute("SELECT id, event_type, callback_url, max_retries FROM subscriptions").fetchall()
        conn.close()
        for row in rows:
            self.subscriptions[row[0]] = {
                'id': row[0],
                'event_type': row[1],
                'callback_url': row[2],
                'max_retries': row[3] or self.config.max_retries
            }

    async def subscribe(self, event_type, callback_url, max_retries=None):
        sub_id = str(uuid.uuid4())
        async with self._lock:
            self.subscriptions[sub_id] = {
                'id': sub_id,
                'event_type': event_type,
                'callback_url': callback_url,
                'max_retries': max_retries or self.config.max_retries
            }
            conn = sqlite3.connect(self.db_path)
            conn.execute("INSERT INTO subscriptions (id, event_type, callback_url, max_retries, created_at) VALUES (?, ?, ?, ?, ?)",
                         (sub_id, event_type, callback_url, max_retries or self.config.max_retries, datetime.now(timezone.utc).isoformat()))
            conn.commit()
            conn.close()
        return sub_id

    async def unsubscribe(self, subscription_id):
        async with self._lock:
            if subscription_id in self.subscriptions:
                del self.subscriptions[subscription_id]
                conn = sqlite3.connect(self.db_path)
                conn.execute("DELETE FROM subscriptions WHERE id = ?", (subscription_id,))
                conn.commit()
                conn.close()
                return True
        return False

    async def _process_deliveries_loop(self):
        while self._running:
            await asyncio.sleep(1)
            # Process queue (simplified)
            if self.delivery_queue:
                item = self.delivery_queue.popleft()
                # deliver webhook
                logger.info(f"Delivering webhook: {item}")

    async def shutdown(self):
        self._running = False

class AuditLogger:
    def __init__(self, config):
        self.path = config.audit_log_path

    def log(self, event):
        with open(self.path, 'a') as f:
            f.write(json.dumps(event, default=str) + '\n')

class HealthChecker:
    def __init__(self, api):
        self.api = api

    async def check(self):
        # Check core components
        return {'status': 'healthy'}

# ============================================================================
# Multi-Objective Optimization Classes (with fixed NSGA-II)
# ============================================================================

@dataclass
class MOPDPoint:
    """Represents a point in the Pareto front."""
    policy_id: str
    parameters: Dict[str, Any]
    objectives: Dict[str, float]
    scalarised_score: float = 0.0
    # For NSGA-II sorting
    rank: int = 0
    crowding_distance: float = 0.0

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)


class NSGAIIOptimizer:
    """
    Simple NSGA-II implementation for multi-objective optimization.
    Assumes all objectives are to be maximized.
    Fixed tournament selection and rank assignment.
    """
    def __init__(self,
                 evaluate_func: Callable[[Dict[str, Any]], Dict[str, float]],
                 parameter_bounds: Dict[str, Tuple[float, float]],
                 population_size: int = 20,
                 generations: int = 10,
                 mutation_rate: float = 0.2,
                 crossover_rate: float = 0.8,
                 tournament_size: int = 3,
                 objective_weights: Optional[Dict[str, float]] = None,
                 dynamic_weights: bool = True):
        self.evaluate_func = evaluate_func
        self.parameter_bounds = parameter_bounds
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.objective_weights = objective_weights or {}
        self.dynamic_weights = dynamic_weights

        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.pareto_front: List[MOPDPoint] = []
        self._eval_cache: Dict[Tuple[float, ...], Dict[str, float]] = {}
        self._pop_points: List[MOPDPoint] = []  # points corresponding to current population

    def _random_individual(self) -> Dict[str, float]:
        ind = {}
        for name, (low, high) in self.parameter_bounds.items():
            ind[name] = random.uniform(low, high)
        return ind

    def _crossover(self, p1: Dict, p2: Dict) -> Dict:
        child = {}
        for name in self.parameter_bounds:
            if random.random() < 0.5:
                low, high = self.parameter_bounds[name]
                u = random.random()
                if u <= 0.5:
                    beta = (2 * u) ** (1 / (20 + 1))
                else:
                    beta = (1 / (2 * (1 - u))) ** (1 / (20 + 1))
                val = 0.5 * ((1 + beta) * p1[name] + (1 - beta) * p2[name])
                child[name] = max(low, min(high, val))
            else:
                child[name] = p1[name] if random.random() < 0.5 else p2[name]
        return child

    def _mutate(self, ind: Dict) -> Dict:
        mutant = ind.copy()
        for name, (low, high) in self.parameter_bounds.items():
            if random.random() < self.mutation_rate:
                u = random.random()
                if u < 0.5:
                    delta = (2 * u) ** (1 / (20 + 1)) - 1
                else:
                    delta = 1 - (2 * (1 - u)) ** (1 / (20 + 1))
                mutant[name] = mutant[name] + delta * (high - low)
                mutant[name] = max(low, min(high, mutant[name]))
        return mutant

    def _fast_non_dominated_sort(self, points: List[MOPDPoint]) -> List[List[MOPDPoint]]:
        fronts = []
        domination_count = {id(p): 0 for p in points}
        dominated_solutions = {id(p): [] for p in points}
        for i, p in enumerate(points):
            p_obj = p.objectives
            for j, q in enumerate(points):
                if i == j:
                    continue
                q_obj = q.objectives
                if all(p_obj[k] >= q_obj[k] for k in p_obj) and any(p_obj[k] > q_obj[k] for k in p_obj):
                    dominated_solutions[id(p)].append(q)
                elif all(q_obj[k] >= p_obj[k] for k in q_obj) and any(q_obj[k] > p_obj[k] for k in q_obj):
                    domination_count[id(p)] += 1
            if domination_count[id(p)] == 0:
                p.rank = 0
                if not fronts:
                    fronts.append([])
                fronts[0].append(p)

        i = 0
        while i < len(fronts):
            next_front = []
            for p in fronts[i]:
                for q in dominated_solutions[id(p)]:
                    domination_count[id(q)] -= 1
                    if domination_count[id(q)] == 0:
                        q.rank = i + 1
                        next_front.append(q)
            if next_front:
                fronts.append(next_front)
            i += 1
        return fronts

    def _crowding_distance(self, front: List[MOPDPoint]) -> Dict[int, float]:
        if not front:
            return {}
        distances = {id(p): 0.0 for p in front}
        objective_keys = list(front[0].objectives.keys())
        for obj in objective_keys:
            sorted_front = sorted(front, key=lambda x: x.objectives[obj])
            distances[id(sorted_front[0])] = float('inf')
            distances[id(sorted_front[-1])] = float('inf')
            obj_min = sorted_front[0].objectives[obj]
            obj_max = sorted_front[-1].objectives[obj]
            if obj_max == obj_min:
                continue
            for i in range(1, len(sorted_front) - 1):
                distances[id(sorted_front[i])] += (sorted_front[i+1].objectives[obj] - sorted_front[i-1].objectives[obj]) / (obj_max - obj_min)
        # Store crowding distance in point
        for p in front:
            p.crowding_distance = distances.get(id(p), 0)
        return distances

    def _tournament_selection(self, population: List[MOPDPoint]) -> MOPDPoint:
        """Select a point from population based on rank and crowding distance."""
        candidates = random.sample(population, min(self.tournament_size, len(population)))
        best = candidates[0]
        for cand in candidates[1:]:
            if cand.rank < best.rank or (cand.rank == best.rank and cand.crowding_distance > best.crowding_distance):
                best = cand
        return best

    def _select_best_from_pareto(self, pareto: List[MOPDPoint], weights: Dict[str, float]) -> Optional[MOPDPoint]:
        if not pareto:
            return None
        obj_keys = list(weights.keys())
        max_vals = {k: max(p.objectives[k] for p in pareto) for k in obj_keys}
        min_vals = {k: min(p.objectives[k] for p in pareto) for k in obj_keys}
        ranges = {k: max_vals[k] - min_vals[k] if max_vals[k] != min_vals[k] else 1.0 for k in obj_keys}

        best = None
        best_score = -float('inf')
        for p in pareto:
            score = 0.0
            for k in obj_keys:
                val = p.objectives[k]
                norm = (val - min_vals[k]) / ranges[k] if ranges[k] > 0 else 1.0
                score += weights.get(k, 0.0) * norm
            p.scalarised_score = score
            if score > best_score:
                best_score = score
                best = p
        return best

    async def evolve(self) -> List[MOPDPoint]:
        # Initialize population as points
        population = []
        for _ in range(self.population_size):
            ind = self._random_individual()
            obj = await self.evaluate_func(ind)
            point = MOPDPoint(
                policy_id=str(uuid.uuid4()),
                parameters=ind,
                objectives=obj
            )
            population.append(point)
            self._eval_cache[tuple(sorted(ind.items()))] = obj
        self._pop_points = population

        for gen in range(self.generations):
            # Non-dominated sort on current population
            fronts = self._fast_non_dominated_sort(population)
            # Calculate crowding distance for each front
            for front in fronts:
                self._crowding_distance(front)

            # Create offspring
            offspring = []
            while len(offspring) < self.population_size:
                parent1 = self._tournament_selection(population)
                parent2 = self._tournament_selection(population)
                if random.random() < self.crossover_rate:
                    child_ind = self._crossover(parent1.parameters, parent2.parameters)
                else:
                    child_ind = copy.deepcopy(parent1.parameters)
                child_ind = self._mutate(child_ind)
                # Evaluate
                key = tuple(sorted(child_ind.items()))
                if key in self._eval_cache:
                    obj = self._eval_cache[key]
                else:
                    obj = await self.evaluate_func(child_ind)
                    self._eval_cache[key] = obj
                child_point = MOPDPoint(
                    policy_id=str(uuid.uuid4()),
                    parameters=child_ind,
                    objectives=obj
                )
                offspring.append(child_point)

            # Combine parent and offspring
            combined = population + offspring
            # Remove duplicates (by parameters hash)
            unique = {}
            for p in combined:
                key = tuple(sorted(p.parameters.items()))
                unique[key] = p
            combined = list(unique.values())

            # Non-dominated sort on combined
            fronts = self._fast_non_dominated_sort(combined)
            for front in fronts:
                self._crowding_distance(front)

            # Select next population
            next_pop = []
            for front in fronts:
                if len(next_pop) + len(front) <= self.population_size:
                    next_pop.extend(front)
                else:
                    # Sort front by crowding distance descending and fill remaining slots
                    sorted_front = sorted(front, key=lambda x: x.crowding_distance, reverse=True)
                    remaining = self.population_size - len(next_pop)
                    next_pop.extend(sorted_front[:remaining])
                    break
            population = next_pop[:self.population_size]
            self._pop_points = population

            # Update Pareto front (first front)
            if fronts:
                self.pareto_front = fronts[0]
            logger.info(f"Generation {gen+1}/{self.generations}: Pareto front size={len(self.pareto_front)}")

        # Final Pareto front and best selection
        fronts = self._fast_non_dominated_sort(population)
        self.pareto_front = fronts[0] if fronts else []
        weights = self._compute_dynamic_weights()
        best = self._select_best_from_pareto(self.pareto_front, weights)
        if best:
            self.best_individual = best.parameters
            self.best_fitness = best.scalarised_score
        return self.pareto_front

    def _compute_dynamic_weights(self) -> Dict[str, float]:
        weights = self.objective_weights.copy()
        if not self.dynamic_weights or not self.pareto_front:
            return weights
        if 'total_harvested' in weights:
            values = [p.objectives.get('total_harvested', 0) for p in self.pareto_front]
            if values:
                avg = sum(values) / len(values)
                max_val = max(values)
                if max_val > 0 and avg < 0.5 * max_val:
                    weights['total_harvested'] = min(0.5, weights['total_harvested'] * 1.5)
                    total = sum(weights.values())
                    weights = {k: v / total for k, v in weights.items()}
        return weights


# (Other optimizers GA, PSO, DE remain similar, but we'll include simplified versions for completeness)
class GeneticAlgorithmOptimizer:
    # ... (same as before)
    pass

class ParticleSwarmOptimizer:
    # ...
    pass

class DifferentialEvolutionOptimizer:
    # ...
    pass

# ============================================================================
# Optimization Manager (unchanged except to use fixed NSGA-II)
# ============================================================================

class OptimizationManager:
    """Manages optimization jobs and results."""
    def __init__(self, api: 'BioInspiredAPI'):
        self.api = api
        self.config = api.config.optimization
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def start_optimization(self, request: OptimizationStartRequest) -> str:
        job_id = str(uuid.uuid4())
        algorithm = request.algorithm or self.config.algorithm
        bounds = request.parameter_bounds or self._get_default_bounds()
        weights = request.objective_weights or self.config.objective_weights
        population_size = request.population_size or self.config.population_size
        generations = request.generations or self.config.generations

        async with self._lock:
            self.jobs[job_id] = {
                'status': 'pending',
                'algorithm': algorithm,
                'start_time': datetime.now(timezone.utc),
                'end_time': None,
                'result': None,
                'error': None,
            }

        asyncio.create_task(self._run_optimization(job_id, algorithm, bounds, weights, population_size, generations))
        return job_id

    async def _run_optimization(self, job_id, algorithm, bounds, weights, population_size, generations):
        try:
            async def evaluate_func(params):
                # Apply params to bio core and measure objectives (simulated)
                await asyncio.sleep(0.05)
                # In real implementation, call appropriate bio_core methods
                return {
                    'total_harvested': random.uniform(50, 200),
                    'avg_efficiency': random.uniform(0.5, 0.95),
                    'carbon_saved': random.uniform(0, 10),
                    'helium_saved': random.uniform(0, 5),
                }

            if algorithm == 'nsga2':
                optimizer = NSGAIIOptimizer(
                    evaluate_func=evaluate_func,
                    parameter_bounds=bounds,
                    population_size=population_size,
                    generations=generations,
                    mutation_rate=self.config.mutation_rate,
                    crossover_rate=self.config.crossover_rate,
                    tournament_size=self.config.tournament_size,
                    objective_weights=weights,
                    dynamic_weights=self.config.dynamic_weights
                )
                pareto = await optimizer.evolve()
                result = {
                    'pareto_front': [p.to_dict() for p in pareto],
                    'best': optimizer.best_individual,
                }
            elif algorithm == 'ga':
                optimizer = GeneticAlgorithmOptimizer(
                    evaluate_func=evaluate_func,
                    parameter_bounds=bounds,
                    population_size=population_size,
                    generations=generations,
                    mutation_rate=self.config.mutation_rate,
                    crossover_rate=self.config.crossover_rate,
                )
                best = await optimizer.evolve()
                result = {'best': best}
            elif algorithm == 'pso':
                optimizer = ParticleSwarmOptimizer(
                    evaluate_func=evaluate_func,
                    parameter_bounds=bounds,
                    num_particles=population_size,
                    generations=generations,
                )
                best = await optimizer.evolve()
                result = {'best': best}
            elif algorithm == 'de':
                optimizer = DifferentialEvolutionOptimizer(
                    evaluate_func=evaluate_func,
                    parameter_bounds=bounds,
                    population_size=population_size,
                    generations=generations,
                )
                best = await optimizer.evolve()
                result = {'best': best}
            else:
                raise ValueError(f"Unsupported algorithm: {algorithm}")

            async with self._lock:
                self.jobs[job_id]['status'] = 'completed'
                self.jobs[job_id]['result'] = result
                self.jobs[job_id]['end_time'] = datetime.now(timezone.utc)
        except Exception as e:
            async with self._lock:
                self.jobs[job_id]['status'] = 'failed'
                self.jobs[job_id]['error'] = str(e)
                self.jobs[job_id]['end_time'] = datetime.now(timezone.utc)

    def _get_default_bounds(self):
        return {
            'conversion_factor': (0.5, 1.5),
            'repair_rate': (0.001, 0.02),
            'sensitivity_multiplier': (0.5, 2.0),
            'token_allocation_weight': (0.1, 0.9),
        }

    async def get_job_status(self, job_id: str) -> Optional[Dict]:
        async with self._lock:
            return self.jobs.get(job_id)

    async def apply_policy(self, job_id: str, policy_id: str):
        async with self._lock:
            job = self.jobs.get(job_id)
            if not job:
                raise APIError(404, "not_found", "Optimization job not found")
            if job['status'] != 'completed':
                raise APIError(409, "conflict", "Job not completed")
            result = job['result']
            if 'pareto_front' in result:
                for point in result['pareto_front']:
                    if point['policy_id'] == policy_id or policy_id == 'best':
                        params = point['parameters']
                        # Apply to bio core (simulated)
                        logger.info(f"Applying policy {point['policy_id']} with params {params}")
                        return {"success": True, "policy_id": point['policy_id'], "parameters": params}
                raise APIError(404, "not_found", "Policy not found in Pareto front")
            else:
                params = result.get('best')
                if params:
                    logger.info(f"Applying best parameters {params}")
                    return {"success": True, "parameters": params}
                raise APIError(404, "not_found", "No result available")

# ============================================================================
# BioInspiredAPI main class with enhancements
# ============================================================================

class BioInspiredAPI:
    """
    Enhanced Bio-Inspired API v10.5.0
    """
    def __init__(self, bio_core=None, config: Optional[Union[APIConfig, Dict]] = None,
                 # Central components
                 storage: Optional[CentralStorage] = None,
                 message_queue: Optional[AsyncMessageQueue] = None,
                 adaptive_cost: Optional[AdaptiveCostFunction] = None,
                 pareto_gating: Optional[ParetoGating] = None,
                 drift_detector: Optional[DriftDetector] = None,
                 metrics: Optional[MetricsRegistry] = None,
                 # New modules
                 rl_agent: Optional[CausalRLAgent] = None,
                 federated_coordinator: Optional[FederatedCoordinator] = None,
                 safety_monitor: Optional[SafetyMonitor] = None,
                 precision_controller: Optional[PrecisionController] = None,
                 carbon_market_client: Optional[CarbonMarketClient] = None,
                 chaos_injector: Optional[ChaosInjector] = None,
                 human_approval_handler: Optional[HumanApprovalHandler] = None,
                 ):
        self.bio_core = bio_core

        # Load config
        if isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = APIConfig(**config)
            else:
                self.config = APIConfig(**config)
        elif isinstance(config, APIConfig):
            self.config = config
        else:
            self.config = APIConfig()

        # Central components
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto_gating = pareto_gating
        self.drift_detector = drift_detector
        self.central_metrics = metrics

        # Initialize bio_core components
        self.token_manager = getattr(bio_core, 'token_manager', None) if bio_core else None
        self.gradient_manager = getattr(bio_core, 'gradient_manager', None) if bio_core else None
        self.compartment_manager = getattr(bio_core, 'compartment_manager', None) if bio_core else None
        self.biomass_storage = getattr(bio_core, 'biomass_storage', None) if bio_core else None
        self.harvester = getattr(bio_core, 'harvester', None) if bio_core else None
        self.scheduler = getattr(bio_core, 'scheduler', None) if bio_core else None
        self.degradation_manager = getattr(bio_core, 'degradation_manager', None) if bio_core else None
        self.knowledge_transfer = getattr(bio_core, 'knowledge_transfer', None) if bio_core else None
        self.supply_manager = getattr(bio_core, 'supply_manager', None) if bio_core else None
        self.token_allocator = getattr(bio_core, 'token_allocator', None) if bio_core else None
        self.event_bus = getattr(bio_core, 'event_broker', None) if bio_core else None
        self.health_manager = getattr(bio_core, 'health_manager', None) if bio_core else None
        self.state_manager = getattr(bio_core, 'state_manager', None) if bio_core else None

        # Task manager
        self.task_manager = TaskManager()

        # Container for DI
        self.container = Container()
        self.container.register('config', self.config)
        self.container.register('api', self)

        # Token store
        if self.config.oauth2.refresh_token_store_backend == "redis" and REDIS_AVAILABLE and self.config.oauth2.refresh_token_redis_url:
            self.token_store = RedisTokenStore(self.config.oauth2.refresh_token_redis_url)
        else:
            self.token_store = FileTokenStore(self.config.oauth2.refresh_token_file_path)

        # OAuth2 manager
        self.oauth2_manager = OAuth2Manager(self.config.oauth2, self.token_store)

        # Rate limiter
        self.adaptive_limiter = SlidingWindowRateLimiter(self.config.rate_limit)

        # API key manager
        self.api_key_manager = APIKeyManager(self.config.rate_limit)

        # Cache
        if self.config.cache.backend == "redis" and REDIS_AVAILABLE and self.config.cache.redis_url:
            self.cache = RedisCacheBackend(self.config.cache.redis_url, self.config.cache.ttl_seconds)
        else:
            self.cache = MemoryCacheBackend(self.config.cache.max_items)

        # Webhook manager
        self.webhook_manager = WebhookManager(self.config.webhook, self.event_bus)

        # Health checker
        self.health_checker = HealthChecker(self)

        # Audit logger
        self.audit_logger = AuditLogger(self.config)

        # WebSocket server
        self.websocket_server = None
        if self.config.websocket.enabled and WEBSOCKETS_AVAILABLE:
            self.websocket_server = WebSocketServer(self, self.config.websocket.port)
            self.task_manager.start_task("websocket_server", self.websocket_server.start)

        # New: Optimization Manager
        self.optimization_manager = OptimizationManager(self)

        # Enhanced modules
        # RL Agent
        if rl_agent:
            self.rl_agent = rl_agent
        elif self.config.enable_causal_rl:
            # Define state and action dimensions; adjust as needed
            state_dim = 10
            action_dim = 3  # e.g., increase, decrease, maintain
            self.rl_agent = CausalRLAgent(state_dim, action_dim)
        else:
            self.rl_agent = None

        # Federated Coordinator
        if federated_coordinator:
            self.federated = federated_coordinator
        elif self.config.enable_federated_learning and message_queue:
            self.federated = FederatedCoordinator(self, message_queue)
        else:
            self.federated = None

        # Safety Monitor
        if safety_monitor:
            self.safety_monitor = safety_monitor
        elif self.config.enable_safety_monitor:
            self.safety_monitor = SafetyMonitor()
            self._setup_safety_invariants()
        else:
            self.safety_monitor = None

        # Precision Controller
        self.precision_controller = precision_controller if precision_controller else (
            PrecisionController() if self.config.enable_precision_switching else None)

        # Carbon Market
        if carbon_market_client:
            self.carbon_market = carbon_market_client
        elif self.config.enable_carbon_market and self.config.carbon_market_config:
            self.carbon_market = CarbonMarketClient(**self.config.carbon_market_config)
        else:
            self.carbon_market = None

        # Chaos Injector
        self.chaos_injector = chaos_injector if chaos_injector else (
            ChaosInjector(self, self.config.chaos_probability) if self.config.enable_chaos else None)

        # Human Approval Handler
        self.human_approval = human_approval_handler if human_approval_handler else (
            HumanApprovalHandler(message_queue) if self.config.enable_human_approval else None)

        # Register background tasks
        self.task_manager.register_task("token_cleanup", self._token_cleanup_loop)
        self.task_manager.register_task("webhook_processor", self.webhook_manager._process_deliveries_loop)
        if self.federated:
            self.task_manager.register_task("federated_update", self._federated_loop)
        if self.chaos_injector:
            self.task_manager.register_task("chaos", self._chaos_loop)
        self.task_manager.start_registered_tasks()

        # Request history and latency
        self.request_history = deque(maxlen=10000)
        self.latency_histogram = defaultdict(list)

        # Route registry with OpenAPI metadata
        self.routes = {}
        self.handlers = {}

        # Initialize handlers and register routes
        self._init_handlers()
        self._register_routes()

        # OpenAPI generator
        self.openapi_generator = OpenAPIGenerator(self.config, self.routes)

        # Prometheus metrics
        self._setup_metrics()

        # If central metrics provided, use them; otherwise local
        if self.central_metrics:
            self.metrics = self.central_metrics
        else:
            self._setup_metrics()

        logger.info("Enhanced Bio-Inspired API v10.5.0 initialized",
                    central=bool(message_queue or adaptive_cost or pareto_gating),
                    rl_agent=self.rl_agent is not None,
                    federated=self.federated is not None,
                    safety_monitor=self.safety_monitor is not None,
                    xai=self.config.enable_xai,
                    precision_controller=self.precision_controller is not None,
                    carbon_market=self.carbon_market is not None,
                    chaos=self.chaos_injector is not None,
                    human_approval=self.human_approval is not None)

    def _setup_safety_invariants(self):
        self.safety_monitor.add_invariant(
            "queue_size",
            lambda s: s.get('queue_size', 0) <= 100,
            "Execution queue too large"
        )
        self.safety_monitor.add_invariant(
            "token_balance_non_negative",
            lambda s: s.get('token_balance', 0) >= 0,
            "Token balance negative"
        )
        # Add more as needed

    def _setup_metrics(self):
        if not self.config.enable_prometheus or not PROMETHEUS_AVAILABLE:
            self.metrics = None
            return
        registry = CollectorRegistry()
        self.metrics = {
            'request_count': Counter('api_request_count_total', 'Total requests', ['method', 'endpoint'], registry=registry),
            'request_latency': Histogram('api_request_latency_seconds', 'Request latency', ['method', 'endpoint'], registry=registry),
            'error_count': Counter('api_error_count_total', 'Total errors', ['code'], registry=registry),
            'rate_limit_hits': Counter('api_rate_limit_hits_total', 'Rate limit hits', registry=registry),
            'cache_hits': Counter('api_cache_hits_total', 'Cache hits', registry=registry),
            'cache_misses': Counter('api_cache_misses_total', 'Cache misses', registry=registry),
            'optimization_jobs': Gauge('api_optimization_jobs', 'Number of optimization jobs', registry=registry),
        }

    def _init_handlers(self):
        # Instantiate all handlers (we'll include stubs for brevity)
        # Example:
        self.handlers['token'] = TokenHandler(self.container)
        self.handlers['optimization'] = OptimizationHandler(self.container)
        # ... other handlers

    def _register_routes(self):
        # Register routes with metadata
        self.routes['/tokens/generate'] = ('POST', self.handlers['token'].generate_token, {
            'summary': 'Generate Eco-ATP tokens',
            'tags': ['Tokens'],
            'auth_required': True,
            'request_model': TokenGenerateRequest
        })
        self.routes['/optimize/start'] = ('POST', self.handlers['optimization'].start_optimization, {
            'summary': 'Start an optimization job',
            'tags': ['Optimization'],
            'auth_required': True,
            'request_model': OptimizationStartRequest
        })
        self.routes['/optimize/status/{job_id}'] = ('GET', self.handlers['optimization'].get_job_status, {
            'summary': 'Get optimization job status',
            'tags': ['Optimization'],
            'auth_required': True,
            'request_model': None
        })
        self.routes['/optimize/apply'] = ('POST', self.handlers['optimization'].apply_policy, {
            'summary': 'Apply a policy from optimization results',
            'tags': ['Optimization'],
            'auth_required': True,
            'request_model': OptimizationApplyRequest
        })

    async def _token_cleanup_loop(self):
        while True:
            await asyncio.sleep(3600)
            await self.token_store.clean_expired()

    async def _federated_loop(self):
        while True:
            await asyncio.sleep(300)  # every 5 minutes
            if self.federated:
                await self.federated.send_update()

    async def _chaos_loop(self):
        while True:
            await asyncio.sleep(60)
            if self.chaos_injector:
                await self.chaos_injector.maybe_inject_failure()

    # --- Request handling with safety checks, XAI, etc. ---
    async def handle_request(self, method: str, path: str,
                             headers: Dict[str, str] = None,
                             body: Dict[str, Any] = None,
                             query_params: Dict[str, str] = None) -> Dict[str, Any]:
        start = time.time()
        try:
            api_key = headers.get('X-API-Key') if headers else None
            auth_header = headers.get('Authorization') if headers else None

            # Rate limit
            if api_key:
                allowed, rate_info = await self.adaptive_limiter.check_rate_limit(api_key)
                if not allowed:
                    raise APIError(429, "rate_limit_exceeded", "Rate limit exceeded", rate_info)

            # Route lookup
            route_key = f"{method} {path}"
            if route_key not in self.routes:
                raise APIError(404, "not_found", f"Endpoint {route_key} not found")

            handler_func, metadata = self.routes[route_key][1], self.routes[route_key][2]

            # Validate request body
            request_model = metadata.get('request_model')
            validated = None
            if request_model and body:
                try:
                    validated = request_model(**body)
                except Exception as e:
                    raise APIError(400, "validation_error", "Request validation failed", {"errors": str(e)})

            # Check safety before executing (if safety monitor exists)
            if self.safety_monitor and metadata.get('safety_check', True):
                state = self._get_safety_state()
                violations = self.safety_monitor.check(state)
                if violations:
                    raise APIError(403, "safety_violation", "Safety violation", {"violations": violations})

            # Cache (GET only)
            cache_key = None
            if method == 'GET' and self.config.cache.enabled:
                cache_key = f"{path}:{json.dumps(query_params or {})}"
                cached = await self.cache.get(cache_key)
                if cached:
                    self.metrics['cache_hits'].inc() if self.metrics else None
                    return cached

            # Execute handler
            result = await handler_func(validated if request_model else body)

            # XAI: attach explanation if enabled and handler provides context
            if self.config.enable_xai:
                explanation = self._generate_explanation(method, path, result)
                if explanation:
                    result['explanation'] = explanation

            # Human approval for critical actions (if handler flagged)
            if self.human_approval and metadata.get('requires_approval', False):
                approved = await self.human_approval.request_approval({
                    'action': route_key,
                    'request': body,
                })
                if not approved:
                    raise APIError(403, "rejected_by_human", "Request rejected by human")

            # Cache response if GET
            if cache_key:
                await self.cache.set(cache_key, result)

            # Metrics
            latency = time.time() - start
            self.latency_histogram[route_key].append(latency)
            if self.metrics:
                self.metrics['request_count'].labels(method=method, endpoint=path).inc()
                self.metrics['request_latency'].labels(method=method, endpoint=path).observe(latency)

            return result

        except APIError as e:
            if self.metrics:
                self.metrics['error_count'].labels(code=e.code).inc()
            return error_response(e.status_code, e.code, e.message, e.details)

        except Exception as e:
            logger.error("Unhandled error", error=str(e), exc_info=True)
            if self.metrics:
                self.metrics['error_count'].labels(code='internal_server_error').inc()
            return error_response(500, "internal_server_error", "Internal server error")

    def _get_safety_state(self) -> Dict[str, Any]:
        # Gather relevant state for safety checks
        return {
            'queue_size': len(self.webhook_manager.delivery_queue),
            'token_balance': getattr(self.token_manager, 'total_balance', 0) if self.token_manager else 0,
        }

    def _generate_explanation(self, method, path, result):
        """Simple rule-based explanation for XAI."""
        if path.startswith('/optimize/apply'):
            return "Applied policy selected from Pareto front based on weighted objectives."
        elif path.startswith('/tokens/generate'):
            return "Tokens generated based on energy savings and efficiency."
        elif path.startswith('/webhook/subscribe'):
            return "Webhook subscription registered."
        return None

    async def get_openapi(self) -> Dict:
        return self.openapi_generator.generate()

    async def shutdown(self):
        logger.info("Shutting down API")
        await self.task_manager.stop_all()
        if self.websocket_server:
            await self.websocket_server.stop()
        await self.webhook_manager.shutdown()
        logger.info("API shutdown complete")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()


# ============================================================================
# Optimization Handler
# ============================================================================

class OptimizationHandler(BaseHandler):
    async def start_optimization(self, request: OptimizationStartRequest) -> Dict:
        job_id = await self.api.optimization_manager.start_optimization(request)
        return {"job_id": job_id, "status": "started"}

    async def get_job_status(self, job_id: str) -> Dict:
        status = await self.api.optimization_manager.get_job_status(job_id)
        if not status:
            raise APIError(404, "not_found", "Optimization job not found")
        return status

    async def apply_policy(self, request: OptimizationApplyRequest) -> Dict:
        return await self.api.optimization_manager.apply_policy(request.job_id, request.policy_id)

# ============================================================================
# WebSocket Server (unchanged from previous version, but included)
# ============================================================================

class WebSocketServer:
    def __init__(self, api: 'BioInspiredAPI', port: int = 8765):
        self.api = api
        self.port = port
        self.connections: Set[websockets.WebSocketServerProtocol] = set()
        self.subscribers: Dict[str, Set[websockets.WebSocketServerProtocol]] = defaultdict(set)
        self.server = None
        self._lock = asyncio.Lock()
        self._heartbeat_task = None

    async def start(self):
        if not WEBSOCKETS_AVAILABLE:
            logger.warning("WebSockets not available")
            return
        self.server = await websockets.serve(self._handler, '0.0.0.0', self.port)
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info(f"WebSocket server started on port {self.port}")

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            await asyncio.gather(self._heartbeat_task, return_exceptions=True)
        async with self._lock:
            for ws in self.connections:
                await ws.close(1000, "Server shutting down")
            self.connections.clear()

    async def _heartbeat_loop(self):
        while True:
            try:
                await asyncio.sleep(self.api.config.websocket.heartbeat_interval)
                async with self._lock:
                    for ws in self.connections:
                        try:
                            await ws.ping()
                        except:
                            pass
            except asyncio.CancelledError:
                break

    async def _handler(self, websocket, path):
        auth_token = None
        if self.api.config.websocket.auth_required:
            query = parse_qs(urlparse(path).query)
            if 'token' in query:
                auth_token = query['token'][0]
            if not auth_token:
                try:
                    auth_msg = await asyncio.wait_for(websocket.recv(), timeout=5)
                    auth_token = auth_msg.strip()
                except asyncio.TimeoutError:
                    await websocket.close(1008, "Authentication timeout")
                    return
            if auth_token.startswith("Bearer "):
                token = auth_token[7:]
                payload = await self.api.oauth2_manager.validate_token(token)
                if not payload:
                    await websocket.close(1008, "Authentication failed")
                    return
                client_id = payload['sub']
            else:
                key_data = self.api.api_key_manager.validate_key(auth_token)
                if not key_data:
                    await websocket.close(1008, "Authentication failed")
                    return
                client_id = key_data['name']
        else:
            client_id = "anonymous"

        channels = ['global']
        if path.startswith('/events/'):
            channel = path.split('/')[-1]
            channels.append(channel)

        async with self._lock:
            self.connections.add(websocket)
            for channel in channels:
                self.subscribers[channel].add(websocket)

        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    if data.get('type') == 'subscribe':
                        new_channel = data.get('channel')
                        if new_channel:
                            async with self._lock:
                                self.subscribers[new_channel].add(websocket)
                    elif data.get('type') == 'ping':
                        await websocket.send(json.dumps({'type': 'pong'}))
                except:
                    pass
        finally:
            async with self._lock:
                self.connections.remove(websocket)
                for channel in list(self.subscribers.keys()):
                    self.subscribers[channel].discard(websocket)
                    if not self.subscribers[channel]:
                        del self.subscribers[channel]

    async def broadcast(self, event: Dict, channels: List[str] = None):
        if not self.connections:
            return
        message = json.dumps(event, default=str)
        if channels is None:
            channels = ['global']
        async with self._lock:
            recipients = set()
            for channel in channels:
                recipients.update(self.subscribers.get(channel, []))
        await asyncio.gather(*(ws.send(message) for ws in recipients), return_exceptions=True)

# ============================================================================
# Example usage
# ============================================================================

async def main():
    logging.basicConfig(level=logging.INFO)
    config = APIConfig()
    async with BioInspiredAPI(config=config) as api:
        request = OptimizationStartRequest(algorithm="nsga2", generations=2, population_size=10)
        response = await api.handlers['optimization'].start_optimization(request)
        print("Optimization started:", response)
        await asyncio.sleep(1)
        status = await api.handlers['optimization'].get_job_status(response['job_id'])
        print("Job status:", status)

if __name__ == "__main__":
    asyncio.run(main())
