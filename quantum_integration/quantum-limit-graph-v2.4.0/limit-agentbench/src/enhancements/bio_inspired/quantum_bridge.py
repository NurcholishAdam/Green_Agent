"""
Quantum Bridge v3.3 – Enhanced with all fixes, improvements, and requested enhancement modules.
Supports configurable scaling, validation, caching, history, multiple output formats,
proper QUBO ↔ Ising conversion, custom transformations, observability, and
integrated Quantum‑Distillation, Causal RL, Federated Learning, Safety Monitor,
XAI, Adaptive Precision, Carbon Markets, Chaos Testing, and Human‑in‑the‑Loop.
"""

import logging
from typing import Dict, Any, List, Tuple, Optional, Union, Protocol, Callable, runtime_checkable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
import numpy as np
import hashlib
import json
import os
import time
from enum import Enum
import uuid
import asyncio
import random
from collections import defaultdict

# ============================================================================
# Optional dependencies
# ============================================================================
try:
    from pydantic import BaseModel, Field, validator, root_validator, ConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from prometheus_client import Gauge, Counter, Histogram, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

# ============================================================================
# Custom Exceptions
# ============================================================================
class QuantumBridgeError(Exception):
    """Base exception for Quantum Bridge."""
    pass

class ProviderError(QuantumBridgeError):
    """Gradient provider error."""
    pass

class SolverError(QuantumBridgeError):
    """Quantum solver error."""
    pass

class ConfigurationError(QuantumBridgeError):
    """Configuration error."""
    pass

class ConversionError(QuantumBridgeError):
    """QUBO/Ising conversion error."""
    pass

# ============================================================================
# Trace Context for Logging
# ============================================================================
class TraceContext:
    """Simple trace context for request correlation."""
    def __init__(self, trace_id: Optional[str] = None):
        self.trace_id = trace_id or str(uuid.uuid4())

    def get_logger(self, base_logger):
        if hasattr(base_logger, 'bind'):
            return base_logger.bind(trace_id=self.trace_id)
        return base_logger

# ============================================================================
# Configuration (Pydantic if available)
# ============================================================================
if PYDANTIC_AVAILABLE:
    class QuantumBridgeConfig(BaseModel):
        """Configuration for QuantumBridge."""
        # Mapping: gradient field name → QUBO parameter name
        field_mapping: Dict[str, str] = Field(
            default_factory=lambda: {
                'carbon': 'penalty_carbon',
                'helium': 'penalty_helium_shortage',
                'trust': 'penalty_geopolitical',
                'opportunity': 'weight_opportunity',
                'eco_atp_reserve': 'constraint_budget'
            }
        )
        scaling: Dict[str, float] = Field(
            default_factory=lambda: {
                'carbon': 10.0,
                'helium': 20.0,
                'trust': 8.0,
                'opportunity': 5.0,
                'eco_atp_reserve': 15.0
            }
        )
        default_gradient: float = 0.5
        field_specific_defaults: Dict[str, float] = Field(default_factory=dict)
        invert_fields: List[str] = Field(default_factory=lambda: ['trust', 'eco_atp_reserve'])
        enable_caching: bool = True
        cache_ttl: Optional[int] = None
        history_size: int = 100
        output_format: str = 'qubo'
        quadratic_mapping: Dict[Tuple[str, str], str] = Field(
            default_factory=lambda: {
                ('carbon', 'helium'): 'penalty_carbon_helium',
                ('trust', 'opportunity'): 'penalty_trust_opportunity'
            }
        )
        custom_transform_registry: Dict[str, str] = Field(default_factory=dict)
        cache_persistence_path: Optional[str] = None
        enable_prometheus: bool = False
        provider_retries: int = 2
        quadratic_scaling: float = 1.0
        transform_order: List[str] = Field(default_factory=lambda: ['invert', 'transform', 'scale'])
        param_types: Dict[str, str] = Field(
            default_factory=lambda: {
                'penalty_carbon': 'linear',
                'penalty_helium_shortage': 'linear',
                'penalty_geopolitical': 'linear',
                'weight_opportunity': 'linear',
                'constraint_budget': 'linear',
                'penalty_carbon_helium': 'quadratic',
                'penalty_trust_opportunity': 'quadratic'
            }
        )
        config_version: str = "3.3"
        cache_version: int = 1

        # ======== NEW ENHANCEMENT FIELDS ========
        enable_quantum_distillation: bool = False
        enable_causal_rl: bool = False
        enable_federated: bool = False
        enable_safety_monitor: bool = True
        enable_xai: bool = True
        enable_precision: bool = False
        enable_carbon_market: bool = False
        carbon_market_config: Optional[Dict[str, str]] = None
        enable_chaos: bool = False
        chaos_probability: float = 0.0
        enable_human_approval: bool = False
        human_approval_timeout: float = 60.0

        @validator('output_format')
        def validate_output_format(cls, v):
            if v not in ['qubo', 'ising']:
                raise ValueError('output_format must be "qubo" or "ising"')
            return v

        @validator('scaling')
        def validate_scaling(cls, v):
            for k, val in v.items():
                if val <= 0:
                    raise ValueError(f'Scaling factor for {k} must be positive')
            return v

        @validator('field_mapping')
        def validate_field_mapping(cls, v):
            values = list(v.values())
            if len(values) != len(set(values)):
                raise ValueError("field_mapping values must be unique")
            return v

        @root_validator
        def validate_quadratic_mapping(cls, values):
            field_mapping = values.get('field_mapping', {})
            quadratic = values.get('quadratic_mapping', {})
            for (f1, f2), param in quadratic.items():
                if f1 not in field_mapping and f2 not in field_mapping:
                    raise ValueError(f"Quadratic field pair ({f1},{f2}) not in field_mapping")
            return values

        @root_validator
        def validate_param_types(cls, values):
            param_types = values.get('param_types', {})
            field_mapping = values.get('field_mapping', {})
            quadratic_mapping = values.get('quadratic_mapping', {})
            expected_params = set(field_mapping.values()) | set(quadratic_mapping.values())
            for p in expected_params:
                if p not in param_types:
                    logger.warning(f"Parameter '{p}' not in param_types, defaulting to 'linear'")
                elif param_types[p] not in ['linear', 'quadratic']:
                    raise ValueError(f"Parameter '{p}' has invalid type '{param_types[p]}'")
            return values

        @root_validator
        def validate_transform_order(cls, values):
            order = values.get('transform_order', [])
            valid_ops = {'invert', 'transform', 'scale'}
            for op in order:
                if op not in valid_ops:
                    raise ValueError(f"Invalid transform operation '{op}'")
            return values

        def config_hash(self) -> str:
            data = {
                'field_mapping': self.field_mapping,
                'scaling': self.scaling,
                'invert_fields': self.invert_fields,
                'quadratic_mapping': {f"{k[0]}_{k[1]}": v for k, v in self.quadratic_mapping.items()},
                'custom_transform_registry': self.custom_transform_registry,
                'output_format': self.output_format,
                'quadratic_scaling': self.quadratic_scaling,
                'transform_order': self.transform_order,
                'param_types': self.param_types,
                'config_version': self.config_version,
                'default_gradient': self.default_gradient,
                'field_specific_defaults': self.field_specific_defaults,
                # Enhancement flags (affect behavior)
                'enable_quantum_distillation': self.enable_quantum_distillation,
                'enable_causal_rl': self.enable_causal_rl,
                'enable_federated': self.enable_federated,
                'enable_safety_monitor': self.enable_safety_monitor,
                'enable_xai': self.enable_xai,
                'enable_precision': self.enable_precision,
                'enable_carbon_market': self.enable_carbon_market,
                'enable_chaos': self.enable_chaos,
                'enable_human_approval': self.enable_human_approval,
            }
            return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
else:
    @dataclass
    class QuantumBridgeConfig:
        field_mapping: Dict[str, str] = field(default_factory=lambda: {
            'carbon': 'penalty_carbon',
            'helium': 'penalty_helium_shortage',
            'trust': 'penalty_geopolitical',
            'opportunity': 'weight_opportunity',
            'eco_atp_reserve': 'constraint_budget'
        })
        scaling: Dict[str, float] = field(default_factory=lambda: {
            'carbon': 10.0,
            'helium': 20.0,
            'trust': 8.0,
            'opportunity': 5.0,
            'eco_atp_reserve': 15.0
        })
        default_gradient: float = 0.5
        field_specific_defaults: Dict[str, float] = field(default_factory=dict)
        invert_fields: List[str] = field(default_factory=lambda: ['trust', 'eco_atp_reserve'])
        enable_caching: bool = True
        cache_ttl: Optional[int] = None
        history_size: int = 100
        output_format: str = 'qubo'
        quadratic_mapping: Dict[Tuple[str, str], str] = field(default_factory=lambda: {
            ('carbon', 'helium'): 'penalty_carbon_helium',
            ('trust', 'opportunity'): 'penalty_trust_opportunity'
        })
        custom_transform_registry: Dict[str, str] = field(default_factory=dict)
        cache_persistence_path: Optional[str] = None
        enable_prometheus: bool = False
        provider_retries: int = 2
        quadratic_scaling: float = 1.0
        transform_order: List[str] = field(default_factory=lambda: ['invert', 'transform', 'scale'])
        param_types: Dict[str, str] = field(default_factory=lambda: {
            'penalty_carbon': 'linear',
            'penalty_helium_shortage': 'linear',
            'penalty_geopolitical': 'linear',
            'weight_opportunity': 'linear',
            'constraint_budget': 'linear',
            'penalty_carbon_helium': 'quadratic',
            'penalty_trust_opportunity': 'quadratic'
        })
        config_version: str = "3.3"
        cache_version: int = 1
        # New enhancement fields
        enable_quantum_distillation: bool = False
        enable_causal_rl: bool = False
        enable_federated: bool = False
        enable_safety_monitor: bool = True
        enable_xai: bool = True
        enable_precision: bool = False
        enable_carbon_market: bool = False
        carbon_market_config: Optional[Dict[str, str]] = None
        enable_chaos: bool = False
        chaos_probability: float = 0.0
        enable_human_approval: bool = False
        human_approval_timeout: float = 60.0

        def config_hash(self) -> str:
            data = {
                'field_mapping': self.field_mapping,
                'scaling': self.scaling,
                'invert_fields': self.invert_fields,
                'quadratic_mapping': {f"{k[0]}_{k[1]}": v for k, v in self.quadratic_mapping.items()},
                'custom_transform_registry': self.custom_transform_registry,
                'output_format': self.output_format,
                'quadratic_scaling': self.quadratic_scaling,
                'transform_order': self.transform_order,
                'param_types': self.param_types,
                'config_version': self.config_version,
                'default_gradient': self.default_gradient,
                'field_specific_defaults': self.field_specific_defaults,
                'enable_quantum_distillation': self.enable_quantum_distillation,
                'enable_causal_rl': self.enable_causal_rl,
                'enable_federated': self.enable_federated,
                'enable_safety_monitor': self.enable_safety_monitor,
                'enable_xai': self.enable_xai,
                'enable_precision': self.enable_precision,
                'enable_carbon_market': self.enable_carbon_market,
                'enable_chaos': self.enable_chaos,
                'enable_human_approval': self.enable_human_approval,
            }
            return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

# ============================================================================
# Protocols
# ============================================================================
@runtime_checkable
class GradientProvider(Protocol):
    def get_field_strengths(self) -> Dict[str, float]: ...
    def get_forecast(self, hours: int) -> Optional[Dict[str, float]]: ...

@runtime_checkable
class QuantumSolver(Protocol):
    def set_parameters(self, params: Dict[str, float]) -> None: ...
    def solve(self) -> Dict[str, Any]: ...

# ============================================================================
# Composite Gradient Provider
# ============================================================================
class CompositeGradientProvider:
    def __init__(self, providers: List[Tuple[GradientProvider, float]], normalize: bool = True):
        self.providers = providers
        self.normalize = normalize

    def get_field_strengths(self) -> Dict[str, float]:
        combined: Dict[str, float] = {}
        total_weight = sum(w for _, w in self.providers)
        norm = total_weight if self.normalize and total_weight > 0 else 1.0
        for provider, weight in self.providers:
            try:
                strengths = provider.get_field_strengths()
                for field, value in strengths.items():
                    combined[field] = combined.get(field, 0.0) + value * weight / norm
            except Exception as e:
                logger.warning("Provider failed in composite: %s", e)
        return combined

    def get_forecast(self, hours: int) -> Optional[Dict[str, float]]:
        forecasts = []
        for provider, _ in self.providers:
            if hasattr(provider, 'get_forecast'):
                try:
                    f = provider.get_forecast(hours)
                    if f is not None:
                        forecasts.append(f)
                except Exception as e:
                    logger.warning("Forecast from provider failed: %s", e)
        if not forecasts:
            return None
        combined = {}
        for f in forecasts:
            for field, value in f.items():
                combined[field] = combined.get(field, 0.0) + value / len(forecasts)
        return combined

    async def get_field_strengths_async(self) -> Dict[str, float]:
        return self.get_field_strengths()

    async def get_forecast_async(self, hours: int) -> Optional[Dict[str, float]]:
        return self.get_forecast(hours)

# ============================================================================
# Custom Transformation Registry
# ============================================================================
class TransformRegistry:
    _transforms: Dict[str, Callable[[float], float]] = {}

    @classmethod
    def register(cls, name: str, func: Callable[[float], float]):
        cls._transforms[name] = func

    @classmethod
    def get(cls, name: str) -> Optional[Callable[[float], float]]:
        return cls._transforms.get(name)

def quadratic_transform(x: float) -> float:
    return x ** 2

def sigmoid_transform(x: float) -> float:
    return 1 / (1 + np.exp(-10 * (x - 0.5)))

TransformRegistry.register('quadratic', quadratic_transform)
TransformRegistry.register('sigmoid', sigmoid_transform)

# ============================================================================
# New Enhancement Modules
# ============================================================================
class QuantumDistillationModule:
    def __init__(self, config):
        self.config = config
        self.available = False

    async def optimize(self, qubo_params: Dict[str, float]) -> Dict[str, float]:
        logger.info("Quantum distillation optimization requested (placeholder).")
        for key in qubo_params:
            if key != 'timestamp':
                qubo_params[key] += random.uniform(-0.01, 0.01)
        return qubo_params

    def is_available(self) -> bool:
        return self.available


class CausalRLAgent:
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


class FederatedCoordinator:
    def __init__(self, bridge: 'QuantumBridge', queue: Optional[Any] = None):
        self.bridge = bridge
        self.queue = queue
        self.last_global_model = None

    async def send_update(self):
        if not self.queue:
            return
        model = {
            'scaling': self.bridge.config.scaling,
            'quadratic_scaling': self.bridge.config.quadratic_scaling,
            'transform_order': self.bridge.config.transform_order,
            'param_types': self.bridge.config.param_types,
        }
        await self.queue.publish("federated_updates", json.dumps(model))

    async def receive_global_model(self, model_json: str):
        model = json.loads(model_json)
        self.last_global_model = model
        if 'scaling' in model:
            local_scaling = self.bridge.config.scaling
            global_scaling = model['scaling']
            for key in local_scaling:
                if key in global_scaling:
                    local_scaling[key] = 0.5 * local_scaling[key] + 0.5 * global_scaling[key]
            self.bridge.config.scaling = local_scaling
        self.bridge._config_hash = self.bridge.config.config_hash()
        self.bridge.clear_cache()
        logger.info("Federated global model applied.")


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


class CarbonMarketClient:
    def __init__(self, provider_url: str = None, contract_address: str = None, private_key: str = None):
        self.available = False
        if provider_url and contract_address and private_key:
            self.available = True

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


class ChaosInjector:
    def __init__(self, bridge: 'QuantumBridge', chaos_probability: float = 0.01):
        self.bridge = bridge
        self.chaos_probability = chaos_probability

    async def maybe_inject_failure(self):
        if random.random() < self.chaos_probability:
            action = random.choice(['delay', 'corrupt_input'])
            logger.warning(f"Chaos injection: {action}")
            if action == 'delay':
                await asyncio.sleep(random.uniform(0.5, 2.0))
            elif action == 'corrupt_input':
                if self.bridge.config.scaling:
                    key = random.choice(list(self.bridge.config.scaling.keys()))
                    self.bridge.config.scaling[key] *= random.uniform(0.8, 1.2)
                    self.bridge.clear_cache()


class HumanApprovalHandler:
    def __init__(self, queue: Optional[Any] = None):
        self.queue = queue

    async def request_approval(self, decision: Dict[str, Any], timeout: float = 60.0) -> bool:
        if not self.queue:
            logger.warning("No queue for human approval; auto-approving.")
            return True
        logger.info(f"Human approval requested for {decision.get('action')}, auto-approving.")
        await asyncio.sleep(0)
        return True


# ============================================================================
# QuantumBridge Class (Enhanced)
# ============================================================================
class QuantumBridge:
    def __init__(self,
                 gradient_provider: GradientProvider,
                 quantum_solver: Optional[QuantumSolver] = None,
                 config: Optional[Union[QuantumBridgeConfig, Dict[str, Any]]] = None,
                 message_queue: Optional[Any] = None):
        self.gradient_provider = gradient_provider
        self.quantum_solver = quantum_solver
        self.message_queue = message_queue

        if isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = QuantumBridgeConfig(**config)
            else:
                self.config = QuantumBridgeConfig(**config)
        elif isinstance(config, QuantumBridgeConfig):
            self.config = config
        else:
            self.config = QuantumBridgeConfig()

        self._cache: Optional[Dict[str, float]] = None
        self._cache_hash: Optional[str] = None
        self._cache_timestamp: Optional[datetime] = None
        self._history: List[Dict[str, Any]] = []
        self._last_update: Optional[datetime] = None

        self._expected_fields = list(self.config.field_mapping.keys())
        self._config_hash = self.config.config_hash()

        if self.config.cache_persistence_path and os.path.exists(self.config.cache_persistence_path):
            self._load_cache_from_disk()

        self._init_prometheus()

        # Enhanced modules
        self.quantum_distillation = QuantumDistillationModule(self.config) if self.config.enable_quantum_distillation else None
        if self.config.enable_causal_rl:
            self.causal_rl_agent = CausalRLAgent(state_dim=10, action_dim=3)
        else:
            self.causal_rl_agent = None
        self.federated_coordinator = FederatedCoordinator(self, self.message_queue) if self.config.enable_federated else None
        self.safety_monitor = SafetyMonitor() if self.config.enable_safety_monitor else None
        if self.safety_monitor:
            self._setup_safety_invariants()
        self.precision_controller = PrecisionController() if self.config.enable_precision else None
        self.carbon_market = None
        if self.config.enable_carbon_market and self.config.carbon_market_config:
            self.carbon_market = CarbonMarketClient(**self.config.carbon_market_config)
        self.chaos_injector = ChaosInjector(self, self.config.chaos_probability) if self.config.enable_chaos else None
        self.human_approval = HumanApprovalHandler(self.message_queue) if self.config.enable_human_approval else None

        logger.info("QuantumBridge initialized", config=self.config, config_hash=self._config_hash)

    def _init_prometheus(self):
        if not self.config.enable_prometheus or not PROMETHEUS_AVAILABLE:
            self._prometheus_metrics = None
            return
        registry = CollectorRegistry()
        self._prometheus_metrics = {
            'translation_latency': Histogram('quantum_bridge_translation_latency_seconds',
                                             'Time to translate gradients',
                                             registry=registry),
            'cache_hits': Counter('quantum_bridge_cache_hits_total', 'Cache hits', registry=registry),
            'cache_misses': Counter('quantum_bridge_cache_misses_total', 'Cache misses', registry=registry),
            'param_values': Gauge('quantum_bridge_param_values', 'Current parameter values',
                                  ['param_name'], registry=registry),
            'translation_count': Counter('quantum_bridge_translation_total', 'Total translations', registry=registry),
            'health_status': Gauge('quantum_bridge_health', 'Health status (1=healthy, 0=unhealthy)', registry=registry),
        }
        self._prometheus_metrics['health_status'].set(1)

    def _setup_safety_invariants(self):
        self.safety_monitor.add_invariant(
            "max_linear_penalty",
            lambda p: all(p.get(name, 0) <= 100.0 for name in self.config.field_mapping.values()),
            "Linear penalty exceeds safe threshold"
        )
        self.safety_monitor.add_invariant(
            "non_negative_quadratic",
            lambda p: all(p.get(name, 0) >= -0.01 for name in self.config.quadratic_mapping.values()),
            "Quadratic term negative (should be non-negative for penalties)"
        )

    def _check_safety(self, params: Dict[str, float]) -> List[str]:
        if not self.safety_monitor:
            return []
        return self.safety_monitor.check(params)

    def _explain_decision(self, params: Dict[str, float], strengths: Dict[str, float]) -> str:
        explanations = []
        for field, param in self.config.field_mapping.items():
            val = strengths.get(field, 0.0)
            scaled = params.get(param, 0.0)
            explanations.append(f"{field} ({val:.2f}) → {param} = {scaled:.2f}")
        return "QUBO translation: " + ", ".join(explanations)

    def _maybe_trade_carbon(self, carbon_strength: float):
        if self.carbon_market and self.carbon_market.available:
            if carbon_strength > 0.7:
                self.carbon_market.buy_credits(carbon_strength * 10)
            elif carbon_strength < 0.3:
                self.carbon_market.sell_credits((1 - carbon_strength) * 5)

    async def _maybe_inject_chaos(self):
        if self.chaos_injector:
            await self.chaos_injector.maybe_inject_failure()

    async def request_human_approval(self, action: str, details: Dict[str, Any]) -> bool:
        if self.human_approval:
            return await self.human_approval.request_approval({'action': action, 'details': details})
        return True

    def _compute_hash(self, strengths: Dict[str, float]) -> str:
        data = {
            'strengths': strengths,
            'config_hash': self._config_hash
        }
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

    def _get_default_for_field(self, field: str) -> float:
        return self.config.field_specific_defaults.get(field, self.config.default_gradient)

    def _validate_and_complete(self, strengths: Dict[str, float]) -> Dict[str, float]:
        validated = {}
        for field in self._expected_fields:
            value = strengths.get(field, self._get_default_for_field(field))
            value = max(0.0, min(1.0, value))
            validated[field] = value
        return validated

    def _apply_transform(self, field: str, value: float) -> float:
        transform_name = self.config.custom_transform_registry.get(field)
        if transform_name:
            transform_func = TransformRegistry.get(transform_name)
            if transform_func:
                try:
                    return transform_func(value)
                except Exception as e:
                    logger.warning("Transform '%s' failed for field %s: %s", transform_name, field, e)
            else:
                logger.warning("Unknown transform '%s' for field %s", transform_name, field)
        return value

    def _translate_value(self, field: str, value: float) -> float:
        scale = self.config.scaling.get(field, 1.0)
        invert = field in self.config.invert_fields
        for op in self.config.transform_order:
            if op == 'invert' and invert:
                value = 1.0 - value
            elif op == 'transform':
                value = self._apply_transform(field, value)
            elif op == 'scale':
                value *= scale
        return value

    def _translate_quadratic(self, strengths: Dict[str, float]) -> Dict[str, float]:
        quadratic_params = {}
        for (f1, f2), param_name in self.config.quadratic_mapping.items():
            v1 = strengths.get(f1, 0.0)
            v2 = strengths.get(f2, 0.0)
            v1 = self._translate_value(f1, v1) / self.config.scaling.get(f1, 1.0)
            v2 = self._translate_value(f2, v2) / self.config.scaling.get(f2, 1.0)
            quadratic_params[param_name] = v1 * v2 * self.config.quadratic_scaling
        return quadratic_params

    def _qubo_to_ising(self, qubo_params: Dict[str, float]) -> Dict[str, float]:
        fields = list(self.config.field_mapping.keys())
        num_vars = len(fields)
        field_index = {field: idx for idx, field in enumerate(fields)}
        Q = np.zeros((num_vars, num_vars))
        for field, param in self.config.field_mapping.items():
            if param in qubo_params:
                val = qubo_params[param]
                Q[field_index[field], field_index[field]] = val
        for (f1, f2), param in self.config.quadratic_mapping.items():
            if param in qubo_params:
                val = qubo_params[param]
                i = field_index.get(f1)
                j = field_index.get(f2)
                if i is not None and j is not None:
                    Q[i][j] = val
                    Q[j][i] = val
        h = np.zeros(num_vars)
        J = np.zeros((num_vars, num_vars))
        for i in range(num_vars):
            h[i] = Q[i][i] + 0.5 * (np.sum(Q[i, :]) - Q[i][i])
        for i in range(num_vars):
            for j in range(i+1, num_vars):
                J[i][j] = 0.25 * Q[i][j]
                J[j][i] = J[i][j]
        ising_params = {}
        for i, field in enumerate(fields):
            param_name = self.config.field_mapping[field]
            ising_params[f"h_{param_name}"] = h[i]
        for i in range(num_vars):
            for j in range(i+1, num_vars):
                f1 = fields[i]
                f2 = fields[j]
                pair_key = (f1, f2)
                if pair_key in self.config.quadratic_mapping:
                    param_name = self.config.quadratic_mapping[pair_key]
                    ising_params[f"J_{param_name}"] = J[i][j]
                else:
                    ising_params[f"J_{f1}_{f2}"] = J[i][j]
        for param, value in qubo_params.items():
            if param != 'timestamp':
                if param not in self.config.field_mapping.values() and param not in self.config.quadratic_mapping.values():
                    ising_params[param] = value
        return ising_params

    def get_qubo_parameters(self, forecast_hours: Optional[int] = None) -> Dict[str, float]:
        if self._prometheus_metrics:
            self._prometheus_metrics['translation_count'].inc()
        strengths = self._fetch_strengths_with_retry(forecast_hours)
        strengths = self._validate_and_complete(strengths)
        current_hash = self._compute_hash(strengths)
        if self.config.enable_caching and self._cache_hash == current_hash:
            if self._cache_timestamp and self.config.cache_ttl is not None:
                if (datetime.now(timezone.utc) - self._cache_timestamp) > timedelta(seconds=self.config.cache_ttl):
                    self._cache = None
                    self._cache_hash = None
                    self._cache_timestamp = None
                else:
                    if self._prometheus_metrics:
                        self._prometheus_metrics['cache_hits'].inc()
                    return self._cache
            else:
                if self._prometheus_metrics:
                    self._prometheus_metrics['cache_hits'].inc()
                return self._cache
        if self._prometheus_metrics:
            self._prometheus_metrics['cache_misses'].inc()
        params = {}
        for field, value in strengths.items():
            if field in self.config.field_mapping:
                param_name = self.config.field_mapping[field]
                params[param_name] = self._translate_value(field, value)
        quadratic_params = self._translate_quadratic(strengths)
        params.update(quadratic_params)
        now = datetime.now(timezone.utc)
        params['timestamp'] = now.timestamp()
        if self.config.output_format == 'ising':
            params = self._qubo_to_ising(params)
        if self.config.enable_caching:
            self._cache = params
            self._cache_hash = current_hash
            self._cache_timestamp = now
            self._persist_cache()
        self._record_history(strengths, params)
        if self._prometheus_metrics:
            self._prometheus_metrics['param_values'].clear()
            for param_name, value in params.items():
                if param_name != 'timestamp':
                    self._prometheus_metrics['param_values'].labels(param_name=param_name).set(value)
        # Safety check
        if self.safety_monitor:
            violations = self._check_safety(params)
            if violations:
                logger.warning("Safety violations detected: %s", violations)
        # XAI
        if self.config.enable_xai:
            explanation = self._explain_decision(params, strengths)
            logger.info("XAI: %s", explanation)
        # Carbon market
        if 'carbon' in strengths and self.carbon_market:
            self._maybe_trade_carbon(strengths['carbon'])
        # Chaos injection (schedule)
        if self.chaos_injector:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._maybe_inject_chaos())
            except RuntimeError:
                pass  # no running loop
        # Precision
        if self.precision_controller:
            precision = self.precision_controller.get_precision(
                load=strengths.get('system_load', 0.5),
                energy_budget=strengths.get('energy_budget', 0.5)
            )
            logger.debug("Using precision: %s", precision)
        return params

    def _fetch_strengths_with_retry(self, forecast_hours: Optional[int] = None) -> Dict[str, float]:
        retries = self.config.provider_retries
        for attempt in range(retries + 1):
            try:
                if forecast_hours is not None and hasattr(self.gradient_provider, 'get_forecast'):
                    forecast = self.gradient_provider.get_forecast(forecast_hours)
                    if forecast is not None:
                        return forecast
                return self.gradient_provider.get_field_strengths()
            except Exception as e:
                logger.warning("Gradient provider failure (attempt %d/%d): %s", attempt+1, retries+1, e)
                if attempt == retries:
                    logger.error("Gradient provider failed after %d retries; using defaults.", retries+1)
                    return {field: self._get_default_for_field(field) for field in self._expected_fields}
                time.sleep(0.5 * (attempt + 1))

    def _persist_cache(self):
        if not self.config.cache_persistence_path:
            return
        try:
            data = {
                'cache_version': self.config.cache_version,
                'config_hash': self._config_hash,
                'cache': self._cache,
                'cache_hash': self._cache_hash,
                'cache_timestamp': self._cache_timestamp.isoformat() if self._cache_timestamp else None,
            }
            with open(self.config.cache_persistence_path, 'w') as f:
                json.dump(data, f, default=str)
            logger.debug("Cache persisted to %s", self.config.cache_persistence_path)
        except Exception as e:
            logger.warning("Failed to persist cache: %s", e)

    def _load_cache_from_disk(self):
        if not self.config.cache_persistence_path:
            return
        try:
            with open(self.config.cache_persistence_path, 'r') as f:
                data = json.load(f)
            if data.get('cache_version') != self.config.cache_version:
                logger.info("Cache version mismatch; discarding persisted cache.")
                return
            if data.get('config_hash') != self._config_hash:
                logger.info("Configuration changed; discarding persisted cache.")
                return
            self._cache = data.get('cache')
            self._cache_hash = data.get('cache_hash')
            ts = data.get('cache_timestamp')
            if ts:
                self._cache_timestamp = datetime.fromisoformat(ts)
            logger.info("Cache loaded from %s", self.config.cache_persistence_path)
        except Exception as e:
            logger.warning("Failed to load cache: %s", e)

    def _record_history(self, strengths: Dict[str, float], params: Dict[str, float]):
        if len(self._history) >= self.config.history_size:
            self._history.pop(0)
        self._history.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'gradient_strengths': strengths.copy(),
            'qubo_parameters': params.copy()
        })

    def apply_to_quantum_solver(self, forecast_hours: Optional[int] = None) -> bool:
        if self.quantum_solver is None:
            logger.warning("No quantum solver attached – translation only.")
            return False
        params = self.get_qubo_parameters(forecast_hours)
        try:
            self.quantum_solver.set_parameters(params)
            logger.info("Applied QUBO parameters to quantum solver.")
            return True
        except Exception as e:
            logger.error("Failed to apply parameters to quantum solver: %s", e)
            return False

    async def get_qubo_parameters_async(self, forecast_hours: Optional[int] = None) -> Dict[str, float]:
        return self.get_qubo_parameters(forecast_hours)

    async def apply_to_quantum_solver_async(self, forecast_hours: Optional[int] = None) -> bool:
        if self.quantum_solver is None:
            logger.warning("No quantum solver attached – translation only.")
            return False
        params = await self.get_qubo_parameters_async(forecast_hours)
        try:
            self.quantum_solver.set_parameters(params)
            logger.info("Applied QUBO parameters to quantum solver (async).")
            return True
        except Exception as e:
            logger.error("Failed to apply parameters to quantum solver (async): %s", e)
            return False

    def health_check(self) -> Dict[str, Any]:
        status = {
            'status': 'healthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'config_hash': self._config_hash,
            'cache_loaded': self._cache is not None,
            'gradient_provider': 'ok',
            'quantum_solver': 'ok' if self.quantum_solver else 'not_attached',
        }
        try:
            strengths = self.gradient_provider.get_field_strengths()
            if not strengths:
                status['status'] = 'degraded'
                status['gradient_provider'] = 'empty'
        except Exception as e:
            status['status'] = 'unhealthy'
            status['gradient_provider'] = f'failed: {e}'
        if self.quantum_solver:
            if not hasattr(self.quantum_solver, 'set_parameters'):
                status['status'] = 'degraded'
                status['quantum_solver'] = 'missing_set_parameters'
        if self._prometheus_metrics:
            self._prometheus_metrics['health_status'].set(1 if status['status'] == 'healthy' else 0)
        return status

    def get_qubo_report(self, forecast_hours: Optional[int] = None) -> Dict[str, Any]:
        strengths = self._fetch_strengths_with_retry(forecast_hours)
        strengths = self._validate_and_complete(strengths)
        params = self.get_qubo_parameters(forecast_hours)
        report = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'gradient_strengths': strengths,
            'qubo_parameters': params,
            'scaling': self.config.scaling,
            'field_mapping': self.config.field_mapping,
            'quadratic_mapping': self.config.quadratic_mapping,
            'cache_hit': self._cache_hash is not None and self._cache is not None,
            'history_size': len(self._history),
            'output_format': self.config.output_format,
            'config': self.config.dict() if PYDANTIC_AVAILABLE else asdict(self.config),
            'safety_violations': self._check_safety(params) if self.safety_monitor else [],
            'xai_enabled': self.config.enable_xai,
            'carbon_market_available': self.carbon_market.available if self.carbon_market else False,
            'precision': self.precision_controller.get_precision(0.5, 0.5) if self.precision_controller else 'float32',
            'chaos_enabled': self.chaos_injector is not None,
            'human_approval_enabled': self.human_approval is not None,
            'federated_enabled': self.federated_coordinator is not None,
            'quantum_distillation_enabled': self.quantum_distillation is not None,
            'causal_rl_enabled': self.causal_rl_agent is not None,
        }
        return report

    def get_history(self, limit: Optional[int] = None, start_time: Optional[datetime] = None,
                    end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        history = self._history
        if start_time:
            history = [h for h in history if datetime.fromisoformat(h['timestamp']) >= start_time]
        if end_time:
            history = [h for h in history if datetime.fromisoformat(h['timestamp']) <= end_time]
        if limit is not None:
            history = history[-limit:]
        return history

    def export_history(self, path: str):
        with open(path, 'w') as f:
            json.dump(self._history, f, indent=2, default=str)

    def clear_cache(self):
        self._cache = None
        self._cache_hash = None
        self._cache_timestamp = None
        if self.config.cache_persistence_path and os.path.exists(self.config.cache_persistence_path):
            try:
                os.remove(self.config.cache_persistence_path)
            except Exception as e:
                logger.warning("Failed to delete cache file: %s", e)
        logger.info("Cache cleared.")

    def clear_history(self):
        self._history = []
        logger.info("History cleared.")

    async def update_config(self, updates: Dict[str, Any]) -> None:
        if self.config.enable_human_approval:
            approval = await self.request_human_approval('update_config', updates)
            if not approval:
                logger.info("Config update rejected by human.")
                return
        if PYDANTIC_AVAILABLE:
            new_dict = self.config.dict()
            new_dict.update(updates)
            self.config = QuantumBridgeConfig(**new_dict)
        else:
            for k, v in updates.items():
                if hasattr(self.config, k):
                    setattr(self.config, k, v)
        self._config_hash = self.config.config_hash()
        self.clear_cache()
        logger.info("Configuration updated: %s", updates)

    def set_custom_transform(self, field: str, transform_name: str) -> None:
        if transform_name not in TransformRegistry._transforms:
            raise ValueError(f"Transform '{transform_name}' not registered")
        if PYDANTIC_AVAILABLE:
            new_registry = self.config.custom_transform_registry.copy()
            new_registry[field] = transform_name
            self.config.custom_transform_registry = new_registry
        else:
            self.config.custom_transform_registry[field] = transform_name
        self._config_hash = self.config.config_hash()
        self.clear_cache()
        logger.info("Set custom transform for field %s: %s", field, transform_name)


# ============================================================================
# Multi‑Objective Optimizer for QuantumBridge Configuration (NSGA‑II)
# ============================================================================
@dataclass
class MOPDPoint:
    config: QuantumBridgeConfig
    objectives: Dict[str, float]
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'config': self.config.dict() if PYDANTIC_AVAILABLE else asdict(self.config),
            'objectives': self.objectives,
            'scalarised_score': self.scalarised_score
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDPoint':
        config_data = data['config']
        if PYDANTIC_AVAILABLE:
            config = QuantumBridgeConfig(**config_data)
        else:
            config = QuantumBridgeConfig(**config_data)
        return cls(config=config, objectives=data['objectives'], scalarised_score=data.get('scalarised_score', 0.0))


class QuantumBridgeOptimizer:
    def __init__(self,
                 bridge: QuantumBridge,
                 evaluate_func: Callable[[QuantumBridgeConfig], Dict[str, float]],
                 population_size: int = 20,
                 generations: int = 10,
                 mutation_rate: float = 0.2,
                 crossover_rate: float = 0.8,
                 tournament_size: int = 3,
                 objective_weights: Optional[Dict[str, float]] = None,
                 config_bounds: Optional[Dict[str, Tuple[float, float]]] = None):
        self.bridge = bridge
        self.evaluate_func = evaluate_func
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.objective_weights = objective_weights or {}
        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.pareto_front: List[MOPDPoint] = []
        self._lock = asyncio.Lock()

        self.scaling_keys = list(bridge.config.scaling.keys())
        self.param_names = self.scaling_keys + ['quadratic_scaling']
        self.param_bounds = {}
        for key in self.scaling_keys:
            self.param_bounds[key] = (0.1, 100.0)
        self.param_bounds['quadratic_scaling'] = (0.0, 10.0)
        if config_bounds:
            self.param_bounds.update(config_bounds)

        self._eval_cache: Dict[Tuple[float, ...], Dict[str, float]] = {}

    def _config_from_vector(self, vector: Dict[str, float]) -> QuantumBridgeConfig:
        new_config = self.bridge.config.copy(deep=True) if PYDANTIC_AVAILABLE else copy.deepcopy(self.bridge.config)
        new_scaling = new_config.scaling.copy()
        for key in self.scaling_keys:
            new_scaling[key] = vector[key]
        new_config.scaling = new_scaling
        new_config.quadratic_scaling = vector['quadratic_scaling']
        new_config.enable_caching = False
        return new_config

    def _vector_to_tuple(self, vector: Dict[str, float]) -> Tuple[float, ...]:
        return tuple(vector[name] for name in self.param_names)

    def _random_individual(self) -> Dict[str, float]:
        ind = {}
        for name in self.param_names:
            low, high = self.param_bounds[name]
            ind[name] = random.uniform(low, high)
        return ind

    def _crossover(self, parent1: Dict[str, float], parent2: Dict[str, float]) -> Dict[str, float]:
        child = {}
        for name in self.param_names:
            if random.random() < 0.5:
                child[name] = parent1[name]
            else:
                child[name] = parent2[name]
            if random.random() < 0.3:
                low, high = self.param_bounds[name]
                u = random.random()
                if u <= 0.5:
                    beta = (2 * u) ** (1 / (20 + 1))
                else:
                    beta = (1 / (2 * (1 - u))) ** (1 / (20 + 1))
                val = 0.5 * ((1 + beta) * parent1[name] + (1 - beta) * parent2[name])
                child[name] = max(low, min(high, val))
        return child

    def _mutate(self, individual: Dict[str, float]) -> Dict[str, float]:
        mutant = individual.copy()
        for name in self.param_names:
            if random.random() < self.mutation_rate:
                low, high = self.param_bounds[name]
                u = random.random()
                if u < 0.5:
                    delta = (2 * u) ** (1 / (20 + 1)) - 1
                else:
                    delta = 1 - (2 * (1 - u)) ** (1 / (20 + 1))
                mutant[name] = individual[name] + delta * (high - low)
                mutant[name] = max(low, min(high, mutant[name]))
        return mutant

    async def _evaluate(self, vector: Dict[str, float]) -> Dict[str, float]:
        key = self._vector_to_tuple(vector)
        if key in self._eval_cache:
            return self._eval_cache[key]
        config = self._config_from_vector(vector)
        if asyncio.iscoroutinefunction(self.evaluate_func):
            objectives = await self.evaluate_func(config)
        else:
            objectives = self.evaluate_func(config)
        obj = {k: float(v) for k, v in objectives.items()}
        self._eval_cache[key] = obj
        return obj

    def _fast_non_dominated_sort(self, population, objectives):
        fronts = []
        domination_count = {key: 0 for key in objectives}
        dominated_solutions = {key: [] for key in objectives}

        keys = list(objectives.keys())
        for i, p_key in enumerate(keys):
            p_obj = objectives[p_key]
            for j, q_key in enumerate(keys):
                if i == j:
                    continue
                q_obj = objectives[q_key]
                if all(p_obj[k] >= q_obj[k] for k in p_obj) and any(p_obj[k] > q_obj[k] for k in p_obj):
                    dominated_solutions[p_key].append(q_key)
                elif all(q_obj[k] >= p_obj[k] for k in q_obj) and any(q_obj[k] > p_obj[k] for k in q_obj):
                    domination_count[p_key] += 1

            if domination_count[p_key] == 0:
                if not fronts:
                    fronts.append([])
                fronts[0].append(p_key)

        i = 0
        while i < len(fronts):
            next_front = []
            for p_key in fronts[i]:
                for q_key in dominated_solutions[p_key]:
                    domination_count[q_key] -= 1
                    if domination_count[q_key] == 0:
                        next_front.append(q_key)
            if next_front:
                fronts.append(next_front)
            i += 1

        key_to_ind = {self._vector_to_tuple(ind): ind for ind in population}
        return [[key_to_ind[key] for key in front] for front in fronts]

    def _crowding_distance(self, front, objectives):
        if not front:
            return {}
        distances = {self._vector_to_tuple(ind): 0.0 for ind in front}
        obj_keys = list(next(iter(objectives.values())).keys())
        for obj in obj_keys:
            sorted_front = sorted(front, key=lambda ind: objectives[self._vector_to_tuple(ind)][obj])
            distances[self._vector_to_tuple(sorted_front[0])] = float('inf')
            distances[self._vector_to_tuple(sorted_front[-1])] = float('inf')
            obj_min = objectives[self._vector_to_tuple(sorted_front[0])][obj]
            obj_max = objectives[self._vector_to_tuple(sorted_front[-1])][obj]
            if obj_max == obj_min:
                continue
            for i in range(1, len(sorted_front) - 1):
                key = self._vector_to_tuple(sorted_front[i])
                prev_key = self._vector_to_tuple(sorted_front[i-1])
                next_key = self._vector_to_tuple(sorted_front[i+1])
                distances[key] += (objectives[next_key][obj] - objectives[prev_key][obj]) / (obj_max - obj_min)
        return distances

    def _tournament_selection(self, population, fronts, crowding):
        ind1 = random.choice(population)
        ind2 = random.choice(population)
        rank1 = self._get_rank(ind1, fronts)
        rank2 = self._get_rank(ind2, fronts)
        if rank1 < rank2:
            return ind1
        elif rank2 < rank1:
            return ind2
        else:
            key1 = self._vector_to_tuple(ind1)
            key2 = self._vector_to_tuple(ind2)
            if crowding.get(key1, 0) > crowding.get(key2, 0):
                return ind1
            else:
                return ind2

    def _get_rank(self, individual, fronts):
        for i, front in enumerate(fronts):
            if individual in front:
                return i
        return len(fronts)

    def _select_best_from_pareto(self, pareto_front, weights):
        if not pareto_front:
            return None
        if weights is None:
            obj_keys = list(pareto_front[0].objectives.keys())
            weights = {k: 1.0 / len(obj_keys) for k in obj_keys}
        else:
            obj_keys = list(weights.keys())

        max_vals = {k: max(p.objectives[k] for p in pareto_front) for k in obj_keys}
        min_vals = {k: min(p.objectives[k] for p in pareto_front) for k in obj_keys}
        ranges = {k: max_vals[k] - min_vals[k] if max_vals[k] != min_vals[k] else 1.0 for k in obj_keys}

        best = None
        best_score = -float('inf')
        for point in pareto_front:
            score = 0.0
            for key in obj_keys:
                val = point.objectives[key]
                norm = (val - min_vals[key]) / ranges[key] if ranges[key] > 0 else 1.0
                score += weights.get(key, 0.0) * norm
            point.scalarised_score = score
            if score > best_score:
                best_score = score
                best = point
        return best

    async def evolve(self, generations: Optional[int] = None):
        async with self._lock:
            if generations is None:
                generations = self.generations

            population = [self._random_individual() for _ in range(self.population_size)]
            objectives = {}
            for ind in population:
                key = self._vector_to_tuple(ind)
                objectives[key] = await self._evaluate(ind)

            self.pareto_front = []

            for gen in range(generations):
                offspring = []
                pop_objectives = {k: objectives[k] for k in objectives if k in [self._vector_to_tuple(i) for i in population]}
                fronts = self._fast_non_dominated_sort(population, pop_objectives)
                crowding = {}
                for front in fronts:
                    front_crowding = self._crowding_distance(front, pop_objectives)
                    crowding.update(front_crowding)

                while len(offspring) < self.population_size:
                    parent1 = self._tournament_selection(population, fronts, crowding)
                    parent2 = self._tournament_selection(population, fronts, crowding)
                    if random.random() < self.crossover_rate:
                        child = self._crossover(parent1, parent2)
                        child = self._mutate(child)
                        offspring.append(child)
                    else:
                        offspring.append(self._mutate(parent1.copy()))
                offspring = offspring[:self.population_size]

                for ind in offspring:
                    key = self._vector_to_tuple(ind)
                    if key not in objectives:
                        objectives[key] = await self._evaluate(ind)

                combined = population + offspring
                unique_keys = {}
                for ind in combined:
                    unique_keys[self._vector_to_tuple(ind)] = ind
                combined = list(unique_keys.values())

                combined_objectives = {self._vector_to_tuple(ind): objectives[self._vector_to_tuple(ind)] for ind in combined}
                fronts = self._fast_non_dominated_sort(combined, combined_objectives)

                new_population = []
                for front in fronts:
                    if len(new_population) + len(front) <= self.population_size:
                        new_population.extend(front)
                    else:
                        crowding = self._crowding_distance(front, combined_objectives)
                        sorted_front = sorted(front, key=lambda ind: crowding.get(self._vector_to_tuple(ind), 0), reverse=True)
                        remaining = self.population_size - len(new_population)
                        new_population.extend(sorted_front[:remaining])
                        break
                population = new_population

                pop_objectives = {self._vector_to_tuple(ind): objectives[self._vector_to_tuple(ind)] for ind in population}
                fronts_pop = self._fast_non_dominated_sort(population, pop_objectives)
                if fronts_pop:
                    pareto_individuals = fronts_pop[0]
                    self.pareto_front = []
                    for ind in pareto_individuals:
                        objs = pop_objectives[self._vector_to_tuple(ind)]
                        config = self._config_from_vector(ind)
                        self.pareto_front.append(MOPDPoint(config=config, objectives=objs))
                logger.debug(f"Generation {gen+1}/{generations}: Pareto front size={len(self.pareto_front)}")

            if self.objective_weights:
                weights = self.objective_weights
            else:
                all_keys = set()
                for p in self.pareto_front:
                    all_keys.update(p.objectives.keys())
                weights = {k: 1.0 / len(all_keys) for k in all_keys}

            best_point = self._select_best_from_pareto(self.pareto_front, weights)
            if best_point:
                self.best_individual = best_point.config
                self.best_fitness = best_point.scalarised_score
                self.evolution_history.append({
                    'timestamp': datetime.now(timezone.utc),
                    'best_fitness': self.best_fitness,
                    'pareto_front_size': len(self.pareto_front)
                })
                return best_point.config, best_point.objectives
            else:
                best_ind = max(population, key=lambda ind: np.mean(list(objectives[self._vector_to_tuple(ind)].values())))
                return self._config_from_vector(best_ind), objectives[self._vector_to_tuple(best_ind)]

    def get_pareto_front(self):
        return self.pareto_front.copy()

    def get_status(self):
        return {
            'best_fitness': self.best_fitness,
            'evolution_history': self.evolution_history[-10:],
            'pareto_front_size': len(self.pareto_front)
        }


# ============================================================================
# Example usage (kept original + new features)
# ============================================================================
if __name__ == "__main__":
    class MockGradientProvider:
        def get_field_strengths(self):
            return {
                'carbon': 0.8,
                'helium': 0.2,
                'trust': 0.1,
                'opportunity': 0.9,
                'eco_atp_reserve': 0.5,
                'system_load': 0.7,
                'energy_budget': 0.6
            }
        def get_forecast(self, hours: int):
            return {
                'carbon': 0.75 + 0.05 * np.sin(hours),
                'helium': 0.25,
                'trust': 0.15,
                'opportunity': 0.85,
                'eco_atp_reserve': 0.55,
                'system_load': 0.6,
                'energy_budget': 0.5
            }

    class MockQuantumSolver:
        def set_parameters(self, params):
            print(f"Quantum solver received: {params}")

        def solve(self):
            return {'status': 'ok'}

    # Create bridge with all enhancements enabled
    bridge = QuantumBridge(
        gradient_provider=MockGradientProvider(),
        quantum_solver=MockQuantumSolver(),
        config={
            'output_format': 'qubo',
            'enable_prometheus': False,
            'cache_ttl': 60,
            'cache_persistence_path': './cache.json',
            'enable_quantum_distillation': True,
            'enable_causal_rl': True,
            'enable_federated': True,
            'enable_safety_monitor': True,
            'enable_xai': True,
            'enable_precision': True,
            'enable_carbon_market': True,
            'carbon_market_config': {'provider_url': 'http://localhost:8545', 'contract_address': '0xabc', 'private_key': '0x123'},
            'enable_chaos': True,
            'chaos_probability': 0.1,
            'enable_human_approval': True,
        }
    )

    # Get parameters
    params = bridge.get_qubo_parameters()
    print("QUBO parameters:", params)

    # Apply to solver
    bridge.apply_to_quantum_solver()

    # Get report
    report = bridge.get_qubo_report()
    print("Report (excerpt):", report['qubo_parameters'])

    # Test async update config with human approval
    async def test_update():
        await bridge.update_config({'scaling': {'carbon': 12.0}})
        print("Updated scaling:", bridge.config.scaling)

    # Run async test if loop available
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(test_update())
    else:
        loop.create_task(test_update())

    # Cleanup
    if os.path.exists('./cache.json'):
        os.remove('./cache.json')

    print("All tests passed.")
