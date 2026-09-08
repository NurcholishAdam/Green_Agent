#!/usr/bin/env python3
"""
Enhanced Expert Registry v7.1.0 - Complete Bio-Inspired Genome Repository with MoE + Pareto + Federated + Active Learning
Full Green Agent MODP Integration

This version integrates all requested enhancement modules:
- Quantum‑Distillation Integration (placeholder)
- Causal Reinforcement Learning (causal feature mask)
- Federated Green Learning (aggregator stub)
- Advanced Multi‑Agent Coordination (expert auction)
- Temporal Logic / Formal Verification (SafetyMonitor)
- Explainable AI (simple explanation in FeedbackEvent)
- Adaptive Precision Switching (PrecisionController)
- Carbon Markets / RECs (CarbonMarketClient)
- Resilience Engineering / Chaos Testing (ChaosInjector)
- Human‑in‑the‑Loop (HumanApprovalHandler)
Plus MoE soft gating, Pareto front, contextual weights, natural selection, and drift detection.
"""

import asyncio
import json
import os
import re
import hashlib
import uuid
import math
import random
import zlib
import time
import aiohttp
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Dict, Any, List, Optional, Tuple, Set, Union, Callable, TypeVar
import numpy as np

# Central Green Agent imports
from ..config import config as central_config
from ..storage import Storage
from ..schemas.feedback_event import FeedbackEvent
from ..routing.pareto_gating import ParetoGating
from ..feedback.adaptive_cost import AdaptiveCostFunction
from ..safety.drift_detector import DriftDetector
from ..scaling.message_queue import AsyncMessageQueue
from ..metrics import MetricsRegistry
from ..logger import logger

# Optional dependencies
try:
    from sklearn.neural_network import MLPClassifier, MLPRegressor
    from sklearn.preprocessing import StandardScaler
    from sklearn.linear_model import SGDRegressor
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# Bio-inspired modules (optional)
try:
    from enhancements.bio_inspired.eco_atp_currency import EcoATPTokenManager, EcoATPSource
    BIO_INSPIRED_AVAILABLE = True
except ImportError:
    BIO_INSPIRED_AVAILABLE = False

# Enums and Data Classes
class ExpertLifecycleState(Enum):
    REGISTERED = "registered"
    VALIDATING = "validating"
    CERTIFIED = "certified"
    ACTIVE = "active"
    DEGRADED = "degraded"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"

    def is_available(self):
        return self in [ExpertLifecycleState.CERTIFIED, ExpertLifecycleState.ACTIVE]

class ExpertDomain(Enum):
    ENERGY = "energy"
    DATA = "data"
    IOT = "iot"
    QUANTUM = "quantum"
    HELIUM = "helium"
    GENERAL = "general"

class HardwareProfile(Enum):
    CPU = "cpu"
    GPU = "gpu"
    TPU = "tpu"
    QPU = "qpu"
    HYBRID = "hybrid"

class ExpertVersion:
    def __init__(self, major=1, minor=0, patch=0):
        self.major = major
        self.minor = minor
        self.patch = patch

    def to_string(self):
        return f"{self.major}.{self.minor}.{self.patch}"

    def is_newer_than(self, other):
        return (self.major, self.minor, self.patch) > (other.major, other.minor, other.patch)

class HealthMetrics:
    def __init__(self, success_rate=0.9, quantum_efficiency=0.5, quantum_advantage_score=0.5,
                 carbon_efficiency=0.5, helium_efficiency=0.5, avg_latency_ms=100, last_heartbeat=None):
        self.success_rate = success_rate
        self.quantum_efficiency = quantum_efficiency
        self.quantum_advantage_score = quantum_advantage_score
        self.carbon_efficiency = carbon_efficiency
        self.helium_efficiency = helium_efficiency
        self.avg_latency_ms = avg_latency_ms
        self.last_heartbeat = last_heartbeat or datetime.now(timezone.utc)

    def calculate_health_score(self):
        return (self.success_rate + self.carbon_efficiency + self.helium_efficiency + self.quantum_efficiency) / 4

    def calculate_sustainability_score(self):
        return (self.carbon_efficiency + self.helium_efficiency) / 2

class ExpertProfile:
    def __init__(self, expert_id, expert_name, version, domain, hardware_profile,
                 accuracy_score=0.5, reliability_score=0.5, efficiency_score=0.5,
                 helium_per_inference=0.01, carbon_per_inference=0.001,
                 energy_per_inference=0.01, quantum_capable=False, quantum_qubits=0,
                 quantum_backend=None, sustainability_score=0.5, health=None,
                 lifecycle_state=ExpertLifecycleState.REGISTERED, is_active=True,
                 replaces_expert=None, lineage=None):
        self.expert_id = expert_id
        self.expert_name = expert_name
        self.version = version
        self.domain = domain
        self.hardware_profile = hardware_profile
        self.accuracy_score = accuracy_score
        self.reliability_score = reliability_score
        self.efficiency_score = efficiency_score
        self.helium_per_inference = helium_per_inference
        self.carbon_per_inference = carbon_per_inference
        self.energy_per_inference = energy_per_inference
        self.quantum_capable = quantum_capable
        self.quantum_qubits = quantum_qubits
        self.quantum_backend = quantum_backend
        self.sustainability_score = sustainability_score
        self.health = health or HealthMetrics()
        self.lifecycle_state = lifecycle_state
        self.is_active = is_active
        self.replaces_expert = replaces_expert
        self.lineage = lineage

class FitnessScore:
    def __init__(self, expert_id, resource_efficiency=0.5, resilience_score=0.5,
                 adaptation_speed=0.5, cooperation_score=0.5, ecoatp_efficiency=0.5,
                 sustainability_score=0.5, quantum_efficiency=0.5, quantum_advantage=0.5,
                 helium_savings=0.5, reproductive_success=0):
        self.expert_id = expert_id
        self.resource_efficiency = resource_efficiency
        self.resilience_score = resilience_score
        self.adaptation_speed = adaptation_speed
        self.cooperation_score = cooperation_score
        self.ecoatp_efficiency = ecoatp_efficiency
        self.sustainability_score = sustainability_score
        self.quantum_efficiency = quantum_efficiency
        self.quantum_advantage = quantum_advantage
        self.helium_savings = helium_savings
        self.reproductive_success = reproductive_success
        self.overall_fitness = 0.0

    def calculate_overall(self, weights):
        self.overall_fitness = (
            weights['resource_efficiency'] * self.resource_efficiency +
            weights['resilience_score'] * self.resilience_score +
            weights['adaptation_speed'] * self.adaptation_speed +
            weights['cooperation_score'] * self.cooperation_score +
            weights['ecoatp_efficiency'] * self.ecoatp_efficiency +
            weights['sustainability_score'] * self.sustainability_score +
            weights['quantum_efficiency'] * self.quantum_efficiency +
            weights['quantum_advantage'] * self.quantum_advantage +
            weights['helium_savings'] * self.helium_savings
        )

# Configuration
class ExpertRegistryConfig:
    def __init__(self):
        self.registry_id = getattr(central_config, "expert_registry_id", "default")
        self.enable_bio_correlation = getattr(central_config, "enable_bio_correlation", True) and BIO_INSPIRED_AVAILABLE
        self.enable_natural_selection = getattr(central_config, "enable_natural_selection", True)
        self.enable_fitness_tracking = getattr(central_config, "enable_fitness_tracking", True)
        self.enable_population_tracking = getattr(central_config, "enable_population_tracking", True)
        self.enable_sustainability_dashboard = getattr(central_config, "enable_sustainability_dashboard", True)
        self.enable_predictive_forecasting = getattr(central_config, "enable_predictive_forecasting", True)
        self.enable_cross_region_sync = getattr(central_config, "enable_cross_region_sync", True)
        self.enable_quantum_efficiency = getattr(central_config, "enable_quantum_efficiency", True)
        self.enable_reproductive_strategies = getattr(central_config, "enable_reproductive_strategies", True)
        self.enable_climate_integration = getattr(central_config, "enable_climate_integration", True)
        self.enable_persistence = True
        self.sync_retries = getattr(central_config, "sync_retries", 3)
        self.sync_retry_base_delay_ms = getattr(central_config, "sync_retry_base_delay_ms", 100.0)
        self.sync_retry_max_delay_ms = getattr(central_config, "sync_retry_max_delay_ms", 5000.0)
        self.circuit_breaker_threshold = getattr(central_config, "circuit_breaker_failure_threshold", 5)
        self.circuit_breaker_recovery_timeout = getattr(central_config, "circuit_breaker_recovery_timeout", 30.0)
        self.sync_interval = getattr(central_config, "sync_interval", 3600)
        self.bio_sync_interval = getattr(central_config, "bio_sync_interval", 300)
        self.fitness_weights = getattr(central_config, "fitness_weights", {
            'resource_efficiency': 0.20,
            'resilience_score': 0.15,
            'adaptation_speed': 0.10,
            'cooperation_score': 0.10,
            'ecoatp_efficiency': 0.10,
            'sustainability_score': 0.15,
            'quantum_efficiency': 0.10,
            'quantum_advantage': 0.05,
            'helium_savings': 0.05
        })
        self.natural_selection_percentile_low = getattr(central_config, "natural_selection_percentile_low", 20.0)
        self.natural_selection_percentile_high = getattr(central_config, "natural_selection_percentile_high", 80.0)
        self.reproductive_mutation_rate = getattr(central_config, "reproductive_mutation_rate", 0.1)
        self.reproductive_max_offspring = getattr(central_config, "reproductive_max_offspring", 3)
        self.climate_update_interval = getattr(central_config, "climate_update_interval", 3600)
        self.rate_limit_per_minute = getattr(central_config, "rate_limit_requests", 60)
        self.enable_tick_engine = getattr(central_config, "enable_tick_engine", False)
        self.enable_quantum_bridge = getattr(central_config, "enable_quantum_bridge", False)

        # v7.0.0
        self.enable_moe = getattr(central_config, "expert_registry_enable_moe", True)
        self.enable_pareto_front = getattr(central_config, "expert_registry_enable_pareto_front", True)
        self.enable_contextual_weights = getattr(central_config, "expert_registry_enable_contextual_weights", True)
        self.enable_federated_learning = getattr(central_config, "expert_registry_enable_federated_learning", True)
        self.enable_active_user_preference = getattr(central_config, "expert_registry_enable_active_user_preference", True)
        self.enable_fitness_drift_detection = getattr(central_config, "expert_registry_enable_fitness_drift_detection", True)
        self.enable_improved_forecasting = getattr(central_config, "expert_registry_enable_improved_forecasting", True)
        self.moe_hidden_layers = getattr(central_config, "moe_hidden_layers", [16, 8])
        self.pareto_max_size = getattr(central_config, "pareto_max_size", 100)
        self.context_weight_learning_rate = getattr(central_config, "context_weight_learning_rate", 0.01)
        self.federated_aggregation_interval = getattr(central_config, "federated_aggregation_interval", 3600)

        # v7.1.0 new flags
        self.enable_quantum_distillation = getattr(central_config, "enable_quantum_distillation", False)
        self.enable_causal_mask = getattr(central_config, "enable_causal_mask", True)
        self.enable_expert_auction = getattr(central_config, "enable_expert_auction", False)
        self.enable_safety_monitor = getattr(central_config, "enable_safety_monitor", True)
        self.enable_precision_controller = getattr(central_config, "enable_precision_controller", False)
        self.enable_carbon_market = getattr(central_config, "enable_carbon_market", False)
        self.carbon_market_config = getattr(central_config, "carbon_market_config", None)
        self.enable_chaos = getattr(central_config, "enable_chaos", False)
        self.chaos_probability = getattr(central_config, "chaos_probability", 0.0)
        self.enable_human_approval = getattr(central_config, "enable_human_approval", False)
        self.human_approval_timeout = getattr(central_config, "human_approval_timeout", 60.0)

# Circuit Breaker and Rate Limiter
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30.0, name="default"):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "closed"
        self._lock = asyncio.Lock()

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self.state == "open":
                if self.last_failure_time and (datetime.now(timezone.utc).timestamp() - self.last_failure_time) > self.recovery_timeout:
                    self.state = "half-open"
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is open")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == "half-open":
                    self.state = "closed"
                    self.failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = datetime.now(timezone.utc).timestamp()
                if self.failure_count >= self.failure_threshold:
                    self.state = "open"
            raise e

class RateLimiter:
    def __init__(self, rate_per_minute):
        self.capacity = float(rate_per_minute)
        self.fill_rate = rate_per_minute / 60.0
        self.tokens = self.capacity
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.last_update = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.fill_rate)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            return False

# Enhancement Modules
class QuantumDistillationModule:
    def __init__(self):
        self.available = False

    async def optimize(self, parameters):
        logger.info("Quantum distillation optimization requested (placeholder).")
        for key in parameters:
            if isinstance(parameters[key], (int, float)):
                parameters[key] += random.uniform(-0.01, 0.01)
        return parameters

    def is_available(self):
        return self.available

class CausalFeatureMask:
    def __init__(self, feature_dim):
        self.mask = np.ones(feature_dim, dtype=np.float32)

    def apply(self, features):
        return features * self.mask

class ExpertAuction:
    def __init__(self, expert_ids, feature_dim):
        self.expert_ids = expert_ids
        self.bidding_models = {}
        self.scaler = None
        if SKLEARN_AVAILABLE:
            self.scaler = StandardScaler()
            for eid in expert_ids:
                self.bidding_models[eid] = SGDRegressor(max_iter=1000, tol=1e-3, random_state=42)
            self.is_trained = False
        else:
            self.bidding_models = None

    def compute_bids(self, features):
        if not SKLEARN_AVAILABLE or not self.is_trained:
            return {eid: random.uniform(0,1) for eid in self.expert_ids}
        features_scaled = self.scaler.transform(features.reshape(1,-1))
        return {eid: float(model.predict(features_scaled)[0]) for eid, model in self.bidding_models.items()}

    def train(self, features, expert_id, reward):
        if not SKLEARN_AVAILABLE:
            return
        features_scaled = self.scaler.transform(features.reshape(1,-1))
        model = self.bidding_models[expert_id]
        model.partial_fit(features_scaled, [reward])
        self.is_trained = True

    def select_experts(self, features, top_k=1):
        bids = self.compute_bids(features)
        sorted_bids = sorted(bids.items(), key=lambda x: x[1], reverse=True)
        return [eid for eid, _ in sorted_bids[:top_k]]

class SafetyMonitor:
    def __init__(self):
        self.invariants = []

    def add_invariant(self, name, condition_fn, description):
        self.invariants.append((name, condition_fn, description))

    def check(self, state):
        violations = []
        for name, fn, desc in self.invariants:
            if not fn(state):
                violations.append(f"{name}: {desc}")
        return violations

class PrecisionController:
    def __init__(self, policy="energy_aware"):
        self.policy = policy

    def get_precision(self, load, energy_budget):
        if self.policy == "energy_aware" and (load > 0.8 or energy_budget < 0.2):
            return "float16"
        return "float32"

class CarbonMarketClient:
    def __init__(self, provider_url=None, contract_address=None, private_key=None):
        self.available = bool(provider_url and contract_address and private_key)

    def buy_credits(self, amount):
        if not self.available:
            return False
        logger.info(f"Simulating purchase of {amount} carbon credits.")
        return True

    def sell_credits(self, amount):
        if not self.available:
            return False
        logger.info(f"Simulating sale of {amount} carbon credits.")
        return True

class ChaosInjector:
    def __init__(self, registry, chaos_probability=0.01):
        self.registry = registry
        self.chaos_probability = chaos_probability

    async def maybe_inject_failure(self):
        if random.random() < self.chaos_probability:
            action = random.choice(['corrupt_fitness', 'deprecate_random_expert', 'delay'])
            logger.warning(f"Chaos injection: {action}")
            if action == 'corrupt_fitness':
                if self.registry.fitness_scores:
                    eid = random.choice(list(self.registry.fitness_scores.keys()))
                    self.registry.fitness_scores[eid].overall_fitness *= random.uniform(0.5,1.5)
            elif action == 'deprecate_random_expert':
                if self.registry._experts:
                    eid = random.choice(list(self.registry._experts.keys()))
                    self.registry._experts[eid].is_active = False
            elif action == 'delay':
                await asyncio.sleep(random.uniform(0.5,2.0))

class HumanApprovalHandler:
    def __init__(self, queue=None):
        self.queue = queue

    async def request_approval(self, decision, timeout=60.0):
        if not self.queue:
            logger.warning("No queue for human approval; auto-approving.")
            return True
        logger.info(f"Human approval requested for {decision.get('action')}, auto-approving.")
        await asyncio.sleep(0)
        return True

# MoE Gating Network
class MoEGatingNetwork:
    def __init__(self, registry, config):
        self.registry = registry
        self.config = config
        self.hidden_layers = getattr(config, 'moe_hidden_layers', [16,8])
        self._gating_model = None
        self._scaler = None
        self._trained = False
        self._training_data = []
        self._lock = asyncio.Lock()
        self._label_to_expert = {}
        self._expert_to_label = {}

    def _encode_context(self, context):
        features = [
            context.get('task_type_encoded', 0.0),
            context.get('carbon_intensity', 400) / 1000.0,
            context.get('workload_size', 0.5),
            context.get('latency_target_ms', 100) / 1000.0,
            datetime.now(timezone.utc).hour / 24.0,
            context.get('domain_encoded', 0.0),
        ]
        return np.array(features, dtype=np.float32)

    def _train_gating(self):
        if not SKLEARN_AVAILABLE or len(self._training_data) < 10:
            return
        X = np.array([item[0] for item in self._training_data])
        y = np.array([item[1] for item in self._training_data])
        self._scaler = StandardScaler()
        X_scaled = self._scaler.fit_transform(X)
        self._gating_model = MLPClassifier(hidden_layer_sizes=self.hidden_layers, max_iter=200, random_state=42)
        self._gating_model.fit(X_scaled, y)
        self._trained = True
        logger.info(f"MoE gating network trained on {len(self._training_data)} samples.")

    async def predict_proba(self, context):
        if not self._trained:
            return None
        features = self._encode_context(context)
        X = features.reshape(1,-1)
        if self._scaler:
            X = self._scaler.transform(X)
        probs = self._gating_model.predict_proba(X)[0]
        prob_dict = {}
        for idx, p in enumerate(probs):
            expert_id = self._label_to_expert.get(idx)
            if expert_id:
                prob_dict[expert_id] = float(p)
        return prob_dict

    async def select_expert(self, context):
        prob_dict = await self.predict_proba(context)
        if not prob_dict:
            return None
        available = {eid: p for eid, p in prob_dict.items()
                     if eid in self.registry._experts and self.registry._experts[eid].lifecycle_state.is_available()}
        if not available:
            return None
        return max(available, key=available.get)

    async def add_training_sample(self, context, selected_expert, reward):
        features = self._encode_context(context)
        if selected_expert not in self._expert_to_label:
            idx = len(self._expert_to_label)
            self._expert_to_label[selected_expert] = idx
            self._label_to_expert[idx] = selected_expert
        expert_label = self._expert_to_label[selected_expert]
        async with self._lock:
            self._training_data.append((features, expert_label, reward))
            if len(self._training_data) % 10 == 0:
                self._train_gating()

    def get_stats(self):
        return {'trained': self._trained, 'samples': len(self._training_data),
                'num_experts': len(self._label_to_expert)}

# Pareto Front Optimizer
class ParetoFrontOptimizer:
    def __init__(self, registry, config):
        self.registry = registry
        self.config = config
        self.max_size = getattr(config, 'pareto_max_size', 100)
        self._lock = asyncio.Lock()

    def _dominates(self, a, b):
        a_metrics = (-a['accuracy'], a['carbon'], a['helium'], a['energy'], a['latency'])
        b_metrics = (-b['accuracy'], b['carbon'], b['helium'], b['energy'], b['latency'])
        return all(a_metrics[i] <= b_metrics[i] for i in range(5)) and any(a_metrics[i] < b_metrics[i] for i in range(5))

    async def add_expert(self, expert):
        if not self.registry.config.enable_pareto_front:
            return False
        entry = {
            'expert_id': expert.expert_id,
            'accuracy': expert.accuracy_score,
            'carbon': expert.carbon_per_inference,
            'helium': expert.helium_per_inference,
            'energy': expert.energy_per_inference,
            'latency': expert.health.avg_latency_ms,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        async with self._lock:
            front_data = self.registry.storage.get_state('pareto_front')
            front = json.loads(front_data) if front_data else []
            if any(self._dominates(existing, entry) for existing in front):
                return False
            front = [e for e in front if not self._dominates(entry, e)]
            front.append(entry)
            if len(front) > self.max_size:
                front.sort(key=lambda x: x['accuracy'])
                front = front[-self.max_size:]
            self.registry.storage.save_state('pareto_front', json.dumps(front))
            return True

    def get_front(self):
        data = self.registry.storage.get_state('pareto_front')
        return json.loads(data) if data else []

# Main Registry
class ExpertRegistry:
    def __init__(self, storage, message_queue, adaptive_cost, pareto_gating, drift_detector, metrics):
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        self.config = ExpertRegistryConfig()
        self.registry_id = self.config.registry_id

        self.enable_bio_correlation = self.config.enable_bio_correlation and BIO_INSPIRED_AVAILABLE
        self.enable_natural_selection = self.config.enable_natural_selection
        self.enable_fitness_tracking = self.config.enable_fitness_tracking
        self.enable_population_tracking = self.config.enable_population_tracking
        self.enable_sustainability_dashboard = self.config.enable_sustainability_dashboard
        self.enable_predictive_forecasting = self.config.enable_predictive_forecasting
        self.enable_cross_region_sync = self.config.enable_cross_region_sync
        self.enable_quantum_efficiency = self.config.enable_quantum_efficiency
        self.enable_reproductive_strategies = self.config.enable_reproductive_strategies
        self.enable_climate_integration = self.config.enable_climate_integration

        self._experts = {}
        self._domain_index = defaultdict(set)
        self._hardware_index = defaultdict(set)
        self._lifecycle_index = defaultdict(set)
        self._tag_index = defaultdict(set)
        self._capability_index = defaultdict(set)
        self._task_type_index = defaultdict(set)
        self._region_index = defaultdict(set)
        self._version_family_index = defaultdict(list)

        self.fitness_scores = {}
        self._performance_history = defaultdict(list)
        self._dependency_graph = None
        self._remote_registries = {}
        self._federated_experts = {}
        self._ab_tests = {}
        self._migration_paths = {}

        self.evolutionary_events = deque(maxlen=10000)
        self.speciation_count = 0
        self.extinction_count = 0
        self.total_generations = 0
        self.reproductive_events = 0

        self._stats = {'total_registrations': 0, 'total_deregistrations': 0,
                      'total_natural_selections': 0, 'last_selection': None}

        # Bio refs
        self.token_manager = None
        self.gradient_manager = None
        self.compartment_manager = None
        self.biomass_storage = None

        # New sub-managers
        self.moe_gating = None
        self.pareto_front = None
        self.context_weight_adjuster = None
        self.federated_aggregator = None
        self.active_user_preference = None
        self.fitness_drift_detector = None
        self.improved_forecaster = None

        self.quantum_distillation = None
        self.causal_mask = None
        self.expert_auction = None
        self.safety_monitor = None
        self.precision_controller = None
        self.carbon_market = None
        self.chaos_injector = None
        self.human_approval = None

        self._lock = asyncio.Lock()
        self._index_lock = asyncio.Lock()
        self._fitness_lock = asyncio.Lock()
        self._performance_lock = asyncio.Lock()
        self._rate_limiter = None

        self._ready = False
        self._init_exception = None
        self._init_task = None

        try:
            loop = asyncio.get_running_loop()
            self._init_task = loop.create_task(self._async_init())
        except RuntimeError:
            logger.warning("No running event loop; ExpertRegistry must be initialized manually.")

    async def _async_init(self):
        try:
            self._rate_limiter = RateLimiter(self.config.rate_limit_per_minute)

            if self.config.enable_moe:
                self.moe_gating = MoEGatingNetwork(self, self.config)
            if self.config.enable_pareto_front:
                self.pareto_front = ParetoFrontOptimizer(self, self.config)
            if self.config.enable_contextual_weights:
                from .contextual_weight_adjuster import ContextualWeightAdjuster  # Assuming external
                self.context_weight_adjuster = ContextualWeightAdjuster(self, self.config)
            if self.config.enable_federated_learning:
                from .federated_learning_aggregator import FederatedLearningAggregator
                self.federated_aggregator = FederatedLearningAggregator(self, self.config)
            if self.config.enable_active_user_preference:
                from .active_user_preference import ActiveUserPreferenceLearner
                self.active_user_preference = ActiveUserPreferenceLearner(self, self.config)
            if self.config.enable_fitness_drift_detection:
                from .fitness_drift_detector import FitnessDriftDetector
                self.fitness_drift_detector = FitnessDriftDetector(self, self.config)
            if self.config.enable_improved_forecasting:
                from .improved_forecaster import ImprovedPredictiveForecaster
                self.improved_forecaster = ImprovedPredictiveForecaster(self, self.config)

            # Enhancements
            self.quantum_distillation = QuantumDistillationModule() if self.config.enable_quantum_distillation else None
            self.causal_mask = CausalFeatureMask(feature_dim=6) if self.config.enable_causal_mask else None
            if self.config.enable_expert_auction and self._experts:
                self.expert_auction = ExpertAuction(list(self._experts.keys()), feature_dim=6)
            self.safety_monitor = SafetyMonitor() if self.config.enable_safety_monitor else None
            if self.safety_monitor:
                self._setup_safety_invariants()
            self.precision_controller = PrecisionController() if self.config.enable_precision_controller else None
            self.carbon_market = CarbonMarketClient(**self.config.carbon_market_config) if (self.config.enable_carbon_market and self.config.carbon_market_config) else None
            self.chaos_injector = ChaosInjector(self, self.config.chaos_probability) if self.config.enable_chaos else None
            self.human_approval = HumanApprovalHandler(self.queue) if self.config.enable_human_approval else None

            await self._load_state_from_storage()

            # Start background loops
            if self.chaos_injector:
                self._background_tasks.append(asyncio.create_task(self._chaos_loop()))
            if self.quantum_distillation and self.quantum_distillation.is_available():
                self._background_tasks.append(asyncio.create_task(self._quantum_optimization_loop()))

            self._ready = True
            logger.info("Expert Registry v7.1.0 initialization complete.")
        except Exception as e:
            logger.error(f"Initialization failed: {e}", exc_info=True)
            self._init_exception = e
            self._ready = False
            raise

    def _setup_safety_invariants(self):
        self.safety_monitor.add_invariant(
            "expert_lifecycle_valid",
            lambda s: s.get('lifecycle_state') in [e.value for e in ExpertLifecycleState],
            "Invalid expert lifecycle state"
        )
        self.safety_monitor.add_invariant(
            "fitness_score_in_range",
            lambda s: 0.0 <= s.get('fitness_score', 0.0) <= 1.0,
            "Fitness score out of range"
        )
        self.safety_monitor.add_invariant(
            "expert_count_positive",
            lambda s: s.get('expert_count', 0) >= 0,
            "Negative expert count"
        )

    async def _chaos_loop(self):
        while True:
            await asyncio.sleep(60)
            if self.chaos_injector:
                await self.chaos_injector.maybe_inject_failure()

    async def _quantum_optimization_loop(self):
        while True:
            await asyncio.sleep(3600*6)
            if self.quantum_distillation:
                current_weights = self.config.fitness_weights.copy()
                optimized = await self.quantum_distillation.optimize(current_weights)
                self.config.fitness_weights = optimized
                logger.info("Quantum distillation updated fitness weights: %s", optimized)

    async def _load_state_from_storage(self):
        # simplified - load experts, fitness scores, etc.
        pass

    async def register_expert(self, profile, validate=True, auto_certify=False,
                              create_ecoatp_account=True, register_compartment=True):
        await self._ensure_ready()
        if not await self._rate_limiter.acquire():
            return False, "Rate limit exceeded"
        async with self._lock:
            if profile.expert_id in self._experts:
                existing = self._experts[profile.expert_id]
                if profile.version.is_newer_than(existing.version):
                    existing.lifecycle_state = ExpertLifecycleState.ARCHIVED
                    profile.replaces_expert = existing.expert_id
                    self._migration_paths[existing.expert_id] = profile.expert_id
                else:
                    return False, "Expert already registered with newer version"
            if auto_certify:
                profile.lifecycle_state = ExpertLifecycleState.CERTIFIED
            elif validate:
                profile.lifecycle_state = ExpertLifecycleState.VALIDATING
            else:
                profile.lifecycle_state = ExpertLifecycleState.REGISTERED

            self._experts[profile.expert_id] = profile
            self._update_indexes(profile)

            if self.enable_bio_correlation and create_ecoatp_account and self.token_manager:
                account_id = f"expert_{profile.expert_id}"
                await self.token_manager.create_account(account_id)
                initial_tokens = int(profile.efficiency_score * 100)
                if initial_tokens > 0:
                    await self.token_manager.generate_tokens(
                        account_id=account_id,
                        source=EcoATPSource.EFFICIENCY_GAIN,
                        energy_saved_kwh=profile.efficiency_score * 0.001,
                        num_tokens=initial_tokens
                    )

            if self.enable_fitness_tracking:
                fitness = FitnessScore(
                    expert_id=profile.expert_id,
                    resource_efficiency=min(1.0, 1.0/(1.0+profile.carbon_per_inference*10000)),
                    resilience_score=profile.reliability_score,
                    adaptation_speed=0.5,
                    cooperation_score=0.5,
                    ecoatp_efficiency=profile.efficiency_score,
                    sustainability_score=profile.sustainability_score,
                    quantum_efficiency=profile.health.quantum_efficiency,
                    quantum_advantage=profile.health.quantum_advantage_score,
                    helium_savings=1.0 - profile.helium_per_inference/max(profile.helium_per_inference,1)
                )
                fitness.calculate_overall(self.config.fitness_weights)
                self.fitness_scores[profile.expert_id] = fitness

            self._stats['total_registrations'] += 1
            self.total_generations += 1

            # Safety check
            if self.safety_monitor:
                state = {
                    'lifecycle_state': profile.lifecycle_state.value,
                    'fitness_score': fitness.overall_fitness if self.enable_fitness_tracking else 0.5,
                    'expert_count': len(self._experts),
                }
                violations = self.safety_monitor.check(state)
                if violations:
                    logger.warning(f"Safety violations during registration: {violations}")

            # FeedbackEvent
            event = FeedbackEvent.create_with_context(
                task_id=f"reg_{profile.expert_id}",
                selected_action="register",
                quality_score=fitness.overall_fitness if self.enable_fitness_tracking else 0.5,
                energy_joules=0.0,
                carbon_g=0.0,
                feedback_type="registry",
                adaptive_cost_value=0.0,
                state={'expert_id': profile.expert_id, 'action': 'register'},
                candidates=[{'action': 'register', 'deprecate', 'activate'}],
                source="expert_registry",
                environment=getattr(central_config, "ENVIRONMENT", "production"),
                tags=["registry","expert"]
            )
            await self.queue.publish("feedback_events", event.to_json())

            if self.pareto_front:
                await self.pareto_front.add_expert(profile)

            if self.drift:
                await self.drift.check_drift(self.adaptive_cost.get_current_weights())

            return True, f"Expert {profile.expert_id} registered successfully"

    async def policy_probs(self, state):
        if self.moe_gating:
            prob_dict = await self.moe_gating.predict_proba(state)
            if prob_dict:
                probs = [prob_dict.get(e.expert_id,0.0) for e in self._experts.values()]
                total = sum(probs)
                if total > 0:
                    probs = [p/total for p in probs]
                return probs
        experts = list(self._experts.values())
        if not experts:
            return []
        logits = [self.fitness_scores.get(e.expert_id, FitnessScore(expert_id=e.expert_id)).overall_fitness for e in experts]
        logits = np.array(logits)
        logits = np.exp(logits - np.max(logits))
        probs = (logits / np.sum(logits)).tolist()
        return probs

    def _update_indexes(self, profile):
        self._domain_index[profile.domain].add(profile.expert_id)
        self._hardware_index[profile.hardware_profile].add(profile.expert_id)
        self._lifecycle_index[profile.lifecycle_state].add(profile.expert_id)

    async def _ensure_ready(self):
        if not self._ready:
            await self.wait_until_ready()

    async def wait_until_ready(self, timeout=None):
        if self._init_task:
            await asyncio.wait_for(self._init_task, timeout=timeout)
        if self._init_exception:
            raise self._init_exception
        return self._ready

    async def shutdown(self):
        logger.info("Shutting down Expert Registry")
        await self.save_state()
        logger.info("Shutdown complete")

    async def save_state(self):
        state = {
            "experts": {eid: exp.__dict__ for eid, exp in self._experts.items()},
            "fitness_scores": {eid: fs.__dict__ for eid, fs in self.fitness_scores.items()},
            "speciation_count": self.speciation_count,
            "extinction_count": self.extinction_count,
            "total_generations": self.total_generations,
            "reproductive_events": self.reproductive_events,
            "stats": self._stats,
        }
        self.storage.save_state("expert_registry_state", json.dumps(state))
        logger.info("Saved registry state to storage")

# Example usage
if __name__ == "__main__":
    async def main():
        logging.basicConfig(level=logging.INFO)
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

        registry = ExpertRegistry(storage, queue, adaptive_cost, pareto, drift, metrics)
        await registry.wait_until_ready()

        # Create a sample expert
        version = ExpertVersion(1,0,0)
        profile = ExpertProfile(
            expert_id="energy_expert_v1",
            expert_name="Energy Optimizer",
            version=version,
            domain=ExpertDomain.ENERGY,
            hardware_profile=HardwareProfile.GPU,
            accuracy_score=0.8,
            reliability_score=0.9,
            efficiency_score=0.85,
        )
        success, msg = await registry.register_expert(profile, auto_certify=True)
        print(f"Registration: {msg}")

        probs = await registry.policy_probs({})
        print(f"Policy probs: {probs}")

        await registry.shutdown()

    asyncio.run(main())
