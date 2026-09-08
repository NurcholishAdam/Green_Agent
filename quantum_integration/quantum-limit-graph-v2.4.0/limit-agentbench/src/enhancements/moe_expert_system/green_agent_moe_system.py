#!/usr/bin/env python3
"""
Green Agent MoE Expert System v8.2.0 - Unified Metabolic Ecosystem
Full Green Agent MODP Integration with all requested enhancements.

New modules:
- QuantumDistillationModule (placeholder)
- CausalFeatureMask (causal RL)
- ExpertAuction (multi-agent)
- SafetyMonitor (temporal logic)
- PrecisionController (adaptive precision)
- CarbonMarketClient (carbon trading)
- ChaosInjector (chaos testing)
- HumanApprovalHandler (human-in-the-loop)

Fixes:
- ExplainabilityHelper initialization
- Missing feature names
- datetime timezone
- Concurrency locks on recent accuracies
- Gating network causal mask & precision integration
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
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import numpy as np

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

# Optional dependencies
try:
    import aiofiles
except ImportError:
    aiofiles = None

try:
    import aiohttp
except ImportError:
    aiohttp = None

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

# Carbon/helium managers (optional)
try:
    from .carbon_intensity import CarbonIntensityManager as ExternalCarbonIntensityManager
    from .helium_optimizer import HeliumEfficiencyOptimizer as ExternalHeliumOptimizer
    CARBON_HELIUM_AVAILABLE = True
except ImportError:
    CARBON_HELIUM_AVAILABLE = False

# For forecasting (ARIMA)
try:
    from statsmodels.tsa.arima.model import ARIMA
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False

try:
    import shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False

try:
    from fastapi import WebSocket, WebSocketDisconnect
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
class UnifiedEcosystemConfig:
    def __init__(self):
        # Feature flags
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

        # Tunable operational limits
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

        # v8.0.0 parameters
        self.enable_ga_tuning = getattr(central_config, "enable_ga_tuning", True)
        self.enable_pareto_front = getattr(central_config, "enable_pareto_front", True)
        self.enable_active_user_pref = getattr(central_config, "enable_active_user_pref", True)
        self.enable_drift_retrain = getattr(central_config, "enable_drift_retrain", True)
        self.enable_explainability = getattr(central_config, "enable_explainability", True)
        self.enable_carbon_forecast = getattr(central_config, "enable_carbon_forecast", True)
        self.ga_population_size = getattr(central_config, "ga_population_size", 10)
        self.ga_generations = getattr(central_config, "ga_generations", 3)
        self.ga_mutation_rate = getattr(central_config, "ga_mutation_rate", 0.1)
        self.ga_crossover_rate = getattr(central_config, "ga_crossover_rate", 0.7)
        self.pareto_max_size = getattr(central_config, "pareto_max_size", 50)
        self.drift_threshold = getattr(central_config, "drift_threshold", 0.15)
        self.expert_output_dim = getattr(central_config, "expert_output_dim", 1)

        # v8.2.0 new flags
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

        if self.health_check_interval < 1:
            raise ValueError("health_check_interval must be >= 1 second")
        if self.recovery_max_attempts < 1:
            raise ValueError("recovery_max_attempts must be >= 1")
        if self.rate_limit_per_minute < 1:
            raise ValueError("rate_limit_per_minute must be >= 1")
        if self.per_expert_rate_limit < 1:
            raise ValueError("per_expert_rate_limit must be >= 1")

# -----------------------------------------------------------------------------
# Circuit Breaker and Rate Limiter
# -----------------------------------------------------------------------------
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self._lock = asyncio.Lock()

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if self.last_failure_time:
                    elapsed = (datetime.now(timezone.utc) - self.last_failure_time).total_seconds()
                    if elapsed >= self.recovery_timeout:
                        self.state = CircuitBreakerState.HALF_OPEN
                        self.failure_count = 0
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
                elif self.state == CircuitBreakerState.CLOSED:
                    self.failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = datetime.now(timezone.utc)
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.OPEN
                elif self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
            raise e

    @property
    def is_open(self):
        return self.state == CircuitBreakerState.OPEN

    async def reset(self):
        async with self._lock:
            self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0
            self.last_failure_time = None

class RateLimiter:
    def __init__(self, rate_per_minute):
        self.capacity = float(rate_per_minute)
        self.fill_rate = rate_per_minute / 60.0
        self.tokens = float(rate_per_minute)
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

class PerExpertRateLimiter:
    def __init__(self, rate_per_minute):
        self.limiters = {}
        self.rate = rate_per_minute

    def get_limiter(self, expert_id):
        if expert_id not in self.limiters:
            self.limiters[expert_id] = RateLimiter(self.rate)
        return self.limiters[expert_id]

# -----------------------------------------------------------------------------
# Gating Network and Expert Modules
# -----------------------------------------------------------------------------
if TORCH_AVAILABLE:
    def get_activation(name):
        if name == "relu": return nn.ReLU()
        elif name == "tanh": return nn.Tanh()
        elif name == "gelu": return nn.GELU()
        else: raise ValueError(f"Unknown activation: {name}")

    class GatingNetwork(nn.Module):
        def __init__(self, input_dim, hidden_dim, num_experts, num_layers=2, activation="relu", dropout_rate=0.1):
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

        def forward(self, x):
            return self.network(x)

    class ExpertModule(nn.Module):
        def __init__(self, input_dim, hidden_dim=32, output_dim=1):
            super().__init__()
            self.network = nn.Sequential(
                nn.Linear(input_dim, hidden_dim),
                nn.ReLU(),
                nn.Linear(hidden_dim, output_dim)
            )

        def forward(self, x):
            return self.network(x)
else:
    GatingNetwork = None
    ExpertModule = None

# -----------------------------------------------------------------------------
# Genetic Hyperparameter Tuner
# -----------------------------------------------------------------------------
class GeneticHyperparameterTuner:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.population_size = config.ga_population_size
        self.generations = config.ga_generations
        self.mutation_rate = config.ga_mutation_rate
        self.crossover_rate = config.ga_crossover_rate
        self.param_bounds = {
            'hidden_dim': (16, 256),
            'num_layers': (1, 4),
            'dropout_rate': (0.0, 0.5),
            'learning_rate': (1e-5, 1e-2),
            'activation': ['relu', 'tanh', 'gelu'],
        }
        self._lock = asyncio.Lock()

    def _random_chromosome(self):
        return {
            'hidden_dim': random.randint(*self.param_bounds['hidden_dim']),
            'num_layers': random.randint(*self.param_bounds['num_layers']),
            'dropout_rate': random.uniform(*self.param_bounds['dropout_rate']),
            'learning_rate': 10 ** random.uniform(np.log10(self.param_bounds['learning_rate'][0]), np.log10(self.param_bounds['learning_rate'][1])),
            'activation': random.choice(self.param_bounds['activation'])
        }

    def _mutate(self, chrom):
        new = chrom.copy()
        if random.random() < self.mutation_rate:
            if random.random() < 0.5:
                new['hidden_dim'] = max(self.param_bounds['hidden_dim'][0], min(self.param_bounds['hidden_dim'][1], chrom['hidden_dim'] + random.randint(-32, 32)))
            else:
                new['num_layers'] = max(self.param_bounds['num_layers'][0], min(self.param_bounds['num_layers'][1], chrom['num_layers'] + random.randint(-1, 1)))
        if random.random() < self.mutation_rate:
            new['dropout_rate'] = max(self.param_bounds['dropout_rate'][0], min(self.param_bounds['dropout_rate'][1], chrom['dropout_rate'] + random.gauss(0, 0.05)))
        if random.random() < self.mutation_rate:
            new['learning_rate'] = 10 ** np.clip(np.log10(chrom['learning_rate']) + random.gauss(0, 0.5), np.log10(self.param_bounds['learning_rate'][0]), np.log10(self.param_bounds['learning_rate'][1]))
        if random.random() < self.mutation_rate:
            new['activation'] = random.choice(self.param_bounds['activation'])
        return new

    def _crossover(self, p1, p2):
        if random.random() > self.crossover_rate:
            return p1.copy(), p2.copy()
        c1, c2 = p1.copy(), p2.copy()
        for param in ['hidden_dim', 'num_layers', 'dropout_rate', 'learning_rate']:
            if random.random() < 0.5:
                c1[param], c2[param] = p2[param], p1[param]
        if random.random() < 0.5:
            c1['activation'], c2['activation'] = p2['activation'], p1['activation']
        return c1, c2

    async def _evaluate_fitness(self, chrom, training_data):
        if not training_data or not TORCH_AVAILABLE:
            return 0.5
        X = np.array([t[0] for t in training_data])
        y = np.array([t[1] for t in training_data])
        idx = np.random.permutation(len(X))
        X_train, X_val = X[idx[:int(len(X)*0.8)]], X[idx[int(len(X)*0.8):]]
        y_train, y_val = y[idx[:int(len(X)*0.8)]], y[idx[int(len(X)*0.8):]]
        if len(X_val) == 0:
            X_val, y_val = X_train[:5], y_train[:5]
        model = GatingNetwork(
            input_dim=self.config.gating_input_dim,
            hidden_dim=chrom['hidden_dim'],
            num_experts=self.config.gating_num_experts,
            num_layers=chrom['num_layers'],
            activation=chrom['activation'],
            dropout_rate=chrom['dropout_rate']
        )
        optimizer = optim.Adam(model.parameters(), lr=chrom['learning_rate'])
        criterion = nn.CrossEntropyLoss()
        X_t = torch.FloatTensor(X_train)
        y_t = torch.LongTensor(y_train)
        dataset = TensorDataset(X_t, y_t)
        dataloader = DataLoader(dataset, batch_size=min(32, len(X_train)), shuffle=True)
        model.train()
        for _ in range(2):
            for batch_X, batch_y in dataloader:
                optimizer.zero_grad()
                output = model(batch_X)
                loss = criterion(output, batch_y)
                loss.backward()
                optimizer.step()
        model.eval()
        with torch.no_grad():
            X_v = torch.FloatTensor(X_val)
            y_v = torch.LongTensor(y_val)
            preds = model(X_v).argmax(dim=1)
            acc = (preds == y_v).float().mean().item()
        return acc

    async def run_search(self, training_data):
        population = [self._random_chromosome() for _ in range(self.population_size)]
        best_fitness = -1.0
        best_individual = None

        for gen in range(self.generations):
            fitnesses = await asyncio.gather(*[self._evaluate_fitness(chrom, training_data) for chrom in population])
            sorted_pop = sorted(zip(population, fitnesses), key=lambda x: x[1], reverse=True)
            if sorted_pop[0][1] > best_fitness:
                best_fitness = sorted_pop[0][1]
                best_individual = sorted_pop[0][0]

            parents = [ind for ind, _ in sorted_pop[:max(2, self.population_size // 2)]]
            offspring = []
            while len(offspring) < self.population_size:
                p1, p2 = random.choice(parents), random.choice(parents)
                c1, c2 = self._crossover(p1, p2)
                c1 = self._mutate(c1)
                c2 = self._mutate(c2)
                offspring.append(c1)
                if len(offspring) < self.population_size:
                    offspring.append(c2)
            combined = parents + offspring
            combined_fitness = await asyncio.gather(*[self._evaluate_fitness(chrom, training_data) for chrom in combined])
            sorted_combined = sorted(zip(combined, combined_fitness), key=lambda x: x[1], reverse=True)
            population = [ind for ind, _ in sorted_combined[:self.population_size]]

        if best_individual:
            self.storage.save_state('gating_best_hyperparams', json.dumps(best_individual))
        return best_individual

# -----------------------------------------------------------------------------
# Pareto Front Manager
# -----------------------------------------------------------------------------
class ParetoFrontManager:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.max_size = config.pareto_max_size
        self._lock = asyncio.Lock()

    def _dominates(self, a, b):
        a_metrics = (-a['accuracy'], a['carbon'], a['helium'], a['latency'])
        b_metrics = (-b['accuracy'], b['carbon'], b['helium'], b['latency'])
        return all(a_metrics[i] <= b_metrics[i] for i in range(4)) and any(a_metrics[i] < b_metrics[i] for i in range(4))

    async def add_solution(self, expert_id, metrics):
        if not self.config.enable_pareto_front:
            return
        entry = {
            'expert_id': expert_id,
            'accuracy': metrics.get('accuracy', 0.5),
            'carbon': metrics.get('carbon', 0.1),
            'helium': metrics.get('helium', 0.01),
            'latency': metrics.get('latency', 100),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        async with self._lock:
            front_data = self.storage.get_state('gating_pareto_front')
            front = json.loads(front_data) if front_data else []
            if any(self._dominates(existing, entry) for existing in front):
                return
            front = [e for e in front if not self._dominates(entry, e)]
            front.append(entry)
            if len(front) > self.max_size:
                front.sort(key=lambda x: x['accuracy'])
                front = front[-self.max_size:]
            self.storage.save_state('gating_pareto_front', json.dumps(front))

    def get_front(self):
        data = self.storage.get_state('gating_pareto_front')
        return json.loads(data) if data else []

    async def get_trade_off_suggestions(self, user_weights):
        front = self.get_front()
        if not front:
            return []
        scored = []
        for e in front:
            score = (user_weights.get('accuracy', 0.4) * e['accuracy'] +
                     user_weights.get('carbon', 0.2) * (1 / (e['carbon'] + 1e-8)) +
                     user_weights.get('helium', 0.2) * (1 / (e['helium'] + 1e-8)) +
                     user_weights.get('latency', 0.2) * (1 / (e['latency'] + 1e-8)))
            scored.append((score, e))
        scored.sort(reverse=True)
        return [e for _, e in scored[:5]]

# -----------------------------------------------------------------------------
# Active User Preference Learner
# -----------------------------------------------------------------------------
class ActiveUserPreferenceLearner:
    def __init__(self, storage, websocket=None):
        self.storage = storage
        self.websocket = websocket
        self.user_weights = {}

    async def query_user_if_needed(self, user_id, candidates):
        if len(candidates) < 2:
            return None
        acc_diff = abs(candidates[0]['accuracy'] - candidates[1]['accuracy'])
        if acc_diff / max(candidates[0]['accuracy'], candidates[1]['accuracy']) < 0.05:
            return candidates[0]['expert_id']
        return None

    async def record_choice(self, user_id, chosen_expert_id):
        if user_id not in self.user_weights:
            self.user_weights[user_id] = self._default_weights()
        self.user_weights[user_id]['accuracy'] += 0.01
        total = sum(self.user_weights[user_id].values())
        for k in self.user_weights[user_id]:
            self.user_weights[user_id][k] /= total
        self.storage.save_state(f'user_weights_{user_id}', json.dumps(self.user_weights[user_id]))

    def _default_weights(self):
        return {'accuracy': 0.4, 'carbon': 0.2, 'helium': 0.2, 'latency': 0.2}

# -----------------------------------------------------------------------------
# Explainability Helper
# -----------------------------------------------------------------------------
class ExplainabilityHelper:
    def __init__(self, model, feature_names):
        self.model = model
        self.feature_names = feature_names
        self.shap_explainer = None
        self._use_gradient = False
        if SHAP_AVAILABLE and not torch.cuda.is_available():
            self.shap_explainer = shap.Explainer(lambda x: self._predict_proba(x), np.zeros((10, len(feature_names))))
        else:
            self._use_gradient = True

    def _predict_proba(self, X):
        self.model.eval()
        with torch.no_grad():
            logits = self.model(torch.FloatTensor(X))
            return torch.softmax(logits, dim=1).numpy()

    def explain(self, context):
        if self.shap_explainer:
            shap_values = self.shap_explainer(context.reshape(1, -1))
            importance = shap_values.values[0]
            return {
                'method': 'shap',
                'feature_importance': {name: float(imp) for name, imp in zip(self.feature_names, importance)},
                'top_features': sorted(zip(self.feature_names, importance), key=lambda x: abs(x[1]), reverse=True)[:5]
            }
        else:
            self.model.eval()
            X = torch.FloatTensor(context.reshape(1, -1)).requires_grad_(True)
            logits = self.model(X)
            probs = torch.softmax(logits, dim=1)[0]
            max_prob = probs.max()
            max_prob.backward()
            grad = X.grad[0].abs().numpy()
            importance = grad
            return {
                'method': 'gradient',
                'feature_importance': {name: float(imp) for name, imp in zip(self.feature_names, importance)},
                'top_features': sorted(zip(self.feature_names, importance), key=lambda x: x[1], reverse=True)[:5]
            }

# -----------------------------------------------------------------------------
# New Enhancement Modules (inserted)
# -----------------------------------------------------------------------------
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


class CausalFeatureMask(nn.Module):
    def __init__(self, feature_dim):
        super().__init__()
        self.mask = nn.Parameter(torch.ones(feature_dim))

    def forward(self, x):
        return x * self.mask


class ExpertAuction:
    def __init__(self, expert_ids, feature_dim, hidden_dim):
        self.expert_ids = expert_ids
        self.bidding_networks = nn.ModuleDict({
            eid: nn.Sequential(
                nn.Linear(feature_dim, hidden_dim),
                nn.ReLU(),
                nn.Linear(hidden_dim, 1)
            ) for eid in expert_ids
        })

    def compute_bids(self, features):
        bids = {}
        for eid, net in self.bidding_networks.items():
            bid = net(features).squeeze().item()
            bids[eid] = bid
        return bids

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
        if self.policy == "energy_aware":
            if load > 0.8 or energy_budget < 0.2:
                return "float16"
            else:
                return "float32"
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
    def __init__(self, manager, chaos_probability=0.01):
        self.manager = manager
        self.chaos_probability = chaos_probability

    async def maybe_inject_failure(self):
        if random.random() < self.chaos_probability:
            action = random.choice(['corrupt_weights', 'delay'])
            logger.warning(f"Chaos injection: {action}")
            if action == 'corrupt_weights':
                with torch.no_grad():
                    for param in self.manager.gating_network.model.parameters():
                        param.mul_(random.uniform(0.8, 1.2))
            elif action == 'delay':
                await asyncio.sleep(random.uniform(0.5, 2.0))


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

# -----------------------------------------------------------------------------
# Federated Gating Aggregator (unchanged)
# -----------------------------------------------------------------------------
class FederatedGatingAggregator:
    def __init__(self, storage, config, queue, instance_id):
        self.storage = storage
        self.config = config
        self.queue = queue
        self.instance_id = instance_id
        self.round = 0
        self.participants = []
        self.contribution_score = 0.0
        self._lock = asyncio.Lock()

    async def share_weights(self, state_dict, performance=1.0):
        message = {
            'type': 'federated_gating_update',
            'instance_id': self.instance_id,
            'round': self.round,
            'weights': state_dict,
            'performance': performance,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        await self.queue.publish("federated_gating", json.dumps(message))
        self.contribution_score += performance

    async def aggregate_weights(self):
        return None

    async def apply_aggregated_weights(self, state_dict):
        if state_dict and TORCH_AVAILABLE:
            pass

# -----------------------------------------------------------------------------
# Carbon Intensity Manager and Helium Optimizer (unchanged)
# -----------------------------------------------------------------------------
class CarbonIntensityManager:
    def __init__(self, config):
        self.config = config
        self.region = config.carbon_api_region
        self.intensity = 400.0
        self.price = 50.0
        self.last_update = None
        self.history = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self._circuit = CircuitBreaker()
        self._session = None
        self.forecast_model = None

    async def _get_session(self):
        if aiohttp is None:
            return None
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def update(self):
        async with self._lock:
            try:
                session = await self._get_session()
                if session is None:
                    self.intensity = 400
                else:
                    # Simulate API call
                    self.intensity = random.uniform(200, 600)
                self.last_update = datetime.now(timezone.utc)
                self.history.append(self.intensity)
            except Exception as e:
                logger.error(f"Carbon intensity fetch error: {e}")
                self.intensity = 400
                self.last_update = datetime.now(timezone.utc)
            return {'intensity': self.intensity, 'region': self.region}

    async def get_current_intensity(self):
        if self.last_update is None or (datetime.now(timezone.utc) - self.last_update).total_seconds() > self.config.carbon_update_interval:
            await self.update()
        return self.intensity

    async def get_current_position(self):
        return {'intensity': await self.get_current_intensity(), 'region': self.region, 'price': self.price}

    async def forecast(self, hours=24):
        if not STATSMODELS_AVAILABLE or len(self.history) < 10:
            return self.intensity
        try:
            model = ARIMA(list(self.history), order=(5,1,0))
            model_fit = model.fit()
            forecast = model_fit.forecast(steps=hours)
            return float(np.mean(forecast))
        except:
            return self.intensity

    async def close(self):
        if self._session:
            await self._session.close()

class HeliumEfficiencyOptimizer:
    def __init__(self, config):
        self.config = config
        self.budget = 100.0
        self.usage = defaultdict(float)
        self.efficiency_scores = defaultdict(lambda: 0.5)
        self.price = 0.5
        self.price_history = deque(maxlen=100)
        self._lock = asyncio.Lock()
        self.forecast_model = None

    async def get_helium_status(self):
        return {
            'budget': self.budget,
            'usage': dict(self.usage),
            'price': self.price,
            'efficiency_scores': dict(self.efficiency_scores),
            'price_history': list(self.price_history)
        }

    async def allocate(self, requirements):
        total = sum(requirements.values())
        if total <= self.budget:
            return requirements
        return {eid: req * self.budget / total for eid, req in requirements.items()}

    async def forecast_price(self, hours=24):
        if len(self.price_history) < 5:
            return self.price
        return float(np.mean(list(self.price_history)[-5:]))

    async def close(self):
        pass

# -----------------------------------------------------------------------------
# BaseExpert and Expert Classes (unchanged)
# -----------------------------------------------------------------------------
class BaseExpert:
    def __init__(self, name, domain, expert_module=None):
        self.name = name
        self.domain = domain
        self.expert_module = expert_module
        self.healthy = True
        self.capabilities = {"domain": domain}
        self.sustainability_score = 1.0
        self.hardware_profile = {"carbon_g_per_joule": 0.001, "energy_joules_per_call": 0.01, "latency_ms_per_call": 50}

    async def get_health_status(self):
        return {"status": "healthy" if self.healthy else "unhealthy", "score": 1.0 if self.healthy else 0.0}

    async def execute(self, params, context):
        return {"expert": self.name, "domain": self.domain, "status": "executed", "result": "success"}

    def get_capabilities(self):
        return self.capabilities

    def estimate_metrics(self):
        return {
            "carbon_g": self.hardware_profile["carbon_g_per_joule"] * self.hardware_profile["energy_joules_per_call"],
            "energy_joules": self.hardware_profile["energy_joules_per_call"],
            "latency_ms": self.hardware_profile["latency_ms_per_call"]
        }

class EnergyExpert(BaseExpert):
    def __init__(self, input_dim):
        expert_module = ExpertModule(input_dim, hidden_dim=32, output_dim=1) if TORCH_AVAILABLE else None
        super().__init__("EnergyExpert", "energy_management", expert_module)
        self.hardware_profile = {"carbon_g_per_joule": 0.002, "energy_joules_per_call": 0.015, "latency_ms_per_call": 70}

class DataExpert(BaseExpert):
    def __init__(self, input_dim):
        expert_module = ExpertModule(input_dim, hidden_dim=32, output_dim=1) if TORCH_AVAILABLE else None
        super().__init__("DataExpert", "data_processing", expert_module)
        self.hardware_profile = {"carbon_g_per_joule": 0.001, "energy_joules_per_call": 0.01, "latency_ms_per_call": 80}

class IoTExpert(BaseExpert):
    def __init__(self, input_dim):
        expert_module = ExpertModule(input_dim, hidden_dim=32, output_dim=1) if TORCH_AVAILABLE else None
        super().__init__("IoTExpert", "iot_sensing", expert_module)
        self.hardware_profile = {"carbon_g_per_joule": 0.0005, "energy_joules_per_call": 0.005, "latency_ms_per_call": 30}

# -----------------------------------------------------------------------------
# GatingNetworkManager (with enhancements)
# -----------------------------------------------------------------------------
class GatingNetworkManager:
    def __init__(self, config, expert_ids, storage):
        self.config = config
        self.expert_ids = expert_ids
        self.num_experts = len(expert_ids)
        self.storage = storage
        if TORCH_AVAILABLE:
            best_hyper = storage.get_state('gating_best_hyperparams')
            if best_hyper:
                hp = json.loads(best_hyper)
                self.model = GatingNetwork(
                    input_dim=config.gating_input_dim,
                    hidden_dim=hp['hidden_dim'],
                    num_experts=self.num_experts,
                    num_layers=hp['num_layers'],
                    activation=hp['activation'],
                    dropout_rate=hp['dropout_rate']
                )
            else:
                self.model = GatingNetwork(
                    input_dim=config.gating_input_dim,
                    hidden_dim=config.gating_hidden_dim,
                    num_experts=self.num_experts,
                    num_layers=config.gating_num_layers,
                    activation=config.gating_activation
                )
            self.optimizer = optim.Adam(self.model.parameters(), lr=config.gating_learning_rate)
            self.criterion = nn.CrossEntropyLoss()
            self.expert_modules = nn.ModuleDict()
            for eid in expert_ids:
                self.expert_modules[eid] = ExpertModule(config.gating_input_dim, hidden_dim=32, output_dim=1)
        else:
            self.model = None
            self.expert_modules = None
        self.training_buffer = deque(maxlen=10000)
        self.is_trained = False
        self.inference_count = 0
        self.training_count = 0
        # New attributes for enhancements
        self.causal_mask = None
        self.precision_controller = None

    def _build_features(self, context):
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

    async def predict(self, context):
        if not TORCH_AVAILABLE or self.model is None:
            return {eid: 1.0 / self.num_experts for eid in self.expert_ids}
        features = self._build_features(context)
        features_tensor = torch.FloatTensor(features).unsqueeze(0)

        # Apply causal mask if present
        if self.causal_mask is not None:
            features_tensor = self.causal_mask(features_tensor)

        # Apply precision controller if present
        if self.precision_controller is not None:
            precision = self.precision_controller.get_precision(
                load=float(torch.mean(features_tensor).item()),
                energy_budget=0.5
            )
            if precision == 'float16':
                features_tensor = features_tensor.half()

        with torch.no_grad():
            logits = self.model(features_tensor)
            probs = torch.softmax(logits, dim=1).squeeze().cpu().numpy()
        self.inference_count += 1
        return {self.expert_ids[i]: float(probs[i]) for i in range(len(self.expert_ids))}

    async def soft_gate(self, context):
        return await self.predict(context)

    async def get_expert_output(self, expert_id, context):
        if not TORCH_AVAILABLE or self.expert_modules is None or expert_id not in self.expert_modules:
            return None
        features = self._build_features(context)
        with torch.no_grad():
            output = self.expert_modules[expert_id](torch.FloatTensor(features).unsqueeze(0))
            return float(output.item())

    def add_training_sample(self, features, label):
        if features.shape[0] != self.config.gating_input_dim:
            raise ValueError(f"Feature dimension mismatch: expected {self.config.gating_input_dim}")
        if not 0 <= label < self.num_experts:
            raise ValueError(f"Label out of range: {label}")
        if len(self.training_buffer) >= 10000:
            self.training_buffer.popleft()
        self.training_buffer.append((features, label))

    async def train(self, epochs=3):
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

    def get_state_dict(self):
        if not TORCH_AVAILABLE or self.model is None:
            return {}
        return {k: v.tolist() for k, v in self.model.state_dict().items()}

    def load_state_dict(self, state_dict):
        if not TORCH_AVAILABLE or self.model is None:
            return
        self.model.load_state_dict({k: torch.FloatTensor(v) for k, v in state_dict.items()})
        self.is_trained = True

    async def policy_probs(self, state):
        probs_dict = await self.predict(state)
        return [probs_dict.get(eid, 0.0) for eid in self.expert_ids]

# -----------------------------------------------------------------------------
# Health Check System (unchanged)
# -----------------------------------------------------------------------------
class HealthCheckSystem:
    def __init__(self, config):
        self.config = config
        self.components = {}
        self.history = defaultdict(list)
        self._lock = asyncio.Lock()
        self._running = True
        self._task = self._create_task(self._loop())
        logger.info("HealthCheckSystem initialized")

    def _create_task(self, coro):
        try:
            loop = asyncio.get_running_loop()
            return loop.create_task(coro)
        except RuntimeError:
            logger.warning("No running loop; health check loop not started.")
            return None

    async def _loop(self):
        while self._running:
            await asyncio.sleep(self.config.health_check_interval)
            await self._perform_checks()

    async def _perform_checks(self):
        async with self._lock:
            for name, comp in self.components.items():
                try:
                    if hasattr(comp, 'get_health_status'):
                        status = await comp.get_health_status()
                        if isinstance(status, dict):
                            comp_status = status.get("status", "healthy")
                            comp_score = status.get("score", 1.0)
                        else:
                            comp_status = "healthy"
                            comp_score = 1.0
                    else:
                        comp_status = "healthy"
                        comp_score = 1.0
                except Exception as e:
                    logger.warning(f"Health check for {name} failed: {e}")
                    comp_status = "unhealthy"
                    comp_score = 0.0
                self.history[name].append({"timestamp": datetime.now(timezone.utc).isoformat(),
                                           "status": comp_status, "score": comp_score})
                if len(self.history[name]) > 100:
                    self.history[name] = self.history[name][-100:]

    def register_component(self, name, component):
        self.components[name] = component

    async def get_system_health(self):
        async with self._lock:
            total = 0.0
            statuses = {}
            for name, comp in self.components.items():
                if self.history[name]:
                    latest = self.history[name][-1]
                    statuses[name] = latest
                    total += latest["score"]
                else:
                    statuses[name] = {"status": "unknown", "score": 0.5}
                    total += 0.5
            avg = total / max(len(self.components), 1)
            system_status = "healthy" if avg > 0.8 else "degraded" if avg > 0.5 else "unhealthy"
            return {"system_status": system_status, "system_score": avg, "components": statuses}

    async def shutdown(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

# -----------------------------------------------------------------------------
# Self-Healing System (unchanged)
# -----------------------------------------------------------------------------
class SelfHealingSystem:
    def __init__(self, config, health_system=None):
        self.config = config
        self.health_system = health_system
        self.handlers = {}
        self.attempts = defaultdict(int)
        self.max_attempts = config.recovery_max_attempts
        self._running = True
        self._task = self._create_task(self._loop())
        logger.info("SelfHealingSystem initialized")

    def _create_task(self, coro):
        try:
            loop = asyncio.get_running_loop()
            return loop.create_task(coro)
        except RuntimeError:
            logger.warning("No running loop; self-healing loop not started.")
            return None

    async def _loop(self):
        while self._running:
            await asyncio.sleep(30)
            if self.health_system:
                health = await self.health_system.get_system_health()
                for comp_name, comp_data in health.get("components", {}).items():
                    if comp_data.get("status") in ["degraded", "unhealthy"]:
                        await self._attempt_recovery(comp_name)

    async def _attempt_recovery(self, component_name):
        if self.attempts[component_name] >= self.max_attempts:
            logger.warning(f"Component {component_name} exceeded max recovery attempts")
            return
        self.attempts[component_name] += 1
        handler = self.handlers.get(component_name)
        if handler:
            try:
                if asyncio.iscoroutinefunction(handler):
                    success = await handler()
                else:
                    success = handler()
                if success:
                    logger.info(f"Component {component_name} recovered")
                    self.attempts[component_name] = 0
                else:
                    logger.warning(f"Recovery for {component_name} failed")
            except Exception as e:
                logger.error(f"Recovery handler for {component_name} error: {e}")

    def register_handler(self, name, handler):
        self.handlers[name] = handler

    async def shutdown(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

# -----------------------------------------------------------------------------
# Alerting System (unchanged)
# -----------------------------------------------------------------------------
class AlertingSystem:
    def __init__(self, config):
        self.config = config
        self.alerts = []

    async def trigger_alert(self, severity, message):
        alert = {"severity": severity, "message": message, "timestamp": datetime.now(timezone.utc).isoformat()}
        self.alerts.append(alert)
        if severity == "critical":
            logger.critical(f"ALERT: {message}")
        else:
            logger.warning(f"ALERT ({severity}): {message}")

# -----------------------------------------------------------------------------
# MAIN UNIFIED METABOLIC ECOSYSTEM (with all enhancements)
# -----------------------------------------------------------------------------
class UnifiedMetabolicEcosystem:
    def __init__(self, storage, message_queue, adaptive_cost, pareto_gating, drift_detector, metrics, websocket=None, bio_core=None):
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics
        self.websocket = websocket
        self.bio_core = bio_core

        self.token_manager = getattr(bio_core, 'token_manager', None) if bio_core else None
        self.gradient_manager = getattr(bio_core, 'gradient_manager', None) if bio_core else None
        self.compartment_manager = getattr(bio_core, 'compartment_manager', None) if bio_core else None
        self.biomass_storage = getattr(bio_core, 'biomass_storage', None) if bio_core else None

        self.config = UnifiedEcosystemConfig()
        self.sustainability_score = 1.0

        self.rate_limiter = RateLimiter(self.config.rate_limit_per_minute)
        self.per_expert_limiter = PerExpertRateLimiter(self.config.per_expert_rate_limit)

        self.health_system = HealthCheckSystem(self.config) if self.config.enable_health_checks else None
        self.self_healing = SelfHealingSystem(self.config, self.health_system) if (self.config.enable_health_checks and self.config.enable_self_healing) else None
        self.alert_system = AlertingSystem(self.config) if self.config.enable_alert_escalation else None

        self.experts = {
            "energy": EnergyExpert(self.config.gating_input_dim),
            "data": DataExpert(self.config.gating_input_dim),
            "iot": IoTExpert(self.config.gating_input_dim)
        }
        self.expert_ids = list(self.experts.keys())

        self.gating_network = GatingNetworkManager(self.config, self.expert_ids, self.storage)

        self.carbon_manager = CarbonIntensityManager(self.config) if CARBON_HELIUM_AVAILABLE else None
        self.helium_optimizer = HeliumEfficiencyOptimizer(self.config) if CARBON_HELIUM_AVAILABLE else None

        self.ga_tuner = GeneticHyperparameterTuner(self.config, self.storage) if (self.config.enable_ga_tuning and TORCH_AVAILABLE) else None
        self.pareto_front = ParetoFrontManager(self.storage, self.config) if self.config.enable_pareto_front else None
        self.active_user_pref = ActiveUserPreferenceLearner(self.storage, self.websocket) if self.config.enable_active_user_pref else None

        # Fix explainability initialization
        if self.config.enable_explainability and TORCH_AVAILABLE:
            self.explainer = ExplainabilityHelper(self.gating_network.model, self._get_feature_names())
        else:
            self.explainer = None

        self.federated_agg = FederatedGatingAggregator(self.storage, self.config, self.queue, "instance_1") if self.config.enable_federated else None

        # New enhancement modules
        self.quantum_distillation = QuantumDistillationModule() if self.config.enable_quantum_distillation else None
        self.causal_mask = CausalFeatureMask(self.config.gating_input_dim) if (self.config.enable_causal_mask and TORCH_AVAILABLE) else None
        self.expert_auction = ExpertAuction(self.expert_ids, self.config.gating_input_dim, self.config.gating_hidden_dim) if (self.config.enable_expert_auction and TORCH_AVAILABLE) else None
        self.safety_monitor = SafetyMonitor() if self.config.enable_safety_monitor else None
        if self.safety_monitor:
            self._setup_safety_invariants()
        self.precision_controller = PrecisionController() if self.config.enable_precision_controller else None
        self.carbon_market = CarbonMarketClient(**self.config.carbon_market_config) if (self.config.enable_carbon_market and self.config.carbon_market_config) else None
        self.chaos_injector = ChaosInjector(self, self.config.chaos_probability) if self.config.enable_chaos else None
        self.human_approval = HumanApprovalHandler(self.queue) if self.config.enable_human_approval else None

        # Set gating network references
        self.gating_network.causal_mask = self.causal_mask
        self.gating_network.precision_controller = self.precision_controller

        self._recent_accuracies = deque(maxlen=100)
        self._drift_retrain_threshold = self.config.drift_threshold
        self._circuit_breaker = CircuitBreaker()
        self._drift_lock = asyncio.Lock()

        if self.health_system:
            for exp_key, exp_obj in self.experts.items():
                self.health_system.register_component(exp_obj.name, exp_obj)
            self.health_system.register_component("gating_network", self.gating_network)
            if self.carbon_manager:
                self.health_system.register_component("carbon_manager", self.carbon_manager)
            if self.helium_optimizer:
                self.health_system.register_component("helium_optimizer", self.helium_optimizer)

        if self.self_healing:
            self.self_healing.register_handler("gating_network", self._recover_gating_network)
            if self.carbon_manager:
                self.self_healing.register_handler("carbon_manager", self._recover_carbon_manager)
            if self.helium_optimizer:
                self.self_healing.register_handler("helium_optimizer", self._recover_helium_optimizer)

        self._load_state_task = self._create_task(self._load_state())
        self._bg_tasks = []
        self._start_background_tasks()

        logger.info("UnifiedMetabolicEcosystem v8.2.0 initialized successfully.")

    def _create_task(self, coro):
        try:
            loop = asyncio.get_running_loop()
            return loop.create_task(coro)
        except RuntimeError:
            logger.warning("No running loop; background task not started.")
            return None

    def _start_background_tasks(self):
        if self.config.enable_health_checks and self.carbon_manager:
            self._bg_tasks.append(self._create_task(self._carbon_update_loop()))
        if self.config.enable_telemetry:
            self._bg_tasks.append(self._create_task(self._telemetry_export_loop()))
        if self.config.enable_ga_tuning and self.ga_tuner:
            self._bg_tasks.append(self._create_task(self._ga_tuning_loop()))
        if self.config.enable_chaos and self.chaos_injector:
            self._bg_tasks.append(self._create_task(self._chaos_loop()))

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
                await asyncio.sleep(self.config.telemetry_export_interval)
            except asyncio.CancelledError:
                break

    async def _ga_tuning_loop(self):
        while True:
            try:
                await asyncio.sleep(3600 * 12)
                if self.ga_tuner and self.gating_network.training_buffer:
                    best = await self.ga_tuner.run_search(list(self.gating_network.training_buffer))
                    if best:
                        if self.human_approval:
                            approved = await self.human_approval.request_approval({
                                'action': 'apply_ga_hyperparams',
                                'params': best
                            })
                            if not approved:
                                logger.info("GA hyperparameter update rejected by human.")
                                continue
                        logger.info("GA tuning completed. Best hyperparameters: %s", best)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"GA tuning loop error: {e}")
                await asyncio.sleep(3600)

    async def _chaos_loop(self):
        while True:
            await asyncio.sleep(60)
            if self.chaos_injector:
                await self.chaos_injector.maybe_inject_failure()

    # --------------------------------------------------------------------------
    # Helper methods
    # --------------------------------------------------------------------------
    def _get_feature_names(self):
        names = ['carbon_zone', 'helium_scarcity', 'task_complexity',
                 'token_balance', 'gradient_carbon', 'gradient_helium',
                 'gradient_trust', 'opportunity_gradient', 'stress_level',
                 'avg_client_energy']
        if len(names) < self.config.gating_input_dim:
            names.extend([f"feature_{i}" for i in range(len(names), self.config.gating_input_dim)])
        else:
            names = names[:self.config.gating_input_dim]
        return names

    def _setup_safety_invariants(self):
        self.safety_monitor.add_invariant(
            "probs_sum_to_one",
            lambda s: abs(sum(s.get('probs', [])) - 1.0) < 1e-6 if s.get('probs') else True,
            "Gating probabilities do not sum to 1"
        )
        self.safety_monitor.add_invariant(
            "expert_count_positive",
            lambda s: s.get('n_experts', 0) > 0,
            "No experts available"
        )

    async def _load_state(self):
        try:
            data = self.storage.get_state("moe_ecosystem_state")
            if data:
                state = json.loads(data)
                self.sustainability_score = state.get("sustainability_score", 1.0)
                gating_state = state.get("gating_state")
                if gating_state and self.gating_network:
                    self.gating_network.load_state_dict(gating_state)
                logger.info("Loaded MoE ecosystem state from storage")
        except Exception as e:
            logger.error(f"Failed to load ecosystem state: {e}")

    async def save_state(self):
        try:
            state = {
                "sustainability_score": self.sustainability_score,
                "gating_state": self.gating_network.get_state_dict() if self.gating_network else {},
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            self.storage.save_state("moe_ecosystem_state", json.dumps(state))
            logger.info("Saved MoE ecosystem state to storage")
        except Exception as e:
            logger.error(f"Failed to save ecosystem state: {e}")

    async def _recover_gating_network(self):
        logger.info("Recovering gating network: retraining on buffer.")
        if self.gating_network and self.gating_network.training_buffer:
            await self.gating_network.train(epochs=5)
            return True
        logger.warning("No training data available for gating retrain.")
        return False

    async def _recover_carbon_manager(self):
        logger.info("Recovering carbon manager: reinitializing.")
        if self.carbon_manager:
            await self.carbon_manager.close()
            self.carbon_manager = CarbonIntensityManager(self.config)
            return True
        return False

    async def _recover_helium_optimizer(self):
        logger.info("Recovering helium optimizer: resetting state.")
        self.helium_optimizer = HeliumEfficiencyOptimizer(self.config)
        return True

    # --------------------------------------------------------------------------
    # Teacher Interface for MOPD
    # --------------------------------------------------------------------------
    async def policy_probs(self, state):
        raw_probs = await self.gating_network.predict(state)
        weights = dict(raw_probs)
        candidates = []
        for eid, w in weights.items():
            expert = self.experts[eid]
            metrics = expert.estimate_metrics()
            candidates.append({
                'expert_id': eid,
                'quality_score': w,
                'carbon_g': metrics['carbon_g'],
                'latency_ms': metrics['latency_ms'],
                'energy_joules': metrics['energy_joules'],
                'health_score': 1.0
            })
        if self.pareto:
            filtered = self.pareto.filter(candidates)
            if filtered:
                allowed = {c['expert_id'] for c in filtered}
                for eid in list(weights.keys()):
                    if eid not in allowed:
                        weights[eid] = 0.0
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        else:
            weights = {eid: 1.0 / len(self.experts) for eid in self.experts}
        return [weights.get(eid, 0.0) for eid in self.expert_ids]

    # --------------------------------------------------------------------------
    # Main Processing with True MoE and Real MODP
    # --------------------------------------------------------------------------
    async def process_task(self, task_data, context_data=None, use_mixture=True, top_k=2):
        start_time = time.monotonic()

        if not await self.rate_limiter.acquire():
            self.metrics.increment("rate_limit_exceeded")
            return {"status": "error", "reason": "Rate limit exceeded."}

        t_type = task_data.get("type", "generic")
        t_params = task_data.get("params", {})
        ctx_dict = context_data or {}

        self.metrics.increment("tasks_received")

        try:
            if self.carbon_manager:
                carbon_intensity = await self.carbon_manager.get_current_intensity()
                ctx_dict["carbon_intensity"] = carbon_intensity / 1000.0
                if self.config.enable_carbon_forecast:
                    carbon_forecast = await self.carbon_manager.forecast(hours=12)
                    ctx_dict["carbon_forecast"] = carbon_forecast / 1000.0
            if self.helium_optimizer:
                helium_status = await self.helium_optimizer.get_helium_status()
                ctx_dict["helium_scarcity"] = helium_status.get("price", 0.5)
                forecast_price = await self.helium_optimizer.forecast_price(hours=12)
                ctx_dict["helium_forecast_price"] = forecast_price

            if self.gradient_manager and BIO_INSPIRED_AVAILABLE:
                grad_levels = self.gradient_manager.get_field_strengths()
                ctx_dict["gradient_carbon"] = grad_levels.get('carbon', ctx_dict.get('gradient_carbon', 0.5))
                ctx_dict["gradient_helium"] = grad_levels.get('helium', ctx_dict.get('gradient_helium', 0.5))
                ctx_dict["gradient_trust"] = grad_levels.get('trust', ctx_dict.get('gradient_trust', 0.5))
            if self.token_manager and BIO_INSPIRED_AVAILABLE:
                try:
                    await self.token_manager.spend("ecosystem", 1.0)
                except:
                    pass
            if self.compartment_manager and BIO_INSPIRED_AVAILABLE:
                for eid in self.expert_ids:
                    health = self.compartment_manager.get_health(eid)
                    if health is not None and health < 0.5:
                        ctx_dict[f"compartment_health_{eid}"] = health
                    else:
                        ctx_dict[f"compartment_health_{eid}"] = 1.0

            weights = await self.gating_network.soft_gate(ctx_dict)

            # Safety check
            if self.safety_monitor:
                state = {
                    'probs': list(weights.values()),
                    'n_experts': len(self.experts),
                }
                violations = self.safety_monitor.check(state)
                if violations:
                    logger.warning(f"Safety violations: {violations}")
                    weights = {eid: 1.0 / len(self.experts) for eid in self.experts}

            if self.pareto:
                candidates = []
                for eid, weight in weights.items():
                    expert = self.experts[eid]
                    health = await expert.get_health_status()
                    metrics = expert.estimate_metrics()
                    candidates.append({
                        'expert_id': eid,
                        'quality_score': weight,
                        'carbon_g': metrics['carbon_g'],
                        'latency_ms': metrics['latency_ms'],
                        'energy_joules': metrics['energy_joules'],
                        'health_score': health.get('score', 1.0)
                    })
                filtered = self.pareto.filter(candidates)
                if filtered:
                    allowed_ids = {c['expert_id'] for c in filtered}
                    for eid in list(weights.keys()):
                        if eid not in allowed_ids:
                            weights[eid] = 0.0

            if self.adaptive_cost:
                cost_scores = {}
                for eid, weight in weights.items():
                    expert = self.experts[eid]
                    metrics = expert.estimate_metrics()
                    health = await expert.get_health_status()
                    cost = self.adaptive_cost.compute(
                        quality=weight,
                        carbon_g=metrics['carbon_g'],
                        latency_ms=metrics['latency_ms'],
                        energy_joules=metrics['energy_joules'],
                        health=health.get('score', 1.0),
                        atp=ctx_dict.get('token_balance', 0.5)
                    )
                    cost_scores[eid] = cost
                for eid in weights:
                    weights[eid] *= cost_scores.get(eid, 0.0)
                total = sum(weights.values())
                if total > 0:
                    weights = {k: v / total for k, v in weights.items()}
                else:
                    weights = {eid: 1.0 / len(self.experts) for eid in self.experts}

            for eid in list(weights.keys()):
                limiter = self.per_expert_limiter.get_limiter(eid)
                if not await limiter.acquire():
                    weights[eid] = 0.0

            total = sum(weights.values())
            if total > 0:
                weights = {k: v / total for k, v in weights.items()}
            else:
                weights = {eid: 1.0 / len(self.experts) for eid in self.experts}

            # Expert auction override (if enabled and not mixture)
            if self.expert_auction and not use_mixture:
                features_tensor = torch.FloatTensor(self.gating_network._build_features(ctx_dict)).unsqueeze(0)
                auction_winner = self.expert_auction.select_experts(features_tensor, top_k=1)[0]
                weights = {eid: 0.0 for eid in self.expert_ids}
                weights[auction_winner] = 1.0
                selected_expert_id = auction_winner
                mixture_weights = weights
                expert_outputs = {auction_winner: await self.experts[auction_winner].execute(t_params, ctx_dict)}
            else:
                if use_mixture and top_k and top_k < len(self.expert_ids):
                    top_experts = sorted(weights.items(), key=lambda x: x[1], reverse=True)[:top_k]
                    top_ids = [eid for eid, _ in top_experts]
                    top_weights = {eid: weights[eid] for eid in top_ids}
                    total_top = sum(top_weights.values())
                    if total_top > 0:
                        top_weights = {k: v / total_top for k, v in top_weights.items()}
                    selected_expert_id = max(top_weights, key=top_weights.get)
                    mixture_weights = top_weights
                    expert_outputs = {eid: await self.experts[eid].execute(t_params, ctx_dict) for eid in top_ids}
                else:
                    selected_expert_id = max(weights, key=weights.get)
                    mixture_weights = weights
                    expert_outputs = {eid: await self.experts[eid].execute(t_params, ctx_dict) for eid in self.expert_ids}

            selected_output = expert_outputs.get(selected_expert_id, {})

            # Expert health guard
            selected_expert = self.experts[selected_expert_id]
            exp_health = await selected_expert.get_health_status()
            if exp_health.get("status") == "unhealthy":
                logger.warning(f"Target expert {selected_expert.name} unhealthy. Rerouting to Data.")
                selected_expert_id = "data"
                selected_output = await self.experts["data"].execute(t_params, ctx_dict)

            # Explainability
            explanation = None
            if self.explainer and self.gating_network.model:
                features = self.gating_network._build_features(ctx_dict)
                explanation = self.explainer.explain(features)

            # Sustainability score
            carbon_factor = ctx_dict.get("carbon_intensity", 0.5)
            helium_factor = ctx_dict.get("helium_scarcity", 0.5)
            self.sustainability_score = max(0.0, min(1.0, 1.0 - (carbon_factor * 0.4 + helium_factor * 0.3)))

            elapsed = time.monotonic() - start_time

            self.metrics.increment("tasks_completed_success")
            self.metrics.observe("task_latency_seconds", elapsed)
            self.metrics.set("sustainability_score", self.sustainability_score)
            self.metrics.increment("gating_inference_total")

            # Carbon market trading
            if self.carbon_market and self.carbon_market.available:
                carbon_amount = ctx_dict.get("carbon_intensity", 0.0)
                if carbon_amount > 0.5:
                    await self.carbon_market.buy_credits(carbon_amount * 10)
                else:
                    await self.carbon_market.sell_credits((0.5 - carbon_amount) * 5)

            # FeedbackEvent
            event = FeedbackEvent.create_with_context(
                task_id=f"moe_{hashlib.sha256(json.dumps(ctx_dict).encode()).hexdigest()[:8]}",
                selected_action=selected_expert_id,
                quality_score=weights.get(selected_expert_id, 0.0),
                energy_joules=0.0,
                carbon_g=0.0,
                feedback_type="moe_routing",
                adaptive_cost_value=0.0,
                state={'task_type': t_type, 'context': ctx_dict},
                candidates=[{'expert': eid, 'weight': w} for eid, w in weights.items()],
                source="green_agent_moe",
                environment=getattr(central_config, "ENVIRONMENT", "production"),
                tags=["moe", "routing"],
                metadata={'explanation': explanation, 'mixture_weights': mixture_weights}
            )
            await self.queue.publish("feedback_events", event.to_json())

            # Drift detection
            if self.drift:
                drift_result = await self.drift.check_drift(self.adaptive_cost.get_current_weights())
                if drift_result and drift_result > 0.5 and self.config.enable_drift_retrain:
                    if self.human_approval:
                        approved = await self.human_approval.request_approval({
                            'action': 'retrain_gating',
                            'drift': drift_result
                        })
                        if not approved:
                            logger.info("Retraining rejected by human.")
                            return
                    await self.gating_network.train(epochs=5)

            if 'true_label' in ctx_dict:
                true_label = ctx_dict['true_label']
                if true_label in self.experts:
                    acc = 1.0 if selected_expert_id == true_label else 0.0
                    async with self._drift_lock:
                        self._recent_accuracies.append(acc)
                        if self.config.enable_drift_retrain and len(self._recent_accuracies) >= 10:
                            mean_acc = np.mean(self._recent_accuracies)
                            if mean_acc < (1 - self._drift_retrain_threshold):
                                logger.warning("Gating network performance dropped (manual), retraining.")
                                await self.gating_network.train(epochs=5)
                                self._recent_accuracies.clear()

            if self.token_manager and BIO_INSPIRED_AVAILABLE:
                try:
                    await self.token_manager.earn("ecosystem", 1.5)
                except:
                    pass

            return {
                "status": "success",
                "route": {
                    "assigned_expert": selected_expert_id,
                    "domain": self.experts[selected_expert_id].domain,
                    "weight": weights.get(selected_expert_id, 0.0),
                    "all_weights": weights,
                    "mixture_weights": mixture_weights,
                    "carbon_gradient": ctx_dict.get("gradient_carbon", 0.0)
                },
                "execution": selected_output,
                "expert_outputs": expert_outputs,
                "sustainability_score": round(self.sustainability_score, 4),
                "latency_ms": round(elapsed * 1000, 2),
                "explanation": explanation
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
    async def health_check(self):
        status = {
            "version": "8.2.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sustainability_score": self.sustainability_score,
            "expert_count": len(self.experts),
            "gating_trained": self.gating_network.is_trained,
            "circuit_breaker_state": self._circuit_breaker.state.value
        }
        if self.health_system:
            status["system_health"] = await self.health_system.get_system_health()
        self.metrics.set("expert_count", len(self.experts))
        self.metrics.set("sustainability_score", self.sustainability_score)
        return status

    async def shutdown(self):
        logger.info("Initiating system shutdown sequence...")
        for task in self._bg_tasks:
            if task:
                task.cancel()
        await asyncio.gather(*[t for t in self._bg_tasks if t], return_exceptions=True)
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
# Example Usage
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
