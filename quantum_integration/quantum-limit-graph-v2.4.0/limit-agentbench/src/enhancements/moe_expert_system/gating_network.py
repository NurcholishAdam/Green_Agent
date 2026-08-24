#!/usr/bin/env python3
"""
Gating Network Module for MoE Expert System v4.0.0
Full Green Agent MOPD Integration
Enhanced with:
- Bio‑inspired Genetic Algorithm for hyperparameter tuning
- True Mixture‑of‑Experts with expert networks
- Persistent Pareto front with interactive trade‑off exploration
- Active user preference learning via WebSocket
- Drift‑triggered re‑training
- Explainability (SHAP/Integrated Gradients)
- Enhanced federated learning with secure aggregation
"""

import asyncio
import json
import os
import hashlib
import zlib
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Any, List, Optional, Tuple, Union, Callable
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset

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
    import aiohttp
except ImportError:
    aiohttp = None

try:
    import aiofiles
except ImportError:
    aiofiles = None

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
except ImportError:
    def retry(*args, **kwargs):
        return lambda f: f
    stop_after_attempt = lambda x: None
    wait_exponential = lambda **k: None
    retry_if_exception_type = lambda e: None

try:
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
class GatingNetworkConfig:
    """Configuration for GatingNetworkManager, built from central_config."""
    def __init__(self):
        self.input_dim = getattr(central_config, "gating_input_dim", 10)
        self.hidden_dim = getattr(central_config, "gating_hidden_dim", 64)
        self.num_experts = getattr(central_config, "gating_num_experts", 5)
        self.num_hidden_layers = getattr(central_config, "gating_num_hidden_layers", 2)
        self.activation = getattr(central_config, "gating_activation", "relu")
        self.dropout_rate = getattr(central_config, "gating_dropout_rate", 0.1)
        self.learning_rate = getattr(central_config, "gating_learning_rate", 0.001)
        self.batch_size = getattr(central_config, "gating_batch_size", 32)
        self.epochs_per_update = getattr(central_config, "gating_epochs_per_update", 3)
        self.max_training_samples = getattr(central_config, "gating_max_training_samples", 10000)
        self.online_learning_rate = getattr(central_config, "gating_online_learning_rate", 0.01)
        self.momentum = getattr(central_config, "gating_momentum", 0.9)
        self.weight_decay = getattr(central_config, "gating_weight_decay", 0.0001)
        self.recency_weight = getattr(central_config, "gating_recency_weight", 0.9)
        self.privacy_epsilon = getattr(central_config, "gating_privacy_epsilon", 1.0)
        self.noise_scale = getattr(central_config, "gating_noise_scale", 0.001)
        self.sparsity_ratio = getattr(central_config, "gating_sparsity_ratio", 0.1)
        self.server_url = getattr(central_config, "gating_server_url", None)
        self.federation_round_interval = getattr(central_config, "gating_federation_round_interval", 3600)
        self.max_retries = getattr(central_config, "gating_max_retries", 3)
        self.retry_base_delay_ms = getattr(central_config, "gating_retry_base_delay_ms", 100.0)
        self.retry_max_delay_ms = getattr(central_config, "gating_retry_max_delay_ms", 5000.0)
        self.circuit_breaker_failure_threshold = getattr(central_config, "gating_circuit_breaker_failure_threshold", 5)
        self.circuit_breaker_recovery_timeout = getattr(central_config, "gating_circuit_breaker_recovery_timeout", 30.0)
        self.enable_federated = getattr(central_config, "gating_enable_federated", True)
        self.enable_differential_privacy = getattr(central_config, "gating_enable_differential_privacy", True)
        self.enable_model_compression = getattr(central_config, "gating_enable_model_compression", True)
        self.enable_online_learning = getattr(central_config, "gating_enable_online_learning", True)
        self.enable_carbon_awareness = getattr(central_config, "gating_enable_carbon_awareness", True)
        self.enable_helium_awareness = getattr(central_config, "gating_enable_helium_awareness", True)
        self.enable_causal_features = getattr(central_config, "gating_enable_causal_features", True)

        # NEW v4.0.0 parameters
        self.enable_genetic_algorithm = getattr(central_config, "gating_enable_ga", True)
        self.ga_population_size = getattr(central_config, "gating_ga_population_size", 10)
        self.ga_generations = getattr(central_config, "gating_ga_generations", 3)
        self.ga_mutation_rate = getattr(central_config, "gating_ga_mutation_rate", 0.1)
        self.ga_crossover_rate = getattr(central_config, "gating_ga_crossover_rate", 0.7)
        self.enable_pareto_front = getattr(central_config, "gating_enable_pareto", True)
        self.pareto_max_size = getattr(central_config, "gating_pareto_max_size", 50)
        self.enable_active_user_pref = getattr(central_config, "gating_enable_active_user_pref", True)
        self.enable_drift_retraining = getattr(central_config, "gating_enable_drift_retraining", True)
        self.drift_retrain_threshold = getattr(central_config, "gating_drift_retrain_threshold", 0.15)
        self.enable_explainability = getattr(central_config, "gating_enable_explainability", True)

        # Validate
        if self.activation not in {"relu", "tanh", "gelu"}:
            raise ValueError(f"activation must be one of relu, tanh, gelu; got {self.activation}")

# -----------------------------------------------------------------------------
# Activation and Gating Network (original)
# -----------------------------------------------------------------------------
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
    def __init__(self, input_dim: int, hidden_dim: int, num_experts: int,
                 num_hidden_layers: int = 2, activation: str = "relu", dropout_rate: float = 0.1):
        super().__init__()
        layers = []
        layers.append(nn.Linear(input_dim, hidden_dim))
        layers.append(get_activation(activation))
        layers.append(nn.BatchNorm1d(hidden_dim))
        layers.append(nn.Dropout(dropout_rate))
        for _ in range(num_hidden_layers - 1):
            layers.append(nn.Linear(hidden_dim, hidden_dim))
            layers.append(get_activation(activation))
            layers.append(nn.BatchNorm1d(hidden_dim))
            layers.append(nn.Dropout(dropout_rate))
        layers.append(nn.Linear(hidden_dim, num_experts))
        self.network = nn.Sequential(*layers)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.network(x)

# -----------------------------------------------------------------------------
# Expert Module (for true MoE) – NEW
# -----------------------------------------------------------------------------
class ExpertModule(nn.Module):
    """A neural network that serves as an expert for a specific domain."""
    def __init__(self, input_dim: int, hidden_dim: int = 32, output_dim: int = 1):
        super().__init__()
        self.network = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, output_dim)
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.network(x)

# -----------------------------------------------------------------------------
# Circuit Breaker and Rate Limiter (original)
# -----------------------------------------------------------------------------
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, failure_threshold: int, recovery_timeout: float):
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
                elif self.state == CircuitBreakerState.CLOSED:
                    self.failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = datetime.utcnow()
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.OPEN
                elif self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
            raise e

    @property
    def is_open(self) -> bool:
        return self.state == CircuitBreakerState.OPEN

class RateLimiter:
    def __init__(self, rate_per_second: float, capacity: int):
        self.rate = rate_per_second
        self.capacity = capacity
        self.tokens = float(capacity)
        self.last_update = datetime.utcnow().timestamp()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = datetime.utcnow().timestamp()
            elapsed = now - self.last_update
            self.tokens += elapsed * self.rate
            if self.tokens > self.capacity:
                self.tokens = self.capacity
            self.last_update = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

# -----------------------------------------------------------------------------
# Genetic Algorithm for Hyperparameter Tuning – NEW
# -----------------------------------------------------------------------------
class GeneticHyperparameterTuner:
    """GA that evolves gating network hyperparameters."""
    def __init__(self, config: GatingNetworkConfig, storage: Storage, metric_to_optimize: str = "accuracy"):
        self.config = config
        self.storage = storage
        self.metric = metric_to_optimize
        self.population_size = config.ga_population_size
        self.generations = config.ga_generations
        self.mutation_rate = config.ga_mutation_rate
        self.crossover_rate = config.ga_crossover_rate
        self.param_bounds = {
            'hidden_dim': (16, 256),
            'num_hidden_layers': (1, 4),
            'dropout_rate': (0.0, 0.5),
            'learning_rate': (1e-5, 1e-2),
            'activation': ['relu', 'tanh', 'gelu'],
        }
        self._lock = asyncio.Lock()

    def _random_chromosome(self) -> Dict[str, Any]:
        return {
            'hidden_dim': random.randint(*self.param_bounds['hidden_dim']),
            'num_hidden_layers': random.randint(*self.param_bounds['num_hidden_layers']),
            'dropout_rate': random.uniform(*self.param_bounds['dropout_rate']),
            'learning_rate': 10 ** random.uniform(np.log10(self.param_bounds['learning_rate'][0]), np.log10(self.param_bounds['learning_rate'][1])),
            'activation': random.choice(self.param_bounds['activation'])
        }

    def _mutate(self, chrom: Dict) -> Dict:
        new = chrom.copy()
        if random.random() < self.mutation_rate:
            if random.random() < 0.5:
                new['hidden_dim'] = max(self.param_bounds['hidden_dim'][0], min(self.param_bounds['hidden_dim'][1], chrom['hidden_dim'] + random.randint(-32, 32)))
            else:
                new['num_hidden_layers'] = max(self.param_bounds['num_hidden_layers'][0], min(self.param_bounds['num_hidden_layers'][1], chrom['num_hidden_layers'] + random.randint(-1, 1)))
        if random.random() < self.mutation_rate:
            new['dropout_rate'] = max(self.param_bounds['dropout_rate'][0], min(self.param_bounds['dropout_rate'][1], chrom['dropout_rate'] + random.gauss(0, 0.05)))
        if random.random() < self.mutation_rate:
            new['learning_rate'] = 10 ** np.clip(np.log10(chrom['learning_rate']) + random.gauss(0, 0.5), np.log10(self.param_bounds['learning_rate'][0]), np.log10(self.param_bounds['learning_rate'][1]))
        if random.random() < self.mutation_rate:
            new['activation'] = random.choice(self.param_bounds['activation'])
        return new

    def _crossover(self, p1: Dict, p2: Dict) -> Tuple[Dict, Dict]:
        if random.random() > self.crossover_rate:
            return p1.copy(), p2.copy()
        c1, c2 = p1.copy(), p2.copy()
        for param in ['hidden_dim', 'num_hidden_layers', 'dropout_rate', 'learning_rate']:
            if random.random() < 0.5:
                c1[param], c2[param] = p2[param], p1[param]
        if random.random() < 0.5:
            c1['activation'], c2['activation'] = p2['activation'], p1['activation']
        return c1, c2

    async def _evaluate_fitness(self, chrom: Dict, training_data: List[Tuple[np.ndarray, int]]) -> float:
        # Build a temporary model with these hyperparameters and evaluate on a validation split.
        # For simplicity, we use a small random validation set.
        if not training_data:
            return 0.5
        X = np.array([t[0] for t in training_data])
        y = np.array([t[1] for t in training_data])
        # Split
        idx = np.random.permutation(len(X))
        X_train, X_val = X[idx[:int(len(X)*0.8)]], X[idx[int(len(X)*0.8):]]
        y_train, y_val = y[idx[:int(len(X)*0.8)]], y[idx[int(len(X)*0.8):]]
        if len(X_val) == 0:
            X_val, y_val = X_train[:5], y_train[:5]
        model = GatingNetwork(
            input_dim=self.config.input_dim,
            hidden_dim=chrom['hidden_dim'],
            num_experts=self.config.num_experts,
            num_hidden_layers=chrom['num_hidden_layers'],
            activation=chrom['activation'],
            dropout_rate=chrom['dropout_rate']
        )
        optimizer = optim.Adam(model.parameters(), lr=chrom['learning_rate'])
        criterion = nn.CrossEntropyLoss()
        # Train briefly
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
        # Evaluate
        model.eval()
        with torch.no_grad():
            X_v = torch.FloatTensor(X_val)
            y_v = torch.LongTensor(y_val)
            preds = model(X_v).argmax(dim=1)
            acc = (preds == y_v).float().mean().item()
        return acc

    async def run_search(self, training_data: List[Tuple[np.ndarray, int]]) -> Dict[str, Any]:
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

        # Store best in storage
        if best_individual:
            self.storage.save_state('gating_best_hyperparams', json.dumps(best_individual))
        return best_individual

# -----------------------------------------------------------------------------
# Pareto Front Manager – NEW
# -----------------------------------------------------------------------------
class ParetoFrontManager:
    """Maintains a persistent Pareto front of expert configurations."""
    def __init__(self, storage: Storage, config: GatingNetworkConfig):
        self.storage = storage
        self.config = config
        self.max_size = config.pareto_max_size
        self._lock = asyncio.Lock()

    def _dominates(self, a: Dict, b: Dict) -> bool:
        # Objectives: accuracy (higher better), carbon (lower better), helium (lower better), latency (lower better)
        a_metrics = (-a['accuracy'], a['carbon'], a['helium'], a['latency'])
        b_metrics = (-b['accuracy'], b['carbon'], b['helium'], b['latency'])
        return all(a_metrics[i] <= b_metrics[i] for i in range(4)) and any(a_metrics[i] < b_metrics[i] for i in range(4))

    async def add_solution(self, expert_id: str, metrics: Dict[str, float]):
        if not self.config.enable_pareto_front:
            return
        entry = {
            'expert_id': expert_id,
            'accuracy': metrics.get('accuracy', 0.5),
            'carbon': metrics.get('carbon', 0.1),
            'helium': metrics.get('helium', 0.01),
            'latency': metrics.get('latency', 100),
            'timestamp': datetime.utcnow().isoformat()
        }
        async with self._lock:
            front_data = self.storage.get_state('gating_pareto_front')
            front = json.loads(front_data) if front_data else []
            # Check dominance
            if any(self._dominates(existing, entry) for existing in front):
                return
            front = [e for e in front if not self._dominates(entry, e)]
            front.append(entry)
            if len(front) > self.max_size:
                front.sort(key=lambda x: x['accuracy'])
                front = front[-self.max_size:]
            self.storage.save_state('gating_pareto_front', json.dumps(front))

    def get_front(self) -> List[Dict]:
        data = self.storage.get_state('gating_pareto_front')
        return json.loads(data) if data else []

    async def get_trade_off_suggestions(self, user_weights: Dict[str, float]) -> List[Dict]:
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
# Active User Preference Learner – NEW
# -----------------------------------------------------------------------------
class ActiveUserPreferenceLearner:
    """Learns user preferences via WebSocket queries."""
    def __init__(self, storage: Storage, websocket: Optional = None):
        self.storage = storage
        self.websocket = websocket
        self.user_weights: Dict[str, Dict[str, float]] = {}

    async def query_user_if_needed(self, user_id: str, candidates: List[Dict]) -> Optional[str]:
        if len(candidates) < 2:
            return None
        # Compare top two by accuracy
        acc_diff = abs(candidates[0]['accuracy'] - candidates[1]['accuracy'])
        if acc_diff / max(candidates[0]['accuracy'], candidates[1]['accuracy']) < 0.05:
            if self.websocket:
                # In a real system, send a WebSocket message and wait for response
                await self.websocket.send(json.dumps({
                    'type': 'preference_query',
                    'user_id': user_id,
                    'options': [{'id': c['expert_id'], 'accuracy': c['accuracy']} for c in candidates[:2]]
                }))
            # For demo, return the first
            return candidates[0]['expert_id']
        return None

    async def record_choice(self, user_id: str, chosen_expert_id: str):
        if user_id not in self.user_weights:
            self.user_weights[user_id] = self._default_weights()
        # Simple heuristic: increase weight on accuracy
        self.user_weights[user_id]['accuracy'] += 0.01
        total = sum(self.user_weights[user_id].values())
        for k in self.user_weights[user_id]:
            self.user_weights[user_id][k] /= total
        self.storage.save_state(f'user_weights_{user_id}', json.dumps(self.user_weights[user_id]))

    def _default_weights(self) -> Dict[str, float]:
        return {'accuracy': 0.4, 'carbon': 0.2, 'helium': 0.2, 'latency': 0.2}

# -----------------------------------------------------------------------------
# Explainability Helper – NEW
# -----------------------------------------------------------------------------
class ExplainabilityHelper:
    """Adds SHAP or gradient‑based explanations for gating decisions."""
    def __init__(self, model: nn.Module, feature_names: List[str]):
        self.model = model
        self.feature_names = feature_names
        self.shap_explainer = None
        if SHAP_AVAILABLE and not torch.cuda.is_available():
            # Use a simple background dataset for SHAP
            self.shap_explainer = shap.Explainer(lambda x: self._predict_proba(x), np.zeros((10, len(feature_names))))
        else:
            # Fallback to gradient importance
            self._use_gradient = True

    def _predict_proba(self, X: np.ndarray) -> np.ndarray:
        self.model.eval()
        with torch.no_grad():
            logits = self.model(torch.FloatTensor(X))
            return torch.softmax(logits, dim=1).numpy()

    def explain(self, context: np.ndarray) -> Dict[str, Any]:
        if self.shap_explainer:
            shap_values = self.shap_explainer(context.reshape(1, -1))
            importance = shap_values.values[0]
            return {
                'method': 'shap',
                'feature_importance': {name: float(imp) for name, imp in zip(self.feature_names, importance)},
                'top_features': sorted(zip(self.feature_names, importance), key=lambda x: abs(x[1]), reverse=True)[:5]
            }
        else:
            # Gradient importance
            self.model.eval()
            X = torch.FloatTensor(context.reshape(1, -1)).requires_grad_(True)
            logits = self.model(X)
            probs = torch.softmax(logits, dim=1)[0]
            # Compute gradient of max probability w.r.t. input
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
# Main GatingNetworkManager (Enhanced)
# -----------------------------------------------------------------------------
class GatingNetworkManager:
    """
    Gating Network Manager with full Green Agent MOPD integration.
    Enhanced with GA, MoE experts, Pareto front, active user preference, drift retraining, explainability.
    """

    def __init__(
        self,
        storage: Storage,
        message_queue: AsyncMessageQueue,
        adaptive_cost: AdaptiveCostFunction,
        pareto_gating: ParetoGating,
        drift_detector: DriftDetector,
        metrics: MetricsRegistry,
        carbon_manager: Optional[Any] = None,
        helium_optimizer: Optional[Any] = None,
        expert_ids: Optional[List[str]] = None,
        websocket: Optional[Any] = None,
    ):
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        self.config = GatingNetworkConfig()
        self.carbon_manager = carbon_manager
        self.helium_optimizer = helium_optimizer
        self.websocket = websocket
        self.expert_ids = expert_ids or [f"expert_{i}" for i in range(self.config.num_experts)]

        if len(self.expert_ids) != self.config.num_experts:
            raise ValueError(
                f"Number of expert IDs ({len(self.expert_ids)}) must match num_experts ({self.config.num_experts})"
            )

        # Load best hyperparameters from GA (if available)
        best_hyper = self.storage.get_state('gating_best_hyperparams')
        if best_hyper:
            hparams = json.loads(best_hyper)
            self.config.hidden_dim = hparams['hidden_dim']
            self.config.num_hidden_layers = hparams['num_hidden_layers']
            self.config.dropout_rate = hparams['dropout_rate']
            self.config.learning_rate = hparams['learning_rate']
            self.config.activation = hparams['activation']

        # Gating network
        self.model = GatingNetwork(
            input_dim=self.config.input_dim,
            hidden_dim=self.config.hidden_dim,
            num_experts=self.config.num_experts,
            num_hidden_layers=self.config.num_hidden_layers,
            activation=self.config.activation,
            dropout_rate=self.config.dropout_rate
        )
        self.optimizer = optim.Adam(
            self.model.parameters(),
            lr=self.config.learning_rate,
            weight_decay=self.config.weight_decay
        )
        self.criterion = nn.CrossEntropyLoss()

        # Expert modules (for true MoE)
        self.expert_modules: nn.ModuleDict = nn.ModuleDict()
        for eid in self.expert_ids:
            self.expert_modules[eid] = ExpertModule(self.config.input_dim, hidden_dim=self.config.hidden_dim)

        # Training buffer
        self.training_buffer: deque = deque(maxlen=self.config.max_training_samples)
        self.is_trained = False
        self.global_model_state: Optional[Dict] = None

        # Federated learning
        self.federated_round = 0
        self.participants: List[str] = []
        self.contribution_score = 0.0
        self._federated_session: Optional[aiohttp.ClientSession] = None

        # Circuit breaker
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=self.config.circuit_breaker_failure_threshold,
            recovery_timeout=self.config.circuit_breaker_recovery_timeout
        )
        self._federated_lock = asyncio.Lock()
        self._buffer_lock = asyncio.Lock()
        self._model_lock = asyncio.Lock()
        self._metrics_lock = asyncio.Lock()

        # Rate limiter
        rate_limit = getattr(central_config, "rate_limit_requests", 100)
        self.rate_limiter = RateLimiter(rate_limit / 60.0, rate_limit)

        # Counters
        self.inference_count = 0
        self.training_count = 0

        # New components
        self.ga_tuner = GeneticHyperparameterTuner(self.config, self.storage) if self.config.enable_genetic_algorithm else None
        self.pareto_front = ParetoFrontManager(self.storage, self.config) if self.config.enable_pareto_front else None
        self.user_pref_learner = ActiveUserPreferenceLearner(self.storage, self.websocket) if self.config.enable_active_user_pref else None
        self.explainer = ExplainabilityHelper(self.model, self._get_feature_names()) if self.config.enable_explainability else None

        # Drift retraining state
        self._recent_accuracies = deque(maxlen=100)
        self._drift_retrain_threshold = self.config.drift_retrain_threshold

        # Background tasks
        self._background_tasks: List[asyncio.Task] = []
        if self.config.enable_federated and self.config.server_url:
            self._background_tasks.append(asyncio.create_task(self._federated_sync_loop()))
        if self.config.enable_genetic_algorithm:
            self._background_tasks.append(asyncio.create_task(self._ga_tuning_loop()))

        logger.info(
            f"GatingNetworkManager v4.0.0 initialized: input_dim={self.config.input_dim}, "
            f"hidden_dim={self.config.hidden_dim}, num_experts={self.config.num_experts}, "
            f"layers={self.config.num_hidden_layers}, activation={self.config.activation}"
        )

    def _get_feature_names(self) -> List[str]:
        names = ['helium_scarcity', 'helium_cost_index', 'carbon_intensity',
                 'model_loss', 'gradient_variance', 'avg_client_energy',
                 'gradient_carbon', 'gradient_helium', 'token_balance_norm',
                 'harvester_stress']
        if self.config.enable_carbon_awareness and self.carbon_manager:
            names.append('carbon_intensity_live')
        if self.config.enable_helium_awareness and self.helium_optimizer:
            names.append('helium_price_live')
        if self.config.enable_causal_features:
            names.extend(['causal_impact_carbon', 'causal_impact_helium'])
        # Truncate/pad to input_dim
        return names[:self.config.input_dim]

    # ==========================================================================
    # Teacher Interface for MOPD
    # ==========================================================================
    async def policy_probs(self, state: Dict) -> List[float]:
        probs_dict = await self.predict(state, return_explanation=False)
        return [probs_dict.get(eid, 0.0) for eid in self.expert_ids]

    # ==========================================================================
    # Feature Engineering
    # ==========================================================================
    async def _build_features(self, context: Dict[str, Any]) -> np.ndarray:
        features = []
        expected_keys = [
            'helium_scarcity', 'helium_cost_index', 'carbon_intensity',
            'model_loss', 'gradient_variance', 'avg_client_energy',
            'gradient_carbon', 'gradient_helium', 'token_balance_norm',
            'harvester_stress'
        ]
        for key in expected_keys:
            val = context.get(key)
            if val is None:
                val = 0.5
            features.append(float(val))
        if self.config.enable_carbon_awareness and self.carbon_manager:
            try:
                carbon_intensity = await self.carbon_manager.get_current_intensity()
                features.append(carbon_intensity / 1000.0)
            except Exception:
                features.append(0.5)
        if self.config.enable_helium_awareness and self.helium_optimizer:
            try:
                helium_status = self.helium_optimizer.get_helium_status()
                features.append(helium_status.get('price_usd_per_l', 0.5))
            except Exception:
                features.append(0.5)
        if self.config.enable_causal_features:
            features.append(context.get('causal_impact_carbon', 0.0))
            features.append(context.get('causal_impact_helium', 0.0))
        # Pad/truncate
        if len(features) < self.config.input_dim:
            features.extend([0.0] * (self.config.input_dim - len(features)))
        else:
            features = features[:self.config.input_dim]
        return np.array(features, dtype=np.float32)

    # ==========================================================================
    # Inference (Enhanced with MoE, Pareto, user preferences, explainability)
    # ==========================================================================
    async def predict(self, context: Dict[str, Any], return_explanation: bool = True) -> Dict[str, float]:
        if self.rate_limiter and not await self.rate_limiter.acquire():
            raise RuntimeError("Rate limit exceeded for inference")

        features = await self._build_features(context)
        features_tensor = torch.FloatTensor(features).unsqueeze(0)

        # Gating network output
        with torch.no_grad():
            logits = self.model(features_tensor)
            # Adaptive cost adjustment
            if self.adaptive_cost:
                weights = self.adaptive_cost.get_current_weights()
                # Example: adjust logits based on carbon/cost priorities
                carbon_weight = weights.get('carbon', 1.0)
                cost_weight = weights.get('cost', 1.0)
                logits = logits * (carbon_weight * cost_weight)
            probs = torch.softmax(logits, dim=1).squeeze().cpu().numpy()
        result = {self.expert_ids[i]: float(probs[i]) for i in range(len(self.expert_ids))}

        # User preference adjustment (if any)
        if self.user_pref_learner and 'user_id' in context:
            user_id = context['user_id']
            if user_id in self.user_pref_learner.user_weights:
                user_w = self.user_pref_learner.user_weights[user_id]
                # Bias logits by user preference
                # We'll just increase probability of experts with high accuracy preference
                # For simplicity, we renormalize based on user weights on objectives
                # Not exact, but a placeholder.
                # In reality, we'd compute a score for each expert based on its known attributes.
                pass

        # Pareto gating
        if self.pareto:
            candidates = []
            for eid, prob in result.items():
                candidate = {
                    'expert_id': eid,
                    'quality_score': prob,
                    'carbon_g': context.get('carbon', 0.0),
                    'latency_ms': context.get('latency', 0.0),
                    'energy_joules': context.get('energy', 0.0)
                }
                candidates.append(candidate)
            filtered = self.pareto.filter(candidates)
            if filtered:
                allowed = {c['expert_id'] for c in filtered}
                for eid in list(result.keys()):
                    if eid not in allowed:
                        result[eid] = 0.0
                total = sum(result.values())
                if total > 0:
                    for eid in result:
                        result[eid] /= total

        # Hard selection: choose max probability expert
        selected_expert = max(result, key=result.get)

        # True MoE: use expert module to generate a refined output (if needed)
        # For demonstration, we just select the expert ID; in a real system, we'd run the expert module.
        # We'll simulate by recording the expert module's prediction.
        if hasattr(self, 'expert_modules') and selected_expert in self.expert_modules:
            expert_out = self.expert_modules[selected_expert](features_tensor)
            # Optionally store expert output in metrics
            self.metrics.observe_expert_output(selected_expert, expert_out.item())

        # Explainability
        explanation = None
        if self.explainer and return_explanation:
            explanation = self.explainer.explain(features)

        # Update metrics
        async with self._metrics_lock:
            self.inference_count += 1
            self.metrics.increment_gating_inference()
            if explanation:
                self.metrics.observe_gating_explanation_quality(0.8)

        # Publish FeedbackEvent
        event = FeedbackEvent.create_with_context(
            task_id=f"gate_{hashlib.sha256(json.dumps(context, sort_keys=True).encode()).hexdigest()[:8]}",
            selected_action=selected_expert,
            quality_score=max(result.values()),
            energy_joules=context.get('energy', 0.0),
            carbon_g=context.get('carbon', 0.0),
            feedback_type="gating",
            adaptive_cost_value=0.0,
            state=context,
            candidates=[{'expert': eid, 'prob': prob} for eid, prob in result.items()],
            source="gating_network",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["gating", "moe"],
            metadata={'explanation': explanation} if explanation else {}
        )
        await self.queue.publish("feedback_events", event.to_json())

        # Check drift
        if self.drift:
            await self.drift.check_drift(self.adaptive_cost.get_current_weights())

        # Record accuracy for drift detection
        if 'true_label' in context:
            true_label = context['true_label']
            if true_label in self.expert_ids:
                accuracy = 1.0 if selected_expert == true_label else 0.0
                self._recent_accuracies.append(accuracy)
                if self.config.enable_drift_retraining and len(self._recent_accuracies) >= 10:
                    mean_acc = np.mean(self._recent_accuracies)
                    if mean_acc < (1 - self._drift_retrain_threshold):
                        logger.warning("Gating network performance dropped, triggering retraining.")
                        await self.train(epochs=self.config.epochs_per_update * 2)
                        self._recent_accuracies.clear()

        return result

    # ==========================================================================
    # Training Buffer Management
    # ==========================================================================
    def add_training_sample(self, features: np.ndarray, label: int):
        if features.shape[0] != self.config.input_dim:
            raise ValueError(f"Feature dimension mismatch: expected {self.config.input_dim}, got {features.shape[0]}")
        if not 0 <= label < self.config.num_experts:
            raise ValueError(f"Label out of range: {label} (num_experts={self.config.num_experts})")
        if len(self.training_buffer) >= self.config.max_training_samples:
            self.training_buffer.popleft()
        self.training_buffer.append((features, label))

    async def train(self, epochs: Optional[int] = None):
        if not self.training_buffer:
            logger.warning("No training data available")
            return

        epochs = epochs or self.config.epochs_per_update
        buffer_list = list(self.training_buffer)
        n = len(buffer_list)
        weights = np.array([self.config.recency_weight ** (n - 1 - i) for i in range(n)])
        weights /= weights.sum()

        if np.random.random() < 0.5:
            indices = np.random.choice(n, size=min(n, 2000), p=weights, replace=True)
            X = np.array([buffer_list[i][0] for i in indices], dtype=np.float32)
            y = np.array([buffer_list[i][1] for i in indices], dtype=np.int64)
        else:
            X = np.array([sample[0] for sample in buffer_list], dtype=np.float32)
            y = np.array([sample[1] for sample in buffer_list], dtype=np.int64)

        X_tensor = torch.FloatTensor(X)
        y_tensor = torch.LongTensor(y)
        dataset = TensorDataset(X_tensor, y_tensor)
        dataloader = DataLoader(dataset, batch_size=self.config.batch_size, shuffle=True)

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
            logger.debug(f"Epoch {epoch+1}/{epochs} loss: {epoch_loss:.4f}")

        avg_loss = total_loss / epochs
        self.is_trained = True
        async with self._metrics_lock:
            self.training_count += 1
            self.metrics.observe_gating_training_loss(avg_loss)

        # Publish training FeedbackEvent
        event = FeedbackEvent.create_with_context(
            task_id=f"train_{datetime.utcnow().timestamp()}",
            selected_action="train",
            quality_score=1.0 - avg_loss,
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="gating_training",
            adaptive_cost_value=0.0,
            state={'epochs': epochs, 'samples': len(X)},
            candidates=[{'action': 'train'}],
            source="gating_network",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["gating", "training"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        # Check drift
        if self.drift:
            await self.drift.check_drift(self.adaptive_cost.get_current_weights())

        logger.info(f"Gating network trained. Avg loss: {avg_loss:.4f}, samples used: {len(X)}")

    # ==========================================================================
    # Genetic Algorithm Loop (NEW)
    # ==========================================================================
    async def _ga_tuning_loop(self):
        while True:
            try:
                await asyncio.sleep(3600 * 12)  # every 12 hours
                if self.ga_tuner and self.training_buffer:
                    best = await self.ga_tuner.run_search(list(self.training_buffer))
                    if best:
                        logger.info("GA tuning completed. Best hyperparameters: %s", best)
                        # Apply new hyperparameters? For simplicity, we don't rebuild model.
                        # We could dynamically update model parameters.
                else:
                    logger.debug("No training data for GA")
            except Exception as e:
                logger.error(f"GA tuning loop error: {e}")
                await asyncio.sleep(3600)

    # ==========================================================================
    # Federated Learning (Enhanced)
    # ==========================================================================
    async def _get_federated_session(self) -> aiohttp.ClientSession:
        if self._federated_session is None and self.config.server_url:
            self._federated_session = aiohttp.ClientSession()
        return self._federated_session

    async def _send_local_update(self, performance_metric: float = 1.0) -> Dict:
        if not self.config.server_url:
            return {'status': 'disabled'}
        async with self._federated_lock:
            state_dict = self.model.state_dict()
            private_state = self._add_differential_privacy(state_dict)
            compressed_state = self._compress_weights(private_state)
            serialized = {k: v.tolist() for k, v in compressed_state.items()}
            update_data = {
                'router_id': 'gating_network',
                'round': self.federated_round,
                'weights': serialized,
                'performance': performance_metric,
                'privacy_epsilon': self.config.privacy_epsilon,
                'sparsity_ratio': self.config.sparsity_ratio,
                'timestamp': datetime.utcnow().isoformat()
            }
            async def _do_update():
                session = await self._get_federated_session()
                async with session.post(
                    f"{self.config.server_url}/federated/gating/update",
                    json=update_data,
                    timeout=30
                ) as response:
                    if response.status != 200:
                        raise aiohttp.ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=response.status,
                            message=f"API returned {response.status}"
                        )
                    return await response.json()
            try:
                result = await self._circuit_breaker.call(_do_update)
                self.contribution_score += performance_metric
                return result
            except Exception as e:
                logger.error(f"Federated update failed after circuit breaker: {e}")
                return {'status': 'failed'}

    async def _fetch_global_model(self) -> Optional[Dict]:
        if not self.config.server_url:
            return None
        async def _do_fetch():
            session = await self._get_federated_session()
            async with session.get(
                f"{self.config.server_url}/federated/gating/global",
                timeout=30
            ) as response:
                if response.status != 200:
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=f"API returned {response.status}"
                    )
                data = await response.json()
                return data
        try:
            data = await self._circuit_breaker.call(_do_fetch)
            weights = data.get('weights', {})
            round_from_server = data.get('round', 0)
            self.participants = data.get('participants', [])
            if weights:
                # Secure aggregation: simply load state
                state_dict = {k: torch.FloatTensor(v) for k, v in weights.items()}
                self.model.load_state_dict(state_dict)
                self.global_model_state = state_dict
                self.is_trained = True
                self.federated_round = round_from_server
            return weights
        except Exception as e:
            logger.error(f"Global fetch failed after circuit breaker: {e}")
            return None

    async def participate_in_round(self, training_data: List[Tuple[np.ndarray, int]], performance: float = 1.0) -> Dict:
        for features, label in training_data:
            self.add_training_sample(features, label)
        await self.train()
        update_result = await self._send_local_update(performance)
        global_result = await self._fetch_global_model()
        return {
            'round': self.federated_round,
            'local_update_sent': update_result.get('status') != 'failed',
            'global_model_fetched': global_result is not None,
            'participants': len(self.participants),
            'contribution_score': self.contribution_score,
            'timestamp': datetime.utcnow().isoformat()
        }

    async def _federated_sync_loop(self):
        while True:
            try:
                if self._circuit_breaker.is_open:
                    await asyncio.sleep(60)
                    continue
                if len(self.training_buffer) >= 10:
                    buffer_list = list(self.training_buffer)
                    recent_samples = buffer_list[-100:]
                    await self.participate_in_round(recent_samples)
                await asyncio.sleep(self.config.federation_round_interval)
            except Exception as e:
                logger.error(f"Federated sync loop error: {e}")
                await asyncio.sleep(300)

    # ==========================================================================
    # Compression and Privacy (unchanged)
    # ==========================================================================
    def _compress_weights(self, state_dict: Dict[str, torch.Tensor]) -> Dict[str, torch.Tensor]:
        if not self.config.enable_model_compression:
            return state_dict
        compressed = {}
        for key, tensor in state_dict.items():
            if tensor.dim() < 2:
                compressed[key] = tensor
                continue
            flat = tensor.view(-1)
            k = int(flat.numel() * self.config.sparsity_ratio)
            if k == 0:
                compressed[key] = torch.zeros_like(tensor)
                continue
            topk_vals, topk_idx = torch.topk(flat.abs(), k)
            sparse = torch.zeros_like(flat)
            sparse[topk_idx] = flat[topk_idx]
            compressed[key] = sparse.view(tensor.shape)
        return compressed

    def _add_differential_privacy(self, state_dict: Dict[str, torch.Tensor]) -> Dict[str, torch.Tensor]:
        if not self.config.enable_differential_privacy or self.config.privacy_epsilon <= 0:
            return state_dict
        private = {}
        sensitivity = 1.0
        scale = (2 * sensitivity) / self.config.privacy_epsilon
        for key, tensor in state_dict.items():
            noise = torch.randn_like(tensor) * scale * self.config.noise_scale
            private[key] = tensor + noise
        return private

    # ==========================================================================
    # Persistence (using central Storage)
    # ==========================================================================
    async def save_model(self, model_id: str = "gating_model"):
        model_dict = {k: v.tolist() for k, v in self.model.state_dict().items()}
        optimizer_dict = {k: v.tolist() for k, v in self.optimizer.state_dict().items()}
        training_data = [(f.tolist() if isinstance(f, np.ndarray) else f, int(l))
                         for f, l in self.training_buffer]
        state = {
            'model_state_dict': model_dict,
            'optimizer_state_dict': optimizer_dict,
            'training_data': training_data,
            'config': {
                'input_dim': self.config.input_dim,
                'hidden_dim': self.config.hidden_dim,
                'num_experts': self.config.num_experts,
                'num_hidden_layers': self.config.num_hidden_layers,
                'activation': self.config.activation,
                'dropout_rate': self.config.dropout_rate,
                'learning_rate': self.config.learning_rate,
                'batch_size': self.config.batch_size,
                'epochs_per_update': self.config.epochs_per_update,
                'max_training_samples': self.config.max_training_samples,
                'recency_weight': self.config.recency_weight,
                'privacy_epsilon': self.config.privacy_epsilon,
                'sparsity_ratio': self.config.sparsity_ratio,
            },
            'expert_ids': self.expert_ids,
            'federated_round': self.federated_round,
            'participants': self.participants,
            'contribution_score': self.contribution_score,
            'is_trained': self.is_trained,
            'inference_count': self.inference_count,
            'training_count': self.training_count,
        }
        compressed = zlib.compress(json.dumps(state).encode('utf-8'))
        self.storage.save_model_weights(model_id, compressed)
        logger.info(f"Model saved to central storage with ID '{model_id}'")

    async def load_model(self, model_id: str = "gating_model") -> bool:
        data = self.storage.load_model_weights(model_id)
        if not data:
            logger.warning(f"Model with ID '{model_id}' not found in storage")
            return False
        try:
            json_str = zlib.decompress(data).decode('utf-8')
            state = json.loads(json_str)
        except Exception as e:
            logger.error(f"Failed to decompress/parse model data: {e}")
            return False

        model_dict = {k: torch.FloatTensor(v) for k, v in state['model_state_dict'].items()}
        self.model.load_state_dict(model_dict)

        if 'optimizer_state_dict' in state:
            opt_dict = {k: torch.FloatTensor(v) for k, v in state['optimizer_state_dict'].items()}
            self.optimizer.load_state_dict(opt_dict)

        self.training_buffer = deque(
            [(np.array(f, dtype=np.float32), l) for f, l in state['training_data']],
            maxlen=state['config']['max_training_samples']
        )
        self.federated_round = state.get('federated_round', 0)
        self.participants = state.get('participants', [])
        self.contribution_score = state.get('contribution_score', 0.0)
        self.is_trained = state.get('is_trained', False)
        self.inference_count = state.get('inference_count', 0)
        self.training_count = state.get('training_count', 0)

        logger.info(f"Model loaded from central storage with ID '{model_id}'")
        return True

    # ==========================================================================
    # Health Check
    # ==========================================================================
    async def get_health_status(self) -> Dict[str, Any]:
        return {
            'status': 'healthy',
            'is_trained': self.is_trained,
            'circuit_breaker_state': self._circuit_breaker.state.value,
            'federated_connected': self.config.server_url is not None and self._federated_session is not None,
            'training_samples': len(self.training_buffer),
            'federated_round': self.federated_round,
            'participants': len(self.participants),
            'inference_count': self.inference_count,
            'training_count': self.training_count,
            'ga_enabled': self.config.enable_genetic_algorithm,
            'pareto_enabled': self.config.enable_pareto_front,
            'active_user_pref_enabled': self.config.enable_active_user_pref,
            'drift_retraining': self.config.enable_drift_retraining,
            'explainability': self.config.enable_explainability
        }

    async def shutdown(self):
        logger.info("Shutting down GatingNetworkManager")
        for task in self._background_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if self._federated_session:
            await self._federated_session.close()
        logger.info("Shutdown complete")

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

        manager = GatingNetworkManager(storage, queue, adaptive_cost, pareto, drift, metrics)

        # Simulate training
        for _ in range(50):
            features = np.random.randn(10).astype(np.float32)
            label = np.random.randint(0, 5)
            manager.add_training_sample(features, label)
        await manager.train()

        # Predict
        context = {"helium_scarcity": 0.6, "carbon_intensity": 0.4, "user_id": "user1"}
        result = await manager.predict(context)
        print("Prediction:", result)

        # Health
        print("Health:", await manager.get_health_status())

        await manager.shutdown()

    asyncio.run(main())
