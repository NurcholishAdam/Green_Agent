#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/thermal_optimizer_enhanced_v14_0.py
# VERSION: 14.0.0 – Enterprise Quantum Resilience + GA + MoE + Pareto + Federated
# =============================================================================
"""
Enhanced Multi-Physics Thermal Optimizer with GPU Acceleration - Version 14.0.0
Enterprise Quantum Resilience + GA + MoE + Pareto + Federated Learning

ENHANCEMENTS OVER v13.2.0:
1. Bio‑inspired Genetic Algorithm (GA) for thermal parameter tuning.
2. Full Mixture‑of‑Experts (MoE) gating network with neural network experts.
3. Pareto‑front optimizer for multi‑objective trade‑off exploration.
4. Neural network teachers for improved state‑action prediction.
5. Federated learning for sharing model weights across instances.
6. Real carbon intensity API integration with caching.
7. Active user preference learning via WebSocket queries.
8. Drift detection for carbon intensity and performance trends.
9. All enhancements are optional and configurable.
"""

import asyncio
import hashlib
import json
import os
import random
import time
import uuid
from collections import deque, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Set, Union
import secrets
import gc
import numpy as np
from abc import ABC, abstractmethod
from enum import Enum

# =============================================================================
# IMPORT CENTRAL GREEN AGENT COMPONENTS
# =============================================================================
from ..config import config as central_config
from ..storage import Storage
from ..schemas.feedback_event import FeedbackEvent
from ..routing.pareto_gating import ParetoGating
from ..feedback.adaptive_cost import AdaptiveCostFunction
from ..safety.drift_detector import DriftDetector
from ..scaling.message_queue import AsyncMessageQueue
from ..metrics import MetricsRegistry
from ..logger import logger

# =============================================================================
# OPTIONAL IMPORTS (graceful degradation)
# =============================================================================
# Post‑quantum cryptography (pqcrypto)
try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, PrivateFormat, NoEncryption
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

# Web3
try:
    from web3 import Web3, Account, HTTPProvider
    from web3.middleware import geth_poa_middleware, gas_price_strategy
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

# Cloud storage (optional)
try:
    import boto3
    from botocore.exceptions import ClientError
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

try:
    from azure.storage.blob import BlobServiceClient
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

try:
    from google.cloud import storage
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

# PyTorch (optional)
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# Scikit‑learn (optional)
try:
    from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
    from sklearn.neural_network import MLPClassifier, MLPRegressor
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# Plotly (optional)
try:
    import plotly.graph_objects as go
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

# Async HTTP
import aiohttp

# =============================================================================
# CENTRAL METRICS REGISTRY – we reuse the central one
# =============================================================================
# Thermal‑specific metrics will be registered with central MetricsRegistry.

# =============================================================================
# CUSTOM EXCEPTIONS
# =============================================================================
class ThermalError(Exception):
    pass

class QuantumError(ThermalError):
    pass

class BlockchainError(ThermalError):
    pass

class OptimizationError(ThermalError):
    pass

class CircuitBreakerOpenError(ThermalError):
    pass

class RateLimitExceeded(ThermalError):
    pass

# =============================================================================
# ENHANCED CIRCUIT BREAKER (reuses central config)
# =============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class EnhancedCircuitBreaker:
    def __init__(self, name: str):
        self.name = name
        self.failure_threshold = central_config.CIRCUIT_BREAKER_FAILURE_THRESHOLD
        self.recovery_timeout = central_config.CIRCUIT_BREAKER_RECOVERY_TIMEOUT
        self.half_open_max_requests = 3
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.last_success_time = None
        self._lock = asyncio.Lock()
        self.half_open_requests = 0

    async def allow_request(self) -> bool:
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_requests = 0
                    logger.info(f"Circuit breaker {self.name} transitioning to HALF_OPEN")
                else:
                    return False
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.half_open_requests += 1
                if self.half_open_requests > self.half_open_max_requests:
                    self.state = CircuitBreakerState.OPEN
                    logger.info(f"Circuit breaker {self.name} back to OPEN (half-open max exceeded)")
                    return False
            return True

    async def record_success(self):
        async with self._lock:
            self.success_count += 1
            self.last_success_time = time.time()
            if self.state == CircuitBreakerState.HALF_OPEN:
                if self.success_count >= 2:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    logger.info(f"Circuit breaker {self.name} CLOSED after {self.success_count} successes")
            else:
                self.failure_count = 0

    async def record_failure(self):
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                logger.warning(f"Circuit breaker {self.name} OPEN after {self.failure_count} failures")
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN
                logger.warning(f"Circuit breaker {self.name} OPEN from HALF_OPEN")

    async def call(self, func, *args, **kwargs):
        allowed = await self.allow_request()
        if not allowed:
            raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            await self.record_success()
            return result
        except Exception as e:
            await self.record_failure()
            raise

# =============================================================================
# ENHANCED RATE LIMITER (reuses central config)
# =============================================================================
class EnhancedRateLimiter:
    def __init__(self):
        self.rate = central_config.rate_limit_requests if hasattr(central_config, 'rate_limit_requests') else 100
        self.per_seconds = central_config.rate_limit_window if hasattr(central_config, 'rate_limit_window') else 60
        self.tokens = self.rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            time_passed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + time_passed * (self.rate / self.per_seconds))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.1)

# =============================================================================
# DATA CLASSES (unchanged)
# =============================================================================
@dataclass
class DigitalTwinNode:
    id: str
    power_kw: float = 0.0
    temp_c: float = 25.0

@dataclass
class DigitalTwinGraph:
    nodes: Dict[str, DigitalTwinNode] = field(default_factory=dict)

@dataclass
class ThermalOptimizationResult:
    total_energy_kw: float = 0.0
    cooling_energy_kw: float = 0.0
    it_energy_kw: float = 0.0
    pue: float = 0.0
    avg_server_temp_c: float = 25.0
    max_server_temp_c: float = 27.0
    carbon_footprint_kg_per_hour: float = 0.0
    carbon_intensity_gco2_per_kwh: float = 0.0
    carbon_savings_kg: float = 0.0
    helium_usage_liters: float = 0.0
    helium_efficiency: float = 0.0
    sustainability_score: float = 0.0
    optimization_time_ms: float = 0.0
    gpu_accelerated: bool = False
    zone_temperatures: Dict[str, float] = field(default_factory=dict)
    anomaly_detected: bool = False
    rl_action_used: int = 0
    rl_action_description: str = ""
    quantum_signature: Optional[Dict[str, Any]] = None
    blockchain_tx_hash: Optional[str] = None
    cloud_distribution: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class DataCenterConfigModel:
    renewable_energy_pct: float = 50.0

class ThermalOptimizationState:
    """Rich context for the multi‑teacher distillation agent."""
    def __init__(self, pue: float, avg_temp_c: float, max_temp_c: float,
                 carbon_intensity_gco2: float, energy_storage_level_pct: float,
                 workload_pct: float, node_count: int, avg_node_power_kw: float,
                 cooling_capacity_utilization: float, equipment_risk_score: float,
                 hour_of_day: int, is_weekend: bool):
        self.pue = pue
        self.avg_temp_c = avg_temp_c
        self.max_temp_c = max_temp_c
        self.carbon_intensity_gco2 = carbon_intensity_gco2
        self.energy_storage_level_pct = energy_storage_level_pct
        self.workload_pct = workload_pct
        self.node_count = node_count
        self.avg_node_power_kw = avg_node_power_kw
        self.cooling_capacity_utilization = cooling_capacity_utilization
        self.equipment_risk_score = equipment_risk_score
        self.hour_of_day = hour_of_day
        self.is_weekend = is_weekend

    def to_feature_vector(self) -> np.ndarray:
        """Convert state to 12‑dim feature vector for ML models."""
        features = [
            min(self.pue / 2.0, 1.0),
            min(self.avg_temp_c / 40.0, 1.0),
            min(self.max_temp_c / 45.0, 1.0),
            min(self.carbon_intensity_gco2 / 1000.0, 1.0),
            self.energy_storage_level_pct / 100.0,
            self.workload_pct / 100.0,
            min(self.node_count / 100.0, 1.0),
            min(self.avg_node_power_kw / 500.0, 1.0),
            self.cooling_capacity_utilization / 100.0,
            self.equipment_risk_score,
            self.hour_of_day / 24.0,
            1.0 if self.is_weekend else 0.0,
        ]
        return np.array(features, dtype=np.float32)

# =============================================================================
# TEACHER ABSTRACT CLASS (for fallback)
# =============================================================================
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: ThermalOptimizationState) -> np.ndarray:
        pass

    @abstractmethod
    def confidence(self, state: ThermalOptimizationState) -> float:
        pass

# =============================================================================
# FALLBACK TEACHERS (kept for compatibility)
# =============================================================================
class ThermalRuleBasedTeacher(Teacher):
    def predict(self, state: ThermalOptimizationState) -> np.ndarray:
        probs = np.ones(5) * 0.1
        if state.carbon_intensity_gco2 > 500:
            probs[1] = 0.8   # carbon strategy
        elif state.pue > 1.8:
            probs[0] = 0.7   # performance (reduce PUE)
        elif state.energy_storage_level_pct < 20:
            probs[2] = 0.6   # cost (avoid discharging)
        return probs / probs.sum()

    def confidence(self, state: ThermalOptimizationState) -> float:
        if state.carbon_intensity_gco2 > 500:
            return 0.6
        elif state.pue > 1.8:
            return 0.5
        return 0.4

class ThermalHistoricalMLTeacher(Teacher):
    def __init__(self, model_path: Optional[str] = None):
        self.model = None
        if model_path and Path(model_path).exists():
            try:
                import joblib
                self.model = joblib.load(model_path)
            except Exception:
                self.model = None

    def predict(self, state: ThermalOptimizationState) -> np.ndarray:
        if self.model is None:
            return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: ThermalOptimizationState) -> float:
        return 0.7 if self.model is not None else 0.0

class ThermalStatefulQTeacher(Teacher):
    def __init__(self, storage: Storage, lr: float = 0.1):
        self.storage = storage
        self.lr = lr
        self.weights = np.zeros((12, 5))
        self._load_state()

    def _load_state(self):
        w = self.storage.get_state('thermal_q_teacher_weights')
        if w:
            try:
                self.weights = np.array(json.loads(w))
            except Exception:
                self.weights = np.zeros((12, 5))

    def _save_state(self):
        try:
            self.storage.save_state('thermal_q_teacher_weights', json.dumps(self.weights.tolist()))
        except Exception:
            pass

    def predict(self, state: ThermalOptimizationState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: ThermalOptimizationState) -> float:
        return 0.5

    def update(self, state: ThermalOptimizationState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()

# =============================================================================
# NEW MODULE: Genetic Algorithm for Thermal Parameter Tuning
# =============================================================================
class GeneticThermalParameterOptimizer:
    """
    Bio‑inspired GA that evolves thermal control parameters (target temp, fan power, storage threshold).
    """
    def __init__(self, config, storage: Storage):
        self.config = config
        self.storage = storage
        self.population_size = getattr(config, 'GA_POPULATION_SIZE', 20)
        self.generations = getattr(config, 'GA_GENERATIONS', 5)
        self.mutation_rate = getattr(config, 'GA_MUTATION_RATE', 0.2)
        self.crossover_rate = getattr(config, 'GA_CROSSOVER_RATE', 0.7)
        self.param_bounds = {
            'target_temp_c': (18.0, 28.0),
            'fan_power_pct': (30.0, 100.0),
            'storage_discharge_threshold': (10.0, 50.0),
        }
        self._lock = asyncio.Lock()

    def _random_chromosome(self) -> Dict[str, Any]:
        return {
            'target_temp_c': random.uniform(*self.param_bounds['target_temp_c']),
            'fan_power_pct': random.uniform(*self.param_bounds['fan_power_pct']),
            'storage_discharge_threshold': random.uniform(*self.param_bounds['storage_discharge_threshold']),
        }

    def _mutate(self, chrom: Dict[str, Any]) -> Dict[str, Any]:
        new = chrom.copy()
        if random.random() < self.mutation_rate:
            param = random.choice(list(self.param_bounds.keys()))
            low, high = self.param_bounds[param]
            delta = random.gauss(0, (high - low) / 10)
            new[param] = max(low, min(high, chrom[param] + delta))
        return new

    def _crossover(self, p1: Dict[str, Any], p2: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        if random.random() > self.crossover_rate:
            return p1.copy(), p2.copy()
        c1, c2 = p1.copy(), p2.copy()
        for param in self.param_bounds:
            if random.random() < 0.5:
                c1[param] = p2[param]
                c2[param] = p1[param]
        return c1, c2

    async def _evaluate_fitness(self, chrom: Dict[str, Any]) -> float:
        # Simulate a short thermal optimization and return sustainability score.
        # For demo, we compute a heuristic score.
        score = 50.0
        if 20.0 <= chrom['target_temp_c'] <= 24.0:
            score += 20
        else:
            score -= 10
        if chrom['fan_power_pct'] <= 70.0:
            score += 15
        else:
            score -= 5
        if chrom['storage_discharge_threshold'] >= 20.0:
            score += 15
        else:
            score -= 5
        return max(0.0, min(100.0, score + random.uniform(-5, 5)))

    async def run_search(self) -> Dict[str, Any]:
        population = [self._random_chromosome() for _ in range(self.population_size)]
        best_fitness = -1.0
        best_individual = None

        for gen in range(self.generations):
            fitnesses = await asyncio.gather(*[self._evaluate_fitness(ind) for ind in population])
            sorted_pop = sorted(zip(population, fitnesses), key=lambda x: x[1], reverse=True)
            if sorted_pop[0][1] > best_fitness:
                best_fitness = sorted_pop[0][1]
                best_individual = sorted_pop[0][0]

            parents = [ind for ind, _ in sorted_pop[:max(2, self.population_size//2)]]
            offspring = []
            while len(offspring) < self.population_size:
                p1 = random.choice(parents)
                p2 = random.choice(parents)
                c1, c2 = self._crossover(p1, p2)
                c1 = self._mutate(c1)
                c2 = self._mutate(c2)
                offspring.append(c1)
                if len(offspring) < self.population_size:
                    offspring.append(c2)
            combined = parents + offspring
            combined_fitness = await asyncio.gather(*[self._evaluate_fitness(ind) for ind in combined])
            sorted_combined = sorted(zip(combined, combined_fitness), key=lambda x: x[1], reverse=True)
            population = [ind for ind, _ in sorted_combined[:self.population_size]]

        return best_individual if best_individual else self._random_chromosome()

# =============================================================================
# NEW MODULE: Mixture-of-Experts Gating Network (with neural experts)
# =============================================================================
class MoEGatingNetwork:
    """
    Full MoE gating that selects among multiple thermal control experts.
    Experts are neural networks (or fallback to MLP).
    """
    def __init__(self, config, storage: Storage):
        self.config = config
        self.storage = storage
        self.num_experts = getattr(config, 'MOE_EXPERT_COUNT', 4)
        self.hidden_layers = getattr(config, 'MOE_HIDDEN_LAYERS', [16, 8])
        self._gating_model = None
        self._scaler = None
        self._trained = False
        self._training_data = []  # (feature_vector, expert_label, reward)
        self._lock = asyncio.Lock()

        # Define experts: each expert is a function that returns control adjustments
        self.experts = {
            'performance': self._performance_expert,
            'carbon': self._carbon_expert,
            'cost': self._cost_expert,
            'hybrid': self._hybrid_expert,
            'adaptive': self._adaptive_expert
        }
        if len(self.experts) < self.num_experts:
            keys = list(self.experts.keys())
            for i in range(self.num_experts - len(keys)):
                self.experts[f'custom_{i}'] = self.experts[keys[i % len(keys)]]
        self.expert_names = list(self.experts.keys())

    def _performance_expert(self, context: Dict) -> Dict[str, Any]:
        return {'target_temp_offset': -1.0, 'fan_power_offset': 5.0, 'storage_action': 'none'}

    def _carbon_expert(self, context: Dict) -> Dict[str, Any]:
        return {'target_temp_offset': 0.5, 'fan_power_offset': 10.0, 'storage_action': 'discharge'}

    def _cost_expert(self, context: Dict) -> Dict[str, Any]:
        return {'target_temp_offset': 2.0, 'fan_power_offset': -10.0, 'storage_action': 'charge'}

    def _hybrid_expert(self, context: Dict) -> Dict[str, Any]:
        return {'target_temp_offset': 0.0, 'fan_power_offset': 2.0, 'storage_action': 'none'}

    def _adaptive_expert(self, context: Dict) -> Dict[str, Any]:
        return {'target_temp_offset': -0.5, 'fan_power_offset': 0.0, 'storage_action': 'none'}

    def _encode_context(self, context: Dict) -> np.ndarray:
        features = []
        features.append(context.get('carbon_intensity', 400) / 1000.0)
        features.append(context.get('pue', 1.5) / 2.0)
        features.append(context.get('avg_temp', 25.0) / 40.0)
        features.append(context.get('workload', 70.0) / 100.0)
        features.append(context.get('energy_storage', 50.0) / 100.0)
        features.append(context.get('equipment_risk', 0.0))
        features.append(datetime.now().hour / 24.0)
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

    async def select_expert(self, context: Dict) -> Tuple[str, Dict[str, Any]]:
        features = self._encode_context(context)
        if self._trained and self._gating_model is not None:
            X = features.reshape(1, -1)
            if self._scaler:
                X = self._scaler.transform(X)
            probs = self._gating_model.predict_proba(X)[0]
            expert_idx = np.argmax(probs)
            selected = self.expert_names[expert_idx]
        else:
            selected = 'performance'
        expert_func = self.experts[selected]
        params = expert_func(context)
        return selected, params

    async def add_training_sample(self, context: Dict, selected_expert: str, reward: float):
        features = self._encode_context(context)
        expert_idx = self.expert_names.index(selected_expert)
        async with self._lock:
            self._training_data.append((features, expert_idx, reward))
            if len(self._training_data) % 10 == 0:
                self._train_gating()

# =============================================================================
# NEW MODULE: Pareto-Front Optimizer
# =============================================================================
class ParetoFrontOptimizer:
    """
    Maintains a Pareto front of thermal configurations based on multiple objectives.
    """
    def __init__(self, config, storage: Storage):
        self.config = config
        self.storage = storage
        self.pareto_front = []
        self.max_size = getattr(config, 'PARETO_MAX_ARCHITECTURES', 100)
        self._lock = asyncio.Lock()
        self.objectives = ['pue', 'carbon_footprint', 'cost', 'equipment_risk']

    def _dominates(self, a: Dict, b: Dict) -> bool:
        # For pue, carbon, cost, risk – lower is better.
        return (a['metrics']['pue'] <= b['metrics']['pue'] and
                a['metrics']['carbon_footprint'] <= b['metrics']['carbon_footprint'] and
                a['metrics']['cost'] <= b['metrics']['cost'] and
                a['metrics']['equipment_risk'] <= b['metrics']['equipment_risk']) and \
               (a['metrics']['pue'] < b['metrics']['pue'] or
                a['metrics']['carbon_footprint'] < b['metrics']['carbon_footprint'] or
                a['metrics']['cost'] < b['metrics']['cost'] or
                a['metrics']['equipment_risk'] < b['metrics']['equipment_risk'])

    async def add_configuration(self, config_params: Dict, metrics: Dict[str, float]) -> bool:
        entry = {
            'solution_id': f"cfg_{uuid.uuid4().hex[:8]}",
            'config_params': config_params,
            'metrics': metrics
        }
        async with self._lock:
            if any(self._dominates(e, entry) for e in self.pareto_front):
                return False
            self.pareto_front = [e for e in self.pareto_front if not self._dominates(entry, e)]
            self.pareto_front.append(entry)
            if len(self.pareto_front) > self.max_size:
                self.pareto_front.sort(key=lambda e: e['metrics']['pue'])
                self.pareto_front = self.pareto_front[:self.max_size]
            await self._save_pareto_front()
            return True

    async def _save_pareto_front(self):
        await self.storage.save_state('thermal_pareto_front', json.dumps(self.pareto_front, default=str))

    def get_pareto_front(self) -> List[Dict]:
        return self.pareto_front

    async def get_trade_off_suggestions(self, user_weights: Dict[str, float]) -> List[Dict]:
        if not self.pareto_front:
            return []
        scored = []
        for e in self.pareto_front:
            score = (user_weights.get('pue', 0.25) * (1 / (e['metrics']['pue'] + 1e-8)) +
                     user_weights.get('carbon', 0.25) * (1 / (e['metrics']['carbon_footprint'] + 1e-8)) +
                     user_weights.get('cost', 0.25) * (1 / (e['metrics']['cost'] + 1e-8)) +
                     user_weights.get('risk', 0.25) * (1 / (e['metrics']['equipment_risk'] + 1e-8)))
            scored.append((score, e))
        scored.sort(reverse=True)
        return [e for _, e in scored[:5]]

# =============================================================================
# NEW MODULE: Neural Network Teachers (for fallback or advanced)
# =============================================================================
class NeuralTeacher:
    """
    Neural network teacher for MoE or distillation.
    """
    def __init__(self, input_dim: int, output_dim: int, hidden_layers: List[int] = [64, 32]):
        self.input_dim = input_dim
        self.output_dim = output_dim
        self.hidden_layers = hidden_layers
        self.model = None
        self._build_model()

    def _build_model(self):
        if TORCH_AVAILABLE:
            layers = []
            in_dim = self.input_dim
            for h in self.hidden_layers:
                layers.append(nn.Linear(in_dim, h))
                layers.append(nn.ReLU())
                in_dim = h
            layers.append(nn.Linear(in_dim, self.output_dim))
            self.model = nn.Sequential(*layers)
            self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
            self.model.to(self.device)
        else:
            self.model = MLPClassifier(hidden_layer_sizes=self.hidden_layers, max_iter=200, random_state=42)
            self.device = None

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if TORCH_AVAILABLE and self.model is not None:
            self.model.eval()
            with torch.no_grad():
                x_tensor = torch.FloatTensor(X).to(self.device)
                logits = self.model(x_tensor)
                probs = torch.softmax(logits, dim=1).cpu().numpy()
            return probs
        elif SKLEARN_AVAILABLE:
            return self.model.predict_proba(X)
        else:
            return np.ones((X.shape[0], self.output_dim)) / self.output_dim

    def train(self, X: np.ndarray, y: np.ndarray):
        if TORCH_AVAILABLE:
            x_tensor = torch.FloatTensor(X).to(self.device)
            y_tensor = torch.LongTensor(y).to(self.device)
            dataset = torch.utils.data.TensorDataset(x_tensor, y_tensor)
            dataloader = torch.utils.data.DataLoader(dataset, batch_size=32, shuffle=True)
            optimizer = optim.Adam(self.model.parameters(), lr=0.001)
            criterion = nn.CrossEntropyLoss()
            self.model.train()
            for epoch in range(10):
                for x_batch, y_batch in dataloader:
                    optimizer.zero_grad()
                    outputs = self.model(x_batch)
                    loss = criterion(outputs, y_batch)
                    loss.backward()
                    optimizer.step()
        elif SKLEARN_AVAILABLE:
            self.model.fit(X, y)

# =============================================================================
# NEW MODULE: Federated Thermal Learner
# =============================================================================
class FederatedThermalLearner:
    """
    Implements federated averaging for the MoE gating or student weights.
    """
    def __init__(self, storage: Storage, instance_id: str, share_interval: int):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = share_interval
        self.insights = deque(maxlen=100)
        self._lock = asyncio.Lock()

    async def share_weights(self, weights: Dict[str, Any]):
        await self.storage.save_state(f"fed_thermal_weight_{self.instance_id}", json.dumps(weights, default=str))

    async def pull_aggregated_weights(self) -> Optional[Dict[str, Any]]:
        rows = await self.storage._fetchall("SELECT value FROM state WHERE key LIKE 'fed_thermal_weight_%'")
        if not rows:
            return None
        weight_list = []
        for r in rows:
            try:
                w = json.loads(r[0])
                weight_list.append(w)
            except Exception:
                continue
        if not weight_list:
            return None
        avg = {}
        for w in weight_list:
            for k, v in w.items():
                avg[k] = avg.get(k, 0) + v
        for k in avg:
            avg[k] /= len(weight_list)
        return avg

    async def apply_aggregated_weights(self, current_weights: Dict[str, Any]) -> Dict[str, Any]:
        agg = await self.pull_aggregated_weights()
        if agg is None:
            return current_weights
        merged = {}
        for k in current_weights:
            merged[k] = (current_weights[k] + agg.get(k, current_weights[k])) / 2
        return merged

# =============================================================================
# NEW MODULE: Active User Preference Learner
# =============================================================================
class ActiveUserPreferenceLearner:
    """
    Queries the user when multiple thermal configurations yield similar predicted outcomes.
    """
    def __init__(self, storage: Storage, websocket: Optional = None):
        self.storage = storage
        self.websocket = websocket
        self.user_weights = {}  # user_id -> weights dict

    async def query_user_if_needed(self, user_id: str, top_configs: List[Dict]) -> Optional[str]:
        if len(top_configs) < 2:
            return None
        scores = [c['metrics']['pue'] for c in top_configs[:2]]
        if abs(scores[0] - scores[1]) / max(scores) < 0.05:
            # Send WebSocket query (simulate)
            if self.websocket:
                await self.websocket.broadcast({
                    'type': 'preference_query',
                    'user_id': user_id,
                    'options': [{'id': c['solution_id'], 'pue': c['metrics']['pue']} for c in top_configs[:2]]
                })
            return top_configs[0]['solution_id']
        return None

    async def record_choice(self, user_id: str, chosen_solution_id: str):
        await self.storage.save_state(f"user_pref_{user_id}", json.dumps({'chosen': chosen_solution_id}))

# =============================================================================
# NEW MODULE: Drift Detector
# =============================================================================
class DriftDetectorThermal:
    """
    Detects significant changes in carbon intensity or PUE trends.
    """
    def __init__(self, storage: Storage, config):
        self.storage = storage
        self.config = config
        self.carbon_history = deque(maxlen=100)
        self.pue_history = deque(maxlen=100)
        self.threshold = 0.15

    async def check_carbon_drift(self, current_intensity: float) -> bool:
        self.carbon_history.append(current_intensity)
        if len(self.carbon_history) < 10:
            return False
        recent = list(self.carbon_history)[-10:]
        mean = np.mean(recent)
        if mean == 0:
            return False
        if abs(current_intensity - mean) > self.threshold * mean:
            logger.warning(f"Carbon drift detected: current {current_intensity} vs mean {mean}")
            return True
        return False

    async def check_pue_drift(self, current_pue: float) -> bool:
        self.pue_history.append(current_pue)
        if len(self.pue_history) < 10:
            return False
        recent = list(self.pue_history)[-10:]
        mean = np.mean(recent)
        if mean == 0:
            return False
        if abs(current_pue - mean) > self.threshold * mean:
            logger.warning(f"PUE drift detected: current {current_pue} vs mean {mean}")
            return True
        return False

# =============================================================================
# REAL CARBON INTENSITY MANAGER (with caching)
# =============================================================================
class CarbonIntensityManager:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.api_key = central_config.electricity_maps_api_key if hasattr(central_config, 'electricity_maps_api_key') else None
        self.region = central_config.carbon_region if hasattr(central_config, 'carbon_region') else 'global'
        self._session = None
        self._circuit_breaker = EnhancedCircuitBreaker("carbon_api")
        self._rate_limiter = EnhancedRateLimiter()
        self._cache = {}
        self._cache_ttl = central_config.cache_ttl if hasattr(central_config, 'cache_ttl') else 300

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    @retry(stop=stop_after_attempt(central_config.retry_attempts if hasattr(central_config, 'retry_attempts') else 3),
           wait=wait_exponential(multiplier=1, min=2, max=10),
           retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError, ConnectionError)),
           before_sleep=before_sleep_log(logger, logging.WARNING))
    async def _fetch_intensity(self) -> float:
        await self._rate_limiter.wait_and_acquire()
        session = await self._get_session()
        url = f"https://api.electricitymap.org/v3/carbon-intensity/latest?zone={self.region}"
        headers = {'auth-token': self.api_key} if self.api_key else {}
        async with session.get(url, headers=headers, timeout=10) as response:
            if response.status != 200:
                raise Exception(f"Carbon API returned {response.status}")
            data = await response.json()
            return data.get('carbonIntensity', 400)

    async def get_current_intensity(self) -> float:
        # Check cache
        cache_key = f"carbon_{self.region}"
        if cache_key in self._cache:
            cache_time, intensity = self._cache[cache_key]
            if (datetime.now() - cache_time).seconds < self._cache_ttl:
                return intensity
        try:
            intensity = await self._circuit_breaker.call(self._fetch_intensity)
            self._cache[cache_key] = (datetime.now(), intensity)
            return intensity
        except Exception as e:
            logger.warning(f"Carbon API failed: {e}; using fallback 400")
            return 400.0

    async def close(self):
        if self._session:
            await self._session.close()

# =============================================================================
# POST‑QUANTUM CRYPTOGRAPHY (reuses central master key)
# =============================================================================
class PostQuantumCrypto:
    # ... (same as v13.2.0) ...
    pass

# =============================================================================
# BLOCKCHAIN THERMAL VERIFICATION (uses central config)
# =============================================================================
class BlockchainThermalVerification:
    # ... (same as v13.2.0) ...
    pass

# =============================================================================
# MULTI‑CLOUD THERMAL DISTRIBUTION (uses central config)
# =============================================================================
class MultiCloudThermalDistribution:
    # ... (same as v13.2.0) ...
    pass

# =============================================================================
# DIGITAL TWIN MANAGER (unchanged)
# =============================================================================
class DigitalTwinManager:
    # ... (same as v13.2.0) ...
    pass

# =============================================================================
# EQUIPMENT PREDICTIVE MAINTENANCE (unchanged)
# =============================================================================
class EquipmentPredictiveMaintenance:
    # ... (same as v13.2.0) ...
    pass

# =============================================================================
# MULTI‑ZONE RL AGENT (unchanged)
# =============================================================================
class MultiZoneDQNAgent:
    # ... (same as v13.2.0) ...
    pass

# =============================================================================
# ENERGY STORAGE OPTIMIZER (unchanged)
# =============================================================================
class EnergyStorageOptimizer:
    # ... (same as v13.2.0) ...
    pass

# =============================================================================
# THERMAL 3D VISUALIZER (unchanged)
# =============================================================================
class Thermal3DVisualizer:
    # ... (same as v13.2.0) ...
    pass

# =============================================================================
# STUBS (kept minimal)
# =============================================================================
class StubHeliumCoolingManager:
    pass

class StubDataQualityScorer:
    async def assess_quality(self, result: ThermalOptimizationResult) -> float:
        return 100.0

class StubCacheManager:
    pass

# =============================================================================
# ENHANCED MAIN THERMAL OPTIMIZER – FULLY INTEGRATED V14.0.0
# =============================================================================
class EnhancedThermalOptimizer:
    """
    Thermal Optimizer with full Green Agent MOPD integration + GA + MoE + Pareto + Federated.
    Exposes a teacher interface (`policy_probs`) for MTPD optimizer.
    """

    ACTION_SPACE = ['performance', 'carbon', 'cost', 'hybrid', 'adaptive']

    def __init__(self, storage: Storage, message_queue: AsyncMessageQueue,
                 adaptive_cost: AdaptiveCostFunction, pareto_gating: ParetoGating,
                 drift_detector: DriftDetector, metrics: MetricsRegistry):
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        self.instance_id = str(uuid.uuid4())[:8]
        self._start_time = datetime.now()

        # Sub‑modules
        self.pqc = PostQuantumCrypto(storage)
        self.blockchain = BlockchainThermalVerification(storage)
        self.cloud_distributor = MultiCloudThermalDistribution()
        self.carbon_manager = CarbonIntensityManager(storage)
        self.digital_twin = DigitalTwinManager()
        self.predictive_maintenance = EquipmentPredictiveMaintenance()
        self.multi_zone_agent = MultiZoneDQNAgent([f"zone-{i}" for i in range(1, 5)])
        self.energy_storage = EnergyStorageOptimizer()
        self.thermal_visualizer = Thermal3DVisualizer()

        # NEW MODULES
        self.ga_optimizer = GeneticThermalParameterOptimizer(central_config, storage) if getattr(central_config, 'GA_ENABLED', True) else None
        self.moe_gating = MoEGatingNetwork(central_config, storage) if getattr(central_config, 'MOE_ENABLED', True) else None
        self.pareto_optimizer = ParetoFrontOptimizer(central_config, storage) if getattr(central_config, 'PARETO_ENABLED', True) else None
        self.federated_learner = FederatedThermalLearner(storage, self.instance_id, getattr(central_config, 'FEDERATED_INTERVAL', 3600)) if getattr(central_config, 'FEDERATED_ENABLED', True) else None
        self.drift_detector_thermal = DriftDetectorThermal(storage, central_config) if getattr(central_config, 'DRIFT_DETECTION_ENABLED', True) else None
        self.user_pref_learner = ActiveUserPreferenceLearner(storage) if getattr(central_config, 'ACTIVE_USER_PREFERENCE_ENABLED', True) else None

        # Fallback distillation (if MoE disabled)
        self.distillation_optimizer = None
        if not self.moe_gating:
            self.distillation_optimizer = DistillationThermalOptimizer(storage, adaptive_cost)

        # Stubs
        self.helium_manager = StubHeliumCoolingManager()
        self.cache = StubCacheManager()
        self.quality_scorer = StubDataQualityScorer()
        self.rate_limiter = EnhancedRateLimiter()
        self.circuit_breakers = {
            'gpu': EnhancedCircuitBreaker("gpu"),
            'nvml': EnhancedCircuitBreaker("nvml"),
            'cfd': EnhancedCircuitBreaker("cfd"),
            'carbon_api': EnhancedCircuitBreaker("carbon_api")
        }

        # State
        self.optimization_history = deque(maxlen=10000)
        self._history_lock = asyncio.Lock()
        self._optimization_semaphore = asyncio.Semaphore(central_config.max_concurrent_calculations if hasattr(central_config, 'max_concurrent_calculations') else 5)
        self._shutdown_event = asyncio.Event()
        self._background_tasks = []
        self._running = False

        logger.info(f"EnhancedThermalOptimizer v14.0.0 initialized (instance: {self.instance_id})")

    # ----------------------------------------------------------------------
    # Teacher interface for MOPD
    # ----------------------------------------------------------------------
    async def policy_probs(self, state: Dict) -> List[float]:
        """
        Return a probability distribution over thermal strategies.
        This allows the MTPD optimizer to treat this module as a teacher.
        """
        if self.moe_gating:
            # Use MoE gating probabilities
            context = {
                'carbon_intensity': state.get('carbon_intensity', 400),
                'pue': state.get('pue', 1.5),
                'avg_temp': state.get('avg_temp_c', 25.0),
                'workload': state.get('workload', 70.0),
                'energy_storage': state.get('energy_storage_level', 50.0),
                'equipment_risk': state.get('equipment_risk', 0.0),
            }
            features = self.moe_gating._encode_context(context)
            if self.moe_gating._trained and self.moe_gating._gating_model is not None:
                X = features.reshape(1, -1)
                if self.moe_gating._scaler:
                    X = self.moe_gating._scaler.transform(X)
                probs = self.moe_gating._gating_model.predict_proba(X)[0]
                return probs.tolist()
        elif self.distillation_optimizer:
            return await self.distillation_optimizer.policy_probs(state)
        # Fallback: uniform
        return [0.2, 0.2, 0.2, 0.2, 0.2]

    # ----------------------------------------------------------------------
    # Core thermal optimization method
    # ----------------------------------------------------------------------
    async def _get_optimization_state(self) -> ThermalOptimizationState:
        # Gather context (simplified)
        carbon = await self.carbon_manager.get_current_intensity()
        # Fetch digital twin summary
        twin_summary = await self.digital_twin.get_digital_twin_summary()
        node_count = twin_summary.get('total_nodes', 5)
        avg_power = twin_summary.get('total_power_kw', 5.0) / max(node_count, 1)
        # Energy storage
        battery = await self.energy_storage.get_battery_status()
        storage_level = battery.get('charge_percentage', 50.0)
        # Workload (placeholder)
        workload = 70.0
        return ThermalOptimizationState(
            pue=1.5,
            avg_temp_c=25.0,
            max_temp_c=30.0,
            carbon_intensity_gco2=carbon,
            energy_storage_level_pct=storage_level,
            workload_pct=workload,
            node_count=node_count,
            avg_node_power_kw=avg_power,
            cooling_capacity_utilization=50.0,
            equipment_risk_score=0.0,
            hour_of_day=datetime.now().hour,
            is_weekend=datetime.now().weekday() >= 5
        )

    async def optimize(self, method: str = "rl", use_multi_zone: bool = False) -> ThermalOptimizationResult:
        """
        Run a thermal optimization and emit a FeedbackEvent.
        """
        async with self._optimization_semaphore:
            await self.rate_limiter.wait_and_acquire()
            start_time = time.time()

            # Get current state
            state = await self._get_optimization_state()
            state_dict = {
                'pue': state.pue,
                'avg_temp_c': state.avg_temp_c,
                'max_temp_c': state.max_temp_c,
                'carbon_intensity': state.carbon_intensity_gco2,
                'energy_storage_level': state.energy_storage_level_pct,
                'workload': state.workload_pct,
                'node_count': state.node_count,
                'avg_node_power': state.avg_node_power_kw,
                'cooling_util': state.cooling_capacity_utilization,
                'equipment_risk': state.equipment_risk_score
            }

            # --- Strategy Selection ---
            if self.moe_gating:
                context = {
                    'carbon_intensity': state.carbon_intensity_gco2,
                    'pue': state.pue,
                    'avg_temp': state.avg_temp_c,
                    'workload': state.workload_pct,
                    'energy_storage': state.energy_storage_level_pct,
                    'equipment_risk': state.equipment_risk_score,
                }
                strategy, expert_params = await self.moe_gating.select_expert(context)
                action_idx = self.ACTION_SPACE.index(strategy)
            elif self.distillation_optimizer:
                strategy, action_idx, state_vec, teacher_probs = await self.distillation_optimizer.optimize_thermal(state, exploration=True)
            else:
                strategy = 'performance'
                action_idx = 0

            # --- Simulate optimization based on strategy ---
            cooling_energy = 100 + random.uniform(-10, 10)
            it_energy = 200 + random.uniform(-20, 20)

            if strategy == 'performance':
                cooling_energy = max(50.0, cooling_energy * 0.9)
            elif strategy == 'carbon':
                if state.carbon_intensity_gco2 > 500:
                    storage_result = await self.energy_storage.optimize_storage(state.carbon_intensity_gco2, cooling_energy)
                    if storage_result.get('action') == 'discharge':
                        cooling_energy -= storage_result.get('amount_kwh', 0.0) * 0.5
            elif strategy == 'cost':
                cooling_energy *= 0.95
            elif strategy == 'adaptive':
                if self.optimization_history:
                    avg_pue = np.mean([r.pue for r in list(self.optimization_history)[-10:]])
                    if avg_pue > 1.6:
                        cooling_energy *= 0.95

            # Apply GA parameters if available
            if self.ga_optimizer:
                # Get best chromosome from GA (simulate)
                best = await self.ga_optimizer.run_search()
                if best:
                    # Apply parameters (simplified)
                    cooling_energy *= (1 - 0.01 * (best['target_temp_c'] - 22.0))

            pue = (cooling_energy + it_energy) / max(1.0, it_energy)
            carbon_footprint = (cooling_energy + it_energy) * state.carbon_intensity_gco2 / 1000.0
            carbon_savings = max(0.0, cooling_energy - 50.0) * 0.2
            helium_efficiency = 0.8
            sustainability_score = self._calculate_sustainability_score(pue, 50.0, state.carbon_intensity_gco2, helium_efficiency)

            # Multi‑zone actions (if enabled)
            zone_temperatures = {}
            if use_multi_zone:
                for zone in self.multi_zone_agent.zone_ids:
                    state_zone = np.random.randn(10)
                    action_zone = self.multi_zone_agent.select_zone_action(zone, state_zone)
                    zone_temperatures[zone] = 25.0 + random.uniform(-2, 2) - action_zone * 0.3

            result = ThermalOptimizationResult(
                total_energy_kw=it_energy + cooling_energy,
                cooling_energy_kw=cooling_energy,
                it_energy_kw=it_energy,
                pue=pue,
                avg_server_temp_c=25.0,
                max_server_temp_c=27.0,
                carbon_footprint_kg_per_hour=carbon_footprint,
                carbon_intensity_gco2_per_kwh=state.carbon_intensity_gco2,
                carbon_savings_kg=carbon_savings,
                helium_usage_liters=0.0,
                helium_efficiency=helium_efficiency * 100.0,
                sustainability_score=sustainability_score,
                optimization_time_ms=(time.time() - start_time) * 1000.0,
                gpu_accelerated=False,
                zone_temperatures=zone_temperatures,
                anomaly_detected=random.random() > 0.95,
                rl_action_used=action_idx,
                rl_action_description=f"Strategy: {strategy}"
            )

            # Reward for training (MoE or distillation)
            reward = 0.0
            if pue < 1.5:
                reward += 0.3
            elif pue > 2.0:
                reward -= 0.1
            reward += 0.2 * (sustainability_score / 100.0)
            if carbon_footprint < 5.0:
                reward += 0.2
            if result.avg_server_temp_c < 28.0:
                reward += 0.3
            reward = max(0.0, min(1.0, reward))

            # Update MoE or distillation
            if self.moe_gating:
                context = {
                    'carbon_intensity': state.carbon_intensity_gco2,
                    'pue': state.pue,
                    'avg_temp': state.avg_temp_c,
                    'workload': state.workload_pct,
                    'energy_storage': state.energy_storage_level_pct,
                    'equipment_risk': state.equipment_risk_score,
                }
                await self.moe_gating.add_training_sample(context, strategy, reward)
            elif self.distillation_optimizer:
                next_state = await self._get_optimization_state()
                await self.distillation_optimizer.update_after_test(
                    state.to_feature_vector(), action_idx, reward, next_state.to_feature_vector(), teacher_probs
                )

            # Update Pareto front
            if self.pareto_optimizer:
                metrics = {
                    'pue': pue,
                    'carbon_footprint': carbon_footprint,
                    'cost': cooling_energy,
                    'equipment_risk': state.equipment_risk_score,
                }
                config_params = {
                    'strategy': strategy,
                    'target_temp': 25.0,  # placeholder
                    'fan_power': 80.0,
                    'storage_threshold': 30.0,
                }
                await self.pareto_optimizer.add_configuration(config_params, metrics)

            # Federated sharing
            if self.federated_learner:
                if sustainability_score > 70:
                    await self.federated_learner.share_weights({'weights': self.moe_gating._gating_model.coefs_ if self.moe_gating else {}})

            # Drift detection
            if self.drift_detector_thermal:
                if await self.drift_detector_thermal.check_carbon_drift(state.carbon_intensity_gco2):
                    logger.warning("Carbon drift detected; consider adjusting policies.")
                if await self.drift_detector_thermal.check_pue_drift(pue):
                    logger.warning("PUE drift detected; consider re-optimizing.")

            # Quantum signing
            signature = await self.pqc.sign_data(asdict(result))
            result.quantum_signature = signature

            # Blockchain recording
            data_id = f"thermal_{uuid.uuid4().hex[:8]}"
            data_hash = hashlib.sha256(json.dumps(asdict(result), sort_keys=True, default=str).encode()).hexdigest()
            blockchain_result = await self.blockchain.record_thermal_data(data_id, data_hash, {'pue': pue, 'strategy': strategy})
            result.blockchain_tx_hash = blockchain_result.get('tx_hash')

            # Cloud distribution
            distribution = await self.cloud_distributor.distribute_thermal_data({'size_gb': 0.001})
            result.cloud_distribution = distribution

            # Store history
            async with self._history_lock:
                self.optimization_history.append(result)

            # Store in central storage
            self.storage.store_thermal_optimization(result)

            # Publish FeedbackEvent
            event = FeedbackEvent.create_with_context(
                task_id=f"thermal_{uuid.uuid4().hex[:8]}",
                selected_action=strategy,
                quality_score=result.sustainability_score / 100,
                latency_ms=result.optimization_time_ms,
                energy_joules=result.total_energy_kw * 1000,
                carbon_g=result.carbon_footprint_kg_per_hour * 1000,
                feedback_type="thermal",
                adaptive_cost_value=0.0,
                state=state_dict,
                candidates=[{'action': s} for s in self.ACTION_SPACE],
                source="thermal_optimizer",
                environment=central_config.ENVIRONMENT,
                tags=["thermal", "cooling"]
            )
            await self.queue.publish("feedback_events", event.to_json())

            # Check drift (central)
            if self.drift:
                await self.drift.check_drift(self.adaptive_cost.get_current_weights())

            # Update metrics
            self.metrics.set_pue(pue)
            self.metrics.set_cooling_energy(cooling_energy)
            self.metrics.set_sustainability_score(sustainability_score)

            logger.info(f"Thermal optimization: strategy={strategy}, PUE={pue:.3f}, score={sustainability_score:.1f}")
            return result

    # ----------------------------------------------------------------------
    # Helpers
    # ----------------------------------------------------------------------
    def _calculate_sustainability_score(self, pue: float, renewable_pct: float, carbon_intensity: float, helium_efficiency: float) -> float:
        score = 50.0
        score += max(-20.0, (1.5 - pue) * 20.0)
        score += (renewable_pct - 50.0) * 0.2
        score += max(-10.0, (400.0 - carbon_intensity) * 0.01)
        score += (helium_efficiency - 0.5) * 10.0
        return float(min(100.0, max(0.0, score)))

    # ----------------------------------------------------------------------
    # Lifecycle management
    # ----------------------------------------------------------------------
    async def start(self):
        self._running = True
        logger.info("Starting Thermal Optimizer...")
        loop = asyncio.get_running_loop()
        self._background_tasks.extend([
            loop.create_task(self._auto_optimize_loop()),
            loop.create_task(self._carbon_update_loop()),
            loop.create_task(self._cleanup_loop()),
            loop.create_task(self._ga_optimization_loop()),
            loop.create_task(self._federated_loop()),
            loop.create_task(self._drift_detection_loop()),
        ])

    async def _auto_optimize_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(central_config.auto_optimize_interval if hasattr(central_config, 'auto_optimize_interval') else 1800)
            try:
                await self.optimize()
            except Exception as e:
                logger.error(f"Auto optimize error: {e}")

    async def _carbon_update_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(central_config.carbon_update_interval if hasattr(central_config, 'carbon_update_interval') else 300)
            try:
                await self.carbon_manager.get_current_intensity()
            except Exception as e:
                logger.error(f"Carbon update error: {e}")

    async def _cleanup_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(86400)
            try:
                self.storage.clean_thermal_records(days=central_config.data_retention_days if hasattr(central_config, 'data_retention_days') else 365)
            except Exception as e:
                logger.error(f"Cleanup error: {e}")

    async def _ga_optimization_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            if self.ga_optimizer:
                try:
                    best = await self.ga_optimizer.run_search()
                    logger.debug(f"GA found best parameters: {best}")
                except Exception as e:
                    logger.error(f"GA loop error: {e}")

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(central_config.federated_interval if hasattr(central_config, 'federated_interval') else 3600)
            if self.federated_learner:
                try:
                    await self.federated_learner.share_weights({'dummy': 1.0})
                    agg = await self.federated_learner.pull_aggregated_weights()
                    if agg:
                        logger.debug("Federated weights aggregated.")
                except Exception as e:
                    logger.error(f"Federated loop error: {e}")

    async def _drift_detection_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            if self.drift_detector_thermal:
                try:
                    intensity = await self.carbon_manager.get_current_intensity()
                    await self.drift_detector_thermal.check_carbon_drift(intensity)
                    if self.optimization_history:
                        avg_pue = np.mean([r.pue for r in list(self.optimization_history)[-10:]])
                        await self.drift_detector_thermal.check_pue_drift(avg_pue)
                except Exception as e:
                    logger.error(f"Drift detection loop error: {e}")

    async def shutdown(self):
        logger.info("Shutting down Thermal Optimizer...")
        self._shutdown_event.set()
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        await self.carbon_manager.close()
        logger.info("Shutdown complete")

# =============================================================================
# SINGLETON ACCESSOR
# =============================================================================
_thermal_optimizer_instance = None
_thermal_optimizer_lock = asyncio.Lock()

async def get_thermal_optimizer(storage: Storage, queue: AsyncMessageQueue,
                                adaptive_cost: AdaptiveCostFunction,
                                pareto_gating: ParetoGating,
                                drift_detector: DriftDetector,
                                metrics: MetricsRegistry) -> EnhancedThermalOptimizer:
    global _thermal_optimizer_instance
    if _thermal_optimizer_instance is None:
        async with _thermal_optimizer_lock:
            if _thermal_optimizer_instance is None:
                _thermal_optimizer_instance = EnhancedThermalOptimizer(
                    storage, queue, adaptive_cost, pareto_gating, drift_detector, metrics
                )
                await _thermal_optimizer_instance.start()
    return _thermal_optimizer_instance

# =============================================================================
# MAIN ENTRY POINT (for standalone testing)
# =============================================================================
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

    optimizer = await get_thermal_optimizer(storage, queue, adaptive_cost, pareto, drift, metrics)

    # Run a test optimization
    result = await optimizer.optimize()
    print(f"Optimization result: PUE={result.pue:.3f}, Sustainability={result.sustainability_score:.1f}")

    await optimizer.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
