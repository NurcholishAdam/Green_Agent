#!/usr/bin/env python3
# File: src/enhancements/federated_learning_enhanced.py
# Version 9.3 – Enhanced with XAI, temporal safety, human approval, chaos testing, and robustness fixes.

"""
Enhanced Federated Learning Orchestrator - Version 9.3
Enterprise Quantum Resilience + MTOP + MOPD Integration

ENHANCEMENTS OVER v9.2:
- Added missing central config fallbacks.
- Improved numerical stability of policy_probs (softmax with normalization).
- Safe async task creation (deferred to start()).
- Added XAI explanation for strategy selection.
- Added temporal safety invariant checks via LimitGraph.
- Added human-in-the-loop approval hook for critical decisions.
- Added chaos testing methods (inject_fault, run_chaos_test).
- Fixed fallback stubs to be more robust.
- Use DriftDetector result to adjust exploration rate.
- RLHF stub now interactive (method to submit human preferences).
- Update Fitness and Strategy evolution to handle new strategies.
"""

import asyncio
import hashlib
import json
import os
import signal
import sys
import time
import uuid
import random
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Union
from collections import deque
from enum import Enum
from concurrent.futures import ThreadPoolExecutor
import numpy as np

# ============================================================
# ENHANCED MODULES IMPORTS (with graceful fallback)
# ============================================================
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator
    from enhancements.moe_system import ExpertRouter
    from enhancements.MODP import ParetoOptimizer
    from enhancements.contextual_bandit import ContextualBandit
    from enhancements.limit_graph import LimitGraph
    from enhancements.rlhf import RLHFOptimizer
    from enhancements.multi_teacher_policy_distillation import MultiTeacherDistiller
    ENHANCEMENTS_AVAILABLE = True
    ADDITIONAL_ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    ADDITIONAL_ENHANCEMENTS_AVAILABLE = False
    # Fallback stubs with improved behavior
    class GeneticPolicyGenerator:
        def __init__(self, *args, **kwargs): pass
        def evolve(self, population, fitness_fn, generations=10, population_size=20):
            if not population:
                return []
            # Simple random mutation
            new_pop = []
            for _ in range(population_size):
                candidate = random.choice(population).copy()
                candidate['params'] = {k: v + random.uniform(-0.1, 0.1) for k, v in candidate.get('params', {}).items()}
                new_pop.append(candidate)
            return new_pop
    class ExpertRouter:
        def __init__(self, *args, **kwargs): pass
        def encode(self, context): return [0.0]*5
        def select(self, encoded): return "fedavg"
    class ParetoOptimizer:
        def __init__(self, *args, **kwargs): pass
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0) * weights.get(k, 1) for k in objectives)
    class ContextualBandit:
        def __init__(self, action_space, fallback_solver, *args, **kwargs):
            self.actions = action_space
            self.fallback = fallback_solver
        def select_action(self, context):
            action = self.fallback(context)
            return action, 1.0, "fallback"
        def update(self, context, action, reward): pass
        def seed_safe_policy(self, context, policy): pass
    class LimitGraph:
        def __init__(self, *args, **kwargs): self.limits = {}
        def build_graph(self, nodes, edges): pass
        def get_limits(self, context): return {}
        def update_from_feedback(self, feedback): pass
    class RLHFOptimizer:
        def __init__(self, action_space, *args, **kwargs): self.actions = action_space; self.preferences = []
        def update(self, context, action, reward): pass
        def sample_action(self, context):
            if self.actions:
                return random.choice(self.actions)
            return None
        def add_human_feedback(self, context, chosen_action, rejected_action):
            self.preferences.append((context, chosen_action, rejected_action))
    class MultiTeacherDistiller:
        def __init__(self, teachers, *args, **kwargs): self.teachers = teachers
        def distill(self, context):
            if not self.teachers:
                return 'fedavg'
            # Simple majority vote
            votes = [teacher(context) for teacher in self.teachers]
            return max(set(votes), key=votes.count) if votes else 'fedavg'

# ============================================================
# IMPORT CENTRAL GREEN AGENT COMPONENTS
# ============================================================
from ..config import config as central_config
from ..storage import Storage
from ..schemas.feedback_event import FeedbackEvent
from ..routing.pareto_gating import ParetoGating
from ..feedback.adaptive_cost import AdaptiveCostFunction
from ..safety.drift_detector import DriftDetector
from ..scaling.message_queue import AsyncMessageQueue
from ..metrics import MetricsRegistry
from ..logger import logger

# ============================================================
# OPTIONAL IMPORTS (graceful degradation)
# ============================================================
try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend

try:
    from web3 import Web3, Account
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

try:
    import boto3
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

try:
    from fastapi import FastAPI, Depends, HTTPException, status, Request
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

try:
    from jose import JWTError, jwt
    from jose.constants import ALGORITHMS
    JOSE_AVAILABLE = True
except ImportError:
    JOSE_AVAILABLE = False

# ============================================================
# CUSTOM EXCEPTIONS (unchanged)
# ============================================================
class FederatedError(Exception): pass
class QuantumError(FederatedError): pass
class BlockchainError(FederatedError): pass
class OptimizationError(FederatedError): pass
class ClientError(FederatedError): pass
class CircuitBreakerOpenError(FederatedError): pass
class RateLimitExceeded(FederatedError): pass
class VaultError(FederatedError): pass
class CloudStorageError(FederatedError): pass

# ============================================================
# ENHANCED CIRCUIT BREAKER (with fallback config)
# ============================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class EnhancedCircuitBreaker:
    def __init__(self, name: str):
        self.name = name
        self.failure_threshold = getattr(central_config, 'CIRCUIT_BREAKER_FAILURE_THRESHOLD', 5)
        self.recovery_timeout = getattr(central_config, 'CIRCUIT_BREAKER_RECOVERY_TIMEOUT', 30.0)
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

# ============================================================
# ENHANCED RATE LIMITER
# ============================================================
class EnhancedRateLimiter:
    def __init__(self):
        self.rate = getattr(central_config, 'rate_limit_requests', 100)
        self.per_seconds = getattr(central_config, 'rate_limit_window', 60)
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

# ============================================================
# DATA CLASSES (unchanged, added explanation field)
# ============================================================
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
    last_update: datetime = field(default_factory=datetime.now)

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
    explanation: str = ""  # XAI
    timestamp: datetime = field(default_factory=datetime.now)

# ============================================================
# POST‑QUANTUM CRYPTOGRAPHY (unchanged)
# ============================================================
class PostQuantumCrypto:
    def __init__(self, storage):
        self.storage = storage
        self.keys = {}

    async def sign_data(self, data: Dict) -> Dict:
        if PQC_AVAILABLE:
            return {'algorithm': 'dilithium', 'signature': ''}
        return {'algorithm': 'none', 'signature': ''}

# ============================================================
# MULTI‑CLOUD STORAGE (unchanged)
# ============================================================
class MultiCloudStorage:
    async def store(self, data: Dict, filename: str) -> Dict:
        logger.info(f"Storing {filename}")
        return {'status': 'ok'}

# ============================================================
# ENHANCED FEDERATED LEARNER – WITH FIXES AND NEW FEATURES
# ============================================================
class EnhancedFederatedLearner:
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

        self.pqc = PostQuantumCrypto(storage)
        self.cloud_storage = MultiCloudStorage()

        self.clients: Dict[str, FederatedClient] = {}
        self.round_count: int = 0
        self.global_model: Any = None
        self.history: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._background_tasks = []
        self._started = False  # NEW: flag to track start state

        # Config fallbacks
        self.federated_interval = getattr(central_config, 'federated_interval', 1800)
        self.data_retention_days = getattr(central_config, 'data_retention_days', 365)
        self.modp_weights = getattr(central_config, 'modp_weights', {'accuracy':0.4, 'energy':0.3, 'carbon':0.2, 'latency':0.1})

        # Sub-modules
        if ENHANCEMENTS_AVAILABLE:
            self.modp = ParetoOptimizer()
            self.moe = ExpertRouter()
            self.bio = GeneticPolicyGenerator()
            self.strategies = ['fedavg', 'fedprox', 'coevolution', 'quantum', 'carbon_aware']
            self.bandit = ContextualBandit(
                action_space=self.strategies,
                fallback_solver=lambda ctx: 'fedavg',
                min_trials_before_bandit=5,
                confidence_threshold=0.6,
            )
            self.strategy_population = [{'name': s, 'params': {}} for s in self.strategies]
            self.strategy_fitness = deque(maxlen=100)
        else:
            self.modp = None
            self.moe = None
            self.bio = None
            self.bandit = None
            self.strategies = ['fedavg', 'fedprox', 'coevolution', 'quantum', 'carbon_aware']
            self.strategy_population = []
            self.strategy_fitness = deque(maxlen=100)

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE:
            self.limit_graph = LimitGraph()
        else:
            self.limit_graph = None

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE:
            self.rlhf = RLHFOptimizer(action_space=self.strategies)
        else:
            self.rlhf = None

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE:
            self.distiller = MultiTeacherDistiller([
                self._bandit_teacher,
                self._modp_teacher,
                self._static_teacher
            ])
        else:
            self.distiller = None

        # For fallback ε‑greedy
        self.strategy_usage = {s: 0 for s in self.strategies}
        self.strategy_rewards = {s: 0.0 for s in self.strategies}
        self.epsilon = 0.1
        self._drift_score = 0.0  # NEW: store latest drift score

        self._load_state()
        logger.info(f"EnhancedFederatedLearner v9.3 initialized (instance: {self.instance_id})")

    # Teacher functions (unchanged, but robust)
    def _bandit_teacher(self, context: Dict) -> str:
        if self.bandit:
            encoded = self.moe.encode(context) if self.moe else context
            strategy, _, _ = self.bandit.select_action(encoded)
            return strategy if strategy else 'fedavg'
        return 'fedavg'

    def _modp_teacher(self, context: Dict) -> str:
        if not self.modp:
            return 'fedavg'
        scores = {}
        for s in self.strategies:
            objectives = {
                'accuracy': 0.7,
                'carbon': 1.0 - (context.get('avg_carbon', 400) / 800),
                'energy': 0.5,
                'latency': 0.8,
            }
            utility = self.modp.evaluate(objectives, self.modp_weights)
            scores[s] = utility
        return max(scores, key=scores.get)

    def _static_teacher(self, context: Dict) -> str:
        return 'fedavg'

    def _load_state(self):
        try:
            state = self.storage.get_federated_optimizer_state()
            if state:
                self.epsilon = state.get('epsilon', 0.1)
                self.strategy_rewards = state.get('strategy_rewards', {s: 0.0 for s in self.strategies})
                self.strategy_usage = state.get('strategy_usage', {s: 0 for s in self.strategies})
                self.strategy_population = state.get('strategy_population', [])
                self.strategy_fitness = deque(state.get('strategy_fitness', []), maxlen=100)
        except Exception as e:
            logger.warning(f"Failed to load optimizer state: {e}")

    def _save_state(self):
        try:
            state = {
                'epsilon': self.epsilon,
                'strategy_rewards': self.strategy_rewards,
                'strategy_usage': self.strategy_usage,
                'strategy_population': self.strategy_population,
                'strategy_fitness': list(self.strategy_fitness),
            }
            self.storage.save_federated_optimizer_state(state)
        except Exception as e:
            logger.warning(f"Failed to save optimizer state: {e}")

    # ------------------ Teacher interface with improved stability ------------------
    async def policy_probs(self, state: Dict) -> List[float]:
        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and self.distiller:
            context = {'num_clients': len(self.clients), 'avg_carbon': 400}
            selected_strategy = self.distiller.distill(context)
            if selected_strategy is None:
                selected_strategy = 'fedavg'
            probs = [0.01] * len(self.strategies)
            idx = self.strategies.index(selected_strategy) if selected_strategy in self.strategies else 0
            probs[idx] = 1.0 - 0.01 * (len(self.strategies) - 1)
            return probs
        else:
            # Use softmax with temperature to avoid overflow
            rewards = np.array([self.strategy_rewards.get(s, 0.0) for s in self.strategies])
            # Normalize to prevent overflow: subtract max
            rewards = rewards - np.max(rewards)
            exp_rewards = np.exp(rewards)
            probs = exp_rewards / np.sum(exp_rewards)
            return probs.tolist()

    # ------------------ Client registration (unchanged) ------------------
    async def register_client(self, client_id: str, initial_data: Dict = None,
                              data_size: int = 1000, compute_power: float = 1000,
                              carbon_intensity: float = 400, renewable_percent: float = 0,
                              trust_score: float = 0.5, region: str = "global") -> bool:
        async with self._lock:
            if client_id in self.clients:
                return False
            client = FederatedClient(
                client_id=client_id,
                data_size=data_size,
                compute_power=compute_power,
                carbon_intensity=carbon_intensity,
                renewable_percent=renewable_percent,
                trust_score=trust_score,
                region=region
            )
            self.clients[client_id] = client
            logger.info(f"Registered client {client_id}")

        event = FeedbackEvent.create_with_context(
            task_id=f"fl_register_{client_id}",
            selected_action="register_client",
            quality_score=trust_score,
            latency_ms=0.0,
            energy_joules=0.0,
            carbon_g=carbon_intensity * 0.1,
            feedback_type="federated",
            adaptive_cost_value=0.0,
            state={'client_id': client_id, 'region': region},
            candidates=[{'action': 'register'}],
            source="federated_learner",
            environment=getattr(central_config, 'ENVIRONMENT', 'production'),
            tags=["federated", "client"]
        )
        await self.queue.publish("feedback_events", event.to_json())
        self.metrics.increment_federated_clients(len(self.clients))
        return True

    # ------------------ Federated round with XAI, safety, approval ------------------
    async def federated_round(self, strategy: str = None) -> Optional[FederatedRoundResult]:
        async with self._lock:
            if len(self.clients) < 1:
                logger.warning("No clients registered")
                return None

            context = {
                'num_clients': len(self.clients),
                'avg_trust': np.mean([c.trust_score for c in self.clients.values()]),
                'avg_carbon': np.mean([c.carbon_intensity for c in self.clients.values()]),
                'avg_renewable': np.mean([c.renewable_percent for c in self.clients.values()]),
                'regions': list(set([c.region for c in self.clients.values()])),
                'hour': datetime.now().hour,
            }

            # Strategy selection with hierarchy and explanation
            explanation = "Strategy selection: "
            if ADDITIONAL_ENHANCEMENTS_AVAILABLE and self.distiller:
                strategy = self.distiller.distill(context)
                source = "distilled"
                explanation += "multi-teacher distillation"
            elif ADDITIONAL_ENHANCEMENTS_AVAILABLE and self.rlhf:
                strategy = self.rlhf.sample_action(context)
                if strategy is None:
                    strategy = 'fedavg'
                source = "rlhf"
                explanation += "RLHF optimizer"
            elif ENHANCEMENTS_AVAILABLE and self.bandit:
                encoded = self.moe.encode(context) if self.moe else context
                strategy, confidence, source = self.bandit.select_action(encoded)
                if strategy is None:
                    strategy = 'fedavg'
                explanation += f"contextual bandit (confidence={confidence:.2f})"
            else:
                if strategy is None:
                    if random.random() < self.epsilon:
                        strategy = random.choice(self.strategies)
                    else:
                        strategy = max(self.strategies, key=lambda s: self.strategy_rewards.get(s, 0.0))
                source = "fallback"
                explanation += f"ε‑greedy (ε={self.epsilon:.3f})"

            # Apply LIMIT Graph constraints
            if ADDITIONAL_ENHANCEMENTS_AVAILABLE and self.limit_graph:
                limits = self.limit_graph.get_limits(context)
                if limits.get('force_carbon_aware') and context.get('avg_carbon', 400) > 600:
                    strategy = 'carbon_aware'
                    source = "limit_graph"
                    explanation += " (overridden by LIMIT Graph: high carbon)"

            # Temporal safety check before proceeding
            if not await self.check_invariants(context, strategy):
                logger.warning("Temporal safety check failed; aborting round.")
                return None

            # Human approval if required (config flag)
            if getattr(central_config, 'REQUIRE_HUMAN_APPROVAL', False):
                if not await self.request_approval(strategy, context):
                    logger.info("Human approval not granted for strategy; using default.")
                    strategy = 'fedavg'

            # Simulate round
            self.round_count += 1
            selected_clients = list(self.clients.values())[:min(5, len(self.clients))]
            num_clients = len(selected_clients)
            global_accuracy = 0.7 + 0.2 * random.random()
            aggregated_loss = 0.5 * random.random()
            energy_used = num_clients * 0.1
            carbon_footprint = energy_used * 0.2

            if self.modp:
                objectives = {
                    'accuracy': global_accuracy,
                    'energy': 1.0 - (energy_used / (num_clients * 0.1 + 1e-8)),
                    'carbon': 1.0 - (carbon_footprint / (num_clients * 0.2 + 1e-8)),
                    'latency': 0.9,
                }
                utility = self.modp.evaluate(objectives, self.modp_weights)
                reward = utility
            else:
                reward = global_accuracy

            # Update learners
            if ENHANCEMENTS_AVAILABLE and self.bandit:
                encoded = self.moe.encode(context) if self.moe else context
                await self.bandit.update(encoded, strategy, reward)
            else:
                self.strategy_usage[strategy] += 1
                count = self.strategy_usage[strategy]
                self.strategy_rewards[strategy] += (reward - self.strategy_rewards[strategy]) / count
                self.epsilon = max(0.01, self.epsilon * 0.99)

            if ADDITIONAL_ENHANCEMENTS_AVAILABLE and self.rlhf:
                self.rlhf.update(context, strategy, reward)

            if ADDITIONAL_ENHANCEMENTS_AVAILABLE and self.limit_graph:
                self.limit_graph.update_from_feedback({
                    'context': context,
                    'strategy': strategy,
                    'reward': reward,
                    'success': reward > 0.7
                })

            if ENHANCEMENTS_AVAILABLE and self.bio:
                self.strategy_fitness.append(reward)

            # Drift update: use drift score to adjust epsilon
            if self.drift:
                drift_score = await self.drift.check_drift(self.adaptive_cost.get_current_weights())
                if drift_score and drift_score > 0.7:
                    self._drift_score = drift_score
                    self.epsilon = min(0.2, self.epsilon * 1.5)
                    logger.warning(f"High drift {drift_score:.2f}; increasing exploration.")

            result = FederatedRoundResult(
                round_id=self.round_count,
                num_clients=num_clients,
                global_accuracy=global_accuracy,
                aggregated_loss=aggregated_loss,
                strategy=strategy,
                carbon_footprint=carbon_footprint,
                energy_used=energy_used,
                explanation=explanation,
            )

            signature = await self.pqc.sign_data(asdict(result))
            result.quantum_signature = signature
            await self.cloud_storage.store(asdict(result), f"fl_round_{self.round_count}.json")
            self.storage.store_federated_round(result)
            self.history.append(result)

            event = FeedbackEvent.create_with_context(
                task_id=f"fl_round_{self.round_count}",
                selected_action=f"round_{strategy}",
                quality_score=global_accuracy,
                latency_ms=0.0,
                energy_joules=energy_used * 3.6e6,
                carbon_g=carbon_footprint * 1000,
                feedback_type="federated",
                adaptive_cost_value=0.0,
                state={'num_clients': num_clients, 'strategy': strategy, 'source': source, 'explanation': explanation},
                candidates=[{'action': s} for s in self.strategies],
                source="federated_learner",
                environment=getattr(central_config, 'ENVIRONMENT', 'production'),
                tags=["federated", "aggregation"]
            )
            await self.queue.publish("feedback_events", event.to_json())

            self.metrics.increment_federated_rounds()
            self.metrics.set_federated_accuracy(global_accuracy)

            logger.info(f"Federated round {self.round_count}: strategy={strategy} (source={source}), accuracy={global_accuracy:.3f}, explanation={explanation}")
            return result

    # ------------------ Temporal safety check ------------------
    async def check_invariants(self, context: Dict, strategy: str) -> bool:
        """Check temporal safety invariants before running a round."""
        # Use LimitGraph if available, else basic checks
        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and self.limit_graph:
            limits = self.limit_graph.get_limits(context)
            # Example: if carbon intensity is critical and strategy is not carbon_aware, fail
            if limits.get('carbon_critical') and strategy != 'carbon_aware':
                logger.warning("Temporal safety: carbon critical but strategy not carbon_aware")
                return False
        # Basic invariant: no strategy if no clients
        if len(self.clients) == 0:
            return False
        return True

    # ------------------ Human approval ------------------
    async def request_approval(self, strategy: str, context: Dict) -> bool:
        """Request human approval for a critical strategy decision."""
        # In a real system, this would interact with a queue or UI.
        # For now, just log and return False to simulate denial.
        logger.warning(f"Human approval required for strategy {strategy}; auto-denying.")
        return False

    # ------------------ Chaos testing ------------------
    async def inject_fault(self, fault_type: str, **params):
        """Inject a fault for resilience testing."""
        if fault_type == 'storage_failure':
            self.storage = None
            logger.warning("Injected storage_failure")
        elif fault_type == 'high_carbon':
            for c in self.clients.values():
                c.carbon_intensity = 800.0
            logger.warning("Injected high_carbon")
        elif fault_type == 'circuit_open':
            # Simulate circuit breaker open by raising exception
            raise CircuitBreakerOpenError("Simulated circuit open")
        else:
            logger.warning(f"Unknown fault type: {fault_type}")

    async def run_chaos_test(self) -> Dict[str, Any]:
        """Run a simple chaos test to verify resilience."""
        report = {'faults': [], 'results': {}}
        # Test storage failure
        await self.inject_fault('storage_failure')
        report['faults'].append('storage_failure')
        try:
            await self.federated_round()
            report['results']['storage_failure'] = 'unexpected_success'
        except Exception as e:
            report['results']['storage_failure'] = f'failed_as_expected: {type(e).__name__}'
        # Reset storage
        self.storage = Storage()
        # Test high carbon
        await self.inject_fault('high_carbon')
        report['faults'].append('high_carbon')
        # Run round and see if carbon_aware is selected
        result = await self.federated_round()
        if result:
            report['results']['high_carbon'] = f"strategy={result.strategy}"
        else:
            report['results']['high_carbon'] = 'round_not_run'
        # Reset carbon
        for c in self.clients.values():
            c.carbon_intensity = 400.0
        return report

    # ------------------ Background tasks (safe start) ------------------
    async def start(self):
        if self._started:
            return
        self._started = True
        loop = asyncio.get_running_loop()
        self._background_tasks.extend([
            loop.create_task(self._optimization_loop()),
            loop.create_task(self._evolution_loop()),
            loop.create_task(self._cleanup_loop()),
        ])
        logger.info("Federated Learner background tasks started")

    async def _optimization_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.federated_interval)
            try:
                await self.federated_round()
            except Exception as e:
                logger.error(f"Optimization loop error: {e}")

    async def _evolution_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                if ENHANCEMENTS_AVAILABLE:
                    await self._evolve_strategies()
            except Exception as e:
                logger.error(f"Evolution loop error: {e}")

    async def _cleanup_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(86400)
            try:
                self.storage.clean_old_federated_rounds(days=self.data_retention_days)
            except Exception as e:
                logger.error(f"Cleanup error: {e}")

    async def _evolve_strategies(self):
        if not self.bio or not self.strategy_population:
            return
        if len(self.strategy_fitness) < 10:
            return

        def fitness(strategy_config):
            name = strategy_config.get('name', 'fedavg')
            return self.strategy_rewards.get(name, 0.0)

        new_population = self.bio.evolve(
            population=self.strategy_population,
            fitness_fn=fitness,
            generations=10,
            population_size=20,
        )
        if new_population and isinstance(new_population, list):
            self.strategy_population = new_population
            new_names = [p['name'] for p in new_population]
            for name in new_names:
                if name not in self.strategies:
                    self.strategies.append(name)
                    if self.bandit:
                        self.bandit.actions = self.strategies
                    if self.rlhf:
                        self.rlhf.actions = self.strategies
                    self.strategy_rewards[name] = 0.0
                    self.strategy_usage[name] = 0
            self._save_state()
            logger.info(f"Evolved strategy population: {len(new_population)} strategies")

    async def shutdown(self):
        logger.info("Shutting down Federated Learner...")
        self._shutdown_event.set()
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._save_state()
        logger.info("Shutdown complete")

# ============================================================
# SINGLETON ACCESSOR (unchanged, but safe start)
# ============================================================
_federated_learner_instance = None
_federated_learner_lock = asyncio.Lock()

async def get_federated_learner(storage: Storage, queue: AsyncMessageQueue,
                                adaptive_cost: AdaptiveCostFunction,
                                pareto_gating: ParetoGating,
                                drift_detector: DriftDetector,
                                metrics: MetricsRegistry) -> EnhancedFederatedLearner:
    global _federated_learner_instance
    if _federated_learner_instance is None:
        async with _federated_learner_lock:
            if _federated_learner_instance is None:
                _federated_learner_instance = EnhancedFederatedLearner(
                    storage, queue, adaptive_cost, pareto_gating, drift_detector, metrics
                )
                await _federated_learner_instance.start()
    return _federated_learner_instance

# ============================================================
# MAIN ENTRY POINT (for standalone testing)
# ============================================================
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

    learner = await get_federated_learner(storage, queue, adaptive_cost, pareto, drift, metrics)
    await learner.register_client("client_1", data_size=1000, compute_power=2000, trust_score=0.8)
    result = await learner.federated_round()
    print(f"Round {result.round_id}: accuracy={result.global_accuracy:.3f}, strategy={result.strategy}, explanation={result.explanation}")
    await learner.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
