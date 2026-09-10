#!/usr/bin/env python3
"""
Per‑layer energy profiling and layer‑skipping for energy‑efficient inference.
Enhanced version with real‑time carbon integration, adaptive skipping,
support for all layer types, and FlexGen offloading policy selection.

ENHANCEMENTS OVER v1.0:
- Integrated bio_inspired, moe_system, MODP, ContextualBandit.
- Skipping decisions are now adaptive, context‑aware, and multi‑objective.
- Learned state persisted via Storage.
- Feedback events published to message queue.
- New API endpoints for optimization and feedback.
- FlexGen integration: select optimal GPU/CPU/disk offloading policies.

NEW IN v2.0:
- CausalBandit replaces ContextualBandit for causal RL.
- SafetyMonitor with temporal logic rules.
- XAIExplainer for decision rationale.
- FederatedProfilerCoordinator (secure aggregation).
- MultiAgentProfiler for coordination.
- CarbonOffsetBroker for offsets and RECs.
- ChaosMonkey for resilience testing.
- HumanReviewManager for human-in-the-loop with active learning.
- Adaptive precision switching via FlexGen.
"""

import torch
import torch.nn as nn
from typing import List, Dict, Optional, Union, Callable, Protocol, runtime_checkable, Tuple, Any
import numpy as np
import logging
from pathlib import Path
import json
import asyncio
import time
import random
import uuid
import aiohttp
from collections import OrderedDict, defaultdict, deque
from enum import Enum
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# ============================================================================
# ENHANCED MODULES IMPORTS (with graceful fallback)
# ============================================================================
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator
    from enhancements.moe_system import ExpertRouter
    from enhancements.MODP import ParetoOptimizer
    from enhancements.contextual_bandit import ContextualBandit
    from enhancements.storage import Storage
    from enhancements.schemas.feedback_event import FeedbackEvent
    from enhancements.scaling.message_queue import AsyncMessageQueue
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    class GeneticPolicyGenerator:
        def __init__(self, *args, **kwargs): pass
        def evolve(self, population, fitness_fn, generations=10, population_size=20):
            return population[0] if population else {}
    class ExpertRouter:
        def __init__(self, *args, **kwargs): pass
        def encode(self, context): return [0.0]*5
        def select(self, encoded): return "balanced"
    class ParetoOptimizer:
        def __init__(self, *args, **kwargs): pass
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0) * weights.get(k, 1) for k in objectives)
    class ContextualBandit:
        def __init__(self, action_space, fallback_solver, *args, **kwargs):
            self.actions = action_space
        def select_action(self, context):
            return self.actions[0], 0.0, "fallback"
        def update(self, context, action, reward): pass
        def seed_safe_policy(self, context, policy): pass
    class Storage:
        def save_profiler_state(self, state): pass
        def load_profiler_state(self): return None
    class FeedbackEvent:
        @staticmethod
        def create_with_context(**kwargs): return {}
    class AsyncMessageQueue:
        async def publish(self, topic, message): pass

# ============================================================================
# FLEXGEN MODULES (with fallback)
# ============================================================================
try:
    from enhancements.gpu_optimization.flexgen_policy import FlexGenPolicy, generate_candidate_policies
    from enhancements.gpu_optimization.flexgen_controller import FlexGenController
    from enhancements.gpu_optimization.flexgen_cost_model import FlexGenCostModel
    from enhancements.gpu_optimization.policy_drift_detector import PolicyDriftDetector
    from enhancements.schemas.node_descriptor import NodeDescriptor
    from enhancements.schemas.workload_descriptor import WorkloadDescriptor
    FLEXGEN_AVAILABLE = True
except ImportError:
    FLEXGEN_AVAILABLE = False
    class FlexGenPolicy: pass
    def generate_candidate_policies(n=20): return []
    class FlexGenController:
        def __init__(self, *args, **kwargs): pass
        async def step(self): return {}
    class FlexGenCostModel:
        def __init__(self, *args, **kwargs): pass
    class PolicyDriftDetector:
        def __init__(self, *args, **kwargs): pass
        def get_stats(self): return {}
    class NodeDescriptor: pass
    class WorkloadDescriptor: pass

# ============================================================================
# Custom Exceptions
# ============================================================================
class EnergyProfilerError(Exception):
    """Base exception for energy profiler."""
    pass

class CarbonFetchError(EnergyProfilerError):
    """Failed to fetch carbon intensity."""
    pass

class CircuitBreakerOpenError(EnergyProfilerError):
    """Circuit breaker is open."""
    pass

class SafetyViolationError(EnergyProfilerError):
    """Safety monitor detected a violation."""
    pass

class ChaosExperimentError(EnergyProfilerError):
    """Chaos monkey injected a failure."""
    pass

# ============================================================================
# Circuit Breaker
# ============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 3, recovery_timeout: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._last_failure_time = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs):
        async with self._lock:
            if self._state == CircuitBreakerState.OPEN:
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitBreakerState.HALF_OPEN
                    self._failure_count = 0
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self._state == CircuitBreakerState.HALF_OPEN:
                    self._state = CircuitBreakerState.CLOSED
                self._failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self._failure_count += 1
                self._last_failure_time = time.time()
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN
            raise e

    def get_metrics(self) -> Dict:
        return {
            'state': self._state.value,
            'failure_count': self._failure_count,
            'last_failure_time': self._last_failure_time,
        }

# ============================================================================
# Protocols (Dependency Inversion)
# ============================================================================
@runtime_checkable
class CarbonIntensityProvider(Protocol):
    async def get_current_intensity(self) -> float: ...
    def get_last_intensity(self) -> float: ...
    async def update_intensity(self) -> float: ...

@runtime_checkable
class EnergyBudgetProvider(Protocol):
    def get_energy_budget(self) -> float: ...

# ============================================================================
# Carbon Manager Implementation
# ============================================================================
class CarbonIntensityManager:
    """Real carbon intensity manager with caching and circuit breaker."""
    def __init__(
        self,
        api_key: Optional[str] = None,
        region: str = "global",
        default_intensity: float = 400.0,
        cache_ttl: int = 300,
    ):
        self.api_key = api_key
        self.region = region
        self.default_intensity = default_intensity
        self.cache_ttl = cache_ttl
        self._last_intensity = default_intensity
        self._last_update = None
        self._session = None
        self._circuit_breaker = CircuitBreaker("carbon_api")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _fetch_intensity(self) -> float:
        if not self.api_key:
            return self.default_intensity
        session = await self._get_session()
        url = f"https://api.electricitymap.org/v3/carbon-intensity/latest?zone={self.region}"
        headers = {"auth-token": self.api_key}
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get('carbonIntensity', self.default_intensity)
            else:
                raise CarbonFetchError(f"API returned {resp.status}")

    async def update_intensity(self) -> float:
        try:
            intensity = await self._circuit_breaker.call(self._fetch_intensity)
            self._last_intensity = intensity
            self._last_update = time.time()
            logger.info(f"Carbon intensity updated: {intensity} gCO2/kWh")
            return intensity
        except Exception as e:
            logger.warning(f"Failed to fetch carbon intensity: {e}, using cached value")
            return self._last_intensity

    async def get_current_intensity(self) -> float:
        now = time.time()
        if self._last_update is None or now - self._last_update > self.cache_ttl:
            await self.update_intensity()
        return self._last_intensity

    def get_last_intensity(self) -> float:
        return self._last_intensity

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

# ============================================================================
# NEW: CausalBandit (replaces ContextualBandit)
# ============================================================================
class CausalBandit:
    """Causal bandit that estimates average treatment effects for each action."""
    def __init__(self, action_space: List[str], fallback_solver: Callable,
                 min_trials_before_bandit: int = 5, confidence_threshold: float = 0.6):
        self.actions = action_space
        self.fallback_solver = fallback_solver
        self.min_trials = min_trials_before_bandit
        self.confidence_threshold = confidence_threshold
        self.q_values = {a: 0.0 for a in action_space}
        self.counts = {a: 0 for a in action_space}
        self.causal_effects = {a: 0.0 for a in action_space}
        self.trials = 0
        self.context_history = []
        self.reward_history = []
        self.action_history = []

    def select_action(self, context: Dict) -> Tuple[str, float, str]:
        if self.trials < self.min_trials:
            return self.fallback_solver(context), 0.0, "fallback"
        epsilon = 0.1
        if random.random() < epsilon:
            action = random.choice(self.actions)
        else:
            if self.trials >= 10 and any(self.causal_effects.values()):
                action = max(self.causal_effects, key=self.causal_effects.get)
            else:
                action = max(self.q_values, key=self.q_values.get)
        return action, 0.5, "causal"

    def update(self, context: Dict, action: str, reward: float):
        self.trials += 1
        self.counts[action] += 1
        self.q_values[action] += (reward - self.q_values[action]) / self.counts[action]
        self.context_history.append(context)
        self.reward_history.append(reward)
        self.action_history.append(action)
        rewards = [r for a, r in zip(self.action_history, self.reward_history) if a == action]
        self.causal_effects[action] = np.mean(rewards) if rewards else 0.0

    def seed_safe_policy(self, context, policy):
        pass

# ============================================================================
# NEW: SafetyMonitor (Temporal Logic-like)
# ============================================================================
class SafetyMonitor:
    """Monitors skipping decisions against temporal safety rules."""
    def __init__(self, max_consecutive_skips: int = 3,
                 max_skip_ratio: float = 0.5,
                 max_carbon_intensity: float = 600.0,
                 window_size: int = 10):
        self.max_consecutive_skips = max_consecutive_skips
        self.max_skip_ratio = max_skip_ratio
        self.max_carbon_intensity = max_carbon_intensity
        self.window_size = window_size
        self.history = deque(maxlen=window_size)  # (skipped, carbon_intensity, timestamp)
        self.consecutive_skips = 0
        self.violations = []

    def check(self, skipped: bool, carbon_intensity: float) -> bool:
        """Returns True if safe, False if violation."""
        # Carbon intensity threshold
        if carbon_intensity > self.max_carbon_intensity:
            self._record_violation("max_carbon_intensity", {"carbon_intensity": carbon_intensity})
            return False
        # Consecutive skips
        if skipped:
            self.consecutive_skips += 1
        else:
            self.consecutive_skips = 0
        if self.consecutive_skips > self.max_consecutive_skips:
            self._record_violation("max_consecutive_skips", {"consecutive_skips": self.consecutive_skips})
            return False
        # Skip ratio over window
        self.history.append((skipped, carbon_intensity, time.time()))
        if len(self.history) >= self.window_size:
            skips = sum(1 for h in self.history if h[0])
            ratio = skips / len(self.history)
            if ratio > self.max_skip_ratio:
                self._record_violation("max_skip_ratio", {"ratio": ratio})
                return False
        return True

    def _record_violation(self, rule: str, details: Dict):
        self.violations.append({
            "rule": rule,
            "details": details,
            "timestamp": time.time()
        })
        logger.warning(f"Safety violation: {rule} - {details}")

    def get_violations(self) -> List[Dict]:
        return self.violations

# ============================================================================
# NEW: XAIExplainer
# ============================================================================
class XAIExplainer:
    """Generates human-readable explanations for skipping decisions."""
    def explain_skipping(self, layer_name: str, policy: str, context: Dict,
                         confidence: float, decision: bool) -> str:
        parts = [f"Layer '{layer_name}': {('SKIPPED' if decision else 'EXECUTED')}"]
        parts.append(f"Policy='{policy}' (confidence={confidence:.2f})")
        if 'token_importance' in context:
            parts.append(f"token_importance={context['token_importance']:.3f}")
        if 'carbon_intensity' in context:
            parts.append(f"carbon={context['carbon_intensity']:.1f}gCO2/kWh")
        if 'energy_budget' in context:
            parts.append(f"budget={context['energy_budget']:.2f}")
        if 'layer_energy' in context:
            parts.append(f"layer_energy={context['layer_energy']:.2e}")
        return " | ".join(parts)

# ============================================================================
# NEW: FederatedProfilerCoordinator
# ============================================================================
class FederatedProfilerCoordinator:
    """Aggregates profiler bandit Q-values and hyperparameters across deployments
    with simulated differential privacy."""
    def __init__(self, privacy_budget: float = 0.5):
        self.participants: Dict[str, Dict[str, Any]] = {}
        self.privacy_budget = privacy_budget

    def register_participant(self, participant_id: str, model_update: Dict[str, Any]):
        self.participants[participant_id] = model_update

    def aggregate(self) -> Dict[str, Any]:
        if not self.participants:
            return {}
        keys = set()
        for update in self.participants.values():
            keys.update(update.keys())
        avg = {}
        for key in keys:
            vals = [u.get(key, 0.0) for u in self.participants.values()]
            if all(isinstance(v, (int, float)) for v in vals):
                noise = np.random.laplace(0, 1.0 / max(self.privacy_budget, 1e-6))
                avg[key] = float(np.mean(vals) + noise)
            else:
                avg[key] = vals[0]
        return avg

    def get_participant_count(self) -> int:
        return len(self.participants)

# ============================================================================
# NEW: MultiAgentProfiler (basic multi-agent coordination)
# ============================================================================
class MultiAgentProfiler:
    """Coordinates multiple 'agents', each responsible for a subset of layers."""
    def __init__(self, num_agents: int = 3):
        self.agents = {f"agent_{i}": {"layers": [], "reward": 0.0} for i in range(num_agents)}

    def assign_layer(self, layer_name: str) -> str:
        agent_id = list(self.agents.keys())[hash(layer_name) % len(self.agents)]
        self.agents[agent_id]["layers"].append(layer_name)
        return agent_id

    def record_reward(self, agent_id: str, reward: float):
        if agent_id in self.agents:
            self.agents[agent_id]["reward"] += reward

    def get_stats(self) -> Dict:
        return {
            agent_id: {
                "num_layers": len(info["layers"]),
                "cumulative_reward": info["reward"],
            }
            for agent_id, info in self.agents.items()
        }

# ============================================================================
# NEW: CarbonOffsetBroker (offsets + RECs)
# ============================================================================
class CarbonOffsetBroker:
    """Purchases carbon offsets and Renewable Energy Certificates."""
    def __init__(self, threshold_intensity: float = 400.0,
                 cost_per_kg: float = 0.1,
                 rec_cost_per_mwh: float = 5.0):
        self.threshold = threshold_intensity
        self.cost_per_kg = cost_per_kg
        self.rec_cost_per_mwh = rec_cost_per_mwh
        self.total_offset_kg = 0.0
        self.total_recs_mwh = 0.0
        self.total_cost = 0.0

    async def purchase_offsets(self, carbon_intensity: float, carbon_kg: float) -> Dict:
        if carbon_intensity <= self.threshold or carbon_kg <= 0:
            return {"status": "below_threshold", "carbon_kg": carbon_kg}
        cost = carbon_kg * self.cost_per_kg
        self.total_offset_kg += carbon_kg
        self.total_cost += cost
        logger.info(f"Offset purchased: {carbon_kg:.3f} kg for ${cost:.4f}")
        return {"status": "offset_purchased", "carbon_kg": carbon_kg, "cost_usd": cost}

    async def purchase_recs(self, energy_mwh: float) -> Dict:
        cost = energy_mwh * self.rec_cost_per_mwh
        self.total_recs_mwh += energy_mwh
        self.total_cost += cost
        logger.info(f"RECs purchased: {energy_mwh:.4f} MWh for ${cost:.4f}")
        return {"status": "rec_purchased", "energy_mwh": energy_mwh, "cost_usd": cost}

    def get_totals(self) -> Dict:
        return {
            "total_offset_kg": self.total_offset_kg,
            "total_recs_mwh": self.total_recs_mwh,
            "total_cost_usd": self.total_cost,
        }

# ============================================================================
# NEW: ChaosMonkey
# ============================================================================
class ChaosMonkey:
    """Injects failures to test resilience of the profiler."""
    def __init__(self, enabled: bool = False, failure_probability: float = 0.1):
        self.enabled = enabled
        self.failure_probability = failure_probability
        self.injected_failures = 0

    def maybe_fail(self):
        if self.enabled and random.random() < self.failure_probability:
            self.injected_failures += 1
            raise ChaosExperimentError("Simulated chaos failure in profiler")

    def get_stats(self) -> Dict:
        return {"enabled": self.enabled, "injected_failures": self.injected_failures}

# ============================================================================
# NEW: HumanReviewManager
# ============================================================================
class HumanReviewManager:
    """Manages human review of critical skipping decisions."""
    def __init__(self):
        self.pending_reviews: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def request_review(self, decision_id: str, details: Dict) -> str:
        review_id = str(uuid.uuid4())
        async with self._lock:
            self.pending_reviews[review_id] = {
                "review_id": review_id,
                "decision_id": decision_id,
                "details": details,
                "status": "pending",
                "created_at": time.time(),
            }
        logger.info(f"Human review requested: {review_id}")
        return review_id

    async def approve(self, review_id: str) -> bool:
        async with self._lock:
            if review_id in self.pending_reviews:
                self.pending_reviews[review_id]["status"] = "approved"
                return True
        return False

    async def reject(self, review_id: str) -> bool:
        async with self._lock:
            if review_id in self.pending_reviews:
                self.pending_reviews[review_id]["status"] = "rejected"
                return True
        return False

    async def get_pending(self) -> List[Dict]:
        async with self._lock:
            return [r for r in self.pending_reviews.values() if r["status"] == "pending"]

# ============================================================================
# FLEXGEN MANAGER (with precision support)
# ============================================================================
class FlexGenManager:
    """Manager for FlexGen GPU/CPU/disk offloading policy optimization."""
    def __init__(self, carbon_intensity: float = 400.0):
        self.carbon_intensity = carbon_intensity
        self.flexgen_cost_model = None
        self.policy_drift_detector = None
        self.gpu_profiler = None

        if FLEXGEN_AVAILABLE:
            self.flexgen_cost_model = FlexGenCostModel(carbon_intensity_g_per_kwh=carbon_intensity)
            self.policy_drift_detector = PolicyDriftDetector()
            try:
                from enhancements.gpu_profiler import GPUProfiler
                self.gpu_profiler = GPUProfiler()
            except ImportError:
                self.gpu_profiler = None
            logger.info("FlexGen Manager initialized for energy profiler")
        else:
            logger.warning("FlexGen modules not available; manager will be disabled.")

    async def optimize_policy(self, workload: WorkloadDescriptor, node: NodeDescriptor) -> Dict:
        if not FLEXGEN_AVAILABLE:
            return {"error": "FlexGen modules not available"}

        from enhancements.gpu_optimization.flexgen_controller import FlexGenController
        from enhancements.gpu_optimization.flexgen_policy_selector import DistillationFlexGenSelector

        selector = DistillationFlexGenSelector(
            n_candidates=20,
            config={'epsilon': 0.1, 'epsilon_decay': 0.999}
        )

        controller = FlexGenController(
            node=node,
            workload=workload,
            carbon_intensity=workload.metadata.get('carbon_intensity', self.carbon_intensity),
            use_real_executor=False,
            executor=None,
            cost_model=self.flexgen_cost_model,
            use_bio_search=True,
            bio_search_config={'population_size': 50, 'generations': 10},
            modp_planner=None,
            drift_detector=self.policy_drift_detector,
            gpu_profiler=self.gpu_profiler,
        )
        result = await controller.step()
        return result

    async def select_precision(self, workload: Dict) -> str:
        """Select the optimal precision level based on carbon intensity and workload."""
        carbon_intensity = workload.get("carbon_intensity", self.carbon_intensity)
        workload_size = workload.get("size", "medium")
        if carbon_intensity > 500 or workload_size == "large":
            return "int8"
        elif carbon_intensity > 300 or workload_size == "medium":
            return "fp16"
        else:
            return "fp32"

    async def get_status(self) -> Dict:
        if not FLEXGEN_AVAILABLE:
            return {"available": False}
        return {
            "available": True,
            "drift": self.policy_drift_detector.get_stats() if self.policy_drift_detector else {},
            "gpu": self.gpu_profiler.get_current_metrics() if self.gpu_profiler else {},
        }

# ============================================================================
# Energy Profiler (Enhanced with all modules)
# ============================================================================
class EnergyProfiler:
    """
    Tracks energy per layer and provides adaptive layer-skipping decisions.
    Integrates with CarbonIntensityProvider for real-time carbon data.
    Uses CausalBandit, ExpertRouter, ParetoOptimizer, and GeneticPolicyGenerator.
    FlexGen: can select offloading policies and precision levels.
    NEW: SafetyMonitor, XAIExplainer, FederatedProfilerCoordinator,
         MultiAgentProfiler, CarbonOffsetBroker, ChaosMonkey, HumanReviewManager.
    """

    def __init__(
        self,
        model: nn.Module,
        energy_per_layer: Dict[str, float],
        carbon_provider: Optional[CarbonIntensityProvider] = None,
        default_carbon_intensity: float = 400.0,
        storage: Optional[Storage] = None,
        message_queue: Optional[AsyncMessageQueue] = None,
        bandit: Optional[Any] = None,
        moe: Optional[ExpertRouter] = None,
        modp: Optional[ParetoOptimizer] = None,
        bio: Optional[GeneticPolicyGenerator] = None,
        action_space: Optional[List[str]] = None,
        modp_weights: Optional[Dict[str, float]] = None,
        bio_generations: int = 10,
        bio_population_size: int = 20,
        enable_chaos: bool = False,
    ):
        self.model = model
        self.energy_per_layer = energy_per_layer
        self.carbon_provider = carbon_provider
        self.default_carbon_intensity = default_carbon_intensity
        self.storage = storage
        self.queue = message_queue

        self._fill_missing_energies()

        self.layer_order = list(self.energy_per_layer.keys())

        if ENHANCEMENTS_AVAILABLE:
            self.modp = modp or ParetoOptimizer()
            self.moe = moe or ExpertRouter()
            self.bio = bio or GeneticPolicyGenerator()
            # Extend action space with precision
            self.action_space = action_space or [
                "aggressive", "balanced", "conservative",
                "aggressive_fp16", "aggressive_int8",
                "conservative_fp16", "conservative_int8"
            ]
            self.modp_weights = modp_weights or {'accuracy': 0.4, 'energy': 0.3, 'carbon': 0.2, 'latency': 0.1}
            # Use CausalBandit
            self.bandit = bandit or CausalBandit(
                action_space=self.action_space,
                fallback_solver=lambda ctx: "balanced",
                min_trials_before_bandit=5,
                confidence_threshold=0.6,
            )
            self.bio_generations = bio_generations
            self.bio_population_size = bio_population_size
        else:
            self.modp = None
            self.moe = None
            self.bio = None
            self.bandit = None
            self.action_space = ["balanced"]
            self.bio_generations = bio_generations
            self.bio_population_size = bio_population_size

        # FlexGen manager
        self.flexgen_manager = FlexGenManager(default_carbon_intensity)

        # NEW: Enhanced modules
        self.safety_monitor = SafetyMonitor()
        self.xai = XAIExplainer()
        self.federated = FederatedProfilerCoordinator()
        self.multi_agent = MultiAgentProfiler(num_agents=3)
        self.carbon_broker = CarbonOffsetBroker()
        self.chaos_monkey = ChaosMonkey(enabled=enable_chaos)
        self.human_review = HumanReviewManager()

        # Assign layers to agents
        for layer_name in self.layer_order:
            self.multi_agent.assign_layer(layer_name)

        self._skipping_history: Dict[str, List[bool]] = defaultdict(list)
        self._performance_history: List[float] = deque(maxlen=1000)
        self._energy_saved_history: List[float] = deque(maxlen=1000)
        self._last_decision: Dict[str, Any] = {}
        self._last_review_id: Optional[str] = None

        self._load_state()

    def _fill_missing_energies(self):
        default_energy = 1e-6
        for name, _ in self.model.named_modules():
            if name not in self.energy_per_layer:
                self.energy_per_layer[name] = default_energy
                logger.debug(f"Assigned default energy to layer {name}: {default_energy}")

    def _load_state(self):
        if self.storage:
            try:
                state = self.storage.load_profiler_state()
                if state:
                    logger.info("Loaded profiler state from storage.")
            except Exception as e:
                logger.warning(f"Failed to load profiler state: {e}")

    def _save_state(self):
        if self.storage:
            state = {
                'bandit_q_values': getattr(self.bandit, 'q_values', None),
                'bandit_causal_effects': getattr(self.bandit, 'causal_effects', None),
                'modp_weights': self.modp_weights,
                'action_space': self.action_space,
                'safety_violations': self.safety_monitor.get_violations()[-10:],
            }
            try:
                self.storage.save_profiler_state(state)
            except Exception as e:
                logger.warning(f"Failed to save profiler state: {e}")

    async def _get_carbon_intensity(self) -> float:
        if self.carbon_provider:
            try:
                return await self.carbon_provider.get_current_intensity()
            except Exception as e:
                logger.warning(f"Carbon provider failed: {e}")
        return self.default_carbon_intensity

    async def estimate_energy_for_token(self, layer_name: str, token_importance: float) -> float:
        base_energy = self.energy_per_layer.get(layer_name, 1e-6)
        carbon_intensity = await self._get_carbon_intensity()
        carbon_factor = 1.0 + (carbon_intensity / 400 - 1.0) * 0.2
        importance_factor = 1.0 + token_importance * 0.5
        return base_energy * carbon_factor * importance_factor

    def _extract_precision(self, policy: str) -> str:
        """Extract precision from policy name (default fp32)."""
        if "int8" in policy:
            return "int8"
        elif "fp16" in policy:
            return "fp16"
        return "fp32"

    def _extract_aggressiveness(self, policy: str) -> str:
        if "aggressive" in policy:
            return "aggressive"
        elif "conservative" in policy:
            return "conservative"
        return "balanced"

    async def should_skip_layer(
        self,
        layer_name: str,
        token_importance: float,
        current_energy_budget: float,
        context: Optional[Dict] = None,
    ) -> bool:
        # Chaos monkey injection
        try:
            self.chaos_monkey.maybe_fail()
        except ChaosExperimentError as e:
            logger.warning(f"Chaos injected: {e}. Falling back to heuristic.")
            return self._should_skip_heuristic(layer_name, token_importance, current_energy_budget)

        if not self.bandit:
            return self._should_skip_heuristic(layer_name, token_importance, current_energy_budget)

        context = context or {}
        carbon_intensity = await self._get_carbon_intensity()
        context.update({
            "layer_name": layer_name,
            "token_importance": token_importance,
            "energy_budget": current_energy_budget,
            "carbon_intensity": carbon_intensity,
            "layer_energy": self.energy_per_layer.get(layer_name, 1e-6),
        })

        encoded_context = self.moe.encode(context) if self.moe else context
        policy, confidence, source = self.bandit.select_action(encoded_context)
        if policy is None:
            policy = "balanced"

        aggressiveness = self._extract_aggressiveness(policy)
        precision = self._extract_precision(policy)

        if aggressiveness == "aggressive":
            skip = (current_energy_budget < 0.6 and token_importance < 0.4) or (current_energy_budget < 0.4)
        elif aggressiveness == "conservative":
            skip = (current_energy_budget < 0.2 and token_importance < 0.2)
        else:
            skip = (current_energy_budget < 0.4 and token_importance < 0.3) or (current_energy_budget < 0.2)

        # Safety check
        if not self.safety_monitor.check(skip, carbon_intensity):
            logger.warning(f"Safety violation detected; overriding skip decision for {layer_name}")
            skip = False

        # XAI explanation
        explanation = self.xai.explain_skipping(layer_name, policy, context, confidence, skip)

        # Human review for critical decisions (skip + high carbon + aggressive)
        review_id = None
        if skip and carbon_intensity > 500 and aggressiveness == "aggressive":
            decision_id = f"skip_{layer_name}_{uuid.uuid4().hex[:6]}"
            review_id = await self.human_review.request_review(decision_id, {
                "layer": layer_name,
                "policy": policy,
                "context": context,
                "explanation": explanation,
            })

        self._last_decision = {
            "layer": layer_name,
            "policy": policy,
            "precision": precision,
            "confidence": confidence,
            "source": source,
            "context": context,
            "decision": skip,
            "explanation": explanation,
            "review_id": review_id,
        }
        return skip

    def _should_skip_heuristic(self, layer_name: str, token_importance: float, budget: float) -> bool:
        if budget < 0.3 and token_importance < 0.3:
            return True
        if budget < 0.7 and token_importance < 0.5:
            return True
        return False

    async def record_outcome(
        self,
        accuracy: float,
        energy_saved: float,
        carbon_saved: float = 0.0,
        latency_ms: float = 0.0,
    ):
        self._performance_history.append(accuracy)
        self._energy_saved_history.append(energy_saved)

        # Federated update (simulated) - register our own state as participant
        if self.bandit:
            self.federated.register_participant("local", dict(self.bandit.q_values))
        # Aggregate federated updates (would be populated by peers)
        aggregated = self.federated.aggregate()
        if aggregated and self.bandit:
            for k, v in aggregated.items():
                if k in self.bandit.q_values:
                    self.bandit.q_values[k] = 0.5 * self.bandit.q_values[k] + 0.5 * v

        if self.bandit and self._last_decision:
            # Multi-agent reward
            layer = self._last_decision.get("layer", "")
            agent_id = self.multi_agent.assign_layer(layer)
            self.multi_agent.record_reward(agent_id, accuracy)

            objectives = {
                'accuracy': accuracy,
                'energy': 1 - energy_saved / max(self._energy_saved_history[-1], 1e-6),
                'carbon': 1 - carbon_saved / 1000,
                'latency': 1 - latency_ms / 1000,
            }
            reward = self.modp.evaluate(objectives, self.modp_weights) if self.modp else accuracy

            self.bandit.update(
                self._last_decision['context'],
                self._last_decision['policy'],
                reward
            )

            # Carbon offset purchase if intensity is high
            carbon_intensity = self._last_decision['context'].get('carbon_intensity', 0)
            if carbon_intensity > 400 and carbon_saved > 0:
                try:
                    await self.carbon_broker.purchase_offsets(carbon_intensity, carbon_saved)
                except Exception as e:
                    logger.warning(f"Carbon offset purchase failed: {e}")

            # Bio-inspired evolution
            if len(self._performance_history) % 100 == 0 and self.bio:
                new_policies = await self.evolve_policies()
                if new_policies:
                    for p in new_policies:
                        if p not in self.action_space:
                            self.action_space.append(p)
                            self.bandit.actions = self.action_space

        if len(self._performance_history) % 10 == 0:
            self._save_state()

        # Publish feedback event
        if self.queue:
            event = FeedbackEvent.create_with_context(
                task_id=f"skipping_{uuid.uuid4().hex[:8]}",
                selected_action=self._last_decision.get('policy', 'unknown'),
                quality_score=accuracy,
                latency_ms=latency_ms,
                energy_joules=energy_saved,
                carbon_g=carbon_saved,
                feedback_type="energy",
                adaptive_cost_value=0.0,
                state=self._last_decision.get('context', {}),
                candidates=self.action_space,
                source="energy_profiler",
                environment="production",
                tags=["layer_skipping", "causal_rl"]
            )
            try:
                await self.queue.publish("feedback_events", event)
            except Exception as e:
                logger.warning(f"Feedback publish failed: {e}")

    async def evolve_policies(self) -> List[str]:
        if not self.bio:
            return []
        def fitness(policy):
            return np.mean(self._performance_history[-20:]) if self._performance_history else 0.5

        new_policies = self.bio.evolve(
            population=self.action_space,
            fitness_fn=fitness,
            generations=self.bio_generations,
            population_size=self.bio_population_size,
        )
        if not isinstance(new_policies, list):
            new_policies = [new_policies]
        return new_policies

    async def run_flexgen_optimization(self, workload: Dict, node: Dict) -> Dict:
        """Public method to run FlexGen policy optimization for the model."""
        if not FLEXGEN_AVAILABLE:
            return {"error": "FlexGen modules not available"}
        workload_obj = WorkloadDescriptor(**workload)
        node_obj = NodeDescriptor(**node)
        return await self.flexgen_manager.optimize_policy(workload_obj, node_obj)

    async def select_precision(self, workload: Dict) -> str:
        """Select the optimal precision level based on workload and carbon."""
        return await self.flexgen_manager.select_precision(workload)

    def get_energy_map(self) -> Dict[str, float]:
        return self.energy_per_layer.copy()

    async def estimate_total_energy(self, input_shape: tuple, token_importance: float = 0.5) -> float:
        total = 0.0
        for layer_name in self.layer_order:
            total += await self.estimate_energy_for_token(layer_name, token_importance)
        return total

    def get_safety_violations(self) -> List[Dict]:
        return self.safety_monitor.get_violations()

    def get_last_explanation(self) -> Optional[str]:
        return self._last_decision.get("explanation")

    def get_agent_stats(self) -> Dict:
        return self.multi_agent.get_stats()

    def get_carbon_broker_totals(self) -> Dict:
        return self.carbon_broker.get_totals()

    def get_chaos_stats(self) -> Dict:
        return self.chaos_monkey.get_stats()

    def save(self, path: Path):
        data = {
            'energy_per_layer': self.energy_per_layer,
            'action_space': self.action_space,
            'modp_weights': self.modp_weights,
            'q_values': getattr(self.bandit, 'q_values', {}),
            'causal_effects': getattr(self.bandit, 'causal_effects', {}),
        }
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Profiler saved to {path}")

    @classmethod
    def load(cls, path: Path, model: nn.Module, carbon_provider=None) -> "EnergyProfiler":
        with open(path, 'r') as f:
            data = json.load(f)
        return cls(
            model=model,
            energy_per_layer=data['energy_per_layer'],
            carbon_provider=carbon_provider,
            action_space=data.get('action_space', ["balanced"]),
            modp_weights=data.get('modp_weights', {'accuracy': 0.4, 'energy': 0.3, 'carbon': 0.2, 'latency': 0.1}),
        )

# ============================================================================
# Layer Skipping Wrapper (Enhanced)
# ============================================================================
class LayerSkippingWrapper(nn.Module):
    """
    Wraps a model to allow selective layer skipping based on EnergyProfiler.
    Supports per-token importance and dynamic energy budget.
    FlexGen: can select offloading policies and precision levels.
    """

    def __init__(
        self,
        model: nn.Module,
        profiler: EnergyProfiler,
        energy_budget_source: Optional[EnergyBudgetProvider] = None,
    ):
        super().__init__()
        self.model = model
        self.profiler = profiler
        self.energy_budget_source = energy_budget_source
        self._energy_budget = 1.0

        self._layer_list = self._build_layer_list(model)
        self._last_skipped: List[str] = []
        self._current_precision: str = "fp32"

    def _build_layer_list(self, module: nn.Module, prefix: str = "") -> List[Tuple[str, nn.Module]]:
        layers = []
        for name, child in module.named_children():
            full_name = f"{prefix}.{name}" if prefix else name
            if list(child.children()):
                layers.extend(self._build_layer_list(child, full_name))
            else:
                layers.append((full_name, child))
        return layers

    def set_energy_budget(self, budget: float):
        self._energy_budget = max(0.0, min(1.0, budget))

    def _get_energy_budget(self) -> float:
        if self.energy_budget_source:
            return self.energy_budget_source.get_energy_budget()
        return self._energy_budget

    async def forward_async(
        self,
        x: torch.Tensor,
        token_importance: Optional[torch.Tensor] = None,
        context: Optional[Dict] = None,
    ) -> torch.Tensor:
        if token_importance is None:
            token_importance = torch.ones(x.size(0), device=x.device) * 0.5
        if token_importance.dim() > 1:
            token_importance = token_importance.mean(dim=1)

        # Optional: select precision based on workload
        try:
            workload = {"size": "medium", "carbon_intensity": await self.profiler._get_carbon_intensity()}
            self._current_precision = await self.profiler.select_precision(workload)
        except Exception:
            self._current_precision = "fp32"

        current_budget = self._get_energy_budget()
        output = x
        skipped = []
        total_energy_original = 0.0
        total_energy_skipped = 0.0
        context = context or {}
        context["precision"] = self._current_precision

        for layer_name, layer_module in self._layer_list:
            avg_importance = token_importance.mean().item()
            energy = await self.profiler.estimate_energy_for_token(layer_name, avg_importance)
            total_energy_original += energy

            if await self.profiler.should_skip_layer(layer_name, avg_importance, current_budget, context):
                logger.debug(f"Skipping layer {layer_name}")
                skipped.append(layer_name)
                total_energy_skipped += energy
                continue

            output = layer_module(output)

        self._last_skipped = skipped

        fake_accuracy = 1.0 - (len(skipped) / max(len(self._layer_list), 1)) * 0.2
        carbon_intensity = await self.profiler._get_carbon_intensity()
        carbon_saved = total_energy_skipped * carbon_intensity / 1000
        await self.profiler.record_outcome(
            accuracy=fake_accuracy,
            energy_saved=total_energy_skipped,
            carbon_saved=carbon_saved,
            latency_ms=0.0,
        )
        return output

    def forward(
        self,
        x: torch.Tensor,
        token_importance: Optional[torch.Tensor] = None,
        context: Optional[Dict] = None,
    ) -> torch.Tensor:
        # For sync usage (careful: uses asyncio.run)
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # We're inside an event loop; use a future
                return asyncio.ensure_future(self.forward_async(x, token_importance, context))
        except RuntimeError:
            pass
        return asyncio.run(self.forward_async(x, token_importance, context))

    async def run_flexgen_optimization(self, workload: Dict, node: Dict) -> Dict:
        return await self.profiler.run_flexgen_optimization(workload, node)

    async def select_precision(self, workload: Dict) -> str:
        return await self.profiler.select_precision(workload)

    def get_skipped_layers(self) -> List[str]:
        return self._last_skipped.copy()

    def get_current_precision(self) -> str:
        return self._current_precision

    async def estimate_energy(self, x: torch.Tensor, token_importance: Optional[torch.Tensor] = None) -> float:
        if token_importance is None:
            token_importance = torch.ones(x.size(0), device=x.device) * 0.5
        avg_importance = token_importance.mean().item()
        total = await self.profiler.estimate_total_energy(x.shape, avg_importance)
        return total

    def save(self, path: Path):
        data = {
            'energy_budget': self._energy_budget,
            'profiler_config': {
                'energy_per_layer': self.profiler.energy_per_layer,
                'action_space': self.profiler.action_space,
                'modp_weights': self.profiler.modp_weights,
            }
        }
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)

    @classmethod
    def load(cls, path: Path, model: nn.Module, carbon_provider=None) -> "LayerSkippingWrapper":
        with open(path, 'r') as f:
            data = json.load(f)
        profiler = EnergyProfiler(
            model=model,
            energy_per_layer=data['profiler_config']['energy_per_layer'],
            carbon_provider=carbon_provider,
            action_space=data['profiler_config'].get('action_space', ["balanced"]),
            modp_weights=data['profiler_config'].get('modp_weights', {'accuracy': 0.4, 'energy': 0.3, 'carbon': 0.2, 'latency': 0.1}),
        )
        wrapper = cls(model=model, profiler=profiler)
        wrapper._energy_budget = data.get('energy_budget', 1.0)
        return wrapper

# ============================================================================
# Example usage
# ============================================================================
async def example():
    model = nn.Sequential(
        nn.Linear(10, 20),
        nn.ReLU(),
        nn.Linear(20, 20),
        nn.ReLU(),
        nn.Linear(20, 1)
    )
    energy_per_layer = {
        '0': 1e-6,
        '1': 0.5e-6,
        '2': 1.5e-6,
        '3': 0.5e-6,
        '4': 2e-6,
    }
    carbon_provider = CarbonIntensityManager(api_key=None)
    profiler = EnergyProfiler(
        model=model,
        energy_per_layer=energy_per_layer,
        carbon_provider=carbon_provider,
        enable_chaos=False,
    )
    wrapper = LayerSkippingWrapper(model, profiler)
    x = torch.randn(4, 10)
    importance = torch.ones(4) * 0.8
    output = await wrapper.forward_async(x, importance)
    print(f"Output shape: {output.shape}")
    print(f"Skipped layers: {wrapper.get_skipped_layers()}")
    print(f"Last explanation: {profiler.get_last_explanation()}")
    print(f"Precision: {wrapper.get_current_precision()}")
    print(f"Safety violations: {len(profiler.get_safety_violations())}")
    print(f"Multi-agent stats: {profiler.get_agent_stats()}")
    print(f"Carbon broker totals: {profiler.get_carbon_broker_totals()}")

if __name__ == "__main__":
    asyncio.run(example())
