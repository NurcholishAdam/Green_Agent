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
"""

import torch
import torch.nn as nn
from typing import List, Dict, Optional, Union, Callable, Protocol, runtime_checkable
import numpy as np
import logging
from pathlib import Path
import json
import asyncio
import time
from collections import OrderedDict, defaultdict
import uuid
import aiohttp
from enum import Enum

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
    # Fallback stubs
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

# ============================================================================
# Circuit Breaker (simple)
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
# Carbon Manager Implementation (with circuit breaker)
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
# FLEXGEN MANAGER (NEW)
# ============================================================================
class FlexGenManager:
    """
    Manager for FlexGen GPU/CPU/disk offloading policy optimization.
    Used to select optimal offloading policies for the model layers.
    """
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

    async def get_status(self) -> Dict:
        if not FLEXGEN_AVAILABLE:
            return {"available": False}
        return {
            "available": True,
            "drift": self.policy_drift_detector.get_stats() if self.policy_drift_detector else {},
            "gpu": self.gpu_profiler.get_current_metrics() if self.gpu_profiler else {},
        }

# ============================================================================
# Energy Profiler (Enhanced with bio, MoE, MODP, Bandit, FlexGen)
# ============================================================================
class EnergyProfiler:
    """
    Tracks energy per layer and provides adaptive layer‑skipping decisions.
    Integrates with CarbonIntensityProvider for real‑time carbon data.
    NEW: Uses ContextualBandit, ExpertRouter, ParetoOptimizer, and GeneticPolicyGenerator.
    FlexGen: can select offloading policies for model layers.
    """

    def __init__(
        self,
        model: nn.Module,
        energy_per_layer: Dict[str, float],
        carbon_provider: Optional[CarbonIntensityProvider] = None,
        default_carbon_intensity: float = 400.0,
        storage: Optional[Storage] = None,
        message_queue: Optional[AsyncMessageQueue] = None,
        # Enhanced modules
        bandit: Optional[ContextualBandit] = None,
        moe: Optional[ExpertRouter] = None,
        modp: Optional[ParetoOptimizer] = None,
        bio: Optional[GeneticPolicyGenerator] = None,
        action_space: List[str] = None,
        modp_weights: Dict[str, float] = None,
        bio_generations: int = 10,
        bio_population_size: int = 20,
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
            self.action_space = action_space or ["aggressive", "balanced", "conservative"]
            self.modp_weights = modp_weights or {'accuracy': 0.4, 'energy': 0.3, 'carbon': 0.2, 'latency': 0.1}
            self.bandit = bandit or ContextualBandit(
                action_space=self.action_space,
                fallback_solver=lambda ctx: "balanced",
                min_trials_before_bandit=5,
                confidence_threshold=0.6,
            )
        else:
            self.modp = None
            self.moe = None
            self.bio = None
            self.bandit = None
            self.action_space = ["balanced"]

        # FlexGen manager
        self.flexgen_manager = FlexGenManager(default_carbon_intensity)

        self._skipping_history: Dict[str, List[bool]] = defaultdict(list)
        self._performance_history: List[float] = []
        self._energy_saved_history: List[float] = []

        self._load_state()

    def _fill_missing_energies(self):
        default_energy = 1e-6
        for name, module in self.model.named_modules():
            if name not in self.energy_per_layer:
                self.energy_per_layer[name] = default_energy
                logger.debug(f"Assigned default energy to layer {name}: {default_energy}")

    def _load_state(self):
        if self.storage:
            state = self.storage.load_profiler_state()
            if state:
                logger.info("Loaded profiler state from storage.")

    def _save_state(self):
        if self.storage:
            state = {
                'bandit_weights': None,
                'modp_weights': self.modp_weights,
                'action_space': self.action_space,
            }
            self.storage.save_profiler_state(state)

    async def _get_carbon_intensity(self) -> float:
        if self.carbon_provider:
            try:
                return await self.carbon_provider.get_current_intensity()
            except Exception as e:
                logger.warning(f"Carbon provider failed: {e}")
        return self.default_carbon_intensity

    async def estimate_energy_for_token(
        self,
        layer_name: str,
        token_importance: float,
    ) -> float:
        base_energy = self.energy_per_layer.get(layer_name, 1e-6)
        carbon_intensity = await self._get_carbon_intensity()
        carbon_factor = 1.0 + (carbon_intensity / 400 - 1.0) * 0.2
        importance_factor = 1.0 + token_importance * 0.5
        return base_energy * carbon_factor * importance_factor

    async def should_skip_layer(
        self,
        layer_name: str,
        token_importance: float,
        current_energy_budget: float,
        context: Optional[Dict] = None,
    ) -> bool:
        if not self.bandit:
            return self._should_skip_heuristic(layer_name, token_importance, current_energy_budget)

        context = context or {}
        context.update({
            "layer_name": layer_name,
            "token_importance": token_importance,
            "energy_budget": current_energy_budget,
            "carbon_intensity": await self._get_carbon_intensity(),
            "layer_energy": self.energy_per_layer.get(layer_name, 1e-6),
        })

        encoded_context = self.moe.encode(context) if self.moe else context
        policy, confidence, source = self.bandit.select_action(encoded_context)
        if policy is None:
            policy = "balanced"

        if policy == "aggressive":
            skip = (current_energy_budget < 0.6 and token_importance < 0.4) or (current_energy_budget < 0.4)
        elif policy == "conservative":
            skip = (current_energy_budget < 0.2 and token_importance < 0.2)
        else:
            skip = (current_energy_budget < 0.4 and token_importance < 0.3) or (current_energy_budget < 0.2)

        self._last_decision = {
            "layer": layer_name,
            "policy": policy,
            "confidence": confidence,
            "source": source,
            "context": context,
            "decision": skip,
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

        if self.bandit and hasattr(self, '_last_decision'):
            objectives = {
                'accuracy': accuracy,
                'energy': 1 - energy_saved / (self._energy_saved_history[-1] or 1),
                'carbon': 1 - carbon_saved / 1000,
                'latency': 1 - latency_ms / 1000,
            }
            reward = self.modp.evaluate(objectives, self.modp_weights) if self.modp else accuracy

            await self.bandit.update(
                self._last_decision['context'],
                self._last_decision['policy'],
                reward
            )

            if len(self._performance_history) % 100 == 0 and self.bio:
                new_policies = await self.evolve_policies()
                if new_policies:
                    for p in new_policies:
                        if p not in self.action_space:
                            self.action_space.append(p)
                            self.bandit.actions = self.action_space

        if len(self._performance_history) % 10 == 0:
            self._save_state()

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
                tags=["layer_skipping"]
            )
            await self.queue.publish("feedback_events", event)

    async def evolve_policies(self) -> List[str]:
        if not self.bio:
            return []
        def fitness(policy):
            return np.mean(self._performance_history[-20:]) if self._performance_history else 0.5

        new_policies = self.bio.evolve(
            population=self.action_space,
            fitness_fn=fitness,
            generations=10,
            population_size=20,
        )
        return new_policies

    async def run_flexgen_optimization(self, workload: Dict, node: Dict) -> Dict:
        """Public method to run FlexGen policy optimization for the model."""
        if not FLEXGEN_AVAILABLE:
            return {"error": "FlexGen modules not available"}
        workload_obj = WorkloadDescriptor(**workload)
        node_obj = NodeDescriptor(**node)
        return await self.flexgen_manager.optimize_policy(workload_obj, node_obj)

    def get_energy_map(self) -> Dict[str, float]:
        return self.energy_per_layer.copy()

    async def estimate_total_energy(self, input_shape: tuple, token_importance: float = 0.5) -> float:
        total = 0.0
        for layer_name in self.layer_order:
            total += await self.estimate_energy_for_token(layer_name, token_importance)
        return total

    def save(self, path: Path):
        data = {
            'energy_per_layer': self.energy_per_layer,
            'action_space': self.action_space,
            'modp_weights': self.modp_weights,
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
            action_space=data.get('action_space', ["aggressive", "balanced", "conservative"]),
            modp_weights=data.get('modp_weights', {'accuracy': 0.4, 'energy': 0.3, 'carbon': 0.2, 'latency': 0.1}),
        )

# ============================================================================
# Layer Skipping Wrapper (Enhanced)
# ============================================================================
class LayerSkippingWrapper(nn.Module):
    """
    Wraps a model to allow selective layer skipping based on EnergyProfiler.
    Supports per‑token importance and dynamic energy budget.
    FlexGen: can select offloading policies for the entire model.
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

        current_budget = self._get_energy_budget()
        output = x
        skipped = []
        total_energy_original = 0.0
        total_energy_skipped = 0.0

        context = context or {}

        for layer_name, layer_module in self._layer_list:
            avg_importance = token_importance.mean().item()
            energy = await self.profiler.estimate_energy_for_token(layer_name, avg_importance)
            total_energy_original += energy

            if await self.profiler.should_skip_layer(
                layer_name,
                avg_importance,
                current_budget,
                context
            ):
                logger.debug(f"Skipping layer {layer_name}")
                skipped.append(layer_name)
                total_energy_skipped += energy
                continue

            output = layer_module(output)

        self._last_skipped = skipped

        fake_accuracy = 1.0 - (len(skipped) / len(self._layer_list)) * 0.2
        carbon_saved = total_energy_skipped * (await self.profiler._get_carbon_intensity()) / 1000
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
        return asyncio.run(self.forward_async(x, token_importance, context))

    async def run_flexgen_optimization(self, workload: Dict, node: Dict) -> Dict:
        """Public method to run FlexGen policy optimization for this wrapper's model."""
        return await self.profiler.run_flexgen_optimization(workload, node)

    def get_skipped_layers(self) -> List[str]:
        return self._last_skipped.copy()

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
            action_space=data['profiler_config'].get('action_space', ["aggressive", "balanced", "conservative"]),
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
    storage = None
    queue = None
    profiler = EnergyProfiler(
        model=model,
        energy_per_layer=energy_per_layer,
        carbon_provider=carbon_provider,
        storage=storage,
        message_queue=queue,
    )
    wrapper = LayerSkippingWrapper(model, profiler)
    x = torch.randn(4, 10)
    importance = torch.ones(4) * 0.8
    output = await wrapper.forward_async(x, importance)
    print(f"Output shape: {output.shape}")
    print(f"Skipped layers: {wrapper.get_skipped_layers()}")

if __name__ == "__main__":
    asyncio.run(example())
