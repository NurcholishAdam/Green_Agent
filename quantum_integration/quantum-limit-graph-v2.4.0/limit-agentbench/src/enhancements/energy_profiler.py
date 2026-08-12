"""
Per‑layer energy profiling and layer‑skipping for energy‑efficient inference.
Enhanced version with real‑time carbon integration, adaptive skipping,
and support for all layer types.
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
from collections import OrderedDict
import uuid

logger = logging.getLogger(__name__)

# ============================================================================
# Custom Exceptions
# ============================================================================
class EnergyProfilerError(Exception):
    """Base exception for energy profiler."""
    pass

class CarbonFetchError(EnergyProfilerError):
    """Failed to fetch carbon intensity."""
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
        self._lock = asyncio.Lock() if asyncio.iscoroutinefunction else None

    async def call(self, func: Callable, *args, **kwargs):
        if asyncio.iscoroutinefunction(func):
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
        else:
            # Sync version
            if self._state == CircuitBreakerState.OPEN:
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitBreakerState.HALF_OPEN
                    self._failure_count = 0
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
            try:
                result = func(*args, **kwargs)
                if self._state == CircuitBreakerState.HALF_OPEN:
                    self._state = CircuitBreakerState.CLOSED
                self._failure_count = 0
                return result
            except Exception as e:
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

class CircuitBreakerOpenError(Exception):
    pass

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
        """Fetch new intensity and cache it."""
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
        """Return cached intensity, update if stale."""
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
# Energy Profiler (Enhanced)
# ============================================================================
class EnergyProfiler:
    """
    Tracks energy per layer and provides adaptive layer‑skipping decisions.
    Integrates with CarbonIntensityProvider for real‑time carbon data.
    """

    def __init__(
        self,
        model: nn.Module,
        energy_per_layer: Dict[str, float],  # layer_name -> energy (Joules)
        carbon_provider: Optional[CarbonIntensityProvider] = None,
        default_carbon_intensity: float = 400.0,
        importance_energy_factor: float = 0.5,
        skip_threshold_low: float = 0.3,
        skip_threshold_high: float = 0.7,
        skipping_strategy: str = "probabilistic",  # "threshold", "probabilistic", "adaptive"
        adaptive_learning_rate: float = 0.1,
    ):
        """
        Args:
            model: The model to profile.
            energy_per_layer: Dictionary mapping layer name to energy (Joules) per token.
            carbon_provider: Optional CarbonIntensityProvider.
            default_carbon_intensity: Fallback carbon intensity (gCO₂/kWh).
            importance_energy_factor: Multiplicative factor for token importance.
            skip_threshold_low: Energy budget below which low‑importance tokens are skipped.
            skip_threshold_high: Energy budget below which medium‑importance tokens are skipped.
            skipping_strategy: Strategy for skipping decisions.
            adaptive_learning_rate: Learning rate for adaptive strategy.
        """
        self.model = model
        self.energy_per_layer = energy_per_layer
        self.carbon_provider = carbon_provider
        self.default_carbon_intensity = default_carbon_intensity
        self.importance_factor = importance_energy_factor
        self.skip_threshold_low = skip_threshold_low
        self.skip_threshold_high = skip_threshold_high
        self.skipping_strategy = skipping_strategy
        self.adaptive_learning_rate = adaptive_learning_rate

        # Fill missing energies
        self._fill_missing_energies()

        # Cache for layer order
        self.layer_order = list(self.energy_per_layer.keys())

        # Adaptive skipping history
        self._skipping_history: Dict[str, List[bool]] = defaultdict(list)
        self._performance_history: List[float] = []

    def _fill_missing_energies(self):
        default_energy = 1e-6
        for name, module in self.model.named_modules():
            if name not in self.energy_per_layer:
                self.energy_per_layer[name] = default_energy
                logger.debug(f"Assigned default energy to layer {name}: {default_energy}")

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
        """Estimate energy for a single token."""
        base_energy = self.energy_per_layer.get(layer_name, 1e-6)
        carbon_intensity = await self._get_carbon_intensity()
        carbon_factor = 1.0 + (carbon_intensity / 400 - 1.0) * 0.2
        importance_factor = 1.0 + token_importance * self.importance_factor
        return base_energy * carbon_factor * importance_factor

    async def should_skip_layer(
        self,
        layer_name: str,
        token_importance: float,
        current_energy_budget: float,
    ) -> bool:
        """
        Determine whether to skip a layer based on chosen strategy.
        """
        if self.skipping_strategy == "threshold":
            return self._should_skip_threshold(layer_name, token_importance, current_energy_budget)
        elif self.skipping_strategy == "probabilistic":
            return self._should_skip_probabilistic(layer_name, token_importance, current_energy_budget)
        elif self.skipping_strategy == "adaptive":
            return await self._should_skip_adaptive(layer_name, token_importance, current_energy_budget)
        else:
            raise ValueError(f"Unknown skipping strategy: {self.skipping_strategy}")

    def _should_skip_threshold(
        self,
        layer_name: str,
        token_importance: float,
        budget: float,
    ) -> bool:
        if budget < self.skip_threshold_low and token_importance < 0.3:
            return True
        if budget < self.skip_threshold_high and token_importance < 0.5:
            return True
        return False

    def _should_skip_probabilistic(
        self,
        layer_name: str,
        token_importance: float,
        budget: float,
    ) -> bool:
        # Probability scales with budget and importance
        prob = max(0.0, 1.0 - budget / 0.5) * (1.0 - token_importance)
        return np.random.rand() < prob

    async def _should_skip_adaptive(
        self,
        layer_name: str,
        token_importance: float,
        budget: float,
    ) -> bool:
        # Use historical performance to adjust skipping
        # For simplicity, we use a heuristic: if recent performance is good, skip more.
        if len(self._performance_history) < 10:
            return budget < 0.5 and token_importance < 0.5
        avg_perf = np.mean(self._performance_history[-10:])
        threshold = 0.4 + (avg_perf - 0.5) * self.adaptive_learning_rate
        return budget < threshold and token_importance < 0.5

    def record_skipping_outcome(self, layer_name: str, skipped: bool, performance_delta: float = 0.0):
        self._skipping_history[layer_name].append(skipped)
        if performance_delta != 0:
            self._performance_history.append(performance_delta)

    def get_energy_map(self) -> Dict[str, float]:
        return self.energy_per_layer.copy()

    async def estimate_total_energy(self, input_shape: tuple, token_importance: float = 0.5) -> float:
        """Estimate total energy for a forward pass through all layers."""
        total = 0.0
        for layer_name in self.layer_order:
            total += await self.estimate_energy_for_token(layer_name, token_importance)
        return total

    def save(self, path: Path):
        data = {
            'energy_per_layer': self.energy_per_layer,
            'importance_factor': self.importance_factor,
            'skip_threshold_low': self.skip_threshold_low,
            'skip_threshold_high': self.skip_threshold_high,
            'skipping_strategy': self.skipping_strategy,
            'adaptive_learning_rate': self.adaptive_learning_rate,
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
            importance_factor=data.get('importance_factor', 0.5),
            skip_threshold_low=data.get('skip_threshold_low', 0.3),
            skip_threshold_high=data.get('skip_threshold_high', 0.7),
            skipping_strategy=data.get('skipping_strategy', 'probabilistic'),
            adaptive_learning_rate=data.get('adaptive_learning_rate', 0.1),
        )

# ============================================================================
# Layer Skipping Wrapper (Enhanced)
# ============================================================================
class LayerSkippingWrapper(nn.Module):
    """
    Wraps a model to allow selective layer skipping based on EnergyProfiler.
    Supports per‑token importance and dynamic energy budget.
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

        # Cache for layer traversal to avoid repeated named_modules calls.
        self._layer_list = self._build_layer_list(model)

        # Track skipped layers per forward pass
        self._last_skipped: List[str] = []

    def _build_layer_list(self, module: nn.Module, prefix: str = "") -> List[Tuple[str, nn.Module]]:
        """Build a flat list of all leaf modules with their names."""
        layers = []
        for name, child in module.named_children():
            full_name = f"{prefix}.{name}" if prefix else name
            if list(child.children()):
                # Recurse
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
    ) -> torch.Tensor:
        """
        Async forward pass with layer skipping.
        """
        if token_importance is None:
            token_importance = torch.ones(x.size(0), device=x.device) * 0.5

        # If token_importance is per‑sequence, we need per‑token for each layer.
        # For simplicity, we'll use per‑batch average if importance is per‑token.
        if token_importance.dim() > 1:
            token_importance = token_importance.mean(dim=1)  # (batch,)

        current_budget = self._get_energy_budget()
        output = x
        skipped = []

        for layer_name, layer_module in self._layer_list:
            # Compute average token importance for this batch
            avg_importance = token_importance.mean().item()
            if await self.profiler.should_skip_layer(layer_name, avg_importance, current_budget):
                logger.debug(f"Skipping layer {layer_name} (budget={current_budget:.2f}, importance={avg_importance:.2f})")
                skipped.append(layer_name)
                continue
            # Apply layer
            output = layer_module(output)

        self._last_skipped = skipped
        # Optionally record skipping outcome (for adaptive strategy)
        # For now, we don't have performance delta; skip.
        return output

    def forward(
        self,
        x: torch.Tensor,
        token_importance: Optional[torch.Tensor] = None,
    ) -> torch.Tensor:
        """
        Synchronous forward pass (for compatibility).
        Uses async forward internally.
        """
        return asyncio.run(self.forward_async(x, token_importance))

    def get_skipped_layers(self) -> List[str]:
        return self._last_skipped.copy()

    async def estimate_energy(self, x: torch.Tensor, token_importance: Optional[torch.Tensor] = None) -> float:
        """
        Estimate total energy for a forward pass (without skipping).
        """
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
                'importance_factor': self.profiler.importance_factor,
                'skip_threshold_low': self.profiler.skip_threshold_low,
                'skip_threshold_high': self.profiler.skip_threshold_high,
                'skipping_strategy': self.profiler.skipping_strategy,
                'adaptive_learning_rate': self.profiler.adaptive_learning_rate,
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
            importance_factor=data['profiler_config'].get('importance_factor', 0.5),
            skip_threshold_low=data['profiler_config'].get('skip_threshold_low', 0.3),
            skip_threshold_high=data['profiler_config'].get('skip_threshold_high', 0.7),
            skipping_strategy=data['profiler_config'].get('skipping_strategy', 'probabilistic'),
            adaptive_learning_rate=data['profiler_config'].get('adaptive_learning_rate', 0.1),
        )
        wrapper = cls(model=model, profiler=profiler)
        wrapper._energy_budget = data.get('energy_budget', 1.0)
        return wrapper

# ============================================================================
# Example usage
# ============================================================================
async def example():
    # Create a dummy model
    model = nn.Sequential(
        nn.Linear(10, 20),
        nn.ReLU(),
        nn.Linear(20, 20),
        nn.ReLU(),
        nn.Linear(20, 1)
    )
    # Define per-layer energies (Joules per token)
    energy_per_layer = {
        '0': 1e-6,
        '1': 0.5e-6,
        '2': 1.5e-6,
        '3': 0.5e-6,
        '4': 2e-6,
    }
    # Carbon provider (real API key would be used)
    carbon_provider = CarbonIntensityManager(api_key=None)
    profiler = EnergyProfiler(
        model=model,
        energy_per_layer=energy_per_layer,
        carbon_provider=carbon_provider,
    )
    wrapper = LayerSkippingWrapper(model, profiler)
    x = torch.randn(4, 10)
    importance = torch.ones(4) * 0.8  # high importance
    output = await wrapper.forward_async(x, importance)
    print(f"Output shape: {output.shape}")
    print(f"Skipped layers: {wrapper.get_skipped_layers()}")

if __name__ == "__main__":
    asyncio.run(example())
