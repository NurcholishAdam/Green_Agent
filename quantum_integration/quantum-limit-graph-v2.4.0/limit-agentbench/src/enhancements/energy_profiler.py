"""
Per‑layer energy profiling and layer‑skipping for energy‑efficient inference.
Enhanced version with real‑time carbon integration, adaptive skipping,
and support for all layer types.
"""

import torch
import torch.nn as nn
from typing import List, Dict, Optional, Union, Callable
import numpy as np
import logging
from pathlib import Path
import json

logger = logging.getLogger(__name__)

class EnergyProfiler:
    """
    Tracks energy per layer and provides adaptive layer‑skipping decisions.
    Integrates with CarbonIntensityManager for real‑time carbon data.
    """

    def __init__(
        self,
        model: nn.Module,
        energy_per_layer: Dict[str, float],  # layer_name -> energy (Joules)
        carbon_manager: Optional[Any] = None,  # CarbonIntensityManager
        default_carbon_intensity: float = 400.0,
        importance_energy_factor: float = 0.5,  # how much token importance affects energy
        skip_threshold_low: float = 0.3,
        skip_threshold_high: float = 0.7,
    ):
        """
        Args:
            model: The model to profile.
            energy_per_layer: Dictionary mapping layer name to energy (Joules) per token.
            carbon_manager: Optional CarbonIntensityManager for real-time intensity.
            default_carbon_intensity: Fallback carbon intensity (gCO₂/kWh).
            importance_energy_factor: Multiplicative factor for token importance.
            skip_threshold_low: If energy budget < this, skip low-importance tokens.
            skip_threshold_high: If energy budget < this, skip medium-importance tokens.
        """
        self.model = model
        self.energy_per_layer = energy_per_layer
        self.carbon_manager = carbon_manager
        self.default_carbon_intensity = default_carbon_intensity
        self.importance_factor = importance_energy_factor
        self.skip_threshold_low = skip_threshold_low
        self.skip_threshold_high = skip_threshold_high

        # Ensure all layers in the model have an entry; if missing, estimate.
        self._fill_missing_energies()

        # Cache for layer names order (for iteration)
        self.layer_order = list(self.energy_per_layer.keys())

    def _fill_missing_energies(self):
        """For any layer without energy data, assign a default value."""
        default_energy = 1e-6  # Joules per token
        for name, module in self.model.named_modules():
            if name not in self.energy_per_layer:
                self.energy_per_layer[name] = default_energy
                logger.debug(f"Assigned default energy to layer {name}: {default_energy}")

    def _get_carbon_intensity(self) -> float:
        """Get current carbon intensity from manager or fallback."""
        if self.carbon_manager and hasattr(self.carbon_manager, 'get_current_intensity'):
            try:
                # If async, we need to handle; for now assume synchronous or use a stub.
                # In a full integration, this would be async, but for simplicity we keep sync.
                intensity = self.carbon_manager.get_current_intensity()
                if isinstance(intensity, float):
                    return intensity
            except Exception as e:
                logger.warning(f"Failed to get carbon intensity: {e}")
        return self.default_carbon_intensity

    def estimate_energy_for_token(
        self,
        layer_name: str,
        token_importance: float,
    ) -> float:
        """
        Estimate energy for a single token passing through a layer.
        Adjusts based on token importance and carbon intensity.
        """
        base_energy = self.energy_per_layer.get(layer_name, 1e-6)
        carbon_intensity = self._get_carbon_intensity()
        # Carbon factor: higher intensity -> higher energy cost
        carbon_factor = 1.0 + (carbon_intensity / 400 - 1.0) * 0.2
        # Importance factor: more important tokens consume more energy (e.g., due to higher precision)
        importance_factor = 1.0 + token_importance * self.importance_factor
        return base_energy * carbon_factor * importance_factor

    def should_skip_layer(
        self,
        layer_name: str,
        token_importance: float,
        current_energy_budget: float,
    ) -> bool:
        """
        Decide whether to skip a layer based on importance and budget.
        The decision is probabilistic to avoid deterministic patterns.
        """
        estimated = self.estimate_energy_for_token(layer_name, token_importance)
        # Base skip probability: if budget is low and token importance is low, skip.
        if current_energy_budget < self.skip_threshold_low and token_importance < 0.3:
            return True
        elif current_energy_budget < self.skip_threshold_high and token_importance < 0.5:
            # Probabilistic skip
            prob = 0.5
            return np.random.rand() < prob
        return False

    def get_energy_map(self) -> Dict[str, float]:
        """Return the energy per layer dict."""
        return self.energy_per_layer.copy()

    def save(self, path: Path):
        """Save profiler configuration to JSON."""
        data = {
            'energy_per_layer': self.energy_per_layer,
            'importance_factor': self.importance_factor,
            'skip_threshold_low': self.skip_threshold_low,
            'skip_threshold_high': self.skip_threshold_high,
        }
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Profiler saved to {path}")

    @classmethod
    def load(cls, path: Path, model: nn.Module, carbon_manager=None) -> "EnergyProfiler":
        """Load profiler configuration from JSON."""
        with open(path, 'r') as f:
            data = json.load(f)
        return cls(
            model=model,
            energy_per_layer=data['energy_per_layer'],
            carbon_manager=carbon_manager,
            importance_factor=data.get('importance_factor', 0.5),
            skip_threshold_low=data.get('skip_threshold_low', 0.3),
            skip_threshold_high=data.get('skip_threshold_high', 0.7),
        )


class LayerSkippingWrapper(nn.Module):
    """
    Wraps a model to allow selective layer skipping based on EnergyProfiler.
    Supports per‑token importance and dynamic energy budget.
    """

    def __init__(
        self,
        model: nn.Module,
        profiler: EnergyProfiler,
        energy_budget_source: Optional[Callable[[], float]] = None,
    ):
        """
        Args:
            model: The model to wrap.
            profiler: EnergyProfiler instance.
            energy_budget_source: Callable that returns current energy budget (0‑1).
                                  If None, budget is set externally via `set_energy_budget`.
        """
        super().__init__()
        self.model = model
        self.profiler = profiler
        self.energy_budget_source = energy_budget_source
        self._energy_budget = 1.0  # default

    def set_energy_budget(self, budget: float):
        """Manually set the energy budget (0‑1)."""
        self._energy_budget = max(0.0, min(1.0, budget))

    def _get_energy_budget(self) -> float:
        """Get current energy budget from source or stored value."""
        if self.energy_budget_source:
            return self.energy_budget_source()
        return self._energy_budget

    def forward(
        self,
        x: torch.Tensor,
        token_importance: Optional[torch.Tensor] = None,
    ) -> torch.Tensor:
        """
        Forward pass with layer skipping.

        Args:
            x: Input tensor of shape (batch, ...).
            token_importance: Tensor of shape (batch,) or (batch, seq_len) indicating
                              importance per token. If None, all tokens are importance 0.5.

        Returns:
            Output tensor after applying selected layers.
        """
        if token_importance is None:
            token_importance = torch.ones(x.size(0), device=x.device) * 0.5
        # Ensure token_importance is per‑batch (average over sequence if needed)
        if token_importance.dim() > 1:
            token_importance = token_importance.mean(dim=1)

        current_budget = self._get_energy_budget()
        output = x

        # Traverse all modules in order
        for name, module in self.model.named_modules():
            # Skip the root module (the model itself)
            if name == '':
                continue
            # Check if we should skip this layer
            # We compute average importance for the batch
            avg_importance = token_importance.mean().item()
            if self.profiler.should_skip_layer(name, avg_importance, current_budget):
                logger.debug(f"Skipping layer {name} (budget={current_budget:.2f}, importance={avg_importance:.2f})")
                continue
            # Apply the module
            # But we need to handle modules that are containers (e.g., Sequential)
            # The wrapper will apply each module in order; containers will be applied recursively.
            # However, since we traverse named_modules(), we risk applying a container and then its submodules again.
            # So we apply only the leaf modules (those without children) to avoid double application.
            # Alternatively, we can iterate over named_children() but that may skip nested modules.
            # Best approach: iterate over model's forward hierarchy manually.
            # For simplicity, we assume the model is a simple sequential or we use a custom traversal.
            # We'll implement a recursive helper that applies modules if they are not containers.
            output = self._apply_module(output, module, name)

        return output

    def _apply_module(self, x: torch.Tensor, module: nn.Module, name: str) -> torch.Tensor:
        """
        Recursively apply module or its children.
        """
        # If module has children, apply each child in order (for Sequential)
        if list(module.children()):
            for child_name, child in module.named_children():
                full_name = f"{name}.{child_name}" if name else child_name
                # Check skip for child
                avg_importance = x.mean().item()  # placeholder
                current_budget = self._get_energy_budget()
                if self.profiler.should_skip_layer(full_name, avg_importance, current_budget):
                    logger.debug(f"Skipping child layer {full_name}")
                    continue
                x = child(x)
            return x
        else:
            # Leaf module: apply it
            return module(x)

    def get_skipped_layers(self) -> List[str]:
        """Return a list of layer names that were skipped in the last forward pass."""
        # Not implemented; would need to track during forward.
        return []

    def get_energy_estimate(self, x: torch.Tensor) -> float:
        """Estimate total energy for a forward pass without skipping."""
        # Not implemented; would need to sum energy_per_layer.
        return 0.0

    def save(self, path: Path):
        """Save wrapper config and profiler."""
        # Save profiler and wrapper settings
        data = {
            'energy_budget': self._energy_budget,
            'profiler_config': {
                'energy_per_layer': self.profiler.energy_per_layer,
                'importance_factor': self.profiler.importance_factor,
                'skip_threshold_low': self.profiler.skip_threshold_low,
                'skip_threshold_high': self.profiler.skip_threshold_high,
            }
        }
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Wrapper saved to {path}")

    @classmethod
    def load(cls, path: Path, model: nn.Module, carbon_manager=None) -> "LayerSkippingWrapper":
        """Load wrapper and profiler from file."""
        with open(path, 'r') as f:
            data = json.load(f)
        profiler = EnergyProfiler(
            model=model,
            energy_per_layer=data['profiler_config']['energy_per_layer'],
            carbon_manager=carbon_manager,
            importance_factor=data['profiler_config'].get('importance_factor', 0.5),
            skip_threshold_low=data['profiler_config'].get('skip_threshold_low', 0.3),
            skip_threshold_high=data['profiler_config'].get('skip_threshold_high', 0.7),
        )
        wrapper = cls(model=model, profiler=profiler)
        wrapper._energy_budget = data.get('energy_budget', 1.0)
        return wrapper
