"""
Per‑layer energy profiling and layer‑skipping for energy‑efficient inference.
"""

import torch
import torch.nn as nn
from typing import List, Tuple, Optional
import numpy as np

class EnergyProfiler:
    """
    Tracks energy per layer and provides layer‑skipping decisions.
    """
    def __init__(self, model: nn.Module, energy_per_layer: List[float]):
        self.model = model
        self.energy_per_layer = energy_per_layer  # pre‑computed or measured
        self.layer_names = [name for name, _ in model.named_modules() if isinstance(_, nn.Linear)]

    def estimate_energy_for_token(self, token_importance: float, layer_idx: int) -> float:
        """Adjust energy based on token importance and current carbon intensity."""
        # Placeholder: use current carbon intensity from global state
        carbon_intensity = 400  # or fetch from CarbonIntensityManager
        base_energy = self.energy_per_layer[layer_idx]
        return base_energy * (1 + (carbon_intensity / 400 - 1) * 0.2)

    def should_skip_layer(
        self,
        layer_idx: int,
        token_importance: float,
        current_energy_budget: float,
    ) -> bool:
        """
        Decide whether to skip a layer based on importance and budget.
        """
        estimated = self.estimate_energy_for_token(token_importance, layer_idx)
        # If budget is low and token importance is low, skip.
        if current_energy_budget < 0.3 and token_importance < 0.5:
            return True
        return False


class LayerSkippingWrapper(nn.Module):
    """
    Wraps a model to allow selective layer skipping based on EnergyProfiler.
    """
    def __init__(self, model: nn.Module, profiler: EnergyProfiler):
        super().__init__()
        self.model = model
        self.profiler = profiler
        self.energy_budget = 1.0  # 0-1, set externally

    def forward(self, x: torch.Tensor, token_importance: Optional[torch.Tensor] = None):
        if token_importance is None:
            token_importance = torch.ones(x.size(0)) * 0.5
        # Iterate layers and decide to skip
        layer_idx = 0
        for name, module in self.model.named_children():
            if isinstance(module, nn.Linear):
                if self.profiler.should_skip_layer(
                    layer_idx,
                    token_importance.mean().item(),
                    self.energy_budget
                ):
                    # Skip this layer: pass input through unchanged
                    continue
                layer_idx += 1
            x = module(x)
        return x
