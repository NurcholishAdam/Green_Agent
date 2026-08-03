# src/enhancements/adapters.py
"""
LoRA adapter support for experts to enable dynamic specialization.
Each expert can have multiple adapter weights for different energy modes.
"""

import torch
import torch.nn as nn
from typing import Dict, Optional

class LoRAAdapter(nn.Module):
    """Low‑Rank Adaptation (LoRA) layer for a given module."""
    def __init__(self, in_features: int, out_features: int, rank: int = 8):
        super().__init__()
        self.lora_A = nn.Linear(in_features, rank, bias=False)
        self.lora_B = nn.Linear(rank, out_features, bias=False)
        # Initialize with small random values
        nn.init.kaiming_uniform_(self.lora_A.weight, a=0.01)
        nn.init.zeros_(self.lora_B.weight)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.lora_B(self.lora_A(x))


class AdapterManager:
    """
    Manages multiple adapters per expert.
    Each expert can have adapters for energy_modes: 'eco', 'balanced', 'performance'.
    """
    def __init__(self, expert: nn.Module, adapter_rank: int = 8):
        self.expert = expert
        self.adapters: Dict[str, Dict[str, LoRAAdapter]] = {}
        self._register_adapters(adapter_rank)

    def _register_adapters(self, rank: int):
        """Create LoRA adapters for all linear layers in the expert."""
        for name, module in self.expert.named_modules():
            if isinstance(module, nn.Linear):
                for mode in ['eco', 'balanced', 'performance']:
                    if mode not in self.adapters:
                        self.adapters[mode] = {}
                    self.adapters[mode][name] = LoRAAdapter(
                        module.in_features, module.out_features, rank
                    )

    def activate_mode(self, mode: str):
        """Add the LoRA output of the selected mode to the original expert layers."""
        if mode not in self.adapters:
            raise ValueError(f"Unknown mode {mode}")
        for name, adapter in self.adapters[mode].items():
            # Find the original module by name and add its output
            # This is a simplified approach; in practice we'd use forward hooks.
            pass  # Implementation omitted for brevity; use hooks.

    def forward_with_mode(self, x: torch.Tensor, mode: str) -> torch.Tensor:
        """Run expert forward pass with the selected mode's adapter."""
        # In a full implementation, we'd apply adapters via hooks.
        # For demonstration, we simulate.
        output = self.expert(x)
        # Add adapter contribution
        for name, adapter in self.adapters[mode].items():
            # Get the module by name and apply adapter
            # This is pseudo-code; real implementation uses hooks.
            pass
        return output
