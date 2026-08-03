# src/enhancements/adapters.py
"""
LoRA adapter support for experts to enable dynamic specialization.
Each expert can have multiple adapter weights for different energy modes.
Enhanced version with full hook-based injection, memory optimization,
and serialization support.
"""

import torch
import torch.nn as nn
import logging
from typing import Dict, Optional, List, Any, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)

class LoRAAdapter(nn.Module):
    """
    Low‑Rank Adaptation (LoRA) layer for a given module.
    Supports a scaling factor that can be adjusted per mode.
    """
    def __init__(self, in_features: int, out_features: int, rank: int = 8):
        super().__init__()
        self.lora_A = nn.Linear(in_features, rank, bias=False)
        self.lora_B = nn.Linear(rank, out_features, bias=False)
        # Initialize with small random values
        nn.init.kaiming_uniform_(self.lora_A.weight, a=0.01)
        nn.init.zeros_(self.lora_B.weight)

    def forward(self, x: torch.Tensor, scale: float = 1.0) -> torch.Tensor:
        """Forward pass with scaling factor."""
        return self.lora_B(self.lora_A(x)) * scale


class AdapterManager:
    """
    Manages adapters for an expert model.
    Uses a single adapter per layer and mode-specific scaling factors.
    Applies adapters via forward hooks.
    """
    def __init__(
        self,
        expert: nn.Module,
        adapter_rank: int = 8,
        mode_scales: Optional[Dict[str, float]] = None,
    ):
        """
        Args:
            expert: The expert model to attach adapters to.
            adapter_rank: Rank for LoRA adapters.
            mode_scales: Scaling factors for each energy mode.
                          Defaults to {'eco': 0.1, 'balanced': 0.5, 'performance': 1.0}.
        """
        self.expert = expert
        self.adapter_rank = adapter_rank
        self.mode_scales = mode_scales or {
            'eco': 0.1,
            'balanced': 0.5,
            'performance': 1.0,
        }
        self._current_mode: Optional[str] = None
        self._adapters: Dict[str, LoRAAdapter] = {}
        self._hooks: Dict[str, torch.utils.hooks.RemovableHandle] = {}
        self._register_adapters()

    def _get_module_sizes(self, module: nn.Module) -> Tuple[int, int]:
        """Return (in_features, out_features) for supported module types."""
        if isinstance(module, nn.Linear):
            return module.in_features, module.out_features
        elif isinstance(module, nn.Conv2d):
            return module.in_channels, module.out_channels
        else:
            raise ValueError(f"Unsupported module type: {type(module)}")

    def _register_adapters(self):
        """
        Create a single LoRA adapter for each supported layer in the expert.
        """
        for name, module in self.expert.named_modules():
            if isinstance(module, (nn.Linear, nn.Conv2d)):
                in_dim, out_dim = self._get_module_sizes(module)
                adapter = LoRAAdapter(in_dim, out_dim, rank=self.adapter_rank)
                self._adapters[name] = adapter
                logger.debug(f"Registered adapter for layer: {name}")

    def _clear_hooks(self):
        """Remove all registered forward hooks."""
        for hook in self._hooks.values():
            hook.remove()
        self._hooks.clear()

    def _attach_hooks(self, mode: str):
        """
        Attach forward hooks for all adapters with the given mode's scaling factor.
        """
        scale = self.mode_scales.get(mode, 1.0)
        for name, adapter in self._adapters.items():
            module = self.expert.get_submodule(name)
            if module is None:
                logger.warning(f"Module {name} not found in expert")
                continue
            # Define hook that adds LoRA output (scaled)
            def make_hook(adapter, scale):
                def hook(module, input, output):
                    # input[0] is the input tensor to the module
                    return output + adapter(input[0], scale=scale)
                return hook
            hook = module.register_forward_hook(make_hook(adapter, scale))
            self._hooks[name] = hook
        logger.info(f"Attached hooks for mode '{mode}' (scale={scale})")

    def set_mode(self, mode: str):
        """
        Switch to a new energy mode, updating the scaling factor for all adapters.
        """
        if mode not in self.mode_scales:
            raise ValueError(f"Unknown mode: {mode}. Available: {list(self.mode_scales.keys())}")
        if mode == self._current_mode:
            return
        self._clear_hooks()
        self._attach_hooks(mode)
        self._current_mode = mode
        logger.info(f"Adapter mode switched to '{mode}'")

    def forward_with_mode(self, x: torch.Tensor, mode: str) -> torch.Tensor:
        """
        Run expert forward pass with the given mode's adapters.
        This method sets the mode temporarily and calls the expert.
        """
        old_mode = self._current_mode
        self.set_mode(mode)
        try:
            return self.expert(x)
        finally:
            # Restore previous mode if any
            if old_mode is not None:
                self.set_mode(old_mode)

    def save(self, path: Path):
        """
        Save adapter weights and mode scales to disk.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        checkpoint = {
            'adapters': {name: adapter.state_dict() for name, adapter in self._adapters.items()},
            'mode_scales': self.mode_scales,
            'adapter_rank': self.adapter_rank,
        }
        torch.save(checkpoint, path)
        logger.info(f"Adapters saved to {path}")

    @classmethod
    def load(cls, path: Path, expert: nn.Module) -> "AdapterManager":
        """
        Load adapter weights from disk and attach them to the expert.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Adapter checkpoint not found: {path}")
        checkpoint = torch.load(path, map_location='cpu')
        rank = checkpoint.get('adapter_rank', 8)
        manager = cls(expert, adapter_rank=rank)
        # Load adapter weights
        for name, state in checkpoint['adapters'].items():
            if name in manager._adapters:
                manager._adapters[name].load_state_dict(state)
            else:
                logger.warning(f"Adapter for layer {name} not found in current expert; skipping")
        # Restore mode scales
        manager.mode_scales = checkpoint.get('mode_scales', manager.mode_scales)
        logger.info(f"Adapters loaded from {path}")
        return manager

    def get_mode(self) -> Optional[str]:
        """Return the currently active mode."""
        return self._current_mode

    def get_available_modes(self) -> List[str]:
        """Return a list of available energy modes."""
        return list(self.mode_scales.keys())

    def __del__(self):
        """Clean up hooks when object is destroyed."""
        self._clear_hooks()
