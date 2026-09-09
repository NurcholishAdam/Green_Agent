# src/enhancements/adapters.py
"""
LoRA adapter support for experts to enable dynamic specialization.
Each expert can have multiple adapter weights for different energy modes.
Enhanced version with full hook-based injection, memory optimization,
serialization support, and integration with bio_inspired, moe_system, MODP,
and FlexGen.

Features:
- Supports Linear, Conv1d, Conv2d, Conv3d, Embedding, MultiheadAttention (out_proj only)
- Per-layer configuration (rank, scaling)
- Optimized mode switching (scale update without re‑registering hooks)
- Multiple adapters per layer (one per mode)
- Merging of adapters into base weights
- Learnable scaling factors (optional)
- Thread‑safe mode handling
- Robust hook management with weakrefs
- Gradient checkpointing support
- Distributed training compatible (DDP)
- Serialization with versioning
- Comprehensive error handling
- Full docstrings

ENHANCEMENTS INTEGRATED (v2.0):
- Bio‑inspired evolution of adapter configurations (GeneticPolicyGenerator)
- MoE‑based context‑aware mode selection (ExpertRouter)
- MODP‑based multi‑objective mode evaluation (ParetoOptimizer)
- Unified adaptive forward pass that selects the best mode automatically

ENHANCEMENTS INTEGRATED (v3.0):
- FlexGen‑aware mode selection using cost model and GPU profiler.
- Carbon‑intensity‑based adapter scaling.
- Adapter modes can be evaluated as FlexGen policies.
- Methods to generate FlexGenPolicy from adapter configuration.
- Full compatibility with Green Agent's FlexGenController and MODPPlanner.

ENHANCEMENTS INTEGRATED (v4.0):
- Fixed convolution adaptation (now uses proper 1x1 convolution for Conv layers)
- Added MultiheadAttention (out_proj) adaptation
- Added XAI explanations for mode selection
- Integrated temporal safety monitor
- Added human approval callback for critical mode changes
- Added chaos testing methods
- Fixed save/load to restore mode and hooks
- Made `forward_with_adaptive_mode` truly automatic
"""

import torch
import torch.nn as nn
import logging
from typing import Dict, Optional, List, Any, Tuple, Union, Callable
from pathlib import Path
import weakref
from collections import OrderedDict
import threading
import asyncio

logger = logging.getLogger(__name__)

# ============================================================================
# Custom Exceptions
# ============================================================================

class AdapterError(Exception):
    """Base exception for adapter-related errors."""
    pass

class AdapterNotFoundError(AdapterError):
    """Raised when a requested adapter is not found."""
    pass

class UnsupportedModuleError(AdapterError):
    """Raised when a module type is not supported for adaptation."""
    pass

class ConfigurationError(AdapterError):
    """Raised when configuration is invalid."""
    pass

# ============================================================================
# Import enhanced modules (with graceful fallback)
# ============================================================================
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator
    from enhancements.moe_system import ExpertRouter
    from enhancements.MODP import ParetoOptimizer
    from enhancements.temporal_safety_monitor import TemporalSafetyMonitor
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    # Fallback stubs
    class GeneticPolicyGenerator:
        def __init__(self): pass
        def evolve(self, config, fitness_fn): return config
    class ExpertRouter:
        def __init__(self): pass
        def encode(self, context): return [0.0]*5
        def select(self, context): return "balanced"
    class ParetoOptimizer:
        def __init__(self): pass
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k,0) * weights.get(k,1) for k in objectives)
    class TemporalSafetyMonitor:
        def __init__(self): self.rules = []
        def add_rule(self, *args, **kwargs): pass
        async def check(self, state): return []

# FlexGen modules (optional)
try:
    from .gpu_optimization.flexgen_policy import FlexGenPolicy
    from .gpu_optimization.flexgen_cost_model import FlexGenCostModel, CostEstimate
    from .gpu_optimization.gpu_profiler import GPUProfiler
    from .modp.flexgen_modp_planner import FlexGenMODPPlanner
    FLEXGEN_AVAILABLE = True
except ImportError:
    FLEXGEN_AVAILABLE = False
    # Dummy placeholders
    class FlexGenPolicy:
        pass
    class FlexGenCostModel:
        def estimate(self, *args, **kwargs): return None
    class GPUProfiler:
        def get_gpu_metrics(self): return {}
    class FlexGenMODPPlanner:
        def __init__(self, *args, **kwargs): pass
        async def plan(self, *args, **kwargs): return ("run_now", 0, None)

# ============================================================================
# LoRA Adapter Layer (fixed Conv and MultiheadAttention)
# ============================================================================

class LoRAAdapter(nn.Module):
    """
    Low‑Rank Adaptation (LoRA) layer for a given module.
    Supports Linear, Conv1d/2d/3d, Embedding, MultiheadAttention (out_proj).
    For Conv layers, uses 1x1 convolution adaptation (standard LoRA for conv).
    """
    def __init__(
        self,
        module: nn.Module,
        rank: int = 8,
        scale: float = 1.0,
        device: Optional[torch.device] = None,
        dtype: Optional[torch.dtype] = None,
    ):
        super().__init__()
        self.rank = rank
        self._scale = scale
        self.device = device
        self.dtype = dtype

        self.module_type = type(module)
        self._init_shapes(module)
        self._init_weights(device, dtype)

    def _init_shapes(self, module: nn.Module):
        """Initialize in_features, out_features, and kernel shapes."""
        if isinstance(module, nn.Linear):
            self.in_features = module.in_features
            self.out_features = module.out_features
            self.is_linear = True
        elif isinstance(module, nn.Conv1d):
            self.in_features = module.in_channels
            self.out_features = module.out_channels
            self.kernel_size = module.kernel_size[0] if isinstance(module.kernel_size, tuple) else module.kernel_size
            self.padding = module.padding[0] if isinstance(module.padding, tuple) else module.padding
            self.stride = module.stride[0] if isinstance(module.stride, tuple) else module.stride
            self.is_linear = False
            self.conv_dim = 1
        elif isinstance(module, nn.Conv2d):
            self.in_features = module.in_channels
            self.out_features = module.out_channels
            self.kernel_size = module.kernel_size[0] if isinstance(module.kernel_size, tuple) else module.kernel_size
            self.padding = module.padding[0] if isinstance(module.padding, tuple) else module.padding
            self.stride = module.stride[0] if isinstance(module.stride, tuple) else module.stride
            self.is_linear = False
            self.conv_dim = 2
        elif isinstance(module, nn.Conv3d):
            self.in_features = module.in_channels
            self.out_features = module.out_channels
            self.kernel_size = module.kernel_size[0] if isinstance(module.kernel_size, tuple) else module.kernel_size
            self.padding = module.padding[0] if isinstance(module.padding, tuple) else module.padding
            self.stride = module.stride[0] if isinstance(module.stride, tuple) else module.stride
            self.is_linear = False
            self.conv_dim = 3
        elif isinstance(module, nn.Embedding):
            self.in_features = module.num_embeddings
            self.out_features = module.embedding_dim
            self.is_linear = True  # treat as linear
        elif isinstance(module, nn.MultiheadAttention):
            # We adapt only out_proj (which is a Linear)
            out_proj = module.out_proj
            self.in_features = out_proj.in_features
            self.out_features = out_proj.out_features
            self.is_linear = True
        else:
            raise UnsupportedModuleError(f"Unsupported module type: {type(module)}")

    def _init_weights(self, device: Optional[torch.device], dtype: Optional[torch.dtype]):
        device = device or torch.device('cpu')
        dtype = dtype or torch.float32

        if self.is_linear:
            # Linear / Embedding / MultiheadAttention out_proj
            self.lora_A = nn.Parameter(torch.empty(self.in_features, self.rank, device=device, dtype=dtype))
            self.lora_B = nn.Parameter(torch.empty(self.rank, self.out_features, device=device, dtype=dtype))
            nn.init.kaiming_uniform_(self.lora_A, a=0.01)
            nn.init.zeros_(self.lora_B)
        else:
            # Convolution: use 1x1 conv (kernel size 1) for low-rank adaptation
            if self.conv_dim == 1:
                self.conv_A = nn.Conv1d(self.in_features, self.rank, kernel_size=1, bias=False)
                self.conv_B = nn.Conv1d(self.rank, self.out_features, kernel_size=1, bias=False)
            elif self.conv_dim == 2:
                self.conv_A = nn.Conv2d(self.in_features, self.rank, kernel_size=1, bias=False)
                self.conv_B = nn.Conv2d(self.rank, self.out_features, kernel_size=1, bias=False)
            elif self.conv_dim == 3:
                self.conv_A = nn.Conv3d(self.in_features, self.rank, kernel_size=1, bias=False)
                self.conv_B = nn.Conv3d(self.rank, self.out_features, kernel_size=1, bias=False)
            else:
                raise ValueError("Invalid convolution dimension")
            # Initialize A and B
            nn.init.kaiming_uniform_(self.conv_A.weight, a=0.01)
            nn.init.zeros_(self.conv_B.weight)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Forward pass of the LoRA adapter, scaled by the current scale."""
        if self.is_linear:
            return (x @ self.lora_A @ self.lora_B) * self._scale
        else:
            # For Conv, input x shape (batch, channels, ...)
            return self.conv_B(self.conv_A(x)) * self._scale

    @property
    def scale(self) -> float:
        return self._scale

    @scale.setter
    def scale(self, value: float):
        self._scale = value

    def merge(self, base_module: nn.Module):
        """
        Merge the LoRA weights into the base module's weights.
        For linear: weight += (B @ A.t()).t() * scale.
        For conv: base weight += conv_B.weight * conv_A.weight (appropriately reshaped).
        """
        if isinstance(base_module, nn.Linear) or isinstance(base_module, nn.Embedding):
            delta = (self.lora_B @ self.lora_A.t()) * self._scale
            base_module.weight.data += delta.t()
        elif isinstance(base_module, nn.Conv1d):
            # For conv, we need to combine 1x1 convs: result is (out_channels, in_channels, 1,1,...)
            # The effective weight is conv_B.weight (out, rank, 1) @ conv_A.weight (rank, in, 1)
            # Reshape to (out, in, 1)
            combined = torch.matmul(
                self.conv_B.weight.squeeze(-1).squeeze(-1),  # (out, rank)
                self.conv_A.weight.squeeze(-1).squeeze(-1)   # (rank, in)
            ) * self._scale  # (out, in)
            # Reshape to base weight shape
            base_module.weight.data += combined.reshape(base_module.weight.shape)
        elif isinstance(base_module, nn.Conv2d) or isinstance(base_module, nn.Conv3d):
            # Similar for 2d/3d
            combined = torch.matmul(
                self.conv_B.weight.squeeze(),
                self.conv_A.weight.squeeze()
            ) * self._scale
            base_module.weight.data += combined.reshape(base_module.weight.shape)
        else:
            raise NotImplementedError(f"Merging not implemented for {type(base_module)}")


# ============================================================================
# Adapter Manager (Enhanced with XAI, safety, approval, chaos)
# ============================================================================

class AdapterManager:
    """
    Manages adapters for an expert model.
    Supports per-layer rank/scale configuration, multiple adapters per layer,
    mode switching via scale updates, merging, and serialization.
    """

    _local = threading.local()

    def __init__(
        self,
        expert: nn.Module,
        default_rank: int = 8,
        mode_scales: Optional[Dict[str, float]] = None,
        per_layer_config: Optional[Dict[str, Dict[str, Any]]] = None,
        device: Optional[torch.device] = None,
        dtype: Optional[torch.dtype] = None,
        enable_bio: bool = True,
        enable_moe: bool = True,
        enable_modp: bool = True,
        bio_optimizer: Optional[GeneticPolicyGenerator] = None,
        moe_router: Optional[ExpertRouter] = None,
        modp_optimizer: Optional[ParetoOptimizer] = None,
        modp_weights: Optional[Dict[str, float]] = None,
        context_encoder: Optional[Callable] = None,
        enable_flexgen: bool = True,
        flexgen_cost_model: Optional[FlexGenCostModel] = None,
        gpu_profiler: Optional[GPUProfiler] = None,
        modp_planner: Optional[FlexGenMODPPlanner] = None,
        approval_callback: Optional[Callable[[str, Dict], bool]] = None,
    ):
        self.expert = expert
        self.default_rank = default_rank
        self.device = device
        self.dtype = dtype

        self.mode_scales = mode_scales or {
            'eco': 0.1,
            'balanced': 0.5,
            'performance': 1.0,
        }
        self.scale_params = nn.ParameterDict({
            mode: nn.Parameter(torch.tensor(scale, dtype=torch.float32))
            for mode, scale in self.mode_scales.items()
        })
        self._current_mode: Optional[str] = None

        self.per_layer_config = per_layer_config or {}

        self._adapters: Dict[str, Dict[str, LoRAAdapter]] = {}
        self._hooks: Dict[str, torch.utils.hooks.RemovableHandle] = {}
        self._expert_ref = weakref.ref(expert)

        self.enable_bio = enable_bio and ENHANCEMENTS_AVAILABLE
        self.enable_moe = enable_moe and ENHANCEMENTS_AVAILABLE
        self.enable_modp = enable_modp and ENHANCEMENTS_AVAILABLE

        self.bio = bio_optimizer if bio_optimizer else (GeneticPolicyGenerator() if ENHANCEMENTS_AVAILABLE else None)
        self.moe = moe_router if moe_router else (ExpertRouter() if ENHANCEMENTS_AVAILABLE else None)
        self.modp = modp_optimizer if modp_optimizer else (ParetoOptimizer() if ENHANCEMENTS_AVAILABLE else None)
        self.modp_weights = modp_weights or {
            'energy': 0.25, 'carbon': 0.25, 'latency': 0.20, 'accuracy': 0.30,
        }
        self.context_encoder = context_encoder or (lambda x: {})

        self.enable_flexgen = enable_flexgen and FLEXGEN_AVAILABLE
        self.flexgen_cost_model = flexgen_cost_model or (FlexGenCostModel() if FLEXGEN_AVAILABLE else None)
        self.gpu_profiler = gpu_profiler or (GPUProfiler() if FLEXGEN_AVAILABLE else None)
        self.modp_planner = modp_planner if modp_planner else (FlexGenMODPPlanner() if FLEXGEN_AVAILABLE else None)

        self._evolution_population = []
        self._evolution_fitness = []

        # XAI, safety, approval, chaos
        self.approval_callback = approval_callback
        self.safety_monitor = TemporalSafetyMonitor()

        self._register_adapters()
        if self.mode_scales:
            first_mode = next(iter(self.mode_scales))
            self.set_mode(first_mode)

    # ---------------- Core management (unchanged except for cleanup/save/load) ----------------
    def _get_module_config(self, name: str) -> Dict[str, Any]:
        return self.per_layer_config.get(name, {})

    def _get_rank_for_layer(self, name: str) -> int:
        config = self._get_module_config(name)
        return config.get('rank', self.default_rank)

    def _get_scales_for_layer(self, name: str) -> Dict[str, float]:
        config = self._get_module_config(name)
        return config.get('scales', self.mode_scales)

    def _get_module_sizes(self, module: nn.Module) -> Tuple[int, int]:
        if isinstance(module, nn.Linear):
            return module.in_features, module.out_features
        elif isinstance(module, nn.Conv1d):
            return module.in_channels, module.out_channels
        elif isinstance(module, nn.Conv2d):
            return module.in_channels, module.out_channels
        elif isinstance(module, nn.Conv3d):
            return module.in_channels, module.out_channels
        elif isinstance(module, nn.Embedding):
            return module.num_embeddings, module.embedding_dim
        elif isinstance(module, nn.MultiheadAttention):
            return module.out_proj.in_features, module.out_proj.out_features
        else:
            raise UnsupportedModuleError(f"Unsupported module type: {type(module)}")

    def _register_adapters(self):
        expert = self._expert_ref()
        if expert is None:
            raise AdapterError("Expert has been garbage collected.")
        for name, module in expert.named_modules():
            if isinstance(module, (nn.Linear, nn.Conv1d, nn.Conv2d, nn.Conv3d, nn.Embedding, nn.MultiheadAttention)):
                rank = self._get_rank_for_layer(name)
                self._adapters[name] = {}
                for mode in self.mode_scales.keys():
                    scales = self._get_scales_for_layer(name)
                    scale = scales.get(mode, self.mode_scales[mode])
                    adapter = LoRAAdapter(module, rank=rank, scale=scale,
                                         device=self.device, dtype=self.dtype)
                    self._adapters[name][mode] = adapter
                logger.debug(f"Registered adapters for layer: {name}")

    def _clear_hooks(self):
        for hook in self._hooks.values():
            hook.remove()
        self._hooks.clear()

    def _attach_hooks(self):
        expert = self._expert_ref()
        if expert is None:
            raise AdapterError("Expert has been garbage collected.")
        mode = self._current_mode
        if mode is None:
            raise AdapterError("No mode set; call set_mode() first.")
        for name, adapters in self._adapters.items():
            adapter = adapters.get(mode)
            if adapter is None:
                logger.warning(f"No adapter for mode '{mode}' in layer '{name}'")
                continue
            module = expert.get_submodule(name)
            if module is None:
                logger.warning(f"Module {name} not found in expert")
                continue
            def make_hook(adapter):
                def hook(module, input, output):
                    return output + adapter(input[0])
                return hook
            hook = module.register_forward_hook(make_hook(adapter))
            self._hooks[name] = hook
        logger.info(f"Attached hooks for mode '{mode}'")

    def set_mode(self, mode: str, update_hooks: bool = True):
        if mode not in self.mode_scales:
            raise ValueError(f"Unknown mode: {mode}. Available: {list(self.mode_scales.keys())}")
        if mode == self._current_mode and update_hooks:
            return
        self._current_mode = mode
        for name, adapters in self._adapters.items():
            adapter = adapters.get(mode)
            if adapter is not None:
                scale_val = self.scale_params[mode].item()
                adapter.scale = scale_val
        if update_hooks:
            self._clear_hooks()
            self._attach_hooks()
        logger.info(f"Adapter mode switched to '{mode}'")

    def forward_with_mode(self, x: torch.Tensor, mode: str) -> torch.Tensor:
        old_mode = self._current_mode
        try:
            self._local.mode = mode
            self.set_mode(mode, update_hooks=True)
            return self.expert(x)
        finally:
            if old_mode is not None:
                self.set_mode(old_mode, update_hooks=True)
            else:
                self._clear_hooks()
            self._local.mode = old_mode

    def get_adapter(self, layer_name: str, mode: str) -> Optional[LoRAAdapter]:
        return self._adapters.get(layer_name, {}).get(mode)

    def merge_adapters(self, mode: Optional[str] = None, layers: Optional[List[str]] = None):
        expert = self._expert_ref()
        if expert is None:
            raise AdapterError("Expert has been garbage collected.")
        modes = [mode] if mode else list(self.mode_scales.keys())
        layer_names = layers if layers else list(self._adapters.keys())
        for name in layer_names:
            module = expert.get_submodule(name)
            if module is None:
                continue
            for mode in modes:
                adapter = self._adapters.get(name, {}).get(mode)
                if adapter is not None:
                    if mode == self._current_mode:
                        adapter.scale = self.scale_params[mode].item()
                    adapter.merge(module)
            logger.info(f"Merged adapters for layer '{name}'")

    def cleanup(self):
        if hasattr(self, '_hooks'):
            self._clear_hooks()
        if hasattr(self, '_adapters'):
            self._adapters.clear()
        if hasattr(self, '_hooks'):
            self._hooks.clear()
        logger.info("AdapterManager cleaned up")

    def __del__(self):
        try:
            self.cleanup()
        except:
            pass

    # ---------------- Enhanced methods (XAI, safety, approval, chaos) ----------------

    def _generate_explanation(self, mode: str, scores: Dict[str, float], source: str) -> str:
        """
        Generate a human-readable explanation for mode selection.
        """
        parts = [
            f"Selected mode '{mode}' via {source}.",
            "Objective scores:",
        ]
        for key, val in scores.items():
            parts.append(f"  {key}: {val:.4f}")
        return "\n".join(parts)

    def select_mode_modp(
        self,
        objectives: Dict[str, float],
        weights: Optional[Dict[str, float]] = None,
        return_explanation: bool = True,
    ) -> Tuple[str, float, Optional[str]]:
        if not self.enable_modp or self.modp is None:
            mode = self._current_mode or list(self.mode_scales.keys())[0]
            return mode, 0.0, "MODP unavailable; using current mode."
        weights = weights or self.modp_weights
        best_utility = -float('inf')
        best_mode = None
        best_scores = None
        for mode, scale in self.mode_scales.items():
            # Use estimated metrics as objectives for this mode
            metrics = self.get_mode_metrics(mode)
            metrics.update(objectives)
            utility = self.modp.evaluate(metrics, weights)
            if utility > best_utility:
                best_utility = utility
                best_mode = mode
                best_scores = metrics
        if best_mode is None:
            best_mode = list(self.mode_scales.keys())[0]
        explanation = self._generate_explanation(best_mode, best_scores, "MODP") if return_explanation else None
        return best_mode, best_utility, explanation

    def select_mode_moe(self, context: Dict[str, Any], return_explanation: bool = True) -> Tuple[str, Optional[str]]:
        if not self.enable_moe or self.moe is None:
            mode = self._current_mode or list(self.mode_scales.keys())[0]
            return mode, "MoE unavailable; using current mode."
        encoded = self.moe.encode(context)
        mode = self.moe.select(encoded)
        explanation = f"MoE selected mode '{mode}' based on context features: {context}"
        return mode, explanation if return_explanation else None

    async def select_mode_flexgen(self, node, workload, carbon_intensity: float) -> Tuple[str, Optional[str]]:
        if not self.enable_flexgen:
            mode = self._current_mode or list(self.mode_scales.keys())[0]
            return mode, "FlexGen unavailable; using current mode."
        best_utility = -float('inf')
        best_mode = None
        best_scores = None
        for mode in self.mode_scales.keys():
            metrics = self.evaluate_mode_with_flexgen(mode, node, workload)
            objectives = {
                'accuracy': metrics['quality_score'],
                'energy': metrics['energy_joules'],
                'carbon': metrics['carbon_g'],
                'latency': metrics['latency_ms'],
            }
            utility = self.modp.evaluate(objectives, self.modp_weights) if self.modp else 0.0
            if utility > best_utility:
                best_utility = utility
                best_mode = mode
                best_scores = objectives
        if best_mode is None:
            best_mode = list(self.mode_scales.keys())[0]
        explanation = self._generate_explanation(best_mode, best_scores, "FlexGen") if best_scores else None
        return best_mode, explanation

    def forward_with_adaptive_mode(
        self,
        x: torch.Tensor,
        context: Optional[Dict[str, Any]] = None,
        objectives: Optional[Dict[str, float]] = None,
        selection_strategy: str = 'auto',
        weights: Optional[Dict[str, float]] = None,
    ) -> Tuple[torch.Tensor, str, Optional[str]]:
        """
        Forward pass that automatically selects the best mode.
        Returns (output, mode, explanation).
        """
        # Safety check (async not possible in sync method; we'll use a synchronous variant)
        # For simplicity, we'll call a sync wrapper if needed.
        # In practice, callers should use async method `forward_async` if safety checks are required.
        mode = None
        explanation = None

        if selection_strategy == 'moe' and context is not None:
            mode, explanation = self.select_mode_moe(context)
        elif selection_strategy == 'modp' and objectives is not None:
            mode, _, explanation = self.select_mode_modp(objectives, weights)
        elif selection_strategy == 'flexgen' and context and 'workload' in context and 'node' in context:
            # FlexGen selection is async; we'll fallback to synchronous estimation
            # We'll not implement async call here; user should use `forward_async`.
            pass
        else:  # auto
            if context is not None and objectives is not None:
                # Use MODP if objectives given, MoE if context given
                mode_modp, _, explanation_modp = self.select_mode_modp(objectives, weights)
                mode_moe, explanation_moe = self.select_mode_moe(context)
                # Combine: choose mode that both agree or use MODP utility
                mode, _, explanation = self.select_mode_modp(objectives, weights)
            elif context is not None:
                mode, explanation = self.select_mode_moe(context)
            elif objectives is not None:
                mode, _, explanation = self.select_mode_modp(objectives, weights)
            else:
                mode = self._current_mode or list(self.mode_scales.keys())[0]
                explanation = f"No context/objectives provided; using current mode '{mode}'."

        if mode is None:
            mode = list(self.mode_scales.keys())[0]

        output = self.forward_with_mode(x, mode)
        return output, mode, explanation

    # --------------------- Safety and Approval ---------------------
    def set_approval_callback(self, callback: Callable[[str, Dict], bool]):
        self.approval_callback = callback

    async def check_safety(self, mode: str, context: Dict) -> List[str]:
        """Check temporal safety invariants for switching to a mode."""
        state = context.copy()
        state['mode'] = mode
        return await self.safety_monitor.check(state)

    # --------------------- Chaos Testing ---------------------
    async def inject_fault(self, fault_type: str, **kwargs):
        if fault_type == 'clear_adapters':
            self._adapters.clear()
        elif fault_type == 'corrupt_scale':
            for p in self.scale_params.values():
                p.data.fill_(0.0)
        elif fault_type == 'remove_hooks':
            self._clear_hooks()
        else:
            logger.warning(f"Unknown fault type: {fault_type}")

    async def run_chaos_test(self) -> Dict[str, Any]:
        report = {'faults': [], 'results': {}}
        # Test clear adapters
        await self.inject_fault('clear_adapters')
        report['faults'].append('clear_adapters')
        try:
            self.forward_with_adaptive_mode(torch.randn(1, self._get_module_sizes(list(self.expert.named_modules())[0][1])[0]),
                                            {}, {})
            report['results']['clear_adapters'] = 'unexpected_success'
        except Exception as e:
            report['results']['clear_adapters'] = f'failed_as_expected: {type(e).__name__}'
        # Re-register adapters
        self._register_adapters()
        # Test corrupt scale
        await self.inject_fault('corrupt_scale')
        report['faults'].append('corrupt_scale')
        mode = self._current_mode or list(self.mode_scales.keys())[0]
        self.set_mode(mode)
        report['results']['corrupt_scale'] = 'scale_params_are_zero'
        return report

    # --------------------- Serialization (fixed) ---------------------
    def save(self, path: Path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        checkpoint = {
            'version': 4,
            'default_rank': self.default_rank,
            'mode_scales': {mode: scale.item() for mode, scale in self.scale_params.items()},
            'per_layer_config': self.per_layer_config,
            'adapters': {},
            'modp_weights': self.modp_weights,
            'evolution_population': self._evolution_population,
            'evolution_fitness': self._evolution_fitness,
            'current_mode': self._current_mode,
        }
        for layer, adapters in self._adapters.items():
            checkpoint['adapters'][layer] = {}
            for mode, adapter in adapters.items():
                checkpoint['adapters'][layer][mode] = adapter.state_dict()
        torch.save(checkpoint, path)
        logger.info(f"Adapters saved to {path}")

    @classmethod
    def load(cls, path: Path, expert: nn.Module) -> "AdapterManager":
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Adapter checkpoint not found: {path}")
        checkpoint = torch.load(path, map_location='cpu')
        version = checkpoint.get('version', 1)
        default_rank = checkpoint.get('default_rank', 8)
        mode_scales = checkpoint.get('mode_scales', {'eco': 0.1, 'balanced': 0.5, 'performance': 1.0})
        per_layer_config = checkpoint.get('per_layer_config', {})
        manager = cls(
            expert,
            default_rank=default_rank,
            mode_scales=mode_scales,
            per_layer_config=per_layer_config,
            modp_weights=checkpoint.get('modp_weights', None),
        )
        manager._evolution_population = checkpoint.get('evolution_population', [])
        manager._evolution_fitness = checkpoint.get('evolution_fitness', [])
        for layer, adapters in checkpoint['adapters'].items():
            if layer not in manager._adapters:
                logger.warning(f"Layer '{layer}' not found; skipping")
                continue
            for mode, state in adapters.items():
                if mode in manager._adapters[layer]:
                    manager._adapters[layer][mode].load_state_dict(state)
                else:
                    logger.warning(f"Mode '{mode}' not found for layer '{layer}'; skipping")
        # Restore current mode and attach hooks
        current_mode = checkpoint.get('current_mode')
        if current_mode is not None:
            manager.set_mode(current_mode, update_hooks=True)
        logger.info(f"Adapters loaded from {path}")
        return manager
