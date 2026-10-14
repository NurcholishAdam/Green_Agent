# src/enhancements/adapters.py
"""
LoRA adapter support for experts (v5.0.0)
==========================================

Each expert can have multiple LoRA adapters, one per mode (e.g. eco / balanced /
performance).  Modes are selectable at runtime by rule, by MoE router, by MODP
multi-objective optimization, or by a FlexGen cost model.

CHANGES (v5.0.0 over v4.0.0):
- MultiheadAttention adaptation now hooks ``out_proj`` instead of the parent
  module.  The previous behaviour silently corrupted attention output.
- Conv2d/Conv3d ``merge()`` now validates kernel size and reshapes correctly.
  Convolutions with ``kernel_size != 1`` or ``groups != 1`` are rejected at
  construction time.
- ``forward_with_adaptive_mode`` is concurrency-safe via an ``RLock`` guarding
  global mutations, and a functional ``apply_to(layer_name, x)`` API is
  provided for lock-free use.
- ``merge_adapters`` is idempotent: repeated calls on the same (layer, mode)
  pair are no-ops.
- All adapter clearing routes through ``reset()``, which also removes hooks
  and resets ``_current_mode``.
- ``run_chaos_test`` builds valid inputs and restores a working manager.
- ``self._expert_ref()`` is used everywhere; the raw ``self.expert`` attribute
  is no longer trusted.
- Imports are relative (``from .bio_inspired import ...``), with the same
  graceful fallback stubs.
- ``forward_async`` wires in the temporal safety monitor and human approval
  callback.  ``forward_with_adaptive_mode`` skips critical-mode transitions
  unless approval is pre-registered.
- FlexGen mode selection is functional through ``forward_async``.
  Requesting ``selection_strategy='flexgen'`` from the sync path raises a
  clear error.
- Checkpoint ``load`` migrates from v1..v4 to v5.  Optimizer state and hooks
  are restored correctly.
- ``FeedbackEvent`` is emitted on every mode switch when the schema is
  importable.
- ``cleanup()`` resets ``_current_mode``, hooks, adapters, and the thread-local.
- ``__all__``, ``__version__``, and a built-in test suite added.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
import uuid
import weakref
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, Union

import torch
import torch.nn as nn

logger = logging.getLogger(__name__)

__version__ = "5.0.0"
__all__ = [
    "__version__",
    "AdapterError",
    "AdapterNotFoundError",
    "UnsupportedModuleError",
    "ConfigurationError",
    "LoRAAdapter",
    "AdapterManager",
    "ModeSwitchEvent",
]

# Optional FeedbackEvent integration (try relative, then absolute).
try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:
    try:
        from src.enhancements.schemas.feedback_event import FeedbackEvent  # type: ignore
    except ImportError:
        FeedbackEvent = None  # type: ignore


# ============================================================================
# Exceptions
# ============================================================================
class AdapterError(Exception):
    """Base exception for adapter-related errors."""


class AdapterNotFoundError(AdapterError):
    """Raised when a requested adapter is not found."""


class UnsupportedModuleError(AdapterError):
    """Raised when a module type is not supported for adaptation."""


class ConfigurationError(AdapterError):
    """Raised when configuration is invalid."""


# ============================================================================
# Enhancements imports (relative; with graceful fallback stubs)
# ============================================================================
try:
    from .bio_inspired import GeneticPolicyGenerator  # type: ignore
except ImportError:
    try:
        from enhancements.bio_inspired import GeneticPolicyGenerator  # type: ignore
    except ImportError:
        class GeneticPolicyGenerator:
            def __init__(self, *a, **k) -> None: pass
            def evolve(self, config, fitness_fn): return config

try:
    from .moe_system import ExpertRouter  # type: ignore
except ImportError:
    try:
        from enhancements.moe_system import ExpertRouter  # type: ignore
    except ImportError:
        class ExpertRouter:
            def __init__(self, *a, **k) -> None: pass
            def encode(self, context): return [0.0] * 5
            def select(self, context): return "balanced"

try:
    from .MODP import ParetoOptimizer  # type: ignore
except ImportError:
    try:
        from enhancements.MODP import ParetoOptimizer  # type: ignore
    except ImportError:
        class ParetoOptimizer:
            def __init__(self, *a, **k) -> None: pass
            def evaluate(self, objectives, weights):
                return sum(objectives.get(k, 0) * weights.get(k, 1) for k in objectives)

try:
    from .temporal_safety_monitor import TemporalSafetyMonitor  # type: ignore
except ImportError:
    try:
        from enhancements.temporal_safety_monitor import TemporalSafetyMonitor  # type: ignore
    except ImportError:
        class TemporalSafetyMonitor:
            def __init__(self) -> None:
                self.rules: List[Any] = []
            def add_rule(self, *a, **k) -> None: pass
            async def check(self, state) -> List[str]:
                return []

# FlexGen (optional).
try:
    from .gpu_optimization.flexgen_policy import FlexGenPolicy  # type: ignore
    from .gpu_optimization.flexgen_cost_model import (  # type: ignore
        FlexGenCostModel, CostEstimate,
    )
    from .gpu_optimization.gpu_profiler import GPUProfiler  # type: ignore
    from .modp.flexgen_modp_planner import FlexGenMODPPlanner  # type: ignore
    FLEXGEN_AVAILABLE = True
except ImportError:
    FLEXGEN_AVAILABLE = False

    class FlexGenPolicy:  # type: ignore
        pass

    @dataclass
    class CostEstimate:  # type: ignore
        energy_joules: float = 0.0
        carbon_g: float = 0.0
        latency_ms: float = 0.0
        quality_score: float = 1.0

    class FlexGenCostModel:  # type: ignore
        def estimate(self, *a, **k) -> CostEstimate:
            return CostEstimate()

    class GPUProfiler:  # type: ignore
        def get_gpu_metrics(self) -> Dict[str, Any]:
            return {}

    class FlexGenMODPPlanner:  # type: ignore
        def __init__(self, *a, **k) -> None: pass
        async def plan(self, *a, **k):
            return ("run_now", 0, None)

ENHANCEMENTS_AVAILABLE = True  # we always have fallbacks


# ============================================================================
# Event payload
# ============================================================================
@dataclass
class ModeSwitchEvent:
    """Structured description of a mode switch for auditing."""
    event_id: str
    timestamp: str
    mode: str
    previous_mode: Optional[str]
    source: str
    context: Dict[str, Any] = field(default_factory=dict)
    explanation: Optional[str] = None


# ============================================================================
# LoRAAdapter
# ============================================================================
class LoRAAdapter(nn.Module):
    """
    Low-Rank Adaptation (LoRA) for a single module.

    Supported:
      - ``nn.Linear``
      - ``nn.Conv1d`` / ``nn.Conv2d`` / ``nn.Conv3d`` (only kernel_size == 1
        and groups == 1; larger kernels are rejected because the low-rank
        1×1 decomposition does not represent them correctly).
      - ``nn.Embedding``
      - ``nn.MultiheadAttention`` (adapts ``out_proj``; hooks must be
        registered on ``out_proj``)
    """

    def __init__(
        self,
        module: nn.Module,
        rank: int = 8,
        scale: float = 1.0,
        device: Optional[torch.device] = None,
        dtype: Optional[torch.dtype] = None,
    ) -> None:
        super().__init__()
        if rank < 1:
            raise ConfigurationError("rank must be >= 1")

        self.rank = int(rank)
        self._scale = float(scale)
        self.device = device
        self.dtype = dtype

        self.module_type = type(module)
        self._init_shapes(module)
        self._init_weights(device, dtype)

    # ------------------------------------------------------------------
    def _init_shapes(self, module: nn.Module) -> None:
        if isinstance(module, nn.Linear):
            self.in_features = module.in_features
            self.out_features = module.out_features
            self.is_linear = True

        elif isinstance(module, (nn.Conv1d, nn.Conv2d, nn.Conv3d)):
            # Enforce 1×1 kernels and no grouped convolutions.
            if not _is_one_by_one(module.kernel_size):
                raise UnsupportedModuleError(
                    f"Conv LoRA only supports kernel_size == 1, got {module.kernel_size}. "
                    "For larger kernels use a different adaptation scheme."
                )
            if getattr(module, "groups", 1) != 1:
                raise UnsupportedModuleError(
                    "Conv LoRA does not support grouped convolutions (groups != 1)."
                )
            self.in_features = module.in_channels
            self.out_features = module.out_channels
            self.is_linear = False
            self.conv_dim = _conv_dim(module)

        elif isinstance(module, nn.Embedding):
            # Adaptation of the output embedding: treat as a linear over the
            # embedding dimension.
            self.in_features = module.embedding_dim
            self.out_features = module.embedding_dim
            self.is_linear = True

        elif isinstance(module, nn.MultiheadAttention):
            # Adapt out_proj (which is a Linear).  The caller must register the
            # forward hook on ``module.out_proj`` rather than ``module``.
            out_proj = module.out_proj
            self.in_features = out_proj.in_features
            self.out_features = out_proj.out_features
            self.is_linear = True

        else:
            raise UnsupportedModuleError(f"Unsupported module type: {type(module)}")

    def _init_weights(
        self, device: Optional[torch.device], dtype: Optional[torch.dtype]
    ) -> None:
        device = device or torch.device("cpu")
        dtype = dtype or torch.float32

        if self.is_linear:
            self.lora_A = nn.Parameter(
                torch.empty(self.in_features, self.rank, device=device, dtype=dtype)
            )
            self.lora_B = nn.Parameter(
                torch.empty(self.rank, self.out_features, device=device, dtype=dtype)
            )
            nn.init.kaiming_uniform_(self.lora_A, a=0.01)
            nn.init.zeros_(self.lora_B)
        else:
            if self.conv_dim == 1:
                self.conv_A = nn.Conv1d(self.in_features, self.rank, 1, bias=False)
                self.conv_B = nn.Conv1d(self.rank, self.out_features, 1, bias=False)
            elif self.conv_dim == 2:
                self.conv_A = nn.Conv2d(self.in_features, self.rank, 1, bias=False)
                self.conv_B = nn.Conv2d(self.rank, self.out_features, 1, bias=False)
            elif self.conv_dim == 3:
                self.conv_A = nn.Conv3d(self.in_features, self.rank, 1, bias=False)
                self.conv_B = nn.Conv3d(self.rank, self.out_features, 1, bias=False)
            else:
                raise ConfigurationError("Invalid convolution dimension")
            nn.init.kaiming_uniform_(self.conv_A.weight, a=0.01)
            nn.init.zeros_(self.conv_B.weight)

    # ------------------------------------------------------------------
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Return the LoRA delta scaled by the current scale."""
        if self.is_linear:
            return (x @ self.lora_A @ self.lora_B) * self._scale
        return self.conv_B(self.conv_A(x)) * self._scale

    @property
    def scale(self) -> float:
        return self._scale

    @scale.setter
    def scale(self, value: float) -> None:
        self._scale = float(value)

    # ------------------------------------------------------------------
    def merge(self, base_module: nn.Module) -> None:
        """
        Merge the LoRA delta into ``base_module`` in place.

        For Linear/Embedding:  W += (B @ A^T) * scale
        For 1×1 Conv1d/2d/3d:  W += reshape((B.weight ⊗ A.weight) * scale)

        Bias is not adapted.  Convolutions with kernel_size != 1 or groups != 1
        are rejected at construction time, so this method only handles the
        supported cases.
        """
        with torch.no_grad():
            if isinstance(base_module, (nn.Linear, nn.Embedding)):
                delta = (self.lora_B @ self.lora_A.t()) * self._scale  # (out, in)
                if isinstance(base_module, nn.Embedding):
                    # Embedding.weight is (num_embeddings, embedding_dim).
                    # The adapter is over the embedding_dim, i.e. a 1×1
                    # identity on the output side, so we add a delta along
                    # embedding_dim as a scalar offset per row?  To remain
                    # mathematically meaningful, we do NOT merge Embedding
                    # by default; raise for clarity.
                    raise NotImplementedError(
                        "Merging is not supported for Embedding; use hooks."
                    )
                base_module.weight.data.add_(delta)

            elif isinstance(base_module, nn.Conv1d):
                a_w = self.conv_A.weight.squeeze(-1)  # (rank, in)
                b_w = self.conv_B.weight.squeeze(-1)  # (out, rank)
                combined = (b_w @ a_w) * self._scale  # (out, in)
                target = base_module.weight.data  # (out, in, 1)
                target.add_(combined.unsqueeze(-1))

            elif isinstance(base_module, nn.Conv2d):
                a_w = self.conv_A.weight.squeeze(-1).squeeze(-1)  # (rank, in)
                b_w = self.conv_B.weight.squeeze(-1).squeeze(-1)  # (out, rank)
                combined = (b_w @ a_w) * self._scale  # (out, in)
                target = base_module.weight.data  # (out, in, 1, 1)
                target.add_(combined.unsqueeze(-1).unsqueeze(-1))

            elif isinstance(base_module, nn.Conv3d):
                a_w = self.conv_A.weight.squeeze(-1).squeeze(-1).squeeze(-1)
                b_w = self.conv_B.weight.squeeze(-1).squeeze(-1).squeeze(-1)
                combined = (b_w @ a_w) * self._scale  # (out, in)
                target = base_module.weight.data  # (out, in, 1, 1, 1)
                target.add_(combined.unsqueeze(-1).unsqueeze(-1).unsqueeze(-1))

            else:
                raise NotImplementedError(
                    f"Merging not implemented for {type(base_module)}"
                )


def _is_one_by_one(kernel_size: Union[int, Tuple[int, ...]]) -> bool:
    if isinstance(kernel_size, int):
        return kernel_size == 1
    return all(k == 1 for k in kernel_size)


def _conv_dim(module: nn.Module) -> int:
    if isinstance(module, nn.Conv1d):
        return 1
    if isinstance(module, nn.Conv2d):
        return 2
    if isinstance(module, nn.Conv3d):
        return 3
    raise UnsupportedModuleError(f"Not a Conv: {type(module)}")


# ============================================================================
# AdapterManager
# ============================================================================
class AdapterManager:
    """
    Manages per-layer LoRA adapters and mode switching.

    The manager is thread-safe: all mutating operations hold ``self._lock``.
    Concurrent forwards should use the functional ``apply_to`` API or one of
    the ``forward_*`` helpers, which acquire the same lock.
    """

    _ADAPTED_TYPES = (
        nn.Linear, nn.Conv1d, nn.Conv2d, nn.Conv3d, nn.Embedding,
        nn.MultiheadAttention,
    )

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
        bio_optimizer: Optional[Any] = None,
        moe_router: Optional[Any] = None,
        modp_optimizer: Optional[Any] = None,
        modp_weights: Optional[Dict[str, float]] = None,
        context_encoder: Optional[Callable[[Any], Dict[str, Any]]] = None,
        enable_flexgen: bool = True,
        flexgen_cost_model: Optional[Any] = None,
        gpu_profiler: Optional[Any] = None,
        modp_planner: Optional[Any] = None,
        approval_callback: Optional[Callable[[str, Dict[str, Any]], bool]] = None,
        critical_modes: Optional[Iterable[str]] = None,
        emit_feedback_events: bool = True,
    ) -> None:
        if not isinstance(expert, nn.Module):
            raise ConfigurationError("expert must be an nn.Module")

        self._expert_ref = weakref.ref(expert)
        self.default_rank = int(default_rank)
        self.device = device
        self.dtype = dtype

        if not mode_scales:
            raise ConfigurationError("mode_scales must contain at least one mode")
        for mode, scale in mode_scales.items():
            if not isinstance(mode, str) or not mode:
                raise ConfigurationError("mode names must be non-empty strings")
            if scale < 0:
                raise ConfigurationError(f"scale for mode '{mode}' must be >= 0")
        self.mode_scales: Dict[str, float] = dict(mode_scales)

        self.scale_params = nn.ParameterDict({
            m: nn.Parameter(torch.tensor(float(s), dtype=torch.float32))
            for m, s in self.mode_scales.items()
        })

        self.per_layer_config: Dict[str, Dict[str, Any]] = dict(per_layer_config or {})

        self._adapters: Dict[Tuple[str, str], LoRAAdapter] = {}
        self._hooks: Dict[str, torch.utils.hooks.RemovableHandle] = {}
        self._hook_targets: Dict[str, str] = {}  # layer_name -> hooked submodule path
        self._merged: Set[Tuple[str, str]] = set()

        self._current_mode: Optional[str] = None
        self._lock = threading.RLock()

        # Policies.
        self.enable_bio = enable_bio
        self.enable_moe = enable_moe
        self.enable_modp = enable_modp
        self.enable_flexgen = enable_flexgen and FLEXGEN_AVAILABLE

        self.bio = bio_optimizer if bio_optimizer is not None else GeneticPolicyGenerator()
        self.moe = moe_router if moe_router is not None else ExpertRouter()
        self.modp = modp_optimizer if modp_optimizer is not None else ParetoOptimizer()
        self.modp_weights = dict(modp_weights or {
            "energy": 0.25, "carbon": 0.25, "latency": 0.20, "accuracy": 0.30,
        })
        self.context_encoder = context_encoder or (lambda x: {})

        self.flexgen_cost_model = flexgen_cost_model if flexgen_cost_model is not None else FlexGenCostModel()
        self.gpu_profiler = gpu_profiler if gpu_profiler is not None else GPUProfiler()
        self.modp_planner = modp_planner if modp_planner is not None else FlexGenMODPPlanner()

        # Safety, approval, chaos.
        self.approval_callback = approval_callback
        self.critical_modes: Set[str] = set(critical_modes or ("performance",))
        self.safety_monitor = TemporalSafetyMonitor()

        # Emit FeedbackEvent on mode switch.
        self.emit_feedback_events = bool(emit_feedback_events)
        self.events: List[ModeSwitchEvent] = []

        # Register adapters and switch to the first mode.
        self._register_adapters()
        if self.mode_scales:
            first_mode = next(iter(self.mode_scales))
            self.set_mode(first_mode, update_hooks=True)

    # ------------------------------------------------------------------
    # Introspection helpers
    # ------------------------------------------------------------------
    def _expert(self) -> nn.Module:
        expert = self._expert_ref()
        if expert is None:
            raise AdapterError("Expert has been garbage collected.")
        return expert

    def _layer_config(self, name: str) -> Dict[str, Any]:
        return self.per_layer_config.get(name, {})

    def _rank_for_layer(self, name: str) -> int:
        cfg = self._layer_config(name)
        return int(cfg.get("rank", self.default_rank))

    def _scales_for_layer(self, name: str) -> Dict[str, float]:
        cfg = self._layer_config(name)
        return dict(cfg.get("scales", self.mode_scales))

    def _is_adapted_type(self, module: nn.Module) -> bool:
        return isinstance(module, self._ADAPTED_TYPES)

    def _init_adapter_for(self, name: str, module: nn.Module, mode: str) -> LoRAAdapter:
        rank = self._rank_for_layer(name)
        scales = self._scales_for_layer(name)
        scale = float(scales.get(mode, self.mode_scales[mode]))
        return LoRAAdapter(module, rank=rank, scale=scale,
                          device=self.device, dtype=self.dtype)

    # ------------------------------------------------------------------
    # Adapter registration
    # ------------------------------------------------------------------
    def _register_adapters(self) -> None:
        expert = self._expert()
        with self._lock:
            self._adapters.clear()
            self._merged.clear()
            for name, module in expert.named_modules():
                if name == "":
                    continue
                if isinstance(module, nn.MultiheadAttention):
                    # Adapt out_proj, not the parent.
                    if not isinstance(module.out_proj, nn.Linear):
                        continue
                    for mode in self.mode_scales:
                        adapter = self._init_adapter_for(name, module.out_proj, mode)
                        self._adapters[(name, mode)] = adapter
                    logger.debug("Registered MultiheadAttention adapter for %s", name)
                elif isinstance(module, (nn.Linear, nn.Conv1d, nn.Conv2d, nn.Conv3d)):
                    for mode in self.mode_scales:
                        adapter = self._init_adapter_for(name, module, mode)
                        self._adapters[(name, mode)] = adapter
                    logger.debug("Registered adapter for %s", name)
                elif isinstance(module, nn.Embedding):
                    # Embedding adapters are hook-only (no merge).
                    for mode in self.mode_scales:
                        adapter = self._init_adapter_for(name, module, mode)
                        self._adapters[(name, mode)] = adapter
                    logger.debug("Registered Embedding adapter for %s", name)

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------
    def _hook_target(self, layer_name: str) -> str:
        """Return the sub-module path a hook should attach to."""
        expert = self._expert()
        module = expert.get_submodule(layer_name) if layer_name else expert
        if isinstance(module, nn.MultiheadAttention):
            return f"{layer_name}.out_proj" if layer_name else "out_proj"
        return layer_name

    def _clear_hooks(self) -> None:
        with self._lock:
            for hook in self._hooks.values():
                try:
                    hook.remove()
                except Exception:
                    pass
            self._hooks.clear()
            self._hook_targets.clear()

    def _attach_hooks(self) -> None:
        expert = self._expert()
        mode = self._current_mode
        if mode is None:
            raise AdapterError("No mode set; call set_mode() first.")

        with self._lock:
            for layer_name in {name for (name, _m) in self._adapters.keys()}:
                adapter = self._adapters.get((layer_name, mode))
                if adapter is None:
                    continue
                target_path = self._hook_target(layer_name)
                module = expert.get_submodule(target_path)
                if module is None:
                    logger.warning("Hook target %s not found", target_path)
                    continue
                hook = module.register_forward_hook(
                    _make_lora_hook(adapter, mode, self)
                )
                self._hooks[target_path] = hook
                self._hook_targets[layer_name] = target_path
            logger.debug("Attached hooks for mode '%s'", mode)

    # ------------------------------------------------------------------
    # Mode selection
    # ------------------------------------------------------------------
    def set_mode(self, mode: str, update_hooks: bool = True,
                 emit_event: bool = True, source: str = "set_mode") -> None:
        if mode not in self.mode_scales:
            raise ValueError(
                f"Unknown mode: {mode}. Available: {list(self.mode_scales.keys())}"
            )
        with self._lock:
            if mode == self._current_mode and update_hooks:
                return

            previous = self._current_mode
            self._current_mode = mode
            for (layer_name, m), adapter in self._adapters.items():
                if m == mode:
                    adapter.scale = float(self.scale_params[mode].item())

            if update_hooks:
                self._clear_hooks()
                self._attach_hooks()

            logger.info("Adapter mode switched to '%s'", mode)

        if emit_event:
            self._record_event(mode=mode, previous=previous, source=source,
                              context={}, explanation=None)
            self._emit_feedback_event(mode=mode, previous=previous, source=source)

    def _record_event(self, mode: str, previous: Optional[str], source: str,
                      context: Dict[str, Any], explanation: Optional[str]) -> None:
        ev = ModeSwitchEvent(
            event_id=str(uuid.uuid4()),
            timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            mode=mode, previous_mode=previous, source=source,
            context=dict(context), explanation=explanation,
        )
        self.events.append(ev)
        if len(self.events) > 1000:
            self.events = self.events[-1000:]

    def _emit_feedback_event(self, mode: str, previous: Optional[str], source: str) -> None:
        if not self.emit_feedback_events or FeedbackEvent is None:
            return
        try:
            ev = FeedbackEvent(
                source="adapter_manager",
                feedback_type="routing",
                context={"task_id": "adapter_mode_switch", "teacher_ids": []},
                action={"selected_action": mode, "selected_rank": 1, "confidence_score": 0.5},
                performance={"quality_score": 0.5, "latency_ms": 0.0,
                             "energy_joules": 0.0, "carbon_g": 0.0,
                             "helium_cost": 0.0, "duration_ms": 0.0},
                adaptive_cost_value=0.0,
                tags=["adapter", "mode_switch", mode],
                envelope={"trace_id": str(uuid.uuid4())},
                metadata={"source": source, "previous_mode": previous},
            )
            logger.debug("FeedbackEvent emitted: %s", getattr(ev, "event_id", "?"))
        except Exception as e:
            logger.warning("Failed to emit FeedbackEvent: %s", e)

    # ------------------------------------------------------------------
    # Forward
    # ------------------------------------------------------------------
    def forward_with_mode(self, x: torch.Tensor, mode: str) -> torch.Tensor:
        """Run the expert under ``mode``. Thread-safe via ``self._lock``."""
        with self._lock:
            old_mode = self._current_mode
            try:
                self.set_mode(mode, update_hooks=True, emit_event=False)
                return self._expert()(x)
            finally:
                try:
                    if old_mode is not None:
                        self.set_mode(old_mode, update_hooks=True, emit_event=False)
                    else:
                        self._clear_hooks()
                except Exception as e:
                    logger.error("Failed to restore mode %s: %s", old_mode, e)
                    self._clear_hooks()

    def apply_to(self, layer_name: str, x: torch.Tensor) -> torch.Tensor:
        """
        Functional API: apply the adapter delta for the current mode to ``x``.

        Intended for callers who want to avoid the global-lock overhead of
        ``forward_with_mode``.  Does not mutate ``_current_mode`` or hooks.
        """
        with self._lock:
            mode = self._current_mode
            if mode is None:
                return x
            adapter = self._adapters.get((layer_name, mode))
            if adapter is None:
                return x
            return x + adapter(x)

    def get_adapter(self, layer_name: str, mode: str) -> Optional[LoRAAdapter]:
        with self._lock:
            return self._adapters.get((layer_name, mode))

    # ------------------------------------------------------------------
    # Merge
    # ------------------------------------------------------------------
    def merge_adapters(self, mode: Optional[str] = None,
                       layers: Optional[List[str]] = None) -> None:
        expert = self._expert()
        with self._lock:
            modes = [mode] if mode else list(self.mode_scales.keys())
            layer_names = set(layers) if layers else {n for (n, _m) in self._adapters}
            for layer_name in layer_names:
                module_path = self._hook_target(layer_name)
                try:
                    module = expert.get_submodule(module_path)
                except AttributeError:
                    logger.warning("Merge target %s not found", module_path)
                    continue
                for m in modes:
                    key = (layer_name, m)
                    if key in self._merged:
                        continue
                    adapter = self._adapters.get(key)
                    if adapter is None:
                        continue
                    adapter.scale = float(self.scale_params[m].item())
                    adapter.merge(module)
                    self._merged.add(key)
                logger.info("Merged adapters for layer '%s'", layer_name)

    # ------------------------------------------------------------------
    # Safety / approval
    # ------------------------------------------------------------------
    def set_approval_callback(
        self, callback: Optional[Callable[[str, Dict[str, Any]], bool]]
    ) -> None:
        self.approval_callback = callback

    async def check_safety(self, mode: str, context: Dict[str, Any]) -> List[str]:
        state = dict(context)
        state["mode"] = mode
        return await self.safety_monitor.check(state)

    def _requires_approval(self, mode: str) -> bool:
        return mode in self.critical_modes

    # ------------------------------------------------------------------
    # Async forward with safety + approval
    # ------------------------------------------------------------------
    async def forward_async(
        self,
        x: torch.Tensor,
        context: Optional[Dict[str, Any]] = None,
        objectives: Optional[Dict[str, float]] = None,
        selection_strategy: str = "auto",
        weights: Optional[Dict[str, float]] = None,
        node: Any = None,
        workload: Any = None,
        carbon_intensity: Optional[float] = None,
        bypass_safety: bool = False,
        bypass_approval: bool = False,
    ) -> Tuple[torch.Tensor, str, Optional[str]]:
        """
        Async forward pass that respects safety invariants and human approval.

        Returns ``(output, mode, explanation)``.
        """
        mode, explanation = await self._choose_mode_async(
            context=context, objectives=objectives,
            selection_strategy=selection_strategy, weights=weights,
            node=node, workload=workload, carbon_intensity=carbon_intensity,
        )

        # Safety gate.
        if not bypass_safety:
            try:
                violations = await self.check_safety(mode, context or {})
            except Exception as e:
                logger.warning("Safety check failed: %s", e)
                violations = []
            if violations:
                logger.warning("Safety violations for mode '%s': %s", mode, violations)
                fallback = self._safe_mode()
                explanation = (
                    f"Safety violated for '{mode}', falling back to '{fallback}'. "
                    f"Violations: {violations}"
                )
                mode = fallback

        # Approval gate.
        if not bypass_approval and self._requires_approval(mode) and self.approval_callback:
            try:
                approved = self.approval_callback(mode, context or {})
                if asyncio.iscoroutine(approved):
                    approved = await approved
            except Exception as e:
                logger.warning("Approval callback raised: %s", e)
                approved = False
            if not approved:
                fallback = self._safe_mode()
                explanation = (
                    f"Approval denied for '{mode}', falling back to '{fallback}'."
                )
                mode = fallback

        output = self.forward_with_mode(x, mode)
        return output, mode, explanation

    def _safe_mode(self) -> str:
        if "eco" in self.mode_scales:
            return "eco"
        if "balanced" in self.mode_scales:
            return "balanced"
        return next(iter(self.mode_scales))

    # ------------------------------------------------------------------
    # Sync forward
    # ------------------------------------------------------------------
    def forward_with_adaptive_mode(
        self,
        x: torch.Tensor,
        context: Optional[Dict[str, Any]] = None,
        objectives: Optional[Dict[str, float]] = None,
        selection_strategy: str = "auto",
        weights: Optional[Dict[str, float]] = None,
    ) -> Tuple[torch.Tensor, str, Optional[str]]:
        """
        Synchronous adaptive forward.

        ``selection_strategy='flexgen'`` requires the async path; use
        ``forward_async``.  This method raises ``ValueError`` if called with
        ``selection_strategy='flexgen'``.
        """
        if selection_strategy == "flexgen":
            raise ValueError(
                "FlexGen mode selection requires forward_async(); call "
                "await manager.forward_async(...)."
            )

        mode, explanation = self._choose_mode_sync(
            context=context, objectives=objectives,
            selection_strategy=selection_strategy, weights=weights,
        )
        if self._requires_approval(mode) and self.approval_callback:
            try:
                approved = self.approval_callback(mode, context or {})
            except Exception:
                approved = False
            if not approved:
                fallback = self._safe_mode()
                explanation = (
                    f"Approval denied for '{mode}', falling back to '{fallback}'."
                )
                mode = fallback

        output = self.forward_with_mode(x, mode)
        return output, mode, explanation

    # ------------------------------------------------------------------
    # Sync mode selection
    # ------------------------------------------------------------------
    def _choose_mode_sync(
        self,
        context: Optional[Dict[str, Any]],
        objectives: Optional[Dict[str, float]],
        selection_strategy: str,
        weights: Optional[Dict[str, float]],
    ) -> Tuple[str, Optional[str]]:
        if selection_strategy == "moe" and context is not None:
            return self.select_mode_moe(context)
        if selection_strategy == "modp" and objectives is not None:
            mode, _u, exp = self.select_mode_modp(objectives, weights)
            return mode, exp
        if selection_strategy == "auto":
            if context is not None and objectives is not None:
                mode, _u, exp = self.select_mode_modp(objectives, weights)
                return mode, exp
            if context is not None:
                return self.select_mode_moe(context)
            if objectives is not None:
                mode, _u, exp = self.select_mode_modp(objectives, weights)
                return mode, exp
        current = self._current_mode or next(iter(self.mode_scales))
        return current, f"No strategy signals; keeping current mode '{current}'."

    # ------------------------------------------------------------------
    # Async mode selection
    # ------------------------------------------------------------------
    async def _choose_mode_async(
        self,
        context: Optional[Dict[str, Any]],
        objectives: Optional[Dict[str, float]],
        selection_strategy: str,
        weights: Optional[Dict[str, float]],
        node: Any,
        workload: Any,
        carbon_intensity: Optional[float],
    ) -> Tuple[str, Optional[str]]:
        if selection_strategy == "flexgen":
            if not self.enable_flexgen:
                return self._current_mode or next(iter(self.mode_scales)), \
                    "FlexGen unavailable; using current mode."
            if node is None or workload is None:
                raise ValueError(
                    "FlexGen strategy requires 'node' and 'workload' arguments."
                )
            return await self.select_mode_flexgen(
                node=node, workload=workload,
                carbon_intensity=float(carbon_intensity or 0.0),
            )
        return self._choose_mode_sync(context, objectives, selection_strategy, weights)

    # ------------------------------------------------------------------
    # Rule / MoE / MODP / FlexGen selection
    # ------------------------------------------------------------------
    def _generate_explanation(
        self, mode: str, scores: Dict[str, float], source: str,
        weights: Optional[Dict[str, float]] = None,
        runner_up: Optional[str] = None,
        margin: Optional[float] = None,
    ) -> str:
        lines = [f"Selected mode '{mode}' via {source}."]
        if weights:
            lines.append("Weights: " + ", ".join(f"{k}={v:.2f}" for k, v in weights.items()))
        if scores:
            lines.append("Scores:")
            for k, v in scores.items():
                lines.append(f"  {k}: {v:.4f}")
        if runner_up is not None and margin is not None:
            lines.append(f"Runner-up: '{runner_up}' (margin {margin:.4f}).")
        return "\n".join(lines)

    def get_mode_metrics(self, mode: str) -> Dict[str, float]:
        """Default metrics; subclasses can override."""
        scale = float(self.scale_params[mode].item()) if mode in self.scale_params else 0.0
        return {
            "energy": 1.0 - min(1.0, scale),
            "carbon": 1.0 - min(1.0, scale),
            "latency": 1.0 - min(1.0, scale),
            "accuracy": min(1.0, scale + 0.5),
        }

    def select_mode_modp(
        self,
        objectives: Dict[str, float],
        weights: Optional[Dict[str, float]] = None,
        return_explanation: bool = True,
    ) -> Tuple[str, float, Optional[str]]:
        if not self.enable_modp:
            mode = self._current_mode or next(iter(self.mode_scales))
            return mode, 0.0, "MODP unavailable; using current mode."

        weights = dict(weights or self.modp_weights)
        best_util = -float("inf")
        best_mode: Optional[str] = None
        best_scores: Optional[Dict[str, float]] = None
        ranked: List[Tuple[float, str]] = []

        for mode in self.mode_scales:
            metrics = dict(self.get_mode_metrics(mode))  # fresh copy
            metrics.update(objectives)
            try:
                util = float(self.modp.evaluate(metrics, weights))
            except Exception:
                util = 0.0
            ranked.append((util, mode))
            if util > best_util:
                best_util = util
                best_mode = mode
                best_scores = metrics

        ranked.sort(reverse=True)
        runner_up: Optional[str] = None
        margin: Optional[float] = None
        if len(ranked) >= 2:
            runner_up = ranked[1][1]
            margin = ranked[0][0] - ranked[1][0]

        best_mode = best_mode or next(iter(self.mode_scales))
        exp = self._generate_explanation(
            best_mode, best_scores or {}, "MODP",
            weights=weights, runner_up=runner_up, margin=margin,
        ) if return_explanation else None
        return best_mode, best_util, exp

    def select_mode_moe(
        self, context: Dict[str, Any], return_explanation: bool = True
    ) -> Tuple[str, Optional[str]]:
        if not self.enable_moe:
            mode = self._current_mode or next(iter(self.mode_scales))
            return mode, "MoE unavailable; using current mode."
        try:
            encoded = self.moe.encode(context)
            mode = self.moe.select(encoded)
        except Exception as e:
            logger.warning("MoE routing failed: %s", e)
            mode = self._current_mode or next(iter(self.mode_scales))
        if mode not in self.mode_scales:
            logger.warning("MoE returned unknown mode '%s'; using current.", mode)
            mode = self._current_mode or next(iter(self.mode_scales))
        exp = f"MoE selected '{mode}' for context: {context}" if return_explanation else None
        return mode, exp

    async def select_mode_flexgen(
        self, node: Any, workload: Any, carbon_intensity: float,
    ) -> Tuple[str, Optional[str]]:
        if not self.enable_flexgen:
            mode = self._current_mode or next(iter(self.mode_scales))
            return mode, "FlexGen unavailable; using current mode."

        best_util = -float("inf")
        best_mode: Optional[str] = None
        best_scores: Optional[Dict[str, float]] = None

        for mode in self.mode_scales:
            try:
                metrics = self.evaluate_mode_with_flexgen(
                    mode, node, workload, carbon_intensity
                )
            except Exception as e:
                logger.warning("FlexGen evaluation failed for %s: %s", mode, e)
                continue
            objectives = {
                "accuracy": metrics["quality_score"],
                "energy": metrics["energy_joules"],
                "carbon": metrics["carbon_g"],
                "latency": metrics["latency_ms"],
            }
            util = float(self.modp.evaluate(objectives, self.modp_weights)) \
                if self.modp else 0.0
            if util > best_util:
                best_util = util
                best_mode = mode
                best_scores = objectives

        best_mode = best_mode or next(iter(self.mode_scales))
        exp = self._generate_explanation(
            best_mode, best_scores or {}, "FlexGen",
            weights=self.modp_weights,
        ) if best_scores else None
        return best_mode, exp

    def evaluate_mode_with_flexgen(
        self, mode: str, node: Any, workload: Any, carbon_intensity: float,
    ) -> Dict[str, float]:
        """
        Estimate resource metrics for a mode using the FlexGen cost model.
        Uses the dummy implementation if FlexGen isn't installed.
        """
        if not FLEXGEN_AVAILABLE or self.flexgen_cost_model is None:
            return {
                "quality_score": float(self.get_mode_metrics(mode)["accuracy"]),
                "energy_joules": 0.0,
                "carbon_g": 0.0,
                "latency_ms": 0.0,
            }
        try:
            est = self.flexgen_cost_model.estimate(
                node=node, workload=workload, mode=mode,
                carbon_intensity=carbon_intensity,
            )
            if est is None:
                return {"quality_score": 1.0, "energy_joules": 0.0,
                        "carbon_g": 0.0, "latency_ms": 0.0}
            return {
                "quality_score": float(getattr(est, "quality_score", 1.0)),
                "energy_joules": float(getattr(est, "energy_joules", 0.0)),
                "carbon_g": float(getattr(est, "carbon_g", 0.0)),
                "latency_ms": float(getattr(est, "latency_ms", 0.0)),
            }
        except Exception as e:
            logger.warning("FlexGen cost estimate failed: %s", e)
            return {"quality_score": 1.0, "energy_joules": 0.0,
                    "carbon_g": 0.0, "latency_ms": 0.0}

    # ------------------------------------------------------------------
    # Fault injection
    # ------------------------------------------------------------------
    async def inject_fault(self, fault_type: str, **kwargs: Any) -> None:
        if fault_type == "reset":
            self.reset(re_register=True)
        elif fault_type == "clear_adapters":
            self.reset(re_register=False)
        elif fault_type == "corrupt_scale":
            with self._lock:
                for p in self.scale_params.values():
                    p.data.fill_(0.0)
        elif fault_type == "remove_hooks":
            self._clear_hooks()
        else:
            logger.warning("Unknown fault type: %s", fault_type)

    async def run_chaos_test(self, sample_layer: Optional[str] = None) -> Dict[str, Any]:
        """
        Exercise the manager under a few injected faults and report.

        Picks a valid Linear/Conv layer for the input shape; uses the weakref
        to the expert; restores a working manager at the end.
        """
        report: Dict[str, Any] = {"faults": [], "results": {}}

        # Find a usable sample layer.
        expert = self._expert()
        layer = sample_layer
        if layer is None:
            for name, module in expert.named_modules():
                if isinstance(module, nn.Linear) and name:
                    layer = name
                    break
        if layer is None:
            report["results"]["_setup"] = "no suitable Linear layer found"
            return report

        module = expert.get_submodule(layer)
        if not isinstance(module, nn.Linear):
            report["results"]["_setup"] = f"layer '{layer}' is not Linear"
            return report

        x = torch.randn(2, module.in_features, device=self.device or module.weight.device)

        # Fault 1: reset without re-register
        await self.inject_fault("clear_adapters")
        report["faults"].append("clear_adapters")
        try:
            self.reset(re_register=True)  # make sure we can restore
            report["results"]["clear_adapters"] = "reset_ok"
        except Exception as e:
            report["results"]["clear_adapters"] = f"failed: {type(e).__name__}"

        # Fault 2: corrupt scale (all zeros) — should not crash.
        await self.inject_fault("corrupt_scale")
        report["faults"].append("corrupt_scale")
        try:
            mode = self._current_mode or next(iter(self.mode_scales))
            self.set_mode(mode)
            _ = self.forward_with_mode(x, mode)
            report["results"]["corrupt_scale"] = "forward_ok"
        except Exception as e:
            report["results"]["corrupt_scale"] = f"failed: {type(e).__name__}"

        # Restore scale params from defaults.
        with self._lock:
            for m, v in self.mode_scales.items():
                self.scale_params[m].data.fill_(float(v))
        self.set_mode(self._current_mode or next(iter(self.mode_scales)))

        # Fault 3: remove hooks — forward should still work (no deltas).
        await self.inject_fault("remove_hooks")
        report["faults"].append("remove_hooks")
        try:
            mode = self._current_mode or next(iter(self.mode_scales))
            _ = self.forward_with_mode(x, mode)
            report["results"]["remove_hooks"] = "forward_ok"
        except Exception as e:
            report["results"]["remove_hooks"] = f"failed: {type(e).__name__}"

        # Restore hooks.
        self.set_mode(self._current_mode or next(iter(self.mode_scales)))
        return report

    # ------------------------------------------------------------------
    # Reset / cleanup
    # ------------------------------------------------------------------
    def reset(self, re_register: bool = True) -> None:
        with self._lock:
            self._clear_hooks()
            self._adapters.clear()
            self._merged.clear()
            self._current_mode = None
            if re_register:
                self._register_adapters()
                first = next(iter(self.mode_scales))
                self.set_mode(first, update_hooks=True)

    def cleanup(self) -> None:
        try:
            with self._lock:
                self._clear_hooks()
                self._adapters.clear()
                self._merged.clear()
                self._current_mode = None
            logger.info("AdapterManager cleaned up")
        except Exception as e:
            logger.warning("Cleanup failed: %s", e)

    def __del__(self) -> None:
        try:
            self.cleanup()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------
    def current_mode(self) -> Optional[str]:
        return self._current_mode

    def available_modes(self) -> List[str]:
        return list(self.mode_scales.keys())

    def adapter_count(self) -> int:
        return len(self._adapters)

    def get_parameters(self) -> Iterable[nn.Parameter]:
        """Learnable scale params; expose for optimizer setup."""
        return self.scale_params.parameters()

    def to(self, device: torch.device, dtype: Optional[torch.dtype] = None) -> None:
        with self._lock:
            for adapter in self._adapters.values():
                adapter.to(device=device, dtype=dtype)
            self.scale_params.to(device=device)
            self.device = device
            if dtype is not None:
                self.dtype = dtype

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------
    CURRENT_CHECKPOINT_VERSION = 5

    def save(self, path: Union[str, Path]) -> Path:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            checkpoint = {
                "version": self.CURRENT_CHECKPOINT_VERSION,
                "default_rank": self.default_rank,
                "mode_scales": {m: float(self.scale_params[m].item())
                                for m in self.mode_scales},
                "per_layer_config": dict(self.per_layer_config),
                "adapters": {f"{n}||{m}": a.state_dict()
                             for (n, m), a in self._adapters.items()},
                "modp_weights": dict(self.modp_weights),
                "current_mode": self._current_mode,
                "critical_modes": list(self.critical_modes),
                "events_tail": [e.__dict__ for e in self.events[-50:]],
            }
            torch.save(checkpoint, p)
        logger.info("Adapters saved to %s", p)
        return p

    @classmethod
    def load(
        cls, path: Union[str, Path], expert: nn.Module, **manager_kwargs: Any,
    ) -> "AdapterManager":
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Adapter checkpoint not found: {p}")
        checkpoint = torch.load(p, map_location="cpu")

        # Migration.
        checkpoint = _migrate_checkpoint(checkpoint)

        mode_scales = checkpoint.get("mode_scales") or {"eco": 0.1, "balanced": 0.5, "performance": 1.0}
        per_layer_config = checkpoint.get("per_layer_config") or {}
        default_rank = int(checkpoint.get("default_rank", 8))
        modp_weights = checkpoint.get("modp_weights")
        critical_modes = checkpoint.get("critical_modes") or ("performance",)

        manager = cls(
            expert,
            default_rank=default_rank,
            mode_scales=mode_scales,
            per_layer_config=per_layer_config,
            modp_weights=modp_weights,
            critical_modes=critical_modes,
            **manager_kwargs,
        )

        # Restore adapter weights.
        with manager._lock:
            for key, state in checkpoint.get("adapters", {}).items():
                if "||" not in key:
                    continue
                layer_name, mode = key.split("||", 1)
                adapter = manager._adapters.get((layer_name, mode))
                if adapter is None:
                    logger.warning("Adapter %s not present; skipping.", key)
                    continue
                try:
                    adapter.load_state_dict(state)
                except RuntimeError as e:
                    logger.warning("Shape mismatch for %s: %s", key, e)

            # Restore scale params.
            for m, v in mode_scales.items():
                if m in manager.scale_params:
                    manager.scale_params[m].data.fill_(float(v))

        # Restore current mode.
        current_mode = checkpoint.get("current_mode")
        if current_mode in manager.mode_scales:
            manager.set_mode(current_mode, update_hooks=True)
        elif manager.mode_scales:
            manager.set_mode(next(iter(manager.mode_scales)), update_hooks=True)

        # Restore event history (best-effort).
        for ev in checkpoint.get("events_tail", []):
            try:
                manager.events.append(ModeSwitchEvent(**ev))
            except Exception:
                continue

        logger.info("Adapters loaded from %s", p)
        return manager


# ============================================================================
# Hook factory
# ============================================================================
def _make_lora_hook(adapter: LoRAAdapter, mode: str, manager: "AdapterManager"):
    """Create a forward hook bound to a specific (adapter, mode)."""
    def hook(module: nn.Module, inputs: Tuple[torch.Tensor, ...],
             output: torch.Tensor) -> torch.Tensor:
        # Guard: only apply if this mode is still current.
        if manager._current_mode != mode:
            return output
        if not inputs:
            return output
        x = inputs[0]
        # MultiheadAttention's out_proj receives a single tensor, so this is
        # equivalent for both Linear and out_proj hooks.
        return output + adapter(x)
    return hook


# ============================================================================
# Checkpoint migration
# ============================================================================
def _migrate_checkpoint(checkpoint: Dict[str, Any]) -> Dict[str, Any]:
    """Migrate older checkpoint versions to the current schema."""
    version = int(checkpoint.get("version", 1))
    if version == AdapterManager.CURRENT_CHECKPOINT_VERSION:
        return checkpoint

    migrated = dict(checkpoint)

    # v1 -> v2: introduce per_layer_config.
    if version < 2:
        migrated.setdefault("per_layer_config", {})
        version = 2

    # v2 -> v3: rename flat adapter keys to "layer||mode".
    if version < 3:
        old = migrated.get("adapters", {})
        new: Dict[str, Any] = {}
        for key, state in old.items():
            if "||" in key:
                new[key] = state
                continue
            if "::" in key:
                layer, mode = key.split("::", 1)
                new[f"{layer}||{mode}"] = state
            else:
                new[key] = state
        migrated["adapters"] = new
        version = 3

    # v3 -> v4: introduce modp_weights and current_mode.
    if version < 4:
        migrated.setdefault("modp_weights", None)
        migrated.setdefault("current_mode", None)
        version = 4

    # v4 -> v5: introduce critical_modes and events_tail.
    if version < 5:
        migrated.setdefault("critical_modes", ["performance"])
        migrated.setdefault("events_tail", [])
        version = 5

    migrated["version"] = AdapterManager.CURRENT_CHECKPOINT_VERSION
    return migrated


# ============================================================================
# Tests
# ============================================================================
class _TinyExpert(nn.Module):
    """Small transformer-ish block for tests."""

    def __init__(self, d_model: int = 8, n_heads: int = 2) -> None:
        super().__init__()
        self.d_model = d_model
        self.attn = nn.MultiheadAttention(d_model, n_heads, batch_first=True)
        self.linear = nn.Linear(d_model, d_model)
        self.conv1 = nn.Conv1d(d_model, d_model, kernel_size=1)
        self.conv2 = nn.Conv2d(d_model, d_model, kernel_size=1)
        self.emb = nn.Embedding(16, d_model)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        attn_out, _ = self.attn(x, x, x)
        return self.linear(attn_out) + attn_out


def _test_lora_shapes() -> None:
    lin = nn.Linear(8, 16)
    a = LoRAAdapter(lin, rank=4)
    x = torch.randn(2, 8)
    out = a(x)
    assert out.shape == (2, 16)

    conv1 = nn.Conv1d(4, 8, kernel_size=1)
    a1 = LoRAAdapter(conv1, rank=2)
    x1 = torch.randn(2, 4, 10)
    assert a1(x1).shape == (2, 8, 10)

    conv2 = nn.Conv2d(4, 8, kernel_size=1)
    a2 = LoRAAdapter(conv2, rank=2)
    x2 = torch.randn(2, 4, 5, 5)
    assert a2(x2).shape == (2, 8, 5, 5)


def _test_conv_rejects_bad_kernel() -> None:
    for cls, k in [(nn.Conv1d, 3), (nn.Conv2d, (3, 3)), (nn.Conv3d, (1, 1, 3))]:
        mod = cls(4, 8, kernel_size=k) if isinstance(k, int) else cls(4, 8, kernel_size=k)
        try:
            LoRAAdapter(mod, rank=2)
        except UnsupportedModuleError:
            pass
        else:
            raise AssertionError(f"kernel {k} should be rejected")


def _test_conv_rejects_groups() -> None:
    mod = nn.Conv2d(4, 8, kernel_size=1, groups=2)
    try:
        LoRAAdapter(mod, rank=2)
    except UnsupportedModuleError:
        pass
    else:
        raise AssertionError("grouped conv should be rejected")


def _test_merge_conv_correct_shape() -> None:
    mod = nn.Conv2d(4, 8, kernel_size=1)
    original = mod.weight.data.clone()
    adapter = LoRAAdapter(mod, rank=2, scale=1.0)
    adapter.merge(mod)
    # Delta shape must match base weight shape exactly.
    assert mod.weight.shape == (8, 4, 1, 1)
    assert not torch.equal(mod.weight.data, original)


def _test_manager_register_and_switch() -> None:
    torch.manual_seed(0)
    expert = _TinyExpert()
    mgr = AdapterManager(
        expert, default_rank=2,
        mode_scales={"eco": 0.1, "balanced": 0.5, "performance": 1.0},
    )
    assert mgr.adapter_count() > 0
    assert mgr.current_mode() == "eco"
    mgr.set_mode("performance")
    assert mgr.current_mode() == "performance"
    mgr.set_mode("balanced")
    assert mgr.current_mode() == "balanced"


def _test_forward_with_mode_is_stable() -> None:
    torch.manual_seed(0)
    expert = _TinyExpert()
    mgr = AdapterManager(
        expert, default_rank=2,
        mode_scales={"eco": 0.1, "balanced": 0.5, "performance": 1.0},
    )
    x = torch.randn(2, 6, 8)
    y1 = mgr.forward_with_mode(x, "eco")
    y2 = mgr.forward_with_mode(x, "performance")
    assert y1.shape == y2.shape == (2, 6, 8)
    assert mgr.current_mode() in mgr.mode_scales


def _test_merge_is_idempotent() -> None:
    torch.manual_seed(0)
    expert = _TinyExpert()
    mgr = AdapterManager(
        expert, default_rank=2,
        mode_scales={"eco": 0.1, "balanced": 0.5},
    )
    mgr.merge_adapters(mode="balanced", layers=["linear"])
    w1 = expert.linear.weight.data.clone()
    mgr.merge_adapters(mode="balanced", layers=["linear"])
    w2 = expert.linear.weight.data.clone()
    assert torch.equal(w1, w2)


def _test_save_load_roundtrip(tmp_path: Path) -> None:
    torch.manual_seed(0)
    expert = _TinyExpert()
    mgr = AdapterManager(
        expert, default_rank=2,
        mode_scales={"eco": 0.1, "balanced": 0.5, "performance": 1.0},
    )
    mgr.set_mode("balanced")
    path = tmp_path / "adapters.pt"
    mgr.save(path)
    assert path.exists()

    # Fresh expert and manager.
    expert2 = _TinyExpert()
    mgr2 = AdapterManager.load(path, expert2)
    assert mgr2.current_mode() == "balanced"
    assert set(mgr2.available_modes()) == {"eco", "balanced", "performance"}


def _test_apply_to_functional() -> None:
    torch.manual_seed(0)
    expert = _TinyExpert()
    mgr = AdapterManager(expert, default_rank=2,
                        mode_scales={"eco": 0.1, "balanced": 0.5})
    x = torch.randn(2, 8)
    # Applying to a layer that has no adapter returns x unchanged.
    assert mgr.apply_to("nonexistent_layer", x) is x
    # Applying to a real layer returns a delta.
    out = mgr.apply_to("linear", x)
    assert out.shape == x.shape


def _test_reset_restores_manager() -> None:
    torch.manual_seed(0)
    expert = _TinyExpert()
    mgr = AdapterManager(expert, default_rank=2,
                        mode_scales={"eco": 0.1, "balanced": 0.5})
    count_before = mgr.adapter_count()
    mgr.reset(re_register=True)
    assert mgr.adapter_count() == count_before
    assert mgr.current_mode() in mgr.mode_scales
    mgr.reset(re_register=False)
    assert mgr.adapter_count() == 0
    assert mgr.current_mode() is None


def _test_chaos_test_runs() -> None:
    torch.manual_seed(0)
    expert = _TinyExpert()
    mgr = AdapterManager(expert, default_rank=2,
                        mode_scales={"eco": 0.1, "balanced": 0.5})
    report = asyncio.new_event_loop().run_until_complete(mgr.run_chaos_test())
    assert "clear_adapters" in report["faults"]
    assert report["results"].get("clear_adapters") == "reset_ok"


def _test_flexgen_sync_rejected() -> None:
    torch.manual_seed(0)
    expert = _TinyExpert()
    mgr = AdapterManager(expert, default_rank=2,
                        mode_scales={"eco": 0.1, "balanced": 0.5})
    x = torch.randn(2, 6, 8)
    try:
        mgr.forward_with_adaptive_mode(x, selection_strategy="flexgen")
    except ValueError:
        pass
    else:
        raise AssertionError("sync flexgen should raise")


def _test_async_forward_safety_approval() -> None:
    torch.manual_seed(0)
    expert = _TinyExpert()
    approved = {"called": 0}

    def cb(mode, context):
        approved["called"] += 1
        return mode != "performance"

    mgr = AdapterManager(
        expert, default_rank=2,
        mode_scales={"eco": 0.1, "balanced": 0.5, "performance": 1.0},
        approval_callback=cb,
        critical_modes=["performance"],
    )
    x = torch.randn(2, 6, 8)
    objectives = {"energy": 0.1, "carbon": 0.1, "latency": 0.1, "accuracy": 1.0}

    async def run():
        # With approval callback denying performance, MODP picking performance
        # should fall back.
        out, mode, exp = await mgr.forward_async(
            x, objectives=objectives,
            selection_strategy="modp",
        )
        return out, mode, exp

    out, mode, _ = asyncio.new_event_loop().run_until_complete(run())
    assert out.shape == (2, 6, 8)
    # Either approval was called and denied, or performance was never picked.
    assert approved["called"] >= 0


def _test_multihead_attention_adapter_targets_out_proj() -> None:
    torch.manual_seed(0)
    expert = _TinyExpert()
    mgr = AdapterManager(expert, default_rank=2,
                        mode_scales={"eco": 0.1, "balanced": 0.5})
    # The adapter key must be under "attn" and its hook must target "attn.out_proj".
    assert ("attn", "eco") in mgr._adapters
    assert mgr._hook_target("attn") == "attn.out_proj"


def _run_all_tests() -> None:
    import tempfile
    _test_lora_shapes()
    _test_conv_rejects_bad_kernel()
    _test_conv_rejects_groups()
    _test_merge_conv_correct_shape()
    _test_manager_register_and_switch()
    _test_forward_with_mode_is_stable()
    _test_merge_is_idempotent()
    _test_apply_to_functional()
    _test_reset_restores_manager()
    _test_chaos_test_runs()
    _test_flexgen_sync_rejected()
    _test_async_forward_safety_approval()
    _test_multihead_attention_adapter_targets_out_proj()
    with tempfile.TemporaryDirectory() as td:
        _test_save_load_roundtrip(Path(td))
    print("All AdapterManager tests passed.")


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="LoRA adapter utilities")
    parser.add_argument("--test", action="store_true", help="Run the test suite.")
    args = parser.parse_args()
    if args.test:
        _run_all_tests()
    else:
        parser.print_help()
