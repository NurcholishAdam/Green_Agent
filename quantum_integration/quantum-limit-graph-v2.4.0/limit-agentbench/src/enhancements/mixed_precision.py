"""
Mixed precision utilities for energy-efficient inference and training.
Supports various precision modes (fp16, fp8, fp4) with PyTorch AMP integration.
"""

import torch
import torch.nn as nn
from contextlib import contextmanager
from typing import Optional, Dict, Any, Union
import logging

logger = logging.getLogger(__name__)

class MixedPrecisionEngine:
    """
    Manages mixed precision for energy-efficient inference and training.
    Provides context managers for forward passes, automatic conversion,
    and fallback for unsupported dtypes.
    """

    # Map string names to torch dtypes
    DTYPE_MAP = {
        "fp16": torch.float16,
        "fp32": torch.float32,
        "bf16": torch.bfloat16,
    }

    # FP8 support is limited; we'll use a custom fallback if not available
    FP8_SUPPORTED = hasattr(torch, "float8_e4m3fn") or hasattr(torch, "float8_e5m2")

    def __init__(
        self,
        default_dtype: str = "fp16",
        use_amp: bool = True,
        amp_dtype: str = "fp16",
    ):
        """
        Args:
            default_dtype: Default dtype for inference (fp16, fp32, bf16, fp8, fp4).
            use_amp: Whether to use automatic mixed precision (AMP) for training.
            amp_dtype: AMP dtype (fp16 or bf16).
        """
        self.default_dtype = default_dtype
        self.use_amp = use_amp
        self.amp_dtype = amp_dtype

        # Validate dtypes
        self._validate_dtype(default_dtype)
        if use_amp:
            if amp_dtype not in ["fp16", "bf16"]:
                raise ValueError("AMP dtype must be 'fp16' or 'bf16'")
        self._amp_enabled = use_amp

        # Store original dtypes per model for restoration
        self._original_dtypes: Dict[nn.Module, torch.dtype] = {}

    def _validate_dtype(self, dtype: str):
        """Check if dtype is supported."""
        if dtype not in self.DTYPE_MAP and dtype not in ["fp8", "fp4"]:
            raise ValueError(f"Unsupported dtype '{dtype}'. Supported: {list(self.DTYPE_MAP.keys()) + ['fp8', 'fp4']}")
        if dtype in ["fp8", "fp4"] and not self.FP8_SUPPORTED:
            logger.warning(f"FP8/FP4 not supported natively; falling back to fp16.")

    def _to_dtype(self, model: nn.Module, dtype: str) -> nn.Module:
        """Convert model parameters to the specified dtype."""
        if dtype in self.DTYPE_MAP:
            target_dtype = self.DTYPE_MAP[dtype]
            return model.to(dtype=target_dtype)
        elif dtype == "fp8" and self.FP8_SUPPORTED:
            # Use fp8 if available; fallback to fp16 if not
            fp8_dtype = getattr(torch, "float8_e4m3fn", None) or getattr(torch, "float8_e5m2", None)
            if fp8_dtype is not None:
                return model.to(dtype=fp8_dtype)
            else:
                logger.warning("FP8 not available; falling back to fp16")
                return model.to(dtype=torch.float16)
        elif dtype == "fp4":
            # FP4 not natively supported; fallback to fp16
            logger.warning("FP4 not supported; falling back to fp16")
            return model.to(dtype=torch.float16)
        else:
            return model

    @contextmanager
    def quantized_forward(self, model: nn.Module, dtype: Optional[str] = None):
        """
        Context manager to run model forward pass in a specified low precision.
        This temporarily converts the model's dtype, runs the forward pass,
        and restores the original dtype.

        Args:
            model: The model to run.
            dtype: Optional dtype override; if None, uses self.default_dtype.
        """
        if dtype is None:
            dtype = self.default_dtype
        self._validate_dtype(dtype)

        # Save original dtype and convert
        if model not in self._original_dtypes:
            self._original_dtypes[model] = next(model.parameters()).dtype
        original_dtype = self._original_dtypes[model]

        # Convert model to target dtype
        converted_model = self._to_dtype(model, dtype)
        try:
            yield converted_model
        finally:
            # Restore original dtype
            converted_model.to(dtype=original_dtype)

    @contextmanager
    def amp_forward(self, model: nn.Module, inputs: torch.Tensor, dtype: Optional[str] = None):
        """
        Context manager for automatic mixed precision (AMP) during forward pass.
        This is useful for training or inference with AMP.

        Args:
            model: The model.
            inputs: Input tensors (used to determine device).
            dtype: Optional AMP dtype; if None, uses self.amp_dtype.
        """
        if not self._amp_enabled:
            # AMP disabled, just forward
            yield model, inputs
            return

        if dtype is None:
            dtype = self.amp_dtype
        if dtype not in ["fp16", "bf16"]:
            raise ValueError("AMP dtype must be 'fp16' or 'bf16'")

        device = inputs.device
        if device.type != "cuda":
            logger.warning("AMP is only supported on CUDA; falling back to normal forward")
            yield model, inputs
            return

        # Use AMP autocast
        dtype_map = {"fp16": torch.float16, "bf16": torch.bfloat16}
        amp_dtype = dtype_map[dtype]
        with torch.cuda.amp.autocast(dtype=amp_dtype, enabled=True):
            yield model, inputs

    def enable_amp(self, enable: bool = True):
        """Enable or disable automatic mixed precision."""
        self._amp_enabled = enable

    def get_energy_savings_estimate(self, original_dtype: str, new_dtype: str) -> float:
        """
        Estimate energy savings percentage when switching dtypes.
        Returns a factor (0-1) representing savings ratio.
        """
        # Rough estimates based on typical energy consumption per operation
        dtype_energy = {
            "fp32": 1.0,
            "fp16": 0.4,
            "bf16": 0.4,
            "fp8": 0.2,
            "fp4": 0.1,
        }
        orig = dtype_energy.get(original_dtype, 1.0)
        new = dtype_energy.get(new_dtype, 1.0)
        if orig == 0:
            return 0.0
        return 1.0 - (new / orig)

    def get_supported_dtypes(self) -> list:
        """Return list of supported dtype names."""
        return list(self.DTYPE_MAP.keys()) + ["fp8", "fp4"]

    def reset_original_dtype(self, model: nn.Module):
        """Remove the stored original dtype for a model."""
        if model in self._original_dtypes:
            del self._original_dtypes[model]

    def quantize_model(self, model: nn.Module, dtype: str) -> nn.Module:
        """
        Permanently convert a model to a lower precision (in-place).
        Use with caution; this will change the model's dtype permanently.
        """
        self._validate_dtype(dtype)
        converted = self._to_dtype(model, dtype)
        logger.info(f"Model quantized to {dtype}")
        return converted

    def dequantize_model(self, model: nn.Module, original_dtype: Optional[str] = None) -> nn.Module:
        """
        Restore a model to its original dtype if stored, or to fp32.
        """
        if model in self._original_dtypes:
            orig_dtype = self._original_dtypes[model]
            model.to(dtype=orig_dtype)
            logger.info(f"Model restored to {orig_dtype}")
        else:
            # Fallback to fp32
            model.to(dtype=torch.float32)
            logger.info("Model restored to fp32 (default)")
        return model

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Nothing to clean up
        pass
