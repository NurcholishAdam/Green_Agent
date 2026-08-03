"""
Mixed precision utilities for energy-efficient inference.
"""
import torch
import torch.nn as nn
from contextlib import contextmanager

class MixedPrecisionEngine:
    def __init__(self, default_dtype: str = "fp16"):
        self.default_dtype = default_dtype
        self.supported = {"fp16", "fp8", "fp4"}

    @contextmanager
    def quantized_forward(self, model: nn.Module, dtype: str = "fp8"):
        """
        Context manager to run forward pass in low precision.
        """
        if dtype not in self.supported:
            raise ValueError(f"Unsupported dtype {dtype}")
        original_dtype = next(model.parameters()).dtype
        try:
            # Convert model to specified dtype
            model.to(dtype=getattr(torch, dtype))
            yield
        finally:
            model.to(original_dtype)
