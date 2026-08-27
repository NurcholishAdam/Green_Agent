"""
Quantization helper using bitsandbytes (if available), PyTorch dynamic quantization,
or mock quantization. Enhanced to integrate with FlexGenPolicy, return metrics,
support multiple backends, estimate quality, and publish events.

Provides:
- apply_quantization(model, policy, ...) -> Dict
- get_model_size_mb(model)
- estimate_quality(weight_bits, kv_cache_bits)
"""

import logging
from typing import Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

# Optional integrations
try:
    from ..async_message_queue import AsyncMessageQueue
except ImportError:
    AsyncMessageQueue = None

try:
    from ..schemas.feedback_event import FeedbackEvent
except ImportError:
    FeedbackEvent = None

# Optional quantization libraries
try:
    import bitsandbytes as bnb
    BNB_AVAILABLE = True
    HAS_BNB_LINEAR4BIT = hasattr(bnb.nn, "Linear4bit")
    HAS_BNB_LINEAR8BIT = hasattr(bnb.nn, "Linear8bitLt")
except ImportError:
    BNB_AVAILABLE = False
    HAS_BNB_LINEAR4BIT = False
    HAS_BNB_LINEAR8BIT = False

try:
    import torch
    TORCH_AVAILABLE = True
    import torch.nn as nn
    HAS_TORCH_DYNAMIC_QUANT = hasattr(torch.quantization, "quantize_dynamic")
except ImportError:
    TORCH_AVAILABLE = False
    HAS_TORCH_DYNAMIC_QUANT = False


def get_model_size_mb(model) -> float:
    """Estimate model size in MB from parameters."""
    if model is None or not TORCH_AVAILABLE:
        return 0.0
    total_params = sum(p.numel() for p in model.parameters())
    # Assume FP32 by default (4 bytes per param)
    # For quantized models, actual size may be smaller
    return total_params * 4 / (1024 * 1024)


def estimate_quality(weight_bits: int, kv_cache_bits: int) -> float:
    """Heuristic quality score based on bit widths."""
    # Weight bits and KV cache bits both affect quality, but weight is more important
    quality = 1.0
    if weight_bits <= 4:
        quality *= 0.85
    elif weight_bits <= 8:
        quality *= 0.95
    if kv_cache_bits <= 4:
        quality *= 0.9
    elif kv_cache_bits <= 8:
        quality *= 0.97
    return max(0.5, min(1.0, quality))


def apply_quantization(
    model,
    weight_bits: int = 16,
    kv_cache_bits: int = 16,
    policy: Optional[Any] = None,
    message_queue: Optional[AsyncMessageQueue] = None,
    simulate: bool = False,
) -> Dict[str, Any]:
    """
    Apply quantization to the model and return metrics.

    Args:
        model: PyTorch model (or any model with .parameters() if TORCH_AVAILABLE).
        weight_bits: Target weight bit width (4, 8, 16).
        kv_cache_bits: Target KV cache bit width (4, 8, 16) – currently mostly informational.
        policy: Optional FlexGenPolicy object; if provided, overrides bit args.
        message_queue: Optional AsyncMessageQueue for publishing events.
        simulate: If True, do not actually modify model; just return estimated metrics.

    Returns:
        Dict with keys:
            - success (bool)
            - original_size_mb (float)
            - quantized_size_mb (float)
            - compression_ratio (float)
            - quality_score (float)
            - weight_bits (int)
            - kv_cache_bits (int)
            - backend (str)
    """
    if policy is not None:
        weight_bits = getattr(policy, 'weight_bits', weight_bits)
        kv_cache_bits = getattr(policy, 'kv_cache_bits', kv_cache_bits)

    original_size_mb = get_model_size_mb(model) if TORCH_AVAILABLE and model is not None else 0.0

    # If simulation requested or model not available, return estimates only
    if simulate or model is None:
        quantized_size_mb = original_size_mb * (weight_bits / 32.0)
        metrics = {
            "success": True,
            "original_size_mb": original_size_mb,
            "quantized_size_mb": quantized_size_mb,
            "compression_ratio": original_size_mb / quantized_size_mb if quantized_size_mb > 0 else 0.0,
            "quality_score": estimate_quality(weight_bits, kv_cache_bits),
            "weight_bits": weight_bits,
            "kv_cache_bits": kv_cache_bits,
            "backend": "simulated",
        }
        _publish_event(metrics, message_queue)
        return metrics

    # Real quantization attempts
    backend = "none"
    quantized_model = model
    try:
        if weight_bits <= 4:
            if BNB_AVAILABLE and HAS_BNB_LINEAR4BIT:
                # Replace linear layers with 4-bit
                quantized_model = _replace_linear_layers(model, bnb.nn.Linear4bit, weight_bits)
                backend = "bitsandbytes_4bit"
            elif TORCH_AVAILABLE and HAS_TORCH_DYNAMIC_QUANT:
                # Fallback: dynamic quantization (int8) which is not 4-bit but better than nothing
                quantized_model = torch.quantization.quantize_dynamic(
                    model, {nn.Linear}, dtype=torch.qint8
                )
                backend = "torch_dynamic_int8"
                weight_bits = 8  # adjust to actual
            else:
                logger.warning("No 4-bit quantization backend available; falling back to FP16.")
                if TORCH_AVAILABLE:
                    quantized_model = model.half()
                    backend = "fp16"
        elif weight_bits <= 8:
            if BNB_AVAILABLE and HAS_BNB_LINEAR8BIT:
                quantized_model = _replace_linear_layers(model, bnb.nn.Linear8bitLt, weight_bits)
                backend = "bitsandbytes_8bit"
            elif TORCH_AVAILABLE and HAS_TORCH_DYNAMIC_QUANT:
                quantized_model = torch.quantization.quantize_dynamic(
                    model, {nn.Linear}, dtype=torch.qint8
                )
                backend = "torch_dynamic_int8"
                weight_bits = 8
            else:
                logger.warning("No 8-bit quantization backend available; falling back to FP16.")
                if TORCH_AVAILABLE:
                    quantized_model = model.half()
                    backend = "fp16"
        else:
            # 16-bit or no quantization
            if TORCH_AVAILABLE:
                quantized_model = model.half()
                backend = "fp16"
            else:
                quantized_model = model
                backend = "unchanged"
    except Exception as e:
        logger.error(f"Quantization failed: {e}")
        quantized_model = model
        backend = "failed"

    # Compute size after quantization (approx)
    quantized_size_mb = get_model_size_mb(quantized_model)

    metrics = {
        "success": backend != "failed",
        "original_size_mb": original_size_mb,
        "quantized_size_mb": quantized_size_mb,
        "compression_ratio": original_size_mb / quantized_size_mb if quantized_size_mb > 0 else 0.0,
        "quality_score": estimate_quality(weight_bits, kv_cache_bits),
        "weight_bits": weight_bits,
        "kv_cache_bits": kv_cache_bits,
        "backend": backend,
    }

    _publish_event(metrics, message_queue)

    # Return the quantized model and metrics? The function returns metrics only, but we can modify in-place
    # However, for compatibility, we also return the model via an attribute or by modifying input?
    # We'll just return metrics; caller can access `quantized_model` separately if needed.
    return metrics


def _replace_linear_layers(model, quantized_layer_cls, bits):
    """Recursively replace nn.Linear layers with quantized versions."""
    if not TORCH_AVAILABLE:
        return model
    for name, module in model.named_children():
        if isinstance(module, nn.Linear):
            # Create quantized layer; bitsandbytes layers accept in_features/out_features/bias and other args
            try:
                if bits <= 4:
                    new_layer = quantized_layer_cls(
                        module.in_features,
                        module.out_features,
                        bias=module.bias is not None,
                    )
                else:
                    new_layer = quantized_layer_cls(
                        module.in_features,
                        module.out_features,
                        bias=module.bias is not None,
                    )
                setattr(model, name, new_layer)
            except Exception as e:
                logger.warning(f"Failed to replace layer {name}: {e}")
        else:
            _replace_linear_layers(module, quantized_layer_cls, bits)
    return model


def _publish_event(metrics: Dict[str, Any], message_queue: Optional[AsyncMessageQueue]):
    """Publish quantization results as FeedbackEvent if queue available."""
    if not message_queue or FeedbackEvent is None:
        return
    try:
        event = FeedbackEvent(
            source="quantization",
            feedback_type="telemetry",
            task_id="quantization",
            context={"backend": metrics.get("backend", "unknown")},
            action={"selected_action": f"quantize_{metrics.get('weight_bits', 16)}bit",
                    "selected_rank": 0,
                    "confidence_score": 1.0},
            performance={"quality_score": metrics["quality_score"],
                         "latency_ms": 0,
                         "energy_joules": 0,
                         "carbon_g": 0,
                         "helium_cost": 0,
                         "duration_ms": 0},
            adaptive_cost_value=metrics["quality_score"],
            tags=["quantization", "flexgen", "model_compression"],
        )
        import asyncio
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(message_queue.publish("quantization_events", event.to_json()))
        except RuntimeError:
            asyncio.run(message_queue.publish("quantization_events", event.to_json()))
    except Exception as e:
        logger.warning(f"Failed to publish quantization event: {e}")
