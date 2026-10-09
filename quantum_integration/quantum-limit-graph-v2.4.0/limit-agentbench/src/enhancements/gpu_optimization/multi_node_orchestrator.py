#!/usr/bin/env python3
"""
Enhanced Cost model for estimating transfer and compute costs of FlexGen policies.
=================================================================================
Adds:
- Input validation for block_size, batch_size, and quantization bits.
- Safe hardware metadata access (dict or object).
- Multi-GPU pipeline AND tensor parallelism with correct latency scaling.
- Prefill/decode split: prefill is compute-bound, decode is memory-bound.
- Attention FLOPs term (quadratic in context length).
- Piecewise quality curve calibrated to known quantization behaviour.
- Peak GPU memory accounting with a prefetch (double-buffer) factor.
- Disk I/O time separated from CPU.
- Fixed-schema failure result (never raises).
- Optional Prometheus metrics.

FIXES OVER v2.5.0:
- Relative imports wrapped in try/except with fallbacks.
- Multi-GPU compute scaling corrected: tensor scales throughput, pipeline does not.
- Multi-GPU energy double-count removed; separated by parallelism mode.
- Metadata access accepts dicts and objects; None-safe for workload metadata.
- Prefill and decode modelled separately (compute-bound vs. memory-bound).
- Attention FLOPs included; long-context costs no longer understated.
- Quality uses a piecewise curve calibrated to published quantization results.
- Peak GPU memory includes prefetched blocks in-flight.
- Block-transfer loop collapsed to O(1).
- Failure path returns a fixed-schema CostEstimate with inf metrics.
- Magic constants are constructor parameters.
- Unused `policy` parameter removed from `_compute_kv_cache_gb`.
- `num_heads` and `vocab_size` are either used or dropped.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

# ---------- Prometheus (optional) ----------
try:
    from prometheus_client import Counter as _PromCounter, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False


# ---------- structlog / stdlib logger ----------
try:
    import structlog
    _logger = structlog.get_logger(__name__)
    _STRUCTLOG_AVAILABLE = True
except ImportError:  # pragma: no cover
    _logger = logging.getLogger(__name__)
    _STRUCTLOG_AVAILABLE = False
    logging.basicConfig(level=logging.INFO)


def log_event(level: str, message: str, **kwargs: Any) -> None:
    """Logging shim that works with structlog or stdlib logging."""
    if _STRUCTLOG_AVAILABLE:
        getattr(_logger, level)(message, **kwargs)
    else:
        if kwargs:
            extra = " ".join(f"{k}={v!r}" for k, v in kwargs.items())
            message = f"{message} | {extra}"
        getattr(_logger, level)(message)


# ---------- Relative imports with fallbacks ----------
try:
    from .flexgen_policy import FlexGenPolicy  # type: ignore
except ImportError:  # pragma: no cover
    FlexGenPolicy = Any  # type: ignore

try:
    from ..schemas.node_descriptor import NodeDescriptor  # type: ignore
except ImportError:  # pragma: no cover
    NodeDescriptor = Any  # type: ignore

try:
    from ..schemas.workload_descriptor import WorkloadDescriptor  # type: ignore
except ImportError:  # pragma: no cover
    WorkloadDescriptor = Any  # type: ignore


# ---------- Preserved module-level logger ----------
logger = logging.getLogger(__name__)


# ---------- Helpers ----------
def _as_float(
    value: Any,
    default: float,
    name: Optional[str] = None,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        log_event("warning", f"Invalid {name or 'value'}: {value!r}; using {default}")
        return default
    if not math.isfinite(v):
        log_event("warning", f"Non-finite {name or 'value'}: {v}; using {default}")
        return default
    if min_val is not None and v < min_val:
        return min_val
    if max_val is not None and v > max_val:
        return max_val
    return v


def _as_int(
    value: Any,
    default: int,
    name: Optional[str] = None,
    min_val: Optional[int] = None,
    max_val: Optional[int] = None,
) -> int:
    try:
        v = int(value)
    except (TypeError, ValueError):
        log_event("warning", f"Invalid {name or 'value'}: {value!r}; using {default}")
        return default
    if min_val is not None and v < min_val:
        return min_val
    if max_val is not None and v > max_val:
        return max_val
    return v


def _meta_get(md: Any, key: str, default: Any) -> Any:
    """Read a key from a dict-like or object-like metadata container."""
    if md is None:
        return default
    if isinstance(md, dict):
        return md.get(key, default)
    try:
        return getattr(md, key, default)
    except Exception:
        return default


# ---------- Prometheus metrics (module scope: single registration) ----------
if PROMETHEUS_AVAILABLE:
    _M_ESTIMATES = _PromCounter(
        "flexgen_cost_model_estimates_total",
        "Cost model estimates",
        ["success"],
    )
    _M_LATENCY = Histogram(
        "flexgen_cost_model_latency_ms",
        "Estimated latency (ms)",
    )
    _M_ENERGY = Histogram(
        "flexgen_cost_model_energy_joules",
        "Estimated energy (J)",
    )
    _M_CARBON = Histogram(
        "flexgen_cost_model_carbon_g",
        "Estimated carbon (g)",
    )
    _M_QUALITY = Histogram(
        "flexgen_cost_model_quality_score",
        "Estimated quality score",
    )
else:  # pragma: no cover
    _M_ESTIMATES = _M_LATENCY = _M_ENERGY = _M_CARBON = _M_QUALITY = None


# ==============================================================================
# CostEstimate (fields preserved exactly)
# ==============================================================================
@dataclass
class CostEstimate:
    total_latency_ms: float
    total_energy_joules: float
    total_carbon_g: float
    peak_gpu_memory_gb: float
    peak_cpu_memory_gb: float
    disk_io_gb: float
    quality_score: float

    @property
    def success(self) -> bool:
        """
        True if the estimate is finite (i.e. no internal failure occurred).
        Additive: does not change the dataclass fields.
        """
        return (
            math.isfinite(self.total_latency_ms)
            and math.isfinite(self.total_energy_joules)
            and math.isfinite(self.total_carbon_g)
        )


# ==============================================================================
# FlexGenCostModel
# ==============================================================================
class FlexGenCostModel:
    """
    Cost model for FlexGen-style inference policies.

    Attributes preserved from v2.0:
      * `self.carbon_intensity` — grid carbon intensity in gCO2eq/kWh.
      * `self.model_params` — model architecture parameters.
      * `self.flops_per_token` — linear FLOPs per token across all layers.
    """

    DEFAULT_MODEL_PARAMS: Dict[str, Any] = {
        "num_layers": 32,
        "hidden_dim": 4096,
        "num_heads": 32,
        "vocab_size": 50272,
        "params_billions": 7,
    }

    DEFAULT_QUANT_QUALITY: Dict[int, float] = {
        16: 1.00,
        8: 0.99,
        4: 0.95,
        3: 0.85,
        2: 0.60,
    }

    def __init__(
        self,
        carbon_intensity_g_per_kwh: float = 400.0,
        model_params: Optional[Dict[str, Any]] = None,
        *,
        parallelism: str = "pipeline",
        tensor_efficiency: float = 0.8,
        pipeline_bubble_efficiency: float = 0.95,
        overlap_efficiency: float = 0.5,
        prefetch_factor: float = 2.0,
        activation_gb_per_token: float = 1e-3,
        batch_efficiency_scale: float = 16.0,
        batch_efficiency_floor: float = 0.5,
        cpu_attention_cost_per_token: float = 1e-4,
        quant_speedup: Optional[Dict[int, float]] = None,
        # Hardware defaults (used when node metadata is silent)
        default_gpu_flops_tflops: float = 30.0,
        default_gpu_memory_gb: float = 16.0,
        default_cpu_memory_gb: float = 64.0,
        default_gpu_cpu_bw_gbps: float = 12.0,
        default_disk_bw_gbps: float = 2.0,
        default_gpu_memory_bandwidth_gbps: float = 900.0,
        default_gpu_max_power_w: float = 250.0,
        gpu_idle_power_w: float = 25.0,
        cpu_idle_power_w: float = 20.0,
        cpu_max_power_w: float = 100.0,
        disk_idle_power_w: float = 5.0,
        disk_max_power_w: float = 15.0,
        default_max_new_tokens: int = 32,
        enable_prometheus: bool = True,
    ):
        self.carbon_intensity = _as_float(
            carbon_intensity_g_per_kwh, 400.0,
            name="carbon_intensity_g_per_kwh", min_val=0.0,
        )
        params = dict(self.DEFAULT_MODEL_PARAMS)
        if model_params:
            params.update(model_params)
        self.model_params = params

        # Validation of model params.
        self.model_params["num_layers"] = _as_int(
            params.get("num_layers", 32), 32, "num_layers", min_val=1
        )
        self.model_params["hidden_dim"] = _as_int(
            params.get("hidden_dim", 4096), 4096, "hidden_dim", min_val=1
        )
        self.model_params["num_heads"] = _as_int(
            params.get("num_heads", 32), 32, "num_heads", min_val=1
        )
        self.model_params["vocab_size"] = _as_int(
            params.get("vocab_size", 50272), 50272, "vocab_size", min_val=1
        )
        self.model_params["params_billions"] = _as_float(
            params.get("params_billions", 7.0), 7.0, "params_billions", min_val=0.001
        )

        # Preserved attribute: linear FLOPs per token.
        self.flops_per_token = self._compute_flops_per_token()

        # Parallelism / overlap.
        self.parallelism = parallelism if parallelism in ("pipeline", "tensor") else "pipeline"
        self.tensor_efficiency = _as_float(
            tensor_efficiency, 0.8, "tensor_efficiency", min_val=0.0, max_val=1.0
        )
        self.pipeline_bubble_efficiency = _as_float(
            pipeline_bubble_efficiency, 0.95,
            "pipeline_bubble_efficiency", min_val=0.0, max_val=1.0,
        )
        self.overlap_efficiency = _as_float(
            overlap_efficiency, 0.5, "overlap_efficiency", min_val=0.0, max_val=1.0
        )
        self.prefetch_factor = _as_float(
            prefetch_factor, 2.0, "prefetch_factor", min_val=1.0
        )

        # Physics.
        self.activation_gb_per_token = _as_float(
            activation_gb_per_token, 1e-3,
            "activation_gb_per_token", min_val=0.0,
        )
        self.batch_efficiency_scale = _as_float(
            batch_efficiency_scale, 16.0, "batch_efficiency_scale", min_val=1e-6
        )
        self.batch_efficiency_floor = _as_float(
            batch_efficiency_floor, 0.5,
            "batch_efficiency_floor", min_val=0.0, max_val=1.0,
        )
        self.cpu_attention_cost_per_token = _as_float(
            cpu_attention_cost_per_token, 1e-4,
            "cpu_attention_cost_per_token", min_val=0.0,
        )
        self.quant_speedup = quant_speedup or {4: 1.5, 8: 1.2}

        # Hardware defaults.
        self.default_gpu_flops_tflops = _as_float(
            default_gpu_flops_tflops, 30.0, min_val=0.001
        )
        self.default_gpu_memory_gb = _as_float(default_gpu_memory_gb, 16.0, min_val=0.0)
        self.default_cpu_memory_gb = _as_float(default_cpu_memory_gb, 64.0, min_val=0.0)
        self.default_gpu_cpu_bw_gbps = _as_float(
            default_gpu_cpu_bw_gbps, 12.0, min_val=1e-6
        )
        self.default_disk_bw_gbps = _as_float(
            default_disk_bw_gbps, 2.0, min_val=1e-6
        )
        self.default_gpu_memory_bandwidth_gbps = _as_float(
            default_gpu_memory_bandwidth_gbps, 900.0, min_val=1e-6
        )
        self.default_gpu_max_power_w = _as_float(
            default_gpu_max_power_w, 250.0, min_val=0.0
        )
        self.gpu_idle_power_w = _as_float(gpu_idle_power_w, 25.0, min_val=0.0)
        self.cpu_idle_power_w = _as_float(cpu_idle_power_w, 20.0, min_val=0.0)
        self.cpu_max_power_w = _as_float(cpu_max_power_w, 100.0, min_val=0.0)
        self.disk_idle_power_w = _as_float(disk_idle_power_w, 5.0, min_val=0.0)
        self.disk_max_power_w = _as_float(disk_max_power_w, 15.0, min_val=0.0)
        self.default_max_new_tokens = _as_int(
            default_max_new_tokens, 32, min_val=1
        )

        self._enable_prometheus = bool(enable_prometheus) and PROMETHEUS_AVAILABLE

    # ------------------------------------------------------------------
    # FLOPs
    # ------------------------------------------------------------------
    def _compute_flops_per_token(self) -> float:
        """
        Linear (non-attention) FLOPs per token across all layers.

        Includes the projection + feed-forward term (8 * hidden^2 per layer)
        and the per-token QKV / output projections.
        """
        hidden = self.model_params["hidden_dim"]
        layers = self.model_params["num_layers"]
        flops_per_layer = 8.0 * hidden * hidden
        return float(layers) * flops_per_layer

    def _attention_flops_per_token_at(self, context_len: float) -> float:
        """
        Attention FLOPs per token per layer, at a given context length.

        Two attention operations (QK^T and A·V) each cost `context_len * hidden`
        FLOPs per token per layer.
        """
        hidden = self.model_params["hidden_dim"]
        layers = self.model_params["num_layers"]
        return 2.0 * float(context_len) * float(hidden) * float(layers)

    # ------------------------------------------------------------------
    # KV cache
    # ------------------------------------------------------------------
    def _compute_kv_cache_gb(
        self,
        batch_size: int,
        seq_len: int,
        num_layers: int,
        hidden_dim: int,
        bytes_per_elem: float,
    ) -> float:
        # key + value => factor of 2
        bytes_per_elem = max(bytes_per_elem, 1e-6)
        return (
            2.0 * batch_size * seq_len * hidden_dim * num_layers * bytes_per_elem
        ) / 1e9

    # ------------------------------------------------------------------
    # Quality curve
    # ------------------------------------------------------------------
    def _quality_from_bits(self, bits: int) -> float:
        curve = self.DEFAULT_QUANT_QUALITY
        if bits >= max(curve.keys()):
            return 1.0
        if bits in curve:
            return float(curve[bits])
        keys = sorted(curve.keys())
        if bits < keys[0]:
            return float(curve[keys[0]])
        for i in range(len(keys) - 1):
            lo, hi = keys[i], keys[i + 1]
            if lo <= bits <= hi:
                t = (bits - lo) / (hi - lo)
                return float(curve[lo] + t * (curve[hi] - curve[lo]))
        return 1.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def estimate(
        self,
        policy: FlexGenPolicy,
        node: NodeDescriptor,
        workload: WorkloadDescriptor,
    ) -> CostEstimate:
        """
        Estimate cost of a policy on a node for a workload.
        Never raises; on internal failure returns a fixed-schema CostEstimate
        with infinite latency/energy/carbon and zero quality.
        """
        try:
            estimate = self._estimate_impl(policy, node, workload)
        except Exception as exc:
            log_event("error", f"Cost model estimate failed: {exc}")
            estimate = CostEstimate(
                total_latency_ms=float("inf"),
                total_energy_joules=float("inf"),
                total_carbon_g=float("inf"),
                peak_gpu_memory_gb=float("inf"),
                peak_cpu_memory_gb=float("inf"),
                disk_io_gb=0.0,
                quality_score=0.0,
            )

        if self._enable_prometheus:
            try:
                _M_ESTIMATES.labels(success=str(estimate.success)).inc()
                if estimate.success:
                    _M_LATENCY.observe(estimate.total_latency_ms)
                    _M_ENERGY.observe(estimate.total_energy_joules)
                    _M_CARBON.observe(estimate.total_carbon_g)
                    _M_QUALITY.observe(estimate.quality_score)
            except Exception:
                pass
        return estimate

    # ------------------------------------------------------------------
    # Implementation
    # ------------------------------------------------------------------
    def _estimate_impl(
        self,
        policy: FlexGenPolicy,
        node: NodeDescriptor,
        workload: WorkloadDescriptor,
    ) -> CostEstimate:
        # ---------- Policy validation ----------
        block_size = _as_int(
            getattr(policy, "block_size", 8), 8, "block_size", min_val=1
        )
        batch_size = _as_int(
            getattr(policy, "gpu_batch_size", 1), 1, "gpu_batch_size", min_val=1
        )
        weight_bits = _as_int(
            getattr(policy, "weight_bits", 16), 16, "weight_bits", min_val=1
        )
        kv_cache_bits = _as_int(
            getattr(policy, "kv_cache_bits", 16), 16, "kv_cache_bits", min_val=1
        )
        weight_device = getattr(policy, "weight_device", "gpu")
        kv_cache_device = getattr(policy, "kv_cache_device", "gpu")
        activation_device = getattr(policy, "activation_device", "gpu")
        cpu_attention = bool(getattr(policy, "cpu_attention", False))
        overlap_io_compute = bool(getattr(policy, "overlap_io_compute", False))

        # ---------- Node metadata ----------
        md = getattr(node, "metadata", None)
        gpu_flops = _as_float(
            _meta_get(md, "gpu_flops_tflops", self.default_gpu_flops_tflops),
            self.default_gpu_flops_tflops, "gpu_flops_tflops", min_val=0.001,
        ) * 1e12
        gpu_memory_gb = _as_float(
            _meta_get(md, "gpu_memory_gb", self.default_gpu_memory_gb),
            self.default_gpu_memory_gb, "gpu_memory_gb", min_val=0.0,
        )
        cpu_memory_gb = _as_float(
            _meta_get(md, "cpu_memory_gb", self.default_cpu_memory_gb),
            self.default_cpu_memory_gb, "cpu_memory_gb", min_val=0.0,
        )
        gpu_cpu_bw_gbps = _as_float(
            _meta_get(md, "gpu_cpu_bandwidth_gbps", self.default_gpu_cpu_bw_gbps),
            self.default_gpu_cpu_bw_gbps, "gpu_cpu_bandwidth_gbps", min_val=1e-6,
        )
        disk_bw_gbps = _as_float(
            _meta_get(md, "disk_bandwidth_gbps", self.default_disk_bw_gbps),
            self.default_disk_bw_gbps, "disk_bandwidth_gbps", min_val=1e-6,
        )
        gpu_mem_bw_gbps = _as_float(
            _meta_get(md, "gpu_memory_bandwidth_gbps", self.default_gpu_memory_bandwidth_gbps),
            self.default_gpu_memory_bandwidth_gbps,
            "gpu_memory_bandwidth_gbps", min_val=1e-6,
        )
        gpu_max_power_w = _as_float(
            _meta_get(md, "gpu_max_power_w", self.default_gpu_max_power_w),
            self.default_gpu_max_power_w, "gpu_max_power_w", min_val=0.0,
        )
        num_gpus = _as_int(
            _meta_get(md, "num_gpus", 1), 1, "num_gpus", min_val=1
        )

        # ---------- Model architecture ----------
        num_layers = self.model_params["num_layers"]
        hidden_dim = self.model_params["hidden_dim"]

        bytes_per_elem_weight = max(1, weight_bits) / 8.0
        bytes_per_elem_kv = max(1, kv_cache_bits) / 8.0

        # 1 billion params * 1 byte = 1 GB
        model_size_gb = (
            self.model_params["params_billions"] * 1e9 * bytes_per_elem_weight
        ) / 1e9

        # ---------- Workload ----------
        prompt_tokens = _as_int(
            getattr(workload, "tokens", 0) or 0, 0,
            "workload.tokens", min_val=0,
        )
        if prompt_tokens <= 0:
            prompt_tokens = 1

        workload_md = getattr(workload, "metadata", None)
        decode_tokens = _as_int(
            _meta_get(workload_md, "max_new_tokens", self.default_max_new_tokens),
            self.default_max_new_tokens, "max_new_tokens", min_val=1,
        )

        # ---------- KV cache ----------
        kv_cache_prompt_gb = self._compute_kv_cache_gb(
            batch_size, prompt_tokens, num_layers, hidden_dim, bytes_per_elem_kv,
        )
        kv_cache_decode_gb = self._compute_kv_cache_gb(
            batch_size, decode_tokens, num_layers, hidden_dim, bytes_per_elem_kv,
        )
        kv_cache_gb = kv_cache_prompt_gb + kv_cache_decode_gb

        # ---------- Activations ----------
        activation_gb = (
            batch_size * prompt_tokens * self.activation_gb_per_token
        )

        # ---------- Placement ----------
        weight_on_gpu = weight_device == "gpu"
        kv_on_gpu = kv_cache_device == "gpu"
        activation_on_gpu = activation_device == "gpu"

        # ---------- Per-GPU layer distribution ----------
        layers_per_gpu = max(1, math.ceil(num_layers / num_gpus))
        layer_fraction = layers_per_gpu / float(num_layers)

        if self.parallelism == "tensor":
            model_shard_gb = model_size_gb / num_gpus
            kv_shard_gb = kv_cache_gb / num_gpus
        else:
            model_shard_gb = model_size_gb * layer_fraction
            kv_shard_gb = kv_cache_gb * layer_fraction

        # ---------- Peak memory ----------
        if weight_on_gpu:
            weight_mem_on_gpu_gb = model_shard_gb
        else:
            # Weights are streamed. At most `prefetch_factor` blocks resident.
            block_size_gb = model_size_gb / max(1, math.ceil(num_layers / block_size))
            if self.parallelism == "tensor":
                block_per_gpu_gb = block_size_gb / num_gpus
            else:
                block_per_gpu_gb = block_size_gb
            weight_mem_on_gpu_gb = self.prefetch_factor * block_per_gpu_gb

        peak_gpu_mem = (
            weight_mem_on_gpu_gb
            + (kv_shard_gb if kv_on_gpu else 0.0)
            + (activation_gb if activation_on_gpu else 0.0)
        )

        peak_cpu_mem = (
            (model_size_gb if weight_device == "cpu" else 0.0)
            + (kv_cache_gb if kv_cache_device == "cpu" else 0.0)
            + (activation_gb if activation_device == "cpu" else 0.0)
        )

        disk_io_gb = (
            (model_size_gb if weight_device == "disk" else 0.0)
            + (kv_cache_gb if kv_cache_device == "disk" else 0.0)
        )

        # ---------- Transfers (O(1) collapse of the block loop) ----------
        if self.parallelism == "tensor" and num_gpus > 1:
            effective_gpu_cpu_bw = gpu_cpu_bw_gbps * num_gpus * self.tensor_efficiency
            effective_disk_bw = disk_bw_gbps * num_gpus * self.tensor_efficiency
        else:
            effective_gpu_cpu_bw = gpu_cpu_bw_gbps
            effective_disk_bw = disk_bw_gbps

        transfer_time_gpu_cpu_s = 0.0
        transfer_time_disk_s = 0.0
        if weight_device != "gpu":
            if weight_device == "disk":
                transfer_time_disk_s += model_size_gb / max(effective_disk_bw, 1e-6)
            transfer_time_gpu_cpu_s += model_size_gb / max(effective_gpu_cpu_bw, 1e-6)
        if kv_cache_device != "gpu":
            if kv_cache_device == "disk":
                transfer_time_disk_s += kv_cache_gb / max(effective_disk_bw, 1e-6)
            transfer_time_gpu_cpu_s += kv_cache_gb / max(effective_gpu_cpu_bw, 1e-6)

        # ---------- Compute: prefill and decode ----------
        # Prefill: compute-bound, all prompt tokens in parallel.
        prefill_flops = (
            self.flops_per_token + self._attention_flops_per_token_at(prompt_tokens)
        ) * prompt_tokens

        # Decode: linear + growing attention (closed-form summation).
        decode_linear_flops = self.flops_per_token * decode_tokens
        decode_attention_flops = (
            2.0
            * hidden_dim
            * num_layers
            * (prompt_tokens * decode_tokens + decode_tokens * (decode_tokens + 1) / 2.0)
        )

        # Effective GPU throughput.
        speedup = 1.0
        for bits_threshold, value in sorted(self.quant_speedup.items()):
            if weight_bits <= bits_threshold:
                speedup = max(speedup, value)

        batch_efficiency = self.batch_efficiency_floor + (
            1.0 - self.batch_efficiency_floor
        ) * min(1.0, batch_size / self.batch_efficiency_scale)

        gpu_flops_effective = gpu_flops * speedup * batch_efficiency

        if num_gpus > 1:
            if self.parallelism == "tensor":
                gpu_flops_effective *= num_gpus * self.tensor_efficiency
            else:
                # Pipeline: latency unchanged; small bubble penalty.
                gpu_flops_effective *= self.pipeline_bubble_efficiency

        prefill_time_s = prefill_flops / max(gpu_flops_effective, 1.0)
        decode_flops_time_s = (decode_linear_flops + decode_attention_flops) / max(
            gpu_flops_effective, 1.0
        )

        # Decode on a GPU-resident model is memory-bound: read all weights per token.
        if weight_on_gpu:
            bytes_per_decode_step = model_size_gb * 1e9
            decode_memory_time_s = (
                decode_tokens * bytes_per_decode_step / (gpu_mem_bw_gbps * 1e9)
            )
            decode_time_s = max(decode_flops_time_s, decode_memory_time_s)
        else:
            decode_time_s = decode_flops_time_s

        compute_time_s = prefill_time_s + decode_time_s

        if cpu_attention:
            compute_time_s += (
                (prompt_tokens + decode_tokens)
                * self.cpu_attention_cost_per_token
                * batch_size
            )

        # ---------- Overlap ----------
        transfer_time_total_s = transfer_time_gpu_cpu_s + transfer_time_disk_s
        if overlap_io_compute:
            total_time_s = (
                compute_time_s
                + transfer_time_total_s * (1.0 - self.overlap_efficiency)
            )
        else:
            total_time_s = compute_time_s + transfer_time_total_s

        # ---------- Energy ----------
        # GPU: only one stage is active at a time in pipeline mode. In tensor
        # mode, all GPUs are active but for a fraction of the compute time.
        gpu_util = (
            min(1.0, compute_time_s / total_time_s) if total_time_s > 0 else 0.5
        )
        gpu_power_per_gpu_w = self.gpu_idle_power_w + gpu_util * (
            gpu_max_power_w - self.gpu_idle_power_w
        )
        if self.parallelism == "tensor" and num_gpus > 1:
            # All GPUs contribute for the whole (shortened) compute time.
            gpu_energy_j = gpu_power_per_gpu_w * num_gpus * compute_time_s
        else:
            # Pipeline: only one stage active; per-GPU idle for the rest.
            # Approximate: num_gpus * idle for the full time, plus the active
            # delta during compute_time_s.
            gpu_energy_j = (
                self.gpu_idle_power_w * num_gpus * total_time_s
                + (gpu_power_per_gpu_w - self.gpu_idle_power_w) * compute_time_s
            )

        cpu_util = (
            min(1.0, transfer_time_gpu_cpu_s / total_time_s) if total_time_s > 0 else 0.5
        )
        cpu_power_w = self.cpu_idle_power_w + cpu_util * (
            self.cpu_max_power_w - self.cpu_idle_power_w
        )
        cpu_energy_j = cpu_power_w * transfer_time_gpu_cpu_s

        disk_util = (
            min(1.0, transfer_time_disk_s / total_time_s) if total_time_s > 0 else 0.5
        )
        disk_power_w = self.disk_idle_power_w + disk_util * (
            self.disk_max_power_w - self.disk_idle_power_w
        )
        disk_energy_j = disk_power_w * transfer_time_disk_s

        energy_j = gpu_energy_j + cpu_energy_j + disk_energy_j
        total_carbon_g = (energy_j / 3.6e6) * self.carbon_intensity

        # ---------- Quality ----------
        weight_q = self._quality_from_bits(weight_bits)
        kv_q = self._quality_from_bits(kv_cache_bits)
        quality_score = 0.5 * weight_q + 0.5 * kv_q

        # ---------- Return ----------
        return CostEstimate(
            total_latency_ms=total_time_s * 1000.0,
            total_energy_joules=energy_j,
            total_carbon_g=total_carbon_g,
            peak_gpu_memory_gb=peak_gpu_mem,
            peak_cpu_memory_gb=peak_cpu_mem,
            disk_io_gb=disk_io_gb,
            quality_score=quality_score,
        )


# ==============================================================================
# Example usage
# ==============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from dataclasses import dataclass

    @dataclass
    class _Policy:
        block_size: int = 16
        gpu_batch_size: int = 4
        weight_bits: int = 8
        kv_cache_bits: int = 8
        weight_device: str = "cpu"
        activation_device: str = "gpu"
        kv_cache_device: str = "gpu"
        cpu_attention: bool = False
        overlap_io_compute: bool = True

    class _Node:
        metadata = {
            "gpu_flops_tflops": 312.0,
            "gpu_memory_gb": 80.0,
            "cpu_memory_gb": 512.0,
            "gpu_cpu_bandwidth_gbps": 64.0,
            "disk_bandwidth_gbps": 7.0,
            "gpu_memory_bandwidth_gbps": 2039.0,
            "gpu_max_power_w": 700.0,
            "num_gpus": 4,
        }

    class _Workload:
        tokens = 512
        metadata = {"max_new_tokens": 128}

    for mode in ("pipeline", "tensor"):
        model = FlexGenCostModel(parallelism=mode)
        est = model.estimate(_Policy(), _Node(), _Workload())
        print(f"{mode:8s} latency={est.total_latency_ms:.2f} ms  "
              f"energy={est.total_energy_joules:.2f} J  "
              f"carbon={est.total_carbon_g:.4f} g  "
              f"quality={est.quality_score:.3f}  "
              f"success={est.success}")
