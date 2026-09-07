#!/usr/bin/env python3
"""
Enhanced Cost model for estimating transfer and compute costs of FlexGen policies.
Adds:
- Input validation for block_size, batch size, and quantization bits.
- Safe hardware metadata access.
- Partial overlap factor for transfer/compute.
- Interpolated quality score based on bit widths.
- Energy scaling with number of GPUs.
- Robust error handling and logging.
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Tuple
import math
import logging

from .flexgen_policy import FlexGenPolicy
from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor

logger = logging.getLogger(__name__)


@dataclass
class CostEstimate:
    total_latency_ms: float
    total_energy_joules: float
    total_carbon_g: float
    peak_gpu_memory_gb: float
    peak_cpu_memory_gb: float
    disk_io_gb: float
    quality_score: float


class FlexGenCostModel:
    def __init__(
        self,
        carbon_intensity_g_per_kwh: float = 400.0,
        model_params: Optional[Dict[str, Any]] = None,
    ):
        self.carbon_intensity = carbon_intensity_g_per_kwh
        self.model_params = model_params or {
            "num_layers": 32,
            "hidden_dim": 4096,
            "num_heads": 32,
            "vocab_size": 50272,
            "params_billions": 7,
        }
        self.flops_per_token = self._compute_flops_per_token()

    def _compute_flops_per_token(self) -> float:
        hidden = self.model_params["hidden_dim"]
        layers = self.model_params["num_layers"]
        flops_per_layer = 8 * hidden * hidden
        return layers * flops_per_layer

    def _compute_kv_cache_gb(
        self,
        policy: FlexGenPolicy,
        batch_size: int,
        seq_len: int,
        num_layers: int,
        hidden_dim: int,
        bytes_per_elem: float,
    ) -> float:
        if bytes_per_elem <= 0:
            bytes_per_elem = 1e-6  # avoid zero
        return (2 * batch_size * seq_len * hidden_dim * num_layers * bytes_per_elem) / 1e9

    def estimate(
        self,
        policy: FlexGenPolicy,
        node: NodeDescriptor,
        workload: WorkloadDescriptor
    ) -> CostEstimate:
        # --------------------- Input validation ---------------------
        # Validate block_size
        block_size = getattr(policy, 'block_size', 8)
        if not isinstance(block_size, int) or block_size <= 0:
            logger.warning(f"Invalid block_size: {block_size}, using 8")
            block_size = 8

        # Validate batch size
        batch_size = getattr(policy, 'gpu_batch_size', 1)
        if batch_size <= 0:
            logger.warning(f"Invalid gpu_batch_size: {batch_size}, using 1")
            batch_size = 1

        # Validate quantization bits
        weight_bits = getattr(policy, 'weight_bits', 16)
        if weight_bits <= 0:
            logger.warning(f"Invalid weight_bits: {weight_bits}, using 16")
            weight_bits = 16
        kv_cache_bits = getattr(policy, 'kv_cache_bits', 16)
        if kv_cache_bits <= 0:
            logger.warning(f"Invalid kv_cache_bits: {kv_cache_bits}, using 16")
            kv_cache_bits = 16

        # --------------------- Hardware parameters ---------------------
        # Safe metadata access
        metadata = getattr(node, 'metadata', None) or {}
        gpu_flops = float(metadata.get("gpu_flops_tflops", 30.0)) * 1e12
        gpu_memory_gb = float(metadata.get("gpu_memory_gb", 16.0))
        cpu_memory_gb = float(metadata.get("cpu_memory_gb", 64.0))
        gpu_cpu_bw_gbps = float(metadata.get("gpu_cpu_bandwidth_gbps", 12.0))
        disk_bw_gbps = float(metadata.get("disk_bandwidth_gbps", 2.0))
        num_gpus = int(metadata.get("num_gpus", 1))
        gpu_max_power_w = float(metadata.get("gpu_max_power_w", 250.0))

        # Ensure positive bandwidths and counts
        gpu_cpu_bw_gbps = max(gpu_cpu_bw_gbps, 0.1)
        disk_bw_gbps = max(disk_bw_gbps, 0.1)
        num_gpus = max(num_gpus, 1)

        # --------------------- Model parameters ---------------------
        num_layers = self.model_params["num_layers"]
        hidden_dim = self.model_params["hidden_dim"]
        bytes_per_elem_weight = weight_bits / 8
        bytes_per_elem_kv = kv_cache_bits / 8
        model_size_gb = self.model_params["params_billions"] * bytes_per_elem_weight

        # --------------------- Workload tokens ---------------------
        prompt_tokens = workload.tokens if workload.tokens and workload.tokens > 0 else 1
        decode_tokens = int(getattr(workload, 'metadata', {}).get("max_new_tokens", 32)) if hasattr(workload, 'metadata') else 32
        if decode_tokens <= 0:
            decode_tokens = 32

        # --------------------- KV cache ---------------------
        kv_cache_prompt_gb = self._compute_kv_cache_gb(
            policy, batch_size, prompt_tokens, num_layers, hidden_dim, bytes_per_elem_kv
        )
        kv_cache_decode_gb = self._compute_kv_cache_gb(
            policy, batch_size, decode_tokens, num_layers, hidden_dim, bytes_per_elem_kv
        )
        kv_cache_gb = kv_cache_prompt_gb + kv_cache_decode_gb

        # Activation memory (rough)
        activation_gb = batch_size * hidden_dim * 0.001

        # --------------------- Placement ---------------------
        weight_on_gpu = policy.weight_device == "gpu"
        kv_on_gpu = policy.kv_cache_device == "gpu"
        activation_on_gpu = policy.activation_device == "gpu"

        # --------------------- Memory ---------------------
        if num_gpus > 1:
            layers_per_gpu = math.ceil(num_layers / num_gpus)
            model_size_per_gpu_gb = model_size_gb * (layers_per_gpu / num_layers)
            kv_cache_per_gpu_gb = kv_cache_gb * (layers_per_gpu / num_layers)
        else:
            model_size_per_gpu_gb = model_size_gb
            kv_cache_per_gpu_gb = kv_cache_gb

        peak_gpu_mem = (
            (model_size_per_gpu_gb if weight_on_gpu else 0) +
            (kv_cache_per_gpu_gb if kv_on_gpu else 0) +
            (activation_gb if activation_on_gpu else 0)
        )
        peak_cpu_mem = (
            (model_size_gb if policy.weight_device == "cpu" else 0) +
            (kv_cache_gb if policy.kv_cache_device == "cpu" else 0) +
            (activation_gb if policy.activation_device == "cpu" else 0)
        )
        disk_io_gb = (
            (model_size_gb if policy.weight_device == "disk" else 0) +
            (kv_cache_gb if policy.kv_cache_device == "disk" else 0)
        )

        # --------------------- Transfer time ---------------------
        num_blocks = max(1, math.ceil(num_layers / block_size))
        block_model_size_gb = model_size_gb / num_blocks
        block_kv_cache_gb = kv_cache_gb / num_blocks

        transfer_time_s = 0.0
        for _ in range(num_blocks):
            if policy.weight_device != "gpu":
                if policy.weight_device == "disk":
                    transfer_time_s += block_model_size_gb / disk_bw_gbps
                transfer_time_s += block_model_size_gb / gpu_cpu_bw_gbps
            if policy.kv_cache_device != "gpu":
                if policy.kv_cache_device == "disk":
                    transfer_time_s += block_kv_cache_gb / disk_bw_gbps
                transfer_time_s += block_kv_cache_gb / gpu_cpu_bw_gbps

        # --------------------- Compute time ---------------------
        total_tokens = prompt_tokens + decode_tokens
        total_flops = self.flops_per_token * total_tokens

        # Batch efficiency (0.5 to 1.0)
        batch_efficiency = 0.5 + 0.5 * min(1.0, batch_size / 16.0)

        # Quantization speedup
        if weight_bits <= 4:
            speedup = 1.5
        elif weight_bits <= 8:
            speedup = 1.2
        else:
            speedup = 1.0

        gpu_flops_effective = gpu_flops * speedup * batch_efficiency
        if num_gpus > 1:
            gpu_flops_effective *= num_gpus * 0.8  # 80% scaling efficiency

        compute_time_s = total_flops / gpu_flops_effective if gpu_flops_effective > 0 else 0.0

        if policy.cpu_attention:
            cpu_attention_time_s = total_tokens * 0.0001 * batch_size
            compute_time_s += cpu_attention_time_s

        # --------------------- Total time (overlap) ---------------------
        if policy.overlap_io_compute:
            # Assume 50% of transfer can overlap with compute
            overlap_ratio = 0.5
            total_time_s = compute_time_s + transfer_time_s * (1 - overlap_ratio)
        else:
            total_time_s = compute_time_s + transfer_time_s

        # --------------------- Energy ---------------------
        gpu_idle_power_w = 25.0
        gpu_util = min(1.0, compute_time_s / total_time_s) if total_time_s > 0 else 0.5
        gpu_power_w = gpu_idle_power_w + gpu_util * (gpu_max_power_w - gpu_idle_power_w)
        # Scale by number of GPUs (assuming all active)
        gpu_power_w *= num_gpus

        cpu_idle_power_w = 20.0
        cpu_max_power_w = 100.0
        cpu_util = min(1.0, transfer_time_s / total_time_s) if total_time_s > 0 else 0.5
        cpu_power_w = cpu_idle_power_w + cpu_util * (cpu_max_power_w - cpu_idle_power_w)

        energy_j = (gpu_power_w * compute_time_s) + (cpu_power_w * transfer_time_s)
        total_energy_joules = energy_j
        total_carbon_g = (energy_j / 3.6e6) * self.carbon_intensity

        # --------------------- Quality score (interpolated) ---------------------
        # Linear interpolation between worst (4-bit) and best (16-bit)
        weight_q = max(0.0, min(1.0, weight_bits / 16.0))
        kv_q = max(0.0, min(1.0, kv_cache_bits / 16.0))
        quality_score = 0.5 * weight_q + 0.5 * kv_q

        return CostEstimate(
            total_latency_ms=total_time_s * 1000.0,
            total_energy_joules=total_energy_joules,
            total_carbon_g=total_carbon_g,
            peak_gpu_memory_gb=peak_gpu_mem,
            peak_cpu_memory_gb=peak_cpu_mem,
            disk_io_gb=disk_io_gb,
            quality_score=quality_score,
        )
