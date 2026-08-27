"""
Cost model for estimating transfer and compute costs of FlexGen policies.
Enhanced with:
- Layer-wise block scheduling (block size from policy)
- FLOP-based compute time (prefill/decode split)
- Separate KV cache for prefill and generation
- Quantization quality penalty
- Dynamic power model
- Multi-GPU support (pipeline parallelism)
- Node-aware hardware parameters
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Tuple
import math

from .flexgen_policy import FlexGenPolicy
from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor


@dataclass
class CostEstimate:
    total_latency_ms: float
    total_energy_joules: float
    total_carbon_g: float
    peak_gpu_memory_gb: float
    peak_cpu_memory_gb: float
    disk_io_gb: float
    quality_score: float  # new: estimated quality after quantization


class FlexGenCostModel:
    def __init__(
        self,
        carbon_intensity_g_per_kwh: float = 400.0,
        model_params: Optional[Dict[str, Any]] = None,
    ):
        """
        Args:
            carbon_intensity_g_per_kwh: Carbon intensity for energy-to-carbon conversion.
            model_params: Dict with model architecture (layers, hidden_dim, heads, vocab_size, params_billions).
                          If None, defaults to a 7B model.
        """
        self.carbon_intensity = carbon_intensity_g_per_kwh
        self.model_params = model_params or {
            "num_layers": 32,
            "hidden_dim": 4096,
            "num_heads": 32,
            "vocab_size": 50272,
            "params_billions": 7,
        }
        # Precompute FLOPs per token for this architecture
        self.flops_per_token = self._compute_flops_per_token()

    def _compute_flops_per_token(self) -> float:
        """Approximate FLOPs per token (forward pass) for transformer."""
        # Each layer: 2 * (attention + MLP)
        # Attention: 4 * hidden_dim^2 * seq_len? Simplified: ~4 * hidden_dim^2 per token
        # MLP: 2 * 4 * hidden_dim^2 (assuming 4x expansion)
        hidden = self.model_params["hidden_dim"]
        layers = self.model_params["num_layers"]
        # Per token per layer: ~8 * hidden^2
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
        """KV cache size in GB."""
        # For each layer: key and value, each of shape [batch, seq_len, hidden_dim]
        # Size = 2 * batch * seq_len * hidden_dim * bytes_per_elem
        return (2 * batch_size * seq_len * hidden_dim * num_layers * bytes_per_elem) / 1e9

    def estimate(
        self,
        policy: FlexGenPolicy,
        node: NodeDescriptor,
        workload: WorkloadDescriptor
    ) -> CostEstimate:
        """
        Estimate costs for a given policy on a node for a workload.
        """
        # Extract hardware specs from node metadata
        gpu_flops = node.metadata.get("gpu_flops_tflops", 30.0) * 1e12  # TFLOPS -> FLOPS
        gpu_memory_gb = node.metadata.get("gpu_memory_gb", 16.0)
        cpu_memory_gb = node.metadata.get("cpu_memory_gb", 64.0)
        gpu_cpu_bw_gbps = node.metadata.get("gpu_cpu_bandwidth_gbps", 12.0)
        disk_bw_gbps = node.metadata.get("disk_bandwidth_gbps", 2.0)
        num_gpus = node.metadata.get("num_gpus", 1)

        # Model parameters
        num_layers = self.model_params["num_layers"]
        hidden_dim = self.model_params["hidden_dim"]
        bytes_per_elem_weight = policy.weight_bits / 8
        bytes_per_elem_kv = policy.kv_cache_bits / 8
        # Model size estimation: params_billions * bytes_per_elem_weight
        # 1 billion params * bytes_per_elem = GB
        model_size_gb = self.model_params["params_billions"] * bytes_per_elem_weight

        # Workload tokens: split prefill and decode?
        # Assume all tokens are prompt; decode tokens = 32 (default)
        prompt_tokens = workload.tokens
        decode_tokens = workload.metadata.get("max_new_tokens", 32) if hasattr(workload, 'metadata') else 32

        # KV cache for prompt (prefill) and decode (generation)
        kv_cache_prompt_gb = self._compute_kv_cache_gb(
            policy, policy.gpu_batch_size, prompt_tokens, num_layers, hidden_dim, bytes_per_elem_kv
        )
        kv_cache_decode_gb = self._compute_kv_cache_gb(
            policy, policy.gpu_batch_size, decode_tokens, num_layers, hidden_dim, bytes_per_elem_kv
        )
        # Peak KV cache is max of prompt and prompt+decode
        kv_cache_gb = kv_cache_prompt_gb + kv_cache_decode_gb  # rough

        # Activation memory (rough, scales with batch and hidden_dim)
        activation_gb = policy.gpu_batch_size * hidden_dim * 0.001  # arbitrary

        # Determine placement
        weight_on_gpu = policy.weight_device == "gpu"
        kv_on_gpu = policy.kv_cache_device == "gpu"
        activation_on_gpu = policy.activation_device == "gpu"

        # Memory usage
        if num_gpus > 1:
            # Pipeline parallelism: split layers across GPUs
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

        # Transfer time per block (zig-zag schedule)
        block_size = policy.block_size
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

        # Compute FLOPs and time
        # Total tokens = prompt_tokens + decode_tokens
        total_flops = self.flops_per_token * (prompt_tokens + decode_tokens)
        # Effective FLOPS considering quantization and batch efficiency
        efficiency = 0.6 + 0.4 * min(1.0, policy.gpu_batch_size / 8.0)  # batch efficiency
        gpu_flops_effective = gpu_flops * efficiency
        if policy.weight_bits < 16:
            gpu_flops_effective *= 0.8  # lower precision may be faster
        compute_time_s = total_flops / gpu_flops_effective

        # CPU attention time if enabled (adds overhead)
        if policy.cpu_attention:
            cpu_attention_time_s = (prompt_tokens + decode_tokens) * 0.0001 * policy.gpu_batch_size
            compute_time_s += cpu_attention_time_s

        # Overlap: if overlap_io_compute, total time is max of compute and transfer
        if policy.overlap_io_compute:
            total_time_s = max(compute_time_s, transfer_time_s)
        else:
            total_time_s = compute_time_s + transfer_time_s

        # Energy model: dynamic power based on utilization
        gpu_idle_power_w = 25.0
        gpu_max_power_w = node.metadata.get("gpu_max_power_w", 250.0)
        gpu_util = min(1.0, compute_time_s / total_time_s) if total_time_s > 0 else 0.5
        gpu_power_w = gpu_idle_power_w + gpu_util * (gpu_max_power_w - gpu_idle_power_w)

        cpu_idle_power_w = 20.0
        cpu_max_power_w = 100.0
        cpu_util = min(1.0, transfer_time_s / total_time_s) if total_time_s > 0 else 0.5
        cpu_power_w = cpu_idle_power_w + cpu_util * (cpu_max_power_w - cpu_idle_power_w)

        energy_j = (gpu_power_w * compute_time_s) + (cpu_power_w * transfer_time_s)
        total_energy_joules = energy_j
        total_carbon_g = (energy_j / 3.6e6) * self.carbon_intensity

        # Quality score based on quantization
        if policy.weight_bits >= 16 and policy.kv_cache_bits >= 16:
            quality_score = 1.0
        elif policy.weight_bits >= 8 and policy.kv_cache_bits >= 8:
            quality_score = 0.95
        elif policy.weight_bits >= 4 and policy.kv_cache_bits >= 4:
            quality_score = 0.85
        else:
            quality_score = 0.7

        return CostEstimate(
            total_latency_ms=total_time_s * 1000.0,
            total_energy_joules=total_energy_joules,
            total_carbon_g=total_carbon_g,
            peak_gpu_memory_gb=peak_gpu_mem,
            peak_cpu_memory_gb=peak_cpu_mem,
            disk_io_gb=disk_io_gb,
            quality_score=quality_score,
        )
