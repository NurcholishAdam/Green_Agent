"""
Cost model for estimating transfer and compute costs of FlexGen policies.
Used to pre-filter candidates without executing them.
"""

from dataclasses import dataclass
from typing import Dict, Any
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


class FlexGenCostModel:
    def __init__(self, carbon_intensity_g_per_kwh: float = 400.0):
        self.carbon_intensity = carbon_intensity_g_per_kwh

    def estimate(
        self,
        policy: FlexGenPolicy,
        node: NodeDescriptor,
        workload: WorkloadDescriptor
    ) -> CostEstimate:
        """
        Simplified analytical cost model.

        Model size approximated from workload (tokens as rough proxy).
        In a real system, this would come from model metadata.
        """
        # Model weights size (GB) - assume 7B model ~14GB in fp16
        model_size_gb = 14.0 * (16 / policy.weight_bits)  # scale by bits

        # KV cache size per token per layer, rough
        seq_len = 512
        hidden_dim = 4096
        layers = 32
        bytes_per_elem = policy.kv_cache_bits / 8
        kv_cache_gb = (
            policy.gpu_batch_size * seq_len * hidden_dim * layers * 2 * bytes_per_elem
        ) / 1e9

        # Activation memory (rough)
        activation_gb = policy.gpu_batch_size * 0.05

        # Compute where each object resides
        weight_on_gpu = policy.weight_device == "gpu"
        kv_on_gpu = policy.kv_cache_device == "gpu"
        activation_on_gpu = policy.activation_device == "gpu"

        peak_gpu_mem = (
            (model_size_gb if weight_on_gpu else 0) +
            (kv_cache_gb if kv_on_gpu else 0) +
            (activation_gb if activation_on_gpu else 0)
        )
        peak_cpu_mem = (
            (model_size_gb if policy.weight_device == "cpu" else 0) +
            (kv_cache_gb if policy.kv_cache_device == "cpu" else 0) +
            (activation_gb if policy.activation_device == "cpu" else 0)
        )
        # Disk usage (if weights or KV on disk)
        disk_io_gb = (
            (model_size_gb if policy.weight_device == "disk" else 0) +
            (kv_cache_gb if policy.kv_cache_device == "disk" else 0)
        )

        # Transfer bandwidths (GB/s) from node metadata (default)
        gpu_cpu_bw = node.metadata.get("gpu_cpu_bandwidth_gbps", 12.0)
        cpu_disk_bw = node.metadata.get("disk_bandwidth_gbps", 2.0)

        # Transfer time: moving weights and KV from non-GPU to GPU during compute
        transfer_time_s = 0.0
        if policy.weight_device != "gpu":
            transfer_time_s += model_size_gb / gpu_cpu_bw
        if policy.kv_cache_device != "gpu":
            transfer_time_s += kv_cache_gb / gpu_cpu_bw
        if policy.weight_device == "disk":
            transfer_time_s += model_size_gb / cpu_disk_bw
        if policy.kv_cache_device == "disk":
            transfer_time_s += kv_cache_gb / cpu_disk_bw

        # Compute time: rough estimate (tokens/s)
        base_tokens_per_s = 50.0  # depends on hardware
        if policy.cpu_attention:
            base_tokens_per_s *= 0.5
        compute_time_s = (workload.tokens / base_tokens_per_s) / policy.gpu_batch_size

        # Overlap: if overlap_io_compute, effective time is max(compute, transfer)
        if policy.overlap_io_compute:
            total_time_s = max(compute_time_s, transfer_time_s)
        else:
            total_time_s = compute_time_s + transfer_time_s

        total_latency_ms = total_time_s * 1000.0

        # Energy: GPU power * compute_time + CPU power * transfer_time
        gpu_power_w = 70.0 if policy.weight_device == "gpu" else 20.0
        cpu_power_w = 30.0
        energy_j = (gpu_power_w * compute_time_s) + (cpu_power_w * transfer_time_s)
        total_energy_joules = energy_j
        total_carbon_g = (energy_j / 3.6e6) * self.carbon_intensity

        return CostEstimate(
            total_latency_ms=total_latency_ms,
            total_energy_joules=total_energy_joules,
            total_carbon_g=total_carbon_g,
            peak_gpu_memory_gb=peak_gpu_mem,
            peak_cpu_memory_gb=peak_cpu_mem,
            disk_io_gb=disk_io_gb,
        )
