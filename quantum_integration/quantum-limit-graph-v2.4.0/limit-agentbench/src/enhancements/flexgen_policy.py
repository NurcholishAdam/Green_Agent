"""
FlexGen‑style Policy and Mock Executor for Green Agent GPU Orchestration.

This module defines the policy space (batch size, device placement, quantization)
and a lightweight simulator that returns realistic metrics for a given policy,
hardware node, and workload. It is intended to be replaced later by a real
FlexGen backend.
"""

from dataclasses import dataclass, asdict
from typing import Dict, List, Any, Optional
import math
import random

from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor


@dataclass
class FlexGenPolicy:
    """Policy variables controlling offloading, batching, and quantization."""
    gpu_batch_size: int = 1
    block_size: int = 16
    weight_device: str = "gpu"          # "gpu", "cpu", "disk"
    activation_device: str = "gpu"      # "gpu", "cpu"
    kv_cache_device: str = "gpu"        # "gpu", "cpu", "disk"
    weight_bits: int = 16               # 4, 8, 16
    kv_cache_bits: int = 16             # 4, 8, 16
    cpu_attention: bool = False
    overlap_io_compute: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class MockFlexGenExecutor:
    """
    Simulates execution of a FlexGen policy on a given node and workload.
    Returns a dictionary of metrics including memory, latency, throughput,
    energy, carbon, and success flag.
    """

    def __init__(self, carbon_intensity_g_per_kwh: float = 400.0):
        self.carbon_intensity = carbon_intensity_g_per_kwh

    def execute(
        self,
        policy: FlexGenPolicy,
        node: NodeDescriptor,
        workload: WorkloadDescriptor
    ) -> Dict[str, Any]:
        """Return simulated performance metrics for the policy."""
        # Model size estimate from workload (tokens not directly used here)
        model_size_gb = workload.tokens * 0.000001  # rough: 1 token = 1 MB? adjust later
        # For simulation, assume a typical 7B model ~14 GB in fp16
        model_size_gb = 14.0

        # KV cache size estimation (simplified)
        # batch_size * sequence_length * hidden_dim * layers * bytes_per_element
        # For a 7B model, hidden_dim=4096, layers=32, bytes = bits/8
        seq_len = 512  # assume prompt length
        bytes_per_elem = policy.kv_cache_bits / 8
        kv_cache_gb = (policy.gpu_batch_size * seq_len * 4096 * 32 * 2 * bytes_per_elem) / 1e9
        # For demonstration, use a simple formula
        kv_cache_gb = policy.gpu_batch_size * 0.2  # ~200 MB per batch

        # Determine peak memory usage
        weight_mem = model_size_gb * (1 if policy.weight_device == "gpu" else 0)
        kv_mem = kv_cache_gb * (1 if policy.kv_cache_device == "gpu" else 0)
        activation_mem = policy.gpu_batch_size * 0.05 * (1 if policy.activation_device == "gpu" else 0)
        total_gpu_mem_gb = weight_mem + kv_mem + activation_mem

        # Assume node has GPU memory from metadata; default 16 GB
        gpu_mem_gb = node.metadata.get("gpu_memory_gb", 16.0)
        cpu_mem_gb = node.metadata.get("cpu_memory_gb", 64.0)

        # Simulate OOM if exceeds GPU memory
        success = total_gpu_mem_gb <= gpu_mem_gb and policy.weight_device != "disk"  # disk still needs some GPU

        # Latency (ms) estimate: base + offloading penalty
        base_latency = 200.0
        offload_penalty = 0.0
        if policy.weight_device == "cpu":
            offload_penalty += 80.0
        if policy.kv_cache_device == "cpu":
            offload_penalty += 40.0
        if policy.cpu_attention:
            offload_penalty += 30.0
        if policy.overlap_io_compute:
            offload_penalty *= 0.5

        # Throughput (tokens/s) inversely related to latency
        latency = base_latency + offload_penalty
        throughput = 1000.0 / latency * policy.gpu_batch_size

        # Energy (J) estimate: GPU power * time + CPU power * time
        gpu_power_w = 70.0 if policy.weight_device == "gpu" else 20.0
        cpu_power_w = 30.0
        time_sec = latency / 1000.0
        energy_j = (gpu_power_w + cpu_power_w) * time_sec

        # Carbon (g) = energy_kwh * carbon_intensity
        energy_kwh = energy_j / 3.6e6
        carbon_g = energy_kwh * self.carbon_intensity

        return {
            "success": success,
            "gpu_memory_used_gb": total_gpu_mem_gb,
            "latency_ms": latency,
            "throughput_tokens_per_s": throughput,
            "energy_joules": energy_j,
            "carbon_g": carbon_g,
            "policy": policy.to_dict(),
        }


def generate_candidate_policies(n: int = 20) -> List[FlexGenPolicy]:
    """Generate a diverse set of candidate policies via random sampling."""
    policies = []
    for _ in range(n):
        policy = FlexGenPolicy(
            gpu_batch_size=random.choice([1, 2, 4, 8]),
            block_size=random.choice([8, 16, 32, 64]),
            weight_device=random.choice(["gpu", "cpu", "disk"]),
            activation_device=random.choice(["gpu", "cpu"]),
            kv_cache_device=random.choice(["gpu", "cpu", "disk"]),
            weight_bits=random.choice([4, 8, 16]),
            kv_cache_bits=random.choice([4, 8, 16]),
            cpu_attention=random.random() < 0.3,
            overlap_io_compute=random.random() < 0.7,
        )
        policies.append(policy)
    return policies
