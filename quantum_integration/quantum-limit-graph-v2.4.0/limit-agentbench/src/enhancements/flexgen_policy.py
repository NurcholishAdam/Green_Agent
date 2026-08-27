"""
FlexGen‑style Policy and Mock Executor for Green Agent GPU Orchestration.
Enhanced with validation, serialization helpers, node‑aware simulation,
grid/heuristic candidate generation, and quality scoring.

This module defines the policy space (batch size, device placement, quantization)
and a lightweight simulator that returns realistic metrics for a given policy,
hardware node, and workload.
"""

from dataclasses import dataclass, asdict, field
from typing import Dict, List, Any, Optional, Tuple
import math
import random

from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor


# Allowed values for fields
ALLOWED_DEVICES = {"gpu", "cpu", "disk"}
ALLOWED_ACTIVATION_DEVICES = {"gpu", "cpu"}
ALLOWED_BITS = {4, 8, 16}


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

    def __post_init__(self):
        """Validate field values."""
        if self.weight_device not in ALLOWED_DEVICES:
            raise ValueError(f"weight_device must be one of {ALLOWED_DEVICES}")
        if self.activation_device not in ALLOWED_ACTIVATION_DEVICES:
            raise ValueError(f"activation_device must be one of {ALLOWED_ACTIVATION_DEVICES}")
        if self.kv_cache_device not in ALLOWED_DEVICES:
            raise ValueError(f"kv_cache_device must be one of {ALLOWED_DEVICES}")
        if self.weight_bits not in ALLOWED_BITS:
            raise ValueError(f"weight_bits must be one of {ALLOWED_BITS}")
        if self.kv_cache_bits not in ALLOWED_BITS:
            raise ValueError(f"kv_cache_bits must be one of {ALLOWED_BITS}")
        if self.gpu_batch_size < 1:
            raise ValueError("gpu_batch_size must be >= 1")
        if self.block_size < 1:
            raise ValueError("block_size must be >= 1")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FlexGenPolicy":
        return cls(**data)

    def to_vector(self) -> List[float]:
        """
        Return a normalized numeric vector representing the policy.
        Used by drift detection and MoE router.
        """
        return [
            self.gpu_batch_size / 8.0,
            self.block_size / 64.0,
            1.0 if self.weight_device == "gpu" else 0.0,
            1.0 if self.weight_device == "cpu" else 0.0,
            1.0 if self.weight_device == "disk" else 0.0,
            1.0 if self.activation_device == "gpu" else 0.0,
            1.0 if self.activation_device == "cpu" else 0.0,
            1.0 if self.kv_cache_device == "gpu" else 0.0,
            1.0 if self.kv_cache_device == "cpu" else 0.0,
            1.0 if self.kv_cache_device == "disk" else 0.0,
            self.weight_bits / 16.0,
            self.kv_cache_bits / 16.0,
            1.0 if self.cpu_attention else 0.0,
            1.0 if self.overlap_io_compute else 0.0,
        ]


class MockFlexGenExecutor:
    """
    Simulates execution of a FlexGen policy on a given node and workload.
    Produces metrics: memory, latency, throughput, energy, carbon, quality, success.
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
        # Extract node specs with defaults
        gpu_memory_gb = node.metadata.get("gpu_memory_gb", 16.0)
        cpu_memory_gb = node.metadata.get("cpu_memory_gb", 64.0)
        gpu_flops_tflops = node.metadata.get("gpu_flops_tflops", 30.0)  # TFLOPS
        gpu_cpu_bw_gbps = node.metadata.get("gpu_cpu_bandwidth_gbps", 12.0)
        disk_bw_gbps = node.metadata.get("disk_bandwidth_gbps", 2.0)
        gpu_max_power_w = node.metadata.get("gpu_max_power_w", 250.0)
        cpu_max_power_w = node.metadata.get("cpu_max_power_w", 100.0)

        # Model parameters (could be overridden by workload metadata)
        model_params = workload.metadata.get("model_params", {})
        num_layers = model_params.get("num_layers", 32)
        hidden_dim = model_params.get("hidden_dim", 4096)
        params_billions = model_params.get("params_billions", 7)
        seq_len_prompt = workload.tokens
        # Decode length default
        max_new_tokens = workload.metadata.get("max_new_tokens", 32)

        # Weight size calculation
        bytes_per_weight = policy.weight_bits / 8
        model_size_gb = params_billions * bytes_per_weight

        # KV cache size (more accurate)
        bytes_per_kv = policy.kv_cache_bits / 8
        # Prefill KV cache: batch * prompt_len * 2 * hidden * layers * bytes
        kv_prompt_gb = (policy.gpu_batch_size * seq_len_prompt * 2 * hidden_dim * num_layers * bytes_per_kv) / 1e9
        # Decode KV cache: batch * decode_len * 2 * hidden * layers * bytes
        kv_decode_gb = (policy.gpu_batch_size * max_new_tokens * 2 * hidden_dim * num_layers * bytes_per_kv) / 1e9
        kv_cache_gb = kv_prompt_gb + kv_decode_gb

        # Activation memory (rough, scale with batch and hidden)
        activation_gb = policy.gpu_batch_size * hidden_dim * 0.0001  # ~0.4 MB per 4096 hidden? adjust

        # Determine memory placement
        weight_on_gpu = policy.weight_device == "gpu"
        kv_on_gpu = policy.kv_cache_device == "gpu"
        activation_on_gpu = policy.activation_device == "gpu"

        peak_gpu_mem_gb = (
            (model_size_gb if weight_on_gpu else 0) +
            (kv_cache_gb if kv_on_gpu else 0) +
            (activation_gb if activation_on_gpu else 0)
        )
        peak_cpu_mem_gb = (
            (model_size_gb if policy.weight_device == "cpu" else 0) +
            (kv_cache_gb if policy.kv_cache_device == "cpu" else 0) +
            (activation_gb if policy.activation_device == "cpu" else 0)
        )
        disk_io_gb = (
            (model_size_gb if policy.weight_device == "disk" else 0) +
            (kv_cache_gb if policy.kv_cache_device == "disk" else 0)
        )

        # Success/OOM check
        success = (peak_gpu_mem_gb <= gpu_memory_gb) and (peak_cpu_mem_gb <= cpu_memory_gb)

        # Compute transfer times (simple, ignoring block scheduling for now)
        transfer_time_s = 0.0
        if policy.weight_device != "gpu":
            if policy.weight_device == "disk":
                transfer_time_s += model_size_gb / disk_bw_gbps
            transfer_time_s += model_size_gb / gpu_cpu_bw_gbps
        if policy.kv_cache_device != "gpu":
            if policy.kv_cache_device == "disk":
                transfer_time_s += kv_cache_gb / disk_bw_gbps
            transfer_time_s += kv_cache_gb / gpu_cpu_bw_gbps

        # Compute time (rough FLOPs estimation)
        # FLOPs per token per layer ~ 8 * hidden_dim^2 (simplified)
        flops_per_token = num_layers * 8 * hidden_dim * hidden_dim
        total_tokens = seq_len_prompt + max_new_tokens
        total_flops = flops_per_token * total_tokens * policy.gpu_batch_size
        # Effective FLOPS considering batch efficiency and quantization speedup
        batch_efficiency = 0.6 + 0.4 * min(1.0, policy.gpu_batch_size / 8.0)
        gpu_flops = gpu_flops_tflops * 1e12 * batch_efficiency
        if policy.weight_bits < 16:
            gpu_flops *= 0.9  # slight speedup with lower precision
        compute_time_s = total_flops / gpu_flops if gpu_flops > 0 else 0.0

        # CPU attention overhead if enabled
        if policy.cpu_attention:
            # Additional time for CPU-side attention
            cpu_attention_time = (total_tokens * policy.gpu_batch_size) * 0.0001  # arbitrary
            compute_time_s += cpu_attention_time

        # Total time with overlap
        if policy.overlap_io_compute:
            total_time_s = max(compute_time_s, transfer_time_s)
        else:
            total_time_s = compute_time_s + transfer_time_s

        latency_ms = total_time_s * 1000.0
        throughput_tokens_per_s = (total_tokens * policy.gpu_batch_size) / total_time_s if total_time_s > 0 else 0.0

        # Energy model: dynamic power based on utilization
        gpu_util = min(1.0, compute_time_s / total_time_s) if total_time_s > 0 else 0.5
        cpu_util = min(1.0, transfer_time_s / total_time_s) if total_time_s > 0 else 0.5
        gpu_power_w = 20.0 + gpu_util * (gpu_max_power_w - 20.0)  # idle 20W, max from node
        cpu_power_w = 10.0 + cpu_util * (cpu_max_power_w - 10.0)
        energy_j = gpu_power_w * compute_time_s + cpu_power_w * transfer_time_s

        # Carbon
        energy_kwh = energy_j / 3.6e6
        carbon_g = energy_kwh * self.carbon_intensity

        # Quality score based on quantization
        quality = 1.0
        if policy.weight_bits <= 4:
            quality *= 0.85
        elif policy.weight_bits <= 8:
            quality *= 0.95
        if policy.kv_cache_bits <= 4:
            quality *= 0.9
        elif policy.kv_cache_bits <= 8:
            quality *= 0.97

        return {
            "success": success,
            "gpu_memory_used_gb": peak_gpu_mem_gb,
            "cpu_memory_used_gb": peak_cpu_mem_gb,
            "disk_io_gb": disk_io_gb,
            "latency_ms": latency_ms,
            "throughput_tokens_per_s": throughput_tokens_per_s,
            "energy_joules": energy_j,
            "carbon_g": carbon_g,
            "quality_score": quality,
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


def generate_grid_policies() -> List[FlexGenPolicy]:
    """
    Generate a systematic grid of policies covering key combinations.
    This provides a diverse but structured set for initial exploration.
    """
    policies = []
    # Sweep over batch size and weight device, fix other parameters for simplicity
    for batch in [1, 2, 4, 8]:
        for weight_device in ["gpu", "cpu", "disk"]:
            for kv_device in ["gpu", "cpu", "disk"]:
                for weight_bits in [4, 8, 16]:
                    # Limit combinations to avoid explosion; sample a subset
                    if random.random() < 0.3:  # keep ~30%
                        continue
                    policies.append(FlexGenPolicy(
                        gpu_batch_size=batch,
                        block_size=16,
                        weight_device=weight_device,
                        activation_device="gpu",
                        kv_cache_device=kv_device,
                        weight_bits=weight_bits,
                        kv_cache_bits=weight_bits,  # same for simplicity
                        cpu_attention=False,
                        overlap_io_compute=True,
                    ))
    return policies


def generate_heuristic_policies() -> List[FlexGenPolicy]:
    """
    Generate policies based on simple heuristics (e.g., for large models offload weights).
    """
    policies = []
    # Low latency: everything on GPU
    policies.append(FlexGenPolicy(
        gpu_batch_size=2,
        block_size=16,
        weight_device="gpu",
        activation_device="gpu",
        kv_cache_device="gpu",
        weight_bits=16,
        kv_cache_bits=16,
        cpu_attention=False,
        overlap_io_compute=True,
    ))
    # Low memory: weights on CPU, KV on CPU
    policies.append(FlexGenPolicy(
        gpu_batch_size=4,
        block_size=32,
        weight_device="cpu",
        activation_device="cpu",
        kv_cache_device="cpu",
        weight_bits=8,
        kv_cache_bits=8,
        cpu_attention=True,
        overlap_io_compute=True,
    ))
    # High throughput: offload to disk, overlap
    policies.append(FlexGenPolicy(
        gpu_batch_size=8,
        block_size=64,
        weight_device="disk",
        activation_device="gpu",
        kv_cache_device="disk",
        weight_bits=4,
        kv_cache_bits=4,
        cpu_attention=False,
        overlap_io_compute=True,
    ))
    # Balanced: weights CPU, KV GPU
    policies.append(FlexGenPolicy(
        gpu_batch_size=4,
        block_size=32,
        weight_device="cpu",
        activation_device="gpu",
        kv_cache_device="gpu",
        weight_bits=8,
        kv_cache_bits=8,
        cpu_attention=False,
        overlap_io_compute=True,
    ))
    return policies


# Optional convenience function to compute reward from metrics
def compute_reward(metrics: Dict[str, Any], workload: WorkloadDescriptor) -> float:
    """
    Compute reward from metrics using Green Agent's standard weights.
    Can be replaced by shared reward function if available.
    """
    latency_score = max(0.0, 1.0 - metrics['latency_ms'] / max(workload.latency_target, 1.0))
    energy_score = max(0.0, 1.0 - metrics['energy_joules'] / 100.0)
    carbon_score = max(0.0, 1.0 - metrics['carbon_g'] / 10.0)
    success_bonus = 1.0 if metrics.get('success', False) else 0.0
    quality = metrics.get('quality_score', 0.9)
    reward = (0.3 * quality +
              0.25 * latency_score +
              0.2 * energy_score +
              0.15 * carbon_score +
              0.1 * success_bonus)
    return max(0.0, min(1.0, reward))
