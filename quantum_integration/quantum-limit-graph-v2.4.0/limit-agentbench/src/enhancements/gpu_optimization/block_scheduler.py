#!/usr/bin/env python3
"""
Enhanced FlexGen-style block scheduler (v2.0).
Simulates layer-wise offloading and asynchronous transfer for Green Agent.
Integrates with FlexGenPolicy, NodeDescriptor, WorkloadDescriptor, cost model,
AsyncMessageQueue, FeedbackEvent, and reward computation.

Enhancements:
- Input validation for block_size and quantization bits.
- Safe metadata access.
- More realistic energy model.
- Improved overlap simulation.
- Robust error handling.
"""

import asyncio
import logging
import time
import math
import random
from typing import Dict, Any, Optional, Tuple, List

from .flexgen_policy import FlexGenPolicy
from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..async_message_queue import AsyncMessageQueue
from ..schemas.feedback_event import FeedbackEvent
from ..logger import logger

# Optional reward import
try:
    from ..gpu_optimization.reward import compute_reward
except ImportError:
    def compute_reward(metrics: Dict[str, Any], workload: WorkloadDescriptor) -> float:
        weights = {'quality': 0.3, 'throughput': 0.25, 'energy': 0.2, 'carbon': 0.15, 'memory': 0.1}
        latency_score = max(0.0, 1.0 - metrics['latency_ms'] / max(workload.latency_target, 1.0))
        energy_score = max(0.0, 1.0 - metrics['energy_joules'] / 100.0)
        carbon_score = max(0.0, 1.0 - metrics['carbon_g'] / 10.0)
        memory_score = 1.0 if metrics.get('success', True) else 0.0
        quality = metrics.get('quality_score', 0.9)
        reward = (weights['quality'] * quality +
                  weights['throughput'] * latency_score +
                  weights['energy'] * energy_score +
                  weights['carbon'] * carbon_score +
                  weights['memory'] * memory_score)
        return max(0.0, min(1.0, reward))


class BlockScheduler:
    """
    Simulates the zig-zag block schedule for a given policy and hardware.
    Produces metrics that can be used for policy evaluation and learning.
    """

    def __init__(
        self,
        policy: FlexGenPolicy,
        node: NodeDescriptor,
        workload: WorkloadDescriptor,
        carbon_intensity: float = 400.0,
        message_queue: Optional[AsyncMessageQueue] = None,
    ):
        self.policy = policy
        self.node = node
        self.workload = workload
        self.carbon_intensity = carbon_intensity
        self.message_queue = message_queue

        # Validate block_size
        if not isinstance(policy.block_size, int) or policy.block_size <= 0:
            logger.warning("Invalid block_size (%s); setting to 8.", policy.block_size)
            self.block_size = 8
        else:
            self.block_size = policy.block_size

        # Extract node capabilities (safe defaults)
        metadata = getattr(node, 'metadata', None) or {}
        self.gpu_memory_gb = float(metadata.get("gpu_memory_gb", 16.0))
        self.cpu_memory_gb = float(metadata.get("cpu_memory_gb", 64.0))
        self.gpu_cpu_bandwidth_gbps = float(metadata.get("gpu_cpu_bandwidth_gbps", 12.0))
        self.disk_bandwidth_gbps = float(metadata.get("disk_bandwidth_gbps", 2.0))

        # Model and workload parameters
        weight_bits = max(1, policy.weight_bits)  # avoid division by zero
        self.model_size_gb = 14.0 * (16.0 / weight_bits)
        self.num_layers = 32
        self.hidden_dim = 4096
        self.seq_len = 512
        self.tokens = workload.tokens if workload.tokens and workload.tokens > 0 else 1

        # KV cache size estimate
        bytes_per_elem = max(1, policy.kv_cache_bits) / 8
        # More realistic: include num_layers, batch, seq_len, hidden_dim, 2 (key+value)
        self.kv_cache_gb = (
            policy.gpu_batch_size * self.seq_len * self.hidden_dim * self.num_layers * 2 * bytes_per_elem
        ) / 1e9

        self.activation_gb_per_token = 0.0001

        self.num_blocks = max(1, math.ceil(self.num_layers / self.block_size))

        self.peak_gpu_mem_gb = 0.0
        self.peak_cpu_mem_gb = 0.0

    def _transfer_time(self, size_gb: float, src: str, dst: str) -> float:
        """Estimate transfer time between devices in seconds."""
        if src == dst or size_gb <= 0:
            return 0.0
        if (src == 'gpu' and dst == 'cpu') or (src == 'cpu' and dst == 'gpu'):
            bandwidth = self.gpu_cpu_bandwidth_gbps
        elif src == 'disk' or dst == 'disk':
            bandwidth = self.disk_bandwidth_gbps
        else:
            bandwidth = self.gpu_cpu_bandwidth_gbps
        if bandwidth <= 0:
            bandwidth = 1.0
        return size_gb / bandwidth

    def _compute_block_time(self, block_layers: int, batch_size: int) -> float:
        """Estimate compute time for a block in seconds."""
        base_flops_per_token = 7e9
        total_tokens = batch_size * self.seq_len
        flops = block_layers * base_flops_per_token * total_tokens
        # GPU throughput varies with quantization bits
        weight_bits = max(1, self.policy.weight_bits)
        gpu_throughput = 30e12 * (weight_bits / 16.0)
        compute_time = flops / gpu_throughput
        return compute_time

    def _compute_energy(self, compute_time: float, transfer_time: float, policy: FlexGenPolicy) -> float:
        """
        Dynamic power model based on device usage.
        Returns energy in joules.
        """
        # GPU power depends on weight device and compute intensity
        if policy.weight_device == 'gpu':
            gpu_power = 250.0  # high power
        elif policy.weight_device == 'cpu':
            gpu_power = 50.0
        else:
            gpu_power = 30.0  # disk offload: GPU mostly idle
        # CPU power during transfers
        cpu_power = 70.0

        energy_gpu = gpu_power * compute_time
        energy_cpu = cpu_power * transfer_time
        return energy_gpu + energy_cpu

    async def run_inference(self, inputs=None) -> Dict[str, Any]:
        """
        Simulate the full inference with block scheduling.
        Returns a dict with metrics.
        """
        start_time = time.time()

        current_gpu_mem = 0.0
        current_cpu_mem = 0.0
        total_transfer_time = 0.0
        total_compute_time = 0.0

        try:
            for block_idx in range(self.num_blocks):
                block_layers = min(self.block_size, self.num_layers - block_idx * self.block_size)
                if block_layers <= 0:
                    break

                weight_size_gb = (self.model_size_gb / self.num_layers) * block_layers

                if self.policy.weight_device == 'gpu':
                    current_gpu_mem += weight_size_gb
                elif self.policy.weight_device == 'cpu':
                    current_cpu_mem += weight_size_gb
                    total_transfer_time += self._transfer_time(weight_size_gb, 'cpu', 'gpu')
                elif self.policy.weight_device == 'disk':
                    total_transfer_time += self._transfer_time(weight_size_gb, 'disk', 'cpu')
                    current_cpu_mem += weight_size_gb
                    total_transfer_time += self._transfer_time(weight_size_gb, 'cpu', 'gpu')

                activation_gb = block_layers * self.policy.gpu_batch_size * self.activation_gb_per_token
                if self.policy.activation_device == 'gpu':
                    current_gpu_mem += activation_gb
                else:
                    current_cpu_mem += activation_gb

                kv_cache_block_gb = self.kv_cache_gb / self.num_blocks
                if self.policy.kv_cache_device == 'gpu':
                    current_gpu_mem += kv_cache_block_gb
                elif self.policy.kv_cache_device == 'cpu':
                    current_cpu_mem += kv_cache_block_gb
                    total_transfer_time += self._transfer_time(kv_cache_block_gb, 'cpu', 'gpu')
                elif self.policy.kv_cache_device == 'disk':
                    current_cpu_mem += kv_cache_block_gb
                    total_transfer_time += self._transfer_time(kv_cache_block_gb, 'disk', 'cpu')

                compute_time = self._compute_block_time(block_layers, self.policy.gpu_batch_size)
                total_compute_time += compute_time

                self.peak_gpu_mem_gb = max(self.peak_gpu_mem_gb, current_gpu_mem)
                self.peak_cpu_mem_gb = max(self.peak_cpu_mem_gb, current_cpu_mem)

                # Release activation and KV cache after block
                if self.policy.activation_device == 'gpu':
                    current_gpu_mem -= activation_gb
                if self.policy.kv_cache_device == 'gpu':
                    current_gpu_mem -= kv_cache_block_gb
                if self.policy.weight_device != 'gpu':
                    current_gpu_mem -= weight_size_gb

            success = (
                self.peak_gpu_mem_gb <= self.gpu_memory_gb and
                self.peak_cpu_mem_gb <= self.cpu_memory_gb
            )

            if self.policy.overlap_io_compute:
                # Assume 50% of transfer can overlap with compute
                overlap_ratio = 0.5
                total_time = total_compute_time + total_transfer_time * (1 - overlap_ratio)
            else:
                total_time = total_compute_time + total_transfer_time

            total_tokens = self.tokens
            throughput = total_tokens / total_time if total_time > 0 else 0.0

            energy_j = self._compute_energy(total_compute_time, total_transfer_time, self.policy)
            energy_kwh = energy_j / 3.6e6
            carbon_g = energy_kwh * self.carbon_intensity

            latency_ms = total_time * 1000.0

            metrics = {
                "success": success,
                "latency_ms": latency_ms,
                "energy_joules": energy_j,
                "carbon_g": carbon_g,
                "gpu_memory_used_gb": self.peak_gpu_mem_gb,
                "cpu_memory_used_gb": self.peak_cpu_mem_gb,
                "throughput_tokens_per_s": throughput,
                "quality_score": 0.9,
                "policy": self.policy.to_dict(),
                "num_tokens": total_tokens,
            }

            if self.message_queue:
                reward = compute_reward(metrics, self.workload)
                event = FeedbackEvent(
                    source="block_scheduler",
                    feedback_type="routing",
                    task_id=self.workload.task_id or "unknown",
                    context={
                        "node_id": self.node.id,
                        "policy": str(self.policy.to_dict()),
                        "block_size": self.block_size,
                    },
                    action={"selected_action": str(self.policy.to_dict()),
                            "selected_rank": 0,
                            "confidence_score": 1.0},
                    performance={"quality_score": metrics["quality_score"],
                                 "latency_ms": metrics["latency_ms"],
                                 "energy_joules": metrics["energy_joules"],
                                 "carbon_g": metrics["carbon_g"],
                                 "helium_cost": 0,
                                 "duration_ms": 0},
                    adaptive_cost_value=reward,
                    tags=["block_scheduler", "flexgen", "inference"],
                )
                await self.message_queue.publish("inference_events", event.to_json())

            logger.info(f"BlockScheduler inference completed: success={success}, latency={latency_ms:.2f}ms, "
                        f"energy={energy_j:.2f}J, carbon={carbon_g:.4f}g")
            return metrics

        except Exception as e:
            logger.error(f"BlockScheduler simulation failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "latency_ms": 0,
                "energy_joules": 0,
                "carbon_g": 0,
                "gpu_memory_used_gb": 0,
                "cpu_memory_used_gb": 0,
                "throughput_tokens_per_s": 0,
                "quality_score": 0,
                "policy": self.policy.to_dict(),
                "num_tokens": self.tokens,
            }
