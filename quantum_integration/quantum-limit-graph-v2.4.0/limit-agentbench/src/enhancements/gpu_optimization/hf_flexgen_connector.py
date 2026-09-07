#!/usr/bin/env python3
"""
Enhanced Hugging Face connector for real FlexGen execution (v2.0).
Loads a model and runs inference with a chosen policy, integrating with
block scheduling, offloading, quantization, measurement, and Green Agent modules.
Supports fallback to simulation if PyTorch/Transformers unavailable,
and provides both synchronous and asynchronous inference methods.
"""

import asyncio
import logging
import time
import os
from typing import Dict, Any, Optional, List, Tuple, Union
import random
import numpy as np

from .block_scheduler import BlockScheduler
from .quantization import apply_quantization
from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..async_message_queue import AsyncMessageQueue
from ..schemas.feedback_event import FeedbackEvent
from ..logger import logger

# Optional imports
try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

try:
    from transformers import AutoModelForCausalLM, AutoTokenizer
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False

try:
    from ..gpu_optimization.gpu_profiler import GPUProfiler
except ImportError:
    GPUProfiler = None

try:
    from ..gpu_optimization.reward import compute_reward
except ImportError:
    def compute_reward(metrics, workload):
        # Simple fallback reward based on latency and energy
        return max(0.0, min(1.0, 1.0 - metrics.get("latency_ms", 500) / 1000))


class HFFlexGenConnector:
    """
    Hugging Face connector that applies FlexGen policies to real model inference.
    Uses accelerate device_map for automatic offloading; manual block scheduling
    is simulated through BlockScheduler if real hooks are not available.
    If PyTorch/Transformers are not installed, falls back to a simulation mode
    that estimates metrics based on policy and workload.
    """

    def __init__(
        self,
        model_name: str = "facebook/opt-1.3b",
        node: Optional[NodeDescriptor] = None,
        workload: Optional[WorkloadDescriptor] = None,
        carbon_intensity_g_per_kwh: float = 400.0,
        message_queue: Optional[AsyncMessageQueue] = None,
        **kwargs
    ):
        self.model_name = model_name
        self.node = node
        self.workload = workload
        self.carbon_intensity = carbon_intensity_g_per_kwh
        self.message_queue = message_queue
        self.model = None
        self.tokenizer = None
        self.gpu_profiler = GPUProfiler() if GPUProfiler else None
        self.last_metrics = {}
        self.simulation_mode = not (TRANSFORMERS_AVAILABLE and TORCH_AVAILABLE)
        if self.simulation_mode:
            logger.warning("PyTorch/Transformers not available; using simulation mode.")
        else:
            logger.info(f"Real model inference enabled for {model_name}")

    def _get_device_map(self, policy: Dict[str, Any]) -> str:
        """
        Determine device_map for accelerate based on policy.
        """
        weight_device = policy.get("weight_device", "gpu")
        if weight_device == "cpu":
            return "cpu"
        elif weight_device == "disk":
            return "disk"
        else:
            return "auto"

    def _apply_cpu_attention(self, model, policy):
        """
        Placeholder for CPU attention replacement.
        In a real implementation, we would swap attention modules with CPU variants.
        Here we only log a warning.
        """
        if policy.get("cpu_attention", False):
            logger.warning("CPU attention requested but not fully implemented; using default attention.")
        return model

    def load_model(self, policy: Dict[str, Any], node: Optional[NodeDescriptor] = None) -> bool:
        """
        Load model and tokenizer with offloading and quantization.
        Returns True on success (or if simulation mode is used).
        """
        if self.simulation_mode:
            logger.info("Simulation mode: model loading simulated.")
            self.last_metrics['model_loaded'] = True
            return True

        if not TRANSFORMERS_AVAILABLE or not TORCH_AVAILABLE:
            logger.error("PyTorch/Transformers not available; cannot load real model.")
            return False

        try:
            logger.info(f"Loading model {self.model_name} with policy {policy}")
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)

            # Determine torch dtype based on quantization bits
            weight_bits = policy.get("weight_bits", 16)
            # Use half precision if quantizing to 8-bit or lower
            torch_dtype = torch.float16 if weight_bits <= 8 else torch.float32

            device_map = self._get_device_map(policy)

            # Load with device_map for offloading
            self.model = AutoModelForCausalLM.from_pretrained(
                self.model_name,
                device_map=device_map,
                torch_dtype=torch_dtype,
                offload_folder="./offload" if policy.get("weight_device") == "disk" else None,
            )

            # Apply quantization (if bitsandbytes available)
            try:
                self.model = apply_quantization(
                    self.model,
                    weight_bits,
                    policy.get("kv_cache_bits", 16),
                    policy.get("group_size", 64),
                )
            except Exception as e:
                logger.warning(f"Quantization failed ({e}); continuing with full precision.")
                # Fallback: do nothing (model already loaded)

            # Apply CPU attention if requested
            self.model = self._apply_cpu_attention(self.model, policy)

            logger.info("Model loaded successfully.")
            return True
        except Exception as e:
            logger.error(f"Model loading failed: {e}")
            self.model = None
            self.tokenizer = None
            # Fallback to simulation
            self.simulation_mode = True
            logger.info("Falling back to simulation mode due to load failure.")
            return False

    def _simulate_inference(self, prompt: str, policy: Dict[str, Any], max_new_tokens: int) -> Dict[str, Any]:
        """
        Generate synthetic metrics based on policy and workload, mimicking real execution.
        """
        # Simulate latency based on model size, token count, policy
        base_latency = 0.5  # seconds per token on GPU
        weight_device = policy.get("weight_device", "gpu")
        if weight_device == "cpu":
            base_latency = 2.0
        elif weight_device == "disk":
            base_latency = 3.0
        # Adjust for quantization
        bits = policy.get("weight_bits", 16)
        if bits <= 4:
            base_latency *= 0.6
        elif bits <= 8:
            base_latency *= 0.8
        # CPU attention slows down
        if policy.get("cpu_attention", False):
            base_latency *= 1.5

        latency = base_latency * max_new_tokens
        # Energy: assume average power based on device
        if weight_device == "cpu":
            power = 100  # watts
        else:
            power = 250  # GPU power
        energy = power * latency
        carbon = energy / 3.6e6 * self.carbon_intensity

        return {
            "success": True,
            "output": prompt + " [simulated]",
            "latency_ms": latency * 1000,
            "num_tokens": max_new_tokens,
            "energy_joules": energy,
            "carbon_g": carbon,
            "gpu_memory_used_mb": 0,
            "throughput_tokens_per_s": max_new_tokens / latency if latency > 0 else 0,
            "quality_score": 0.95,  # simulation slightly lower
            "policy": policy,
            "simulated": True,
        }

    def run_inference_sync(
        self,
        prompt: str,
        policy: Dict[str, Any],
        max_new_tokens: int = 20,
        use_block_scheduling: bool = False,
    ) -> Dict[str, Any]:
        """Synchronous inference (falls back to simulation if real model unavailable)."""
        if self.simulation_mode or self.model is None:
            metrics = self._simulate_inference(prompt, policy, max_new_tokens)
            self.last_metrics = metrics
            return metrics

        if self.tokenizer is None:
            logger.error("Tokenizer not loaded.")
            return {"success": False, "error": "tokenizer_not_loaded"}

        try:
            start_time = time.time()
            gpu_before = {}
            if self.gpu_profiler:
                gpu_before = self.gpu_profiler.get_gpu_metrics()

            inputs = self.tokenizer(prompt, return_tensors="pt")
            if hasattr(self.model, 'device'):
                inputs = {k: v.to(self.model.device) for k, v in inputs.items()}

            with torch.no_grad():
                outputs = self.model.generate(
                    **inputs,
                    max_new_tokens=max_new_tokens,
                    do_sample=False,
                )

            latency = time.time() - start_time
            output_text = self.tokenizer.decode(outputs[0], skip_special_tokens=True)

            gpu_after = {}
            if self.gpu_profiler:
                gpu_after = self.gpu_profiler.get_gpu_metrics()

            power_before = gpu_before.get("gpu_power_watts", 65.0)
            power_after = gpu_after.get("gpu_power_watts", 65.0)
            avg_power = (power_before + power_after) / 2
            energy = avg_power * latency
            carbon = energy / 3.6e6 * self.carbon_intensity

            metrics = {
                "success": True,
                "output": output_text,
                "latency_ms": latency * 1000,
                "num_tokens": max_new_tokens,
                "energy_joules": energy,
                "carbon_g": carbon,
                "gpu_memory_used_mb": gpu_after.get("gpu_memory_used_mb", 0),
                "throughput_tokens_per_s": max_new_tokens / latency if latency > 0 else 0,
                "quality_score": 1.0,
                "policy": policy,
                "simulated": False,
            }

            self.last_metrics = metrics
            self._publish_feedback(metrics, policy)
            return metrics

        except Exception as e:
            logger.error(f"Inference failed: {e}")
            # Fallback to simulation on error
            metrics = self._simulate_inference(prompt, policy, max_new_tokens)
            self.last_metrics = metrics
            return metrics

    async def run_inference(
        self,
        prompt: str,
        policy: Dict[str, Any],
        max_new_tokens: int = 20,
        use_block_scheduling: bool = False,
    ) -> Dict[str, Any]:
        """Asynchronous wrapper around synchronous inference."""
        # Run synchronous inference in executor to avoid blocking
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self.run_inference_sync,
            prompt,
            policy,
            max_new_tokens,
            use_block_scheduling,
        )

    def _publish_feedback(self, metrics: Dict[str, Any], policy: Dict[str, Any]):
        """Publish feedback event if message queue and FeedbackEvent available."""
        if not self.message_queue or FeedbackEvent is None:
            return
        reward = compute_reward(metrics, self.workload) if self.workload else 0.5
        event = FeedbackEvent(
            source="hf_flexgen_connector",
            feedback_type="routing",
            task_id=self.workload.task_id if self.workload else "unknown",
            context={
                "model_name": self.model_name,
                "policy": str(policy),
                "node_id": self.node.id if self.node else "unknown",
                "simulation": metrics.get("simulated", False),
            },
            action={"selected_action": str(policy), "selected_rank": 0, "confidence_score": 1.0},
            performance={
                "quality_score": metrics.get("quality_score", 0.0),
                "latency_ms": metrics.get("latency_ms", 0),
                "energy_joules": metrics.get("energy_joules", 0),
                "carbon_g": metrics.get("carbon_g", 0),
                "helium_cost": 0,
                "duration_ms": 0,
            },
            adaptive_cost_value=reward,
            tags=["hf_flexgen", "inference", "policy_execution"],
        )
        try:
            asyncio.create_task(self.message_queue.publish("inference_events", event.to_json()))
        except RuntimeError:
            asyncio.run(self.message_queue.publish("inference_events", event.to_json()))

    def close(self):
        """Clean up resources."""
        if self.gpu_profiler:
            self.gpu_profiler.shutdown()
        self.model = None
        self.tokenizer = None
