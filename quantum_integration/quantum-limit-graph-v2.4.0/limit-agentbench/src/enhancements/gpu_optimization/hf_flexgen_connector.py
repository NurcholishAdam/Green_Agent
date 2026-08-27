"""
Hugging Face connector for real FlexGen execution.
Loads a model and runs inference with a chosen policy.
Currently a stub that can be extended to use block scheduling and offloading.
"""

import logging
from typing import Dict, Any, Optional

from .block_scheduler import BlockScheduler
from .quantization import apply_quantization
from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..logger import logger


class HFFlexGenConnector:
    def __init__(self, model_name: str = "facebook/opt-1.3b"):
        self.model_name = model_name
        self.model = None
        self.tokenizer = None

    def load_model(self, policy: Dict[str, Any], node: NodeDescriptor):
        """Load model and tokenizer, optionally applying offloading."""
        try:
            from transformers import AutoModelForCausalLM, AutoTokenizer
            logger.info(f"Loading model {self.model_name} with policy {policy}")
            # In a real system, use device_map="auto" or manual placement
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            self.model = AutoModelForCausalLM.from_pretrained(
                self.model_name,
                device_map="auto",  # let accelerate handle offloading
                torch_dtype="auto",
            )
            # Apply quantization
            self.model = apply_quantization(self.model, policy.get("weight_bits", 16))
            return True
        except Exception as e:
            logger.error(f"Model loading failed: {e}")
            return False

    def run_inference(self, prompt: str, policy: Dict[str, Any], max_new_tokens: int = 20) -> Dict:
        """Run inference with the model; returns metrics."""
        if self.model is None or self.tokenizer is None:
            logger.error("Model not loaded.")
            return {}
        try:
            scheduler = BlockScheduler(policy)
            inputs = self.tokenizer(prompt, return_tensors="pt")
            # In a real system, would use block scheduling and measure metrics
            import time
            start = time.time()
            outputs = self.model.generate(**inputs, max_new_tokens=max_new_tokens)
            latency = time.time() - start
            return {
                "output": self.tokenizer.decode(outputs[0], skip_special_tokens=True),
                "latency_ms": latency * 1000,
                "num_tokens": max_new_tokens,
                "success": True,
            }
        except Exception as e:
            logger.error(f"Inference failed: {e}")
            return {"success": False, "error": str(e)}
