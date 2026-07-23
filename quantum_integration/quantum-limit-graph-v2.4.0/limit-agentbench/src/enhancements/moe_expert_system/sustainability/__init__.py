# sustainability/__init__.py
"""
Sustainability-Aware Model Compression and Pruning Module
Single-file drop-in for Green_Agent MoE system.
"""

import torch
import torch.nn.utils.prune as prune
from torch.quantization import quantize_dynamic
from dataclasses import dataclass, field
from typing import Optional, Any, Callable
import logging

# ==============================================
# 1. CONFIGURATION
# ==============================================
SUSTAINABILITY_CONFIG = {
    # Triggers compression if full inference energy exceeds this (Joules)
    "energy_threshold": 5.0,
    # Max allowable accuracy drop (absolute difference, e.g., 0.02 = 2%)
    "accuracy_drop_tolerance": 0.02,
    # Energy estimation coefficient (pJ per MAC operation)
    "energy_per_mac": 0.5e-12,
    # Fitness weighting
    "fitness_accuracy_weight": 0.6,
    "fitness_energy_weight": 0.4,
    # Pruning sparsity levels to try
    "pruning_sparsity": 0.3,
    "hybrid_pruning_sparsity": 0.2,
}

logger = logging.getLogger(__name__)

# ==============================================
# 2. EXPERT PROFILE EXTENSION
# ==============================================
@dataclass
class SustainabilityAwareExpertProfile:
    """
    Extended ExpertProfile with sustainability metrics.
    Attach this to your existing ExpertProfile or use it as a wrapper.
    """
    # --- Core fields (match your existing ExpertProfile) ---
    expert_id: str
    model_path: Optional[str] = None

    # --- SUSTAINABILITY EXTENSIONS ---
    compressed_flag: bool = False
    compression_method: Optional[str] = None  # "int8_quant", "pruning_30%", "hybrid"

    energy_per_inference_full: float = float('inf')  # Joules
    energy_per_inference_compressed: Optional[float] = None  # Joules

    accuracy_full: float = 0.0
    accuracy_compressed: Optional[float] = None

    # Trade-off metric
    sustainability_fitness_score: float = 0.0


# ==============================================
# 3. CORE COMPRESSOR (Quantization, Pruning, FLOPs)
# ==============================================
class SustainabilityCompressor:
    def __init__(self, model: torch.nn.Module, profile: SustainabilityAwareExpertProfile):
        self.model = model
        self.profile = profile
        self.config = SUSTAINABILITY_CONFIG

    def _estimate_energy(self, model: torch.nn.Module, sample_input: torch.Tensor) -> float:
        """
        Estimate inference energy in Joules using FLOPs.
        Falls back to a manual FLOP counter if `thop` is not installed.
        """
        try:
            from thop import profile
            flops, _ = profile(model, inputs=(sample_input,), verbose=False)
        except ImportError:
            # Manual fallback: count linear layer parameters as rough FLOPs
            flops = 0
            for module in model.modules():
                if isinstance(module, torch.nn.Linear):
                    flops += module.in_features * module.out_features
            flops = flops * 2  # approximate multiply-adds

        energy_joules = flops * self.config["energy_per_mac"]
        return energy_joules

    def apply_int8_quantization(self) -> torch.nn.Module:
        """Apply dynamic INT8 quantization to Linear layers."""
        quantized_model = quantize_dynamic(
            self.model,
            {torch.nn.Linear},
            dtype=torch.qint8
        )
        return quantized_model

    def apply_pruning(self, sparsity: float = None) -> torch.nn.Module:
        """Apply global unstructured magnitude pruning."""
        if sparsity is None:
            sparsity = self.config["pruning_sparsity"]

        parameters_to_prune = []
        for module in self.model.modules():
            if isinstance(module, torch.nn.Linear):
                parameters_to_prune.append((module, "weight"))

        prune.global_unstructured(
            parameters_to_prune,
            pruning_method=prune.L1Unstructured,
            amount=sparsity
        )
        # Make pruning permanent
        for module, _ in parameters_to_prune:
            prune.remove(module, "weight")
        return self.model

    def _evaluate_accuracy(self, model: torch.nn.Module, val_loader: Any) -> float:
        """
        Evaluate accuracy on a validation dataloader.
        Assumes classification or task-specific metrics.
        """
        model.eval()
        correct = 0
        total = 0
        with torch.no_grad():
            for inputs, labels in val_loader:
                outputs = model(inputs)
                _, predicted = torch.max(outputs.data, 1)
                total += labels.size(0)
                correct += (predicted == labels).sum().item()
        return correct / total if total > 0 else 0.0

    def evaluate_tradeoff_and_compress(self, val_loader: Any) -> bool:
        """
        Main orchestration:
        1. Measure baseline.
        2. If energy is low, skip.
        3. Test Pruning -> INT8 -> Hybrid, pick first that meets accuracy threshold.
        """
        # 1. Baseline metrics
        sample_input = next(iter(val_loader))[0]
        baseline_acc = self._evaluate_accuracy(self.model, val_loader)
        baseline_energy = self._estimate_energy(self.model, sample_input)

        self.profile.accuracy_full = baseline_acc
        self.profile.energy_per_inference_full = baseline_energy

        if baseline_energy <= self.config["energy_threshold"]:
            logger.info(f"Expert {self.profile.expert_id} energy ({baseline_energy:.2f} J) already within threshold. Skipping compression.")
            return False

        # 2. Try Pruning
        pruned_model = self.apply_pruning()
        pruned_acc = self._evaluate_accuracy(pruned_model, val_loader)
        if (baseline_acc - pruned_acc) <= self.config["accuracy_drop_tolerance"]:
            self.model = pruned_model
            self.profile.compressed_flag = True
            self.profile.compression_method = f"pruning_{int(self.config['pruning_sparsity']*100)}%"
            self.profile.accuracy_compressed = pruned_acc
            self.profile.energy_per_inference_compressed = self._estimate_energy(pruned_model, sample_input)
            return True

        # 3. Try INT8 Quantization
        quantized_model = self.apply_int8_quantization()
        quant_acc = self._evaluate_accuracy(quantized_model, val_loader)
        if (baseline_acc - quant_acc) <= self.config["accuracy_drop_tolerance"]:
            self.model = quantized_model
            self.profile.compressed_flag = True
            self.profile.compression_method = "int8_quant"
            self.profile.accuracy_compressed = quant_acc
            self.profile.energy_per_inference_compressed = self._estimate_energy(quantized_model, sample_input)
            return True

        # 4. Fallback: Hybrid (Prune + Quant)
        hybrid_model = self.apply_int8_quantization()
        hybrid_model = self.apply_pruning(sparsity=self.config["hybrid_pruning_sparsity"])
        hybrid_acc = self._evaluate_accuracy(hybrid_model, val_loader)
        if (baseline_acc - hybrid_acc) <= self.config["accuracy_drop_tolerance"]:
            self.model = hybrid_model
            self.profile.compressed_flag = True
            self.profile.compression_method = "hybrid"
            self.profile.accuracy_compressed = hybrid_acc
            self.profile.energy_per_inference_compressed = self._estimate_energy(hybrid_model, sample_input)
            return True

        # 5. If all fail, revert
        logger.warning(f"Expert {self.profile.expert_id} cannot be compressed without exceeding accuracy drop tolerance.")
        return False


# ==============================================
# 4. SUSTAINABILITY FITNESS SCORER
# ==============================================
class SustainabilityFitnessScorer:
    """
    Computes a multi-objective fitness score used by the router and optimizer.
    Higher score = better (accurate + energy-efficient).
    """
    def __init__(self):
        self.config = SUSTAINABILITY_CONFIG
        self.alpha = self.config["fitness_accuracy_weight"]
        self.beta = self.config["fitness_energy_weight"]

    def compute(self, profile: SustainabilityAwareExpertProfile) -> float:
        # Use compressed metrics if available, otherwise fallback to full
        acc = profile.accuracy_compressed if profile.compressed_flag else profile.accuracy_full
        energy = profile.energy_per_inference_compressed if profile.compressed_flag else profile.energy_per_inference_full

        # Normalize energy (assuming a reasonable max of 10J, cap at 1.0)
        normalized_energy = max(0.0, 1.0 - (energy / 10.0))

        # Fitness = Weighted Accuracy + Weighted Energy efficiency + Compression bonus
        base_fitness = (self.alpha * acc) + (self.beta * normalized_energy)

        # Small bonus for being compressed (encourages routing to efficient experts)
        compression_bonus = 0.05 if profile.compressed_flag else 0.0

        profile.sustainability_fitness_score = base_fitness + compression_bonus
        return profile.sustainability_fitness_score


# ==============================================
# 5. MLOPS PIPELINE INTEGRATION HOOK
# ==============================================
class MLOpsPipelineExtension:
    """
    Connects to your existing MLOpsPipeline.
    Automatically triggers compression when an expert's energy exceeds the threshold.
    """
    def __init__(self, pipeline: Any):
        self.pipeline = pipeline  # Reference to your main MLOpsPipeline instance

    def on_expert_registered(self, expert_id: str, model: torch.nn.Module,
                             profile: SustainabilityAwareExpertProfile,
                             val_loader: Any) -> None:
        """
        Hook to run immediately after an expert is trained/registered.
        """
        if profile.energy_per_inference_full > SUSTAINABILITY_CONFIG["energy_threshold"]:
            logger.info(f"[SUSTAINABILITY] Triggering compression for expert {expert_id}...")

            compressor = SustainabilityCompressor(model, profile)
            success = compressor.evaluate_tradeoff_and_compress(val_loader)

            if success:
                # Update the global registry with the new compressed model and profile
                self.pipeline.model_registry[expert_id] = compressor.model
                self.pipeline.profile_registry[expert_id] = profile
                logger.info(f"[SUSTAINABILITY] Expert {expert_id} compressed. "
                            f"New Energy: {profile.energy_per_inference_compressed:.4f} J "
                            f"(Drop in acc: {profile.accuracy_full - profile.accuracy_compressed:.4f})")
            else:
                logger.info(f"[SUSTAINABILITY] Expert {expert_id} remains uncompressed (trade-off not viable).")


# ==============================================
# 6. ROUTER INTEGRATION (PREFER COMPRESSED EXPERTS)
# ==============================================
class SustainabilityAwareRouter:
    """
    Extends your existing router to prefer compressed experts 
    when accuracy requirements are met.
    """
    def __init__(self, base_router: Any):
        self.base_router = base_router  # wrap the existing router

    def route(self, query: Any, required_accuracy: float = 0.90) -> Any:
        """
        Override the route method.
        Filters by accuracy, then selects the expert with the highest 
        sustainability_fitness_score.
        """
        candidates = self.base_router.get_all_experts(query)

        # 1. Filter by minimum required accuracy
        valid_candidates = []
        for exp_id, profile in candidates:
            acc = profile.accuracy_compressed if profile.compressed_flag else profile.accuracy_full
            if acc >= required_accuracy:
                valid_candidates.append((exp_id, profile))

        if not valid_candidates:
            # Fallback to original router if no candidate meets accuracy
            return self.base_router.route(query)

        # 2. Score all valid candidates
        scorer = SustainabilityFitnessScorer()
        for exp_id, profile in valid_candidates:
            scorer.compute(profile)

        # 3. Select the expert with the highest sustainability fitness score
        best_exp_id, best_profile = max(valid_candidates, key=lambda x: x[1].sustainability_fitness_score)

        # 4. Load the appropriate model version (compressed if flagged)
        if best_profile.compressed_flag:
            return self.base_router.load_compressed_model(best_exp_id)
        else:
            return self.base_router.load_full_model(best_exp_id)

# ==============================================
# 7. CONVENIENCE EXPORTS
# ==============================================
__all__ = [
    "SUSTAINABILITY_CONFIG",
    "SustainabilityAwareExpertProfile",
    "SustainabilityCompressor",
    "SustainabilityFitnessScorer",
    "MLOpsPipelineExtension",
    "SustainabilityAwareRouter",
]
