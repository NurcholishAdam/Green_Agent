import torch
import torch.nn.utils.prune as prune
from torch.quantization import quantize_dynamic
from thop import profile  # FLOPs counter
from .config import SUSTAINABILITY_CONFIG

class SustainabilityCompressor:
    def __init__(self, model, expert_profile):
        self.model = model
        self.profile = expert_profile
        self.accuracy_threshold = SUSTAINABILITY_CONFIG["accuracy_drop_tolerance"]  # e.g., 0.02
        self.energy_threshold = SUSTAINABILITY_CONFIG["energy_per_inference_limit"] # e.g., 5.0 J

    def estimate_energy(self, model, sample_input):
        """Estimate energy in Joules using FLOPs (approx 0.5 pJ per MAC)."""
        flops, _ = profile(model, inputs=(sample_input,), verbose=False)
        energy_joules = flops * 0.5e-12  # Convert pJ to Joules
        return energy_joules

    def apply_int8_quantization(self):
        """Apply dynamic INT8 quantization."""
        # Note: Use dynamic for LLM/MoE experts to avoid calibration data overhead
        quantized_model = quantize_dynamic(
            self.model, 
            {torch.nn.Linear}, 
            dtype=torch.qint8
        )
        return quantized_model

    def apply_pruning(self, sparsity=0.3):
        """Apply global magnitude pruning to reduce FLOPs."""
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

    def evaluate_tradeoff_and_compress(self, val_loader):
        """Main orchestration method."""
        # 1. Measure baseline
        baseline_acc = self._evaluate_accuracy(self.model, val_loader)
        baseline_energy = self.estimate_energy(self.model, next(iter(val_loader))[0])
        self.profile.accuracy_full = baseline_acc
        self.profile.energy_per_inference_full = baseline_energy

        # 2. Skip if energy is already acceptable
        if baseline_energy <= self.energy_threshold:
            return False  # No compression needed

        # 3. Attempt Pruning first (FLOPs reduction)
        pruned_model = self.apply_pruning(sparsity=0.3)
        pruned_acc = self._evaluate_accuracy(pruned_model, val_loader)
        pruned_energy = self.estimate_energy(pruned_model, next(iter(val_loader))[0])

        # 4. If pruning meets threshold, keep it
        if (baseline_acc - pruned_acc) <= self.accuracy_threshold:
            self.model = pruned_model
            self.profile.compressed_flag = True
            self.profile.compression_method = "pruning_30%"
            self.profile.accuracy_compressed = pruned_acc
            self.profile.energy_per_inference_compressed = pruned_energy
            return True

        # 5. Else, attempt INT8 Quantization (usually more accurate than pruning)
        quantized_model = self.apply_int8_quantization()
        quant_acc = self._evaluate_accuracy(quantized_model, val_loader)
        quant_energy = self.estimate_energy(quantized_model, next(iter(val_loader))[0])

        if (baseline_acc - quant_acc) <= self.accuracy_threshold:
            self.model = quantized_model
            self.profile.compressed_flag = True
            self.profile.compression_method = "int8_quant"
            self.profile.accuracy_compressed = quant_acc
            self.profile.energy_per_inference_compressed = quant_energy
            return True

        # 6. Fallback: Apply both (Prune + Quant) if individual failed
        hybrid_model = self.apply_int8_quantization()
        hybrid_model = self.apply_pruning(sparsity=0.2) # lower sparsity to save acc
        hybrid_acc = self._evaluate_accuracy(hybrid_model, val_loader)
        
        if (baseline_acc - hybrid_acc) <= self.accuracy_threshold:
            self.model = hybrid_model
            self.profile.compressed_flag = True
            self.profile.compression_method = "hybrid"
            self.profile.accuracy_compressed = hybrid_acc
            self.profile.energy_per_inference_compressed = self.estimate_energy(hybrid_model, ...)
            return True
        
        # 7. If all fail, revert to original
        return False

    def _evaluate_accuracy(self, model, loader):
        # Standard evaluation loop
        pass
