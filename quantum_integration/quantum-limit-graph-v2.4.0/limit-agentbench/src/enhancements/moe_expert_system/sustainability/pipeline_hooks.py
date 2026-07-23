from .compressor import SustainabilityCompressor

class MLOpsPipelineExtension:
    def __init__(self, pipeline):
        self.pipeline = pipeline  # Reference to your main MLOpsPipeline

    def on_expert_registered(self, expert_id, model, profile, val_loader):
        """Hook to run immediately after an expert is trained/registered."""
        # Check if energy is a problem
        if profile.energy_per_inference_full > SUSTAINABILITY_CONFIG["energy_threshold"]:
            print(f"[SUSTAINABILITY] Triggering compression for expert {expert_id}...")
            
            compressor = SustainabilityCompressor(model, profile)
            success = compressor.evaluate_tradeoff_and_compress(val_loader)
            
            if success:
                # Update the global registry with the new compressed model
                self.pipeline.model_registry[expert_id] = compressor.model
                self.pipeline.profile_registry[expert_id] = profile
                print(f"[SUSTAINABILITY] Expert {expert_id} compressed. New Energy: {profile.energy_per_inference_compressed:.4f} J")
            else:
                print(f"[SUSTAINABILITY] Expert {expert_id} cannot be compressed without violating accuracy threshold.")
