from .config import SUSTAINABILITY_CONFIG
from .compressor import SustainabilityCompressor
from .fitness_scorer import SustainabilityFitnessScorer  # enhanced scorer
import logging

logger = logging.getLogger(__name__)

class MLOpsPipelineExtension:
    def __init__(self, pipeline, config=None, scorer=None):
        self.pipeline = pipeline
        self.config = config or SUSTAINABILITY_CONFIG
        self.scorer = scorer or SustainabilityFitnessScorer(config)

    async def on_expert_registered(self, expert_id, model, profile, val_loader):
        """Async hook to run immediately after an expert is trained/registered."""
        try:
            if profile.energy_per_inference_full > self.config.energy_threshold:
                logger.info(f"Triggering compression for expert {expert_id}")
                compressor = SustainabilityCompressor(model, profile, config=self.config)
                success = await compressor.evaluate_tradeoff_and_compress(val_loader)
                if success:
                    self.pipeline.model_registry[expert_id] = compressor.model
                    self.pipeline.profile_registry[expert_id] = profile
                    logger.info(f"Compressed expert {expert_id}")
                else:
                    logger.info(f"Expert {expert_id} remains uncompressed")
        except Exception as e:
            logger.error(f"Compression failed for {expert_id}: {e}")

class SustainabilityAwareRouter:
    def __init__(self, base_router, scorer=None):
        self.base_router = base_router
        self.scorer = scorer or SustainabilityFitnessScorer()

    def route(self, query, required_accuracy=0.90):
        candidates = self.base_router.get_all_experts(query)

        valid_candidates = []
        for exp in candidates:
            acc = exp.accuracy_compressed if exp.compressed_flag else exp.accuracy_full
            if acc >= required_accuracy:
                valid_candidates.append(exp)

        if not valid_candidates:
            return self.base_router.route(query)

        # Compute fitness scores for all valid candidates
        for exp in valid_candidates:
            self.scorer.compute(exp)

        best_expert = max(valid_candidates, key=lambda x: x.sustainability_fitness_score)

        if best_expert.compressed_flag:
            return self.base_router.load_compressed_model(best_expert.expert_id)
        else:
            return self.base_router.load_full_model(best_expert.expert_id)
