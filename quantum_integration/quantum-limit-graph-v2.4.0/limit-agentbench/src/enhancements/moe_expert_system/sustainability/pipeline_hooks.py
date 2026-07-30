# mlops_extension.py
"""
Enhanced MLOps pipeline extension for sustainability‑aware compression and routing.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Callable

from .config import SUSTAINABILITY_CONFIG, SustainabilityConfig
from .compressor import SustainabilityCompressor
from .fitness_scorer import SustainabilityFitnessScorer
from .history import CompressionHistoryManager
from .storage import CompressedModelStorage

logger = logging.getLogger(__name__)


class MLOpsPipelineExtension:
    """
    Integrates sustainability‑aware compression into an ML pipeline.
    Supports async registration, periodic re‑compression, and anomaly‑triggered compression.
    """

    def __init__(
        self,
        pipeline: Any,
        config: Optional[SustainabilityConfig] = None,
        scorer: Optional[SustainabilityFitnessScorer] = None,
        telemetry: Optional[Any] = None,
        carbon_manager: Optional[Any] = None,
        anomaly_detector: Optional[Any] = None,
        accuracy_fn: Optional[Callable[[torch.nn.Module, Any], float]] = None,
    ):
        """
        Args:
            pipeline: An object with attributes:
                - model_registry: dict mapping expert_id to model
                - profile_registry: dict mapping expert_id to SustainabilityAwareExpertProfile
                - val_loaders: optional dict mapping expert_id to validation DataLoader
            config: SustainabilityConfig instance (default: SUSTAINABILITY_CONFIG)
            scorer: SustainabilityFitnessScorer instance (if None, a new one is created)
            telemetry: Optional telemetry collector (e.g., for Prometheus metrics)
            carbon_manager: Optional carbon intensity manager
            anomaly_detector: Optional anomaly detector for trigger callbacks
            accuracy_fn: Optional custom accuracy evaluation function
        """
        self.pipeline = pipeline
        self.config = config or SUSTAINABILITY_CONFIG
        self.telemetry = telemetry
        self.carbon_manager = carbon_manager
        self.anomaly_detector = anomaly_detector
        self.accuracy_fn = accuracy_fn

        # Create scorer if not provided
        self.scorer = scorer or SustainabilityFitnessScorer(config, telemetry=telemetry)

        # History and storage managers
        self.history_manager = CompressionHistoryManager(
            db_path=self.config.history_db_path,
            retention_days=self.config.history_retention_days
        )
        self.storage = CompressedModelStorage(self.config.compressed_model_dir)

        # Background tasks
        self._running = False
        self._recompress_task: Optional[asyncio.Task] = None

        # Lock for registry updates
        self._lock = asyncio.Lock()

        # Cache for compressed accuracies (for rollback monitoring)
        self._compressed_acc_cache: Dict[str, float] = {}

        # Ensure pipeline has required attributes
        self._ensure_pipeline()

    def _ensure_pipeline(self):
        """Validate that the pipeline has the expected attributes."""
        if not hasattr(self.pipeline, 'model_registry'):
            raise AttributeError("Pipeline must have 'model_registry' attribute")
        if not hasattr(self.pipeline, 'profile_registry'):
            raise AttributeError("Pipeline must have 'profile_registry' attribute")
        if not hasattr(self.pipeline, 'val_loaders'):
            self.pipeline.val_loaders = {}

    async def on_expert_registered(
        self,
        expert_id: str,
        model: torch.nn.Module,
        profile: SustainabilityAwareExpertProfile,
        val_loader: Any,
    ) -> None:
        """
        Async hook to run immediately after an expert is trained/registered.

        If the expert's energy exceeds the threshold, compression is triggered.
        If a compressed model exists on disk, it is loaded instead.
        """
        self._ensure_pipeline()

        # Update material index based on hardware profile
        profile.update_material_index(self.config)

        # Check if a compressed version exists on disk
        if self.storage:
            latest = self.history_manager.get_latest(expert_id)
            if latest:
                method = latest['method']
                if self.storage.load(expert_id, method, model):
                    async with self._lock:
                        profile.compressed_flag = True
                        profile.compression_method = method
                        profile.accuracy_compressed = latest['accuracy_after']
                        profile.energy_per_inference_compressed = latest['energy_after']
                        profile.carbon_savings_kg = latest['carbon_savings_kg']
                        profile.last_compressed_at = datetime.fromisoformat(latest['timestamp'])
                        self.pipeline.model_registry[expert_id] = model
                        self.pipeline.profile_registry[expert_id] = profile
                        self._compressed_acc_cache[expert_id] = profile.accuracy_compressed
                    logger.info(f"Loaded compressed model for expert {expert_id} (method: {method})")
                    return

        # Trigger compression if energy exceeds threshold
        if profile.energy_per_inference_full > self.config.energy_threshold_joules:
            logger.info(f"[SUSTAINABILITY] Triggering compression for expert {expert_id}...")
            compressor = SustainabilityCompressor(
                model=model,
                profile=profile,
                config=self.config,
                telemetry=self.telemetry,
                carbon_manager=self.carbon_manager,
                history_manager=self.history_manager,
                storage=self.storage,
                accuracy_fn=self.accuracy_fn,
            )
            # Get a sample input from the validation loader
            try:
                sample_input = next(iter(val_loader))[0]
            except (StopIteration, TypeError, IndexError) as e:
                logger.error(f"Could not extract sample input from val_loader for {expert_id}: {e}")
                return

            try:
                success = await compressor.evaluate_tradeoff_and_compress(val_loader, sample_input)
            except Exception as e:
                logger.error(f"Compression failed for {expert_id}: {e}")
                return

            if success:
                async with self._lock:
                    self.pipeline.model_registry[expert_id] = compressor.model
                    self.pipeline.profile_registry[expert_id] = profile
                    self._compressed_acc_cache[expert_id] = profile.accuracy_compressed
                logger.info(f"Compressed expert {expert_id}")
            else:
                logger.info(f"Expert {expert_id} remains uncompressed")
        else:
            logger.info(
                f"Expert {expert_id} energy ({profile.energy_per_inference_full:.2f} J) "
                f"within threshold. No compression."
            )

    # ---------- Background tasks ----------
    async def start_recompress_loop(self):
        """Start the periodic re‑compression background task."""
        if self.config.recompress_interval_seconds <= 0:
            logger.info("Re‑compression disabled (interval <= 0).")
            return
        self._running = True
        async def loop():
            while self._running:
                await asyncio.sleep(self.config.recompress_interval_seconds)
                await self._recompress_all()
        self._recompress_task = asyncio.create_task(loop())
        logger.info("Started periodic re‑compression loop.")

    async def stop_recompress_loop(self):
        """Stop the periodic re‑compression background task."""
        self._running = False
        if self._recompress_task:
            self._recompress_task.cancel()
            try:
                await self._recompress_task
            except asyncio.CancelledError:
                pass
            self._recompress_task = None
            logger.info("Stopped periodic re‑compression loop.")

    async def _recompress_all(self):
        """Re‑evaluate compression for all experts in the pipeline."""
        self._ensure_pipeline()
        for expert_id, model in list(self.pipeline.model_registry.items()):
            profile = self.pipeline.profile_registry.get(expert_id)
            if profile is None:
                continue
            val_loader = self.pipeline.val_loaders.get(expert_id)
            if val_loader is None:
                continue
            compressor = SustainabilityCompressor(
                model=model,
                profile=profile,
                config=self.config,
                telemetry=self.telemetry,
                carbon_manager=self.carbon_manager,
                history_manager=self.history_manager,
                storage=self.storage,
                accuracy_fn=self.accuracy_fn,
            )
            try:
                sample_input = next(iter(val_loader))[0]
            except Exception as e:
                logger.error(f"Could not get sample input for {expert_id}: {e}")
                continue
            success = await compressor.evaluate_tradeoff_and_compress(val_loader, sample_input)
            if success:
                async with self._lock:
                    self.pipeline.model_registry[expert_id] = compressor.model
                    self.pipeline.profile_registry[expert_id] = profile
                    self._compressed_acc_cache[expert_id] = profile.accuracy_compressed

    # ---------- Anomaly‑triggered compression ----------
    async def on_anomaly_detected(self, node_id: str, metrics: Dict):
        """
        Callback from AnomalyDetector. Triggers compression for experts on the affected node.
        """
        if not self.config.enable_anomaly_trigger:
            return
        self._ensure_pipeline()
        async with self._lock:
            for expert_id, profile in self.pipeline.profile_registry.items():
                if profile.node_id == node_id:
                    model = self.pipeline.model_registry.get(expert_id)
                    if model is None:
                        continue
                    compressor = SustainabilityCompressor(
                        model=model,
                        profile=profile,
                        config=self.config,
                        telemetry=self.telemetry,
                        carbon_manager=self.carbon_manager,
                        history_manager=self.history_manager,
                        storage=self.storage,
                        accuracy_fn=self.accuracy_fn,
                    )
                    val_loader = self.pipeline.val_loaders.get(expert_id)
                    if val_loader:
                        try:
                            sample_input = next(iter(val_loader))[0]
                        except Exception as e:
                            logger.error(f"Could not get sample input for {expert_id}: {e}")
                            continue
                        success = await compressor.evaluate_tradeoff_and_compress(val_loader, sample_input)
                        if success:
                            self.pipeline.model_registry[expert_id] = compressor.model
                            self.pipeline.profile_registry[expert_id] = profile
                            self._compressed_acc_cache[expert_id] = profile.accuracy_compressed
                    break  # assume one expert per node for simplicity

    # ---------- Rollback monitoring ----------
    async def monitor_rollback(self, expert_id: str, current_accuracy: float):
        """
        If the compressed expert's accuracy falls below a threshold, revert to the full model.
        Should be called periodically by the pipeline.
        """
        if expert_id not in self._compressed_acc_cache:
            return
        compressed_acc = self._compressed_acc_cache[expert_id]
        if compressed_acc == 0:
            return
        if current_accuracy < compressed_acc * self.config.accuracy_drop_tolerance:
            logger.warning(
                f"Expert {expert_id} accuracy {current_accuracy:.4f} dropped below "
                f"{compressed_acc*self.config.accuracy_drop_tolerance:.4f}. Reverting to full model."
            )
            profile = self.pipeline.profile_registry.get(expert_id)
            if profile and not profile.compressed_flag:
                logger.info(f"Expert {expert_id} already full model.")
                return
            if hasattr(self.pipeline, 'full_models'):
                full_model = self.pipeline.full_models.get(expert_id)
                if full_model is not None:
                    async with self._lock:
                        self.pipeline.model_registry[expert_id] = full_model
                        profile.compressed_flag = False
                        profile.accuracy_compressed = None
                        profile.energy_per_inference_compressed = None
                    logger.info(f"Reverted expert {expert_id} to full model.")
                else:
                    logger.error(f"No full model available for expert {expert_id} to revert.")

    # ---------- Health check ----------
    async def health_check(self) -> Dict[str, Any]:
        """Return health status of the extension."""
        return {
            "status": "healthy",
            "running": self._running,
            "recompress_task_active": self._recompress_task is not None and not self._recompress_task.done(),
            "num_experts": len(self.pipeline.model_registry),
            "compressed_count": sum(
                1 for p in self.pipeline.profile_registry.values() if p.compressed_flag
            ),
            "config_version": self.config.version,
        }


# ==============================================
# SustainabilityAwareRouter (enhanced)
# ==============================================

class SustainabilityAwareRouter:
    """
    Router that selects the most sustainable expert based on fitness score,
    while respecting a minimum accuracy requirement.
    """

    def __init__(
        self,
        base_router: Any,
        scorer: Optional[SustainabilityFitnessScorer] = None,
        default_required_accuracy: float = 0.90,
    ):
        """
        Args:
            base_router: Object with methods:
                - get_all_experts(query) -> list of (expert_id, profile)
                - load_compressed_model(expert_id) -> model
                - load_full_model(expert_id) -> model
            scorer: SustainabilityFitnessScorer instance (if None, a new one is created)
            default_required_accuracy: Default minimum accuracy for routing
        """
        self.base_router = base_router
        self.scorer = scorer or SustainabilityFitnessScorer()
        self.default_required_accuracy = default_required_accuracy

    def route(
        self,
        query: Any,
        required_accuracy: Optional[float] = None,
    ) -> Any:
        """
        Route the query to the best expert based on sustainability fitness.

        Args:
            query: The input query to route.
            required_accuracy: Minimum accuracy requirement. If None, uses default.

        Returns:
            The selected model (compressed or full).
        """
        required = required_accuracy if required_accuracy is not None else self.default_required_accuracy

        # Get all candidates from the base router
        try:
            candidates = self.base_router.get_all_experts(query)
        except Exception as e:
            logger.error(f"Failed to get candidates from base router: {e}")
            # Fallback to base router's default routing
            return self.base_router.route(query)

        # Filter by accuracy
        valid = []
        for exp_id, profile in candidates:
            acc = profile.accuracy_compressed if profile.compressed_flag else profile.accuracy_full
            if acc >= required:
                valid.append((exp_id, profile))

        if not valid:
            # No candidate meets accuracy requirement; fallback to base router
            return self.base_router.route(query)

        # Compute fitness scores for all valid candidates
        for _, profile in valid:
            self.scorer.compute(profile)

        # Select the candidate with the highest fitness score
        best_id, best_profile = max(valid, key=lambda x: x[1].sustainability_fitness_score)

        # Load the appropriate model
        if best_profile.compressed_flag:
            return self.base_router.load_compressed_model(best_id)
        else:
            return self.base_router.load_full_model(best_id)
