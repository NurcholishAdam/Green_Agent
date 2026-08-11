# mlops_extension.py
"""
Enhanced MLOps pipeline extension for sustainability‑aware compression and routing.
Includes MOPD support: exposes Pareto fronts, provides retrieval methods,
and optionally uses Pareto‑aware routing.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Callable

from .config import SUSTAINABILITY_CONFIG, SustainabilityConfig
from .compressor import SustainabilityCompressor
from .fitness_scorer import SustainabilityFitnessScorer
from .history import CompressionHistoryManager
from .storage import CompressedModelStorage
from .profiles import SustainabilityAwareExpertProfile, MOPDPoint  # assuming these are imported

logger = logging.getLogger(__name__)


class MLOpsPipelineExtension:
    """
    Integrates sustainability‑aware compression into an ML pipeline.
    Supports async registration, periodic re‑compression, anomaly‑triggered compression,
    and MOPD (Multi‑Objective Pareto Decision) front exposure.
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

        self.scorer = scorer or SustainabilityFitnessScorer(config, telemetry=telemetry)

        self.history_manager = CompressionHistoryManager(
            db_path=self.config.history_db_path,
            retention_days=self.config.history_retention_days
        )
        self.storage = CompressedModelStorage(self.config.compressed_model_dir)

        # Background tasks
        self._running = False
        self._recompress_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

        # Cache for compressed accuracies and Pareto fronts
        self._compressed_acc_cache: Dict[str, float] = {}
        self._pareto_fronts: Dict[str, List[MOPDPoint]] = {}   # NEW

        self._ensure_pipeline()

    def _ensure_pipeline(self):
        if not hasattr(self.pipeline, 'model_registry'):
            raise AttributeError("Pipeline must have 'model_registry' attribute")
        if not hasattr(self.pipeline, 'profile_registry'):
            raise AttributeError("Pipeline must have 'profile_registry' attribute")
        if not hasattr(self.pipeline, 'val_loaders'):
            self.pipeline.val_loaders = {}

    # ---------- Expert Registration ----------
    async def on_expert_registered(
        self,
        expert_id: str,
        model: torch.nn.Module,
        profile: SustainabilityAwareExpertProfile,
        val_loader: Any,
    ) -> None:
        self._ensure_pipeline()
        profile.update_material_index(self.config)

        # Try loading compressed model from disk
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
                        # Load Pareto front if available
                        pareto = self.history_manager.get_pareto_front(expert_id)
                        if pareto:
                            profile.pareto_front = pareto
                            self._pareto_fronts[expert_id] = pareto
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
            try:
                sample_input = next(iter(val_loader))[0]
            except Exception as e:
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
                    # Store Pareto front
                    if profile.pareto_front:
                        self._pareto_fronts[expert_id] = profile.pareto_front
                        # Also ensure it's persisted in history (compressor already does this)
                    logger.info(f"Compressed expert {expert_id}")
                # Telemetry for MOPD
                if self.telemetry and self.config.mopd.enabled:
                    await self.telemetry.increment(f"{self.config.version}.mopd_generations")
                    if profile.pareto_front:
                        await self.telemetry.histogram(
                            f"{self.config.version}.pareto_front_size",
                            len(profile.pareto_front)
                        )
            else:
                logger.info(f"Expert {expert_id} remains uncompressed")
        else:
            logger.info(
                f"Expert {expert_id} energy ({profile.energy_per_inference_full:.2f} J) "
                f"within threshold. No compression."
            )

    # ---------- Background tasks (unchanged) ----------
    async def start_recompress_loop(self):
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
                    if profile.pareto_front:
                        self._pareto_fronts[expert_id] = profile.pareto_front

    # ---------- Anomaly‑triggered compression ----------
    async def on_anomaly_detected(self, node_id: str, metrics: Dict):
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
                            if profile.pareto_front:
                                self._pareto_fronts[expert_id] = profile.pareto_front
                    break

    # ---------- Rollback monitoring (unchanged) ----------
    async def monitor_rollback(self, expert_id: str, current_accuracy: float):
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
                        # Also clear Pareto front if reverted
                        if expert_id in self._pareto_fronts:
                            del self._pareto_fronts[expert_id]
                    logger.info(f"Reverted expert {expert_id} to full model.")
                else:
                    logger.error(f"No full model available for expert {expert_id} to revert.")

    # ---------- NEW: MOPD query methods ----------
    async def get_pareto_front(self, expert_id: str) -> Optional[List[MOPDPoint]]:
        """Return the Pareto front for a given expert, if available."""
        if not self.config.mopd.enabled:
            logger.warning("MOPD is disabled; Pareto fronts are not stored.")
            return None
        # Return from cache, or fetch from history if not present
        if expert_id in self._pareto_fronts:
            return self._pareto_fronts[expert_id]
        # Fallback: load from history
        pareto = self.history_manager.get_pareto_front(expert_id)
        if pareto:
            self._pareto_fronts[expert_id] = pareto
        return pareto

    async def get_mopd_summary(self) -> Dict[str, Any]:
        """Return a summary of MOPD‑related metrics."""
        if not self.config.mopd.enabled:
            return {"enabled": False}
        total_fronts = len(self._pareto_fronts)
        sizes = [len(front) for front in self._pareto_fronts.values()]
        return {
            "enabled": True,
            "objective_weights": self.config.mopd.objective_weights,
            "grid_resolution": self.config.mopd.grid_resolution,
            "total_experts_with_pareto_front": total_fronts,
            "average_pareto_size": np.mean(sizes) if sizes else 0,
            "max_pareto_size": max(sizes) if sizes else 0,
        }

    # ---------- Health check (enhanced) ----------
    async def health_check(self) -> Dict[str, Any]:
        return {
            "status": "healthy",
            "running": self._running,
            "recompress_task_active": self._recompress_task is not None and not self._recompress_task.done(),
            "num_experts": len(self.pipeline.model_registry),
            "compressed_count": sum(
                1 for p in self.pipeline.profile_registry.values() if p.compressed_flag
            ),
            "config_version": self.config.version,
            "mopd_enabled": self.config.mopd.enabled,
            "pareto_fronts_stored": len(self._pareto_fronts),
        }


# ==============================================
# SustainabilityAwareRouter (enhanced with MOPD)
# ==============================================

class SustainabilityAwareRouter:
    """
    Router that selects the most sustainable expert based on fitness score,
    with optional MOPD‑aware selection using Pareto fronts and configurable weights.
    """

    def __init__(
        self,
        base_router: Any,
        scorer: Optional[SustainabilityFitnessScorer] = None,
        default_required_accuracy: float = 0.90,
        config: Optional[SustainabilityConfig] = None,
    ):
        """
        Args:
            base_router: Object with methods:
                - get_all_experts(query) -> list of (expert_id, profile)
                - load_compressed_model(expert_id) -> model
                - load_full_model(expert_id) -> model
            scorer: SustainabilityFitnessScorer instance (if None, a new one is created)
            default_required_accuracy: Default minimum accuracy for routing
            config: SustainabilityConfig (if None, uses SUSTAINABILITY_CONFIG)
        """
        self.base_router = base_router
        self.scorer = scorer or SustainabilityFitnessScorer()
        self.default_required_accuracy = default_required_accuracy
        self.config = config or SUSTAINABILITY_CONFIG

    def route(
        self,
        query: Any,
        required_accuracy: Optional[float] = None,
        use_mopd: Optional[bool] = None,
        objective_weights: Optional[Dict[str, float]] = None,
    ) -> Any:
        """
        Route the query to the best expert.

        Args:
            query: The input query to route.
            required_accuracy: Minimum accuracy requirement. If None, uses default.
            use_mopd: If True, use Pareto front selection (if available). If None, uses config.mopd.enabled.
            objective_weights: Override weights for scalarisation when using MOPD.

        Returns:
            The selected model (compressed or full).
        """
        required = required_accuracy if required_accuracy is not None else self.default_required_accuracy
        use_mopd = use_mopd if use_mopd is not None else self.config.mopd.enabled

        # Get candidates
        try:
            candidates = self.base_router.get_all_experts(query)
        except Exception as e:
            logger.error(f"Failed to get candidates from base router: {e}")
            return self.base_router.route(query)

        # Filter by accuracy
        valid = []
        for exp_id, profile in candidates:
            acc = profile.accuracy_compressed if profile.compressed_flag else profile.accuracy_full
            if acc >= required:
                valid.append((exp_id, profile))

        if not valid:
            return self.base_router.route(query)

        # If MOPD is enabled and we have Pareto fronts, use them
        if use_mopd and self.config.mopd.enabled:
            # Find the best expert by scalarising their Pareto front
            weights = objective_weights if objective_weights is not None else self.config.mopd.objective_weights
            best_id = None
            best_score = -float('inf')
            best_profile = None
            for exp_id, profile in valid:
                if not profile.pareto_front:
                    # If no Pareto front, fallback to scalar fitness
                    self.scorer.compute(profile)
                    score = profile.sustainability_fitness_score
                else:
                    # Compute scalarised score for each point and take the best
                    # For simplicity, we use the same scalarisation as in compressor
                    # We could also use the Pareto front to select a point
                    # Here we take the point with highest scalarised score
                    points = profile.pareto_front
                    # Compute scalarised scores for each point using the weights
                    # We could store scalarised scores in the points if already computed
                    # but we recompute for clarity.
                    acc_vals = [p.accuracy for p in points]
                    energy_vals = [p.energy for p in points]
                    carbon_vals = [p.carbon_savings_kg for p in points]
                    material_vals = [p.material_index for p in points]

                    max_acc = max(acc_vals) if acc_vals else 1
                    max_energy = max(energy_vals) if energy_vals else 1
                    max_carbon = max(carbon_vals) if carbon_vals else 1
                    max_material = max(material_vals) if material_vals else 1

                    max_score = -float('inf')
                    for pt in points:
                        acc_norm = pt.accuracy / max_acc if max_acc > 0 else 0
                        energy_norm = 1.0 - (pt.energy / max_energy) if max_energy > 0 else 0
                        carbon_norm = pt.carbon_savings_kg / max_carbon if max_carbon > 0 else 0
                        material_norm = 1.0 - (pt.material_index / max_material) if max_material > 0 else 0
                        score = (weights.get('accuracy', 0.4) * acc_norm +
                                 weights.get('energy', 0.3) * energy_norm +
                                 weights.get('carbon', 0.2) * carbon_norm +
                                 weights.get('material', 0.1) * material_norm)
                        if score > max_score:
                            max_score = score
                    score = max_score
                if score > best_score:
                    best_score = score
                    best_id = exp_id
                    best_profile = profile
        else:
            # Use scalar fitness (fallback)
            for _, profile in valid:
                self.scorer.compute(profile)
            best_id, best_profile = max(valid, key=lambda x: x[1].sustainability_fitness_score)

        # Load the appropriate model
        if best_profile.compressed_flag:
            return self.base_router.load_compressed_model(best_id)
        else:
            return self.base_router.load_full_model(best_id)
