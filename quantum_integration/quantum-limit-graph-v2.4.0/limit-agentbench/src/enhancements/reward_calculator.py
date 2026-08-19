"""
reward_calculator.py

Enhanced reward calculator with MODP, bio_inspired, and moe_system integration.

Features:
- MODP‑based multi‑objective evaluation (with fallback to weighted sum).
- Bio‑inspired weight adaptation via genetic algorithm.
- MoE context‑aware dynamic weighting.
- Persistence of weights to JSON.
- Configurable objectives.
- Full objective vector retrieval for MODP and bio modules.
"""

import json
import os
import time
import logging
from typing import Dict, Any, Optional, List, Callable

# Optional imports with fallback stubs
try:
    from .MODP import ParetoOptimizer
except ImportError:
    class ParetoOptimizer:
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0) * weights.get(k, 1) for k in objectives)

try:
    from .bio_inspired import GeneticOptimizer
except ImportError:
    class GeneticOptimizer:
        def adapt(self, context, reward, weights):
            return weights  # no change

try:
    from .moe_system import ExpertRouter
except ImportError:
    class ExpertRouter:
        def encode(self, task):
            return {}  # no context


class RewardCalculator:
    """
    Enhanced reward calculator with MODP, bio, and MoE integration.
    """

    def __init__(
        self,
        weights: Optional[Dict[str, float]] = None,
        modp_optimizer: Optional[Any] = None,
        bio_optimizer: Optional[Any] = None,
        moe_router: Optional[Any] = None,
        persistence_file: Optional[str] = "reward_weights.json",
        enable_adaptation: bool = True,
        min_quality_threshold: float = 0.5,
        max_latency_ms: float = 1e9,
    ):
        """
        Args:
            weights: Initial weights for objectives (quality, throughput, energy_efficiency, carbon_efficiency, memory_efficiency).
            modp_optimizer: MODP optimizer instance (default: ParetoOptimizer).
            bio_optimizer: Bio‑inspired optimizer for weight adaptation (default: GeneticOptimizer).
            moe_router: MoE router for context‑aware weighting (default: ExpertRouter).
            persistence_file: Path to save/load weights.
            enable_adaptation: Whether to allow bio‑inspired weight updates.
            min_quality_threshold: Minimum quality to avoid penalty.
            max_latency_ms: Maximum latency before penalty.
        """
        self.logger = logging.getLogger(__name__)

        # Set up modules
        self.modp = modp_optimizer if modp_optimizer else ParetoOptimizer()
        self.bio = bio_optimizer if bio_optimizer else GeneticOptimizer()
        self.moe = moe_router if moe_router else ExpertRouter()

        # Objectives and weights
        self.objective_names = [
            "quality",
            "throughput",
            "energy_efficiency",
            "carbon_efficiency",
            "memory_efficiency"
        ]
        self.weights = weights or {
            "quality": 0.30,
            "throughput": 0.25,
            "energy_efficiency": 0.20,
            "carbon_efficiency": 0.15,
            "memory_efficiency": 0.10,
        }
        # Ensure all objectives are present
        for obj in self.objective_names:
            if obj not in self.weights:
                self.weights[obj] = 0.0

        self.persistence_file = persistence_file
        self.enable_adaptation = enable_adaptation
        self.min_quality = min_quality_threshold
        self.max_latency_ms = max_latency_ms

        # Load persisted weights if available
        self._load_weights()

    # --------------------- Persistence ---------------------
    def _load_weights(self):
        """Load weights from JSON file."""
        if not self.persistence_file or not os.path.exists(self.persistence_file):
            return
        try:
            with open(self.persistence_file, "r") as f:
                data = json.load(f)
                if "weights" in data:
                    self.weights.update(data["weights"])
                if "last_update" in data:
                    self.last_update = data["last_update"]
            self.logger.info("Loaded weights from %s", self.persistence_file)
        except Exception as e:
            self.logger.warning("Failed to load weights: %s", e)

    def _save_weights(self):
        """Save current weights to JSON."""
        if not self.persistence_file:
            return
        try:
            data = {
                "weights": self.weights,
                "last_update": time.time(),
            }
            with open(self.persistence_file, "w") as f:
                json.dump(data, f)
            self.logger.debug("Weights saved.")
        except Exception as e:
            self.logger.warning("Failed to save weights: %s", e)

    # --------------------- Objective Extraction ---------------------
    def _extract_objectives(
        self,
        aggregated_metrics: Dict[str, Any],
        carbon_intensity_gco2_kwh: float = 0.0
    ) -> Dict[str, float]:
        """
        Extract and normalize all objectives from metrics.
        Returns a dict of objective values (0-1 scale where appropriate).
        """
        quality = aggregated_metrics.get("quality_score", 1.0)
        throughput = aggregated_metrics.get("tokens_per_sec", 0.0)
        total_energy_kwh = aggregated_metrics.get("total_energy_kwh", 0.0)
        mem_eff = aggregated_metrics.get("memory_efficiency", 0.0)

        # Carbon efficiency: higher is better (tokens per kg CO2)
        if throughput > 0 and total_energy_kwh > 0:
            carbon_per_token = (total_energy_kwh * carbon_intensity_gco2_kwh) / throughput
            carbon_eff = max(0.0, 1.0 - (carbon_per_token / 100.0))
        else:
            carbon_eff = 0.0

        # Energy efficiency: tokens per kWh, normalized
        if total_energy_kwh > 0 and throughput > 0:
            energy_eff = min(1.0, throughput / (total_energy_kwh * 1000))
        else:
            energy_eff = 0.0

        return {
            "quality": min(1.0, max(0.0, quality)),
            "throughput": min(1.0, throughput / 100.0),  # normalize
            "energy_efficiency": energy_eff,
            "carbon_efficiency": carbon_eff,
            "memory_efficiency": min(1.0, max(0.0, mem_eff)),
        }

    # --------------------- Compute Reward (MODP + Penalties) ---------------------
    def compute(
        self,
        aggregated_metrics: Dict[str, Any],
        constraints: Dict[str, Any],
        carbon_intensity_gco2_kwh: float = 0.0
    ) -> float:
        """
        Compute reward using MODP (or weighted sum fallback) with penalties.
        Returns a reward between -10.0 and 10.0.
        """
        # 1. Extract objectives
        objectives = self._extract_objectives(aggregated_metrics, carbon_intensity_gco2_kwh)

        # 2. Compute base utility using MODP
        utility = self.modp.evaluate(objectives, self.weights)

        # 3. Penalties for constraint violations
        penalty = 0.0
        if aggregated_metrics.get("gpu_oom", False):
            penalty -= 10.0

        max_latency = constraints.get("max_latency_ms", self.max_latency_ms)
        if aggregated_metrics.get("elapsed_sec", 0) * 1000 > max_latency:
            penalty -= 5.0

        min_quality = constraints.get("min_quality", self.min_quality)
        if objectives["quality"] < min_quality:
            penalty -= 5.0

        # 4. Final reward
        reward = utility + penalty
        reward = max(-10.0, min(10.0, reward))

        return reward

    # --------------------- Adaptation (Bio‑inspired) ---------------------
    def adapt_weights(self, context: Dict[str, Any], reward: float):
        """
        Update weights using bio‑inspired optimizer based on outcome.
        """
        if not self.enable_adaptation:
            return

        # Include MoE context if available
        moe_context = self.moe.encode(context)
        if moe_context:
            context.update(moe_context)

        # Let bio optimizer adapt weights
        new_weights = self.bio.adapt(context, reward, self.weights)
        if new_weights:
            self.weights.update(new_weights)
            self._save_weights()
            self.logger.info("Weights adapted via bio‑inspired optimizer.")

    # --------------------- Context‑Aware Weight Adjustment (MoE) ---------------------
    def adjust_weights_for_context(self, task: Dict[str, Any]):
        """
        Modify weights dynamically based on MoE context (e.g., priority).
        This can be called before compute() for each task.
        """
        # Example: if task priority is "eco", boost carbon efficiency weight
        priority = task.get("priority", "normal")
        if priority == "eco":
            self.weights["carbon_efficiency"] = 0.4
            self.weights["throughput"] = 0.1
        elif priority == "speed":
            self.weights["throughput"] = 0.5
            self.weights["carbon_efficiency"] = 0.1
        else:
            # Reset to default (or use MoE to decide)
            # For simplicity, we keep current weights if not overridden.
            pass

        # Could also use MoE router to get a full vector of weights
        # moe_weights = self.moe.get_weights(task)
        # if moe_weights:
        #     self.weights.update(moe_weights)

    # --------------------- Utility Methods ---------------------
    def get_objectives(
        self,
        aggregated_metrics: Dict[str, Any],
        carbon_intensity_gco2_kwh: float = 0.0
    ) -> Dict[str, float]:
        """Return the full objective vector (useful for MODP and bio modules)."""
        return self._extract_objectives(aggregated_metrics, carbon_intensity_gco2_kwh)

    def get_weights(self) -> Dict[str, float]:
        """Return current weights."""
        return self.weights.copy()

    def reset_weights(self, weights: Optional[Dict[str, float]] = None):
        """Reset weights to default or provided values."""
        if weights:
            self.weights.update(weights)
        else:
            self.weights = {
                "quality": 0.30,
                "throughput": 0.25,
                "energy_efficiency": 0.20,
                "carbon_efficiency": 0.15,
                "memory_efficiency": 0.10,
            }
        self._save_weights()

    # --------------------- Backward Compatibility (Optional) ---------------------
    # The original compute() signature remains unchanged.
