# src/enhancements/adaptive_precision_controller.py
"""
Adaptive Precision Controller for FlexGen with Exploration and Robust Learning.

Selects the optimal precision for FlexGen inference based on hardware telemetry
and task features using a linear policy trained with policy gradient.

Features:
    - Epsilon-greedy exploration (with decay) to avoid local optima.
    - Adaptive feature normalization (running mean/std).
    - Adam optimizer for stable updates.
    - Thread-safe updates (asyncio.Lock).
    - Periodic weight saving to reduce I/O overhead.

Usage:
    controller = AdaptivePrecisionController(learning_rate=0.01, epsilon=0.1)
    precision = await controller.select_precision(task_features, hardware_metrics)
    # after receiving reward
    await controller.update(task_features, hardware_metrics, action, reward)
"""

import asyncio
import logging
import numpy as np
from typing import Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)


class AdaptivePrecisionController:
    """
    A policy that maps a feature vector to one of several precision levels.
    """

    PRECISIONS = ["fp32", "fp16", "int8"]

    def __init__(
        self,
        weights_path: str = "precision_student_weights.npz",
        learning_rate: float = 0.01,
        epsilon: float = 0.1,
        epsilon_decay: float = 0.995,
        min_epsilon: float = 0.01,
        feature_dim: int = 5,
        use_adam: bool = True,
        save_interval: int = 10,
        normalization: Optional[Dict[str, np.ndarray]] = None,
    ):
        """
        Initialize the controller.

        Args:
            weights_path: Path to save/load model weights.
            learning_rate: Step size for weight updates (used by Adam if enabled).
            epsilon: Initial exploration probability.
            epsilon_decay: Multiplicative factor to reduce epsilon after each update.
            min_epsilon: Minimum value of epsilon (exploration never fully stops).
            feature_dim: Dimension of the feature vector (default 5).
            use_adam: If True, use Adam optimizer; otherwise plain SGD.
            save_interval: Save weights after this many updates (0 = save every update).
            normalization: Optional dict with 'mean' and 'std' arrays for feature scaling.
                           If None, running statistics will be computed.
        """
        self.weights_path = weights_path
        self.learning_rate = learning_rate
        self.epsilon = epsilon
        self.epsilon_decay = epsilon_decay
        self.min_epsilon = min_epsilon
        self.feature_dim = feature_dim
        self.use_adam = use_adam
        self.save_interval = save_interval
        self.update_count = 0

        # Initialize weights (small random values to break symmetry)
        self.weights = np.random.randn(feature_dim, len(self.PRECISIONS)) * 0.01

        # Normalization running statistics
        if normalization is not None:
            self.mean = normalization.get("mean", np.zeros(feature_dim))
            self.std = normalization.get("std", np.ones(feature_dim))
        else:
            self.mean = np.zeros(feature_dim)
            self.std = np.ones(feature_dim)
            self._n_samples = 0

        # Adam optimizer state
        self._m = np.zeros_like(self.weights)
        self._v = np.zeros_like(self.weights)
        self._t = 0  # Adam timestep

        # Lock for thread safety (use asyncio.Lock if running in async context)
        self._lock = asyncio.Lock()

        # Load saved weights if they exist
        self._load_weights()

    def _load_weights(self):
        """Load weights and normalization stats from disk if available."""
        try:
            data = np.load(self.weights_path, allow_pickle=True)
            self.weights = data["weights"]
            if "mean" in data and "std" in data:
                self.mean = data["mean"]
                self.std = data["std"]
                self._n_samples = int(data.get("n_samples", 0))
            logger.info(f"Loaded weights from {self.weights_path}")
        except FileNotFoundError:
            logger.info(f"No existing weights found at {self.weights_path}; starting fresh.")

    def _save_weights(self):
        """Save weights and normalization statistics to disk."""
        np.savez(
            self.weights_path,
            weights=self.weights,
            mean=self.mean,
            std=self.std,
            n_samples=self._n_samples,
        )
        logger.debug(f"Saved weights to {self.weights_path}")

    def _extract_features(
        self, task_features: Dict[str, float], hardware_metrics: Dict[str, float]
    ) -> np.ndarray:
        """
        Convert task and hardware metrics into a normalized feature vector.

        Features (order fixed):
            0: carbon_intensity (gCO2/kWh)
            1: gpu_util (0.0–1.0)
            2: memory_used (0.0–1.0)
            3: latency_target (ms)
            4: accuracy_requirement (0.0–1.0)
        """
        raw = np.array(
            [
                task_features.get("carbon_intensity", 250.0),  # default 250 gCO2/kWh
                hardware_metrics.get("gpu_util", 0.5),
                hardware_metrics.get("memory_used", 0.5),
                task_features.get("latency_target", 500.0),  # default 500 ms
                task_features.get("accuracy_requirement", 0.5),
            ],
            dtype=np.float32,
        )

        # Update running mean and std if we have no fixed normalization
        if self._n_samples == 0:
            self.mean = raw.copy()
            self.std = np.ones_like(raw)
            self._n_samples = 1
        else:
            self._n_samples += 1
            delta = raw - self.mean
            self.mean += delta / self._n_samples
            # Welford's online variance update
            delta2 = raw - self.mean
            self.std = np.sqrt(
                ((self._n_samples - 1) * self.std**2 + delta * delta2) / self._n_samples
            )
            # Prevent division by zero
            self.std = np.maximum(self.std, 1e-6)

        # Normalize
        return (raw - self.mean) / self.std

    def _softmax(self, logits: np.ndarray) -> np.ndarray:
        """Compute softmax probabilities."""
        exp_logits = np.exp(logits - np.max(logits))  # subtract max for numerical stability
        return exp_logits / np.sum(exp_logits)

    async def select_precision(
        self, task_features: Dict[str, float], hardware_metrics: Dict[str, float]
    ) -> str:
        """
        Select a precision level based on current features.

        Args:
            task_features: Dictionary with keys like 'carbon_intensity', 'latency_target', etc.
            hardware_metrics: Dictionary with 'gpu_util', 'memory_used'.

        Returns:
            One of the strings in `self.PRECISIONS`.
        """
        async with self._lock:
            features = self._extract_features(task_features, hardware_metrics)
            logits = features @ self.weights
            probs = self._softmax(logits)

            # Epsilon-greedy exploration
            if np.random.rand() < self.epsilon:
                action = np.random.randint(len(self.PRECISIONS))
                logger.debug(f"Exploring: chose action {action} (epsilon={self.epsilon:.3f})")
            else:
                action = int(np.argmax(probs))
                logger.debug(f"Exploiting: chose action {action}")

            return self.PRECISIONS[action]

    async def update(
        self,
        task_features: Dict[str, float],
        hardware_metrics: Dict[str, float],
        action: int,
        reward: float,
    ):
        """
        Update the policy weights based on the reward received for a chosen action.

        Args:
            task_features: Same as in `select_precision`.
            hardware_metrics: Same as in `select_precision`.
            action: Index of the action that was taken (0, 1, or 2).
            reward: Scalar reward signal (positive good, negative bad).
        """
        async with self._lock:
            features = self._extract_features(task_features, hardware_metrics)
            logits = features @ self.weights
            probs = self._softmax(logits)

            # Policy gradient: gradient of log probability of chosen action
            grad = -probs
            grad[action] += 1.0
            grad = np.outer(features, grad)

            # Update weights using Adam or SGD
            if self.use_adam:
                self._t += 1
                self._m = 0.9 * self._m + 0.1 * grad
                self._v = 0.999 * self._v + 0.001 * (grad**2)
                m_hat = self._m / (1 - 0.9**self._t)
                v_hat = self._v / (1 - 0.999**self._t)
                self.weights += self.learning_rate * m_hat / (np.sqrt(v_hat) + 1e-8)
            else:
                self.weights += self.learning_rate * reward * grad

            # Decay epsilon
            self.epsilon = max(self.min_epsilon, self.epsilon * self.epsilon_decay)

            self.update_count += 1
            if self.save_interval > 0 and self.update_count % self.save_interval == 0:
                self._save_weights()
            elif self.save_interval == 0:
                self._save_weights()

            logger.debug(
                f"Updated weights (step {self.update_count}), epsilon={self.epsilon:.4f}, "
                f"reward={reward:.2f}"
            )
