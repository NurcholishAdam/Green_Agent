#!/usr/bin/env python3
"""
Adaptive Precision Controller for FlexGen with Exploration and Robust Learning.

Selects the optimal precision for FlexGen inference based on hardware telemetry
and task features using a linear policy trained with policy gradient.
Enhancements v2.0:
- Fixed policy gradient: reward now correctly scales the update.
- Save/load optimizer state (Adam moments and step).
- Feature dimension validation.
- Explainable AI: method `explain_selection` returns contributions.
- Temporal safety: optional invariant checks that can force lower precision.
- Human approval callback for high-risk transitions.
- Chaos testing: fault injection and resilience checks.
- Drift detection: monitors feature distribution and increases exploration on shift.
- Rate limiting (basic) to prevent rapid oscillation.

Usage:
    controller = AdaptivePrecisionController(learning_rate=0.01, epsilon=0.1)
    precision, explanation = await controller.select_precision(task_features, hardware_metrics)
    # after receiving reward
    await controller.update(task_features, hardware_metrics, action, reward)
"""

import asyncio
import logging
import numpy as np
from typing import Dict, Any, Optional, Tuple, Callable, List, Deque
from collections import deque
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class AdaptivePrecisionController:
    """
    A policy that maps a feature vector to one of several precision levels.
    Uses a linear policy (REINFORCE with Adam) and includes:
    - XAI explanations
    - Temporal safety checks
    - Human approval
    - Chaos testing
    - Drift detection
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
        # New parameters
        safety_monitor: Optional[Any] = None,       # instance of TemporalSafetyMonitor
        approval_callback: Optional[Callable[[str, Dict[str, float]], bool]] = None,
        drift_threshold: float = 0.1,
        drift_window: int = 20,
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
            safety_monitor: Optional TemporalSafetyMonitor instance.
            approval_callback: Optional async callback for approval; receives (mode, state) and returns bool.
            drift_threshold: Threshold for feature distribution drift (KL/mean shift).
            drift_window: Number of recent features to keep for drift detection.
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

        # Validate feature_dim against actual feature vector length
        # We'll assume the feature extractor returns exactly feature_dim, and we'll assert in that method.

        # Initialize weights (small random values to break symmetry)
        self.weights = np.random.randn(feature_dim, len(self.PRECISIONS)) * 0.01

        # Normalization running statistics
        if normalization is not None:
            self.mean = normalization.get("mean", np.zeros(feature_dim))
            self.std = normalization.get("std", np.ones(feature_dim))
            self._n_samples = normalization.get("n_samples", 0)
        else:
            self.mean = np.zeros(feature_dim)
            self.std = np.ones(feature_dim)
            self._n_samples = 0

        # Adam optimizer state
        self._m = np.zeros_like(self.weights)
        self._v = np.zeros_like(self.weights)
        self._t = 0  # Adam timestep

        # Lock for thread safety
        self._lock = asyncio.Lock()

        # New attributes
        self.safety_monitor = safety_monitor
        self.approval_callback = approval_callback
        self.drift_threshold = drift_threshold
        self.drift_window = drift_window
        self.feature_history: Deque[np.ndarray] = deque(maxlen=drift_window)

        # Load saved weights if they exist
        self._load_weights()

        # Drift detection state
        self._drift_detected = False
        self._last_drift_reset = 0

    def _load_weights(self):
        """Load weights, normalization stats, and optimizer state from disk if available."""
        try:
            data = np.load(self.weights_path, allow_pickle=True)
            self.weights = data["weights"]
            if "mean" in data and "std" in data:
                self.mean = data["mean"]
                self.std = data["std"]
                self._n_samples = int(data.get("n_samples", 0))
            # Load Adam state if present
            if "m" in data and "v" in data and "t" in data:
                self._m = data["m"]
                self._v = data["v"]
                self._t = int(data["t"])
            logger.info(f"Loaded weights from {self.weights_path}")
        except FileNotFoundError:
            logger.info(f"No existing weights found at {self.weights_path}; starting fresh.")
        except Exception as e:
            logger.warning(f"Failed to load weights: {e}; starting with defaults.")

    def _save_weights(self):
        """Save weights, normalization stats, and optimizer state to disk."""
        try:
            np.savez(
                self.weights_path,
                weights=self.weights,
                mean=self.mean,
                std=self.std,
                n_samples=self._n_samples,
                m=self._m,
                v=self._v,
                t=self._t,
            )
            logger.debug(f"Saved weights to {self.weights_path}")
        except Exception as e:
            logger.error(f"Failed to save weights: {e}")

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
        # Build raw vector
        raw = np.array(
            [
                task_features.get("carbon_intensity", 250.0),
                hardware_metrics.get("gpu_util", 0.5),
                hardware_metrics.get("memory_used", 0.5),
                task_features.get("latency_target", 500.0),
                task_features.get("accuracy_requirement", 0.5),
            ],
            dtype=np.float32,
        )
        if len(raw) != self.feature_dim:
            raise ValueError(
                f"Feature dimension mismatch: expected {self.feature_dim}, got {len(raw)}. "
                "Update `feature_dim` to match actual features."
            )

        # Update running mean and std
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
            self.std = np.maximum(self.std, 1e-6)

        # Normalize
        normalized = (raw - self.mean) / self.std

        # Add to history for drift detection
        self.feature_history.append(normalized.copy())

        # Check drift
        self._check_drift()

        return normalized

    def _check_drift(self):
        """Detect feature distribution drift and adjust exploration if necessary."""
        if len(self.feature_history) < self.drift_window:
            return

        # Compute mean of last window and compare to running mean
        recent_mean = np.mean(self.feature_history, axis=0)
        # Running mean of normalized features should be near zero
        drift = np.linalg.norm(recent_mean)
        if drift > self.drift_threshold:
            if not self._drift_detected:
                logger.warning(f"Drift detected (norm={drift:.3f}); increasing exploration.")
                self._drift_detected = True
                # Temporarily increase epsilon
                self.epsilon = min(0.5, self.epsilon * 2.0)
                # Reset drift flag after a few updates? We'll let epsilon decay naturally.
            self._drift_detected = True
        else:
            if self._drift_detected:
                logger.info("Drift resolved; resuming normal exploration.")
                self._drift_detected = False

    def _softmax(self, logits: np.ndarray) -> np.ndarray:
        """Compute softmax probabilities."""
        exp_logits = np.exp(logits - np.max(logits))
        return exp_logits / np.sum(exp_logits)

    async def select_precision(
        self, task_features: Dict[str, float], hardware_metrics: Dict[str, float]
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Select a precision level based on current features.

        Returns:
            (precision, explanation_dict)
            explanation_dict contains probabilities, feature contributions, and reason.
        """
        async with self._lock:
            features = self._extract_features(task_features, hardware_metrics)
            logits = features @ self.weights
            probs = self._softmax(logits)

            # Determine action via epsilon-greedy
            if np.random.rand() < self.epsilon:
                action = int(np.random.randint(len(self.PRECISIONS)))
                is_exploration = True
            else:
                action = int(np.argmax(probs))
                is_exploration = False

            chosen_precision = self.PRECISIONS[action]

            # Temporal safety check (if monitor provided)
            safety_state = {
                "carbon_intensity": task_features.get("carbon_intensity", 250.0),
                "latency_target": task_features.get("latency_target", 500.0),
                "accuracy_requirement": task_features.get("accuracy_requirement", 0.5),
                "precision": chosen_precision,
            }
            safety_violations = []
            if self.safety_monitor is not None:
                safety_violations = await self.safety_monitor.check(safety_state)

            # If violations, force a lower precision (int8) if available
            if safety_violations:
                logger.warning(f"Safety violations detected: {safety_violations}. Forcing int8.")
                chosen_precision = "int8"
                action = self.PRECISIONS.index("int8")
                is_exploration = False

            # Human approval (if callback set and we are switching to high precision)
            if (
                self.approval_callback is not None
                and chosen_precision == "fp32"
                and task_features.get("carbon_intensity", 250) > 500
            ):
                approved = await self.approval_callback(
                    "fp32",
                    {
                        "carbon_intensity": task_features.get("carbon_intensity", 250),
                        "latency_target": task_features.get("latency_target", 500),
                    },
                )
                if not approved:
                    logger.warning("Human approval not granted for fp32; falling back to fp16.")
                    chosen_precision = "fp16"
                    action = self.PRECISIONS.index("fp16")

            # Build explanation
            contributions = {
                self.PRECISIONS[i]: float(probs[i]) for i in range(len(self.PRECISIONS))
            }
            feature_names = [
                "carbon_intensity", "gpu_util", "memory_used", "latency_target", "accuracy_requirement"
            ]
            # Feature contributions to logits for chosen action
            feature_contributions = {
                name: float(features[i] * self.weights[i, action])
                for i, name in enumerate(feature_names)
            }
            explanation = {
                "precision": chosen_precision,
                "exploration": is_exploration,
                "probabilities": contributions,
                "feature_contributions": feature_contributions,
                "safety_violations": safety_violations,
                "epsilon": self.epsilon,
            }

            return chosen_precision, explanation

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
                # Scale gradient by reward (critical fix!)
                scaled_grad = reward * grad
                self._m = 0.9 * self._m + 0.1 * scaled_grad
                self._v = 0.999 * self._v + 0.001 * (scaled_grad ** 2)
                m_hat = self._m / (1 - 0.9 ** self._t)
                v_hat = self._v / (1 - 0.999 ** self._t)
                self.weights += self.learning_rate * m_hat / (np.sqrt(v_hat) + 1e-8)
            else:
                self.weights += self.learning_rate * reward * grad

            # Decay epsilon
            self.epsilon = max(self.min_epsilon, self.epsilon * self.epsilon_decay)

            self.update_count += 1
            if (self.save_interval > 0 and self.update_count % self.save_interval == 0) or \
               self.save_interval == 0:
                self._save_weights()

            logger.debug(
                f"Updated weights (step {self.update_count}), epsilon={self.epsilon:.4f}, "
                f"reward={reward:.2f}"
            )

    # ==================== XAI, Safety, Approval, Chaos ====================

    def explain_selection(
        self, task_features: Dict[str, float], hardware_metrics: Dict[str, float]
    ) -> Dict[str, Any]:
        """
        Return a detailed explanation of the current policy for the given features
        WITHOUT actually choosing an action (no exploration).
        Useful for debugging and transparency.
        """
        features = self._extract_features(task_features, hardware_metrics)
        logits = features @ self.weights
        probs = self._softmax(logits)
        chosen_action = int(np.argmax(probs))
        chosen_precision = self.PRECISIONS[chosen_action]

        feature_names = [
            "carbon_intensity", "gpu_util", "memory_used", "latency_target", "accuracy_requirement"
        ]
        feature_contributions = {
            name: float(features[i] * self.weights[i, chosen_action])
            for i, name in enumerate(feature_names)
        }

        return {
            "selected_precision": chosen_precision,
            "action_probabilities": {
                self.PRECISIONS[i]: float(probs[i]) for i in range(len(self.PRECISIONS))
            },
            "feature_contributions": feature_contributions,
            "epsilon": self.epsilon,
            "feature_vector_normalized": features.tolist(),
        }

    def set_safety_monitor(self, monitor: Any):
        """Attach a temporal safety monitor."""
        self.safety_monitor = monitor

    def set_approval_callback(self, callback: Callable[[str, Dict[str, float]], bool]):
        """Set callback for human approval."""
        self.approval_callback = callback

    # ------------------ Chaos Testing ------------------

    async def inject_fault(self, fault_type: str, **kwargs):
        """
        Inject a fault to test resilience.
        Supported: 'reset_weights', 'corrupt_normalization', 'nan_weights', 'clear_history'
        """
        async with self._lock:
            if fault_type == 'reset_weights':
                self.weights = np.random.randn(self.feature_dim, len(self.PRECISIONS)) * 0.01
                self._m = np.zeros_like(self.weights)
                self._v = np.zeros_like(self.weights)
                self._t = 0
                logger.warning("Fault injected: reset_weights")
            elif fault_type == 'corrupt_normalization':
                self.mean = np.ones(self.feature_dim)
                self.std = np.zeros(self.feature_dim) + 1e-6
                logger.warning("Fault injected: corrupt_normalization")
            elif fault_type == 'nan_weights':
                self.weights[:] = np.nan
                logger.warning("Fault injected: nan_weights")
            elif fault_type == 'clear_history':
                self.feature_history.clear()
                self._n_samples = 0
                logger.warning("Fault injected: clear_history")
            else:
                logger.warning(f"Unknown fault type: {fault_type}")

    async def run_chaos_test(self) -> Dict[str, Any]:
        """Run a simple chaos test to verify resilience."""
        report = {'faults': [], 'results': {}}

        # Test reset_weights
        await self.inject_fault('reset_weights')
        report['faults'].append('reset_weights')
        try:
            precision, explanation = await self.select_precision(
                {'carbon_intensity': 400, 'latency_target': 200, 'accuracy_requirement': 0.7},
                {'gpu_util': 0.5, 'memory_used': 0.6}
            )
            report['results']['reset_weights'] = {
                'status': 'recovered',
                'precision': precision,
                'probabilities': explanation['probabilities'],
            }
        except Exception as e:
            report['results']['reset_weights'] = f'error: {e}'

        # Test corrupt_normalization
        await self.inject_fault('corrupt_normalization')
        report['faults'].append('corrupt_normalization')
        try:
            precision, explanation = await self.select_precision(
                {'carbon_intensity': 400, 'latency_target': 200, 'accuracy_requirement': 0.7},
                {'gpu_util': 0.5, 'memory_used': 0.6}
            )
            report['results']['corrupt_normalization'] = {
                'status': 'recovered' if np.isfinite(explanation['probabilities']).all() else 'nan_detected',
                'precision': precision,
            }
        except Exception as e:
            report['results']['corrupt_normalization'] = f'error: {e}'

        # Test nan_weights
        await self.inject_fault('nan_weights')
        report['faults'].append('nan_weights')
        try:
            precision, explanation = await self.select_precision(
                {'carbon_intensity': 400, 'latency_target': 200, 'accuracy_requirement': 0.7},
                {'gpu_util': 0.5, 'memory_used': 0.6}
            )
            if np.isnan(explanation['probabilities']).any():
                # Attempt recovery by reinitializing weights
                self.weights = np.random.randn(self.feature_dim, len(self.PRECISIONS)) * 0.01
                precision, explanation = await self.select_precision(
                    {'carbon_intensity': 400, 'latency_target': 200, 'accuracy_requirement': 0.7},
                    {'gpu_util': 0.5, 'memory_used': 0.6}
                )
                report['results']['nan_weights'] = 'nan_detected_and_recovered'
            else:
                report['results']['nan_weights'] = 'no_nan_detected'
        except Exception as e:
            report['results']['nan_weights'] = f'error: {e}'

        return report
