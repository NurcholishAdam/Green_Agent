# src/enhancements/adaptive_precision_controller.py
"""
Adaptive Precision Controller for FlexGen.

Selects the optimal precision for FlexGen inference based on hardware
telemetry and task features using a simple distillation student.

Usage:
    controller = AdaptivePrecisionController()
    precision = controller.select_precision(task_features, hardware_metrics)
"""

import numpy as np
from typing import Dict, Any

class AdaptivePrecisionController:
    PRECISIONS = ["fp32", "fp16", "int8"]

    def __init__(self, weights_path: str = "precision_student_weights.npz"):
        self.weights_path = weights_path
        self.weights = np.zeros((5, 3))  # 5 features -> 3 precisions
        self._load_weights()

    def _load_weights(self):
        try:
            data = np.load(self.weights_path)
            self.weights = data['weights']
        except FileNotFoundError:
            pass

    def select_precision(self, task_features: Dict[str, float],
                         hardware_metrics: Dict[str, float]) -> str:
        # Feature vector: [carbon_intensity, gpu_util, memory_pressure, latency_target, accuracy_requirement]
        feature_vec = np.array([
            task_features.get('carbon_intensity', 0.4) / 500.0,
            hardware_metrics.get('gpu_util', 0.5),
            hardware_metrics.get('memory_used', 0.5),
            task_features.get('latency_target', 100) / 1000.0,
            task_features.get('accuracy_requirement', 0.5)
        ])
        logits = feature_vec @ self.weights
        probs = np.exp(logits) / np.sum(np.exp(logits))
        action = int(np.argmax(probs))
        return self.PRECISIONS[action]

    def update(self, feature_vec: np.ndarray, action: int, reward: float):
        """Simple online update (policy gradient)."""
        logits = feature_vec @ self.weights
        probs = np.exp(logits) / np.sum(np.exp(logits))
        grad = -probs
        grad[action] += 1.0
        self.weights += 0.1 * reward * np.outer(feature_vec, grad)
        # Save weights
        np.savez(self.weights_path, weights=self.weights)
