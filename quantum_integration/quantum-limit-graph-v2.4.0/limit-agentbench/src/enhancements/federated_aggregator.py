# src/enhancements/federated_aggregator.py
"""
Federated Aggregator for Green Agent Students.

Enables sharing distilled student weights across deployments without raw data.
Each instance can export its student weights (or update via message queue),
and a central aggregator averages them to produce a global model.

Usage:
    aggregator = FederatedAggregator()
    global_weights = aggregator.aggregate([weights1, weights2, ...])
"""

import numpy as np
from typing import List, Dict

class FederatedAggregator:
    def __init__(self):
        self.global_weights = None

    def aggregate(self, weight_list: List[Dict[str, np.ndarray]]) -> Dict[str, np.ndarray]:
        """
        Average a list of weight dictionaries (each from a student).
        Weight dict format: {"student_weights": ndarray, "student_bias": ndarray}
        """
        if not weight_list:
            return {}
        n = len(weight_list)
        avg_weights = {}
        keys = weight_list[0].keys()
        for key in keys:
            arrs = [w[key] for w in weight_list]
            avg_weights[key] = np.mean(arrs, axis=0)
        self.global_weights = avg_weights
        return avg_weights

    def export_weights(self, student) -> Dict[str, np.ndarray]:
        """Extract weights from a distillation student object."""
        return {
            "student_weights": student.weights.copy(),
            "student_bias": student.biases.copy()
        }

    def import_weights(self, student, weight_dict: Dict[str, np.ndarray]):
        """Load averaged weights into a student."""
        student.weights = weight_dict["student_weights"].copy()
        student.biases = weight_dict["student_bias"].copy()
