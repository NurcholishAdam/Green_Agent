# src/enhancements/federated_aggregator.py
"""
Enhanced Federated Aggregator for Green Agent Students.

Supports weighted averaging, incremental updates, persistence, and robustness checks.
Enables sharing distilled student weights across deployments without raw data.

Usage:
    aggregator = FederatedAggregator(aggregation='weighted')
    # Batch aggregation:
    global_weights = aggregator.aggregate([(weights1, 100), (weights2, 200)])
    # Or incremental update:
    aggregator.update(weights1, sample_size=100)
    aggregator.update(weights2, sample_size=200)
    global_weights = aggregator.get_global_weights()
"""

import copy
import logging
import os
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np

logger = logging.getLogger(__name__)


class FederatedAggregator:
    """
    Federated averaging aggregator with optional weighting and persistence.

    Attributes:
        aggregation: Type of aggregation ('average' or 'weighted').
        global_weights: Current global model weights (dict of numpy arrays).
        total_samples: Total number of samples used in weighted aggregation (for incremental updates).
        weights_path: Path to save/load global weights.
    """

    def __init__(
        self,
        aggregation: str = "average",
        weights_path: Optional[str] = None,
    ):
        """
        Initialize the aggregator.

        Args:
            aggregation: 'average' for equal weighting, 'weighted' for
                         weighting by sample size (must provide sample_size in update/aggregate).
            weights_path: Optional path to a .npz file to persist global weights.
        """
        self.aggregation = aggregation.lower()
        if self.aggregation not in ("average", "weighted"):
            raise ValueError("aggregation must be 'average' or 'weighted'")
        self.global_weights: Optional[Dict[str, np.ndarray]] = None
        self.total_samples = 0
        self.weights_path = weights_path
        if weights_path and os.path.exists(weights_path):
            self._load_global_weights()

    def _validate_weight_dict(self, weight_dict: Dict[str, np.ndarray]) -> None:
        """Ensure all values are numpy arrays and shapes are consistent with existing global weights."""
        if not weight_dict:
            raise ValueError("Weight dictionary cannot be empty")
        for key, value in weight_dict.items():
            if not isinstance(value, np.ndarray):
                raise TypeError(f"Weight '{key}' must be a numpy array, got {type(value)}")
        if self.global_weights is not None:
            # Check that all keys in global_weights exist in weight_dict
            for key in self.global_weights:
                if key not in weight_dict:
                    raise KeyError(f"Missing weight key '{key}' in provided weights")
                if self.global_weights[key].shape != weight_dict[key].shape:
                    raise ValueError(
                        f"Shape mismatch for '{key}': global {self.global_weights[key].shape} "
                        f"vs provided {weight_dict[key].shape}"
                    )

    def _load_global_weights(self):
        """Load global weights from a .npz file."""
        if not self.weights_path:
            return
        try:
            data = np.load(self.weights_path, allow_pickle=True)
            self.global_weights = {key: data[key] for key in data.files}
            logger.info(f"Loaded global weights from {self.weights_path}")
        except Exception as e:
            logger.error(f"Failed to load weights from {self.weights_path}: {e}")

    def _save_global_weights(self):
        """Save global weights to a .npz file."""
        if not self.weights_path or self.global_weights is None:
            return
        try:
            np.savez(self.weights_path, **self.global_weights)
            logger.debug(f"Saved global weights to {self.weights_path}")
        except Exception as e:
            logger.error(f"Failed to save weights to {self.weights_path}: {e}")

    def aggregate(
        self,
        weight_list: List[Union[Dict[str, np.ndarray], Tuple[Dict[str, np.ndarray], int]]],
    ) -> Dict[str, np.ndarray]:
        """
        Aggregate a list of weight dictionaries (or (weights, sample_size) tuples).

        Args:
            weight_list: List of weight dictionaries, or list of (weights, sample_size)
                         tuples if aggregation='weighted'.

        Returns:
            Averaged weight dictionary (also stored in self.global_weights).
        """
        if not weight_list:
            raise ValueError("weight_list cannot be empty")

        # Normalize to list of (weights, sample_size) with sample_size=None for 'average'
        normalized: List[Tuple[Dict[str, np.ndarray], Optional[int]]] = []
        for item in weight_list:
            if isinstance(item, tuple):
                if self.aggregation != "weighted":
                    raise ValueError("Tuples (weights, sample_size) are only allowed for 'weighted' aggregation")
                w, size = item
                if not isinstance(size, (int, np.integer)) or size <= 0:
                    raise ValueError("sample_size must be a positive integer")
                normalized.append((w, int(size)))
            else:
                if self.aggregation == "weighted":
                    raise ValueError("For 'weighted' aggregation, provide (weights, sample_size) tuples")
                normalized.append((item, None))

        # Validate all weight dictionaries have the same keys and shapes
        first_weights = normalized[0][0]
        for w, _ in normalized[1:]:
            if set(w.keys()) != set(first_weights.keys()):
                raise ValueError("All weight dictionaries must have the same keys")
            for key in first_weights:
                if first_weights[key].shape != w[key].shape:
                    raise ValueError(f"Shape mismatch for key '{key}' among clients")

        # Perform aggregation
        if self.aggregation == "average":
            # Simple average: sum all weights then divide by count
            sum_weights = {key: np.zeros_like(first_weights[key]) for key in first_weights}
            for w, _ in normalized:
                for key in w:
                    sum_weights[key] += w[key]
            n = len(normalized)
            avg_weights = {key: sum_weights[key] / n for key in sum_weights}
            self.total_samples = n  # not used, but for record

        elif self.aggregation == "weighted":
            # Weighted average by sample size
            total_size = sum(size for _, size in normalized)
            if total_size == 0:
                raise ValueError("Total sample size must be > 0")
            weighted_sum = {key: np.zeros_like(first_weights[key]) for key in first_weights}
            for w, size in normalized:
                for key in w:
                    weighted_sum[key] += w[key] * size
            avg_weights = {key: weighted_sum[key] / total_size for key in weighted_sum}
            self.total_samples = total_size

        self.global_weights = avg_weights
        self._save_global_weights()
        logger.info(f"Aggregated {len(normalized)} clients using '{self.aggregation}' method")
        return copy.deepcopy(self.global_weights)

    def update(
        self,
        client_weights: Dict[str, np.ndarray],
        sample_size: Optional[int] = None,
    ) -> None:
        """
        Incrementally update the global weights with a new client's weights.

        Args:
            client_weights: Weight dictionary from a client.
            sample_size: Number of samples the client used (required for 'weighted').
        """
        if self.aggregation == "weighted" and sample_size is None:
            raise ValueError("sample_size is required for 'weighted' aggregation")
        if self.aggregation == "average":
            sample_size = 1  # each client contributes equally

        if self.global_weights is None:
            # First client initializes global weights
            self.global_weights = {key: np.array(val, dtype=np.float64) for key, val in client_weights.items()}
            self.total_samples = sample_size if sample_size else 1
        else:
            # Validate keys and shapes
            self._validate_weight_dict(client_weights)
            new_total = self.total_samples + sample_size if sample_size else self.total_samples + 1
            # Update weighted average
            for key in self.global_weights:
                self.global_weights[key] = (
                    self.global_weights[key] * self.total_samples + client_weights[key] * (sample_size or 1)
                ) / new_total
            self.total_samples = new_total

        self._save_global_weights()
        logger.debug(f"Updated global model with new client (total samples: {self.total_samples})")

    def get_global_weights(self) -> Optional[Dict[str, np.ndarray]]:
        """Return a copy of the current global weights."""
        if self.global_weights is None:
            return None
        return copy.deepcopy(self.global_weights)

    def export_weights(self, student: Any, attr_names: List[str] = ["weights", "biases"]) -> Dict[str, np.ndarray]:
        """
        Extract weights from a student object. Supports custom attribute names.

        Args:
            student: Student object with attributes listed in attr_names.
            attr_names: List of attribute names containing numpy arrays to extract.

        Returns:
            Dictionary mapping attribute names to numpy arrays (copies).
        """
        weight_dict = {}
        for name in attr_names:
            if not hasattr(student, name):
                raise AttributeError(f"Student does not have attribute '{name}'")
            attr = getattr(student, name)
            if not isinstance(attr, np.ndarray):
                raise TypeError(f"Attribute '{name}' must be a numpy array")
            weight_dict[name] = attr.copy()
        return weight_dict

    def import_weights(self, student: Any, weight_dict: Dict[str, np.ndarray]) -> None:
        """
        Load averaged weights into a student object.

        Args:
            student: Student object to update.
            weight_dict: Dictionary of weight arrays (keys must match student attributes).
        """
        for name, array in weight_dict.items():
            if not hasattr(student, name):
                logger.warning(f"Student does not have attribute '{name}'; skipping.")
                continue
            setattr(student, name, array.copy())
        logger.info("Imported global weights into student")
