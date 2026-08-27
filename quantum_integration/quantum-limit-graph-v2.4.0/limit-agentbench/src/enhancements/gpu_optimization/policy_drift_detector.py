"""
Drift detection for selected FlexGen policies.
Monitors the distribution of chosen policies and triggers rollback if drift occurs.
"""

import asyncio
from typing import Dict, Any, Optional, List
from collections import deque
import json
import hashlib
import time
import pickle
from pathlib import Path

from ..drift_detection import DriftDetector  # if available
from ..logger import logger


class PolicyDriftDetector:
    """
    Specialized drift detector for policy selection.
    Uses Euclidean distance between recent policy feature vectors.
    """

    def __init__(self, threshold: float = 0.3, history_size: int = 50):
        self.threshold = threshold
        self.history = deque(maxlen=history_size)
        self.last_snapshot = None

    def add_policy(self, policy_dict: Dict[str, Any]):
        """Record a chosen policy."""
        # Convert policy to normalized vector
        vec = self._policy_to_vector(policy_dict)
        self.history.append(vec)

    def _policy_to_vector(self, policy: Dict[str, Any]) -> list:
        # Simple one-hot/continuous encoding
        vec = [
            policy.get('gpu_batch_size', 1) / 8.0,
            policy.get('block_size', 16) / 64.0,
            1.0 if policy.get('weight_device') == 'gpu' else 0.0,
            1.0 if policy.get('weight_device') == 'cpu' else 0.0,
            1.0 if policy.get('kv_cache_device') == 'gpu' else 0.0,
            policy.get('weight_bits', 16) / 16.0,
            policy.get('kv_cache_bits', 16) / 16.0,
            1.0 if policy.get('cpu_attention') else 0.0,
            1.0 if policy.get('overlap_io_compute') else 0.0,
        ]
        return vec

    def detect_drift(self) -> bool:
        """Return True if recent policies differ significantly from history."""
        if len(self.history) < 10:
            return False
        # Compare last policy to average of earlier ones
        recent = list(self.history)[-1]
        earlier = list(self.history)[:-1]
        if not earlier:
            return False
        avg_earlier = [sum(x[i] for x in earlier)/len(earlier) for i in range(len(recent))]
        dist = sum((recent[i]-avg_earlier[i])**2 for i in range(len(recent)))**0.5
        return dist > self.threshold
