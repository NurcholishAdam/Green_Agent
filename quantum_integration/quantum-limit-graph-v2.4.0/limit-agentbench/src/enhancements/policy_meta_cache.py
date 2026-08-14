"""
policy_meta_cache.py

Stores successful policies keyed by a workload fingerprint.
Uses a simple brute‑force nearest neighbour search (no extra deps).
Implements staleness and distance thresholds to avoid recommending
outdated or irrelevant policies.
"""
import time
from typing import Dict, Any, Optional, Tuple
import numpy as np


class WorkloadFingerprint:
    """Normalised fingerprint of a workload for similarity search."""
    def __init__(self, model_size_mb: float, prompt_len: int, gen_len: int,
                 gpu_mem_free_mb: float, disk_speed_class: int):
        self.model_size_mb = model_size_mb
        self.prompt_len = prompt_len
        self.gen_len = gen_len
        self.gpu_mem_free_mb = gpu_mem_free_mb
        self.disk_speed_class = disk_speed_class  # 0=HDD,1=SATA-SSD,2=NVMe

    def to_vector(self) -> np.ndarray:
        # Normalise to similar scales for distance calculation
        return np.array([
            self.model_size_mb / 1000.0,          # scale to GB
            self.prompt_len / 1024.0,
            self.gen_len / 1024.0,
            self.gpu_mem_free_mb / 1000.0,
            self.disk_speed_class / 2.0,
        ])


class PolicyMetaCache:
    """
    Caches policies with their observed average reward.
    Uses Euclidean distance to retrieve the closest historical workload.
    """
    def __init__(self, max_age_hours: float = 24.0, dist_threshold: float = 0.2):
        self.max_age_seconds = max_age_hours * 3600
        self.dist_threshold = dist_threshold
        self.store = {}  # key: tuple(vector) -> (policy, timestamp, avg_reward)
        self.vectors = []   # list of np.ndarray for brute‑force search
        self.keys = []      # corresponding tuple keys

    def _vector_to_key(self, vec: np.ndarray) -> tuple:
        return tuple(vec.tolist())

    def get_best_policy(self, fp: WorkloadFingerprint) -> Optional[Dict[str, Any]]:
        vec = fp.to_vector()
        if not self.vectors:
            return None

        # Brute‑force nearest neighbour (fine for small cache)
        best_idx = -1
        best_dist = float('inf')
        for i, stored_vec in enumerate(self.vectors):
            dist = np.linalg.norm(vec - stored_vec)
            if dist < best_dist:
                best_dist = dist
                best_idx = i

        if best_idx == -1 or best_dist > self.dist_threshold:
            return None

        key = self.keys[best_idx]
        policy, timestamp, avg_reward = self.store[key]
        # Check staleness
        if (time.time() - timestamp) > self.max_age_seconds:
            return None

        return policy

    def update(self, fp: WorkloadFingerprint, policy: Dict[str, Any], reward: float):
        vec = fp.to_vector()
        key = self._vector_to_key(vec)

        if key in self.store:
            # Update existing entry with moving average
            old_policy, old_ts, old_reward = self.store[key]
            # Conservative: keep the policy that yielded the highest reward
            if reward > old_reward:
                self.store[key] = (policy, time.time(), reward)
        else:
            # New entry
            self.store[key] = (policy, time.time(), reward)
            self.vectors.append(vec)
            self.keys.append(key)
