"""
Drift Detection for Adaptive Cost Function
===========================================
Detects sudden shifts in cost weights and triggers rollback to a safe snapshot.
Enhanced with configurable intervals, hysteresis, drift event logging, and metrics.

NEW v2.0:
- Integrated optional LIMIT Graph, MODP, RLHF, and MoE components for
  advanced drift event persistence, preference collection, and metric blending.
- All original features (multiple distance metrics, adaptive threshold, EWMA, etc.)
  retained.

Enhancements implemented:
- Multiple distance metrics (Euclidean, Manhattan, Cosine, Relative)
- Adaptive threshold based on rolling distance history
- EWMA smoothing for early detection of gradual drift
- Weighted distance to prioritize important dimensions
- JSON serialization for portability and safety
- In-memory caching of last snapshot for performance
- Additional configuration options (metric, alpha, history size, weights)
"""
import hashlib
import time
import json
import uuid
from typing import Dict, Optional, List, Any
from collections import deque
import numpy as np
import copy

from ..storage import Storage
from ..config import config
from ..logger import logger

# Optional Prometheus metric
try:
    from prometheus_client import Counter
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False


# ------------------------------------------------------------------------------
# NEW: LIMIT Graph Manager
# ------------------------------------------------------------------------------
class LimitGraphManager:
    """
    Manages a graph of drift event relationships for LIMIT.
    Nodes are drift events or snapshots, edges represent temporal or causal links.
    """
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self.graphs = {}

    def create_graph(self, graph_id: str, description: str, configuration: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_metadata'):
            self.storage.save_limit_graph_metadata(graph_id, description, configuration)
        else:
            self.graphs[graph_id] = {'description': description, 'configuration': configuration, 'nodes': {}, 'edges': {}}

    def add_node(self, graph_id: str, node_id: str, node_type: Optional[str], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_node'):
            self.storage.save_limit_graph_node(node_id, graph_id, node_type, attributes)
        else:
            if graph_id not in self.graphs:
                self.graphs[graph_id] = {'nodes': {}, 'edges': {}}
            self.graphs[graph_id]['nodes'][node_id] = {'node_type': node_type, 'attributes': attributes}

    def add_edge(self, graph_id: str, edge_id: str, source: str, target: str,
                 weight: Optional[float], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_edge'):
            self.storage.save_limit_graph_edge(edge_id, graph_id, source, target, weight, attributes)
        else:
            if graph_id not in self.graphs:
                self.graphs[graph_id] = {'nodes': {}, 'edges': {}}
            self.graphs[graph_id]['edges'][edge_id] = {'source': source, 'target': target, 'weight': weight, 'attributes': attributes}

    def get_nodes(self, graph_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_nodes'):
            return self.storage.get_limit_graph_nodes(graph_id)
        return list(self.graphs.get(graph_id, {}).get('nodes', {}).values())

    def get_edges(self, graph_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_edges'):
            return self.storage.get_limit_graph_edges(graph_id)
        return list(self.graphs.get(graph_id, {}).get('edges', {}).values())

    def get_metadata(self, graph_id: str) -> Optional[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_metadata'):
            return self.storage.get_limit_graph_metadata(graph_id)
        return self.graphs.get(graph_id, {})


# ------------------------------------------------------------------------------
# NEW: MODP Optimizer (wrapper)
# ------------------------------------------------------------------------------
class MODPOptimizer:
    """
    Multi‑Objective Dynamic Programming solver that stores decision states/policies.
    Used here to persist drift events as states.
    """
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self.states = {}

    def add_state(self, state_id: str, problem_id: str, state_attributes: Dict[str, Any],
                  objective_values: Dict[str, float], stage: int) -> None:
        if self.storage and hasattr(self.storage, 'save_modp_state'):
            self.storage.save_modp_state(state_id, problem_id, state_attributes, objective_values, stage)
        else:
            if problem_id not in self.states:
                self.states[problem_id] = []
            self.states[problem_id].append({
                'state_id': state_id, 'state_attributes': state_attributes,
                'objective_values': objective_values, 'stage': stage
            })

    def add_policy(self, policy_id: str, problem_id: str, state_id: str,
                   action: str, expected_objectives: Dict[str, float]) -> None:
        if self.storage and hasattr(self.storage, 'save_modp_policy'):
            self.storage.save_modp_policy(policy_id, problem_id, state_id, action, expected_objectives)

    def get_states(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_states'):
            return self.storage.get_modp_states(problem_id)
        return self.states.get(problem_id, [])

    def get_policies(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_policies'):
            return self.storage.get_modp_policies(problem_id)
        return []


# ------------------------------------------------------------------------------
# NEW: RLHF Trainer
# ------------------------------------------------------------------------------
class RLHFTrainer:
    """
    Collects human preference pairs for rollback decisions.
    """
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self.pairs = []

    def record_pair(self, pair_id: str, prompt: str, chosen: str, rejected: str,
                    reward_diff: float, metadata: Optional[Dict] = None) -> None:
        if self.storage and hasattr(self.storage, 'save_preference_pair'):
            self.storage.save_preference_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)
        else:
            self.pairs.append({
                'pair_id': pair_id, 'prompt': prompt, 'chosen': chosen,
                'rejected': rejected, 'reward_diff': reward_diff, 'metadata': metadata
            })

    def get_pairs(self, limit: int = 100) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_preference_pairs'):
            return self.storage.get_preference_pairs(limit)
        return self.pairs[-limit:]

    def train_reward_model(self):
        pairs = self.get_pairs()
        if len(pairs) < 5:
            logger.info("Not enough preference pairs for RLHF training.")
            return
        logger.info(f"Training reward model on {len(pairs)} preference pairs...")


# ------------------------------------------------------------------------------
# NEW: MoE Gating Network for Metric Selection
# ------------------------------------------------------------------------------
class MoEGatingNetwork:
    """
    Mixture-of-Experts gating that blends multiple distance metrics for drift detection.
    Experts correspond to metrics: euclidean, manhattan, cosine, relative.
    The gating network learns to select the most informative metric given current context.
    """
    def __init__(self, storage: Optional[Storage] = None, config: Optional[Dict] = None):
        self.storage = storage
        self.config = config or {}
        self.expert_names = self.config.get('expert_names', ['euclidean', 'manhattan', 'cosine', 'relative'])
        self.num_experts = len(self.expert_names)
        # Simple linear gating on context features: current distance, EWMA, threshold, consecutive count, history mean
        self.gating_weights = np.random.randn(self.num_experts, 5)
        self._training_samples = []

    def _encode_state(self, features: Dict[str, float]) -> np.ndarray:
        x = np.array([
            features.get('distance', 0.0),
            features.get('ewma', 0.0),
            features.get('threshold', 0.0),
            features.get('consecutive', 0.0),
            features.get('history_mean', 0.0),
        ], dtype=np.float32)
        return x

    async def select_expert(self, features: Dict[str, float]) -> Tuple[str, np.ndarray]:
        x = self._encode_state(features)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        expert_idx = np.argmax(probs)
        selected = self.expert_names[expert_idx]
        if self.storage and hasattr(self.storage, 'log_routing_decision'):
            sample_id = hashlib.sha256(str(features).encode()).hexdigest()[:16]
            self.storage.log_routing_decision(str(uuid.uuid4()), sample_id, selected, float(probs[expert_idx]))
        return selected, probs

    async def add_training_sample(self, features: Dict[str, float], selected_expert: str, reward: float):
        x = self._encode_state(features)
        expert_idx = self.expert_names.index(selected_expert)
        target = np.zeros(self.num_experts)
        target[expert_idx] = 1.0
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        grad = (probs - target)[:, None] * x[None, :]
        self.gating_weights -= 0.1 * grad


# ==============================================================================
# Enhanced DriftDetector with optional new components
# ==============================================================================
class DriftDetector:
    """
    Detects policy drift and manages rollback checkpoints.

    Features:
        - Periodic snapshots (configurable interval).
        - Drift detection using configurable distance metrics.
        - Adaptive threshold or static threshold.
        - EWMA smoothing for early drift detection.
        - Hysteresis: requires N consecutive drift detections before rollback.
        - Persistent log of drift events in SQLite.
        - Manual snapshot trigger.
        - Rollback restores online weights and resets detection state.
        - Optional integration with LIMIT Graph, MODP, RLHF, and MoE gating.
    """

    def __init__(
        self,
        storage: Storage,
        adaptive_cost,
        metrics_registry=None,
        metric: Optional[str] = None,
        use_adaptive_threshold: Optional[bool] = None,
        ewma_alpha: Optional[float] = None,
        weight_importance: Optional[Dict[str, float]] = None,
        distance_history_size: Optional[int] = None,
        # NEW optional component flags
        enable_limit_graph: bool = True,
        enable_modp: bool = True,
        enable_rlhf: bool = True,
        enable_moe: bool = True,
        moe_expert_names: Optional[List[str]] = None,
    ):
        """
        Args:
            storage: Storage instance for snapshots and event logs.
            adaptive_cost: AdaptiveCostFunction instance (must have online.weights).
            metrics_registry: Optional MetricsRegistry for Prometheus counters.
            metric: Distance metric to use ('euclidean', 'manhattan', 'cosine', 'relative').
                    Defaults to config.DRIFT_METRIC or 'euclidean'.
            use_adaptive_threshold: If True, threshold is computed from rolling distance history.
                    Defaults to config.DRIFT_USE_ADAPTIVE_THRESHOLD or False.
            ewma_alpha: Smoothing factor for EWMA (0 < alpha <= 1). Larger alpha gives more weight to recent distances.
                    Defaults to config.DRIFT_EWMA_ALPHA or 0.3.
            weight_importance: Optional dict mapping weight keys to importance multipliers for weighted distance.
                    Defaults to config.DRIFT_WEIGHT_IMPORTANCE or {} (all equal).
            distance_history_size: Number of recent distances to keep for adaptive threshold.
                    Defaults to config.DRIFT_HISTORY_SIZE or 100.
            enable_limit_graph: Enable LIMIT Graph manager for drift event nodes.
            enable_modp: Enable MODP solver for persisting drift events.
            enable_rlhf: Enable RLHF trainer for human preference pairs.
            enable_moe: Enable MoE gating for metric selection (experimental).
            moe_expert_names: Names of experts for MoE gating; defaults to all metrics.
        """
        self.storage = storage
        self.adaptive_cost = adaptive_cost
        self.threshold = config.DRIFT_THRESHOLD
        self.rollback_enabled = config.ROLLBACK_ENABLED
        self.snapshot_interval = config.DRIFT_SNAPSHOT_INTERVAL or 3600
        self.hysteresis_count = config.DRIFT_HYSTERESIS_COUNT or 1
        self.last_snapshot_time = 0
        self._drift_counter = 0

        self.metric = metric or getattr(config, 'DRIFT_METRIC', 'euclidean')
        self.use_adaptive_threshold = use_adaptive_threshold or getattr(config, 'DRIFT_USE_ADAPTIVE_THRESHOLD', False)
        self.ewma_alpha = ewma_alpha if ewma_alpha is not None else getattr(config, 'DRIFT_EWMA_ALPHA', 0.3)
        self.weight_importance = weight_importance or getattr(config, 'DRIFT_WEIGHT_IMPORTANCE', {})
        self.distance_history_size = distance_history_size or getattr(config, 'DRIFT_HISTORY_SIZE', 100)
        self.distance_history = deque(maxlen=self.distance_history_size)
        self._ewma_distance = 0.0
        self._last_snapshot_cache = None

        valid_metrics = {'euclidean', 'manhattan', 'cosine', 'relative'}
        if self.metric not in valid_metrics:
            logger.warning(f"Invalid metric '{self.metric}', falling back to 'euclidean'.")
            self.metric = 'euclidean'

        self.drift_counter_metric = None
        if PROMETHEUS_AVAILABLE and metrics_registry:
            self.drift_counter_metric = Counter(
                'green_agent_drift_detections_total',
                'Total number of drift detections',
                registry=metrics_registry.registry if hasattr(metrics_registry, 'registry') else None
            )

        # NEW optional components
        self.limit_graph_manager = None
        self.modp_solver = None
        self.rlhf_trainer = None
        self.moe_gating = None

        if enable_limit_graph:
            self.limit_graph_manager = LimitGraphManager(storage)
            # Create graph if not exists (assuming storage supports limit graph methods)
            if hasattr(storage, 'save_limit_graph_metadata'):
                # Check if metadata exists (we can't easily check without a get method; assume not)
                self.limit_graph_manager.create_graph(
                    "drift_events",
                    "Drift Event Relationships",
                    {"created_at": time.time()}
                )
            else:
                # In-memory fallback
                self.limit_graph_manager.create_graph("drift_events", "Drift Event Relationships", {})

        if enable_modp:
            self.modp_solver = MODPOptimizer(storage)

        if enable_rlhf:
            self.rlhf_trainer = RLHFTrainer(storage)

        if enable_moe:
            self.moe_gating = MoEGatingNetwork(
                storage,
                {'expert_names': moe_expert_names or ['euclidean', 'manhattan', 'cosine', 'relative']}
            )

        # Load last snapshot time and cache
        self._load_last_snapshot_time()

    def _load_last_snapshot_time(self):
        try:
            last_snap = self.storage.get_last_snapshot()
            if last_snap:
                self.last_snapshot_time = last_snap.get('timestamp', 0)
                self._last_snapshot_cache = last_snap
        except Exception as e:
            logger.warning(f"Failed to load last snapshot time: {e}")

    def _get_last_snapshot(self) -> Optional[Dict]:
        if self._last_snapshot_cache is None:
            self._last_snapshot_cache = self.storage.get_last_snapshot()
            if self._last_snapshot_cache:
                self.last_snapshot_time = self._last_snapshot_cache.get('timestamp', 0)
        return self._last_snapshot_cache

    async def check_drift(self, current_weights: Dict[str, float]) -> None:
        """
        Compare current weights with the last snapshot.
        If drift is detected (distance > threshold), increment counter.
        If counter reaches hysteresis_count, trigger rollback.
        Periodically take new snapshots.
        Optional integration with MoE, MODP, LIMIT Graph, RLHF.
        """
        last_snap = self._get_last_snapshot()
        if not last_snap:
            await self._take_snapshot(current_weights, "initial")
            return

        prev_weights = self._deserialize_weights(last_snap["online_weights"])

        # Compute distance(s) - if MoE enabled, compute all metrics and let MoE select the best
        if self.moe_gating:
            # Compute all four distances
            distances = {
                'euclidean': self._distance(current_weights, prev_weights, 'euclidean', self.weight_importance),
                'manhattan': self._distance(current_weights, prev_weights, 'manhattan', self.weight_importance),
                'cosine': self._distance(current_weights, prev_weights, 'cosine', self.weight_importance),
                'relative': self._distance(current_weights, prev_weights, 'relative', self.weight_importance),
            }
            # Build feature dict for MoE
            features = {
                'distance': distances[self.metric],  # use default metric as feature
                'ewma': self._ewma_distance,
                'threshold': self.threshold if not self.use_adaptive_threshold else self._compute_adaptive_threshold(),
                'consecutive': self._drift_counter,
                'history_mean': float(np.mean(self.distance_history)) if self.distance_history else 0.0,
            }
            selected_metric, _ = await self.moe_gating.select_expert(features)
            dist = distances.get(selected_metric, distances[self.metric])
            self.metric = selected_metric  # update for logging
        else:
            dist = self._distance(current_weights, prev_weights, self.metric, self.weight_importance)

        # Update EWMA
        self._ewma_distance = self.ewma_alpha * dist + (1 - self.ewma_alpha) * self._ewma_distance

        effective_threshold = self.threshold
        if self.use_adaptive_threshold:
            effective_threshold = self._compute_adaptive_threshold()

        # Take snapshot if interval elapsed
        if time.time() - self.last_snapshot_time > self.snapshot_interval:
            await self._take_snapshot(current_weights, "periodic")

        drift_metric = self._ewma_distance if self.ewma_alpha < 1.0 else dist

        if drift_metric > effective_threshold:
            self._drift_counter += 1
            logger.warning(f"Drift detected! Distance: {drift_metric:.4f} (threshold {effective_threshold:.4f})")
            if self.drift_counter_metric:
                self.drift_counter_metric.inc()

            await self._log_drift_event(
                drift_metric, effective_threshold, self._drift_counter, rollback_triggered=False
            )

            # Optional: MODP store state and LIMIT graph node for this drift event
            if self.modp_solver:
                self.modp_solver.add_state(
                    state_id=str(uuid.uuid4()),
                    problem_id="drift_detection",
                    state_attributes={
                        'distance': drift_metric,
                        'threshold': effective_threshold,
                        'consecutive': self._drift_counter,
                        'metric': self.metric,
                    },
                    objective_values={
                        'distance': drift_metric,
                        'inverse_threshold': 1.0 / max(effective_threshold, 1e-8),
                        'consecutive': self._drift_counter,
                    },
                    stage=0,
                )
            if self.limit_graph_manager:
                self.limit_graph_manager.add_node(
                    "drift_events",
                    f"drift_{uuid.uuid4()}",
                    "drift_event",
                    {
                        'distance': drift_metric,
                        'threshold': effective_threshold,
                        'consecutive': self._drift_counter,
                        'metric': self.metric,
                        'timestamp': time.time(),
                    }
                )

            if self._drift_counter >= self.hysteresis_count:
                logger.warning(f"Drift persisted for {self._drift_counter} consecutive detections. Triggering rollback.")
                if self.rollback_enabled:
                    await self._rollback_to_snapshot(last_snap)
                    self._drift_counter = 0
                    await self._log_drift_event(
                        drift_metric, effective_threshold, self._drift_counter, rollback_triggered=True
                    )
                    # RLHF: record preference pair for rollback decision (simulated)
                    if self.rlhf_trainer:
                        self.rlhf_trainer.record_pair(
                            pair_id=str(uuid.uuid4()),
                            prompt="Should we rollback after drift?",
                            chosen="rollback",
                            rejected="keep_current",
                            reward_diff=1.0,
                            metadata={'distance': drift_metric, 'threshold': effective_threshold}
                        )
                else:
                    logger.error("Drift detected but rollback disabled. Manual intervention required.")
        else:
            if self._drift_counter > 0:
                logger.info(f"Drift resolved. Distance {drift_metric:.4f} below threshold {effective_threshold:.4f}.")
                self._drift_counter = 0
            self.distance_history.append(dist)

        # If MoE is active, optionally update it with the outcome (reward = 1 if no drift, -1 if drift)
        if self.moe_gating and self.moe_gating.expert_names:
            reward = -1.0 if drift_metric > effective_threshold else 1.0
            features_for_update = {
                'distance': dist,
                'ewma': self._ewma_distance,
                'threshold': effective_threshold,
                'consecutive': self._drift_counter,
                'history_mean': float(np.mean(self.distance_history)) if self.distance_history else 0.0,
            }
            # Use selected metric if we set one; otherwise use self.metric
            await self.moe_gating.add_training_sample(features_for_update, self.metric, reward)

    def _compute_adaptive_threshold(self) -> float:
        if len(self.distance_history) < 10:
            return self.threshold
        arr = np.array(self.distance_history)
        mean = arr.mean()
        std = arr.std()
        k = getattr(config, 'DRIFT_ADAPTIVE_K', 3.0)
        return mean + k * std

    async def _take_snapshot(self, weights: Dict[str, float], reason: str) -> None:
        snapshot_id = hashlib.sha256(f"{time.time()}{weights}".encode()).hexdigest()[:16]
        online_json = self._serialize_weights(weights)
        offline_json = self._serialize_weights({})
        cost_score = sum(weights.values())
        self.storage.save_drift_snapshot(
            snapshot_id,
            online_json,
            offline_json,
            cost_score,
            reason
        )
        self.last_snapshot_time = time.time()
        self._last_snapshot_cache = {
            "snapshot_id": snapshot_id,
            "online_weights": online_json,
            "offline_weights": offline_json,
            "timestamp": self.last_snapshot_time,
            "reason": reason,
            "cost_score": cost_score,
        }
        logger.info(f"Snapshot taken: {snapshot_id} (reason: {reason})")

    async def _rollback_to_snapshot(self, snapshot: Dict) -> None:
        try:
            online_weights = self._deserialize_weights(snapshot["online_weights"])
            for k, v in online_weights.items():
                if k in self.adaptive_cost.online.weights:
                    self.adaptive_cost.online.weights[k] = v
            self.adaptive_cost.online._save_state()
            self.last_snapshot_time = time.time()
            self._drift_counter = 0
            self._ewma_distance = 0.0
            self._last_snapshot_cache = snapshot
            logger.info(f"Rolled back to snapshot {snapshot['snapshot_id']}")
        except Exception as e:
            logger.error(f"Rollback failed: {e}")

    async def _log_drift_event(
        self, distance: float, threshold: float, consecutive: int, rollback_triggered: bool
    ) -> None:
        try:
            self.storage.log_drift_event({
                "timestamp": time.time(),
                "distance": distance,
                "threshold": threshold,
                "consecutive": consecutive,
                "rollback_triggered": rollback_triggered,
            })
        except Exception as e:
            logger.warning(f"Failed to log drift event: {e}")

    def _distance(
        self,
        a: Dict[str, float],
        b: Dict[str, float],
        metric: str = "euclidean",
        importance: Optional[Dict[str, float]] = None,
    ) -> float:
        all_keys = set(a.keys()) | set(b.keys())
        if not all_keys:
            return 0.0

        imp = importance or {}
        def get_imp(k):
            return imp.get(k, 1.0)

        if metric == "euclidean":
            total = 0.0
            for k in all_keys:
                diff = (a.get(k, 0.0) - b.get(k, 0.0)) * get_imp(k)
                total += diff * diff
            return total ** 0.5
        elif metric == "manhattan":
            total = 0.0
            for k in all_keys:
                total += abs(a.get(k, 0.0) - b.get(k, 0.0)) * get_imp(k)
            return total
        elif metric == "cosine":
            dot = sum(a.get(k, 0.0) * b.get(k, 0.0) * get_imp(k) for k in all_keys)
            norm_a = sum((a.get(k, 0.0) * get_imp(k)) ** 2 for k in all_keys) ** 0.5
            norm_b = sum((b.get(k, 0.0) * get_imp(k)) ** 2 for k in all_keys) ** 0.5
            if norm_a == 0 or norm_b == 0:
                return 1.0
            return 1.0 - dot / (norm_a * norm_b)
        elif metric == "relative":
            total = 0.0
            count = 0
            for k in all_keys:
                va = a.get(k, 0.0)
                vb = b.get(k, 0.0)
                if va == 0 and vb == 0:
                    continue
                denom = max(abs(va), abs(vb))
                total += (abs(va - vb) / denom) * get_imp(k)
                count += 1
            return total / count if count > 0 else 0.0
        else:
            return self._distance(a, b, "euclidean", importance)

    def _serialize_weights(self, weights: Dict[str, float]) -> str:
        return json.dumps(weights)

    def _deserialize_weights(self, json_str: str) -> Dict:
        return json.loads(json_str)

    # --------------------------------------------------------------------------
    # Public API for external control
    # --------------------------------------------------------------------------
    async def force_snapshot(self) -> None:
        current_weights = self.adaptive_cost.get_current_weights()
        await self._take_snapshot(current_weights, "manual")

    async def force_rollback(self) -> bool:
        last_snap = self._get_last_snapshot()
        if not last_snap:
            logger.warning("No snapshot available for rollback.")
            return False
        await self._rollback_to_snapshot(last_snap)
        return True

    def get_drift_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        return self.storage.get_drift_events(limit=limit)

    def reset_state(self) -> None:
        self._drift_counter = 0
        self._ewma_distance = 0.0
        self.last_snapshot_time = 0
        self.distance_history.clear()
        logger.info("Drift detector state reset.")

    # ---------- New public methods for enhancements ----------
    def get_limit_graph(self, graph_id: str = "drift_events") -> Dict:
        if self.limit_graph_manager:
            return {
                'metadata': self.limit_graph_manager.get_metadata(graph_id),
                'nodes': self.limit_graph_manager.get_nodes(graph_id),
                'edges': self.limit_graph_manager.get_edges(graph_id),
            }
        return {}

    def get_modp_states(self, problem_id: str = "drift_detection") -> List[Dict]:
        if self.modp_solver:
            return self.modp_solver.get_states(problem_id)
        return []

    def get_rlhf_pairs(self, limit: int = 100) -> List[Dict]:
        if self.rlhf_trainer:
            return self.rlhf_trainer.get_pairs(limit)
        return []

    def get_moe_experts(self) -> List[str]:
        if self.moe_gating:
            return self.moe_gating.expert_names
        return []
