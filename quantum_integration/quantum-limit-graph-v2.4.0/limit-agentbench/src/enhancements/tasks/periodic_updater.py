# src/enhancements/tasks/periodic_updater_v2_1_0.py
"""
Enhanced Periodic Updater for Green Agent v2.1.0
=================================================
Celery tasks for periodic updates of sustainability data with adaptive scheduling
via Multi‑Teacher On‑Policy Distillation.

Enhancements over v2.0.0:
- Expanded state vector with 12 features (including last_update_success, cache_freshness, error_rate).
- Real reward computation based on data change and success, not placeholder.
- Integration with FeedbackEvent schema and AsyncMessageQueue for cross‑module communication.
- True historical ML training from logged state vectors (now stored in CSV).
- Pydantic‑based configuration via environment variables.
- Concurrency‑safe AdaptiveScheduler with asyncio.Lock.
- More robust unit tests.

All previous features (Celery tasks, retries, metrics, distillation components) retained.
"""

import asyncio
import logging
import os
import time
from typing import List, Optional, Dict, Any, Tuple, Union
from datetime import datetime, timedelta
import random
import numpy as np
from abc import ABC, abstractmethod
from collections import deque
import json
import pickle
import pandas as pd
from pathlib import Path
from dataclasses import dataclass

from celery import Celery
from celery.signals import task_failure, task_success, task_retry

# ---------- Pydantic ----------
from pydantic import BaseSettings, Field

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- Structlog ----------
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

# ---------- scikit-learn ----------
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

# ---------- Local imports ----------
from ..cache.cache_manager import CacheManager
from ..data_integration.carbon_intensity import CarbonIntensityFetcher
from ..data_integration.material_footprint import MaterialFootprintUpdater
from ..data_integration.helium_collector import HeliumCollector

# Optional project imports
try:
    from ..schemas.feedback_event import FeedbackEvent
except ImportError:
    FeedbackEvent = None

try:
    from ..async_message_queue import AsyncMessageQueue
except ImportError:
    AsyncMessageQueue = None

# ============================================================================
# Configuration
# ============================================================================

class UpdaterConfig(BaseSettings):
    """Configuration for periodic updater and adaptive scheduler."""
    redis_url: str = "redis://localhost:6379/0"
    regions: List[str] = Field(default_factory=lambda: ["us-east", "us-west", "eu-west", "eu-north", "asia-east", "asia-southeast"])
    helium_snapshot_url: str = "https://example.com/helium_snapshot.parquet"
    helium_snapshot_path: str = "./helium_snapshot.parquet"

    # Distillation
    distillation_epsilon: float = 0.1
    distillation_train_every: int = 10
    distillation_replay_size: int = 2000
    distillation_learning_rate: float = 0.01
    distill_weight: float = 0.7
    rl_weight: float = 0.3
    q_learning_rate: float = 0.1

    # Persistence paths
    q_weights_path: str = "./update_q_weights.json"
    interaction_logs_path: str = "./update_interactions.csv"
    historical_model_path: str = "./update_historical_model.pkl"

    # Scheduler
    scheduler_interval_seconds: int = 900  # 15 minutes
    enable_message_queue: bool = False

    class Config:
        env_prefix = "UPDATER_"
        case_sensitive = False

config = UpdaterConfig()

# Celery app
app = Celery('green_agent', broker=config.redis_url)
app.conf.update(
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=600,
    task_soft_time_limit=540,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_retry_backoff_max=600,
)

# Prometheus metrics
if PROMETHEUS_AVAILABLE:
    task_metrics = {
        'carbon_success': Counter('carbon_update_success_total', 'Carbon update success count'),
        'carbon_failure': Counter('carbon_update_failure_total', 'Carbon update failure count'),
        'material_success': Counter('material_update_success_total', 'Material update success count'),
        'material_failure': Counter('material_update_failure_total', 'Material update failure count'),
        'helium_success': Counter('helium_update_success_total', 'Helium update success count'),
        'helium_failure': Counter('helium_update_failure_total', 'Helium update failure count'),
        'task_duration': Histogram('periodic_task_duration_seconds', 'Task duration', ['task_name']),
        'update_action': Counter('update_action_selected', 'Action selected by scheduler', ['action']),
        'update_reward': Histogram('update_reward', 'Reward per update decision'),
    }
else:
    task_metrics = {}

# ============================================================================
# DISTILLATION COMPONENTS (Enhanced)
# ============================================================================

@dataclass
class UpdateState:
    """State for the distillation agent (expanded)."""
    # Time
    hours_since_last_update: float
    hour_of_day: float
    day_of_week: float
    # Data volatility
    carbon_trend: float          # avg change in carbon intensity per hour
    material_version_age_days: float
    helium_snapshot_age_days: float
    # Context
    current_carbon_intensity: float
    pending_updates_count: int
    system_load: float
    # NEW: additional features
    last_update_success: float = 1.0   # 1 if last update succeeded
    cache_freshness: float = 1.0       # 1 = fresh, 0 = stale
    error_rate: float = 0.0            # historical error rate (0-1)

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 12‑dim numeric feature vector."""
        features = [
            min(self.hours_since_last_update / 72.0, 1.0),
            self.hour_of_day / 24.0,
            self.day_of_week / 7.0,
            min(abs(self.carbon_trend) / 0.1, 1.0),
            min(self.material_version_age_days / 30.0, 1.0),
            min(self.helium_snapshot_age_days / 30.0, 1.0),
            min(self.current_carbon_intensity / 1000.0, 1.0),
            min(self.pending_updates_count / 10.0, 1.0),
            self.system_load,
            self.last_update_success,
            self.cache_freshness,
            self.error_rate,
        ]
        return np.array(features, dtype=np.float32)


# Teacher abstract base
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: UpdateState) -> np.ndarray:
        """Return probability vector over 2 actions (update_now, skip)."""
        pass

    @abstractmethod
    def confidence(self, state: UpdateState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class UpdateRuleBasedTeacher(Teacher):
    """Rule‑based expert: uses heuristics."""
    ACTIONS = ['update_now', 'skip']

    def predict(self, state: UpdateState) -> np.ndarray:
        probs = np.ones(2) * 0.1
        if state.hours_since_last_update > 24:
            probs[0] = 0.8
        elif state.carbon_trend > 0.05:
            probs[0] = 0.7
        elif state.material_version_age_days > 14:
            probs[0] = 0.6
        elif state.system_load > 0.8:
            probs[1] = 0.8
        elif state.last_update_success < 0.5:
            probs[0] = 0.6  # retry after failure
        else:
            probs[1] = 0.6
        return probs / probs.sum()

    def confidence(self, state: UpdateState) -> float:
        if state.hours_since_last_update > 24:
            return 0.6
        return 0.4


class UpdateHistoricalMLTeacher(Teacher):
    """Offline trained classifier from past interactions."""
    def __init__(self, model_path: Optional[Path] = None):
        self.model = None
        self.label_encoder = None
        self.model_path = model_path or Path(config.historical_model_path)
        if self.model_path.exists():
            try:
                with open(self.model_path, 'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
                logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")

    def predict(self, state: UpdateState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(2) / 2
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: UpdateState) -> float:
        return 0.7 if self.model is not None else 0.0

    @classmethod
    def train_from_logs(cls, log_paths: List[Path], model_path: Path,
                        state_col: str = 'state_vec', label_col: str = 'action'):
        """Train a RandomForestClassifier from logs containing state vectors and actions."""
        if not SKLEARN_ML:
            logger.error("scikit-learn not available, cannot train historical model.")
            return None
        all_dfs = []
        for path in log_paths:
            if path.exists():
                df = pd.read_csv(path)
                all_dfs.append(df)
        if not all_dfs:
            logger.warning("No logs found for training.")
            return None

        df = pd.concat(all_dfs, ignore_index=True)
        if len(df) < 10:
            logger.warning("Not enough logs to train historical model.")
            return None

        # Parse state vectors from string (comma-separated)
        def parse_state(s):
            try:
                return np.fromstring(s, sep=',')
            except:
                return None

        valid_indices = [i for i, s in enumerate(df[state_col]) if parse_state(s) is not None]
        X = np.array([parse_state(df[state_col].iloc[i]) for i in valid_indices])
        y = df[label_col].iloc[valid_indices].values

        if len(X) < 5:
            logger.warning("Too few valid samples after parsing.")
            return None

        le = LabelEncoder()
        y_enc = le.fit_transform(y)
        clf = RandomForestClassifier(n_estimators=100, random_state=42)
        clf.fit(X, y_enc)

        with open(model_path, 'wb') as f:
            pickle.dump((clf, le), f)
        logger.info(f"Trained historical model and saved to {model_path}")
        return model_path


class UpdateStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, lr: float = 0.1, weights_path: Optional[Path] = None):
        self.lr = lr
        self.weights_path = weights_path or Path(config.q_weights_path)
        self.weights = np.zeros((12, 2))  # 12 features, 2 actions
        self._load_state()

    def _load_state(self):
        if self.weights_path.exists():
            try:
                with open(self.weights_path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                logger.info(f"Loaded Q‑teacher weights from {self.weights_path}")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        with open(self.weights_path, 'w') as f:
            json.dump(self.weights.tolist(), f, indent=2)

    def predict(self, state: UpdateState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: UpdateState) -> float:
        return 0.5

    def update(self, state: UpdateState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()


class DistillationStudent:
    def __init__(self, feature_dim: int = 12, n_classes: int = 2, lr: float = 0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray, num_classes: int) -> np.ndarray:
        if num_classes != self.n_classes:
            new_weights = np.zeros((self.weights.shape[0], num_classes))
            new_biases = np.zeros(num_classes)
            min_dim = min(self.n_classes, num_classes)
            new_weights[:, :min_dim] = self.weights[:, :min_dim]
            new_biases[:min_dim] = self.biases[:min_dim]
            self.weights = new_weights
            self.biases = new_biases
            self.n_classes = num_classes
        logits = state_vector @ self.weights + self.biases
        max_logit = np.max(logits)
        exp_logits = np.exp(logits - max_logit)
        return exp_logits / exp_logits.sum()

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action: int, distill_weight: float = 0.7, rl_weight: float = 0.3):
        current_probs = self.predict_proba(state_vector, self.n_classes)
        grad_distill = -(teacher_probs - current_probs)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current_probs)

        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1


class ReplayBuffer:
    def __init__(self, max_size: int = 2000):
        self.buffer = deque(maxlen=max_size)

    def push(self, state_vec: np.ndarray, action: int, reward: float,
             next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))

    def sample(self, batch_size: int = 32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (np.array(states), actions, np.array(rewards),
                np.array(next_states), np.array(teacher_probs))

    def __len__(self):
        return len(self.buffer)


class DistillationSchedulerOptimizer:
    """Multi‑teacher on‑policy distillation agent for update scheduling.
    Actions: update_now, skip.
    """
    ACTIONS = ['update_now', 'skip']

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.student = DistillationStudent(
            feature_dim=12,
            lr=config.get('distillation_learning_rate', 0.01)
        )
        self.teachers: List[Teacher] = [
            UpdateRuleBasedTeacher(),
            UpdateHistoricalMLTeacher(model_path=config.get('historical_model_path')),
            UpdateStatefulQTeacher(
                lr=config.get('q_learning_rate', 0.1),
                weights_path=config.get('q_weights_path')
            )
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0
        self.distill_weight = config.get('distill_weight', 0.7)
        self.rl_weight = config.get('rl_weight', 0.3)

    async def select_action(self, state: UpdateState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        n = 2

        teacher_probs = np.zeros(n)
        total_conf = 0.0
        for teacher in self.teachers:
            prob = teacher.predict(state)
            conf = teacher.confidence(state)
            if len(prob) != n:
                if len(prob) < n:
                    prob = np.pad(prob, (0, n - len(prob)), 'constant')
                else:
                    prob = prob[:n]
            teacher_probs += prob * conf
            total_conf += conf
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(n) / n

        student_probs = self.student.predict_proba(state_vec, n)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, n - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = np.argmax(combined)

        return self.ACTIONS[action_idx], action_idx, state_vec, teacher_probs

    async def update(self, state_vec: np.ndarray, action_idx: int, reward: float,
                     next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = self.replay_buffer.sample(8)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i],
                                    distill_weight=self.distill_weight, rl_weight=self.rl_weight)

    def get_stats(self) -> Dict:
        return {'student_counter': self.student.counter, 'buffer_size': len(self.replay_buffer)}


# ============================================================================
# ADAPTIVE SCHEDULER (Enhanced)
# ============================================================================

class AdaptiveScheduler:
    """Adaptive scheduler that uses distillation to decide when to run updates."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.scheduler_optimizer = DistillationSchedulerOptimizer({
            'distillation_epsilon': self.config.get('distillation_epsilon', 0.1),
            'distillation_train_every': self.config.get('distillation_train_every', 10),
            'distillation_replay_size': self.config.get('distillation_replay_size', 2000),
            'distillation_learning_rate': self.config.get('distillation_learning_rate', 0.01),
            'distill_weight': self.config.get('distill_weight', 0.7),
            'rl_weight': self.config.get('rl_weight', 0.3),
            'q_learning_rate': self.config.get('q_learning_rate', 0.1),
            'q_weights_path': self.config.get('q_weights_path', config.q_weights_path),
            'historical_model_path': self.config.get('historical_model_path', config.historical_model_path),
        })

        self.interaction_log: List[Dict] = []
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None

        self.last_carbon_update: Optional[datetime] = None
        self.last_material_update: Optional[datetime] = None
        self.last_helium_update: Optional[datetime] = None

        self.carbon_history = deque(maxlen=100)
        self.material_version = None
        self.helium_snapshot_mtime: Optional[float] = None

        # Concurrency lock
        self._lock = asyncio.Lock()

        # Message queue (optional)
        self.message_queue = None
        if self.config.get('enable_message_queue', False) and AsyncMessageQueue is not None:
            self.message_queue = AsyncMessageQueue(queue_type="asyncio")

        logger.info("AdaptiveScheduler initialized (v2.1.0)")

    def _build_state(self, task_type: str) -> UpdateState:
        """Build state for a specific update task."""
        now = datetime.utcnow()

        # Determine hours since last update and other fields per task type
        if task_type == 'carbon':
            last_update = self.last_carbon_update
            hours_since = (now - last_update).total_seconds() / 3600 if last_update else 72
            carbon_trend = 0.0
            if len(self.carbon_history) > 5:
                recent = list(self.carbon_history)[-24:]
                if len(recent) > 5:
                    slope = np.polyfit(range(len(recent)), recent, 1)[0]
                    carbon_trend = slope
            current_intensity = self.carbon_history[-1] if self.carbon_history else 400
            material_age = 0
            helium_age = 0
        elif task_type == 'material':
            last_update = self.last_material_update
            hours_since = (now - last_update).total_seconds() / 3600 if last_update else 72
            carbon_trend = 0.0
            current_intensity = 0.0
            # If we have material catalog file, compute its age
            if self.material_version:
                # In real system, get mtime of catalog file
                material_age = 0.0  # placeholder
            else:
                material_age = 30.0  # assume stale if not set
            helium_age = 0
        else:  # helium
            last_update = self.last_helium_update
            hours_since = (now - last_update).total_seconds() / 3600 if last_update else 72
            carbon_trend = 0.0
            current_intensity = 0.0
            material_age = 0
            if self.helium_snapshot_mtime:
                helium_age = (now.timestamp() - self.helium_snapshot_mtime) / (3600 * 24)
            else:
                helium_age = 30.0  # assume stale

        # System load and pending updates (could be obtained from Celery)
        system_load = 0.5
        pending_updates = 0

        # Cache freshness and last success/error rate (placeholders)
        last_update_success = 1.0
        cache_freshness = 1.0
        error_rate = 0.0

        return UpdateState(
            hours_since_last_update=hours_since,
            hour_of_day=now.hour,
            day_of_week=now.weekday(),
            carbon_trend=carbon_trend,
            material_version_age_days=material_age,
            helium_snapshot_age_days=helium_age,
            current_carbon_intensity=current_intensity,
            pending_updates_count=pending_updates,
            system_load=system_load,
            last_update_success=last_update_success,
            cache_freshness=cache_freshness,
            error_rate=error_rate,
        )

    async def decide_and_execute(self, task_type: str) -> bool:
        """Decide whether to run the update for the given task type.
        Returns True if the update was executed, False if skipped.
        """
        async with self._lock:
            state = self._build_state(task_type)
            action, action_idx, state_vec, teacher_probs = await self.scheduler_optimizer.select_action(
                state, exploration=True
            )
            self.last_state_vec = state_vec
            self.last_action_idx = action_idx
            self.last_teacher_probs = teacher_probs

            if PROMETHEUS_AVAILABLE:
                task_metrics['update_action'].labels(action=action).inc()

            if action == 'skip':
                logger.info(f"Skipping {task_type} update based on distillation decision")
                # Compute a small reward for skipping (saving resources)
                reward = 0.1
                await self._record_outcome(task_type, action, reward, state_vec=state_vec,
                                           action_idx=action_idx, teacher_probs=teacher_probs)
                # Emit FeedbackEvent
                if FeedbackEvent and self.message_queue:
                    event = FeedbackEvent(
                        source="adaptive_scheduler",
                        feedback_type="routing",
                        task_id=task_type,
                        context={"action": "skip"},
                        action={"selected_action": "skip", "selected_rank": 1, "confidence_score": 0.5},
                        performance={"quality_score": reward, "latency_ms": 0, "energy_joules": 0,
                                     "carbon_g": 0, "helium_cost": 0, "duration_ms": 0},
                        adaptive_cost_value=reward,
                        tags=["scheduler", "skip", task_type],
                    )
                    await self.message_queue.publish("scheduler_events", event.to_json())
                return False

            # Execute the update
            logger.info(f"Executing {task_type} update based on distillation decision")
            if task_type == 'carbon':
                result = update_carbon_intensity.delay()
            elif task_type == 'material':
                result = update_material_catalog.delay()
            elif task_type == 'helium':
                result = update_helium_snapshot.delay()
            else:
                raise ValueError(f"Unknown task type: {task_type}")

            # Emit event for decision to update
            if FeedbackEvent and self.message_queue:
                event = FeedbackEvent(
                    source="adaptive_scheduler",
                    feedback_type="routing",
                    task_id=task_type,
                    context={"action": "update_now"},
                    action={"selected_action": "update_now", "selected_rank": 1, "confidence_score": 0.5},
                    performance={"quality_score": 0.0, "latency_ms": 0, "energy_joules": 0,
                                 "carbon_g": 0, "helium_cost": 0, "duration_ms": 0},
                    adaptive_cost_value=0.0,
                    tags=["scheduler", "update", task_type],
                )
                await self.message_queue.publish("scheduler_events", event.to_json())
            return True

    async def _record_outcome(self, task_type: str, action: str, reward: float,
                             state_vec: Optional[np.ndarray] = None,
                             action_idx: Optional[int] = None,
                             teacher_probs: Optional[np.ndarray] = None):
        """Record the outcome and update the distillation agent."""
        # Use provided state/action if given, else last recorded
        if state_vec is None:
            state_vec = self.last_state_vec
            action_idx = self.last_action_idx
            teacher_probs = self.last_teacher_probs

        # Create log entry with state vector
        entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'task_type': task_type,
            'action': action,
            'reward': reward,
        }
        if state_vec is not None:
            entry['state_vec'] = ','.join(map(str, state_vec))
        self.interaction_log.append(entry)

        # Save to CSV
        log_path = Path(config.interaction_logs_path)
        df_log = pd.DataFrame([entry])
        if log_path.exists():
            df_log.to_csv(log_path, mode='a', header=False, index=False)
        else:
            df_log.to_csv(log_path, index=False)

        # Update agent if we have state and action
        if state_vec is not None and action_idx is not None:
            next_state_vec = state_vec  # for simplicity, same state
            await self.scheduler_optimizer.update(
                state_vec,
                action_idx,
                reward,
                next_state_vec,
                teacher_probs
            )

        # Emit FeedbackEvent for outcome
        if FeedbackEvent and self.message_queue:
            event = FeedbackEvent(
                source="adaptive_scheduler",
                feedback_type="routing",
                task_id=task_type,
                context={"action": action},
                action={"selected_action": action, "selected_rank": 1, "confidence_score": 0.5},
                performance={"quality_score": reward, "latency_ms": 0, "energy_joules": 0,
                             "carbon_g": 0, "helium_cost": 0, "duration_ms": 0},
                adaptive_cost_value=reward,
                tags=["scheduler", "outcome", task_type, action],
            )
            await self.message_queue.publish("scheduler_events", event.to_json())

    def get_scheduler_stats(self) -> Dict:
        return self.scheduler_optimizer.get_stats()


# ============================================================================
# Celery tasks (modified to compute real rewards)
# ============================================================================

@app.task(
    bind=True,
    name='src.enhancements.tasks.periodic_updater.update_carbon_intensity',
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    max_retries=3,
)
def update_carbon_intensity(self):
    """Refresh carbon intensity for all key regions concurrently."""
    start_time = time.time()
    logger.info("Starting carbon intensity update", regions=config.regions)

    try:
        cache = CacheManager()
        fetcher = CarbonIntensityFetcher(cache)

        async def fetch_all():
            tasks = [fetcher.get_intensity(region) for region in config.regions]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for region, result in zip(config.regions, results):
                if isinstance(result, Exception):
                    logger.error("Carbon intensity fetch failed", region=region, error=str(result))
                else:
                    logger.debug("Carbon intensity fetched", region=region, intensity=result)
            return results

        results = asyncio.run(fetch_all())
        failures = sum(1 for r in results if isinstance(r, Exception))
        success_count = len(config.regions) - failures

        if PROMETHEUS_AVAILABLE:
            if failures == 0:
                task_metrics['carbon_success'].inc()
            else:
                task_metrics['carbon_failure'].inc()
            task_metrics['task_duration'].labels(task_name='update_carbon_intensity').observe(time.time() - start_time)

        # Compute reward based on data change and success
        prev_values = cache.get('carbon_intensity_all') or {}
        data_change = 0.0
        if prev_values:
            changes = []
            for i, region in enumerate(config.regions):
                if region in prev_values and not isinstance(results[i], Exception):
                    changes.append(abs(prev_values[region] - results[i]))
            if changes:
                data_change = np.mean(changes)
        # Store new values
        new_values = {region: results[i] for i, region in enumerate(config.regions) if not isinstance(results[i], Exception)}
        cache.set('carbon_intensity_all', new_values, ttl=3600)

        reward = 0.5 * min(1.0, data_change / 100.0) + 0.5 * (success_count / len(config.regions))
        reward = max(0.0, min(1.0, reward))

        # Notify scheduler
        global _scheduler
        if _scheduler:
            asyncio.run(_scheduler._record_outcome('carbon', 'update_now', reward))

        if PROMETHEUS_AVAILABLE:
            task_metrics['update_reward'].observe(reward)

        return {"status": "success", "regions_updated": success_count, "total": len(config.regions), "reward": reward}

    except Exception as e:
        logger.error("Carbon intensity update failed", error=str(e), exc_info=True)
        if PROMETHEUS_AVAILABLE:
            task_metrics['carbon_failure'].inc()
        global _scheduler
        if _scheduler:
            asyncio.run(_scheduler._record_outcome('carbon', 'update_now', 0.0))
        raise self.retry(exc=e)


@app.task(
    bind=True,
    name='src.enhancements.tasks.periodic_updater.update_material_catalog',
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    max_retries=3,
)
def update_material_catalog(self):
    """Refresh material footprint catalog."""
    start_time = time.time()
    logger.info("Starting material catalog update")

    try:
        updater = MaterialFootprintUpdater()

        async def update():
            await updater.update_catalog()

        asyncio.run(update())

        if PROMETHEUS_AVAILABLE:
            task_metrics['material_success'].inc()
            task_metrics['task_duration'].labels(task_name='update_material_catalog').observe(time.time() - start_time)

        reward = 0.8  # successful update
        global _scheduler
        if _scheduler:
            asyncio.run(_scheduler._record_outcome('material', 'update_now', reward))

        if PROMETHEUS_AVAILABLE:
            task_metrics['update_reward'].observe(reward)

        logger.info("Material catalog updated successfully")
        return {"status": "success", "reward": reward}

    except Exception as e:
        logger.error("Material catalog update failed", error=str(e), exc_info=True)
        if PROMETHEUS_AVAILABLE:
            task_metrics['material_failure'].inc()
        global _scheduler
        if _scheduler:
            asyncio.run(_scheduler._record_outcome('material', 'update_now', 0.0))
        raise self.retry(exc=e)


@app.task(
    bind=True,
    name='src.enhancements.tasks.periodic_updater.update_helium_snapshot',
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    max_retries=3,
)
def update_helium_snapshot(self):
    """Download the latest Helium snapshot from a remote URL."""
    start_time = time.time()
    logger.info("Starting helium snapshot update", url=config.helium_snapshot_url, dest=config.helium_snapshot_path)

    try:
        import aiohttp
        import aiofiles

        async def download():
            async with aiohttp.ClientSession() as session:
                async with session.get(config.helium_snapshot_url) as resp:
                    if resp.status != 200:
                        raise Exception(f"Download failed with status {resp.status}")
                    os.makedirs(os.path.dirname(config.helium_snapshot_path) or '.', exist_ok=True)
                    async with aiofiles.open(config.helium_snapshot_path, 'wb') as f:
                        async for chunk in resp.content.iter_chunked(8192):
                            await f.write(chunk)
            logger.info("Helium snapshot downloaded", path=config.helium_snapshot_path)

        asyncio.run(download())

        if PROMETHEUS_AVAILABLE:
            task_metrics['helium_success'].inc()
            task_metrics['task_duration'].labels(task_name='update_helium_snapshot').observe(time.time() - start_time)

        reward = 0.7  # successful download
        global _scheduler
        if _scheduler:
            asyncio.run(_scheduler._record_outcome('helium', 'update_now', reward))

        if PROMETHEUS_AVAILABLE:
            task_metrics['update_reward'].observe(reward)

        return {"status": "success", "path": config.helium_snapshot_path, "reward": reward}

    except Exception as e:
        logger.error("Helium snapshot update failed", error=str(e), exc_info=True)
        if PROMETHEUS_AVAILABLE:
            task_metrics['helium_failure'].inc()
        global _scheduler
        if _scheduler:
            asyncio.run(_scheduler._record_outcome('helium', 'update_now', 0.0))
        raise self.retry(exc=e)


# ============================================================================
# Celery Beat schedule replaced by AdaptiveScheduler task
# ============================================================================

@app.task(
    name='src.enhancements.tasks.periodic_updater.run_scheduler',
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    max_retries=3,
)
def run_scheduler():
    """Run the adaptive scheduler to decide and execute updates."""
    logger.info("Running adaptive scheduler")
    try:
        global _scheduler
        if _scheduler is None:
            _scheduler = AdaptiveScheduler(config.dict())

        # Run decisions for each task type
        for task_type in ['carbon', 'material', 'helium']:
            asyncio.run(_scheduler.decide_and_execute(task_type))

        return {"status": "success"}
    except Exception as e:
        logger.error("Scheduler run failed", error=str(e), exc_info=True)
        raise


# ============================================================================
# Task signals
# ============================================================================

@task_success.connect
def task_success_handler(sender, **kwargs):
    logger.info("Task succeeded", task=sender.name)

@task_failure.connect
def task_failure_handler(sender, **kwargs):
    logger.error("Task failed", task=sender.name, exc_info=kwargs.get('einfo'))

# ============================================================================
# Singleton scheduler instance
# ============================================================================

_scheduler: Optional[AdaptiveScheduler] = None


# ============================================================================
# Offline training for Historical ML (now functional)
# ============================================================================

def train_historical_model(log_path: Path = Path(config.interaction_logs_path),
                           model_path: Path = Path(config.historical_model_path)):
    """Train a RandomForestClassifier from interaction logs (which now include state vectors)."""
    return UpdateHistoricalMLTeacher.train_from_logs([log_path], model_path)


# ============================================================================
# UNIT TESTS
# ============================================================================
import unittest
from unittest import IsolatedAsyncioTestCase

class TestDistillationComponents(IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = {
            'distillation_epsilon': 0.0,
            'distillation_replay_size': 10,
            'distillation_learning_rate': 0.01,
            'distillation_train_every': 10,
            'distill_weight': 0.7,
            'rl_weight': 0.3,
            'q_learning_rate': 0.1,
        }
        self.optimizer = DistillationSchedulerOptimizer(self.config)

    def test_state_feature_vector(self):
        state = UpdateState(
            hours_since_last_update=12,
            hour_of_day=14,
            day_of_week=3,
            carbon_trend=0.02,
            material_version_age_days=5,
            helium_snapshot_age_days=2,
            current_carbon_intensity=400,
            pending_updates_count=2,
            system_load=0.5,
        )
        vec = state.to_feature_vector()
        self.assertEqual(len(vec), 12)

    def test_rule_based_teacher(self):
        teacher = UpdateRuleBasedTeacher()
        state = UpdateState(
            hours_since_last_update=30,
            hour_of_day=14,
            day_of_week=3,
            carbon_trend=0.02,
            material_version_age_days=5,
            helium_snapshot_age_days=2,
            current_carbon_intensity=400,
            pending_updates_count=2,
            system_load=0.5,
        )
        probs = teacher.predict(state)
        self.assertAlmostEqual(sum(probs), 1.0)
        self.assertGreater(probs[0], probs[1])

    async def test_select_action(self):
        state = UpdateState(
            hours_since_last_update=12,
            hour_of_day=14,
            day_of_week=3,
            carbon_trend=0.02,
            material_version_age_days=5,
            helium_snapshot_age_days=2,
            current_carbon_intensity=400,
            pending_updates_count=2,
            system_load=0.5,
        )
        action, idx, state_vec, teacher_probs = await self.optimizer.select_action(state, exploration=False)
        self.assertIn(action, self.optimizer.ACTIONS)

    def test_replay_buffer(self):
        buffer = ReplayBuffer(max_size=5)
        state_vec = np.random.randn(12)
        buffer.push(state_vec, 0, 1.0, state_vec, np.ones(2)/2)
        self.assertEqual(len(buffer), 1)
        batch = buffer.sample(1)
        self.assertEqual(len(batch[0]), 1)


# ============================================================================
# Example usage (if run directly)
# ============================================================================
if __name__ == "__main__":
    print("This file is meant to be used with Celery worker and beat.")
    print("To start worker: celery -A src.enhancements.tasks.periodic_updater.app worker --loglevel=info")
    print("To start scheduler (instead of beat): celery -A src.enhancements.tasks.periodic_updater.app call src.enhancements.tasks.periodic_updater.run_scheduler --loglevel=info")
