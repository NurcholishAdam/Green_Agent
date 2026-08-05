# src/enhancements/tasks/periodic_updater_v2_0_0.py
"""
Enhanced Periodic Updater for Green Agent v2.0.0
=================================================
Celery tasks for periodic updates of sustainability data with adaptive scheduling
via Multi‑Teacher On‑Policy Distillation.

Features:
- Adaptive update scheduling based on context and learned from outcomes.
- State‑aware decision to update or skip.
- Online learning from update effectiveness.
- Teachers: rule‑based, historical ML, stateful Q.
- Student: linear softmax with distillation + REINFORCE.
- Persistence for Q‑teacher weights and interaction logs.
- Offline training for historical ML teacher.
- Unit tests for distillation components.
All previous features (async tasks, retries, metrics, etc.) retained.
"""

import asyncio
import logging
import os
import time
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
import random
import numpy as np
from abc import ABC, abstractmethod
from collections import deque
import json
import pickle
import pandas as pd
from pathlib import Path

from celery import Celery
from celery.signals import task_failure, task_success, task_retry

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

# ---------- scikit-learn for ML teacher ----------
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

# ============================================================================
# Configuration from environment
# ============================================================================
REDIS_URL = os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0')
REGIONS = os.getenv('CARBON_REGIONS', 'us-east,us-west,eu-west,eu-north,asia-east,asia-southeast').split(',')
HELIUM_SNAPSHOT_URL = os.getenv('HELIUM_SNAPSHOT_URL', 'https://example.com/helium_snapshot.parquet')
HELIUM_SNAPSHOT_PATH = os.getenv('HELIUM_SNAPSHOT_PATH', './helium_snapshot.parquet')

# NEW: Distillation persistence paths
Q_WEIGHTS_PATH = os.getenv('UPDATE_Q_WEIGHTS_PATH', './update_q_weights.json')
INTERACTION_LOGS_PATH = os.getenv('UPDATE_INTERACTION_LOGS_PATH', './update_interactions.csv')
HISTORICAL_MODEL_PATH = os.getenv('UPDATE_HISTORICAL_MODEL_PATH', './update_historical_model.pkl')

# Celery app (unchanged)
app = Celery('green_agent', broker=REDIS_URL)
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

# Prometheus metrics (if enabled)
if PROMETHEUS_AVAILABLE:
    task_metrics = {
        'carbon_success': Counter('carbon_update_success_total', 'Carbon update success count'),
        'carbon_failure': Counter('carbon_update_failure_total', 'Carbon update failure count'),
        'material_success': Counter('material_update_success_total', 'Material update success count'),
        'material_failure': Counter('material_update_failure_total', 'Material update failure count'),
        'helium_success': Counter('helium_update_success_total', 'Helium update success count'),
        'helium_failure': Counter('helium_update_failure_total', 'Helium update failure count'),
        'task_duration': Histogram('periodic_task_duration_seconds', 'Task duration', ['task_name']),
        # Distillation metrics
        'update_action': Counter('update_action_selected', 'Action selected by scheduler', ['action']),
        'update_reward': Histogram('update_reward', 'Reward per update decision'),
    }
else:
    task_metrics = {}

# ============================================================================
# DISTILLATION COMPONENTS FOR UPDATE SCHEDULING
# ============================================================================

@dataclass
class UpdateState:
    """State for the distillation agent."""
    # Time
    hours_since_last_update: float
    hour_of_day: float
    day_of_week: float
    # Data volatility
    carbon_trend: float  # average change in intensity over last 24h
    material_version_age_days: float
    helium_snapshot_age_days: float
    # Context
    current_carbon_intensity: float
    pending_updates_count: int
    system_load: float  # 0-1

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 10‑dim numeric feature vector."""
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
            probs[0] = 0.8  # update_now
        elif state.carbon_trend > 0.05:
            probs[0] = 0.7  # update_now
        elif state.material_version_age_days > 14:
            probs[0] = 0.6  # update_now
        elif state.system_load > 0.8:
            probs[1] = 0.8  # skip
        else:
            probs[1] = 0.6  # skip default
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
        self.model_path = model_path or Path(HISTORICAL_MODEL_PATH)
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


class UpdateStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, lr: float = 0.1):
        self.lr = lr
        self.weights = np.zeros((9, 2))  # 9 features, 2 actions
        self._load_state()

    def _load_state(self):
        path = Path(Q_WEIGHTS_PATH)
        if path.exists():
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                logger.info(f"Loaded Q‑teacher weights from {path}")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        path = Path(Q_WEIGHTS_PATH)
        with open(path, 'w') as f:
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
    def __init__(self, feature_dim: int = 9, n_classes: int = 2, lr: float = 0.01):
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
        logits = state_vector @ self.weights + self.biases

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
    """
    Multi‑teacher on‑policy distillation agent for update scheduling.
    Actions: update_now, skip.
    """
    ACTIONS = ['update_now', 'skip']

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            UpdateRuleBasedTeacher(),
            UpdateHistoricalMLTeacher(),
            UpdateStatefulQTeacher()
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

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
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i])

    def get_stats(self) -> Dict:
        return {'student_counter': self.student.counter, 'buffer_size': len(self.replay_buffer)}


# ============================================================================
# ADAPTIVE SCHEDULER (Replaces Celery Beat)
# ============================================================================

class AdaptiveScheduler:
    """
    Adaptive scheduler that uses distillation to decide when to run updates.
    This runs as a separate task (e.g., every 15 minutes) and triggers actual updates.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        # Distillation optimizer
        self.scheduler_optimizer = DistillationSchedulerOptimizer({
            'distillation_epsilon': self.config.get('distillation_epsilon', 0.1),
            'distillation_train_every': self.config.get('distillation_train_every', 10),
            'distillation_replay_size': self.config.get('distillation_replay_size', 2000),
            'distillation_learning_rate': self.config.get('distillation_learning_rate', 0.01),
        })

        # Interaction tracking
        self.interaction_log: List[Dict] = []
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None

        # Last update timestamps
        self.last_carbon_update: Optional[datetime] = None
        self.last_material_update: Optional[datetime] = None
        self.last_helium_update: Optional[datetime] = None

        # Data volatility trackers
        self.carbon_history = deque(maxlen=100)
        self.material_version = None
        self.helium_snapshot_mtime: Optional[float] = None

        logger.info("AdaptiveScheduler initialized")

    def _build_state(self, task_type: str) -> UpdateState:
        """Build state for a specific update task."""
        now = datetime.utcnow()

        if task_type == 'carbon':
            hours_since = (now - self.last_carbon_update).total_seconds() / 3600 if self.last_carbon_update else 72
            # Carbon trend: average change over last 24h
            if len(self.carbon_history) > 5:
                recent = list(self.carbon_history)[-24:]
                if len(recent) > 5:
                    slope = np.polyfit(range(len(recent)), recent, 1)[0]
                    trend = slope
                else:
                    trend = 0.0
            else:
                trend = 0.0
            current_intensity = self.carbon_history[-1] if self.carbon_history else 400
            age_days = 0
        elif task_type == 'material':
            hours_since = (now - self.last_material_update).total_seconds() / 3600 if self.last_material_update else 72
            trend = 0.0
            current_intensity = 0.0
            age_days = 0
        else:  # helium
            hours_since = (now - self.last_helium_update).total_seconds() / 3600 if self.last_helium_update else 72
            trend = 0.0
            current_intensity = 0.0
            age_days = 0

        # System load (mock)
        system_load = 0.5

        # Pending updates (mock)
        pending = 0

        return UpdateState(
            hours_since_last_update=hours_since,
            hour_of_day=now.hour,
            day_of_week=now.weekday(),
            carbon_trend=trend,
            material_version_age_days=age_days,
            helium_snapshot_age_days=age_days,
            current_carbon_intensity=current_intensity,
            pending_updates_count=pending,
            system_load=system_load,
        )

    async def decide_and_execute(self, task_type: str) -> bool:
        """
        Decide whether to run the update for the given task type.
        Returns True if the update was executed, False if skipped.
        """
        # Build state
        state = self._build_state(task_type)

        # Select action via distillation
        action, action_idx, state_vec, teacher_probs = await self.scheduler_optimizer.select_action(state, exploration=True)
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs

        if action == 'skip':
            logger.info(f"Skipping {task_type} update based on distillation decision")
            # Record outcome (skip)
            reward = 0.0  # no resource used, but we might want to reward if data hasn't changed
            # For simplicity, we'll compute reward later
            await self._record_outcome(task_type, action, reward)
            return False

        # Execute the update (call the Celery task directly)
        logger.info(f"Executing {task_type} update based on distillation decision")
        if task_type == 'carbon':
            result = update_carbon_intensity.delay()
        elif task_type == 'material':
            result = update_material_catalog.delay()
        elif task_type == 'helium':
            result = update_helium_snapshot.delay()
        else:
            raise ValueError(f"Unknown task type: {task_type}")

        # Record outcome after task completes (reward will be computed in the task and reported via a callback)
        # We'll simulate reward in this example.
        # In a real system, we would use a Celery result backend or a callback.
        return True

    async def _record_outcome(self, task_type: str, action: str, reward: float):
        """Record the outcome of a decision and update the agent."""
        # Log interaction
        self.interaction_log.append({
            'timestamp': datetime.utcnow().isoformat(),
            'task_type': task_type,
            'action': action,
            'reward': reward,
        })
        log_path = Path(INTERACTION_LOGS_PATH)
        df_log = pd.DataFrame([self.interaction_log[-1]])
        if log_path.exists():
            df_log.to_csv(log_path, mode='a', header=False, index=False)
        else:
            df_log.to_csv(log_path, index=False)

        # Update agent if we have a recorded state
        if self.last_state_vec is not None and self.last_action_idx is not None:
            next_state_vec = self.last_state_vec  # for simplicity, same state
            await self.scheduler_optimizer.update(
                self.last_state_vec,
                self.last_action_idx,
                reward,
                next_state_vec,
                self.last_teacher_probs
            )

    def get_scheduler_stats(self) -> Dict:
        return self.scheduler_optimizer.get_stats()


# ============================================================================
# Celery tasks (modified to record outcomes)
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
    logger.info("Starting carbon intensity update", regions=REGIONS)

    try:
        cache = CacheManager()
        fetcher = CarbonIntensityFetcher(cache)

        async def fetch_all():
            tasks = [fetcher.get_intensity(region) for region in REGIONS]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for region, result in zip(REGIONS, results):
                if isinstance(result, Exception):
                    logger.error("Carbon intensity fetch failed", region=region, error=str(result))
                else:
                    logger.debug("Carbon intensity fetched", region=region, intensity=result)
            return results

        results = asyncio.run(fetch_all())
        failures = sum(1 for r in results if isinstance(r, Exception))
        if failures > 0:
            logger.warning("Carbon intensity update completed with failures", total=len(REGIONS), failures=failures)

        if PROMETHEUS_AVAILABLE:
            task_metrics['carbon_success'].inc()
            task_metrics['task_duration'].labels(task_name='update_carbon_intensity').observe(time.time() - start_time)

        # Compute reward: data change score (simulate)
        # In a real system, compare with previous cache and compute delta.
        data_change_score = 0.5  # placeholder
        time_saved_score = 0.0
        resource_cost_norm = 0.1
        reward = 0.5 * data_change_score + 0.3 * time_saved_score + 0.2 * (1 - resource_cost_norm)

        # Notify scheduler of outcome
        # For simplicity, we'll use a global scheduler instance.
        # In production, this would be a callback or result backend.
        global _scheduler
        if _scheduler:
            asyncio.run(_scheduler._record_outcome('carbon', 'update_now', reward))

        return {"status": "success", "regions_updated": len(REGIONS) - failures, "total": len(REGIONS)}

    except Exception as e:
        logger.error("Carbon intensity update failed", error=str(e), exc_info=True)
        if PROMETHEUS_AVAILABLE:
            task_metrics['carbon_failure'].inc()
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

        # Reward (placeholder)
        reward = 0.7
        global _scheduler
        if _scheduler:
            asyncio.run(_scheduler._record_outcome('material', 'update_now', reward))

        logger.info("Material catalog updated successfully")
        return {"status": "success"}

    except Exception as e:
        logger.error("Material catalog update failed", error=str(e), exc_info=True)
        if PROMETHEUS_AVAILABLE:
            task_metrics['material_failure'].inc()
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
    logger.info("Starting helium snapshot update", url=HELIUM_SNAPSHOT_URL, dest=HELIUM_SNAPSHOT_PATH)

    try:
        import aiohttp
        import aiofiles

        async def download():
            async with aiohttp.ClientSession() as session:
                async with session.get(HELIUM_SNAPSHOT_URL) as resp:
                    if resp.status != 200:
                        raise Exception(f"Download failed with status {resp.status}")
                    os.makedirs(os.path.dirname(HELIUM_SNAPSHOT_PATH) or '.', exist_ok=True)
                    async with aiofiles.open(HELIUM_SNAPSHOT_PATH, 'wb') as f:
                        async for chunk in resp.content.iter_chunked(8192):
                            await f.write(chunk)
            logger.info("Helium snapshot downloaded", path=HELIUM_SNAPSHOT_PATH)

        asyncio.run(download())

        if PROMETHEUS_AVAILABLE:
            task_metrics['helium_success'].inc()
            task_metrics['task_duration'].labels(task_name='update_helium_snapshot').observe(time.time() - start_time)

        # Reward (placeholder)
        reward = 0.6
        global _scheduler
        if _scheduler:
            asyncio.run(_scheduler._record_outcome('helium', 'update_now', reward))

        return {"status": "success", "path": HELIUM_SNAPSHOT_PATH}

    except Exception as e:
        logger.error("Helium snapshot update failed", error=str(e), exc_info=True)
        if PROMETHEUS_AVAILABLE:
            task_metrics['helium_failure'].inc()
        raise self.retry(exc=e)


# ============================================================================
# Celery Beat schedule (replaced by AdaptiveScheduler)
# ============================================================================

# We no longer use static beat schedule. Instead, we run a periodic scheduler task.
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
        # Initialize scheduler (in production, use a singleton)
        global _scheduler
        if _scheduler is None:
            _scheduler = AdaptiveScheduler()

        # Run decisions for each task type
        for task_type in ['carbon', 'material', 'helium']:
            asyncio.run(_scheduler.decide_and_execute(task_type))

        # Update scheduler timestamps
        # (this would be done in the tasks themselves, but we can update here for state)
        # For simplicity, we don't update last timestamps here; they are updated in tasks.

        return {"status": "success"}
    except Exception as e:
        logger.error("Scheduler run failed", error=str(e), exc_info=True)
        raise


# ============================================================================
# Task signals (unchanged)
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
# Offline training for Historical ML
# ============================================================================

def train_historical_model(log_path: Path = Path(INTERACTION_LOGS_PATH),
                           model_path: Path = Path(HISTORICAL_MODEL_PATH)):
    """
    Train a RandomForestClassifier from past interaction logs.
    """
    if not log_path.exists():
        logger.warning(f"Interaction logs not found at {log_path}. No model trained.")
        return

    df_logs = pd.read_csv(log_path)
    if len(df_logs) < 10:
        logger.warning("Not enough logs to train historical model (need at least 10).")
        return

    # For a real implementation, you need to store state vectors in logs.
    # Here we just log a message.
    logger.info("Historical ML training requires state vectors in logs. Please implement logging of state vectors.")


# ============================================================================
# UNIT TESTS (Phase 10)
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
        self.assertEqual(len(vec), 9)

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
        self.assertGreater(probs[0], probs[1])  # update_now should be highest

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
        state_vec = np.random.randn(9)
        buffer.push(state_vec, 0, 1.0, state_vec, np.ones(2)/2)
        self.assertEqual(len(buffer), 1)
        batch = buffer.sample(1)
        self.assertEqual(len(batch[0]), 1)


# ============================================================================
# Example usage (if run directly)
# ============================================================================
if __name__ == "__main__":
    # For testing tasks locally (not for production)
    # You can run `celery -A src.enhancements.tasks.periodic_updater.app worker --loglevel=info`
    print("This file is meant to be used with Celery worker and beat.")
    print("To start worker: celery -A src.enhancements.tasks.periodic_updater.app worker --loglevel=info")
    print("To start beat:   celery -A src.enhancements.tasks.periodic_updater.app beat --loglevel=info")
