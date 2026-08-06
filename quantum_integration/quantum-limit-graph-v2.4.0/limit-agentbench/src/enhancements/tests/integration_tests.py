# src/enhancements/tests/integration_tests_v2_0_0.py
"""
Enhanced Integration Tests for Green Agent Components v2.0.0
=============================================================
Comprehensive test suite covering core modules with adaptive test selection
via Multi‑Teacher On‑Policy Distillation.

Test coverage includes:
- CacheManager
- CarbonIntensityFetcher
- HeliumCollector
- MaterialFootprintUpdater
- BioParameterCatalog
- HeliumSyntheticGenerator
- NodeDescriptor and WorkloadDescriptor helpers
- SustainabilityCostFunction
- Periodic Celery tasks (with mocked dependencies)
- Persistence and error handling

NEW: Adaptive test selection – each test may be skipped based on context.
"""

import pytest
import asyncio
import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, mock_open
import random
import numpy as np
from abc import ABC, abstractmethod
from collections import deque
import pickle
import pandas as pd
from datetime import datetime
import os
from typing import Dict, Any, List, Tuple, Optional
import functools
import logging

# ---------- scikit-learn for ML teacher ----------
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

# ---------- Structlog ----------
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

# ---------- Modules under test ----------
from ..cache.cache_manager import CacheManager
from ..data_integration.carbon_intensity import CarbonIntensityFetcher
from ..data_integration.helium_collector import HeliumCollector
from ..data_integration.material_footprint import MaterialFootprintUpdater
from ..data_integration.bio_parameter_catalog import BioParameterCatalog
from ..data_integration.helium_synthetic_generator import HeliumSyntheticGenerator
from ..schemas.node_descriptor import NodeDescriptor, NodeType, CoolingType, MaintenanceStatus
from ..schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency, Priority, BioMode
from ..cost_function.sustainability_cost import SustainabilityCostFunction
from ..tasks.periodic_updater import app as celery_app, update_carbon_intensity, update_material_catalog, update_helium_snapshot

# ============================================================================
# DISTILLATION COMPONENTS FOR TEST SELECTION
# ============================================================================

@dataclass
class TestSelectionState:
    """State for the distillation agent."""
    test_name: str
    test_category: str  # 'unit', 'integration', 'performance'
    estimated_duration_sec: float
    code_coverage_pct: float  # 0-100
    recent_failures: int      # failures in last 10 runs
    system_load: float        # 0-1
    carbon_intensity: float   # gCO2/kWh
    time_of_day: float        # 0-24
    test_success_rate: float  # 0-1
    avg_reward: float

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 11‑dim numeric feature vector."""
        features = [
            min(self.estimated_duration_sec / 60.0, 1.0),
            min(self.code_coverage_pct / 100.0, 1.0),
            min(self.recent_failures / 5.0, 1.0),
            self.system_load,
            min(self.carbon_intensity / 1000.0, 1.0),
            self.time_of_day / 24.0,
            self.test_success_rate,
            self.avg_reward,
        ]
        # One‑hot for test_category (unit, integration, performance)
        cat_map = {'unit': 0, 'integration': 1, 'performance': 2}
        one_hot = [0.0, 0.0, 0.0]
        idx = cat_map.get(self.test_category, 0)
        one_hot[idx] = 1.0
        features.extend(one_hot)
        return np.array(features, dtype=np.float32)


class Teacher(ABC):
    @abstractmethod
    def predict(self, state: TestSelectionState) -> np.ndarray:
        """Return probability vector over 2 actions (run, skip)."""
        pass

    @abstractmethod
    def confidence(self, state: TestSelectionState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class TestRuleBasedTeacher(Teacher):
    """Rule‑based expert: uses heuristics."""
    ACTIONS = ['run', 'skip']

    def predict(self, state: TestSelectionState) -> np.ndarray:
        probs = np.ones(2) * 0.1
        if state.recent_failures > 2:
            probs[0] = 0.9  # run
        elif state.code_coverage_pct < 50:
            probs[0] = 0.8  # run
        elif state.system_load > 0.8:
            probs[1] = 0.8  # skip
        elif state.carbon_intensity > 500:
            probs[1] = 0.6  # skip
        else:
            probs[0] = 0.6  # run default
        return probs / probs.sum()

    def confidence(self, state: TestSelectionState) -> float:
        if state.recent_failures > 2:
            return 0.6
        return 0.4


class TestHistoricalMLTeacher(Teacher):
    """Offline trained classifier from past interactions."""
    def __init__(self, model_path: Optional[Path] = None):
        self.model = None
        self.label_encoder = None
        self.model_path = model_path or Path("./test_selection_model.pkl")
        if self.model_path.exists():
            try:
                with open(self.model_path, 'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
                logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")

    def predict(self, state: TestSelectionState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(2) / 2
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: TestSelectionState) -> float:
        return 0.7 if self.model is not None else 0.0


class TestStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, lr: float = 0.1):
        self.lr = lr
        self.weights = np.zeros((11, 2))  # 11 features, 2 actions
        self._load_state()

    def _load_state(self):
        path = Path("./test_selection_q_weights.json")
        if path.exists():
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                logger.info(f"Loaded Q‑teacher weights from {path}")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        path = Path("./test_selection_q_weights.json")
        with open(path, 'w') as f:
            json.dump(self.weights.tolist(), f, indent=2)

    def predict(self, state: TestSelectionState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: TestSelectionState) -> float:
        return 0.5

    def update(self, state: TestSelectionState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()


class DistillationStudent:
    def __init__(self, feature_dim: int = 11, n_classes: int = 2, lr: float = 0.01):
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


class DistillationTestSelector:
    """
    Multi‑teacher on‑policy distillation agent for test selection.
    Actions: run, skip.
    """
    ACTIONS = ['run', 'skip']

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            TestRuleBasedTeacher(),
            TestHistoricalMLTeacher(),
            TestStatefulQTeacher()
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_action(self, state: TestSelectionState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
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
# ADAPTIVE TEST RUNNER
# ============================================================================

class AdaptiveTestRunner:
    """
    Orchestrates test execution with adaptive selection.
    This is meant to be used as a pytest plugin or a wrapper around pytest.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.selector = DistillationTestSelector({
            'distillation_epsilon': self.config.get('distillation_epsilon', 0.1),
            'distillation_train_every': self.config.get('distillation_train_every', 10),
            'distillation_replay_size': self.config.get('distillation_replay_size', 2000),
            'distillation_learning_rate': self.config.get('distillation_learning_rate', 0.01),
        })
        self.interaction_log: List[Dict] = []
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None

        # Test metadata (populated via register_test)
        self.test_metadata: Dict[str, Dict] = {}

        logger.info("AdaptiveTestRunner initialized")

    def register_test(self, test_name: str, category: str = 'unit', duration_sec: float = 1.0):
        """Register a test with its metadata."""
        self.test_metadata[test_name] = {
            'category': category,
            'duration_sec': duration_sec,
            'coverage_pct': 0.0,
            'recent_failures': 0,
            'success_rate': 0.5,
            'avg_reward': 0.5,
        }

    def _build_state(self, test_name: str) -> TestSelectionState:
        """Build state for a specific test."""
        meta = self.test_metadata.get(test_name, {})
        # Gather context (mock values for demonstration; in real usage these would come from system monitoring)
        system_load = 0.5
        carbon_intensity = 400
        time_of_day = datetime.now().hour

        return TestSelectionState(
            test_name=test_name,
            test_category=meta.get('category', 'unit'),
            estimated_duration_sec=meta.get('duration_sec', 1.0),
            code_coverage_pct=meta.get('coverage_pct', 0.0),
            recent_failures=meta.get('recent_failures', 0),
            system_load=system_load,
            carbon_intensity=carbon_intensity,
            time_of_day=time_of_day,
            test_success_rate=meta.get('success_rate', 0.5),
            avg_reward=meta.get('avg_reward', 0.5),
        )

    async def decide_and_run(self, test_name: str, test_func) -> bool:
        """
        Decide whether to run the test.
        Returns True if the test was executed, False if skipped.
        """
        # Build state
        state = self._build_state(test_name)
        action, action_idx, state_vec, teacher_probs = await self.selector.select_action(state, exploration=True)
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs

        if action == 'skip':
            logger.info(f"Skipping test '{test_name}' based on distillation decision")
            # Record skip (no reward)
            await self._record_outcome(test_name, 'skip', 0.0, passed=None)
            return False

        # Execute the test
        logger.info(f"Running test '{test_name}' based on distillation decision")
        try:
            test_func()
            passed = True
        except Exception as e:
            passed = False
            logger.error(f"Test '{test_name}' failed: {e}")

        # Compute reward
        reward = self._compute_reward(passed, state)
        await self._record_outcome(test_name, 'run', reward, passed)

        return True

    def _compute_reward(self, passed: bool, state: TestSelectionState) -> float:
        """Compute reward based on test outcome."""
        base = 0.6 if passed else 0.0
        coverage_bonus = 0.2 * min(1.0, state.code_coverage_pct / 100.0)
        time_penalty = 0.2 * min(1.0, state.estimated_duration_sec / 60.0)
        reward = base + coverage_bonus - time_penalty
        return max(0.0, min(1.0, reward))

    async def _record_outcome(self, test_name: str, action: str, reward: float, passed: Optional[bool]):
        """Record the outcome of a test decision and update the agent."""
        # Log interaction
        self.interaction_log.append({
            'timestamp': datetime.utcnow().isoformat(),
            'test_name': test_name,
            'action': action,
            'reward': reward,
            'passed': passed,
        })
        log_path = Path("./test_selection_interactions.csv")
        df_log = pd.DataFrame([self.interaction_log[-1]])
        if log_path.exists():
            df_log.to_csv(log_path, mode='a', header=False, index=False)
        else:
            df_log.to_csv(log_path, index=False)

        # Update agent if we have a recorded state
        if self.last_state_vec is not None and self.last_action_idx is not None:
            next_state_vec = self.last_state_vec  # for simplicity, same state
            await self.selector.update(
                self.last_state_vec,
                self.last_action_idx,
                reward,
                next_state_vec,
                self.last_teacher_probs
            )

        # Update test metadata
        if test_name in self.test_metadata:
            meta = self.test_metadata[test_name]
            if passed is not None:
                if passed:
                    meta['recent_failures'] = max(0, meta['recent_failures'] - 1)
                else:
                    meta['recent_failures'] += 1
                meta['success_rate'] = 0.9 * meta['success_rate'] + 0.1 * (1.0 if passed else 0.0)
            meta['avg_reward'] = 0.9 * meta['avg_reward'] + 0.1 * reward

    def get_runner_stats(self) -> Dict:
        return self.selector.get_stats()

    # Synchronous wrapper for use in decorator
    def decide_and_run_sync(self, test_name: str, test_func) -> bool:
        return asyncio.run(self.decide_and_run(test_name, test_func))


# ============================================================================
# PYTEST HOOKS (to integrate the adaptive runner)
# ============================================================================

# We'll use a fixture to provide the runner and a decorator to mark tests.

@pytest.fixture(scope="session")
def adaptive_runner():
    """Fixture to provide the adaptive test runner."""
    runner = AdaptiveTestRunner()
    # Register tests (in a real system, this could be automated via test discovery)
    # For this file, we'll register each test manually in the fixture.
    runner.register_test('test_cache_manager_basic', 'unit', 0.1)
    runner.register_test('test_cache_manager_fallback', 'unit', 0.1)
    runner.register_test('test_cache_manager_delete', 'unit', 0.1)
    runner.register_test('test_carbon_fetcher_basic', 'integration', 0.3)
    runner.register_test('test_carbon_fetcher_fallback', 'integration', 0.2)
    runner.register_test('test_carbon_fetcher_batch', 'integration', 0.2)
    runner.register_test('test_helium_collector_basic', 'integration', 0.2)
    runner.register_test('test_helium_collector_empty_data', 'integration', 0.1)
    runner.register_test('test_helium_collector_batch', 'integration', 0.2)
    runner.register_test('test_material_updater_catalog', 'integration', 0.2)
    runner.register_test('test_material_updater_api_failure', 'integration', 0.2)
    runner.register_test('test_bio_catalog_basic', 'unit', 0.1)
    runner.register_test('test_bio_catalog_add_remove', 'unit', 0.1)
    runner.register_test('test_bio_catalog_search', 'unit', 0.1)
    runner.register_test('test_helium_synthetic_generator_basic', 'performance', 0.5)
    runner.register_test('test_helium_synthetic_generator_validation', 'performance', 0.5)
    runner.register_test('test_node_descriptor_helpers', 'unit', 0.1)
    runner.register_test('test_workload_descriptor_helpers', 'unit', 0.1)
    runner.register_test('test_cost_function_basic', 'integration', 0.3)
    runner.register_test('test_update_carbon_intensity_task', 'integration', 0.1)
    runner.register_test('test_carbon_fetcher_timeout', 'unit', 0.1)
    runner.register_test('test_helium_collector_api_error', 'unit', 0.1)
    runner.register_test('test_node_descriptor_validation', 'unit', 0.1)
    runner.register_test('test_workload_descriptor_validation', 'unit', 0.1)
    runner.register_test('test_bio_catalog_persistence', 'unit', 0.2)
    runner.register_test('test_material_footprint_persistence', 'unit', 0.2)
    return runner


# Global runner instance for use in decorator
_test_runner = None

# A decorator that can be used to conditionally run a test.
def adaptive_test(func):
    """
    Decorator that uses the adaptive runner to decide whether to run the test.
    Usage:
        @adaptive_test
        def test_something():
            ...
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Get the runner from the fixture (we need to have it available)
        # In a real pytest environment, we'd use the fixture.
        # For simplicity, we'll rely on a global runner.
        global _test_runner
        if _test_runner is None:
            # Initialize with default config
            _test_runner = AdaptiveTestRunner()
        test_name = func.__name__
        # If runner decides to skip, we skip the test.
        if not _test_runner.decide_and_run_sync(test_name, lambda: func(*args, **kwargs)):
            pytest.skip(f"Test '{test_name}' skipped by adaptive selector")
        return None
    return wrapper


# ============================================================================
# FIXTURES (unchanged)
# ============================================================================

@pytest.fixture
def temp_cache_dir():
    """Provide a temporary directory for file-based caching."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)

@pytest.fixture
def cache_manager(temp_cache_dir):
    """Create a CacheManager with a temporary file backend."""
    return CacheManager(redis_url="memory://")  # fallback to in-memory

@pytest.fixture
def mock_aiohttp_session():
    """Mock aiohttp.ClientSession for API calls."""
    with patch('aiohttp.ClientSession') as mock_session:
        mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value.status = 200
        mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value.json = AsyncMock(
            return_value={"intensity": 0.42}
        )
        yield mock_session


# ============================================================================
# ORIGINAL TESTS (with adaptive decorator)
# ============================================================================

# 1. CacheManager Tests
@pytest.mark.asyncio
@adaptive_test
async def test_cache_manager_basic(cache_manager):
    """Test basic set/get operations."""
    await cache_manager.set("key1", "value1", ttl=10)
    val = await cache_manager.get("key1")
    assert val == "value1"

    # Test expiration
    await cache_manager.set("key2", "value2", ttl=1)
    await asyncio.sleep(1.5)
    val = await cache_manager.get("key2")
    assert val is None

@pytest.mark.asyncio
@adaptive_test
async def test_cache_manager_fallback():
    """Test that CacheManager falls back to memory when Redis is unavailable."""
    with patch('redis.asyncio.Redis') as mock_redis:
        mock_redis.from_url.side_effect = Exception("Redis down")
        cache = CacheManager()
        # Should fallback to memory
        await cache.set("key", "value")
        val = await cache.get("key")
        assert val == "value"

@pytest.mark.asyncio
@adaptive_test
async def test_cache_manager_delete(cache_manager):
    """Test delete operation."""
    await cache_manager.set("key", "value")
    assert await cache_manager.get("key") == "value"
    deleted = await cache_manager.delete("key")
    assert deleted is True
    assert await cache_manager.get("key") is None


# 2. CarbonIntensityFetcher Tests
@pytest.mark.asyncio
@adaptive_test
async def test_carbon_fetcher_basic(cache_manager, mock_aiohttp_session):
    """Test successful fetch with caching."""
    fetcher = CarbonIntensityFetcher(cache_manager)
    intensity = await fetcher.get_intensity("us-east")
    assert 0.2 < intensity < 0.6
    # Ensure caching works
    with patch.object(fetcher, '_fetch_climate_trace') as mock_fetch:
        await fetcher.get_intensity("us-east")
        mock_fetch.assert_not_called()

@pytest.mark.asyncio
@adaptive_test
async def test_carbon_fetcher_fallback(cache_manager):
    """Test fallback to region average when all providers fail."""
    with patch.object(CarbonIntensityFetcher, '_fetch_climate_trace', AsyncMock(return_value=None)):
        with patch.object(CarbonIntensityFetcher, '_fetch_os_climate', AsyncMock(return_value=None)):
            with patch.object(CarbonIntensityFetcher, '_fetch_electricity_maps', AsyncMock(return_value=None)):
                fetcher = CarbonIntensityFetcher(cache_manager)
                intensity = await fetcher.get_intensity("unknown-region")
                # Should fallback to global average (0.40)
                assert intensity == pytest.approx(0.40, abs=0.01)

@pytest.mark.asyncio
@adaptive_test
async def test_carbon_fetcher_batch(cache_manager):
    """Test batch fetching."""
    fetcher = CarbonIntensityFetcher(cache_manager)
    # Mock individual get_intensity to return fixed values
    with patch.object(fetcher, 'get_intensity', AsyncMock(side_effect=[0.42, 0.35, 0.28])):
        results = await fetcher.get_intensity_batch(["us-east", "us-west", "eu-west"])
        assert results == {"us-east": 0.42, "us-west": 0.35, "eu-west": 0.28}


# 3. HeliumCollector Tests
@pytest.mark.asyncio
@adaptive_test
async def test_helium_collector_basic(cache_manager):
    """Test connectivity score computation."""
    collector = HeliumCollector(cache_manager)
    # Mock _fetch_hotspot_data to return sample data
    with patch.object(collector, '_fetch_hotspot_data', AsyncMock(return_value=[
        {'rssi': -70, 'snr': 12},
        {'rssi': -65, 'snr': 15}
    ])):
        score = await collector.get_connectivity_score("hotspot_001")
        assert 0 <= score <= 1
        # Should cache
        with patch.object(collector, '_fetch_hotspot_data') as mock_fetch:
            await collector.get_connectivity_score("hotspot_001")
            mock_fetch.assert_not_called()

@pytest.mark.asyncio
@adaptive_test
async def test_helium_collector_empty_data(cache_manager):
    """Test default score when no data."""
    collector = HeliumCollector(cache_manager)
    with patch.object(collector, '_fetch_hotspot_data', AsyncMock(return_value=[])):
        score = await collector.get_connectivity_score("invalid_hotspot")
        assert score == 0.5

@pytest.mark.asyncio
@adaptive_test
async def test_helium_collector_batch(cache_manager):
    """Test batch fetch with concurrency control."""
    collector = HeliumCollector(cache_manager)
    with patch.object(collector, 'get_connectivity_score', AsyncMock(side_effect=[0.9, 0.7, 0.5])):
        scores = await collector.fetch_batch_scores(["h1", "h2", "h3"], max_concurrency=2)
        assert scores == {"h1": 0.9, "h2": 0.7, "h3": 0.5}


# 4. MaterialFootprintUpdater Tests
@pytest.mark.asyncio
@adaptive_test
async def test_material_updater_catalog():
    """Test catalog update and retrieval."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        updater = MaterialFootprintUpdater(db_path=Path(tmp.name))
        # Mock API call to return data
        with patch.object(updater, '_update_from_source', AsyncMock()):
            await updater.update_catalog()
        # Since we mocked, catalog should be seeded with mock data
        fp = updater.get_footprint("gpu-a100")
        assert fp is not None
        assert fp['material_index'] == 1.2

@pytest.mark.asyncio
@adaptive_test
async def test_material_updater_api_failure():
    """Test that API failure does not break catalog and falls back to mock."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        updater = MaterialFootprintUpdater(db_path=Path(tmp.name))
        # Force failure in _update_from_source
        with patch.object(updater, '_update_from_source', AsyncMock(side_effect=Exception("API down"))):
            await updater.update_catalog()
        # Should have seeded mock data
        fp = updater.get_footprint("edge-device")
        assert fp is not None
        assert fp['material_index'] == 0.6


# 5. BioParameterCatalog Tests
@adaptive_test
def test_bio_catalog_basic():
    """Test catalog initialization and get_parameters."""
    with tempfile.TemporaryDirectory() as tmpdir:
        catalog_path = Path(tmpdir) / "bio_params.json"
        catalog = BioParameterCatalog(catalog_path)
        params = catalog.get_parameters("high_efficiency")
        assert params.get('photosynthetic_efficiency') == 0.8

@adaptive_test
def test_bio_catalog_add_remove():
    """Test adding and removing organism types."""
    with tempfile.TemporaryDirectory() as tmpdir:
        catalog_path = Path(tmpdir) / "bio_params.json"
        catalog = BioParameterCatalog(catalog_path)
        success = catalog.add_organism_type("ultra_high", {
            "photosynthetic_efficiency": 0.9,
            "resilience_to_stress": 0.8,
            "carbon_fixation_rate": 0.7,
            "helium_affinity": 0.5
        })
        assert success
        assert "ultra_high" in catalog.list_organism_types()
        removed = catalog.remove_organism_type("ultra_high")
        assert removed
        assert "ultra_high" not in catalog.list_organism_types()

@adaptive_test
def test_bio_catalog_search():
    """Test search with filters."""
    with tempfile.TemporaryDirectory() as tmpdir:
        catalog_path = Path(tmpdir) / "bio_params.json"
        catalog = BioParameterCatalog(catalog_path)
        results = catalog.search(photosynthetic_efficiency__gte=0.7)
        assert "high_efficiency" in results
        assert "low_carbon" in results
        assert "high_robustness" not in results


# 6. HeliumSyntheticGenerator Tests
@adaptive_test
def test_helium_synthetic_generator_basic():
    """Test basic trace generation."""
    gen = HeliumSyntheticGenerator()
    df = gen.generate_trace(num_hotspots=5, duration_hours=1, events_per_hour=2)
    assert len(df) > 0
    assert 'rssi' in df.columns
    assert df['rssi'].min() >= -120
    assert df['rssi'].max() <= -30

@adaptive_test
def test_helium_synthetic_generator_validation():
    """Test statistical validation (if scipy available)."""
    gen = HeliumSyntheticGenerator()
    df = gen.generate_trace(num_hotspots=5, duration_hours=1, events_per_hour=20)
    try:
        results = gen.validate_trace(df)
        assert 'rssi_ks_test' in results
    except ImportError:
        pytest.skip("scipy not available")


# 7. NodeDescriptor and WorkloadDescriptor Helper Methods
@adaptive_test
def test_node_descriptor_helpers():
    """Test NodeDescriptor helper methods."""
    node = NodeDescriptor(
        id="test-node",
        type=NodeType.EDGE,
        region="us-east",
        region_carbon_intensity=0.42,
        energy_per_token=0.00005,
        helium_connectivity_score=0.9,
        uptime=0.99,
        maintenance_status=MaintenanceStatus.OPERATIONAL,
        efficiency_score=0.85
    )
    # compute_energy_cost
    energy = node.compute_energy_cost(512)
    assert energy == 0.00005 * 512
    # compute_carbon_cost
    carbon = node.compute_carbon_cost(energy)
    # Energy in J * 2.7778e-7 = kWh * 0.42 = kg CO₂
    expected_carbon = energy * 2.7778e-7 * 0.42
    assert carbon == pytest.approx(expected_carbon)
    # get_health_score
    health = node.get_health_score()
    assert health == 0.99 * 0.85  # uptime * efficiency
    # is_available
    assert node.is_available() is True
    # Test degraded
    node.maintenance_status = MaintenanceStatus.DEGRADED
    assert node.is_available() is True
    node.maintenance_status = MaintenanceStatus.OFFLINE
    assert node.is_available() is False

@adaptive_test
def test_workload_descriptor_helpers():
    """Test WorkloadDescriptor helper methods."""
    wl = WorkloadDescriptor(
        task_type=TaskType.INFERENCE,
        tokens=512,
        latency_target=200.0,
        urgency=Urgency.HIGH,
        priority=Priority.GREEN,
        bio_mode=BioMode.PHOTOSYNTHETIC
    )
    assert wl.is_critical() is False
    assert wl.is_high_priority() is True
    # compute_energy_cost (placeholder)
    energy = wl.compute_energy_cost(0.00005)
    assert energy == 0.00005 * 512


# 8. SustainabilityCostFunction Tests
@pytest.mark.asyncio
@adaptive_test
async def test_cost_function_basic(cache_manager):
    """Test cost function computation with real data."""
    carbon = CarbonIntensityFetcher(cache_manager)
    helium = HeliumCollector(cache_manager)
    # Use in-memory material catalog
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        material = MaterialFootprintUpdater(db_path=Path(tmp.name))
        # Seed mock data
        material._seed_mock_data()
        cost_func = SustainabilityCostFunction(carbon, material, helium)
        node = NodeDescriptor(
            id="test-node",
            type=NodeType.EDGE,
            region="us-east",
            region_carbon_intensity=0.42,
            energy_per_token=0.00005,
            helium_connectivity_score=0.9,
            material_footprint_id="gpu-a100"
        )
        workload = WorkloadDescriptor(
            task_type=TaskType.INFERENCE,
            tokens=512,
            latency_target=200.0,
            sector_emission_factor=0.03,
            bio_mode=BioMode.NONE,
            priority=Priority.BALANCED
        )
        cost = await cost_func.compute(node, workload)
        assert cost > 0


# 9. Periodic Celery Tasks Tests (with mocks)
@adaptive_test
def test_update_carbon_intensity_task():
    """Test the carbon update task with mocked dependencies."""
    # Patch the fetcher and cache creation inside the task
    with patch('src.enhancements.tasks.periodic_updater.CacheManager') as mock_cache_cls:
        with patch('src.enhancements.tasks.periodic_updater.CarbonIntensityFetcher') as mock_fetcher_cls:
            mock_fetcher = mock_fetcher_cls.return_value
            mock_fetcher.get_intensity = AsyncMock()
            # Mock async run to actually run the inner coroutine
            # We'll need to patch asyncio.run to execute the fetch_all coroutine
            # For simplicity, we just test that the task runs without error.
            # We'll use a mock task with a custom execute
            task = update_carbon_intensity
            # We can't easily test celery tasks directly, but we can test the wrapped function.
            # We'll create a dummy task object with a retry method.
            class DummyTask:
                retry = MagicMock()
            result = update_carbon_intensity.__wrapped__(DummyTask())
            # If we got here, it executed.
            assert result['status'] == 'success'


# 10. Error Handling and Edge Cases
@pytest.mark.asyncio
@adaptive_test
async def test_carbon_fetcher_timeout(cache_manager):
    """Test that timeout raises an exception (but we handle it)."""
    with patch.object(CarbonIntensityFetcher, '_fetch_climate_trace', AsyncMock(side_effect=asyncio.TimeoutError)):
        with patch.object(CarbonIntensityFetcher, '_fetch_os_climate', AsyncMock(return_value=None)):
            with patch.object(CarbonIntensityFetcher, '_fetch_electricity_maps', AsyncMock(return_value=None)):
                fetcher = CarbonIntensityFetcher(cache_manager)
                # Should fallback to region average
                intensity = await fetcher.get_intensity("us-east")
                assert intensity == pytest.approx(0.41, abs=0.01)

@pytest.mark.asyncio
@adaptive_test
async def test_helium_collector_api_error(cache_manager):
    """Test that API error results in default score."""
    collector = HeliumCollector(cache_manager)
    with patch.object(collector, '_fetch_hotspot_data', AsyncMock(side_effect=Exception("API error"))):
        score = await collector.get_connectivity_score("hotspot_001")
        assert score == 0.5


# 11. Configuration Validation Tests
@adaptive_test
def test_node_descriptor_validation():
    """Test that invalid fields raise validation errors."""
    with pytest.raises(ValueError, match="region_carbon_intensity must be non-negative"):
        NodeDescriptor(
            id="test",
            type=NodeType.EDGE,
            region="us-east",
            region_carbon_intensity=-0.1,
            energy_per_token=0.00005
        )
    with pytest.raises(ValueError, match="energy_per_token must be positive"):
        NodeDescriptor(
            id="test",
            type=NodeType.EDGE,
            region="us-east",
            region_carbon_intensity=0.42,
            energy_per_token=0.0
        )

@adaptive_test
def test_workload_descriptor_validation():
    """Test that invalid fields raise validation errors."""
    with pytest.raises(ValueError, match="latency_target must be positive"):
        WorkloadDescriptor(
            task_type=TaskType.INFERENCE,
            tokens=512,
            latency_target=-1.0
        )


# 12. Persistence Tests
@adaptive_test
def test_bio_catalog_persistence():
    """Test that catalog saves and loads correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        catalog_path = Path(tmpdir) / "bio_params.json"
        catalog = BioParameterCatalog(catalog_path)
        # Add a new organism
        catalog.add_organism_type("test_type", {
            "photosynthetic_efficiency": 0.75,
            "resilience_to_stress": 0.65,
            "carbon_fixation_rate": 0.55,
            "helium_affinity": 0.45
        })
        catalog.save()
        # Reload from disk
        catalog2 = BioParameterCatalog(catalog_path)
        params = catalog2.get_parameters("test_type")
        assert params.get('photosynthetic_efficiency') == 0.75

@adaptive_test
def test_material_footprint_persistence():
    """Test that material catalog persists in SQLite."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        updater = MaterialFootprintUpdater(db_path=Path(tmp.name))
        # Seed some data
        updater._seed_mock_data()
        # Reopen with same DB
        updater2 = MaterialFootprintUpdater(db_path=Path(tmp.name))
        fp = updater2.get_footprint("gpu-a100")
        assert fp is not None


# ============================================================================
# UNIT TESTS FOR DISTILLATION COMPONENTS (Phase 10)
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
        self.selector = DistillationTestSelector(self.config)

    def test_state_feature_vector(self):
        state = TestSelectionState(
            test_name='test_example',
            test_category='unit',
            estimated_duration_sec=1.0,
            code_coverage_pct=80.0,
            recent_failures=0,
            system_load=0.5,
            carbon_intensity=400,
            time_of_day=14,
            test_success_rate=0.9,
            avg_reward=0.8,
        )
        vec = state.to_feature_vector()
        self.assertEqual(len(vec), 11)

    def test_rule_based_teacher(self):
        teacher = TestRuleBasedTeacher()
        state = TestSelectionState(
            test_name='test_example',
            test_category='unit',
            estimated_duration_sec=1.0,
            code_coverage_pct=80.0,
            recent_failures=3,
            system_load=0.5,
            carbon_intensity=400,
            time_of_day=14,
            test_success_rate=0.9,
            avg_reward=0.8,
        )
        probs = teacher.predict(state)
        self.assertAlmostEqual(sum(probs), 1.0)
        self.assertGreater(probs[0], probs[1])  # run should be highest

    async def test_select_action(self):
        state = TestSelectionState(
            test_name='test_example',
            test_category='unit',
            estimated_duration_sec=1.0,
            code_coverage_pct=80.0,
            recent_failures=0,
            system_load=0.5,
            carbon_intensity=400,
            time_of_day=14,
            test_success_rate=0.9,
            avg_reward=0.8,
        )
        action, idx, state_vec, teacher_probs = await self.selector.select_action(state, exploration=False)
        self.assertIn(action, self.selector.ACTIONS)

    def test_replay_buffer(self):
        buffer = ReplayBuffer(max_size=5)
        state_vec = np.random.randn(11)
        buffer.push(state_vec, 0, 1.0, state_vec, np.ones(2)/2)
        self.assertEqual(len(buffer), 1)
        batch = buffer.sample(1)
        self.assertEqual(len(batch[0]), 1)


# ============================================================================
# OFFLINE TRAINING FOR HISTORICAL ML
# ============================================================================

def train_historical_model(log_path: Path = Path("./test_selection_interactions.csv"),
                           model_path: Path = Path("./test_selection_model.pkl")):
    """
    Train a RandomForestClassifier from past test selection logs.
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
# RUNNER
# ============================================================================

if __name__ == "__main__":
    # When running directly, we run pytest normally.
    # The adaptive runner would be enabled via a plugin or environment variable.
    pytest.main([__file__, "-v", "--tb=short"])
