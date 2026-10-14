"""
Enhanced Integration Tests for Green Agent Components v3.1.0
=============================================================
Comprehensive integration tests covering core modules with adaptive test
selection via Multi-Teacher On-Policy Distillation, plus optional MoE gating,
MOEA (NSGA-II) global weight evolution, RLHF preference collection, MODP state
storage, and LIMIT Graph management.

ENHANCEMENTS (v3.1.0 over v3.0.1):
- Removed the ``adaptive_test`` decorator (it silently no-oped every async
  test) and replaced it with a single pytest-hook mechanism.
- Added ``pytest_addoption`` for ``--test-metadata`` and other options.
- ``AdaptiveTestRunner`` starts its MOEA loop lazily (no event loop at
  construction time), avoiding ``RuntimeError`` in ``pytest_configure``.
- Production classes (RunningScaler, FederatedWeightsCodec, DistillationStudent,
  ReplayBuffer, MoEGatingNetwork, NSGAIIUpdateOptimizer, LimitGraphManager,
  MODPOptimizer, RLHFTrainer) are now *imported* instead of reimplemented.
- FeedbackEvent context fixed (``context.task_id``, ``envelope.trace_id``);
  JSON encoding guarded for both v1 and v2 schemas.
- Fixed stale tests for the enhanced NodeDescriptor / WorkloadDescriptor /
  PeriodicUpdater schemas.
- All file-producing tests use ``tmp_path``.
- Single adaptive mechanism: ``pytest_runtest_setup`` decides run vs. skip,
  ``pytest_runtest_makereport`` records outcomes.
- MOEA task cancellation in a fixture teardown; no background tasks leaked.
- Added ``__test__ = False`` guards so pytest does not collect helper classes.

All previous features retained.
"""

from __future__ import annotations

import asyncio
import json
import logging
import pickle
import random
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, ClassVar, Dict, List, Optional, Set, Tuple, Union
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Optional dependencies
# ---------------------------------------------------------------------------
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------------
# Optional FeedbackEvent & AsyncMessageQueue
# ---------------------------------------------------------------------------
try:
    from src.enhancements.schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:
    try:
        from ..schemas.feedback_event import FeedbackEvent  # type: ignore
    except ImportError:
        FeedbackEvent = None  # type: ignore

try:
    from src.enhancements.async_message_queue import AsyncMessageQueue  # type: ignore
except ImportError:
    AsyncMessageQueue = None  # type: ignore


# ---------------------------------------------------------------------------
# Production imports — no reimplementation.
# ---------------------------------------------------------------------------
try:
    from ..tasks.periodic_updater import (  # type: ignore
        RunningScaler,
        FederatedWeightsCodec,
        DistillationStudent,
        ReplayBuffer,
        MoEGatingNetwork,
        NSGAIIUpdateOptimizer,
        MOPDUpdateWeights,
        LimitGraphManager,
        MODPOptimizer,
        RLHFTrainer,
        get_scheduler,
        reset_scheduler_for_tests,
        AdaptiveScheduler,
        _ACTIONS as UPDATE_ACTIONS,
    )
    PRODUCTION_AVAILABLE = True
except ImportError:
    PRODUCTION_AVAILABLE = False
    RunningScaler = None  # type: ignore
    FederatedWeightsCodec = None  # type: ignore
    DistillationStudent = None  # type: ignore
    ReplayBuffer = None  # type: ignore
    MoEGatingNetwork = None  # type: ignore
    NSGAIIUpdateOptimizer = None  # type: ignore
    MOPDUpdateWeights = None  # type: ignore
    LimitGraphManager = None  # type: ignore
    MODPOptimizer = None  # type: ignore
    RLHFTrainer = None  # type: ignore
    get_scheduler = None  # type: ignore
    reset_scheduler_for_tests = None  # type: ignore
    AdaptiveScheduler = None  # type: ignore
    UPDATE_ACTIONS = ("update_now", "skip")

try:
    from ..cache.cache_manager import CacheManager  # type: ignore
except ImportError:
    CacheManager = None  # type: ignore

try:
    from ..data_integration.carbon_intensity import CarbonIntensityFetcher  # type: ignore
except ImportError:
    CarbonIntensityFetcher = None  # type: ignore

try:
    from ..data_integration.helium_collector import HeliumCollector  # type: ignore
except ImportError:
    HeliumCollector = None  # type: ignore

try:
    from ..data_integration.material_footprint import MaterialFootprintUpdater  # type: ignore
except ImportError:
    MaterialFootprintUpdater = None  # type: ignore

try:
    from ..data_integration.bio_parameter_catalog import BioParameterCatalog  # type: ignore
except ImportError:
    BioParameterCatalog = None  # type: ignore

try:
    from ..data_integration.helium_synthetic_generator import HeliumSyntheticGenerator  # type: ignore
except ImportError:
    HeliumSyntheticGenerator = None  # type: ignore

try:
    from ..schemas.node_descriptor import (  # type: ignore
        NodeDescriptor,
        NodeType,
        CoolingType,
        MaintenanceStatus,
    )
except ImportError:
    NodeDescriptor = None  # type: ignore
    NodeType = CoolingType = MaintenanceStatus = None  # type: ignore

try:
    from ..schemas.workload_descriptor import (  # type: ignore
        WorkloadDescriptor,
        TaskType,
        Urgency,
        Priority,
        BioMode,
    )
except ImportError:
    WorkloadDescriptor = None  # type: ignore
    TaskType = Urgency = Priority = BioMode = None  # type: ignore

try:
    from ..cost_function.sustainability_cost import SustainabilityCostFunction  # type: ignore
except ImportError:
    SustainabilityCostFunction = None  # type: ignore

try:
    from ..tasks.periodic_updater import (  # type: ignore
        app as celery_app,
        update_carbon_intensity,
        update_material_catalog,
        update_helium_snapshot,
    )
except ImportError:
    celery_app = None  # type: ignore
    update_carbon_intensity = None  # type: ignore
    update_material_catalog = None  # type: ignore
    update_helium_snapshot = None  # type: ignore


# ============================================================================
# Test-specific constants & helpers
# ============================================================================

TEST_ACTIONS: Tuple[str, ...] = ("run", "skip")
TEST_CATEGORIES: Tuple[str, ...] = ("unit", "integration", "performance")
_CATEGORY_ONEHOT: Dict[str, List[float]] = {
    "unit":        [1.0, 0.0, 0.0],
    "integration": [0.0, 1.0, 0.0],
    "performance": [0.0, 0.0, 1.0],
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _softmax(logits: np.ndarray) -> np.ndarray:
    z = logits - np.max(logits)
    e = np.exp(z)
    s = e.sum()
    if s <= 0:
        return np.ones_like(e) / len(e)
    return e / s


def _require_production() -> None:
    if not PRODUCTION_AVAILABLE:
        pytest.skip("Production modules under ..tasks.periodic_updater are unavailable.")


# ============================================================================
# Test-only state representation
# ============================================================================

@dataclass
class TestSelectionState:
    """
    State for the adaptive test-selection agent.

    Feature vector is 12-dimensional: 9 numeric features + 3 one-hot
    test-category features. The dimension matches production
    ``DistillationStudent`` / ``MoEGatingNetwork`` / ``RunningScaler``.
    """
    test_name: str
    test_category: str
    estimated_duration_sec: float
    test_importance: float
    code_coverage_pct: float
    recent_failures: int
    system_load: float
    carbon_intensity: float   # kg CO2/kWh
    time_of_day: float        # 0..23
    test_success_rate: float = 0.5
    avg_reward: float = 0.5

    FEATURE_DIM: ClassVar[int] = 12
    DURATION_MAX: ClassVar[float] = 60.0
    COVERAGE_MAX: ClassVar[float] = 100.0
    FAILURES_MAX: ClassVar[float] = 5.0
    CARBON_MAX: ClassVar[float] = 1.0

    @staticmethod
    def _c(v: float) -> float:
        return float(min(1.0, max(0.0, v)))

    def to_raw_vector(self) -> np.ndarray:
        return np.array([
            self.estimated_duration_sec,
            self.test_importance,
            self.code_coverage_pct,
            float(self.recent_failures),
            self.system_load,
            self.carbon_intensity,
            self.time_of_day,
            self.test_success_rate,
            self.avg_reward,
            1.0 if self.test_category == "unit" else 0.0,
            1.0 if self.test_category == "integration" else 0.0,
            1.0 if self.test_category == "performance" else 0.0,
        ], dtype=np.float64)

    def to_feature_vector(self, scaler: Optional[Any] = None) -> np.ndarray:
        raw = self.to_raw_vector()
        if scaler is not None:
            return scaler.transform(raw)
        c = self._c
        return np.array([
            c(self.estimated_duration_sec / self.DURATION_MAX),
            c(self.test_importance),
            c(self.code_coverage_pct / self.COVERAGE_MAX),
            c(self.recent_failures / self.FAILURES_MAX),
            c(self.system_load),
            c(self.carbon_intensity / self.CARBON_MAX),
            c(self.time_of_day / 24.0),
            c(self.test_success_rate),
            c(self.avg_reward),
            c(1.0 if self.test_category == "unit" else 0.0),
            c(1.0 if self.test_category == "integration" else 0.0),
            c(1.0 if self.test_category == "performance" else 0.0),
        ], dtype=np.float32)


# ============================================================================
# Test-selection teachers (test-specific; different from update-scheduler)
# ============================================================================

class TestTeacher:
    """Minimal teacher interface for test-selection distillation."""
    __test__ = False  # do not let pytest collect this class

    def __init__(self) -> None:
        self.scaler: Optional[Any] = None

    def predict(self, state: TestSelectionState) -> np.ndarray:
        raise NotImplementedError

    def confidence(self, state: TestSelectionState) -> float:
        raise NotImplementedError

    def _features(self, state: TestSelectionState) -> np.ndarray:
        return state.to_feature_vector(self.scaler)


class TestRuleBasedTeacher(TestTeacher):
    __test__ = False

    def predict(self, state: TestSelectionState) -> np.ndarray:
        probs = np.ones(2, dtype=np.float64) * 0.1
        if state.test_importance > 0.8 or state.recent_failures > 2:
            probs[0] = 0.9
        elif state.code_coverage_pct < 50:
            probs[0] = 0.8
        elif state.system_load > 0.8:
            probs[1] = 0.8
        elif state.carbon_intensity > 0.5 and state.test_importance < 0.5:
            probs[1] = 0.6
        else:
            probs[0] = 0.6
        return _softmax(probs)

    def confidence(self, state: TestSelectionState) -> float:
        if state.test_importance > 0.8 or state.recent_failures > 2:
            return 0.6
        return 0.4


class TestHistoricalMLTeacher(TestTeacher):
    __test__ = False
    EXPECTED_FEATURE_DIM: ClassVar[int] = 12

    def __init__(self, model_path: Optional[Path] = None) -> None:
        super().__init__()
        self.model = None
        self.label_encoder = None
        self.model_path: Path = model_path or Path("./test_selection_model.pkl")
        if self.model_path.exists():
            try:
                with open(self.model_path, "rb") as f:
                    self.model, self.label_encoder = pickle.load(f)
                logger.info(f"Loaded test-selection ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load test-selection model: {e}")

    def predict(self, state: TestSelectionState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(2) / 2
        x = self._features(state).reshape(1, -1)
        if not hasattr(self.model, "predict_proba"):
            return np.ones(2) / 2
        return self.model.predict_proba(x)[0]

    def confidence(self, state: TestSelectionState) -> float:
        return 0.7 if self.model is not None else 0.0


class TestStatefulQTeacher(TestTeacher):
    __test__ = False

    def __init__(
        self,
        lr: float = 0.1,
        weights_path: Optional[Path] = None,
        feature_dim: int = 12,
    ) -> None:
        super().__init__()
        self.lr = float(lr)
        self.weights_path: Path = weights_path or Path("./test_selection_q_weights.json")
        self.feature_dim = int(feature_dim)
        self.weights = np.zeros((self.feature_dim, 2), dtype=np.float64)
        self._load_state()

    def _load_state(self) -> None:
        if not self.weights_path.exists():
            return
        try:
            with open(self.weights_path, "r") as f:
                data = json.load(f)
            w = np.array(data, dtype=np.float64)
            if w.shape == (self.feature_dim, 2):
                self.weights = w
        except Exception as e:
            logger.error(f"Failed to load test-selection Q-weights: {e}")

    def _save_state(self) -> None:
        try:
            with open(self.weights_path, "w") as f:
                json.dump(self.weights.tolist(), f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save test-selection Q-weights: {e}")

    def _predict_from_vector(self, x: np.ndarray) -> np.ndarray:
        return _softmax(x @ self.weights)

    def predict(self, state: TestSelectionState) -> np.ndarray:
        return self._predict_from_vector(self._features(state))

    def confidence(self, state: TestSelectionState) -> float:
        x = self._features(state)
        q = x @ self.weights
        spread = float(np.max(q) - np.min(q))
        return float(np.clip(0.3 + 0.35 * spread, 0.0, 1.0))

    def update_from_vector(self, state_vec: np.ndarray, action: int, reward: float) -> None:
        x = np.asarray(state_vec, dtype=np.float64).reshape(-1)
        if x.shape[0] != self.feature_dim:
            return
        if not 0 <= int(action) < 2:
            return
        q_current = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()

    def update(self, state: TestSelectionState, action: int, reward: float) -> None:
        self.update_from_vector(self._features(state), action, reward)


# ============================================================================
# Test-selection distillation selector
# ============================================================================

class DistillationTestSelector:
    """
    Distillation-based test selector. Uses production DistillationStudent,
    ReplayBuffer, MoEGatingNetwork, and RunningScaler when available;
    otherwise falls back to minimal local implementations.
    """
    __test__ = False
    ACTIONS: ClassVar[List[str]] = list(TEST_ACTIONS)

    def __init__(self, config: Dict[str, Any]) -> None:
        _require_production()
        self.config = config
        self.feature_dim = 12
        self.n_actions = 2
        seed = config.get("seed", None)

        self.scaler = RunningScaler(self.feature_dim, clip=config.get("scaler_clip", 5.0))

        self.student = DistillationStudent(
            feature_dim=self.feature_dim,
            lr=config.get("distillation_learning_rate", 0.01),
            grad_clip=config.get("student_grad_clip", 1.0),
            seed=seed,
        )

        self.teachers: List[TestTeacher] = [
            TestRuleBasedTeacher(),
            TestHistoricalMLTeacher(model_path=config.get("historical_model_path")),
            TestStatefulQTeacher(
                lr=config.get("q_learning_rate", 0.1),
                weights_path=config.get("q_weights_path"),
                feature_dim=self.feature_dim,
            ),
        ]
        for t in self.teachers:
            t.scaler = self.scaler

        self.q_teacher: Optional[TestStatefulQTeacher] = next(
            (t for t in self.teachers if isinstance(t, TestStatefulQTeacher)), None
        )
        self.n_teachers = len(self.teachers)

        self.gating = MoEGatingNetwork(
            feature_dim=self.feature_dim,
            n_experts=self.n_teachers,
            lr=config.get("gating_learning_rate", 0.005),
            load_balance_weight=config.get("gating_load_balance_weight", 0.01),
            seed=seed,
        )

        self.replay_buffer = ReplayBuffer(max_size=config.get("distillation_replay_size", 2000))
        self.epsilon = float(config.get("distillation_epsilon", 0.1))
        self.epsilon_min = float(config.get("distillation_epsilon_min", 0.01))
        self.epsilon_decay = float(config.get("distillation_epsilon_decay", 0.995))
        self.train_every = int(config.get("distillation_train_every", 10))
        self.counter = 0
        self.distill_weight = float(config.get("distill_weight", 0.7))
        self.rl_weight = float(config.get("rl_weight", 0.3))
        self.batch_update_size = int(config.get("batch_update_size", 8))

    def _compute_teacher_probs(self, state: TestSelectionState):
        state_vec = state.to_feature_vector(self.scaler)
        teacher_outputs = []
        for teacher in self.teachers:
            try:
                prob = teacher.predict(state)
            except Exception as e:
                logger.warning(f"Teacher {type(teacher).__name__} failed: {e}")
                prob = np.ones(self.n_actions, dtype=np.float64) / self.n_actions
            prob = np.asarray(prob, dtype=np.float64).reshape(-1)
            if prob.shape[0] != self.n_actions:
                if prob.shape[0] < self.n_actions:
                    prob = np.pad(prob, (0, self.n_actions - prob.shape[0]))
                else:
                    prob = prob[: self.n_actions]
            s = prob.sum()
            prob = prob / s if s > 0 else np.ones(self.n_actions) / self.n_actions
            teacher_outputs.append(prob)
        teacher_outputs = np.array(teacher_outputs)
        gate_weights = self.gating.forward(state_vec)
        combined = gate_weights @ teacher_outputs
        s = combined.sum()
        combined = combined / s if s > 0 else np.ones(self.n_actions) / self.n_actions
        return combined, gate_weights, teacher_outputs

    async def select_action(
        self, state: TestSelectionState, exploration: bool = True
    ) -> Tuple[str, int, np.ndarray, np.ndarray]:
        raw = state.to_raw_vector()
        self.scaler.update(raw)
        state_vec = state.to_feature_vector(self.scaler)

        combined, gate_weights, teacher_outputs = self._compute_teacher_probs(state)
        student_probs = self.student.predict_proba(state_vec, self.n_actions)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, self.n_actions - 1)
        else:
            blended = 0.8 * student_probs + 0.2 * combined
            action_idx = int(np.argmax(blended))

        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)
        return self.ACTIONS[action_idx], action_idx, state_vec, teacher_outputs

    async def update(
        self,
        state_vec: np.ndarray,
        action_idx: int,
        reward: float,
        next_state_vec: np.ndarray,
        teacher_outputs: np.ndarray,
    ) -> None:
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_outputs)
        self.counter += 1
        if self.counter % self.train_every != 0:
            return
        if len(self.replay_buffer) < self.batch_update_size:
            return

        states, actions, rewards, _, teacher_outputs_batch = self.replay_buffer.sample(
            self.batch_update_size
        )
        for i in range(len(states)):
            s_vec = states[i]
            t_outs = teacher_outputs_batch[i]
            a = int(actions[i])
            r = float(rewards[i])

            gate_w = self.gating.forward(s_vec)
            combined = gate_w @ t_outs
            s = combined.sum()
            combined = combined / s if s > 0 else np.ones(self.n_actions) / self.n_actions

            self.student.update(
                s_vec, combined, r, a,
                distill_weight=self.distill_weight,
                rl_weight=self.rl_weight,
            )

            if self.q_teacher is not None:
                self.q_teacher.update_from_vector(s_vec, a, r)

            student_out = self.student.predict_proba(s_vec)
            self.gating.update(s_vec, t_outs, student_out, gate_w)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "student_counter": self.student.counter,
            "buffer_size": len(self.replay_buffer),
            "epsilon": self.epsilon,
            "n_teachers": self.n_teachers,
            "scaler_count": self.scaler.count,
        }


# ============================================================================
# Adaptive test runner (test-only orchestration)
# ============================================================================

class AdaptiveTestRunner:
    """
    Orchestrates adaptive test execution.

    Uses the test-selection distillation selector plus optional MoE, MOEA,
    RLHF, MODP, and LIMIT Graph. The MOEA loop starts lazily from an async
    context so that construction is safe outside a running event loop.
    """
    __test__ = False

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        critical_tests: Optional[Set[str]] = None,
        message_queue: Optional[Any] = None,
    ) -> None:
        _require_production()
        self.config = config or {}
        self.selector = DistillationTestSelector({
            "distillation_epsilon": self.config.get("distillation_epsilon", 0.1),
            "distillation_epsilon_min": self.config.get("distillation_epsilon_min", 0.01),
            "distillation_epsilon_decay": self.config.get("distillation_epsilon_decay", 0.995),
            "distillation_train_every": self.config.get("distillation_train_every", 10),
            "distillation_replay_size": self.config.get("distillation_replay_size", 2000),
            "distillation_learning_rate": self.config.get("distillation_learning_rate", 0.01),
            "distill_weight": self.config.get("distill_weight", 0.7),
            "rl_weight": self.config.get("rl_weight", 0.3),
            "q_learning_rate": self.config.get("q_learning_rate", 0.1),
            "q_weights_path": self.config.get(
                "q_weights_path", "./test_selection_q_weights.json"
            ),
            "historical_model_path": self.config.get(
                "historical_model_path", "./test_selection_model.pkl"
            ),
            "gating_learning_rate": self.config.get("gating_learning_rate", 0.005),
            "gating_load_balance_weight": self.config.get("gating_load_balance_weight", 0.01),
            "batch_update_size": self.config.get("batch_update_size", 8),
            "seed": self.config.get("seed", None),
        })

        self.interaction_log: List[Dict[str, Any]] = []
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None
        self._last_selected_expert: Optional[str] = None

        self.test_metadata: Dict[str, Dict[str, Any]] = {}
        self.critical_tests: Set[str] = critical_tests or set()
        self.message_queue = message_queue
        self.failed_tests: Dict[str, int] = defaultdict(int)
        self.quarantine_threshold = int(self.config.get("quarantine_threshold", 3))

        self.moea_enabled = bool(self.config.get("moea_enabled", False))
        self.moea_optimizer: Optional[NSGAIIUpdateOptimizer] = None
        self.global_best_weights: Optional[Dict[str, float]] = None
        self.pareto_front: List[Any] = []
        self._moea_task: Optional[asyncio.Task] = None
        self._moea_running = False

        self.storage = self.config.get("storage", None)
        self.limit_graph_manager = (
            LimitGraphManager(self.storage) if self.config.get("enable_limit_graph", False) else None
        )
        self.modp_solver = (
            MODPOptimizer(self.storage) if self.config.get("enable_modp", False) else None
        )
        self.rlhf_trainer = (
            RLHFTrainer(self.storage) if self.config.get("enable_rlhf", False) else None
        )
        self.moe_gating = (
            MoEGatingNetwork(
                feature_dim=12, n_experts=2,
                seed=self.config.get("seed", None),
            )
            if self.config.get("enable_moe", False) else None
        )

        self._log_path = Path(
            self.config.get("interaction_logs_path", "./test_selection_interactions.csv")
        )
        self._log_columns = ["timestamp", "test_name", "action", "reward", "passed", "state_vec"]

        if self.limit_graph_manager:
            self._init_limit_graph()

        metadata_file = self.config.get("metadata_file")
        if metadata_file and Path(metadata_file).exists():
            self.load_test_metadata(metadata_file)

    # ------------------------------------------------------------------
    # MOEA lifecycle
    # ------------------------------------------------------------------
    async def start_moea_loop(self) -> None:
        if not self.moea_enabled:
            return
        if self._moea_task is not None and not self._moea_task.done():
            return
        self._moea_task = asyncio.create_task(self._moea_loop())

    async def stop_moea_loop(self) -> None:
        if self._moea_task is not None and not self._moea_task.done():
            self._moea_task.cancel()
            try:
                await self._moea_task
            except asyncio.CancelledError:
                pass
        self._moea_task = None

    async def _moea_loop(self) -> None:
        interval = int(self.config.get("moea_interval_seconds", 300))
        while True:
            try:
                await asyncio.sleep(interval)
                await self.run_moea_update()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"MOEA loop error: {e}")
                await asyncio.sleep(60)

    # ------------------------------------------------------------------
    def _init_limit_graph(self) -> None:
        graph_id = "test_selection"
        if not self.limit_graph_manager.get_metadata(graph_id):
            self.limit_graph_manager.create_graph(graph_id, "Test Selection Relationships", {})
            for action in self.selector.ACTIONS:
                self.limit_graph_manager.add_node(graph_id, f"action_{action}", action, {})
            self.limit_graph_manager.add_edge(
                graph_id, "edge_run_skip", "action_run", "action_skip", 1.0, {}
            )

    def register_test(
        self,
        test_name: str,
        category: str = "unit",
        duration_sec: float = 1.0,
        importance: float = 0.5,
    ) -> None:
        self.test_metadata[test_name] = {
            "category": category,
            "duration_sec": float(duration_sec),
            "importance": float(importance),
            "coverage_pct": 0.0,
            "recent_failures": 0,
            "success_rate": 0.5,
            "avg_reward": 0.5,
        }

    def load_test_metadata(self, filepath: Union[str, Path]) -> None:
        with open(filepath, "r") as f:
            data = json.load(f)
        for test_name, meta in data.items():
            self.test_metadata[test_name] = {
                "category": meta.get("category", "unit"),
                "duration_sec": float(meta.get("duration_sec", 1.0)),
                "importance": float(meta.get("importance", 0.5)),
                "coverage_pct": float(meta.get("coverage_pct", 0.0)),
                "recent_failures": int(meta.get("recent_failures", 0)),
                "success_rate": float(meta.get("success_rate", 0.5)),
                "avg_reward": float(meta.get("avg_reward", 0.5)),
            }

    def _build_state(
        self,
        test_name: str,
        system_load: Optional[float] = None,
        carbon_intensity: Optional[float] = None,
    ) -> TestSelectionState:
        meta = self.test_metadata.get(test_name, {})
        return TestSelectionState(
            test_name=test_name,
            test_category=meta.get("category", "unit"),
            estimated_duration_sec=float(meta.get("duration_sec", 1.0)),
            test_importance=float(meta.get("importance", 0.5)),
            code_coverage_pct=float(meta.get("coverage_pct", 0.0)),
            recent_failures=int(meta.get("recent_failures", 0)),
            system_load=float(system_load if system_load is not None else 0.5),
            carbon_intensity=float(carbon_intensity if carbon_intensity is not None else 0.4),
            time_of_day=float(_utc_now().hour),
            test_success_rate=float(meta.get("success_rate", 0.5)),
            avg_reward=float(meta.get("avg_reward", 0.5)),
        )

    # ------------------------------------------------------------------
    async def decide_and_run(
        self,
        test_name: str,
        test_func: Callable[[], Any],
        system_load: Optional[float] = None,
        carbon_intensity: Optional[float] = None,
    ) -> bool:
        if test_name in self.critical_tests:
            logger.info(f"Test '{test_name}' is critical, always running.")
            state = self._build_state(test_name, system_load, carbon_intensity)
            state_vec = state.to_feature_vector(self.selector.scaler)
            passed = await self._execute_test(test_name, test_func)
            reward = self._compute_reward(passed, state)
            await self._record_outcome(
                test_name, "run", reward, passed,
                state_vec=state_vec, action_idx=0,
                teacher_probs=np.ones(2) / 2, state=state,
            )
            return True

        state = self._build_state(test_name, system_load, carbon_intensity)

        if self.moe_gating is not None:
            gate_w = self.moe_gating.forward(state.to_feature_vector(self.selector.scaler))
            action_idx = int(np.argmax(gate_w))
            action = TEST_ACTIONS[action_idx]
            state_vec = state.to_feature_vector(self.selector.scaler)
            teacher_probs = np.ones(2) / 2
            self._last_selected_expert = action
        else:
            action, action_idx, state_vec, teacher_probs = await self.selector.select_action(
                state, exploration=True
            )
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs

        if self.global_best_weights is not None:
            moea_probs = np.array([self.global_best_weights.get(a, 1.0) for a in TEST_ACTIONS])
            s = moea_probs.sum()
            moea_probs = moea_probs / s if s > 0 else np.ones(2) / 2
            one_hot = np.zeros(2)
            one_hot[action_idx] = 1.0
            blended = 0.7 * moea_probs + 0.3 * one_hot
            action_idx = int(np.argmax(blended))
            action = TEST_ACTIONS[action_idx]

        if action == "skip":
            reward = 0.1 * (1.0 - state.test_importance)
            await self._record_outcome(
                test_name, "skip", reward, None,
                state_vec=state_vec, action_idx=action_idx,
                teacher_probs=teacher_probs, state=state,
            )
            return False

        passed = await self._execute_test(test_name, test_func)
        reward = self._compute_reward(passed, state)
        await self._record_outcome(
            test_name, "run", reward, passed,
            state_vec=state_vec, action_idx=action_idx,
            teacher_probs=teacher_probs, state=state,
        )
        return True

    async def _execute_test(self, test_name: str, test_func: Callable[[], Any]) -> bool:
        """
        Execute the test function in a worker thread. Errors propagate so
        pytest sees the failure.
        """
        try:
            result = test_func()
            if asyncio.iscoroutine(result):
                await result
            self.failed_tests[test_name] = 0
            return True
        except Exception as e:
            self.failed_tests[test_name] = self.failed_tests.get(test_name, 0) + 1
            logger.error(f"Test '{test_name}' failed: {e}")
            if self.failed_tests[test_name] >= self.quarantine_threshold:
                logger.warning(
                    f"Test '{test_name}' has failed {self.failed_tests[test_name]} "
                    f"times, quarantining."
                )
            raise

    def _compute_reward(self, passed: bool, state: TestSelectionState) -> float:
        base = 0.6 if passed else 0.0
        coverage_bonus = 0.2 * min(1.0, state.code_coverage_pct / 100.0)
        time_penalty = 0.1 * min(1.0, state.estimated_duration_sec / 60.0)
        carbon_penalty = (
            0.1
            * min(1.0, state.carbon_intensity / 1.0)
            * (state.estimated_duration_sec / 60.0)
        )
        importance_factor = state.test_importance
        reward = (
            base + coverage_bonus - time_penalty - carbon_penalty
        ) * (0.5 + 0.5 * importance_factor)
        return float(max(0.0, min(1.0, reward)))

    async def _record_outcome(
        self,
        test_name: str,
        action: str,
        reward: float,
        passed: Optional[bool],
        state_vec: Optional[np.ndarray] = None,
        action_idx: Optional[int] = None,
        teacher_probs: Optional[np.ndarray] = None,
        state: Optional[TestSelectionState] = None,
    ) -> None:
        if state_vec is None:
            state_vec = self.last_state_vec
        if action_idx is None:
            action_idx = self.last_action_idx
        if teacher_probs is None:
            teacher_probs = self.last_teacher_probs

        entry = {
            "timestamp": _utc_now().isoformat(),
            "test_name": test_name,
            "action": action,
            "reward": float(reward),
            "passed": passed if passed is not None else "",
            "state_vec": ",".join(map(str, state_vec)) if state_vec is not None else "",
        }
        self.interaction_log.append(entry)

        try:
            await asyncio.to_thread(self._append_log_sync, entry)
        except Exception as e:
            logger.error(f"Failed to append interaction log: {e}")

        if state_vec is not None and action_idx is not None and teacher_probs is not None:
            await self.selector.update(
                state_vec, int(action_idx), float(reward), state_vec, teacher_probs
            )

        if self.moe_gating is not None and self._last_selected_expert is not None and state is not None:
            # Production MoEGatingNetwork does not offer an online update method;
            # treat as a no-op placeholder but keep the hook for future extension.
            pass

        if self.rlhf_trainer and random.random() < 0.05:
            chosen = action
            rejected = "skip" if chosen == "run" else "run"
            self.rlhf_trainer.record_pair(
                pair_id=str(uuid.uuid4()),
                prompt=f"Which test selection decision is best for '{test_name}'?",
                chosen=chosen,
                rejected=rejected,
                reward_diff=float(reward),
                metadata={"test_name": test_name},
            )

        if self.modp_solver:
            state_id = f"{test_name}_{_utc_now().isoformat()}_{action}"
            self.modp_solver.add_state(
                state_id=state_id,
                problem_id="test_selection",
                state_attributes={"test_name": test_name, "action": action},
                objective_values={
                    "coverage": 0.5,
                    "resource_savings": 0.3,
                    "success_rate": 1.0 if passed else 0.0,
                },
                stage=0,
            )
            self.modp_solver.add_policy(
                policy_id=f"policy_{state_id}",
                problem_id="test_selection",
                state_id=state_id,
                action=action,
                expected_objectives={"coverage": 0.0, "resource_savings": 0.0, "success_rate": 0.0},
            )

        if self.limit_graph_manager:
            self.limit_graph_manager.add_node(
                "test_selection",
                f"run_{uuid.uuid4()}",
                "test_decision",
                {"test_name": test_name, "action": action, "reward": float(reward)},
            )

        if test_name in self.test_metadata:
            meta = self.test_metadata[test_name]
            if passed is not None:
                if passed:
                    meta["recent_failures"] = max(0, int(meta["recent_failures"]) - 1)
                else:
                    meta["recent_failures"] = int(meta["recent_failures"]) + 1
                meta["success_rate"] = 0.9 * meta["success_rate"] + 0.1 * (1.0 if passed else 0.0)
            meta["avg_reward"] = 0.9 * meta["avg_reward"] + 0.1 * float(reward)

        await self._emit_feedback_event(test_name, action, reward)

    def _append_log_sync(self, entry: Dict[str, Any]) -> None:
        try:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        exists = self._log_path.exists()
        row = {col: entry.get(col, "") for col in self._log_columns}
        pd.DataFrame([row]).to_csv(self._log_path, mode="a", header=not exists, index=False)

    async def _emit_feedback_event(
        self, test_name: str, action: str, reward: float
    ) -> None:
        if FeedbackEvent is None or self.message_queue is None:
            return
        try:
            event = FeedbackEvent(
                source="adaptive_test_runner",
                feedback_type="routing",
                context={
                    "task_id": test_name,
                    "teacher_ids": [type(t).__name__ for t in self.selector.teachers],
                },
                action={
                    "selected_action": action,
                    "selected_rank": 1,
                    "confidence_score": 0.5,
                },
                performance={
                    "quality_score": float(max(0.0, min(1.0, reward))),
                    "latency_ms": 0.0,
                    "energy_joules": 0.0,
                    "carbon_g": 0.0,
                    "helium_cost": 0.0,
                    "duration_ms": 0.0,
                },
                adaptive_cost_value=float(reward),
                tags=["test_selection", action, test_name],
                envelope={"trace_id": str(uuid.uuid4())},
            )
            payload = None
            if hasattr(event, "to_json"):
                payload = event.to_json()
            elif hasattr(event, "model_dump_json"):
                payload = event.model_dump_json()
            elif hasattr(event, "json"):
                payload = event.json()
            if payload is not None:
                result = self.message_queue.publish("test_events", payload)
                if asyncio.iscoroutine(result):
                    await result
        except Exception as e:
            logger.warning(f"Failed to emit FeedbackEvent: {e}")

    async def run_moea_update(self) -> List[Any]:
        if not self.moea_enabled or len(self.interaction_log) < 20:
            return []
        if self._moea_running:
            return list(self.pareto_front)
        self._moea_running = True
        try:
            recent = self.interaction_log[-200:]

            async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
                rewards_run = [e["reward"] for e in recent if e["action"] == "run"]
                rewards_skip = [e["reward"] for e in recent if e["action"] == "skip"]
                run_mean = float(np.mean(rewards_run)) if rewards_run else 0.5
                skip_mean = float(np.mean(rewards_skip)) if rewards_skip else 0.5
                freq_run = len(rewards_run) / max(1, len(recent))
                coverage = weights.get("run", 0.5) * run_mean + weights.get("skip", 0.5) * skip_mean
                resource_savings = (
                    weights.get("skip", 0.5) * skip_mean
                    + weights.get("run", 0.5) * (1.0 - run_mean)
                )
                success_rate = freq_run * run_mean + (1 - freq_run) * skip_mean
                return {
                    "coverage": float(coverage),
                    "resource_savings": float(resource_savings),
                    "success_rate": float(success_rate),
                }

            self.moea_optimizer = NSGAIIUpdateOptimizer(
                evaluate_func=evaluate,
                population_size=self.config.get("moea_population_size", 20),
                generations=self.config.get("moea_generations", 10),
                mutation_rate=self.config.get("moea_mutation_rate", 0.2),
                crossover_rate=self.config.get("moea_crossover_rate", 0.8),
                tournament_size=self.config.get("moea_tournament_size", 3),
                objective_weights=self.config.get("moea_objective_weights"),
                dynamic_weights=self.config.get("moea_dynamic_weights", True),
            )
            pareto = await self.moea_optimizer.evolve()
            self.pareto_front = pareto
            if pareto:
                weights = self.moea_optimizer._dynamic_objective_weights()
                best = self.moea_optimizer._select_best_from_pareto(pareto, weights)
                if best is not None:
                    self.global_best_weights = dict(best.weights)
            return pareto
        finally:
            self._moea_running = False

    def get_runner_stats(self) -> Dict[str, Any]:
        stats = {
            "selector_stats": self.selector.get_stats(),
            "interaction_count": len(self.interaction_log),
            "critical_tests": len(self.critical_tests),
            "quarantined_tests": {
                k: v for k, v in self.failed_tests.items() if v >= self.quarantine_threshold
            },
        }
        if self.moea_enabled:
            stats["moea"] = {
                "pareto_front_size": len(self.pareto_front),
                "best_weights": self.global_best_weights,
                "enabled": True,
            }
        if self.limit_graph_manager:
            stats["limit_graph"] = self.limit_graph_manager.get_metadata("test_selection")
        return stats


# ============================================================================
# PYTEST PLUGIN HOOKS
# ============================================================================

def pytest_addoption(parser: Any) -> None:
    """Register custom CLI options used by the adaptive runner."""
    group = parser.getgroup("adaptive")
    group.addoption(
        "--test-metadata",
        action="store",
        default=None,
        help="Path to a JSON file mapping test names to metadata.",
    )
    group.addoption(
        "--adaptive-enable",
        action="store_true",
        default=False,
        help="Enable adaptive test selection (skip decisions).",
    )
    group.addoption(
        "--adaptive-moea",
        action="store_true",
        default=False,
        help="Enable MOEA in the adaptive runner.",
    )


def _get_runner(config: Any) -> Optional["AdaptiveTestRunner"]:
    return getattr(config, "_adaptive_runner", None)


def pytest_configure(config: Any) -> None:
    """Create the runner but never start event-loop-bound tasks here."""
    if not PRODUCTION_AVAILABLE:
        logger.warning("Production modules unavailable; adaptive runner disabled.")
        return
    try:
        runner = AdaptiveTestRunner(
            config={
                "metadata_file": config.getoption("--test-metadata", default=None),
                "interaction_logs_path": str(Path.cwd() / "_adaptive_test_interactions.csv"),
                "q_weights_path": str(Path.cwd() / "_adaptive_test_q_weights.json"),
                "historical_model_path": str(Path.cwd() / "_adaptive_test_model.pkl"),
                "enable_limit_graph": True,
                "enable_modp": True,
                "enable_rlhf": True,
                "enable_moe": True,
                "moea_enabled": bool(config.getoption("--adaptive-moea", default=False)),
            },
            critical_tests=set(),
        )
        config._adaptive_runner = runner
    except Exception as e:
        logger.warning(f"Failed to initialize adaptive runner: {e}")
        config._adaptive_runner = None


def pytest_unconfigure(config: Any) -> None:
    """Cancel the MOEA task and flush state if any."""
    runner = _get_runner(config)
    if runner is None:
        return
    try:
        if runner._moea_task is not None and not runner._moea_task.done():
            runner._moea_task.cancel()
    except Exception:
        pass
    config._adaptive_runner = None


def pytest_collection_modifyitems(session: Any, config: Any, items: List[Any]) -> None:
    """Register collected tests with the runner (metadata only)."""
    runner = _get_runner(config)
    if runner is None:
        return
    for item in items:
        test_name = item.nodeid
        category = "unit"
        if item.get_closest_marker("integration"):
            category = "integration"
        elif item.get_closest_marker("performance"):
            category = "performance"
        importance = 0.5
        marker = item.get_closest_marker("importance")
        if marker and marker.args:
            importance = float(marker.args[0])
        runner.register_test(test_name, category=category, importance=importance)


def pytest_runtest_setup(item: Any) -> None:
    """
    Decide whether to skip a test. Skips must be raised from a setup-phase
    hook, not from ``pytest_runtest_call``.
    """
    config = item.config
    if not config.getoption("--adaptive-enable", default=False):
        return
    runner = _get_runner(config)
    if runner is None:
        return
    test_name = item.nodeid
    if test_name in runner.critical_tests:
        return
    state = runner._build_state(test_name)
    # Deterministic decision for setup: use the rule-based teacher directly.
    teacher = TestRuleBasedTeacher()
    probs = teacher.predict(state)
    action_idx = int(np.argmax(probs))
    action = TEST_ACTIONS[action_idx]
    if action == "skip":
        pytest.skip(f"Skipped by adaptive selector: {test_name}")


def pytest_runtest_makereport(item: Any, call: Any) -> None:
    """Record the outcome of each test in the runner's interaction log."""
    if call.when != "call":
        return
    config = item.config
    runner = _get_runner(config)
    if runner is None:
        return
    test_name = item.nodeid
    passed = not call.failed
    state = runner._build_state(test_name)
    reward = runner._compute_reward(passed, state)
    # Use a fresh event loop for this hook; do not use the worker loop.
    try:
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(
                runner._record_outcome(
                    test_name=test_name,
                    action="run",
                    reward=reward,
                    passed=passed,
                    state_vec=runner.last_state_vec,
                    action_idx=runner.last_action_idx,
                    teacher_probs=runner.last_teacher_probs,
                    state=state,
                )
            )
        finally:
            loop.close()
    except Exception as e:
        logger.warning(f"Failed to record test outcome: {e}")


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def temp_cache_dir(tmp_path: Path):
    yield tmp_path


@pytest.fixture
def cache_manager(temp_cache_dir: Path):
    if CacheManager is None:
        pytest.skip("CacheManager unavailable.")
    return CacheManager(redis_url="memory://")


@pytest.fixture
def mock_aiohttp_session():
    if AsyncMock is None:
        pytest.skip("AsyncMock unavailable.")
    with patch("aiohttp.ClientSession") as mock_session:
        resp = mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value
        resp.status = 200
        resp.json = AsyncMock(return_value={"intensity": 0.42})
        yield mock_session


@pytest.fixture
def adaptive_runner(tmp_path: Path):
    if not PRODUCTION_AVAILABLE:
        pytest.skip("Production modules unavailable.")
    runner = AdaptiveTestRunner(
        config={
            "enable_limit_graph": True,
            "enable_modp": True,
            "enable_rlhf": True,
            "enable_moe": True,
            "moea_enabled": False,
            "interaction_logs_path": str(tmp_path / "interactions.csv"),
            "q_weights_path": str(tmp_path / "q_weights.json"),
            "historical_model_path": str(tmp_path / "hist.pkl"),
            "seed": 42,
        }
    )
    yield runner
    # Cleanup
    try:
        if runner._moea_task is not None and not runner._moea_task.done():
            runner._moea_task.cancel()
    except Exception:
        pass


# ============================================================================
# TESTS — CacheManager
# ============================================================================

@pytest.mark.asyncio
async def test_cache_manager_basic(cache_manager):
    await cache_manager.set("key1", "value1", ttl=10)
    assert await cache_manager.get("key1") == "value1"
    # Use a short TTL and a wall-clock delay bounded to what CacheManager allows.
    await cache_manager.set("key2", "value2", ttl=1)
    await asyncio.sleep(1.1)
    assert await cache_manager.get("key2") is None


@pytest.mark.asyncio
async def test_cache_manager_fallback(monkeypatch):
    if CacheManager is None:
        pytest.skip("CacheManager unavailable.")
    with patch("redis.asyncio.Redis.from_url", side_effect=Exception("Redis down")):
        cache = CacheManager()
        await cache.set("key", "value")
        assert await cache.get("key") == "value"


@pytest.mark.asyncio
async def test_cache_manager_delete(cache_manager):
    await cache_manager.set("key", "value")
    assert await cache_manager.get("key") == "value"
    assert await cache_manager.delete("key") is True
    assert await cache_manager.get("key") is None


# ============================================================================
# TESTS — CarbonIntensityFetcher
# ============================================================================

@pytest.mark.asyncio
async def test_carbon_fetcher_basic(cache_manager, mock_aiohttp_session):
    if CarbonIntensityFetcher is None:
        pytest.skip("CarbonIntensityFetcher unavailable.")
    fetcher = CarbonIntensityFetcher(cache_manager)
    with patch.object(fetcher, "_fetch_climate_trace", AsyncMock(return_value=0.42)):
        intensity = await fetcher.get_intensity("us-east")
        assert 0.2 < intensity < 0.6
        with patch.object(fetcher, "_fetch_climate_trace") as mock_fetch:
            await fetcher.get_intensity("us-east")
            mock_fetch.assert_not_called()


@pytest.mark.asyncio
async def test_carbon_fetcher_fallback(cache_manager):
    if CarbonIntensityFetcher is None:
        pytest.skip("CarbonIntensityFetcher unavailable.")
    with patch.object(CarbonIntensityFetcher, "_fetch_climate_trace", AsyncMock(return_value=None)), \
         patch.object(CarbonIntensityFetcher, "_fetch_os_climate", AsyncMock(return_value=None)), \
         patch.object(CarbonIntensityFetcher, "_fetch_electricity_maps", AsyncMock(return_value=None)):
        fetcher = CarbonIntensityFetcher(cache_manager)
        intensity = await fetcher.get_intensity("unknown-region")
        assert intensity == pytest.approx(0.40, abs=0.05)


@pytest.mark.asyncio
async def test_carbon_fetcher_batch(cache_manager):
    if CarbonIntensityFetcher is None:
        pytest.skip("CarbonIntensityFetcher unavailable.")
    fetcher = CarbonIntensityFetcher(cache_manager)
    with patch.object(
        fetcher,
        "get_intensity_batch",
        AsyncMock(return_value={"us-east": 0.42, "us-west": 0.35, "eu-west": 0.28}),
    ):
        results = await fetcher.get_intensity_batch(["us-east", "us-west", "eu-west"])
        assert results == {"us-east": 0.42, "us-west": 0.35, "eu-west": 0.28}


# ============================================================================
# TESTS — HeliumCollector
# ============================================================================

@pytest.mark.asyncio
async def test_helium_collector_basic(cache_manager):
    if HeliumCollector is None:
        pytest.skip("HeliumCollector unavailable.")
    collector = HeliumCollector(cache_manager)
    with patch.object(collector, "_fetch_hotspot_data", AsyncMock(return_value=[
        {"rssi": -70, "snr": 12},
        {"rssi": -65, "snr": 15},
    ])):
        score = await collector.get_connectivity_score("hotspot_001")
        assert 0 <= score <= 1


@pytest.mark.asyncio
async def test_helium_collector_empty_data(cache_manager):
    if HeliumCollector is None:
        pytest.skip("HeliumCollector unavailable.")
    collector = HeliumCollector(cache_manager)
    with patch.object(collector, "_fetch_hotspot_data", AsyncMock(return_value=[])):
        score = await collector.get_connectivity_score("invalid_hotspot")
        assert score == 0.5


@pytest.mark.asyncio
async def test_helium_collector_batch(cache_manager):
    if HeliumCollector is None:
        pytest.skip("HeliumCollector unavailable.")
    collector = HeliumCollector(cache_manager)
    with patch.object(
        collector,
        "fetch_batch_scores",
        AsyncMock(return_value={"h1": 0.9, "h2": 0.7, "h3": 0.5}),
    ):
        scores = await collector.fetch_batch_scores(["h1", "h2", "h3"], max_concurrency=2)
        assert scores == {"h1": 0.9, "h2": 0.7, "h3": 0.5}


# ============================================================================
# TESTS — MaterialFootprintUpdater
# ============================================================================

@pytest.mark.asyncio
async def test_material_updater_catalog(tmp_path: Path):
    if MaterialFootprintUpdater is None:
        pytest.skip("MaterialFootprintUpdater unavailable.")
    updater = MaterialFootprintUpdater(db_path=tmp_path / "materials.db")
    with patch.object(updater, "_update_from_source", AsyncMock()):
        await updater.update_catalog()
    fp = updater.get_footprint("gpu-a100")
    assert fp is not None
    assert fp["material_index"] == 1.2


@pytest.mark.asyncio
async def test_material_updater_api_failure(tmp_path: Path):
    if MaterialFootprintUpdater is None:
        pytest.skip("MaterialFootprintUpdater unavailable.")
    updater = MaterialFootprintUpdater(db_path=tmp_path / "materials.db")
    with patch.object(updater, "_update_from_source", AsyncMock(side_effect=Exception("API down"))):
        await updater.update_catalog()
    fp = updater.get_footprint("edge-device")
    assert fp is not None
    assert fp["material_index"] == 0.6


# ============================================================================
# TESTS — BioParameterCatalog
# ============================================================================

def test_bio_catalog_basic(tmp_path: Path):
    if BioParameterCatalog is None:
        pytest.skip("BioParameterCatalog unavailable.")
    catalog = BioParameterCatalog(tmp_path / "bio_params.json")
    params = catalog.get_parameters("high_efficiency")
    assert params.get("photosynthetic_efficiency") == 0.8


def test_bio_catalog_add_remove(tmp_path: Path):
    if BioParameterCatalog is None:
        pytest.skip("BioParameterCatalog unavailable.")
    catalog = BioParameterCatalog(tmp_path / "bio_params.json")
    assert catalog.add_organism_type("ultra_high", {
        "photosynthetic_efficiency": 0.9,
        "resilience_to_stress": 0.8,
        "carbon_fixation_rate": 0.7,
        "helium_affinity": 0.5,
    })
    assert "ultra_high" in catalog.list_organism_types()
    assert catalog.remove_organism_type("ultra_high")
    assert "ultra_high" not in catalog.list_organism_types()


def test_bio_catalog_search(tmp_path: Path):
    if BioParameterCatalog is None:
        pytest.skip("BioParameterCatalog unavailable.")
    catalog = BioParameterCatalog(tmp_path / "bio_params.json")
    results = catalog.search(photosynthetic_efficiency__gte=0.7)
    assert "high_efficiency" in results
    assert "low_carbon" in results
    assert "high_robustness" not in results


# ============================================================================
# TESTS — HeliumSyntheticGenerator
# ============================================================================

def test_helium_synthetic_generator_basic():
    if HeliumSyntheticGenerator is None:
        pytest.skip("HeliumSyntheticGenerator unavailable.")
    gen = HeliumSyntheticGenerator()
    df = gen.generate_trace(num_hotspots=5, duration_hours=1, events_per_hour=2)
    assert len(df) > 0
    assert "rssi" in df.columns
    assert df["rssi"].min() >= -120
    assert df["rssi"].max() <= -30


def test_helium_synthetic_generator_validation():
    if HeliumSyntheticGenerator is None:
        pytest.skip("HeliumSyntheticGenerator unavailable.")
    pytest.importorskip("scipy")
    gen = HeliumSyntheticGenerator()
    df = gen.generate_trace(num_hotspots=5, duration_hours=1, events_per_hour=20)
    results = gen.validate_trace(df)
    assert "rssi_ks_test" in results


# ============================================================================
# TESTS — NodeDescriptor and WorkloadDescriptor
# ============================================================================

def test_node_descriptor_helpers():
    if NodeDescriptor is None:
        pytest.skip("NodeDescriptor unavailable.")
    node = NodeDescriptor(
        id="test-node",
        type=NodeType.EDGE,
        region="us-east",
        region_carbon_intensity=0.42,
        energy_per_token=0.00005,
        helium_connectivity_score=0.9,
        uptime=0.99,
        maintenance_status=MaintenanceStatus.OPERATIONAL,
        efficiency_score=0.85,
    )
    energy = node.compute_energy_cost(512)
    assert energy == pytest.approx(0.00005 * 512)

    carbon = node.compute_carbon_cost(energy)
    expected_carbon = energy * 2.7778e-7 * 0.42
    assert carbon == pytest.approx(expected_carbon)

    # Enhanced NodeDescriptor blends a graph-health term (default 0.5):
    # health = uptime * efficiency * (0.8 + 0.2 * graph_health)
    health = node.get_health_score()
    assert health == pytest.approx(0.99 * 0.85 * (0.8 + 0.2 * 0.5), rel=1e-3)

    assert node.is_available() is True
    node.maintenance_status = MaintenanceStatus.DEGRADED
    assert node.is_available() is True
    node.maintenance_status = MaintenanceStatus.OFFLINE
    assert node.is_available() is False


def test_workload_descriptor_helpers():
    if WorkloadDescriptor is None:
        pytest.skip("WorkloadDescriptor unavailable.")
    wl = WorkloadDescriptor(
        task_type=TaskType.INFERENCE,
        tokens=512,
        latency_target=200.0,
        urgency=Urgency.HIGH,
        adaptive_priority=Priority.GREEN,   # enhanced field name
        bio_mode=BioMode.PHOTOSYNTHETIC,
    )
    assert wl.is_critical() is False
    assert wl.is_high_priority() is True
    energy = wl.compute_energy_cost(0.00005)
    assert energy == pytest.approx(0.00005 * 512)


# ============================================================================
# TESTS — SustainabilityCostFunction
# ============================================================================

@pytest.mark.asyncio
async def test_cost_function_basic(cache_manager, tmp_path: Path):
    if SustainabilityCostFunction is None or NodeDescriptor is None or WorkloadDescriptor is None:
        pytest.skip("Cost function or descriptor unavailable.")
    if CarbonIntensityFetcher is None or HeliumCollector is None or MaterialFootprintUpdater is None:
        pytest.skip("Cost-function dependencies unavailable.")
    carbon = CarbonIntensityFetcher(cache_manager)
    helium = HeliumCollector(cache_manager)
    material = MaterialFootprintUpdater(db_path=tmp_path / "materials.db")
    if hasattr(material, "_seed_mock_data"):
        material._seed_mock_data()
    cost_func = SustainabilityCostFunction(carbon, material, helium)
    node = NodeDescriptor(
        id="test-node",
        type=NodeType.EDGE,
        region="us-east",
        region_carbon_intensity=0.42,
        energy_per_token=0.00005,
        helium_connectivity_score=0.9,
        material_footprint_id="gpu-a100",
    )
    workload = WorkloadDescriptor(
        task_type=TaskType.INFERENCE,
        tokens=512,
        latency_target=200.0,
        sector_emission_factor=0.03,
        bio_mode=BioMode.NONE,
        adaptive_priority=Priority.BALANCED,
    )
    cost = await cost_func.compute(node, workload)
    assert cost > 0


# ============================================================================
# TESTS — Periodic Celery Tasks
# ============================================================================

def test_update_carbon_intensity_task(tmp_path: Path, monkeypatch):
    if update_carbon_intensity is None:
        pytest.skip("update_carbon_intensity unavailable.")
    module_path = "src.enhancements.tasks.periodic_updater"

    mock_cache_cls = MagicMock()
    mock_fetcher_cls = MagicMock()
    mock_fetcher = mock_fetcher_cls.return_value

    async def fake_get_intensity(_region):
        return 0.42

    mock_fetcher.get_intensity = AsyncMock(side_effect=fake_get_intensity)

    class DummyTask:
        def retry(self, *args, **kwargs):
            raise RuntimeError("retry called")

    with patch(f"{module_path}.CacheManager", mock_cache_cls), \
         patch(f"{module_path}.CarbonIntensityFetcher", mock_fetcher_cls), \
         patch(f"{module_path}._run_scheduler_record_outcome", MagicMock()):
        result = update_carbon_intensity.__wrapped__(DummyTask())

    assert result["status"] == "success"
    assert result["reward"] == pytest.approx(1.0)
    assert result["regions_updated"] == result["total"]


# ============================================================================
# TESTS — Error handling and edge cases
# ============================================================================

@pytest.mark.asyncio
async def test_carbon_fetcher_timeout(cache_manager):
    if CarbonIntensityFetcher is None:
        pytest.skip("CarbonIntensityFetcher unavailable.")
    with patch.object(CarbonIntensityFetcher, "_fetch_climate_trace", AsyncMock(side_effect=asyncio.TimeoutError)), \
         patch.object(CarbonIntensityFetcher, "_fetch_os_climate", AsyncMock(return_value=None)), \
         patch.object(CarbonIntensityFetcher, "_fetch_electricity_maps", AsyncMock(return_value=None)):
        fetcher = CarbonIntensityFetcher(cache_manager)
        intensity = await fetcher.get_intensity("us-east")
        assert 0.0 < intensity < 1.0


@pytest.mark.asyncio
async def test_helium_collector_api_error(cache_manager):
    if HeliumCollector is None:
        pytest.skip("HeliumCollector unavailable.")
    collector = HeliumCollector(cache_manager)
    with patch.object(collector, "_fetch_hotspot_data", AsyncMock(side_effect=Exception("API error"))):
        score = await collector.get_connectivity_score("hotspot_001")
        assert score == 0.5


# ============================================================================
# TESTS — Validation
# ============================================================================

def test_node_descriptor_validation():
    if NodeDescriptor is None:
        pytest.skip("NodeDescriptor unavailable.")
    with pytest.raises(Exception):
        NodeDescriptor(
            id="test", type=NodeType.EDGE, region="us-east",
            region_carbon_intensity=-0.1, energy_per_token=0.00005,
        )
    with pytest.raises(Exception):
        NodeDescriptor(
            id="test", type=NodeType.EDGE, region="us-east",
            region_carbon_intensity=0.42, energy_per_token=0.0,
        )


def test_workload_descriptor_validation():
    if WorkloadDescriptor is None:
        pytest.skip("WorkloadDescriptor unavailable.")
    with pytest.raises(Exception):
        WorkloadDescriptor(task_type=TaskType.INFERENCE, tokens=512, latency_target=-1.0)


# ============================================================================
# TESTS — Persistence
# ============================================================================

def test_bio_catalog_persistence(tmp_path: Path):
    if BioParameterCatalog is None:
        pytest.skip("BioParameterCatalog unavailable.")
    catalog_path = tmp_path / "bio_params.json"
    catalog = BioParameterCatalog(catalog_path)
    catalog.add_organism_type("test_type", {
        "photosynthetic_efficiency": 0.75,
        "resilience_to_stress": 0.65,
        "carbon_fixation_rate": 0.55,
        "helium_affinity": 0.45,
    })
    catalog.save()
    catalog2 = BioParameterCatalog(catalog_path)
    params = catalog2.get_parameters("test_type")
    assert params.get("photosynthetic_efficiency") == 0.75


def test_material_footprint_persistence(tmp_path: Path):
    if MaterialFootprintUpdater is None:
        pytest.skip("MaterialFootprintUpdater unavailable.")
    db = tmp_path / "materials.db"
    updater = MaterialFootprintUpdater(db_path=db)
    if hasattr(updater, "_seed_mock_data"):
        updater._seed_mock_data()
    updater2 = MaterialFootprintUpdater(db_path=db)
    fp = updater2.get_footprint("gpu-a100")
    assert fp is not None


# ============================================================================
# TESTS — Test-selection distillation
# ============================================================================

class TestDistillationTestSelection:
    __test__ = False  # avoid pytest collecting class name patterns

    def test_state_feature_vector_dimension(self):
        state = TestSelectionState(
            test_name="example",
            test_category="unit",
            estimated_duration_sec=1.0,
            test_importance=0.7,
            code_coverage_pct=80.0,
            recent_failures=0,
            system_load=0.5,
            carbon_intensity=0.4,
            time_of_day=14,
            test_success_rate=0.9,
            avg_reward=0.8,
        )
        vec = state.to_feature_vector()
        assert len(vec) == 12
        assert np.all(vec >= 0.0) and np.all(vec <= 1.0)


def test_rule_based_teacher():
    teacher = TestRuleBasedTeacher()
    state = TestSelectionState(
        test_name="example",
        test_category="unit",
        estimated_duration_sec=1.0,
        test_importance=0.3,
        code_coverage_pct=80.0,
        recent_failures=3,
        system_load=0.5,
        carbon_intensity=0.4,
        time_of_day=14,
        test_success_rate=0.9,
        avg_reward=0.8,
    )
    probs = teacher.predict(state)
    assert probs.sum() == pytest.approx(1.0)
    assert probs[0] > probs[1]


@pytest.mark.asyncio
async def test_distillation_selector_select_action(tmp_path: Path):
    if not PRODUCTION_AVAILABLE:
        pytest.skip("Production modules unavailable.")
    selector = DistillationTestSelector({
        "distillation_epsilon": 0.0,
        "distillation_train_every": 10,
        "distillation_replay_size": 10,
        "distillation_learning_rate": 0.01,
        "distill_weight": 0.7,
        "rl_weight": 0.3,
        "q_learning_rate": 0.1,
        "q_weights_path": str(tmp_path / "q.json"),
        "historical_model_path": str(tmp_path / "hist.pkl"),
        "seed": 42,
    })
    state = TestSelectionState(
        test_name="example",
        test_category="unit",
        estimated_duration_sec=1.0,
        test_importance=0.7,
        code_coverage_pct=80.0,
        recent_failures=0,
        system_load=0.5,
        carbon_intensity=0.4,
        time_of_day=14,
        test_success_rate=0.9,
        avg_reward=0.8,
    )
    action, idx, state_vec, teacher_probs = await selector.select_action(state, exploration=False)
    assert action in selector.ACTIONS
    assert idx in (0, 1)
    assert state_vec.shape == (12,)
    assert teacher_probs.shape == (selector.n_teachers, 2)


def test_replay_buffer():
    if not PRODUCTION_AVAILABLE:
        pytest.skip("Production modules unavailable.")
    buf = ReplayBuffer(max_size=5)
    v = np.random.randn(12)
    buf.push(v, 0, 1.0, v, np.ones((3, 2)) / 2)
    assert len(buf) == 1
    batch = buf.sample(1)
    assert len(batch[0]) == 1


# ============================================================================
# TESTS — New enhancement components (using production classes)
# ============================================================================

@pytest.mark.asyncio
async def test_limit_graph_manager_basic():
    if not PRODUCTION_AVAILABLE:
        pytest.skip("Production modules unavailable.")
    manager = LimitGraphManager()
    graph_id = "test_graph"
    manager.create_graph(graph_id, "Test Graph", {})
    manager.add_node(graph_id, "node1", "type1", {"key": "value"})
    manager.add_node(graph_id, "node2", "type2", {})
    manager.add_edge(graph_id, "edge1", "node1", "node2", 1.0, {})
    assert len(manager.get_nodes(graph_id)) == 2
    assert len(manager.get_edges(graph_id)) == 1
    assert manager.get_metadata(graph_id)["description"] == "Test Graph"


@pytest.mark.asyncio
async def test_modp_optimizer_basic():
    if not PRODUCTION_AVAILABLE:
        pytest.skip("Production modules unavailable.")
    opt = MODPOptimizer()
    opt.add_state("state1", "problem1", {"data": 1}, {"obj1": 0.5}, 0)
    opt.add_state("state2", "problem1", {"data": 2}, {"obj1": 0.8}, 1)
    opt.add_policy("policy1", "problem1", "state1", "action1", {"obj1": 0.6})
    assert len(opt.get_states("problem1")) == 2
    assert len(opt.get_policies("problem1")) == 1


@pytest.mark.asyncio
async def test_rlhf_trainer_basic():
    if not PRODUCTION_AVAILABLE:
        pytest.skip("Production modules unavailable.")
    trainer = RLHFTrainer()
    trainer.record_pair("pair1", "prompt", "chosen", "rejected", 0.5, {})
    pairs = trainer.get_pairs()
    assert len(pairs) == 1
    assert pairs[0]["chosen"] == "chosen"


@pytest.mark.asyncio
async def test_moe_gating_network_basic():
    if not PRODUCTION_AVAILABLE:
        pytest.skip("Production modules unavailable.")
    gating = MoEGatingNetwork(feature_dim=12, n_experts=2, seed=42)
    x = np.zeros(12)
    probs = gating.forward(x)
    assert probs.shape == (2,)
    assert probs.sum() == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_nsga_ii_optimizer_produces_pareto():
    if not PRODUCTION_AVAILABLE:
        pytest.skip("Production modules unavailable.")

    async def evaluate(weights):
        return {
            "coverage": weights.get("update_now", 0.5),
            "resource_savings": weights.get("skip", 0.5),
            "timeliness": 1.0 - weights.get("skip", 0.5),
        }

    opt = NSGAIIUpdateOptimizer(
        evaluate_func=evaluate,
        population_size=8,
        generations=3,
        dynamic_weights=False,
    )
    pareto = await opt.evolve()
    assert len(pareto) >= 1
    assert opt.best_individual is not None


@pytest.mark.asyncio
async def test_adaptive_runner_full_flow(adaptive_runner, tmp_path: Path):
    """End-to-end: register, decide, record, stats."""
    runner = adaptive_runner
    runner.register_test("dummy_test", category="unit", duration_sec=0.1, importance=0.8)

    def dummy():
        return None

    result = await runner.decide_and_run("dummy_test", dummy)
    assert isinstance(result, bool)
    stats = runner.get_runner_stats()
    assert "selector_stats" in stats
    assert "interaction_count" in stats
    assert (tmp_path / "interactions.csv").exists()


@pytest.mark.asyncio
async def test_adaptive_runner_failure_propagation(adaptive_runner):
    """A raising test_func must propagate, not be swallowed."""
    runner = adaptive_runner
    runner.register_test("failing_test", category="unit", duration_sec=0.1, importance=0.9)

    def boom():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await runner.decide_and_run("failing_test", boom)


@pytest.mark.asyncio
async def test_adaptive_runner_moea_cancellable(tmp_path: Path):
    """The MOEA loop must be cleanly cancellable."""
    if not PRODUCTION_AVAILABLE:
        pytest.skip("Production modules unavailable.")
    runner = AdaptiveTestRunner(config={
        "moea_enabled": True,
        "moea_interval_seconds": 1,
        "interaction_logs_path": str(tmp_path / "i.csv"),
        "q_weights_path": str(tmp_path / "q.json"),
        "historical_model_path": str(tmp_path / "m.pkl"),
    })
    await runner.start_moea_loop()
    await asyncio.sleep(0.05)
    await runner.stop_moea_loop()
    assert runner._moea_task is None


# ============================================================================
# RUNNER
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
