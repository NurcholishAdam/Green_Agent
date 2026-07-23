# File: src/enhancements/adaptive_cost_function.py
"""
Adaptive Sustainability Cost Function with online weight learning.
Extends the base SustainabilityCostFunction with SGD weight adaptation.
"""

import asyncio
import logging
import json
from typing import Dict, Any, List, Optional, Tuple
from collections import deque
from datetime import datetime
import numpy as np

from pydantic import BaseModel, Field
from sqlalchemy import Column, String, Float, DateTime, Integer, JSON, text
from sqlalchemy.ext.declarative import declarative_base

from .sustainability_cost import SustainabilityCostFunction
from ..database.manager import DatabaseManager
from ..expert_registry import ExpertRegistry

logger = logging.getLogger(__name__)

Base = declarative_base()

class FeedbackRecordDB(Base):
    __tablename__ = 'feedback_records'
    id = Column(Integer, primary_key=True)
    request_id = Column(String(128))
    expert_id = Column(String(128))
    node_id = Column(String(128))
    predicted_cost = Column(Float)
    actual_cost = Column(Float)
    energy_joules = Column(Float)
    carbon_kg = Column(Float)
    helium_units = Column(Float)
    latency_ms = Column(Float)
    accuracy = Column(Float)
    timestamp = Column(DateTime, default=datetime.now)
    weights_snapshot = Column(JSON, nullable=True)

class WeightHistoryDB(Base):
    __tablename__ = 'weight_history'
    id = Column(Integer, primary_key=True)
    alpha = Column(Float)
    beta = Column(Float)
    gamma = Column(Float)
    delta = Column(Float)
    epsilon = Column(Float)
    zeta = Column(Float)
    timestamp = Column(DateTime, default=datetime.now)
    reason = Column(String(64), nullable=True)  # e.g., "update", "rollback"

class AdaptiveCostFunction(SustainabilityCostFunction):
    """
    Extends SustainabilityCostFunction with online SGD weight adaptation.
    """

    def __init__(self, config: Dict[str, float]):
        super().__init__(config)
        self.learning_rate = config.get('learning_rate', 0.01)
        self.normalisation_window = config.get('normalisation_window', 1000)
        self.mae_threshold = config.get('mae_threshold', 1.0)  # for rollback
        self.rollback_enabled = config.get('rollback_enabled', True)

        self.history: Dict[str, deque] = {
            'E': deque(maxlen=self.normalisation_window),
            'CO2': deque(maxlen=self.normalisation_window),
            'H': deque(maxlen=self.normalisation_window),
            'M': deque(maxlen=self.normalisation_window),
            'L': deque(maxlen=self.normalisation_window),
            'A': deque(maxlen=self.normalisation_window),
        }
        self.prediction_errors = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self._running = False
        self._validation_task: Optional[asyncio.Task] = None
        self.db_manager: Optional[DatabaseManager] = None
        self.registry: Optional[ExpertRegistry] = None
        self._weight_history: List[Dict[str, float]] = []
        self._last_snapshot: Dict[str, float] = self.weights.copy()

    def inject_dependencies(
        self,
        db_manager: DatabaseManager,
        registry: ExpertRegistry,
        carbon_manager=None,
        helium_dashboard=None,
        node_registry=None
    ):
        self.db_manager = db_manager
        self.registry = registry
        super().inject_dependencies(carbon_manager, helium_dashboard, node_registry)

    async def start_validation_loop(self, interval_seconds: int = 3600):
        """Start background task to monitor weight adaptation performance."""
        self._running = True
        self._validation_task = asyncio.create_task(
            self._validation_loop(interval_seconds)
        )
        logger.info("AdaptiveCostFunction validation loop started")

    async def _validation_loop(self, interval: int):
        while self._running:
            try:
                await self._validate_weights()
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Weight validation error: {e}")
                await asyncio.sleep(60)

    async def _validate_weights(self):
        """Check MAE and optionally roll back weights if performance degrades."""
        if len(self.prediction_errors) < 100:
            return
        errors = list(self.prediction_errors)
        mae = np.mean(np.abs(errors))
        logger.info(f"Weight adaptation MAE (last 100): {mae:.4f}")
        if self.rollback_enabled and mae > self.mae_threshold:
            logger.warning(f"MAE {mae:.4f} exceeds threshold {self.mae_threshold}. Rolling back weights.")
            await self._rollback_weights()

    async def _rollback_weights(self):
        """Restore weights to the last snapshot."""
        async with self._lock:
            self.weights = self._last_snapshot.copy()
            # Record rollback in history
            if self.db_manager:
                await self._persist_weight_history(reason="rollback")
            logger.info("Weights rolled back to previous snapshot.")

    async def record_feedback(
        self,
        context: Dict[str, Any],
        actual_metrics: Dict[str, float]
    ) -> None:
        """
        Record actual metrics after a request and update weights.
        """
        expert_id = context.get('expert_id')
        if not expert_id or not self.registry:
            logger.warning("Missing expert_id or registry; skipping feedback")
            return

        expert = self.registry.get_expert(expert_id)
        if not expert:
            logger.warning(f"Expert {expert_id} not found; skipping feedback")
            return

        # Recompute predicted cost using the same context
        predicted_cost = await self.compute(expert, context)

        # Extract actual metrics
        E = actual_metrics.get('energy_joules', 0.0)
        CO2 = actual_metrics.get('carbon_kg', 0.0)
        H = actual_metrics.get('helium_units', 0.0)
        M = actual_metrics.get('material_index', 0.0)
        L = actual_metrics.get('latency_ms', 0.0)
        A = 1.0 - actual_metrics.get('accuracy', 0.5)

        actual_cost = (
            self.weights.get('alpha', 1.0) * E +
            self.weights.get('beta', 1.0) * CO2 +
            self.weights.get('gamma', 1.0) * H +
            self.weights.get('delta', 1.0) * M +
            self.weights.get('epsilon', 1.0) * L +
            self.weights.get('zeta', 1.0) * A
        )

        # Update normalisation statistics
        async with self._lock:
            self.history['E'].append(E)
            self.history['CO2'].append(CO2)
            self.history['H'].append(H)
            self.history['M'].append(M)
            self.history['L'].append(L)
            self.history['A'].append(A)
            self.prediction_errors.append(actual_cost - predicted_cost)

        # Perform SGD update if we have enough history
        if all(len(q) >= 10 for q in self.history.values()):
            await self._update_weights(E, CO2, H, M, L, A, predicted_cost, actual_cost)

        # Persist feedback record
        if self.db_manager:
            await self._persist_feedback(
                context, actual_metrics, predicted_cost, actual_cost
            )

    async def _update_weights(
        self,
        E: float,
        CO2: float,
        H: float,
        M: float,
        L: float,
        A: float,
        pred: float,
        actual: float
    ):
        """Apply SGD update to weights using normalised inputs."""
        error = pred - actual

        def normalise(value: float, key: str) -> float:
            q = self.history[key]
            if len(q) < 2:
                return 0.0
            mean = np.mean(q)
            std = max(np.std(q), 1e-8)
            return (value - mean) / std

        E_norm = normalise(E, 'E')
        CO2_norm = normalise(CO2, 'CO2')
        H_norm = normalise(H, 'H')
        M_norm = normalise(M, 'M')
        L_norm = normalise(L, 'L')
        A_norm = normalise(A, 'A')

        async with self._lock:
            self.weights['alpha'] -= self.learning_rate * error * E_norm
            self.weights['beta']  -= self.learning_rate * error * CO2_norm
            self.weights['gamma'] -= self.learning_rate * error * H_norm
            self.weights['delta'] -= self.learning_rate * error * M_norm
            self.weights['epsilon'] -= self.learning_rate * error * L_norm
            self.weights['zeta']   -= self.learning_rate * error * A_norm

            # Clamp weights to reasonable range
            for k in self.weights:
                self.weights[k] = max(-5.0, min(5.0, self.weights[k]))

            # Save snapshot if changed significantly
            if abs(self.weights['alpha'] - self._last_snapshot.get('alpha', 0)) > 0.1:
                self._last_snapshot = self.weights.copy()
                if self.db_manager:
                    await self._persist_weight_history(reason="update")

            logger.debug(f"Weights updated: alpha={self.weights['alpha']:.3f}, "
                         f"beta={self.weights['beta']:.3f}, gamma={self.weights['gamma']:.3f}, "
                         f"delta={self.weights['delta']:.3f}, epsilon={self.weights['epsilon']:.3f}, "
                         f"zeta={self.weights['zeta']:.3f}")

    async def _persist_feedback(
        self,
        context: Dict,
        actual: Dict,
        pred: float,
        actual_cost: float
    ):
        """Store feedback record in database for auditing."""
        if not self.db_manager:
            return
        async with self.db_manager.get_session() as session:
            session.execute(
                text("""
                    INSERT INTO feedback_records
                    (request_id, expert_id, node_id, predicted_cost, actual_cost,
                     energy_joules, carbon_kg, helium_units, latency_ms, accuracy,
                     weights_snapshot)
                    VALUES (:request_id, :expert_id, :node_id, :predicted_cost, :actual_cost,
                     :energy_joules, :carbon_kg, :helium_units, :latency_ms, :accuracy,
                     :weights_snapshot)
                """),
                {
                    'request_id': context.get('request_id'),
                    'expert_id': context.get('expert_id'),
                    'node_id': context.get('node_id'),
                    'predicted_cost': pred,
                    'actual_cost': actual_cost,
                    'energy_joules': actual.get('energy_joules', 0),
                    'carbon_kg': actual.get('carbon_kg', 0),
                    'helium_units': actual.get('helium_units', 0),
                    'latency_ms': actual.get('latency_ms', 0),
                    'accuracy': actual.get('accuracy', 0),
                    'weights_snapshot': json.dumps(self.weights)
                }
            )

    async def _persist_weight_history(self, reason: str = "update"):
        """Record weight snapshot for audit and rollback."""
        if not self.db_manager:
            return
        async with self.db_manager.get_session() as session:
            session.execute(
                text("""
                    INSERT INTO weight_history
                    (alpha, beta, gamma, delta, epsilon, zeta, reason)
                    VALUES (:alpha, :beta, :gamma, :delta, :epsilon, :zeta, :reason)
                """),
                {
                    'alpha': self.weights['alpha'],
                    'beta': self.weights['beta'],
                    'gamma': self.weights['gamma'],
                    'delta': self.weights['delta'],
                    'epsilon': self.weights['epsilon'],
                    'zeta': self.weights['zeta'],
                    'reason': reason
                }
            )

    async def stop(self):
        self._running = False
        if self._validation_task:
            self._validation_task.cancel()
            try:
                await self._validation_task
            except asyncio.CancelledError:
                pass
        logger.info("AdaptiveCostFunction stopped")

    def get_weight_history(self, limit: int = 100) -> List[Dict[str, float]]:
        """Return recent weight changes (could be from DB)."""
        # In production, query the weight_history table.
        # For simplicity, we return an empty list.
        return []
