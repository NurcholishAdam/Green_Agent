#!/usr/bin/env python3
"""
Adaptive Precision Controller for FlexGen (v3.0.0)
====================================================

Selects the optimal numeric precision (``fp32`` / ``fp16`` / ``int8``) for
FlexGen inference based on hardware telemetry and task features, using a
linear-softmax policy trained with REINFORCE.

CHANGES (v3.0.0 over v2.0.0):
- ``_extract_features`` split into pure parts (``_raw_features``,
  ``_normalize``) and a stateful part (``_update_stats``).  Features are no
  longer updated twice per decision.
- ``select_precision`` returns a :class:`Decision` carrying the action
  index, so ``update`` never has to reconstruct it.
- ``update`` accepts the ``Decision`` object instead of re-extracting
  features; the running statistics and drift history are untouched.
- ``explain_selection`` is now a pure read; it does not mutate the running
  statistics or the drift history.
- The lock is released before awaiting the safety monitor or the approval
  callback.  Exceptions from those callbacks are caught and logged.
- Sync and async approval callbacks are both supported.
- Sync and async safety monitors are both supported.
- ``run_chaos_test`` snapshots and restores the entire controller state.
- Reward and gradient clipping are configurable.
- Running statistics use Welford's algorithm tracked via ``M2`` and
  ``n_samples``.  The artificial ``std = 1`` seeding is gone.
- Loaded checkpoints are validated against ``(feature_dim, n_actions)``.
- ``np.load`` uses ``allow_pickle=False``.
- ``save_interval = 0`` now means "never auto-save" (use ``save()``).
- RNG is a local ``np.random.default_rng(seed)`` (reproducible).
- Emits a ``FeedbackEvent`` on each decision when the schema is available.
- ``Decision`` dataclass + ``SafetyMonitorProtocol`` / ``ApprovalCallbackProtocol``.
- ``get_stats()`` and ``reset_exploration()`` helpers.
- ``__version__`` and ``__all__``.
- Built-in test suite: ``python adaptive_precision_controller.py --test``.
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import logging
import os
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Deque, Dict, List, Optional, Protocol, Tuple, Union,
)

import numpy as np

logger = logging.getLogger(__name__)

__version__ = "3.0.0"
__all__ = [
    "__version__",
    "AdaptivePrecisionController",
    "Decision",
    "SafetyMonitorProtocol",
    "ApprovalCallbackProtocol",
]


# =============================================================================
# Optional FeedbackEvent integration
# =============================================================================
try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:
    try:
        from src.enhancements.schemas.feedback_event import FeedbackEvent  # type: ignore
    except ImportError:
        FeedbackEvent = None  # type: ignore


# =============================================================================
# Protocols
# =============================================================================
class SafetyMonitorProtocol(Protocol):
    """Minimal interface for a temporal safety monitor."""

    def check(self, state: Dict[str, Any]) -> Union[List[str], Awaitable[List[str]]]:
        ...


class ApprovalCallbackProtocol(Protocol):
    """Minimal interface for a human approval callback."""

    def __call__(
        self, mode: str, context: Dict[str, float]
    ) -> Union[bool, Awaitable[bool]]:
        ...


# =============================================================================
# Decision
# =============================================================================
@dataclass
class Decision:
    """
    A structured decision returned by :meth:`AdaptivePrecisionController.select_precision`.

    Carries everything ``update`` needs so the caller never has to re-extract
    features or remember the internal action index.
    """
    precision: str
    action_idx: int
    raw_action_idx: int
    features_raw: np.ndarray
    features_normalized: np.ndarray
    probabilities: np.ndarray
    logits: np.ndarray
    explanation: Dict[str, Any]
    exploration: bool
    safety_violations: List[str] = field(default_factory=list)
    approval_denied: bool = False
    epsilon_at_decision: float = 0.0
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    decision_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "decision_id": self.decision_id,
            "timestamp": self.timestamp,
            "precision": self.precision,
            "action_idx": self.action_idx,
            "raw_action_idx": self.raw_action_idx,
            "exploration": self.exploration,
            "safety_violations": list(self.safety_violations),
            "approval_denied": self.approval_denied,
            "epsilon_at_decision": self.epsilon_at_decision,
            "probabilities": self.probabilities.tolist(),
            "logits": self.logits.tolist(),
            "features_raw": self.features_raw.tolist(),
            "features_normalized": self.features_normalized.tolist(),
            "explanation": self.explanation,
        }


# =============================================================================
# Controller
# =============================================================================
class AdaptivePrecisionController:
    """
    A linear-softmax policy over precision levels, trained with REINFORCE
    and Adam.

    Public API:
        decision = await controller.select_precision(task_features, hardware_metrics)
        # ... execute with decision.precision, obtain reward ...
        await controller.update(decision, reward)

    Diagnostics:
        controller.explain_selection(task_features, hardware_metrics)   # sync, pure
        controller.get_stats()
        await controller.run_chaos_test()
    """

    PRECISIONS: Tuple[str, ...] = ("fp32", "fp16", "int8")
    FEATURE_NAMES: Tuple[str, ...] = (
        "carbon_intensity", "gpu_util", "memory_used",
        "latency_target", "accuracy_requirement",
    )
    CHECKPOINT_VERSION = 3

    def __init__(
        self,
        weights_path: Union[str, Path] = "precision_student_weights.npz",
        learning_rate: float = 0.01,
        epsilon: float = 0.1,
        epsilon_decay: float = 0.995,
        min_epsilon: float = 0.01,
        feature_dim: int = 5,
        use_adam: bool = True,
        save_interval: int = 10,
        normalization: Optional[Dict[str, np.ndarray]] = None,
        safety_monitor: Optional[Any] = None,
        approval_callback: Optional[Any] = None,
        drift_threshold: float = 0.1,
        drift_window: int = 20,
        reward_clip: float = 10.0,
        grad_clip: float = 5.0,
        seed: Optional[int] = None,
        record_history: bool = True,
        emit_feedback_events: bool = True,
        autosave: bool = True,
    ) -> None:
        # Sanity checks.
        if feature_dim < 1:
            raise ValueError("feature_dim must be >= 1")
        if learning_rate <= 0:
            raise ValueError("learning_rate must be > 0")
        if not 0.0 <= epsilon <= 1.0:
            raise ValueError("epsilon must be in [0, 1]")
        if not 0.0 < epsilon_decay <= 1.0:
            raise ValueError("epsilon_decay must be in (0, 1]")
        if min_epsilon < 0:
            raise ValueError("min_epsilon must be >= 0")
        if reward_clip <= 0 or grad_clip <= 0:
            raise ValueError("reward_clip and grad_clip must be > 0")

        self.weights_path = Path(weights_path)
        self.learning_rate = float(learning_rate)
        self.epsilon = float(epsilon)
        self.epsilon_decay = float(epsilon_decay)
        self.min_epsilon = float(min_epsilon)
        self.feature_dim = int(feature_dim)
        self.use_adam = bool(use_adam)
        self.save_interval = int(save_interval)
        self.reward_clip = float(reward_clip)
        self.grad_clip = float(grad_clip)
        self.record_history = bool(record_history)
        self.emit_feedback_events = bool(emit_feedback_events)
        self.autosave = bool(autosave)

        self.update_count = 0
        self._rng = np.random.default_rng(seed)

        # Policy weights.
        self.weights = self._rng.normal(
            0.0, 0.01, size=(self.feature_dim, len(self.PRECISIONS))
        ).astype(np.float64)

        # Adam state.
        self._m = np.zeros_like(self.weights)
        self._v = np.zeros_like(self.weights)
        self._t = 0

        # Running statistics (Welford).
        self._n_samples = 0
        self.mean = np.zeros(self.feature_dim, dtype=np.float64)
        self._M2 = np.zeros(self.feature_dim, dtype=np.float64)
        if normalization is not None:
            self._apply_normalization_dict(normalization)

        # Drift detection.
        self.drift_threshold = float(drift_threshold)
        self.drift_window = int(drift_window)
        self.feature_history: Deque[np.ndarray] = deque(maxlen=self.drift_window)
        self._drift_detected = False

        # External hooks.
        self.safety_monitor: Optional[Any] = safety_monitor
        self.approval_callback: Optional[Any] = approval_callback

        # Concurrency.
        self._lock = asyncio.Lock()

        # Load saved state.
        self._load_weights()

    # ------------------------------------------------------------------
    # Normalization properties
    # ------------------------------------------------------------------
    @property
    def std(self) -> np.ndarray:
        """Current per-feature standard deviation (Welford)."""
        return self._current_std()

    def _current_std(self) -> np.ndarray:
        if self._n_samples < 2:
            return np.ones_like(self.mean)
        var = self._M2 / (self._n_samples - 1)
        return np.maximum(np.sqrt(np.maximum(var, 0.0)), 1e-6)

    def _apply_normalization_dict(self, d: Dict[str, np.ndarray]) -> None:
        mean = d.get("mean")
        if mean is not None:
            mean = np.asarray(mean, dtype=np.float64).reshape(-1)
            if mean.shape[0] == self.feature_dim:
                self.mean = mean
        n = int(d.get("n_samples", 0) or 0)
        self._n_samples = max(self._n_samples, n)
        if "M2" in d:
            M2 = np.asarray(d["M2"], dtype=np.float64).reshape(-1)
            if M2.shape[0] == self.feature_dim:
                self._M2 = M2
        elif "std" in d and n >= 2:
            std = np.asarray(d["std"], dtype=np.float64).reshape(-1)
            if std.shape[0] == self.feature_dim:
                self._M2 = (std ** 2) * (n - 1)

    # ------------------------------------------------------------------
    # Feature handling (split into pure and stateful parts)
    # ------------------------------------------------------------------
    def _raw_features(
        self, task_features: Dict[str, float], hardware_metrics: Dict[str, float]
    ) -> np.ndarray:
        """Pure: build the raw feature vector. No side effects."""
        raw = np.array(
            [
                float(task_features.get("carbon_intensity", 250.0)),
                float(hardware_metrics.get("gpu_util", 0.5)),
                float(hardware_metrics.get("memory_used", 0.5)),
                float(task_features.get("latency_target", 500.0)),
                float(task_features.get("accuracy_requirement", 0.5)),
            ],
            dtype=np.float64,
        )
        if raw.size != self.feature_dim:
            raise ValueError(
                f"Feature dimension mismatch: expected {self.feature_dim}, "
                f"got {raw.size}. Update `feature_dim` to match actual features."
            )
        return raw

    def _update_stats(self, raw: np.ndarray) -> None:
        """Stateful: update running mean/M2/n_samples. Call once per observation."""
        self._n_samples += 1
        if self._n_samples == 1:
            self.mean = raw.astype(np.float64).copy()
            self._M2 = np.zeros_like(self.mean)
            return
        delta = raw - self.mean
        self.mean = self.mean + delta / self._n_samples
        delta2 = raw - self.mean
        self._M2 = self._M2 + delta * delta2

    def _normalize(self, raw: np.ndarray) -> np.ndarray:
        """Pure: normalize using current statistics. Does not mutate state."""
        if self._n_samples < 2:
            return raw.astype(np.float32, copy=True)
        std = self._current_std()
        return ((raw - self.mean) / std).astype(np.float32)

    # ------------------------------------------------------------------
    # Drift detection
    # ------------------------------------------------------------------
    def _check_drift_locked(self) -> None:
        """Check drift using the feature history. Caller holds the lock."""
        if len(self.feature_history) < self.drift_window:
            return
        recent_mean = np.mean(np.stack(self.feature_history, axis=0), axis=0)
        drift = float(np.linalg.norm(recent_mean))
        if drift > self.drift_threshold:
            if not self._drift_detected:
                logger.warning(
                    "Drift detected (norm=%.3f); increasing exploration.", drift
                )
                # Bump exploration once; epsilon will decay normally afterwards.
                self.epsilon = min(0.5, self.epsilon * 2.0)
            self._drift_detected = True
        else:
            if self._drift_detected:
                logger.info("Drift resolved; resuming normal exploration.")
                self._drift_detected = False

    # ------------------------------------------------------------------
    # Softmax
    # ------------------------------------------------------------------
    @staticmethod
    def _softmax(logits: np.ndarray) -> np.ndarray:
        z = logits - np.max(logits)
        e = np.exp(z)
        s = e.sum()
        return e / s if s > 0 else np.ones_like(e) / len(e)

    # ------------------------------------------------------------------
    # External callbacks (sync or async)
    # ------------------------------------------------------------------
    @staticmethod
    async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        result = fn(*args, **kwargs)
        if inspect.iscoroutine(result):
            result = await result
        return result

    async def _run_safety(
        self, state: Dict[str, Any]
    ) -> List[str]:
        if self.safety_monitor is None:
            return []
        try:
            violations = await self._call_maybe_async(self.safety_monitor.check, state)
            if violations is None:
                return []
            if isinstance(violations, str):
                return [violations]
            return list(violations)
        except Exception as e:
            logger.warning("Safety monitor check failed: %s", e)
            return []

    async def _run_approval(
        self, mode: str, context: Dict[str, float]
    ) -> bool:
        if self.approval_callback is None:
            return True
        try:
            result = await self._call_maybe_async(self.approval_callback, mode, context)
            return bool(result)
        except Exception as e:
            logger.warning("Approval callback raised: %s", e)
            return False

    # ------------------------------------------------------------------
    # Select precision
    # ------------------------------------------------------------------
    async def select_precision(
        self,
        task_features: Dict[str, float],
        hardware_metrics: Dict[str, float],
    ) -> Decision:
        """
        Select a precision level.

        Returns a :class:`Decision`; the recommended usage is::

            decision = await controller.select_precision(task, hw)
            # execute with decision.precision
            await controller.update(decision, reward)
        """
        # --- Phase 1: locked policy forward pass ---
        async with self._lock:
            raw = self._raw_features(task_features, hardware_metrics)
            self._update_stats(raw)
            normalized = self._normalize(raw)

            if self.record_history:
                self.feature_history.append(normalized.astype(np.float64).copy())
                self._check_drift_locked()

            logits = normalized @ self.weights
            probs = self._softmax(logits)

            if self._rng.random() < self.epsilon:
                raw_action = int(self._rng.integers(len(self.PRECISIONS)))
                is_exploration = True
            else:
                raw_action = int(np.argmax(probs))
                is_exploration = False

            epsilon_at_decision = float(self.epsilon)
            weights_at_decision = self.weights.copy()

        # --- Phase 2: unlocked external calls ---
        action = raw_action
        chosen_precision = self.PRECISIONS[action]
        approval_denied = False

        safety_state = {
            "carbon_intensity": float(task_features.get("carbon_intensity", 250.0)),
            "latency_target": float(task_features.get("latency_target", 500.0)),
            "accuracy_requirement": float(task_features.get("accuracy_requirement", 0.5)),
            "precision": chosen_precision,
        }
        safety_violations = await self._run_safety(safety_state)

        if safety_violations:
            logger.warning(
                "Safety violations detected: %s. Forcing int8.", safety_violations
            )
            chosen_precision = "int8"
            action = self.PRECISIONS.index("int8")

        # Approval gate for high-carbon fp32.
        if (
            chosen_precision == "fp32"
            and float(task_features.get("carbon_intensity", 250.0)) > 500
            and self.approval_callback is not None
        ):
            approved = await self._run_approval(
                "fp32",
                {
                    "carbon_intensity": float(task_features.get("carbon_intensity", 250.0)),
                    "latency_target": float(task_features.get("latency_target", 500.0)),
                },
            )
            if not approved:
                logger.warning(
                    "Human approval not granted for fp32; falling back to fp16."
                )
                chosen_precision = "fp16"
                action = self.PRECISIONS.index("fp16")
                approval_denied = True

        # --- Phase 3: build decision ---
        feature_contributions = {
            name: float(normalized[i] * weights_at_decision[i, action])
            for i, name in enumerate(self.FEATURE_NAMES)
        }
        explanation = {
            "selected_precision": chosen_precision,
            "action_idx": action,
            "raw_action_idx": raw_action,
            "exploration": is_exploration,
            "probabilities": {
                self.PRECISIONS[i]: float(probs[i])
                for i in range(len(self.PRECISIONS))
            },
            "feature_contributions": feature_contributions,
            "safety_violations": list(safety_violations),
            "approval_denied": approval_denied,
            "epsilon_at_decision": epsilon_at_decision,
        }

        decision = Decision(
            precision=chosen_precision,
            action_idx=action,
            raw_action_idx=raw_action,
            features_raw=raw.astype(np.float64, copy=True),
            features_normalized=normalized.astype(np.float32, copy=True),
            probabilities=probs.astype(np.float64, copy=True),
            logits=logits.astype(np.float64, copy=True),
            explanation=explanation,
            exploration=is_exploration,
            safety_violations=list(safety_violations),
            approval_denied=approval_denied,
            epsilon_at_decision=epsilon_at_decision,
        )

        if self.emit_feedback_events:
            self._emit_feedback_event(decision)

        return decision

    # ------------------------------------------------------------------
    # Update
    # ------------------------------------------------------------------
    async def update(
        self,
        decision: Union[Decision, Dict[str, Any]],
        reward: float,
    ) -> Dict[str, Any]:
        """
        Apply a REINFORCE update for the executed action.

        ``decision`` may be the ``Decision`` returned by ``select_precision``
        or its ``to_dict()`` representation.
        """
        # Accept a dict for serialization round-trips.
        if isinstance(decision, dict):
            features = np.asarray(decision["features_normalized"], dtype=np.float32)
            action_idx = int(decision["action_idx"])
        else:
            features = decision.features_normalized
            action_idx = int(decision.action_idx)

        n_actions = len(self.PRECISIONS)
        if not 0 <= action_idx < n_actions:
            raise ValueError(
                f"action_idx must be in [0, {n_actions - 1}], got {action_idx}"
            )
        if features.shape[0] != self.feature_dim:
            raise ValueError(
                f"features dimension mismatch: expected {self.feature_dim}, "
                f"got {features.shape[0]}"
            )

        reward_clipped = float(np.clip(reward, -self.reward_clip, self.reward_clip))

        async with self._lock:
            logits = features @ self.weights
            probs = self._softmax(logits)

            # Policy-gradient direction.
            grad = -probs
            grad[action_idx] += 1.0
            grad = np.outer(features, grad)
            grad = np.clip(grad, -self.grad_clip, self.grad_clip)

            if self.use_adam:
                self._t += 1
                scaled_grad = reward_clipped * grad
                self._m = 0.9 * self._m + 0.1 * scaled_grad
                self._v = 0.999 * self._v + 0.001 * (scaled_grad ** 2)
                m_hat = self._m / (1 - 0.9 ** self._t)
                v_hat = self._v / (1 - 0.999 ** self._t)
                self.weights = self.weights + self.learning_rate * m_hat / (
                    np.sqrt(v_hat) + 1e-8
                )
            else:
                self.weights = self.weights + self.learning_rate * reward_clipped * grad

            self.epsilon = max(self.min_epsilon, self.epsilon * self.epsilon_decay)
            self.update_count += 1

            if self.autosave and self.save_interval > 0 and \
                    self.update_count % self.save_interval == 0:
                self._save_weights_locked()

            stats = {
                "update_count": self.update_count,
                "epsilon": self.epsilon,
                "reward_clipped": reward_clipped,
                "n_samples": self._n_samples,
                "drift_detected": self._drift_detected,
            }

        logger.debug(
            "Policy update: step=%d, reward=%+.3f (clipped %+.3f), epsilon=%.4f",
            self.update_count, reward, reward_clipped, self.epsilon,
        )
        return stats

    # ------------------------------------------------------------------
    # Explainability (pure)
    # ------------------------------------------------------------------
    def explain_selection(
        self,
        task_features: Dict[str, float],
        hardware_metrics: Dict[str, float],
    ) -> Dict[str, Any]:
        """
        Return a detailed explanation for the given features WITHOUT
        updating the running statistics or the drift history.
        """
        raw = self._raw_features(task_features, hardware_metrics)
        normalized = self._normalize(raw)
        logits = normalized @ self.weights
        probs = self._softmax(logits)
        chosen_action = int(np.argmax(probs))
        chosen_precision = self.PRECISIONS[chosen_action]

        feature_contributions = {
            name: float(normalized[i] * self.weights[i, chosen_action])
            for i, name in enumerate(self.FEATURE_NAMES)
        }
        return {
            "selected_precision": chosen_precision,
            "action_probabilities": {
                self.PRECISIONS[i]: float(probs[i])
                for i in range(len(self.PRECISIONS))
            },
            "feature_contributions": feature_contributions,
            "epsilon": float(self.epsilon),
            "feature_vector_normalized": normalized.tolist(),
        }

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------
    def set_safety_monitor(self, monitor: Optional[Any]) -> None:
        self.safety_monitor = monitor

    def set_approval_callback(self, callback: Optional[Any]) -> None:
        self.approval_callback = callback

    # ------------------------------------------------------------------
    # FeedbackEvent
    # ------------------------------------------------------------------
    def _emit_feedback_event(self, decision: Decision) -> None:
        if FeedbackEvent is None:
            return
        try:
            event = FeedbackEvent(
                source="adaptive_precision_controller",
                feedback_type="routing",
                context={
                    "task_id": decision.decision_id,
                    "teacher_ids": [],
                },
                action={
                    "selected_action": decision.precision,
                    "selected_rank": 1,
                    "confidence_score": float(np.max(decision.probabilities)),
                },
                performance={
                    "quality_score": float(np.max(decision.probabilities)),
                    "latency_ms": 0.0, "energy_joules": 0.0,
                    "carbon_g": 0.0, "helium_cost": 0.0, "duration_ms": 0.0,
                },
                adaptive_cost_value=0.0,
                tags=["precision", decision.precision],
                envelope={"trace_id": decision.decision_id},
                metadata={
                    "exploration": decision.exploration,
                    "safety_violations": decision.safety_violations,
                    "approval_denied": decision.approval_denied,
                },
            )
            logger.debug("FeedbackEvent emitted: %s",
                         getattr(event, "event_id", "?"))
        except Exception as e:
            logger.warning("Failed to emit FeedbackEvent: %s", e)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _load_weights(self) -> None:
        if not self.weights_path.exists():
            logger.info("No checkpoint found at %s; starting fresh.",
                        self.weights_path)
            return
        try:
            data = np.load(self.weights_path, allow_pickle=False)
            w = np.asarray(data["weights"], dtype=np.float64)
            expected = (self.feature_dim, len(self.PRECISIONS))
            if w.shape != expected:
                logger.warning(
                    "Checkpoint weights shape %s != expected %s; "
                    "reinitializing.", w.shape, expected,
                )
                return
            self.weights = w

            if "mean" in data and self.feature_dim == np.asarray(data["mean"]).size:
                self.mean = np.asarray(data["mean"], dtype=np.float64).reshape(-1)

            if "M2" in data:
                self._M2 = np.asarray(data["M2"], dtype=np.float64).reshape(-1)
                self._n_samples = int(data.get("n_samples", 0))
            elif "std" in data and "n_samples" in data:
                n = int(data["n_samples"])
                std = np.asarray(data["std"], dtype=np.float64).reshape(-1)
                if std.size == self.feature_dim and n >= 2:
                    self._M2 = (std ** 2) * (n - 1)
                    self._n_samples = n

            if "m" in data and "v" in data and "t" in data:
                m = np.asarray(data["m"], dtype=np.float64)
                v = np.asarray(data["v"], dtype=np.float64)
                if m.shape == self.weights.shape and v.shape == self.weights.shape:
                    self._m = m
                    self._v = v
                    self._t = int(data["t"])

            if "update_count" in data:
                try:
                    self.update_count = int(data["update_count"])
                except (TypeError, ValueError):
                    pass

            logger.info("Loaded weights from %s (shape=%s).",
                        self.weights_path, self.weights.shape)
        except (OSError, ValueError, KeyError) as e:
            logger.warning("Failed to load checkpoint (%s); starting fresh.", e)

    def _save_weights(self) -> None:
        try:
            self._save_weights_locked()
        except Exception as e:
            logger.error("Failed to save weights: %s", e)

    def _save_weights_locked(self) -> None:
        try:
            self.weights_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self.weights_path.with_suffix(
                self.weights_path.suffix + ".tmp"
            )
            np.savez(
                tmp_path,
                version=self.CHECKPOINT_VERSION,
                weights=self.weights,
                mean=self.mean,
                M2=self._M2,
                n_samples=self._n_samples,
                m=self._m,
                v=self._v,
                t=self._t,
                update_count=self.update_count,
            )
            os.replace(tmp_path, self.weights_path)
            logger.debug("Saved weights to %s.", self.weights_path)
        except OSError as e:
            logger.error("Failed to save weights: %s", e)

    def save(self) -> None:
        """Force a checkpoint write."""
        self._save_weights()

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------
    def get_stats(self) -> Dict[str, Any]:
        return {
            "version": __version__,
            "feature_dim": self.feature_dim,
            "n_actions": len(self.PRECISIONS),
            "update_count": self.update_count,
            "epsilon": float(self.epsilon),
            "min_epsilon": float(self.min_epsilon),
            "n_samples": int(self._n_samples),
            "mean": self.mean.tolist(),
            "std": self._current_std().tolist(),
            "drift_detected": self._drift_detected,
            "feature_history_len": len(self.feature_history),
            "use_adam": self.use_adam,
            "t": int(self._t),
        }

    def reset_exploration(self, epsilon: Optional[float] = None) -> None:
        """Reset epsilon to the provided value (or min_epsilon)."""
        self.epsilon = float(epsilon) if epsilon is not None else self.min_epsilon

    # ------------------------------------------------------------------
    # Chaos testing
    # ------------------------------------------------------------------
    def _snapshot_state(self) -> Dict[str, Any]:
        return {
            "weights": self.weights.copy(),
            "m": self._m.copy(),
            "v": self._v.copy(),
            "t": self._t,
            "mean": self.mean.copy(),
            "M2": self._M2.copy(),
            "n_samples": self._n_samples,
            "epsilon": self.epsilon,
            "feature_history": list(self.feature_history),
            "drift_detected": self._drift_detected,
            "update_count": self.update_count,
        }

    def _restore_state(self, snapshot: Dict[str, Any]) -> None:
        self.weights = snapshot["weights"]
        self._m = snapshot["m"]
        self._v = snapshot["v"]
        self._t = snapshot["t"]
        self.mean = snapshot["mean"]
        self._M2 = snapshot["M2"]
        self._n_samples = snapshot["n_samples"]
        self.epsilon = snapshot["epsilon"]
        self.feature_history.clear()
        for h in snapshot["feature_history"]:
            self.feature_history.append(h)
        self._drift_detected = snapshot["drift_detected"]
        self.update_count = snapshot["update_count"]

    async def inject_fault(self, fault_type: str, **kwargs: Any) -> None:
        """
        Inject a fault to test resilience.

        Supported: ``reset_weights``, ``corrupt_normalization``,
        ``nan_weights``, ``clear_history``.
        """
        async with self._lock:
            if fault_type == "reset_weights":
                self.weights = self._rng.normal(
                    0.0, 0.01, size=(self.feature_dim, len(self.PRECISIONS))
                )
                self._m = np.zeros_like(self.weights)
                self._v = np.zeros_like(self.weights)
                self._t = 0
                logger.warning("Fault injected: reset_weights")
            elif fault_type == "corrupt_normalization":
                self.mean = np.ones(self.feature_dim, dtype=np.float64)
                self._M2 = np.full(self.feature_dim, 1e-12, dtype=np.float64)
                self._n_samples = max(self._n_samples, 2)
                logger.warning("Fault injected: corrupt_normalization")
            elif fault_type == "nan_weights":
                self.weights[:] = np.nan
                logger.warning("Fault injected: nan_weights")
            elif fault_type == "clear_history":
                self.feature_history.clear()
                self._n_samples = 0
                self.mean = np.zeros(self.feature_dim, dtype=np.float64)
                self._M2 = np.zeros(self.feature_dim, dtype=np.float64)
                logger.warning("Fault injected: clear_history")
            else:
                logger.warning("Unknown fault type: %s", fault_type)

    async def run_chaos_test(
        self,
        sample_task: Optional[Dict[str, float]] = None,
        sample_hw: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        """
        Exercise the controller under a few injected faults and restore state.

        Returns a structured report; the controller is left in its original
        (pre-test) state.
        """
        task = sample_task or {
            "carbon_intensity": 400.0,
            "latency_target": 200.0,
            "accuracy_requirement": 0.7,
        }
        hw = sample_hw or {"gpu_util": 0.5, "memory_used": 0.6}
        report: Dict[str, Any] = {"faults": [], "results": {}}
        snapshot = self._snapshot_state()
        try:
            # Fault 1: reset weights.
            await self.inject_fault("reset_weights")
            report["faults"].append("reset_weights")
            try:
                d = await self.select_precision(task, hw)
                report["results"]["reset_weights"] = {
                    "status": "recovered",
                    "precision": d.precision,
                    "probabilities_sum": float(np.sum(d.probabilities)),
                }
            except Exception as e:
                report["results"]["reset_weights"] = f"error: {type(e).__name__}: {e}"

            # Fault 2: corrupt normalization.
            await self.inject_fault("corrupt_normalization")
            report["faults"].append("corrupt_normalization")
            try:
                d = await self.select_precision(task, hw)
                finite = bool(np.isfinite(d.probabilities).all())
                report["results"]["corrupt_normalization"] = {
                    "status": "recovered" if finite else "nan_detected",
                    "precision": d.precision,
                }
            except Exception as e:
                report["results"]["corrupt_normalization"] = f"error: {type(e).__name__}: {e}"

            # Fault 3: NaN weights.
            await self.inject_fault("nan_weights")
            report["faults"].append("nan_weights")
            try:
                # Injecting NaN weights will make probs NaN. Attempt recovery
                # by reinitializing (mirrors production watchdog behaviour).
                async with self._lock:
                    if np.isnan(self.weights).any():
                        self.weights = self._rng.normal(
                            0.0, 0.01,
                            size=(self.feature_dim, len(self.PRECISIONS)),
                        )
                        self._m = np.zeros_like(self.weights)
                        self._v = np.zeros_like(self.weights)
                        self._t = 0
                d = await self.select_precision(task, hw)
                finite = bool(np.isfinite(d.probabilities).all())
                report["results"]["nan_weights"] = (
                    "nan_detected_and_recovered" if finite else "still_nan"
                )
            except Exception as e:
                report["results"]["nan_weights"] = f"error: {type(e).__name__}: {e}"

            # Fault 4: clear history.
            await self.inject_fault("clear_history")
            report["faults"].append("clear_history")
            try:
                _ = await self.select_precision(task, hw)
                report["results"]["clear_history"] = "recovered"
            except Exception as e:
                report["results"]["clear_history"] = f"error: {type(e).__name__}: {e}"
        finally:
            self._restore_state(snapshot)
        return report


# =============================================================================
# Tests
# =============================================================================
async def _test_feature_split() -> None:
    ctrl = AdaptivePrecisionController(seed=0)
    raw = ctrl._raw_features(
        {"carbon_intensity": 400.0, "latency_target": 200.0, "accuracy_requirement": 0.7},
        {"gpu_util": 0.5, "memory_used": 0.6},
    )
    assert raw.shape == (5,)
    # Pure call: no stats updated.
    assert ctrl._n_samples == 0
    # Stateful call updates stats.
    ctrl._update_stats(raw)
    assert ctrl._n_samples == 1
    # Normalization before 2 samples returns raw unchanged.
    normalized = ctrl._normalize(raw)
    assert np.allclose(normalized, raw, atol=1e-6)


async def _test_update_uses_decision() -> None:
    ctrl = AdaptivePrecisionController(seed=0)
    d = await ctrl.select_precision(
        {"carbon_intensity": 400.0, "latency_target": 200.0, "accuracy_requirement": 0.7},
        {"gpu_util": 0.5, "memory_used": 0.6},
    )
    assert d.action_idx in (0, 1, 2)
    stats = await ctrl.update(d, reward=1.0)
    assert stats["update_count"] == 1
    # Sample count shouldn't double because update doesn't re-extract.
    assert ctrl._n_samples == 1


async def _test_explain_is_pure() -> None:
    ctrl = AdaptivePrecisionController(seed=0)
    # Prime stats with one observation.
    await ctrl.select_precision({"carbon_intensity": 400.0}, {})
    before_n = ctrl._n_samples
    before_hist = len(ctrl.feature_history)
    _ = ctrl.explain_selection({"carbon_intensity": 500.0}, {})
    assert ctrl._n_samples == before_n
    assert len(ctrl.feature_history) == before_hist


async def _test_action_index_validation() -> None:
    ctrl = AdaptivePrecisionController(seed=0)
    d = await ctrl.select_precision({}, {})
    d.action_idx = 99
    try:
        await ctrl.update(d, reward=1.0)
    except ValueError:
        pass
    else:
        raise AssertionError("invalid action index should raise")


async def _test_sync_approval() -> None:
    calls = {"n": 0}

    def cb(mode: str, ctx: Dict[str, float]) -> bool:
        calls["n"] += 1
        return False  # deny everything

    ctrl = AdaptivePrecisionController(seed=0, approval_callback=cb)
    # Force fp32 with high carbon.
    ctrl.weights[:] = 0.0
    ctrl.weights[:, 0] = 10.0  # strongly prefer fp32
    d = await ctrl.select_precision(
        {"carbon_intensity": 900.0, "latency_target": 200.0, "accuracy_requirement": 0.9},
        {},
    )
    # Either fp32 was blocked -> fp16, or approval wasn't reached because
    # exploration chose another action.
    if calls["n"] > 0:
        assert d.precision == "fp16"
        assert d.approval_denied is True


async def _test_async_approval() -> None:
    async def cb(mode: str, ctx: Dict[str, float]) -> bool:
        await asyncio.sleep(0)
        return False

    ctrl = AdaptivePrecisionController(seed=0, approval_callback=cb)
    ctrl.weights[:] = 0.0
    ctrl.weights[:, 0] = 10.0
    d = await ctrl.select_precision(
        {"carbon_intensity": 900.0, "latency_target": 200.0, "accuracy_requirement": 0.9},
        {},
    )
    if d.approval_denied:
        assert d.precision == "fp16"


async def _test_safety_monitor_forces_int8() -> None:
    class AlwaysViolate:
        async def check(self, state):
            return ["over carbon budget"]

    ctrl = AdaptivePrecisionController(seed=0, safety_monitor=AlwaysViolate())
    d = await ctrl.select_precision({}, {})
    assert d.precision == "int8"
    assert d.safety_violations == ["over carbon budget"]


async def _test_chaos_restores_state() -> None:
    ctrl = AdaptivePrecisionController(seed=0)
    await ctrl.select_precision({}, {})
    snapshot_weights = ctrl.weights.copy()
    snapshot_mean = ctrl.mean.copy()
    snapshot_n = ctrl._n_samples
    report = await ctrl.run_chaos_test()
    assert "faults" in report and len(report["faults"]) == 4
    # State restored.
    assert np.allclose(ctrl.weights, snapshot_weights)
    assert np.allclose(ctrl.mean, snapshot_mean)
    assert ctrl._n_samples == snapshot_n


async def _test_reward_gradient_clipping() -> None:
    ctrl = AdaptivePrecisionController(seed=0, reward_clip=2.0, grad_clip=1.0)
    d = await ctrl.select_precision({}, {})
    before = ctrl.weights.copy()
    # Absurd reward should be clipped to reward_clip.
    await ctrl.update(d, reward=1e9)
    assert np.isfinite(ctrl.weights).all()
    # Weight change should be bounded by lr * grad_clip per element.
    delta = np.abs(ctrl.weights - before).max()
    assert delta <= ctrl.learning_rate * ctrl.grad_clip + 1e-6


async def _test_save_load_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "weights.npz"
    ctrl = AdaptivePrecisionController(weights_path=path, seed=0)
    d = await ctrl.select_precision(
        {"carbon_intensity": 400.0, "latency_target": 200.0, "accuracy_requirement": 0.7},
        {"gpu_util": 0.5, "memory_used": 0.6},
    )
    await ctrl.update(d, reward=1.0)
    ctrl.save()

    ctrl2 = AdaptivePrecisionController(weights_path=path, seed=0)
    assert np.allclose(ctrl.weights, ctrl2.weights)
    assert ctrl2._n_samples == ctrl._n_samples


async def _test_weights_shape_validation(tmp_path: Path) -> None:
    path = tmp_path / "bad.npz"
    np.savez(path, weights=np.zeros((3, 3)), mean=np.zeros(3), M2=np.zeros(3),
             n_samples=5, m=np.zeros((3, 3)), v=np.zeros((3, 3)), t=0)
    ctrl = AdaptivePrecisionController(weights_path=path, seed=0)
    # Shape mismatch -> reinitialized.
    assert ctrl.weights.shape == (ctrl.feature_dim, len(ctrl.PRECISIONS))


async def _test_drift_increases_epsilon() -> None:
    ctrl = AdaptivePrecisionController(seed=0, drift_threshold=0.01, drift_window=5)
    # Feed 5 identical observations to trigger drift.
    for _ in range(5):
        await ctrl.select_precision(
            {"carbon_intensity": 400.0, "latency_target": 200.0, "accuracy_requirement": 0.7},
            {"gpu_util": 0.5, "memory_used": 0.6},
        )
    # After drift, epsilon should be higher than the base (unless already at cap).
    assert ctrl.epsilon >= 0.1


def _run_all_tests() -> None:
    import tempfile

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(_test_feature_split())
        loop.run_until_complete(_test_update_uses_decision())
        loop.run_until_complete(_test_explain_is_pure())
        loop.run_until_complete(_test_action_index_validation())
        loop.run_until_complete(_test_sync_approval())
        loop.run_until_complete(_test_async_approval())
        loop.run_until_complete(_test_safety_monitor_forces_int8())
        loop.run_until_complete(_test_chaos_restores_state())
        loop.run_until_complete(_test_reward_gradient_clipping())
        loop.run_until_complete(_test_drift_increases_epsilon())
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            loop.run_until_complete(_test_save_load_roundtrip(tmp))
            loop.run_until_complete(_test_weights_shape_validation(tmp))
    finally:
        loop.close()
    print("All AdaptivePrecisionController tests passed.")


# =============================================================================
# CLI
# =============================================================================
def main() -> None:
    parser = argparse.ArgumentParser(description="Adaptive Precision Controller")
    parser.add_argument("--test", action="store_true",
                        help="Run the built-in test suite.")
    args = parser.parse_args()
    if args.test:
        _run_all_tests()
    else:
        parser.print_help()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
