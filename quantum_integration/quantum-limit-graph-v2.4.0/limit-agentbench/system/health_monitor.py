# -*- coding: utf-8 -*-
"""
HealthMonitor (Enhanced v2)

Monitors training health with optional advanced decision-making:
  - LIMIT Graph metrics influence the health score.
  - MODP (multi-objective) reward determines importance.
  - RLHF: human feedback adjusts the policy.
  - Multi-Teacher On-Policy Distillation + MoE: a learned policy decides
    whether the run is healthy / warning / critical.
  - Bio-inspired: reserved (raises if enabled).

---- Critical fixes vs v1 ------------------------------------------------------
  * MoE gate now uses proper softmax backprop of KL(combined || student).
    v1's `np.dot(tp, error)` broadcast a scalar to all teachers — gate
    could not learn.
  * Reward is distance-based: correct=+1.0, adjacent=-0.3, opposite=-1.0,
    scaled by severity confidence. v1 gave `warning` a +0.3 floor that made
    it almost as rewarding as `critical` on truly critical states.
  * In enhanced mode, `check_loss` no longer raises on NaN/explosion —
    severity is a signal for the policy, not a pre-emptive exception.
  * Legacy mode returns `{"status": "warning"|"critical"}` when
    energy/reward checks warn. v1 always returned `{"status": "ok"}`.
  * `energy_norm = energy / budget` (was `/100`, inconsistent with
    `check_energy`).
  * `loss_norm` / `reward_norm` divisors are configurable (`loss_scale`,
    `reward_scale`).
  * `density` metric added to the feature vector (6 → 7 dims).
  * Adam, epsilon decay, prioritized replay, thread safety, determinism,
    checkpointing.
  * `report_outcome()` closes the RLHF loop with actual ground truth.

---- Backward compatibility ---------------------------------------------------
  * `HealthMonitor(config)` accepts the same fields (new fields default).
  * `check_loss`, `check_energy`, `check_reward` retain their signatures.
  * `comprehensive_check` returns a superset of the v1 dict.
  * With `use_enhancements=False` the class behaves like v1 — except that
    the status is now correct (no more false "ok").
"""

from __future__ import annotations

import logging
import random
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np

try:
    import torch
    _TORCH = True
except ImportError:                       # pragma: no cover
    torch = None
    _TORCH = False

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class HealthMonitorConfig:
    use_enhancements: bool = False

    # LIMIT graph
    graph_metrics: Dict[str, float] = field(default_factory=lambda: {
        "centrality": 0.5,
        "connectivity": 0.5,
        "density": 0.5,
    })

    # MODP weights: [loss, energy, reward]
    modp_weights: Optional[List[float]] = None
    loss_scale: float = 1e6                  # divisor for loss_norm
    reward_scale: float = 100.0              # divisor for reward_norm

    # RLHF
    human_feedback_score: float = 0.5

    # Distillation
    use_distillation: bool = True
    distillation_lr: float = 0.01
    distillation_batch: int = 8
    gating_lr: float = 0.005
    replay_size: int = 2000
    train_every: int = 10
    epsilon_start: float = 0.10
    epsilon_end: float = 0.01
    epsilon_decay_steps: int = 1000

    # Bio-inspired (reserved)
    use_evolutionary: bool = False

    # Determinism
    seed: Optional[int] = None

    # Legacy strictness (raise on NaN when enhancements are off)
    strict_legacy_mode: bool = True

    def __post_init__(self) -> None:
        if self.use_evolutionary:
            raise NotImplementedError(
                "use_evolutionary=True is reserved; not implemented in v2."
            )
        if self.modp_weights is None:
            self.modp_weights = [0.4, 0.3, 0.3]
        if len(self.modp_weights) != 3:
            raise ValueError(
                "modp_weights must have length 3 [loss, energy, reward]"
            )
        total = float(sum(self.modp_weights))
        if total <= 0:
            raise ValueError("modp_weights must sum to a positive value")
        self.modp_weights = [w / total for w in self.modp_weights]

        if not (0.0 <= self.human_feedback_score <= 1.0):
            raise ValueError("human_feedback_score must be in [0, 1]")
        for k in ("centrality", "connectivity", "density"):
            self.graph_metrics.setdefault(k, 0.5)
            self.graph_metrics[k] = float(
                np.clip(self.graph_metrics[k], 0.0, 1.0)
            )

        if self.seed is not None:
            random.seed(self.seed)
            np.random.seed(self.seed)


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
class HealthState:
    FEATURE_NAMES: Tuple[str, ...] = (
        "loss_norm", "energy_norm", "reward_norm",
        "centrality", "connectivity", "density", "human_feedback",
    )
    FEATURE_DIM: int = len(FEATURE_NAMES)

    def __init__(self, loss: float, energy: float, reward: float,
                 budget: float,
                 graph_metrics: Dict[str, float],
                 human_feedback: float,
                 loss_scale: float = 1e6,
                 reward_scale: float = 100.0):
        # FIX #5, #6, #7 — use caller-supplied scales, not magic numbers
        ls = max(float(loss_scale), 1e-9)
        rs = max(float(reward_scale), 1e-9)
        budget = max(float(budget), 1e-9)

        self.loss_norm = float(np.clip(abs(float(loss)) / ls, 0.0, 1.0))
        self.energy_norm = float(np.clip(float(energy) / budget, 0.0, 1.0))
        self.reward_norm = float(np.clip(abs(float(reward)) / rs, 0.0, 1.0))

        self.centrality = self._clamp01(graph_metrics.get("centrality", 0.5))
        self.connectivity = self._clamp01(graph_metrics.get("connectivity", 0.5))
        self.density = self._clamp01(graph_metrics.get("density", 0.5))
        self.human_feedback = self._clamp01(human_feedback)

    @staticmethod
    def _clamp01(v: Any) -> float:
        try:
            x = float(v)
        except (TypeError, ValueError):
            return 0.5
        if not np.isfinite(x):
            return 0.5
        return float(np.clip(x, 0.0, 1.0))

    def to_vector(self) -> np.ndarray:
        return np.array([
            self.loss_norm, self.energy_norm, self.reward_norm,
            self.centrality, self.connectivity, self.density,
            self.human_feedback,
        ], dtype=np.float64)

    def severity(self) -> float:
        """Mean severity in [0,1] — the label the policy should learn."""
        return float(np.mean([self.loss_norm, self.energy_norm,
                              self.reward_norm]))


# ---------------------------------------------------------------------------
# Adam optimiser
# ---------------------------------------------------------------------------
class _Adam:
    def __init__(self, shape, lr, betas=(0.9, 0.999), eps=1e-8):
        self.lr = float(lr)
        self.b1, self.b2 = betas
        self.eps = float(eps)
        self.m = np.zeros(shape, dtype=np.float64)
        self.v = np.zeros(shape, dtype=np.float64)
        self.t = 0

    def step(self, param: np.ndarray, grad: np.ndarray) -> np.ndarray:
        self.t += 1
        self.m = self.b1 * self.m + (1 - self.b1) * grad
        self.v = self.b2 * self.v + (1 - self.b2) * grad * grad
        m_hat = self.m / (1 - self.b1 ** self.t)
        v_hat = self.v / (1 - self.b2 ** self.t)
        return param - self.lr * m_hat / (np.sqrt(v_hat) + self.eps)


# ---------------------------------------------------------------------------
# Prioritized replay
# ---------------------------------------------------------------------------
class _PrioritizedReplay:
    def __init__(self, capacity: int, alpha: float = 0.6, eps: float = 1e-6):
        self.capacity = int(capacity)
        self.alpha = float(alpha)
        self.eps = float(eps)
        self.data: List[Any] = []
        self.priorities = np.zeros(self.capacity, dtype=np.float64)
        self.pos = 0
        self.size = 0
        self._lock = threading.Lock()

    def add(self, item, priority: Optional[float] = None) -> None:
        with self._lock:
            if priority is None:
                priority = (float(self.priorities[:self.size].max())
                            if self.size > 0 else 1.0)
            if self.size < self.capacity:
                self.data.append(item)
            else:
                self.data[self.pos] = item
            self.priorities[self.pos] = max(float(priority), self.eps)
            self.pos = (self.pos + 1) % self.capacity
            self.size = min(self.size + 1, self.capacity)

    def sample(self, batch_size: int):
        with self._lock:
            if self.size == 0:
                return [], np.array([], dtype=np.int64), np.array([])
            p = self.priorities[:self.size] ** self.alpha
            s = p.sum()
            p = p / s if s > 0 else np.ones(self.size) / self.size
            k = min(batch_size, self.size)
            idx = np.random.choice(self.size, size=k, replace=False, p=p)
            items = [self.data[i] for i in idx]
            return items, idx, p[idx]

    def update_priorities(self, idx, priorities) -> None:
        with self._lock:
            for i, pr in zip(idx, priorities):
                if 0 <= i < self.size:
                    self.priorities[i] = max(float(pr), self.eps)

    def __len__(self) -> int:
        return self.size


# ---------------------------------------------------------------------------
# Distillation + MoE (FIX #1)
# ---------------------------------------------------------------------------
class HealthDistillationOptimizer:
    """
    Actions: 0 = healthy, 1 = warning, 2 = critical.

    Gate gradient
    -------------
    Loss = KL(combined_teacher || student)
    combined = g @ T                       (g = gate softmax, T = per-teacher)
    dLoss/dg_j     = -Σ_a T[j,a] · log(student_a)
    dLoss/dlogit_k = g_k · (dLoss/dg_k - Σ_j g_j · dLoss/dg_j)
    dLoss/dW       = s ⊗ dLoss/dlogit
    """

    N_ACTIONS = 3
    STATUS_MAP = {0: "healthy", 1: "warning", 2: "critical"}

    def __init__(self, config: HealthMonitorConfig,
                 persistence_path: Optional[str] = None):
        self.config = config
        self.feature_dim = HealthState.FEATURE_DIM
        self.persistence_path = persistence_path
        self._lock = threading.RLock()

        # Student
        self.student_weights = np.zeros(
            (self.feature_dim, self.N_ACTIONS), dtype=np.float64
        )
        self.student_bias = np.zeros(self.N_ACTIONS, dtype=np.float64)
        self._adam_sw = _Adam(self.student_weights.shape, config.distillation_lr)
        self._adam_sb = _Adam(self.student_bias.shape, config.distillation_lr)

        # Teachers (injectable list)
        self.teachers: List[Callable[[HealthState], np.ndarray]] = [
            self._rule_teacher,
            self._rlhf_teacher,
            self._historical_teacher,
        ]

        # MoE gate
        rng = np.random.default_rng(config.seed or 0)
        n_teachers = len(self.teachers)
        self.gate_weights = rng.normal(0.0, 0.01,
                                       (self.feature_dim, n_teachers))
        self.gate_bias = np.zeros(n_teachers, dtype=np.float64)
        self._adam_gw = _Adam(self.gate_weights.shape, config.gating_lr)
        self._adam_gb = _Adam(self.gate_bias.shape, config.gating_lr)

        # Replay + training
        self.replay = _PrioritizedReplay(config.replay_size)
        self.counter = 0
        self.train_every = max(1, config.train_every)
        self.batch_size = max(1, config.distillation_batch)

        # Stats
        self._stats = {
            "actions": [0] * self.N_ACTIONS,
            "rewards_sum": 0.0,
            "updates": 0,
        }

        if persistence_path:
            self._try_load()

    # -- teachers ---------------------------------------------------------
    def _rule_teacher(self, state: HealthState) -> np.ndarray:
        if state.loss_norm > 0.8 or state.reward_norm > 0.8:
            return np.array([0.1, 0.2, 0.7])
        if state.energy_norm > 0.6 or state.loss_norm > 0.4:
            return np.array([0.2, 0.7, 0.1])
        return np.array([0.8, 0.15, 0.05])

    def _rlhf_teacher(self, state: HealthState) -> np.ndarray:
        if state.human_feedback > 0.7:
            return np.array([0.7, 0.2, 0.1])
        if state.human_feedback < 0.3:
            return np.array([0.3, 0.4, 0.3])
        return np.array([0.5, 0.3, 0.2])

    def _historical_teacher(self, state: HealthState) -> np.ndarray:
        if state.centrality > 0.7:
            return np.array([0.6, 0.3, 0.1])
        return np.array([0.4, 0.4, 0.2])

    # -- helpers ----------------------------------------------------------
    @staticmethod
    def _softmax(x: np.ndarray) -> np.ndarray:
        x = np.clip(x, -50.0, 50.0)
        x = x - x.max()
        e = np.exp(x)
        s = e.sum()
        return e / s if s > 0 else np.full_like(e, 1.0 / e.size)

    def _gate_forward(self, s: np.ndarray) -> np.ndarray:
        return self._softmax(s @ self.gate_weights + self.gate_bias)

    def _teacher_outputs(self, state: HealthState) -> np.ndarray:
        outs = []
        for i, t in enumerate(self.teachers):
            p = np.asarray(t(state), dtype=np.float64).ravel()
            if p.size != self.N_ACTIONS:
                raise ValueError(
                    f"teacher {i} returned {p.size} probabilities, "
                    f"expected {self.N_ACTIONS}"
                )
            if np.any(~np.isfinite(p)) or np.any(p < 0):
                p = np.clip(np.nan_to_num(p, nan=0.0), 0.0, None)
            s = p.sum()
            outs.append(p / s if s > 0
                        else np.full(self.N_ACTIONS, 1.0 / self.N_ACTIONS))
        return np.stack(outs, axis=0)

    def current_epsilon(self) -> float:
        t = min(self.counter, self.config.epsilon_decay_steps)
        f = t / self.config.epsilon_decay_steps
        return self.config.epsilon_start + \
               (self.config.epsilon_end - self.config.epsilon_start) * f

    # -- public API -------------------------------------------------------
    def select_health_status(self, state: HealthState,
                             exploration: bool = True
                             ) -> Tuple[int, np.ndarray, np.ndarray, np.ndarray]:
        x = state.to_vector()
        t_out = self._teacher_outputs(state)                  # (n_teachers, A)
        gate = self._gate_forward(x)                          # (n_teachers,)
        combined = gate @ t_out
        combined = combined / combined.sum()

        student = self._softmax(x @ self.student_weights + self.student_bias)
        if exploration and random.random() < self.current_epsilon():
            action = random.randint(0, self.N_ACTIONS - 1)
        else:
            action = int(np.argmax(0.8 * student + 0.2 * combined))

        return action, x, t_out, combined

    def update(self, state_vec: np.ndarray, action: int, reward: float,
               teacher_out: np.ndarray, teacher_combined: np.ndarray) -> None:
        with self._lock:
            self.replay.add(
                (state_vec.copy(), int(action), float(reward),
                 teacher_out.copy(), teacher_combined.copy()),
                priority=abs(float(reward)) + 1e-6,
            )
            self._stats["actions"][int(action)] += 1
            self._stats["rewards_sum"] += float(reward)
            self.counter += 1
            if self.counter % self.train_every == 0 and len(self.replay) >= 2:
                self._train()

    def _train(self) -> None:
        batch, idx, weights = self.replay.sample(self.batch_size)
        if not batch:
            return
        new_priorities = []
        for (s, a, r, t_out, t_comb), w in zip(batch, weights):
            # --- Student ---
            student = self._softmax(s @ self.student_weights + self.student_bias)
            grad_distill = student - t_comb
            one_hot = np.zeros(self.N_ACTIONS); one_hot[a] = 1.0
            grad_rl = -r * (one_hot - student)
            grad = w * (0.7 * grad_distill + 0.3 * grad_rl)
            self.student_weights = self._adam_sw.step(
                self.student_weights, np.outer(s, grad))
            self.student_bias = self._adam_sb.step(self.student_bias, grad)

            # --- Gate: proper softmax backprop of KL(combined || student) ---
            gate = self._gate_forward(s)
            log_student = np.log(np.clip(student, 1e-8, 1.0))
            dL_dg = -(t_out @ log_student)                    # (n_teachers,)
            dL_dlogit = gate * (dL_dg - float(gate @ dL_dg))  # softmax Jacobian
            self.gate_weights = self._adam_gw.step(
                self.gate_weights, w * np.outer(s, dL_dlogit))
            self.gate_bias = self._adam_gb.step(
                self.gate_bias, w * dL_dlogit)

            # Priority = |TD-error| proxy
            v_est = float(student @ np.array([0.0, 1.0, 1.0]))
            new_priorities.append(abs(r - v_est) + 1e-6)

        self.replay.update_priorities(idx, new_priorities)
        self._stats["updates"] += 1

    # -- persistence / stats ---------------------------------------------
    def stats(self) -> Dict[str, Any]:
        with self._lock:
            s = dict(self._stats)
            s["epsilon"] = self.current_epsilon()
            s["replay_size"] = len(self.replay)
            return s

    def _try_load(self) -> None:
        try:
            if self.persistence_path and \
                    __import__("os").path.exists(self.persistence_path):
                with np.load(self.persistence_path) as z:
                    self.student_weights = z["student_weights"]
                    self.student_bias = z["student_bias"]
                    self.gate_weights = z["gate_weights"]
                    self.gate_bias = z["gate_bias"]
                    self.counter = int(z["counter"])
                logger.info("Loaded health distillation checkpoint: %s",
                            self.persistence_path)
        except Exception as exc:                        # noqa: BLE001
            logger.warning("Could not load checkpoint: %s", exc)

    def save(self) -> None:
        if not self.persistence_path:
            return
        with self._lock:
            tmp = f"{self.persistence_path}.tmp.npz"
            np.savez(
                tmp,
                student_weights=self.student_weights,
                student_bias=self.student_bias,
                gate_weights=self.gate_weights,
                gate_bias=self.gate_bias,
                counter=self.counter,
            )
        import os
        os.replace(tmp, self.persistence_path)


# ---------------------------------------------------------------------------
# HealthMonitor
# ---------------------------------------------------------------------------
class HealthMonitor:
    """
    Monitors training health with optional advanced decision-making.

    Legacy mode (use_enhancements=False):
        Returns {'status', 'loss', 'energy', 'reward', 'severity'} with the
        status derived from the same threshold checks that print warnings.
        Raises from check_loss are propagated when strict_legacy_mode=True.

    Enhanced mode:
        Uses a learned multi-teacher policy to classify the run as
        healthy / warning / critical, with an optional `report_outcome()`
        call to close the RLHF loop with the true result.
    """

    def __init__(self, config: Optional[HealthMonitorConfig] = None,
                 persistence_path: Optional[str] = None):
        self.config = config or HealthMonitorConfig()
        self.use_enhancements = (
            self.config.use_enhancements and self.config.use_distillation
        )
        self._lock = threading.RLock()

        self.distillation_optimizer: Optional[HealthDistillationOptimizer] = None
        if self.use_enhancements:
            self.distillation_optimizer = HealthDistillationOptimizer(
                self.config, persistence_path=persistence_path,
            )

        # Pending outcomes keyed by outcome_id
        self._pending: Dict[str, Tuple[np.ndarray, int, np.ndarray, np.ndarray]] = {}

        # Mutable human feedback (was static in v1)
        self.human_feedback_score = float(self.config.human_feedback_score)

    # -- feedback ---------------------------------------------------------
    def update_human_feedback(self, score: float) -> None:
        self.human_feedback_score = float(np.clip(score, 0.0, 1.0))

    # -- legacy checks (signature preserved) ------------------------------
    def check_loss(self, loss) -> Optional[str]:
        """
        Detect NaN / explosion. In legacy strict mode, raises. In enhanced
        mode, returns a severity string and does NOT raise (FIX #3).
        """
        is_nan = False
        is_exploded = False
        if _TORCH and torch.is_tensor(loss):
            is_nan = bool(torch.isnan(loss).any().item())
            is_exploded = bool((loss.abs() > 1e6).any().item())
        else:
            try:
                lv = float(loss)
                is_nan = np.isnan(lv)
                is_exploded = abs(lv) > 1e6
            except (TypeError, ValueError):
                is_nan = True

        if is_nan:
            if self.use_enhancements:
                logger.warning("NaN detected in PPO loss (enhanced mode: flagged)")
                return "nan"
            if self.config.strict_legacy_mode:
                raise RuntimeError("NaN detected in PPO loss")
            logger.warning("NaN detected in PPO loss (non-strict)")
            return "nan"
        if is_exploded:
            if self.use_enhancements:
                logger.warning("Loss explosion detected (enhanced mode: flagged)")
                return "exploded"
            if self.config.strict_legacy_mode:
                raise RuntimeError("Loss explosion detected")
            logger.warning("Loss explosion detected (non-strict)")
            return "exploded"
        return None

    def check_energy(self, energy: float, budget: float) -> Optional[str]:
        if budget <= 0:
            raise ValueError("budget must be positive")
        if energy > budget * 2:
            logger.warning("Extreme energy spike: %.2f > 2 * %.2f",
                           energy, budget)
            return "spike"
        return None

    def check_reward(self, reward: float) -> Optional[str]:
        if abs(reward) > 100:
            logger.warning("Abnormal reward magnitude: %.2f", reward)
            return "abnormal"
        return None

    # -- main entry point -------------------------------------------------
    def comprehensive_check(
        self,
        loss,
        energy: float,
        reward: float,
        budget: float,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        # Legacy checks (may raise in strict legacy mode)
        loss_flag = self.check_loss(loss)
        energy_flag = self.check_energy(energy, budget)
        reward_flag = self.check_reward(reward)

        # Normalize inputs
        if _TORCH and torch.is_tensor(loss):
            loss_val = float(loss.detach().mean().item())
        else:
            loss_val = float(loss)
        energy_val = float(energy)
        reward_val = float(reward)

        # Build state (uses configurable scales)
        if graph_metrics is None:
            graph_metrics = self.config.graph_metrics
        hf = (self.human_feedback_score if human_feedback_score is None
              else float(np.clip(human_feedback_score, 0.0, 1.0)))

        state = HealthState(
            loss=loss_val, energy=energy_val, reward=reward_val,
            budget=budget,
            graph_metrics=graph_metrics,
            human_feedback=hf,
            loss_scale=self.config.loss_scale,
            reward_scale=self.config.reward_scale,
        )

        result: Dict[str, Any] = {
            "loss": loss_val,
            "energy": energy_val,
            "reward": reward_val,
            "severity": state.severity(),
            "loss_flag": loss_flag,
            "energy_flag": energy_flag,
            "reward_flag": reward_flag,
        }

        # ---------------- Legacy path ----------------
        if not self.use_enhancements or self.distillation_optimizer is None:
            # FIX #4 — status reflects energy/reward warnings, not just "ok"
            if loss_flag == "nan" or loss_flag == "exploded":
                result["status"] = "critical"
            elif energy_flag == "spike" or reward_flag == "abnormal":
                result["status"] = "warning"
            elif state.severity() > 0.66:
                result["status"] = "critical"
            elif state.severity() > 0.33:
                result["status"] = "warning"
            else:
                result["status"] = "healthy"
            result["distillation_stats"] = None
            return result

        # ---------------- Enhanced path ----------------
        action, s_vec, t_out, t_comb = \
            self.distillation_optimizer.select_health_status(state)
        status = HealthDistillationOptimizer.STATUS_MAP[action]

        reward_signal = self._reward(state.severity(), action)
        self.distillation_optimizer.update(s_vec, action, reward_signal,
                                           t_out, t_comb)

        outcome_id = f"{self.distillation_optimizer.counter}:{action}:{id(s_vec)}"
        with self._lock:
            self._pending[outcome_id] = (s_vec, action, t_out, t_comb)
            if len(self._pending) > 1024:
                self._pending.pop(next(iter(self._pending)))

        result["status"] = status
        result["action"] = action
        result["outcome_id"] = outcome_id
        result["distillation_stats"] = self.distillation_optimizer.stats()
        return result

    # -- RLHF loop --------------------------------------------------------
    def report_outcome(self, outcome_id: str, correct: Optional[bool] = None,
                       true_severity: Optional[float] = None) -> None:
        """
        Close the RLHF loop. Callers provide ground truth:
          correct=True/False   — was the chosen status correct?
          true_severity ∈ [0,1] — the actual severity (preferred)
        """
        if self.distillation_optimizer is None or outcome_id is None:
            return
        with self._lock:
            pending = self._pending.pop(outcome_id, None)
        if pending is None:
            logger.debug("report_outcome: unknown id %r", outcome_id)
            return

        s_vec, action, t_out, t_comb = pending
        if true_severity is not None:
            reward = self._reward(float(np.clip(true_severity, 0.0, 1.0)),
                                  action)
        elif correct is not None:
            reward = 1.0 if correct else -1.0
        else:
            return
        self.distillation_optimizer.update(s_vec, action, reward, t_out, t_comb)

    # -- reward shaping (FIX #2) -----------------------------------------
    @staticmethod
    def _reward(true_severity: float, action: int) -> float:
        """
        Distance-based reward:
          correct   → +1.0
          adjacent  → -0.3
          opposite  → -1.0
        Magnitude scaled by confidence = |severity - 0.5| * 2.
        """
        if true_severity < 0.33:
            correct = 0
        elif true_severity < 0.66:
            correct = 1
        else:
            correct = 2
        distance = abs(action - correct)
        base = (1.0, -0.3, -1.0)[min(distance, 2)]
        confidence = abs(true_severity - 0.5) * 2.0
        return float(base * (0.5 + 0.5 * confidence))

    # -- operability ------------------------------------------------------
    def stats(self) -> Dict[str, Any]:
        with self._lock:
            s: Dict[str, Any] = {
                "enhanced": self.use_enhancements,
                "pending_outcomes": len(self._pending),
                "human_feedback": self.human_feedback_score,
            }
            if self.distillation_optimizer is not None:
                s["distillation"] = self.distillation_optimizer.stats()
            return s

    def save(self) -> None:
        if self.distillation_optimizer is not None:
            self.distillation_optimizer.save()

    def close(self) -> None:
        self.save()

    def __enter__(self) -> "HealthMonitor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")

    # --- Legacy compatibility ---
    mon = HealthMonitor()
    r = mon.comprehensive_check(loss=0.5, energy=10.0, reward=1.0, budget=100.0)
    assert r["status"] == "healthy", r

    # Legacy mode should now flag a big energy spike instead of returning "ok"
    r = mon.comprehensive_check(loss=0.5, energy=500.0, reward=1.0, budget=100.0)
    assert r["status"] == "warning", r
    print("legacy check ok:", r["status"])

    # --- Enhanced mode ---
    cfg = HealthMonitorConfig(
        use_enhancements=True,
        use_distillation=True,
        seed=42,
        graph_metrics={"centrality": 0.6, "connectivity": 0.5, "density": 0.5},
        human_feedback_score=0.6,
        loss_scale=10.0,
        reward_scale=10.0,
    )
    with HealthMonitor(cfg, persistence_path="/tmp/health_distill.npz") as mon:
        # Ensure the reward is discriminating: correct > adjacent > opposite
        for sev, correct_action in ((0.1, 0), (0.5, 1), (0.9, 2)):
            rewards = [mon._reward(sev, a) for a in range(3)]
            assert rewards[correct_action] == max(rewards), (sev, rewards)
            print(f"severity={sev} rewards={[round(r,3) for r in rewards]}")

        # Full loop with outcome feedback
        for step in range(30):
            sev_target = random.random()
            r = mon.comprehensive_check(
                loss=sev_target * 10.0,
                energy=sev_target * 100.0,
                reward=sev_target * 10.0,
                budget=100.0,
            )
            mon.report_outcome(r["outcome_id"], true_severity=sev_target)
            print(f"step {step:2d} status={r['status']:9s} "
                  f"severity={r['severity']:.2f}")

        print("\nstats:", mon.stats())
