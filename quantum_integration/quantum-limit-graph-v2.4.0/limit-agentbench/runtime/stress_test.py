# -*- coding: utf-8 -*-
"""
Stress Injection & Spike Simulation (Enhanced v2)

Provides random failure injection, energy spike simulation, and reward spike
simulation for robustness testing of the Green Agent benchmark harness.

---- Critical fixes vs. v1 -----------------------------------------------------
  * The "crash the process" action is REMOVED from the learned action space.
    SIGTERM before reward observation means the policy can never learn from its
    own failure. Enhanced mode now emits a *soft failure* signal to the caller.
    Hard crash remains available via a dedicated ``hard_crash()`` method that is
    never part of the learned action set.
  * ``_compute_reward`` now returns a distinct, informative signal for EVERY
    action (v1 returned ``-0.2`` for all actions under the default context).
    The reward uses ``modp_weights`` — v1 advertised but never read it.
  * ``maybe_inject`` returns a dict with the injected ``energy``/``reward``
    deltas and a ``soft_failure`` flag. v1 returned ``None`` and silently
    no-op'd actions 2 and 3.
  * The MoE gate now uses proper softmax backprop of ``KL(combined||student)``.
    v1's ``np.outer(s, scalar)`` broadcast the same gradient to all teachers.
  * Deterministic via ``StressConfig.seed``; validates ``modp_weights`` length
    (must be exactly 3: [reliability, energy, reward]).
  * ``density`` now reaches the feature vector (feature_dim raised 6 → 7).
  * ``StressState`` fields are clamped to [0,1]; teacher outputs are validated
    (raise, no silent ``np.pad[:n_actions]`` truncation).
  * Adam optimiser, epsilon decay, prioritized replay, and thread-safe updates.

---- Backward compatibility ----------------------------------------------------
  * ``random_failure_injection``, ``energy_spike_simulation``,
    ``reward_spike_simulation`` retain their original signatures and semantics.
  * ``StressInjector(config)`` accepts the same ``StressConfig`` shape.
  * With ``use_enhancements=False`` the bus falls through to the legacy path.

---- FlexGen integration -------------------------------------------------------
  These stress functions test the system's resilience when running LLM inference
  via FlexGen. Call ``injector.maybe_inject(context=..., energy=..., reward=...)``
  before/after FlexGen operations. Inspect the returned dict to apply spikes and
  to react to soft failures. Use ``injector.report_outcome(...)`` afterwards to
  close the reinforcement loop with the *actual* resilience result.
"""

from __future__ import annotations

import logging
import os
import random
import signal
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class StressConfig:
    # Master switch
    use_enhancements: bool = False

    # LIMIT Graph metrics
    graph_metrics: Dict[str, float] = field(default_factory=lambda: {
        "centrality": 0.5,
        "connectivity": 0.5,
        "density": 0.4,
    })

    # MODP weights: [reliability, energy, reward]
    modp_weights: Optional[List[float]] = None   # default [0.4, 0.3, 0.3]

    # RLHF
    human_feedback_score: float = 0.5

    # Distillation + MoE
    use_distillation: bool = True
    distillation_lr: float = 0.01
    distillation_batch: int = 8
    gating_lr: float = 0.005
    replay_size: int = 2000
    train_every: int = 10
    epsilon_start: float = 0.10
    epsilon_end: float = 0.01
    epsilon_decay_steps: int = 1000

    # Bio-inspired (reserved; raises if True)
    use_evolutionary: bool = False

    # Legacy probability knobs (used only on the legacy path)
    failure_prob: float = 0.05
    energy_spike_prob: float = 0.1
    energy_spike_multiplier: float = 5.0
    reward_spike_prob: float = 0.1
    reward_spike_multiplier: float = 10.0

    # Determinism (critical for reproducible stress tests)
    seed: Optional[int] = None

    def __post_init__(self) -> None:
        if self.use_evolutionary:
            raise NotImplementedError(
                "use_evolutionary=True is reserved but not implemented in v2."
            )
        # Normalize + validate MODP weights
        if self.modp_weights is None:
            self.modp_weights = [0.4, 0.3, 0.3]
        if len(self.modp_weights) != 3:
            raise ValueError(
                "modp_weights must have length 3 [reliability, energy, reward]"
            )
        total = float(sum(self.modp_weights))
        if total <= 0:
            raise ValueError("modp_weights must sum to a positive value")
        self.modp_weights = [w / total for w in self.modp_weights]

        # Validate scalar ranges
        if not (0.0 <= self.human_feedback_score <= 1.0):
            raise ValueError("human_feedback_score must be in [0, 1]")
        for k in ("centrality", "connectivity", "density"):
            self.graph_metrics.setdefault(k, 0.5)
            self.graph_metrics[k] = float(
                np.clip(self.graph_metrics[k], 0.0, 1.0)
            )

        # Determinism
        if self.seed is not None:
            random.seed(self.seed)
            np.random.seed(self.seed)


# ---------------------------------------------------------------------------
# Legacy utility functions (signatures preserved)
# ---------------------------------------------------------------------------
def _hard_crash() -> None:
    """Terminate the current process. Cross-platform SIGTERM + fallback."""
    logger.warning("[STRESS] Injecting artificial crash")
    try:
        os.kill(os.getpid(), signal.SIGTERM)
        # Give SIGTERM a chance to be handled; hard-exit if it isn't.
        import time
        time.sleep(0.1)
    except (AttributeError, OSError):
        # Windows or missing signal support
        pass
    os._exit(1)


def random_failure_injection(prob: float = 0.05) -> None:
    """Inject an artificial crash with given probability.

    This stays in the public API for backward compatibility but is NOT part of
    the learned action space. Use ``StressInjector.hard_crash()`` from the
    enhanced path to trigger a controlled crash.
    """
    if random.random() < prob:
        _hard_crash()


def energy_spike_simulation(energy: float, prob: float = 0.1,
                            multiplier: float = 5.0) -> float:
    """Return energy multiplied if a spike occurs (original)."""
    if energy is None:
        return energy
    if random.random() < prob:
        return energy * multiplier
    return energy


def reward_spike_simulation(reward: float, prob: float = 0.1,
                            multiplier: float = 10.0) -> float:
    """Return reward multiplied if a spike occurs (original)."""
    if reward is None:
        return reward
    if random.random() < prob:
        return reward * multiplier
    return reward


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
class StressState:
    """7-dim feature vector for the injection policy."""

    FEATURE_NAMES: Tuple[str, ...] = (
        "task_complexity", "load", "energy_norm",
        "centrality", "connectivity", "density", "human_feedback",
    )
    FEATURE_DIM: int = len(FEATURE_NAMES)

    def __init__(self, context: Dict[str, float],
                 graph_metrics: Dict[str, float],
                 human_feedback: float):
        self.task_complexity = self._clamp(context.get("complexity", 0.5))
        self.load = self._clamp(context.get("load", 0.5))
        self.energy_norm = self._clamp(context.get("energy_norm", 0.0))
        self.centrality = self._clamp(graph_metrics.get("centrality", 0.5))
        self.connectivity = self._clamp(graph_metrics.get("connectivity", 0.5))
        self.density = self._clamp(graph_metrics.get("density", 0.5))
        self.human_feedback = self._clamp(human_feedback)

    @staticmethod
    def _clamp(v: Any) -> float:
        try:
            x = float(v)
        except (TypeError, ValueError):
            return 0.5
        if not np.isfinite(x):
            return 0.5
        return float(np.clip(x, 0.0, 1.0))

    def to_vector(self) -> np.ndarray:
        return np.array([
            self.task_complexity,
            self.load,
            self.energy_norm,
            self.centrality,
            self.connectivity,
            self.density,
            self.human_feedback,
        ], dtype=np.float64)


# ---------------------------------------------------------------------------
# Adam optimiser (minimal)
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
        self.m = self.b1 * self.m + (1.0 - self.b1) * grad
        self.v = self.b2 * self.v + (1.0 - self.b2) * grad * grad
        m_hat = self.m / (1.0 - self.b1 ** self.t)
        v_hat = self.v / (1.0 - self.b2 ** self.t)
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
                priority = float(self.priorities[:self.size].max()) if self.size > 0 else 1.0
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
# Distillation + MoE (fixed)
# ---------------------------------------------------------------------------
class StressDistillationOptimizer:
    """
    Multi-teacher distillation with MoE gating.

    Actions:
      0 = no injection
      1 = soft failure  (recoverable — caller raises / throttles)
      2 = energy spike  (returned to caller)
      3 = reward spike  (returned to caller)

    The hard crash (SIGTERM) is intentionally NOT an action — the learner can
    never observe its own death. Use ``StressInjector.hard_crash()`` for the
    legacy behavior.

    Gate gradient
    -------------
    Loss  = KL(combined_teacher || student)
    combined = g @ T                       (g = gate softmax, T = per-teacher)
    dLoss/dg_j        = -Σ_a T[j,a] · log(student_a)
    dLoss/dlogit_k    = g_k · (dLoss/dg_k - Σ_j g_j · dLoss/dg_j)
    dLoss/dW          = s ⊗ dLoss/dlogit
    """

    N_ACTIONS = 4

    def __init__(self, config: StressConfig,
                 persistence_path: Optional[str] = None):
        self.config = config
        self.feature_dim = StressState.FEATURE_DIM
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
        self.teachers: List[Any] = [
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

        # Replay
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

        if persistence_path and os.path.exists(persistence_path):
            self._load()

    # -- teachers ---------------------------------------------------------
    def _rule_teacher(self, state: StressState) -> np.ndarray:
        probs = np.ones(self.N_ACTIONS) * 0.05
        if state.load > 0.8:
            probs[1] = 0.5
        elif state.energy_norm > 0.5:
            probs[2] = 0.5
        elif state.task_complexity > 0.7:
            probs[3] = 0.4
        else:
            probs[0] = 0.7
        return probs / probs.sum()

    def _rlhf_teacher(self, state: StressState) -> np.ndarray:
        probs = np.ones(self.N_ACTIONS) / self.N_ACTIONS
        if state.human_feedback > 0.7:
            probs[0] += 0.2
        elif state.human_feedback < 0.3:
            probs[1] += 0.2
            probs[2] += 0.1
        return probs / probs.sum()

    def _historical_teacher(self, state: StressState) -> np.ndarray:
        probs = np.ones(self.N_ACTIONS) * 0.05
        if state.centrality > 0.7:
            probs[0] = 0.6
        else:
            probs[1] = 0.3
            probs[2] = 0.3
        return probs / probs.sum()

    # -- helpers ----------------------------------------------------------
    @staticmethod
    def _softmax(x: np.ndarray) -> np.ndarray:
        x = np.clip(x, -50.0, 50.0)
        x = x - x.max()
        e = np.exp(x)
        s = e.sum()
        return e / s if s > 0 else np.full_like(e, 1.0 / e.size)

    def _gate_forward(self, state_vec: np.ndarray) -> np.ndarray:
        return self._softmax(state_vec @ self.gate_weights + self.gate_bias)

    def _teacher_outputs(self, state: StressState) -> np.ndarray:
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
    def select_action(self, state: StressState, exploration: bool = True
                      ) -> Tuple[int, np.ndarray, np.ndarray, np.ndarray]:
        x = state.to_vector()
        teacher_out = self._teacher_outputs(state)                 # (n_teach, A)
        gate = self._gate_forward(x)                               # (n_teach,)
        combined = gate @ teacher_out                              # (A,)
        combined = combined / combined.sum()

        student = self._softmax(x @ self.student_weights + self.student_bias)
        if exploration and random.random() < self.current_epsilon():
            action = random.randint(0, self.N_ACTIONS - 1)
        else:
            action = int(np.argmax(0.8 * student + 0.2 * combined))

        return action, x, teacher_out, combined

    def update(self, state_vec: np.ndarray, action: int, reward: float,
               teacher_out: np.ndarray, teacher_combined: np.ndarray) -> None:
        with self._lock:
            priority = abs(float(reward)) + 1e-6
            self.replay.add(
                (state_vec.copy(), int(action), float(reward),
                 teacher_out.copy(), teacher_combined.copy()),
                priority=priority,
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
            dL_dg = -(t_out @ log_student)                       # (n_teach,)
            dL_dlogit = gate * (dL_dg - float(gate @ dL_dg))     # softmax Jacobian
            self.gate_weights = self._adam_gw.step(
                self.gate_weights, w * np.outer(s, dL_dlogit))
            self.gate_bias = self._adam_gb.step(
                self.gate_bias, w * dL_dlogit)

            # Priority ≈ |TD-error| proxy: |r - P(active)|
            v_est = float(student @ np.array([0.0, 1.0, 1.0, 1.0]))
            new_priorities.append(abs(r - v_est) + 1e-6)

        self.replay.update_priorities(idx, new_priorities)
        self._stats["updates"] += 1

    # -- stats / persistence ----------------------------------------------
    def stats(self) -> Dict[str, Any]:
        with self._lock:
            s = dict(self._stats)
            s["epsilon"] = self.current_epsilon()
            s["replay_size"] = len(self.replay)
            return s

    def _load(self) -> None:
        try:
            with np.load(self.persistence_path) as z:
                self.student_weights = z["student_weights"]
                self.student_bias = z["student_bias"]
                self.gate_weights = z["gate_weights"]
                self.gate_bias = z["gate_bias"]
                self.counter = int(z["counter"])
            logger.info("Loaded distillation checkpoint from %s",
                        self.persistence_path)
        except Exception as exc:                       # noqa: BLE001
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
        os.replace(tmp, self.persistence_path)


# ---------------------------------------------------------------------------
# StressInjector (main API)
# ---------------------------------------------------------------------------
class StressInjector:
    """
    Wrapper for stress injection with optional enhancements.

    Legacy path (``use_enhancements=False``):
      * ``maybe_inject`` runs ``random_failure_injection`` at
        ``config.failure_prob`` and returns spike-adjusted energy / reward.

    Enhanced path (``use_enhancements=True`` and ``use_distillation=True``):
      * A multi-teacher + MoE policy picks one of {none, soft failure,
        energy spike, reward spike}. The result is returned to the caller as a
        dict — the caller must honor ``soft_failure`` and apply the returned
        ``energy`` / ``reward`` values.

    Reinforcement loop:
      * ``maybe_inject`` rewards the *injection quality*.
      * ``report_outcome`` feeds back the *actual* resilience result — this is
        the missing RLHF signal, and it overrides the heuristic reward.
    """

    def __init__(self, config: Optional[StressConfig] = None,
                 persistence_path: Optional[str] = None):
        self.config = config or StressConfig()
        self.use_enhancements = (
            self.config.use_enhancements and self.config.use_distillation
        )
        self._lock = threading.RLock()

        self.distillation_optimizer: Optional[StressDistillationOptimizer] = None
        if self.use_enhancements:
            self.distillation_optimizer = StressDistillationOptimizer(
                self.config, persistence_path=persistence_path,
            )

        # Pending outcomes keyed by action id so report_outcome() can update
        self._pending: Dict[str, Tuple[np.ndarray, int, np.ndarray, np.ndarray]] = {}

    # -- hard crash (never part of the learned action set) ----------------
    def hard_crash(self) -> None:
        """Trigger a controlled process crash (legacy behavior)."""
        _hard_crash()

    # -- main entry point -------------------------------------------------
    def maybe_inject(
        self,
        context: Optional[Dict[str, float]] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None,
        energy: Optional[float] = None,
        reward: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Decide and apply a stress event.

        Returns
        -------
        dict with keys:
            'action'        : int   (0=none, 1=soft failure, 2=energy, 3=reward)
            'energy'        : Optional[float] — possibly spiked value
            'reward'        : Optional[float] — possibly spiked value
            'soft_failure'  : bool — caller should raise / throttle if True
            'hard_crash'    : bool — always False on the enhanced path
            'outcome_id'    : str — pass to report_outcome() later
        """
        context = context or {}
        if graph_metrics is None:
            graph_metrics = self.config.graph_metrics
        hf = (self.config.human_feedback_score
              if human_feedback_score is None else human_feedback_score)

        # ---------------- Legacy path ----------------
        if not self.use_enhancements or self.distillation_optimizer is None:
            if random.random() < self.config.failure_prob:
                logger.warning("[STRESS] legacy crash")
                _hard_crash()
            e = (energy_spike_simulation(
                    energy, self.config.energy_spike_prob,
                    self.config.energy_spike_multiplier)
                 if energy is not None else None)
            r = (reward_spike_simulation(
                    reward, self.config.reward_spike_prob,
                    self.config.reward_spike_multiplier)
                 if reward is not None else None)
            return {
                "action": -1,
                "energy": e,
                "reward": r,
                "soft_failure": False,
                "hard_crash": False,
                "outcome_id": None,
            }

        # ---------------- Enhanced path ----------------
        state = StressState(context, graph_metrics, hf)
        action, s_vec, t_out, t_comb = \
            self.distillation_optimizer.select_action(state)

        e = energy
        r = reward
        soft_failure = False
        if action == 1:
            soft_failure = True
        elif action == 2 and energy is not None:
            e = energy * self.config.energy_spike_multiplier
        elif action == 3 and reward is not None:
            r = reward * self.config.reward_spike_multiplier

        # Heuristic reward for the injection *choice* (updated later by
        # report_outcome with the real resilience signal).
        base_reward = self._compute_reward(context, action)
        self.distillation_optimizer.update(s_vec, action, base_reward,
                                           t_out, t_comb)

        outcome_id = str(id(s_vec)) + ":" + str(action) + ":" + str(self.distillation_optimizer.counter)
        with self._lock:
            self._pending[outcome_id] = (s_vec, action, t_out, t_comb)
            if len(self._pending) > 1024:
                # Prevent unbounded growth if report_outcome is never called
                self._pending.pop(next(iter(self._pending)))

        return {
            "action": action,
            "energy": e,
            "reward": r,
            "soft_failure": soft_failure,
            "hard_crash": False,
            "outcome_id": outcome_id,
        }

    # -- real resilience feedback ----------------------------------------
    def report_outcome(self, outcome_id: str, survived: bool,
                       recovered_ms: Optional[float] = None,
                       energy_delta: Optional[float] = None) -> None:
        """
        Feed back the *actual* resilience result. Overrides the heuristic
        reward for the pending decision, closing the RLHF loop.
        """
        if self.distillation_optimizer is None or outcome_id is None:
            return
        with self._lock:
            pending = self._pending.pop(outcome_id, None)
        if pending is None:
            logger.debug("report_outcome: unknown id %r", outcome_id)
            return

        s_vec, action, t_out, t_comb = pending
        reward = self._resilience_reward(
            survived=survived, action=action,
            recovered_ms=recovered_ms, energy_delta=energy_delta,
        )
        self.distillation_optimizer.update(s_vec, action, reward,
                                           t_out, t_comb)

    # -- reward shaping ---------------------------------------------------
    def _compute_reward(self, context: Dict[str, float], action: int) -> float:
        """Injection-quality heuristic — must differ for every action."""
        load = float(np.clip(context.get("load", 0.5), 0.0, 1.0))
        energy_norm = float(np.clip(context.get("energy_norm", 0.0), 0.0, 1.0))
        complexity = float(np.clip(context.get("complexity", 0.5), 0.0, 1.0))

        w = self.config.modp_weights  # [reliability, energy, reward]

        # Target stress level implied by the context
        desired = 0.5 * load + 0.3 * complexity + 0.2 * energy_norm

        # Actual injected stress (higher is more aggressive)
        injected = {0: 0.0, 1: 0.8, 2: 0.5, 3: 0.4}[action]

        centrality = float(self.config.graph_metrics.get("centrality", 0.5))
        reliability = 1.0 - 0.5 * abs(injected - desired)
        if action == 1:
            # Penalize soft failure on critical nodes
            reliability -= 0.5 * centrality

        # Energy spike only valuable when system is under-utilized
        energy_score = 1.0 - abs((1.0 if action == 2 else 0.0)
                                 - (1.0 - energy_norm))
        # Reward spike only valuable when the task is not already rich in signal
        reward_score = 1.0 - abs((1.0 if action == 3 else 0.0)
                                 - (1.0 - complexity))

        return float(np.clip(
            w[0] * reliability + w[1] * energy_score + w[2] * reward_score,
            -1.0, 1.0,
        ))

    def _resilience_reward(self, survived: bool, action: int,
                           recovered_ms: Optional[float],
                           energy_delta: Optional[float]) -> float:
        """Reward grounded in observed system behavior."""
        if action == 0:
            # No injection: reward is neutral; slightly negative if the system
            # was clearly under stress and needed testing.
            return 0.0
        if not survived:
            return -1.0
        # Survived — bigger reward for higher-stress actions
        base = {1: 0.6, 2: 0.4, 3: 0.3}[action]
        if recovered_ms is not None and recovered_ms > 0:
            # Reward fast recovery
            base += 0.4 / (1.0 + recovered_ms / 1000.0)
        if energy_delta is not None and energy_delta < 0:
            # Spikes that produced *no* energy delta suggest the system absorbed
            # them — small extra reward for that resilience
            base += 0.1
        return float(np.clip(base, -1.0, 1.0))

    # -- operability ------------------------------------------------------
    def stats(self) -> Dict[str, Any]:
        with self._lock:
            s: Dict[str, Any] = {
                "enhanced": self.use_enhancements,
                "pending_outcomes": len(self._pending),
            }
            if self.distillation_optimizer is not None:
                s["distillation"] = self.distillation_optimizer.stats()
            return s

    def save(self) -> None:
        if self.distillation_optimizer is not None:
            self.distillation_optimizer.save()

    def close(self) -> None:
        self.save()

    def __enter__(self) -> "StressInjector":
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
    legacy = StressInjector(StressConfig(use_enhancements=False,
                                         failure_prob=0.0))  # avoid crash
    out = legacy.maybe_inject(energy=100.0, reward=1.0)
    assert out["action"] == -1 and out["energy"] is not None, out
    print("legacy path ok:", out)

    # --- Enhanced path ---
    cfg = StressConfig(
        use_enhancements=True,
        use_distillation=True,
        seed=42,
        graph_metrics={"centrality": 0.6, "connectivity": 0.5, "density": 0.4},
        human_feedback_score=0.6,
    )
    with StressInjector(cfg, persistence_path="/tmp/stress_distill.npz") as inj:
        # Every action must get a distinct reward on the same context
        for ctx in [
            {"load": 0.9, "complexity": 0.7, "energy_norm": 0.4},
            {"load": 0.1, "complexity": 0.2, "energy_norm": 0.1},
            {"load": 0.5, "complexity": 0.5, "energy_norm": 0.5},
        ]:
            rewards = [inj._compute_reward(ctx, a) for a in range(4)]
            assert len(set(round(r, 3) for r in rewards)) == 4, (ctx, rewards)
            print(f"context={ctx} rewards={[round(r,3) for r in rewards]}")

        # Exercise the full loop with outcome feedback
        for step in range(30):
            ctx = {"load": random.random(),
                   "complexity": random.random(),
                   "energy_norm": random.random()}
            res = inj.maybe_inject(context=ctx, energy=50.0, reward=1.0)
            inj.report_outcome(res["outcome_id"],
                               survived=(random.random() > 0.3),
                               recovered_ms=random.uniform(50, 800),
                               energy_delta=res["energy"] - 50.0
                               if res["energy"] is not None else None)
            print(f"step {step:2d} action={res['action']} "
                  f"soft_failure={res['soft_failure']} "
                  f"energy={res['energy']:.1f}")

        print("\nstats:", inj.stats())
