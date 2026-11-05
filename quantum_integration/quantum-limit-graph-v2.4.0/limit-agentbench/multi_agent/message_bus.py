# -*- coding: utf-8 -*-
"""
MessageBus (Enhanced v2)

Fixes and enhancements over the original prototype:

Correctness
  * MoE gate now uses a *real* gradient (softmax backprop on a KL loss).
  * Sender encoding is process-stable (SHA-1, not salted ``hash``).
  * Feature vector includes ``density`` (was silently dropped).
  * Student uses Adam + proper distillation + reward-weighted loss.

Robustness
  * Thread-safe (RLock) for publish / dispatch / training.
  * Bounded queue with backpressure policy (drop-oldest / drop-newest / raise).
  * Dead-letter queue for dropped messages.
  * Input validation on payload / config / teacher outputs.

Bus semantics (matching the name)
  * ``subscribe(callback, topic_pattern)`` with MQTT-style ``*`` / ``#`` wildcards.
  * ``Message`` dataclass with ``id``, ``topic``, ``ttl``, ``priority``.
  * Optional priority ordering in ``consume_all``.
  * TTL-based expiry.

Learning
  * Prioritized Experience Replay.
  * Epsilon decay schedule.
  * Configurable reward-shaping constants.
  * Per-teacher outputs preserved (required for correct gate update).

Operability
  * ``stats()`` counters, structured ``logging``.
  * ``save`` / ``load`` checkpointing.
  * Backward-compatible ``consume_all()`` returning ``(sender, payload)`` tuples.
"""

from __future__ import annotations

import hashlib
import logging
import os
import pickle
import random
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
@dataclass
class MessageBusConfig:
    # Master switch — when False, behaves exactly like the original prototype.
    use_enhancements: bool = False

    # ---- Queue / bus behavior ----
    max_queue_size: int = 10_000
    backpressure_policy: str = "drop_oldest"  # "drop_oldest" | "drop_newest" | "raise"
    dead_letter_size: int = 1_000
    default_ttl: Optional[float] = None        # seconds; None = never expires
    priority_mode: str = "fifo"                # "fifo" | "score"

    # ---- LIMIT Graph metrics (all four now reach the feature vector) ----
    graph_metrics: Dict[str, float] = field(default_factory=lambda: {
        "centrality": 0.5,
        "connectivity": 0.5,
        "density": 0.4,
    })

    # ---- MODP weights: [relevance, urgency, energy, carbon] ----
    modp_weights: Optional[List[float]] = None   # default [0.4, 0.3, 0.2, 0.1]

    # ---- RLHF ----
    human_feedback_score: float = 0.5
    human_feedback_history_size: int = 100

    # ---- Distillation + MoE ----
    use_distillation: bool = True
    distillation_lr: float = 0.01
    distillation_betas: Tuple[float, float] = (0.9, 0.999)
    distillation_eps: float = 1e-8
    gating_lr: float = 0.005
    gating_betas: Tuple[float, float] = (0.9, 0.999)
    gating_eps: float = 1e-8
    replay_size: int = 2_000
    replay_batch_size: int = 8
    train_every: int = 10
    distill_w: float = 0.7
    rl_w: float = 0.3

    # ---- Exploration schedule ----
    epsilon_start: float = 0.1
    epsilon_end: float = 0.01
    epsilon_decay_steps: int = 1_000

    # ---- Prioritized Experience Replay ----
    use_prioritized_replay: bool = True
    per_alpha: float = 0.6
    per_beta_start: float = 0.4
    per_beta_steps: int = 10_000
    per_eps: float = 1e-6

    # ---- Reward shaping ----
    drop_important_penalty: float = 0.5
    keep_unimportant_penalty: float = 0.2
    important_threshold: float = 0.7
    unimportant_threshold: float = 0.3
    energy_scale: float = 10.0
    carbon_scale: float = 0.4

    # ---- Bio-inspired (reserved; raises if True since not implemented) ----
    use_evolutionary: bool = False

    # ---- Backward compatibility ----
    legacy_tuples: bool = True  # consume_all returns (sender, payload) tuples


# ------------------------------------------------------------------------------
# Message
# ------------------------------------------------------------------------------
@dataclass(frozen=True)
class Message:
    id: str
    topic: str
    sender: str
    payload: Any
    timestamp: float
    priority: float
    expires_at: Optional[float]

    def is_expired(self, now: Optional[float] = None) -> bool:
        if self.expires_at is None:
            return False
        return (now if now is not None else time.time()) >= self.expires_at


# ------------------------------------------------------------------------------
# MessageState — feature extraction
# ------------------------------------------------------------------------------
class MessageState:
    """Feature vector representing a message and its context."""

    FEATURE_NAMES: Tuple[str, ...] = (
        "importance", "urgency", "energy_norm",
        "centrality", "connectivity", "density",
        "human_feedback", "sender_hash",
    )
    FEATURE_DIM: int = len(FEATURE_NAMES)

    def __init__(self, sender: str, payload: Any,
                 graph_metrics: Dict[str, float],
                 human_feedback: float,
                 energy_scale: float = 10.0):
        self.sender_hash = self._encode_name(sender)

        if isinstance(payload, dict):
            self.importance = self._clamp01(payload.get("importance", 0.5))
            self.urgency = self._clamp01(payload.get("urgency", 0.5))
            self.energy_cost = self._nonneg(payload.get("energy", 0.0))
        else:
            self.importance = 0.5
            self.urgency = 0.5
            self.energy_cost = 0.0

        self.centrality = self._clamp01(graph_metrics.get("centrality", 0.5))
        self.connectivity = self._clamp01(graph_metrics.get("connectivity", 0.5))
        self.density = self._clamp01(graph_metrics.get("density", 0.5))
        self.human_feedback = self._clamp01(human_feedback)
        self._energy_scale = max(1e-6, float(energy_scale))

    @staticmethod
    def _clamp01(v: Any) -> float:
        try:
            x = float(v)
        except (TypeError, ValueError):
            return 0.5
        if not np.isfinite(x):
            return 0.5
        return float(np.clip(x, 0.0, 1.0))

    @staticmethod
    def _nonneg(v: Any) -> float:
        try:
            x = float(v)
        except (TypeError, ValueError):
            return 0.0
        if not np.isfinite(x):
            return 0.0
        return max(0.0, x)

    @staticmethod
    def _encode_name(name: str) -> float:
        # Stable across processes — Python's ``hash`` is salted per process.
        h = hashlib.sha1(name.encode("utf-8")).digest()
        return int.from_bytes(h[:4], "big") / 0xFFFFFFFF

    def to_vector(self) -> np.ndarray:
        return np.array([
            self.importance,
            self.urgency,
            min(self.energy_cost / self._energy_scale, 1.0),
            self.centrality,
            self.connectivity,
            self.density,
            self.human_feedback,
            self.sender_hash,
        ], dtype=np.float64)


# ------------------------------------------------------------------------------
# Adam optimiser
# ------------------------------------------------------------------------------
class _Adam:
    """Minimal Adam implementation that operates on numpy arrays."""

    def __init__(self, shape, lr: float, betas=(0.9, 0.999), eps: float = 1e-8):
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
        param -= self.lr * m_hat / (np.sqrt(v_hat) + self.eps)
        return param


# ------------------------------------------------------------------------------
# Prioritized Replay Buffer
# ------------------------------------------------------------------------------
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

    def sample(self, batch_size: int, beta: float = 0.4):
        with self._lock:
            if self.size == 0:
                return [], np.array([], dtype=np.int64), np.array([])
            p = self.priorities[:self.size] ** self.alpha
            s = p.sum()
            p = p / s if s > 0 else np.ones(self.size) / self.size
            k = min(batch_size, self.size)
            idx = np.random.choice(self.size, size=k, replace=False, p=p)
            items = [self.data[i] for i in idx]
            w = (self.size * p[idx]) ** (-beta)
            w = w / w.max() if w.size and w.max() > 0 else w
            return items, idx, w

    def update_priorities(self, idx, priorities) -> None:
        with self._lock:
            for i, pr in zip(idx, priorities):
                if 0 <= i < self.size:
                    self.priorities[i] = max(float(pr), self.eps)

    def __len__(self) -> int:
        return self.size


# ------------------------------------------------------------------------------
# Distillation + MoE Optimiser
# ------------------------------------------------------------------------------
class MessageDistillationOptimizer:
    """
    Multi-teacher distillation with MoE gating.

    Actions: 0 = drop, 1 = keep.

    Gate gradient derivation
    ------------------------
    Loss = KL( combined_teacher || student )
         = -Σ_a combined_a · log student_a   (up to a constant)
    combined = g @ T                      (g = gate softmax, T = per-teacher probs)
    dLoss/dg_j        = -Σ_a T[j,a] · log(student_a)
    dLoss/dlogit_k    = g_k · (dLoss/dg_k - Σ_j g_j · dLoss/dg_j)
    dLoss/dW          = s ⊗ dLoss/dlogit
    """

    N_ACTIONS = 2

    def __init__(self, config: MessageBusConfig):
        self.config = config
        self.feature_dim = MessageState.FEATURE_DIM

        # Student (linear + bias, trained with Adam)
        self.student_weights = np.zeros((self.feature_dim, self.N_ACTIONS), dtype=np.float64)
        self.student_bias = np.zeros(self.N_ACTIONS, dtype=np.float64)
        self._student_adam_w = _Adam(
            (self.feature_dim, self.N_ACTIONS),
            lr=config.distillation_lr,
            betas=config.distillation_betas,
            eps=config.distillation_eps,
        )
        self._student_adam_b = _Adam(
            (self.N_ACTIONS,),
            lr=config.distillation_lr,
            betas=config.distillation_betas,
            eps=config.distillation_eps,
        )

        # Teachers (injectable list)
        self.teachers: List[Callable[[MessageState], np.ndarray]] = [
            self._rule_teacher,
            self._rlhf_teacher,
            self._historical_teacher,
        ]

        # MoE gate
        n_teachers = len(self.teachers)
        rng = np.random.default_rng(0)
        self.gate_weights = rng.normal(0.0, 0.01, (self.feature_dim, n_teachers))
        self.gate_bias = np.zeros(n_teachers, dtype=np.float64)
        self._gate_adam_w = _Adam(
            (self.feature_dim, n_teachers),
            lr=config.gating_lr,
            betas=config.gating_betas,
            eps=config.gating_eps,
        )
        self._gate_adam_b = _Adam(
            (n_teachers,),
            lr=config.gating_lr,
            betas=config.gating_betas,
            eps=config.gating_eps,
        )

        # Replay + scheduling
        self.replay = _PrioritizedReplay(
            capacity=config.replay_size,
            alpha=config.per_alpha,
            eps=config.per_eps,
        )
        self.counter = 0
        self.train_every = max(1, config.train_every)
        self.batch_size = max(1, config.replay_batch_size)

        # Epsilon schedule
        self._epsilon_start = float(config.epsilon_start)
        self._epsilon_end = float(config.epsilon_end)
        self._epsilon_steps = max(1, config.epsilon_decay_steps)

        # PER beta schedule
        self._beta_start = float(config.per_beta_start)
        self._beta_steps = max(1, config.per_beta_steps)

    # -- teachers -------------------------------------------------------------
    def _rule_teacher(self, state: MessageState) -> np.ndarray:
        if state.importance > 0.7 or state.urgency > 0.7:
            return np.array([0.1, 0.9])
        if state.centrality > 0.7:
            return np.array([0.3, 0.7])
        return np.array([0.8, 0.2])

    def _rlhf_teacher(self, state: MessageState) -> np.ndarray:
        if state.human_feedback > 0.7:
            return np.array([0.2, 0.8])
        if state.human_feedback < 0.3:
            return np.array([0.8, 0.2])
        return np.array([0.5, 0.5])

    def _historical_teacher(self, state: MessageState) -> np.ndarray:
        if state.energy_cost < 0.2 and state.importance > 0.4:
            return np.array([0.3, 0.7])
        return np.array([0.7, 0.3])

    # -- helpers --------------------------------------------------------------
    @staticmethod
    def _softmax(x: np.ndarray) -> np.ndarray:
        x = x - x.max()
        e = np.exp(x)
        return e / e.sum()

    def _validate_teacher_output(self, prob: Any, teacher_idx: int) -> np.ndarray:
        arr = np.asarray(prob, dtype=np.float64).ravel()
        if arr.size != self.N_ACTIONS:
            raise ValueError(
                f"teacher {teacher_idx} returned shape {arr.shape}, "
                f"expected ({self.N_ACTIONS},)"
            )
        if np.any(arr < 0):
            arr = np.clip(arr, 0.0, None)
        s = arr.sum()
        if s <= 0:
            arr = np.full(self.N_ACTIONS, 1.0 / self.N_ACTIONS)
        else:
            arr = arr / s
        return arr

    def _teacher_outputs(self, state: MessageState) -> np.ndarray:
        outs = [self._validate_teacher_output(t(state), i) for i, t in enumerate(self.teachers)]
        return np.stack(outs, axis=0)              # (n_teachers, n_actions)

    def _gate_forward(self, state_vec: np.ndarray) -> np.ndarray:
        return self._softmax(state_vec @ self.gate_weights + self.gate_bias)

    def current_epsilon(self) -> float:
        t = min(self.counter, self._epsilon_steps)
        frac = t / self._epsilon_steps
        return self._epsilon_start + (self._epsilon_end - self._epsilon_start) * frac

    def current_beta(self) -> float:
        t = min(self.counter, self._beta_steps)
        frac = t / self._beta_steps
        return min(1.0, self._beta_start + (1.0 - self._beta_start) * frac)

    # -- public API -----------------------------------------------------------
    def select_action(self, state: MessageState,
                      exploration: bool = True) -> Tuple[int, np.ndarray, np.ndarray, np.ndarray]:
        x = state.to_vector()
        teacher_out = self._teacher_outputs(state)                 # (n_teachers, A)
        gate = self._gate_forward(x)                               # (n_teachers,)
        combined = gate @ teacher_out                              # (A,)
        combined = combined / combined.sum()

        student_probs = self._softmax(x @ self.student_weights + self.student_bias)

        if exploration and random.random() < self.current_epsilon():
            action = random.randint(0, self.N_ACTIONS - 1)
        else:
            blend = 0.8 * student_probs + 0.2 * combined
            action = int(np.argmax(blend))

        # Return *per-teacher* outputs — the gate update requires them.
        return action, x, teacher_out, combined

    def update(self,
               state_vec: np.ndarray,
               action: int,
               reward: float,
               next_state_vec: np.ndarray,
               teacher_out: np.ndarray,
               teacher_combined: np.ndarray) -> None:
        # Reward-weighted priority ≈ |reward| (higher signal → higher priority).
        priority = abs(float(reward)) + self.config.per_eps
        self.replay.add(
            (state_vec.copy(), int(action), float(reward),
             next_state_vec.copy(), teacher_out.copy(), teacher_combined.copy()),
            priority=priority,
        )
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay) >= 2:
            self._train_batch()

    # -- training -------------------------------------------------------------
    def _train_batch(self) -> None:
        beta = self.current_beta()
        batch, idx, weights = self.replay.sample(self.batch_size, beta=beta)
        if not batch:
            return

        new_priorities = []
        for (s, a, r, _ns, t_out, t_comb), w in zip(batch, weights):
            # --- Student ---
            logits = s @ self.student_weights + self.student_bias
            student = self._softmax(logits)

            # Distillation gradient (KL from combined teacher)
            grad_distill = (student - t_comb)           # d/dlogits of KL(t_comb || student)

            # Reward-weighted CE (simple policy-gradient-like surrogate)
            one_hot = np.zeros(self.N_ACTIONS)
            one_hot[a] = 1.0
            grad_rl = -r * (one_hot - student)

            grad_logits = w * (self.config.distill_w * grad_distill
                               + self.config.rl_w * grad_rl)

            gW = np.outer(s, grad_logits)
            gb = grad_logits
            self.student_weights = self._student_adam_w.step(self.student_weights, gW)
            self.student_bias = self._student_adam_b.step(self.student_bias, gb)

            # --- Gate ---
            gate_logits = s @ self.gate_weights + self.gate_bias
            gate = self._softmax(gate_logits)
            log_student = np.log(np.clip(student, 1e-8, 1.0))

            dL_dg = -(t_out @ log_student)              # (n_teachers,)
            dL_dlogits = gate * (dL_dg - np.sum(gate * dL_dg))  # softmax backprop

            gW_gate = np.outer(s, w * dL_dlogits)
            gb_gate = w * dL_dlogits
            self.gate_weights = self._gate_adam_w.step(self.gate_weights, gW_gate)
            self.gate_bias = self._gate_adam_b.step(self.gate_bias, gb_gate)

            # Priority = |TD-error| proxy: |reward - V(s)| with V = E[student]
            v_est = float(student @ np.array([0.0, 1.0]))   # P(keep)
            td = abs(r - v_est)
            new_priorities.append(td + self.config.per_eps)

        self.replay.update_priorities(idx, new_priorities)

    # -- persistence ----------------------------------------------------------
    def state_dict(self) -> Dict[str, Any]:
        return {
            "feature_dim": self.feature_dim,
            "student_weights": self.student_weights.copy(),
            "student_bias": self.student_bias.copy(),
            "gate_weights": self.gate_weights.copy(),
            "gate_bias": self.gate_bias.copy(),
            "counter": self.counter,
        }

    def load_state_dict(self, state: Dict[str, Any]) -> None:
        if state.get("feature_dim") != self.feature_dim:
            raise ValueError("feature dimension mismatch on load")
        self.student_weights = state["student_weights"].copy()
        self.student_bias = state["student_bias"].copy()
        self.gate_weights = state["gate_weights"].copy()
        self.gate_bias = state["gate_bias"].copy()
        self.counter = int(state.get("counter", 0))


# ------------------------------------------------------------------------------
# Topic matching (MQTT-style)
# ------------------------------------------------------------------------------
def _topic_matches(pattern: str, topic: str) -> bool:
    if pattern == "#":
        return True
    p_parts = pattern.split(".")
    t_parts = topic.split(".")
    for i, p in enumerate(p_parts):
        if p == "#":
            return True
        if i >= len(t_parts):
            return False
        if p == "*":
            continue
        if p != t_parts[i]:
            return False
    return len(p_parts) == len(t_parts)


# ------------------------------------------------------------------------------
# MessageBus
# ------------------------------------------------------------------------------
class MessageBus:
    """
    Publish-subscribe bus with optional intelligent message filtering.

    Backward compatibility:
        * ``publish(sender, payload)`` and ``consume_all()`` behave like the
          original prototype when ``use_enhancements=False``.
        * ``consume_all()`` returns a list of ``(sender, payload)`` tuples
          when ``legacy_tuples=True`` (default).
    """

    def __init__(self, config: Optional[MessageBusConfig] = None):
        self.config = config or MessageBusConfig()

        if self.config.use_evolutionary:
            raise NotImplementedError(
                "use_evolutionary=True is reserved but not implemented in v2."
            )

        if self.config.backpressure_policy not in {"drop_oldest", "drop_newest", "raise"}:
            raise ValueError(f"unknown backpressure_policy: {self.config.backpressure_policy}")
        if self.config.priority_mode not in {"fifo", "score"}:
            raise ValueError(f"unknown priority_mode: {self.config.priority_mode}")

        # Validate MODP weights
        if self.config.modp_weights is not None:
            if len(self.config.modp_weights) != 4:
                raise ValueError("modp_weights must have length 4")
            total = float(sum(self.config.modp_weights))
            if total <= 0:
                raise ValueError("modp_weights must sum to a positive value")
            self._modp = [w / total for w in self.config.modp_weights]
        else:
            self._modp = [0.4, 0.3, 0.2, 0.1]

        # Buffers
        self.messages: deque = deque(maxlen=self.config.max_queue_size)
        self.dead_letters: deque = deque(maxlen=self.config.dead_letter_size)

        # Subscribers
        self._subs: Dict[int, Tuple[str, Callable[[Message], None]]] = {}
        self._next_sub_id = 0

        # Lock protects queue, dead letters, subscribers, optimiser updates.
        self._lock = threading.RLock()

        # Stats
        self._stats: Dict[str, float] = {
            "published": 0,
            "kept": 0,
            "dropped": 0,
            "expired": 0,
            "overwritten": 0,
            "dispatched": 0,
            "rewards_sum": 0.0,
        }

        # Learning component
        self.distillation_optimizer: Optional[MessageDistillationOptimizer] = None
        if self.config.use_enhancements and self.config.use_distillation:
            self.distillation_optimizer = MessageDistillationOptimizer(self.config)

    # -- subscription ---------------------------------------------------------
    def subscribe(self, callback: Callable[[Message], None],
                  topic_pattern: str = "#") -> int:
        if not callable(callback):
            raise TypeError("callback must be callable")
        with self._lock:
            sub_id = self._next_sub_id
            self._next_sub_id += 1
            self._subs[sub_id] = (topic_pattern, callback)
        return sub_id

    def unsubscribe(self, sub_id: int) -> bool:
        with self._lock:
            return self._subs.pop(sub_id, None) is not None

    # -- publish --------------------------------------------------------------
    def publish(self, sender: str, payload: Any,
                topic: str = "default",
                priority: Optional[float] = None,
                ttl: Optional[float] = None) -> Optional[Message]:
        """
        Publish a message.

        * When ``use_enhancements=False`` (or distillation disabled) this is
          a plain append — identical to the original behaviour.
        * When enhancements are on, the message is scored/filtered; if kept it
          is stored and dispatched to matching subscribers. Dropped messages
          go to the dead-letter queue.
        """
        # ---------- Legacy fast path ----------
        if (not self.config.use_enhancements) or (self.distillation_optimizer is None):
            with self._lock:
                self._enqueue_legacy(sender, payload)
                self._stats["published"] += 1
                self._stats["kept"] += 1
            # Dispatch to subscribers even in legacy mode (additive feature)
            self._dispatch_legacy(sender, payload, topic)
            return None

        # ---------- Enhanced path ----------
        state = MessageState(
            sender=sender,
            payload=payload,
            graph_metrics=self.config.graph_metrics,
            human_feedback=self.config.human_feedback_score,
            energy_scale=self.config.energy_scale,
        )

        with self._lock:
            action, state_vec, teacher_out, teacher_comb = \
                self.distillation_optimizer.select_action(state)

            reward = self._compute_reward(state, action)
            self._stats["rewards_sum"] += reward

            kept = (action == 1)
            msg = Message(
                id=str(uuid.uuid4()),
                topic=topic,
                sender=sender,
                payload=payload,
                timestamp=time.time(),
                priority=self._priority_of(state, priority),
                expires_at=(time.time() + (ttl if ttl is not None else self.config.default_ttl))
                           if (ttl is not None or self.config.default_ttl is not None) else None,
            )

            if kept:
                stored = self._enqueue(msg)
                self._stats["kept"] += 1
                if stored:
                    self._stats["overwritten"] += 1
            else:
                self.dead_letters.append((msg, "filtered"))
                self._stats["dropped"] += 1

            self._stats["published"] += 1

            self.distillation_optimizer.update(
                state_vec=state_vec,
                action=action,
                reward=reward,
                next_state_vec=state_vec,        # simplified: no real next state
                teacher_out=teacher_out,
                teacher_combined=teacher_comb,
            )

        if kept:
            self._dispatch_message(msg)
        return msg if kept else None

    # -- consume --------------------------------------------------------------
    def consume_all(self) -> List[Any]:
        """Return and clear all currently stored messages."""
        with self._lock:
            now = time.time()
            msgs: List[Message] = []
            for m in self.messages:
                if m.is_expired(now):
                    self.dead_letters.append((m, "expired"))
                    self._stats["expired"] += 1
                else:
                    msgs.append(m)
            self.messages.clear()

            if self.config.priority_mode == "score":
                msgs.sort(key=lambda m: m.priority, reverse=True)

        if self.config.legacy_tuples:
            return [(m.sender, m.payload) for m in msgs]
        return msgs

    def consume_all_messages(self) -> List[Message]:
        """Same as ``consume_all`` but always returns ``Message`` objects."""
        saved = self.config.legacy_tuples
        self.config.legacy_tuples = False
        try:
            return self.consume_all()
        finally:
            self.config.legacy_tuples = saved

    def pending_count(self) -> int:
        with self._lock:
            return len(self.messages)

    # -- stats / operability --------------------------------------------------
    def stats(self) -> Dict[str, float]:
        with self._lock:
            s = dict(self._stats)
            s["pending"] = len(self.messages)
            s["dead_letters"] = len(self.dead_letters)
            s["subscribers"] = len(self._subs)
            s["drop_rate"] = (
                s["dropped"] / s["published"] if s["published"] else 0.0
            )
            s["mean_reward"] = (
                s["rewards_sum"] / s["published"] if s["published"] else 0.0
            )
            if self.distillation_optimizer is not None:
                s["epsilon"] = self.distillation_optimizer.current_epsilon()
                s["replay_size"] = len(self.distillation_optimizer.replay)
            return s

    def save(self, path: str) -> None:
        with self._lock:
            blob: Dict[str, Any] = {
                "config": self.config,
                "messages": list(self.messages),
                "dead_letters": list(self.dead_letters),
                "stats": dict(self._stats),
            }
            if self.distillation_optimizer is not None:
                blob["optimizer"] = self.distillation_optimizer.state_dict()
        tmp = f"{path}.tmp"
        with open(tmp, "wb") as f:
            pickle.dump(blob, f)
        os.replace(tmp, path)

    def load(self, path: str) -> None:
        with open(path, "rb") as f:
            blob = pickle.load(f)
        with self._lock:
            self.config = blob["config"]
            self.messages = deque(blob["messages"], maxlen=self.config.max_queue_size)
            self.dead_letters = deque(blob["dead_letters"], maxlen=self.config.dead_letter_size)
            self._stats = blob["stats"]
            if self.distillation_optimizer is not None and "optimizer" in blob:
                self.distillation_optimizer.load_state_dict(blob["optimizer"])

    def clear(self) -> None:
        with self._lock:
            self.messages.clear()
            self.dead_letters.clear()

    # -- internals ------------------------------------------------------------
    def _enqueue_legacy(self, sender: str, payload: Any) -> None:
        msg = Message(
            id=str(uuid.uuid4()),
            topic="default",
            sender=sender,
            payload=payload,
            timestamp=time.time(),
            priority=0.0,
            expires_at=None,
        )
        self._enqueue(msg)

    def _enqueue(self, msg: Message) -> bool:
        """Return True if a slot was reclaimed (backpressure fired)."""
        policy = self.config.backpressure_policy
        if len(self.messages) < self.config.max_queue_size:
            self.messages.append(msg)
            return False
        # Full queue
        if policy == "drop_oldest":
            self.messages.append(msg)          # deque auto-evicts oldest
            return True
        if policy == "drop_newest":
            self.dead_letters.append((msg, "backpressure"))
            self._stats["dropped"] += 1
            return False
        raise RuntimeError("MessageBus queue full")

    def _priority_of(self, state: MessageState, override: Optional[float]) -> float:
        if override is not None:
            return float(override)
        # Default priority = MODP reward proxy of the state alone
        relevance = state.importance
        urgency = state.urgency
        energy_eff = 1.0 - min(state.energy_cost / self.config.energy_scale, 1.0)
        carbon_eff = 1.0 - min(state.energy_cost * self.config.carbon_scale, 1.0)
        return float(np.dot([relevance, urgency, energy_eff, carbon_eff], self._modp))

    def _compute_reward(self, state: MessageState, action: int) -> float:
        relevance = state.importance
        urgency = state.urgency
        energy_eff = 1.0 - min(state.energy_cost / self.config.energy_scale, 1.0)
        carbon_eff = 1.0 - min(state.energy_cost * self.config.carbon_scale, 1.0)

        reward = float(np.dot([relevance, urgency, energy_eff, carbon_eff], self._modp))

        if action == 0 and state.importance > self.config.important_threshold:
            reward -= self.config.drop_important_penalty
        if action == 1 and state.importance < self.config.unimportant_threshold:
            reward -= self.config.keep_unimportant_penalty

        return float(np.clip(reward, -1.0, 1.0))

    def _dispatch_legacy(self, sender: str, payload: Any, topic: str) -> None:
        if not self._subs:
            return
        msg = Message(
            id=str(uuid.uuid4()),
            topic=topic,
            sender=sender,
            payload=payload,
            timestamp=time.time(),
            priority=0.0,
            expires_at=None,
        )
        self._dispatch_message(msg)

    def _dispatch_message(self, msg: Message) -> None:
        with self._lock:
            subscribers = list(self._subs.values())
        for pattern, cb in subscribers:
            if not _topic_matches(pattern, msg.topic):
                continue
            try:
                cb(msg)
                with self._lock:
                    self._stats["dispatched"] += 1
            except Exception:                       # noqa: BLE001
                logger.exception("subscriber raised on topic %r", msg.topic)


# ------------------------------------------------------------------------------
# Smoke test (run: ``python -m this_module``)
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Legacy mode: identical behavior to the original prototype ---
    bus = MessageBus()
    bus.publish("alice", {"importance": 0.9, "text": "urgent"})
    bus.publish("bob", "hello")
    out = bus.consume_all()
    assert out == [
        ("alice", {"importance": 0.9, "text": "urgent"}),
        ("bob", "hello"),
    ], out

    # --- Enhanced mode ---
    cfg = MessageBusConfig(
        use_enhancements=True,
        use_distillation=True,
        priority_mode="score",
        max_queue_size=5,
        train_every=2,
    )
    bus = MessageBus(cfg)

    received: List[Message] = []
    bus.subscribe(lambda m: received.append(m), topic_pattern="orders.*")

    for i in range(30):
        bus.publish(
            sender=f"user{i % 3}",
            payload={"importance": (i % 10) / 10.0,
                     "urgency": ((i + 3) % 10) / 10.0,
                     "energy": i * 0.05},
            topic="orders.critical" if i % 2 else "misc",
        )

    out = bus.consume_all_messages()
    print("kept:", len(out), "top-priority example:", out[0].topic if out else None)
    print("stats:", bus.stats())
    print("dispatched to orders.* subscriber:", len(received))

    # --- Persistence round-trip ---
    bus.save("/tmp/msgbus.pkl")
    bus.load("/tmp/msgbus.pkl")
    print("after reload:", bus.stats())
