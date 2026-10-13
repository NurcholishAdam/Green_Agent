"""
multi_agent_coordinator.py — Enhanced v17.1.0
==============================================

Advanced multi-agent coordination for MOPD, extended with ten Green Agent
enhancements in a single self-contained file.

FIXES OVER v17.0.0:
 - Per-recipient message routing: send() delivers to the target agent's
   inbox; consume_messages() drains only the matching inbox + broadcast.
 - Role-affinity learning: each agent carries a personal 5-dim affinity
   vector that is updated on reward and used to bias bid().
 - bid() re-locks properly: winner computed and counters updated inside
   the lock, broadcast executed outside (deadlock-free).
 - Byzantine detection uses median + MAD, not a mean-relative threshold.
 - Retired agents are pruned from every coalition.
 - bid() guards against an empty agent pool.
 - Reputations are clamped to [0, 1] after every decay.
 - decay_reputations() is debounced; step() respects the min interval.
 - _on_critical_violation passes distinct metric-rich candidates.
 - Agent state persisted only when the role actually changes.
 - Log shim, Prometheus metrics, orchestrator lifecycle.
 - All shared fixes for the ten enhancement classes.

Self-contained: Python stdlib + optional numpy / sklearn / torch.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import random
import re
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import (
    Any, Awaitable, Callable, Deque, Dict, FrozenSet, List, Optional,
    Set, Tuple,
)

# ---------- Optional dependencies ----------
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore
    NUMPY_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression
    SKLEARN_AVAILABLE = True
except ImportError:  # pragma: no cover
    LinearRegression = None  # type: ignore
    SKLEARN_AVAILABLE = False

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:  # pragma: no cover
    torch = None  # type: ignore
    TORCH_AVAILABLE = False

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter as _PromCounter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False

# ---------- structlog / stdlib logger ----------
try:
    import structlog
    _STRUCTLOG_LOGGER = structlog.get_logger(__name__)
    _STRUCTLOG_AVAILABLE = True
except ImportError:  # pragma: no cover
    _STRUCTLOG_LOGGER = logging.getLogger(__name__)
    _STRUCTLOG_AVAILABLE = False
    logging.basicConfig(level=logging.INFO)

# ---------- Preserved module-level logger ----------
logger = logging.getLogger(__name__)


def log_event(level: str, message: str, **kwargs: Any) -> None:
    """Structured-logging shim (structlog or stdlib)."""
    if _STRUCTLOG_AVAILABLE:
        getattr(_STRUCTLOG_LOGGER, level)(message, **kwargs)
    else:
        if kwargs:
            extra = " ".join(f"{k}={v!r}" for k, v in kwargs.items())
            message = f"{message} | {extra}"
        getattr(logger, level)(message)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# =============================================================================
# CONFIG HELPER
# =============================================================================
def _cfg_get(config: Any, key: str, default: Any = None) -> Any:
    if config is None:
        return default
    if isinstance(config, dict):
        return config.get(key, default)
    if hasattr(config, "model_dump"):
        try:
            return config.model_dump().get(key, default)
        except Exception:
            pass
    if hasattr(config, "dict") and callable(getattr(config, "dict")):
        try:
            return config.dict().get(key, default)
        except Exception:
            pass
    return getattr(config, key, default)


# =============================================================================
# PROMETHEUS METRICS
# =============================================================================
if PROMETHEUS_AVAILABLE:
    _M_MA_AGENTS = Gauge(
        "ma_agents_total", "Agents per role", ["role"],
    )
    _M_MA_BIDS = _PromCounter(
        "ma_bids_total", "Bids", ["outcome"],
    )
    _M_MA_COALITIONS = _PromCounter(
        "ma_coalitions_formed_total", "Coalitions formed",
    )
    _M_MA_VOTES = _PromCounter(
        "ma_coalition_votes_total", "Coalition votes", ["approved"],
    )
    _M_MA_MESSAGES = _PromCounter(
        "ma_messages_total", "Messages sent", ["topic"],
    )
    _M_MA_BYZANTINE = _PromCounter(
        "ma_byzantine_flags_total", "Byzantine flags",
    )
    _M_MA_RETIRED = _PromCounter(
        "ma_retired_agents_total", "Retired agents",
    )
    _M_MA_REPUTATION = Gauge(
        "ma_reputation", "Reputation", ["agent_id"],
    )
    _M_MA_STEP_SECONDS = Histogram(
        "ma_step_seconds", "step() latency",
    )
    _M_PRECISION_SWITCHES = _PromCounter(
        "ma_precision_switches_total", "Precision switches",
        ["from_p", "to_p"],
    )
    _M_CHAOS = _PromCounter(
        "ma_chaos_experiments_total", "Chaos experiments",
        ["fault_type", "status"],
    )
else:
    class _Dummy:
        def labels(self, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass

    _M_MA_AGENTS = _M_MA_BIDS = _M_MA_COALITIONS = _M_MA_VOTES = _Dummy()
    _M_MA_MESSAGES = _M_MA_BYZANTINE = _M_MA_RETIRED = _Dummy()
    _M_MA_REPUTATION = _M_MA_STEP_SECONDS = _Dummy()
    _M_PRECISION_SWITCHES = _M_CHAOS = _Dummy()


# =============================================================================
# ENHANCEMENT 4 — MULTI-AGENT COORDINATION
# =============================================================================
class _Agent:
    """An agent with reputation, utilities, personal affinity, and lifecycle."""

    ROLES = ["orchestrator", "validator", "optimizer", "reporter", "negotiator"]

    def __init__(self, agent_id: str):
        self.id = agent_id
        self.role = "validator"
        self.reputation = 0.5
        self.utilities = {r: random.uniform(0.3, 0.7) for r in self.ROLES}
        # Personal preference over roles (5-dim), initialised from utilities.
        base = [self.utilities[r] for r in self.ROLES]
        s = sum(base) or 1.0
        self.affinity: List[float] = [x / s for x in base]
        self.completed = 0
        self.joined_at = _now_utc()
        self.last_active = self.joined_at
        self.byzantine_flags = 0
        # Cache of last persisted role to avoid redundant storage writes.
        self._last_persisted_role: Optional[str] = None


@dataclass
class _Message:
    """An inter-agent message with optional reply expectations."""
    message_id: str
    topic: str
    sender: str
    recipient: str  # "*" for broadcast, or a specific agent_id
    payload: Dict[str, Any]
    expects_reply: bool = False
    reply_to: Optional[str] = None
    created_at: str = field(
        default_factory=lambda: _now_utc().isoformat())


class MultiAgentCoordinator:
    """
    Advanced multi-agent coordination for MOPD.

    v17.1 enhancements:
      * Per-recipient message routing (inbox + broadcast).
      * Per-agent affinity learning that influences bidding.
      * Deadlock-free locked bidding (compute under lock, broadcast outside).
      * Robust Byzantine detection (median + MAD).
      * Coalition cleanup on retirement.
      * Debounced reputation decay.
    """

    def __init__(self,
                 config: Any,
                 storage: Any,
                 causal_rl: Optional["CausalPolicyAdapter"] = None,
                 temporal: Optional["TemporalLogicVerifier"] = None,
                 xai: Optional["XAIDecisionExplainer"] = None,
                 carbon_market: Optional["CarbonMarketIntegrator"] = None,
                 chaos: Optional["ChaosTestingEngine"] = None,
                 hitl: Optional["ActiveUserPreferenceLearner"] = None,
                 precision: Optional["AdaptivePrecisionSwitcher"] = None,
                 federated: Optional["FederatedGreenAggregator"] = None):
        self.config = config
        self.storage = storage
        count = int(_cfg_get(config, "agent_count", 5) or 5)
        self.agents: Dict[str, _Agent] = {
            f"agent_{i:02d}": _Agent(f"agent_{i:02d}") for i in range(count)
        }

        # Routing: per-recipient inboxes plus a broadcast queue. `bus` is
        # kept as an alias for the broadcast queue for backward compatibility.
        self.bus: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._inboxes: Dict[str, asyncio.Queue] = {
            aid: asyncio.Queue(maxsize=200) for aid in self.agents
        }
        self._lock = asyncio.Lock()

        # Enhancement hooks.
        self.causal_rl = causal_rl
        self.temporal = temporal
        self.xai = xai
        self.carbon_market = carbon_market
        self.chaos = chaos
        self.hitl = hitl
        self.precision = precision
        self.federated = federated

        # Learnt role affinity: {role -> 5-dim aggregate preference}.
        self.role_affinity: Dict[str, List[float]] = {
            role: [0.2] * 5 for role in _Agent.ROLES
        }

        # Decay & detection thresholds.
        self.reputation_decay = float(
            _cfg_get(config, "reputation_decay", 0.999) or 0.999)
        self.byzantine_threshold = float(
            _cfg_get(config, "byzantine_threshold", 0.35) or 0.35)
        self.decay_min_interval_s = float(
            _cfg_get(config, "reputation_decay_interval_s", 60.0) or 60.0)

        self.message_history: Deque[_Message] = deque(maxlen=1000)
        self.coalitions: Dict[str, Set[str]] = {}

        self.agent_generation = 0
        self.retired_agents: Deque[str] = deque(maxlen=200)
        self._last_decay_at: float = 0.0

    # ------------------------------------------------------------------
    # Role specialisation
    # ------------------------------------------------------------------
    async def _specialise(self) -> None:
        """Each agent picks its highest-utility role; persist on change."""
        for a in self.agents.values():
            new_role = max(a.utilities, key=lambda r: a.utilities[r])
            a.role = new_role
            if a._last_persisted_role != new_role:
                a._last_persisted_role = new_role
                try:
                    await asyncio.to_thread(
                        self.storage.save_agent, a.id, a.role,
                        a.reputation, a.utilities)
                except Exception:
                    pass
        try:
            counts: Dict[str, int] = defaultdict(int)
            for a in self.agents.values():
                counts[a.role] += 1
            for role in _Agent.ROLES:
                _M_MA_AGENTS.labels(role=role).set(counts.get(role, 0))
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Bidirectional messaging (per-recipient routing)
    # ------------------------------------------------------------------
    async def send(self, topic: str, sender: str,
                   payload: Dict[str, Any],
                   recipient: str = "*",
                   expects_reply: bool = False,
                   reply_to: Optional[str] = None) -> str:
        """Send a message; returns message_id."""
        msg_id = uuid.uuid4().hex[:12]
        msg = _Message(
            message_id=msg_id, topic=topic, sender=sender,
            recipient=recipient, payload=payload,
            expects_reply=expects_reply, reply_to=reply_to,
        )
        self.message_history.append(msg)

        if recipient == "*":
            try:
                self.bus.put_nowait(msg)
            except asyncio.QueueFull:
                log_event("warning", "MA broadcast queue full; dropping")
        else:
            inbox = self._inboxes.get(recipient)
            if inbox is None:
                # Recipient unknown; fall through to broadcast.
                try:
                    self.bus.put_nowait(msg)
                except asyncio.QueueFull:
                    pass
            else:
                try:
                    inbox.put_nowait(msg)
                except asyncio.QueueFull:
                    log_event("warning",
                              f"MA inbox for {recipient} full; dropping")

        try:
            await asyncio.to_thread(
                self.storage.save_agent_message,
                msg_id, topic, sender, recipient, payload)
        except Exception:
            pass
        try:
            _M_MA_MESSAGES.labels(topic=topic).inc()
        except Exception:
            pass
        return msg_id

    async def broadcast(self, topic: str, sender: str,
                        payload: Dict[str, Any]) -> str:
        return await self.send(topic, sender, payload, recipient="*")

    async def consume_messages(self, max_count: int = 10,
                               agent_id: Optional[str] = None
                               ) -> List[_Message]:
        """
        Drain up to `max_count` messages.

        If `agent_id` is None, drains the broadcast queue only. If given,
        drains that agent's inbox first, then broadcast.
        """
        consumed: List[_Message] = []
        if agent_id is not None:
            inbox = self._inboxes.get(agent_id)
            if inbox is not None:
                for _ in range(max_count):
                    try:
                        consumed.append(inbox.get_nowait())
                    except asyncio.QueueEmpty:
                        break
        remaining = max_count - len(consumed)
        for _ in range(max(0, remaining)):
            try:
                consumed.append(self.bus.get_nowait())
            except asyncio.QueueEmpty:
                break
        return consumed

    async def reply(self, original: _Message, sender: str,
                    payload: Dict[str, Any]) -> str:
        return await self.send(
            topic=f"reply:{original.topic}",
            sender=sender, payload=payload,
            recipient=original.sender,
            reply_to=original.message_id)

    # ------------------------------------------------------------------
    # Bidding (locked, affinity-aware)
    # ------------------------------------------------------------------
    async def bid(self, task: Dict[str, Any]) -> Tuple[str, float]:
        preferred = task.get("preferred_role", "orchestrator")
        try:
            preferred_idx = _Agent.ROLES.index(preferred)
        except ValueError:
            preferred_idx = 0

        async with self._lock:
            if not self.agents:
                raise RuntimeError("No agents available for bidding")
            best_id: Optional[str] = None
            best_score = -1.0
            for aid, a in self.agents.items():
                bonus = 1.0 if a.role == preferred else 0.6
                affinity_bonus = a.affinity[preferred_idx]
                score = (a.utilities[a.role] * bonus
                         + 0.3 * a.reputation
                         + 0.2 * affinity_bonus)
                score += random.uniform(-0.02, 0.02)
                if score > best_score:
                    best_score, best_id = score, aid
            if best_id is not None:
                self.agents[best_id].completed += 1
                self.agents[best_id].last_active = _now_utc()

        await self.broadcast("task_bid", best_id or "none",
                             {"task": task.get("name", "?"),
                              "score": best_score})
        try:
            _M_MA_BIDS.labels(
                outcome="won" if best_id else "none").inc()
        except Exception:
            pass
        return best_id or next(iter(self.agents)), best_score

    # ------------------------------------------------------------------
    # Coalition formation (locked)
    # ------------------------------------------------------------------
    async def form_coalition(self, task: Dict[str, Any],
                             size: int = 3) -> str:
        preferred = task.get("preferred_role", "orchestrator")
        async with self._lock:
            ranked = sorted(
                self.agents.items(),
                key=lambda kv: kv[1].utilities.get(preferred, 0.0)
                + 0.5 * kv[1].reputation,
                reverse=True)
            members = {aid for aid, _ in ranked[:size]}
        coalition_id = f"coal_{uuid.uuid4().hex[:8]}"
        self.coalitions[coalition_id] = members
        await self.broadcast(
            "coalition_formed", "coordinator",
            {"coalition_id": coalition_id,
             "members": sorted(members),
             "task": task.get("name", "?")})
        try:
            _M_MA_COALITIONS.inc()
        except Exception:
            pass
        return coalition_id

    async def coalition_vote(self, coalition_id: str,
                             proposal: Dict[str, Any]) -> Dict[str, Any]:
        members = set(self.coalitions.get(coalition_id, set()))
        # Prune retired agents.
        members = {aid for aid in members if aid in self.agents}
        if not members:
            return {"approved": False, "reason": "unknown_coalition"}
        threshold = float(proposal.get("threshold", 0.5))
        votes: Dict[str, bool] = {}
        for aid in members:
            a = self.agents[aid]
            votes[aid] = a.utilities.get(a.role, 0.5) >= threshold
        approved = sum(votes.values()) > len(votes) / 2
        try:
            _M_MA_VOTES.labels(approved=str(approved)).inc()
        except Exception:
            pass
        return {"approved": approved, "votes": votes,
                "coalition_id": coalition_id}

    # ------------------------------------------------------------------
    # Task decomposition
    # ------------------------------------------------------------------
    async def decompose_task(self,
                             task: Dict[str, Any]) -> Dict[str, str]:
        subtasks = task.get("subtasks")
        if not subtasks:
            name = task.get("name", "task")
            subtasks = [
                {"name": f"{name}_plan",
                 "preferred_role": "orchestrator"},
                {"name": f"{name}_exec",
                 "preferred_role": "optimizer"},
                {"name": f"{name}_validate",
                 "preferred_role": "validator"},
            ]
        assignments: Dict[str, str] = {}
        for sub in subtasks:
            agent_id, _ = await self.bid(sub)
            assignments[sub.get("name", "subtask")] = agent_id
        return assignments

    # ------------------------------------------------------------------
    # Reputation & reward
    # ------------------------------------------------------------------
    async def reward(self, agent_id: str, reward: float) -> None:
        if agent_id not in self.agents:
            return
        a = self.agents[agent_id]

        # Reputation EMA.
        alpha = 0.1
        a.reputation = max(0.0, min(1.0,
            (1 - alpha) * a.reputation + alpha * reward))

        # Role utility additive update (clamped).
        a.utilities[a.role] = min(1.0, a.utilities[a.role] + 0.05 * reward)

        # Personal affinity: reinforce the agent's current role.
        try:
            idx = _Agent.ROLES.index(a.role)
            a.affinity[idx] = min(1.0, a.affinity[idx] + 0.05 * reward)
            s = sum(a.affinity) or 1.0
            a.affinity = [x / s for x in a.affinity]
        except ValueError:
            pass

        # Aggregate role affinity: blend toward this agent's preference.
        agg = self.role_affinity.get(a.role, [0.2] * 5)
        blended = [0.9 * agg[i] + 0.1 * a.affinity[i] for i in range(5)]
        s = sum(blended) or 1.0
        self.role_affinity[a.role] = [x / s for x in blended]

        a.last_active = _now_utc()
        try:
            _M_MA_REPUTATION.labels(agent_id=a.id).set(a.reputation)
        except Exception:
            pass

    def decay_reputations(self, force: bool = False) -> None:
        """Apply time-decay to all reputations, debounced by min interval."""
        now = time.monotonic()
        if not force and (now - self._last_decay_at) < self.decay_min_interval_s:
            return
        self._last_decay_at = now
        for a in self.agents.values():
            a.reputation = max(0.0, min(1.0,
                a.reputation * self.reputation_decay))

    # ------------------------------------------------------------------
    # Byzantine fault detection (median + MAD)
    # ------------------------------------------------------------------
    @staticmethod
    def _median(values: List[float]) -> float:
        s = sorted(values)
        n = len(s)
        if n == 0:
            return 0.0
        return s[n // 2] if n % 2 else 0.5 * (s[n // 2 - 1] + s[n // 2])

    @staticmethod
    def _mad(values: List[float], median: float) -> float:
        if not values:
            return 0.0
        devs = [abs(v - median) for v in values]
        return MultiAgentCoordinator._median(devs)

    async def detect_byzantine(
        self, outcomes: Dict[str, float],
    ) -> List[str]:
        """
        Flag agents whose outcomes are robust outliers.

        Uses median and MAD (Median Absolute Deviation). The flagging
        threshold is derived from `byzantine_threshold` config (default
        0.35 → k = 3.5).
        """
        if not outcomes:
            return []

        values = list(outcomes.values())
        median = self._median(values)
        mad = self._mad(values, median)
        # 1.4826 * MAD is the consistent estimator of σ for normal data.
        sigma = max(mad * 1.4826, 1e-6 * max(abs(median), 1.0), 1e-6)
        k = max(1.0, self.byzantine_threshold * 10.0)

        flagged: List[str] = []
        for aid, v in outcomes.items():
            z = abs(v - median) / sigma
            if z > k:
                if aid in self.agents:
                    self.agents[aid].byzantine_flags += 1
                    try:
                        _M_MA_BYZANTINE.inc()
                    except Exception:
                        pass
                    if self.agents[aid].byzantine_flags >= 3:
                        flagged.append(aid)
        for aid in flagged:
            await self.retire_agent(aid)
        return flagged

    # ------------------------------------------------------------------
    # Agent lifecycle
    # ------------------------------------------------------------------
    async def retire_agent(self, agent_id: str) -> None:
        if agent_id not in self.agents:
            return
        self.retired_agents.append(agent_id)
        del self.agents[agent_id]
        # Remove from every coalition.
        for cid, members in list(self.coalitions.items()):
            members.discard(agent_id)
            if not members:
                del self.coalitions[cid]
        # Drop the inbox.
        self._inboxes.pop(agent_id, None)

        self.agent_generation += 1
        new_id = f"agent_g{self.agent_generation:03d}"
        self.agents[new_id] = _Agent(new_id)
        self._inboxes[new_id] = asyncio.Queue(maxsize=200)
        try:
            _M_MA_RETIRED.inc()
        except Exception:
            pass
        await self.broadcast(
            "agent_replaced", "coordinator",
            {"retired": agent_id, "successor": new_id})

    async def onboard_agent(self, agent_id: Optional[str] = None) -> str:
        aid = agent_id or f"agent_{uuid.uuid4().hex[:6]}"
        if aid not in self.agents:
            self.agents[aid] = _Agent(aid)
            self._inboxes[aid] = asyncio.Queue(maxsize=200)
            await self.broadcast(
                "agent_onboarded", "coordinator", {"agent_id": aid})
        return aid

    # ------------------------------------------------------------------
    # Policy export
    # ------------------------------------------------------------------
    def get_policy(self) -> List[float]:
        counts: Dict[str, int] = defaultdict(int)
        for a in self.agents.values():
            counts[a.role] += 1
        total = max(1, sum(counts.values()))
        out = [0.0] * 5
        for role, c in counts.items():
            w = c / total
            vec = self.role_affinity.get(role, [0.2] * 5)
            s = sum(vec) or 1.0
            for i, v in enumerate(vec):
                out[i] += w * (v / s)
        total_out = sum(out) or 1.0
        return [x / total_out for x in out]

    # ------------------------------------------------------------------
    # Coordination step
    # ------------------------------------------------------------------
    async def step(self) -> Dict[str, Any]:
        t0 = time.monotonic()
        await self._specialise()
        self.decay_reputations()

        consumed = await self.consume_messages(max_count=20)
        for msg in consumed:
            if msg.expects_reply:
                await self.reply(msg, "coordinator",
                                 {"status": "acknowledged"})

        # Chaos hook (off the critical path if configured).
        if self.chaos is not None and random.random() < 0.05:
            try:
                fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
                await self.chaos.run_experiment(
                    f"multi_agent_{uuid.uuid4().hex[:6]}", fault)
            except Exception as exc:
                log_event("debug", f"chaos hook failed: {exc}")

        # Carbon-aware deferral (does not mutate the market).
        carbon_deferred = False
        if self.carbon_market is not None:
            try:
                decision = await self.carbon_market.net_zero_schedule(
                    workload_kwh=0.1, intensity=0.2, commit=False)
                carbon_deferred = decision.get("action") == "defer"
            except Exception:
                pass

        # Temporal verification with real state.
        temporal_ok = True
        if self.temporal is not None:
            try:
                await self.temporal.push_state({
                    "active_agents": len(self.agents),
                    "quality": 1.0,
                })
                temporal_ok = all((await self.temporal.verify()).values())
            except Exception:
                pass

        try:
            _M_MA_STEP_SECONDS.observe(time.monotonic() - t0)
        except Exception:
            pass

        return {
            "roles": {a.id: a.role for a in self.agents.values()},
            "role_distribution": self.get_policy(),
            "processed_messages": len(consumed),
            "coalitions": {k: sorted(v) for k, v in self.coalitions.items()},
            "carbon_deferred": carbon_deferred,
            "temporal_ok": temporal_ok,
            "retired_agents": list(self.retired_agents),
        }


# =============================================================================
# ENHANCEMENT 1 — QUANTUM-DISTILLATION ENGINE
# =============================================================================
class QuantumDistillationEngine:
    def __init__(self, temperature: float = 2.0, alpha: float = 0.5,
                 n_actions: int = 5):
        self.temperature = float(temperature)
        self.alpha = float(alpha)
        self.n_actions = int(n_actions)
        self.teachers: Dict[str, List[float]] = {}
        self.student_policy = [1.0 / self.n_actions] * self.n_actions
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

    def register_teacher(self, name: str, policy: List[float]) -> None:
        if not policy:
            return
        s = sum(policy) or 1.0
        self.teachers[str(name)] = [max(0.0, p / s) for p in policy]

    def unregister_teacher(self, name: str) -> None:
        self.teachers.pop(name, None)

    def _softmax(self, x: List[float], temp: float) -> List[float]:
        if not x:
            return []
        m = max(x)
        t = max(temp, 1e-6)
        exps = [math.exp((v - m) / t) for v in x]
        s = sum(exps) or 1.0
        return [e / s for e in exps]

    def _superpose(self) -> List[float]:
        if not self.teachers:
            return list(self.student_policy)
        n = self.n_actions
        accum = [0.0] * n
        for pol in self.teachers.values():
            for i in range(min(n, len(pol))):
                accum[i] += math.sqrt(max(pol[i], 1e-9))
        accum = [a / len(self.teachers) for a in accum]
        sq = [a * a for a in accum]
        s = sum(sq) or 1.0
        return [x / s for x in sq]

    async def step(self, storage: Any,
                   student_id: str = "ma_student") -> Dict[str, Any]:
        target = self._softmax(self._superpose(), self.temperature)
        lr = 0.1
        new = []
        for s, t in zip(self.student_policy, target):
            grad = -(t / max(s, 1e-9))
            new.append(max(0.01, s - lr * grad))
        ns = sum(new) or 1.0
        self.student_policy = [x / ns for x in new]

        # Per-teacher KL against the teacher's own distribution.
        for tid, pol in self.teachers.items():
            weight = float(pol[0]) if pol else 0.0
            amplitude = math.sqrt(max(weight, 1e-9))
            kl = 0.0
            for i in range(self.n_actions):
                pi = pol[i] if i < len(pol) else 1e-9
                si = self.student_policy[i]
                pi, si = max(pi, 1e-9), max(si, 1e-9)
                kl += pi * math.log(pi / si)
            try:
                await asyncio.to_thread(
                    storage.save_teacher_superposition,
                    student_id, tid, weight, self.temperature,
                    amplitude, kl)
            except Exception:
                pass

        entry = {"target": target, "student": list(self.student_policy),
                 "ts": _now_utc().isoformat()}
        self.history.append(entry)
        return entry

    def get_policy(self) -> List[float]:
        return list(self.student_policy)


# =============================================================================
# ENHANCEMENT 2 — CAUSAL RL
# =============================================================================
class CausalGraphLearner:
    def __init__(self, storage: Any = None):
        self.storage = storage
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []
        self._lock = asyncio.Lock()

    async def learn(self, samples: List[Dict[str, float]],
                    variables: List[str],
                    threshold: float = 0.25) -> Dict[str, Any]:
        self.variables = list(variables)
        # Insufficient data → empty graph (no fabrication).
        if len(samples) < 5 or not NUMPY_AVAILABLE:
            async with self._lock:
                self.graph.clear()
            return self.summary()
        try:
            X = np.asarray(
                [[float(s.get(v, 0.0)) for v in variables] for s in samples],
                dtype=float)
        except Exception as exc:
            log_event("warning", f"Causal samples parse failed: {exc}")
            return self.summary()
        if X.shape[0] < 3 or X.shape[1] < 2:
            return self.summary()
        X = (X - X.mean(0)) / (X.std(0) + 1e-9)
        try:
            corr = np.corrcoef(X, rowvar=False)
        except Exception:
            return self.summary()

        async with self._lock:
            self.graph.clear()
            for i in range(len(variables)):
                for j in range(len(variables)):
                    if i == j:
                        continue
                    c = abs(float(corr[i, j]))
                    if not math.isfinite(c) or c <= threshold:
                        continue
                    vi, vj = float(X[:, i].var()), float(X[:, j].var())
                    src, dst = ((variables[i], variables[j]) if vi > vj
                                else (variables[j], variables[i]))
                    self.graph[src][dst] = {
                        "weight": float(corr[i, j]), "confidence": c}
                    if self.storage is not None:
                        try:
                            await asyncio.to_thread(
                                self.storage.save_causal_edge,
                                src, dst, float(corr[i, j]), c)
                        except Exception:
                            pass
        return self.summary()

    def parents(self, node: str) -> List[str]:
        return [s for s, e in self.graph.items() if node in e]

    def children(self, node: str) -> List[str]:
        return list(self.graph.get(node, {}).keys())

    def summary(self) -> Dict[str, Any]:
        return {"nodes": len(self.variables),
                "edges": sum(len(v) for v in self.graph.values()),
                "variables": list(self.variables)}


class CausalPolicyAdapter:
    ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]
    STRATEGY_MAP = {
        "performance": "balanced", "carbon": "carbon_focus",
        "cost": "operational_focus", "hybrid": "balanced",
        "adaptive": "balanced",
    }

    def __init__(self, config: Any, storage: Any,
                 graph: CausalGraphLearner):
        self.config = config
        self.storage = storage
        self.graph = graph
        self.values: Dict[str, float] = defaultdict(float)
        self.counts: Dict[str, int] = defaultdict(int)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self.epsilon = float(_cfg_get(config, "causal_exploration_rate", 0.1)
                             or 0.1)
        self._lock = asyncio.Lock()

    def to_strategy(self, action: str) -> str:
        return self.STRATEGY_MAP.get(action, "balanced")

    async def choose_action(self, state: Dict[str, Any]) -> str:
        async with self._lock:
            if random.random() < self.epsilon:
                return random.choice(self.ACTIONS)
            return max(self.ACTIONS, key=lambda a: self.values.get(a, 0.0))

    async def update(self, action: str, reward: float,
                     state: Dict[str, Any]) -> None:
        async with self._lock:
            if action not in self.ACTIONS:
                action = self.ACTIONS[0]
            self.counts[action] += 1
            n = self.counts[action]
            self.values[action] += (reward - self.values[action]) / n
            vals = [self.values.get(a, 0.0) for a in self.ACTIONS]
            m = max(vals)
            exps = [math.exp((v - m) / 0.5) for v in vals]
            s = sum(exps) or 1.0
            self.policy = [e / s for e in exps]

    async def estimate_ate(self, treatment: str, outcome: str,
                           samples: int = 100) -> float:
        w = float(self.graph.graph.get(treatment, {}).get(outcome, {})
                  .get("weight", 0.0))
        try:
            await asyncio.to_thread(
                self.storage.save_causal_experiment,
                f"exp_{uuid.uuid4().hex[:8]}", treatment, outcome,
                w, samples, "edge_weight_proxy")
        except Exception:
            pass
        return w

    def get_policy(self) -> List[float]:
        return list(self.policy)


# =============================================================================
# ENHANCEMENT 3 — FEDERATED GREEN LEARNING
# =============================================================================
class FederatedGreenAggregator:
    def __init__(self, storage: Any, instance_id: str,
                 share_interval: int = 3600):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = int(share_interval)
        self.rounds = 0
        self.last_aggregated: Optional[bytes] = None

    async def share_weights(self, model_id: str, weights: bytes) -> None:
        if not weights:
            return
        try:
            await asyncio.to_thread(
                self.storage.save_federated_weights,
                self.instance_id, model_id, weights,
                float(len(weights)), self.rounds)
            self.rounds += 1
        except Exception as exc:
            log_event("warning", f"federated share failed: {exc}")

    async def pull_aggregated_weights(self, model_id: str) -> Optional[bytes]:
        try:
            rows = await asyncio.to_thread(
                self.storage.get_federated_weights, model_id)
        except Exception:
            return None
        if not rows:
            return None
        blobs = [r["weights"] for r in rows if r["weights"]]
        if not blobs:
            return None
        n = min(len(b) for b in blobs)
        avg = bytearray(n)
        for i in range(n):
            avg[i] = int(sum(b[i] for b in blobs) / len(blobs)) & 0xFF
        self.rounds += 1
        self.last_aggregated = bytes(avg)
        return self.last_aggregated

    async def apply_aggregated_weights(self, model_id: str,
                                       current: bytes) -> bytes:
        agg = await self.pull_aggregated_weights(model_id)
        if agg is None or not current:
            return current
        n = min(len(current), len(agg))
        return bytes([(current[i] + agg[i]) // 2 for i in range(n)])

    def stats(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "rounds": self.rounds,
            "last_aggregated_bytes": (
                len(self.last_aggregated) if self.last_aggregated else 0),
        }


# =============================================================================
# ENHANCEMENT 5 — TEMPORAL LOGIC
# =============================================================================
_ATOMIC_RE = re.compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")


class TemporalRule:
    def __init__(self, rule_id: str, operator: str,
                 conditions: List[Callable[[Dict], bool]],
                 window: float = 0.0, description: str = "",
                 severity: str = "warning"):
        self.rule_id = rule_id
        self.operator = operator
        self.conditions = conditions
        self.window = float(window)
        self.description = description or rule_id
        self.severity = severity
        self.violations = 0
        self.last_violation: Optional[datetime] = None

    def evaluate(self, trace: Deque[Tuple[datetime, Dict]]) -> bool:
        if not trace:
            return False
        if self.window > 0:
            cutoff = trace[-1][0] - timedelta(seconds=self.window)
            while trace and trace[0][0] < cutoff:
                trace.popleft()
        op = self.operator
        if op == "always":
            return any(not self.conditions[0](s) for _, s in trace)
        if op == "eventually":
            return not any(self.conditions[0](s) for _, s in trace)
        if op == "never":
            return any(self.conditions[0](s) for _, s in trace)
        return False


class TemporalLogicVerifier:
    def __init__(self, storage: Any, config: Any):
        self.storage = storage
        self.config = config
        self.rules: Dict[str, TemporalRule] = {}
        self.trace: Deque[Tuple[datetime, Dict]] = deque(
            maxlen=int(_cfg_get(config, "temporal_max_trace", 2000) or 2000))
        self.approval_cb: Optional[Callable] = None
        self._pending_rule_saves: List[Tuple[str, str, str, str]] = []
        for formula in _cfg_get(config, "temporal_formulas", []) or []:
            self._install(formula)

    def _install(self, formula: str) -> None:
        f = (formula or "").strip()
        if not f:
            return
        try:
            if f.startswith("G "):
                self._add_atomic(f[2:].strip().strip("()"), "always")
            elif f.startswith("F "):
                self._add_atomic(f[2:].strip().strip("()"), "eventually")
            elif f.startswith("NEVER "):
                self._add_atomic(f[6:].strip().strip("()"), "never")
            else:
                log_event("warning",
                          f"Unrecognised temporal formula: {formula!r}")
        except Exception as exc:
            log_event("warning", f"Temporal install failed: {exc}")

    def _add_atomic(self, expr: str, op: str) -> None:
        m = _ATOMIC_RE.match(expr)
        if not m:
            log_event("warning", f"Atomic formula did not parse: {expr!r}")
            return
        var, cmp, val = m.group(1), m.group(2), float(m.group(3))

        def cond(state, v=var, c=cmp, x=val):
            try:
                sv = float(state.get(v, 0.0))
            except Exception:
                return True
            return {">=": sv >= x, "<=": sv <= x, "==": sv == x,
                    "!=": sv != x, ">": sv > x, "<": sv < x}[c]

        rid = f"{op}:{expr}"
        self.rules[rid] = TemporalRule(rid, op, [cond],
                                        description=expr, severity="warning")
        self._pending_rule_saves.append((rid, expr, op, "warning"))

    async def _flush_pending_rule_saves(self) -> None:
        while self._pending_rule_saves:
            rid, expr, op, sev = self._pending_rule_saves.pop(0)
            try:
                await asyncio.to_thread(
                    self.storage.save_temporal_rule,
                    rid, expr, op, sev, expr, 0.0, True)
            except Exception:
                pass

    def set_approval_callback(self, cb: Callable) -> None:
        self.approval_cb = cb

    async def push_state(self, state: Dict[str, Any]) -> None:
        await self._flush_pending_rule_saves()
        self.trace.append((_now_utc(), dict(state)))
        try:
            await asyncio.to_thread(self.storage.save_temporal_trace, state)
        except Exception:
            pass

    async def verify(self) -> Dict[str, bool]:
        await self._flush_pending_rule_saves()
        result: Dict[str, bool] = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace)
            violated = rule.evaluate(copy)
            approved = False
            if violated:
                rule.violations += 1
                rule.last_violation = _now_utc()
                if rule.severity == "critical" and self.approval_cb:
                    try:
                        approved = self.approval_cb(
                            rid, self.trace[-1][1] if self.trace else {})
                        if asyncio.iscoroutine(approved):
                            approved = await approved
                    except Exception as exc:
                        log_event("warning", f"Approval callback failed: {exc}")
                        approved = False
                try:
                    await asyncio.to_thread(
                        self.storage.save_temporal_violation,
                        rid, rule.description, len(self.trace) - 1,
                        self.trace[-1][1] if self.trace else {},
                        rule.severity, bool(approved))
                except Exception:
                    pass
            result[rid] = (not violated) or bool(approved)
        return result


# =============================================================================
# ENHANCEMENT 6 — EXPLAINABLE AI
# =============================================================================
class XAIDecisionExplainer:
    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.method = str(_cfg_get(config, "xai_method", "kernel_shap")
                          or "kernel_shap")
        self.depth = int(_cfg_get(config, "xai_depth", 5) or 5)

    def _permutation_shap(self, f, x, names, n=64):
        if not NUMPY_AVAILABLE:
            return {k: 0.0 for k in names}
        base = np.zeros_like(x)
        contrib = np.zeros(len(x))
        for _ in range(n):
            perm = list(range(len(x)))
            random.shuffle(perm)
            prev = base.copy()
            for i in perm:
                cur = prev.copy()
                cur[i] = x[i]
                try:
                    delta = float(f(cur.reshape(1, -1))) - \
                            float(f(prev.reshape(1, -1)))
                except Exception:
                    delta = 0.0
                contrib[i] += delta
                prev = cur
        contrib = contrib / max(1, n)
        return dict(zip(names, contrib.tolist()))

    def _kernel_shap(self, f, x, names, n=64):
        return self._permutation_shap(f, x, names, n)

    def _nl(self, decision, attrs):
        top = sorted(attrs.items(), key=lambda kv: abs(kv[1]),
                     reverse=True)[:self.depth]
        lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    async def explain(self, decision_id: str, label: str, features,
                      names: List[str], model_fn: Callable) -> Dict[str, Any]:
        if not NUMPY_AVAILABLE:
            attrs = {k: 0.0 for k in names}
        else:
            attrs = self._permutation_shap(model_fn, features, names)
        nl = self._nl(label, attrs)

        feature_map: Dict[str, Any] = {}
        try:
            if NUMPY_AVAILABLE and hasattr(features, "tolist"):
                feature_map = dict(zip(names, features.tolist()))
            else:
                feature_map = dict(zip(names, list(features)))
        except Exception:
            feature_map = {"raw": list(features)}

        try:
            await asyncio.to_thread(
                self.storage.save_xai_explanation,
                decision_id, decision_id, self.method, label,
                feature_map, attrs, nl)
        except Exception:
            pass
        return {"decision_id": decision_id, "method": self.method,
                "attributions": attrs, "explanation": nl}


# =============================================================================
# ENHANCEMENT 7 — ADAPTIVE PRECISION SWITCHER
# =============================================================================
class AdaptivePrecisionSwitcher:
    ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55,
              "fp16": 0.5, "int8": 0.3}
    LATENCY = {"fp32": 1.0, "tf32": 0.8, "bf16": 0.65,
               "fp16": 0.6, "int8": 0.4}

    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.current = "fp32"
        self.saved_wh = 0.0
        self.history: Deque[Dict] = deque(maxlen=500)
        self._acc_window: Deque[float] = deque(maxlen=20)
        self._hw_cache: Optional[Dict[str, Any]] = None

    def _probe(self, force_refresh: bool = False) -> Dict[str, Any]:
        if self._hw_cache is not None and not force_refresh:
            return self._hw_cache
        info = {"cuda": False, "bf16": False, "device": "cpu"}
        if TORCH_AVAILABLE and torch is not None:
            try:
                info["cuda"] = bool(torch.cuda.is_available())
                if info["cuda"]:
                    info["device"] = torch.cuda.get_device_name(0)
                    try:
                        info["bf16"] = bool(torch.cuda.is_bf16_supported())
                    except Exception:
                        info["bf16"] = False
            except Exception:
                pass
        self._hw_cache = info
        return info

    def _allowed_precisions(self) -> List[str]:
        raw = list(_cfg_get(self.config, "precision_levels",
                            ["fp32", "fp16", "bf16", "int8"]))
        return [c for c in raw if c in self.ENERGY]

    def select_precision(self) -> str:
        hw = self._probe()
        cands = self._allowed_precisions()
        if not cands:
            return "fp32"
        if not hw["cuda"]:
            cands = [c for c in cands if c in ("fp32", "int8")]
        if not hw["bf16"]:
            cands = [c for c in cands if c != "bf16"]
        if not cands:
            return "fp32"
        return min(cands, key=lambda c: self.ENERGY.get(c, 1.0))

    def record_accuracy(self, accuracy: float) -> None:
        try:
            self._acc_window.append(float(accuracy))
        except (TypeError, ValueError):
            pass

    async def switch_to(self, target: str, reason: str = "policy") -> bool:
        if target == self.current or target not in self.ENERGY:
            return False
        if target not in self._allowed_precisions():
            log_event("warning",
                      f"Precision {target!r} not in precision_levels")
            return False
        old = self.current
        self.current = target
        saved = max(0.0, self.ENERGY.get(old, 1.0) -
                    self.ENERGY.get(target, 1.0))
        self.saved_wh += saved
        self.history.append({"from": old, "to": target,
                             "reason": reason, "saved_wh": saved})
        try:
            await asyncio.to_thread(
                self.storage.save_precision_switch,
                old, target, reason, saved, 0.0)
            _M_PRECISION_SWITCHES.labels(from_p=old, to_p=target).inc()
        except Exception:
            pass
        return True

    async def auto_switch(self, recent_acc: float = 0.0,
                          baseline_acc: float = 0.0) -> None:
        if recent_acc <= 0 or baseline_acc <= 0:
            if not self._acc_window:
                return
            recent_acc = sum(self._acc_window) / len(self._acc_window)
            baseline_acc = max(self._acc_window)
        if baseline_acc <= 0:
            return
        drop = (baseline_acc - recent_acc) / baseline_acc
        thresh = float(_cfg_get(self.config,
                                "precision_switch_threshold", 0.02) or 0.02)
        if drop > thresh:
            await self.switch_to("fp32", reason=f"acc drop {drop:.3f}")
        elif drop < thresh / 2:
            target = self.select_precision()
            if target != self.current:
                await self.switch_to(target, reason="headroom")


# =============================================================================
# ENHANCEMENT 8 — CARBON MARKETS / REC
# =============================================================================
class CarbonMarketIntegrator:
    DEFER_INTENSITY_THRESHOLD = 0.3
    RUN_OFFSET_COST_THRESHOLD = 0.5
    MIN_PRICE_USD = 5.0
    MAX_PRICE_USD = 200.0
    DEFAULT_PRICE_USD = 25.0

    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.last_price = float(
            _cfg_get(config, "carbon_default_price_usd",
                     self.DEFAULT_PRICE_USD) or self.DEFAULT_PRICE_USD)
        self._rec_balance_mwh = 0.0

    async def _fetch_price(self) -> float:
        candidate = self.last_price + random.gauss(0, 1.5)
        return max(self.MIN_PRICE_USD, min(self.MAX_PRICE_USD, candidate))

    async def update_price(self) -> float:
        try:
            price = await self._fetch_price()
        except Exception:
            price = self.last_price
        try:
            price = max(self.MIN_PRICE_USD,
                        min(self.MAX_PRICE_USD, float(price)))
        except (TypeError, ValueError):
            price = self.last_price
        try:
            await asyncio.to_thread(self.storage.save_credit_price, price)
        except Exception:
            pass
        self.last_price = price
        return price

    async def purchase_rec(self, mwh: float, price_per_mwh: float = 5.0,
                           source: str = "wind") -> float:
        mwh = max(0.0, float(mwh))
        if mwh == 0.0:
            return 0.0
        price_per_mwh = max(0.0, float(price_per_mwh))
        src = (source or "").strip().lower() or "unspecified"
        cost = mwh * price_per_mwh
        self._rec_balance_mwh += mwh
        try:
            await asyncio.to_thread(
                self.storage.save_rec, mwh, price_per_mwh, src)
        except Exception:
            pass
        return cost

    async def net_zero_schedule(self, workload_kwh: float, intensity: float,
                                price: Optional[float] = None,
                                commit: bool = True) -> Dict[str, Any]:
        workload_kwh = max(0.0, float(workload_kwh))
        intensity = max(0.0, float(intensity))
        if price is None:
            price = float(self.last_price)
        else:
            try:
                price = max(0.0, float(price))
            except (TypeError, ValueError):
                price = float(self.last_price)
        carbon_kg = workload_kwh * intensity
        offset_cost = (carbon_kg / 1000.0) * price
        defer_hours = 0
        if intensity > self.DEFER_INTENSITY_THRESHOLD:
            action = "defer"
            defer_hours = max(
                1, int((intensity - self.DEFER_INTENSITY_THRESHOLD) * 10))
        elif offset_cost < self.RUN_OFFSET_COST_THRESHOLD:
            action = "run_offset"
        else:
            action = "run"
        if commit:
            try:
                await asyncio.to_thread(
                    self.storage.save_net_zero_match,
                    uuid.uuid4().hex[:8], workload_kwh, intensity,
                    action, carbon_kg, offset_cost, price)
            except Exception:
                pass
        return {"action": action, "defer_hours": defer_hours,
                "carbon_kg": carbon_kg, "offset_cost_usd": offset_cost,
                "credit_price_usd": price,
                "rec_balance_mwh": float(self._rec_balance_mwh)}

    def get_rec_balance(self) -> float:
        return float(self._rec_balance_mwh)


# =============================================================================
# ENHANCEMENT 9 — CHAOS TESTING
# =============================================================================
class ChaosTestingEngine:
    FAULT_TYPES = ["latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop"]

    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        self._shared_state: Dict[str, Any] = {"payload": "ok", "counter": 0}

    async def _health_ok(self) -> bool:
        async with self._lock:
            return not self.active

    async def run_experiment(self, name: str,
                             fault_type: str) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        t0 = time.monotonic()
        before = await self._health_ok()
        status = "completed"
        injected_ms = 0.0
        try:
            async with self._lock:
                self.active[name] = {"fault_type": fault_type}
            if fault_type == "latency":
                injected_ms = 500.0
                await asyncio.sleep(0.5)
            elif fault_type == "exception":
                raise RuntimeError("chaos: injected exception")
            elif fault_type == "data_corruption":
                async with self._lock:
                    self._shared_state["payload"] = "corrupted"
                    self._shared_state["counter"] += 1
                injected_ms = 5.0
            elif fault_type == "memory_pressure":
                _ = bytearray(5 * 1024 * 1024)
                injected_ms = 10.0
            elif fault_type == "network_drop":
                injected_ms = 200.0
                await asyncio.sleep(0.2)
        except Exception:
            status = "injected"
        finally:
            async with self._lock:
                self.active.pop(name, None)
        after = await self._health_ok()
        duration = (time.monotonic() - t0) * 1000.0
        try:
            await asyncio.to_thread(
                self.storage.save_chaos_experiment,
                name, name, fault_type,
                float(_cfg_get(self.config, "chaos_blast_radius", 0.1) or 0.1),
                int(before), int(after), status, duration)
            _M_CHAOS.labels(fault_type=fault_type, status=status).inc()
        except Exception:
            pass
        return {"name": name, "fault_type": fault_type,
                "steady_before": before, "steady_after": after,
                "status": status, "duration_ms": duration,
                "injected_ms": injected_ms}


# =============================================================================
# ENHANCEMENT 10 — HITL ACTIVE LEARNING
# =============================================================================
class ActiveUserPreferenceLearner:
    _PREF_KEYS = ("quality_score", "carbon_g", "cost_usd", "latency_ms")

    def __init__(self, storage: Any, dashboard: Any = None):
        self.storage = storage
        self.dashboard = dashboard
        self.preferences: Dict[str, Dict[str, float]] = {}
        self._responses: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._pending: Dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()

    async def submit_response(self, user_id: str, chosen_id: str,
                              request_id: Optional[str] = None) -> None:
        if request_id is not None:
            async with self._lock:
                fut = self._pending.pop(request_id, None)
            if fut is not None and not fut.done():
                fut.set_result({"user_id": user_id, "chosen": chosen_id})
                return
            log_event("warning",
                      f"Unknown HITL request_id {request_id!r}")
        try:
            self._responses.put_nowait(
                {"user_id": user_id, "chosen": chosen_id})
        except asyncio.QueueFull:
            log_event("warning", "HITL response queue full; dropping")

    async def query_user_if_needed(self, user_id: str,
                                   candidates: List[Dict[str, Any]],
                                   timeout: float = 3.0) -> Optional[str]:
        if len(candidates) < 2:
            return None
        req_id = uuid.uuid4().hex[:8]
        try:
            await asyncio.to_thread(
                self.storage.enqueue_hitl_request,
                req_id, "query", {"candidates": candidates[:2]}, "info")
        except Exception:
            pass
        if self.dashboard is not None:
            try:
                await self.dashboard.broadcast({
                    "type": "preference_query",
                    "request_id": req_id, "user_id": user_id})
            except Exception:
                pass
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        async with self._lock:
            self._pending[req_id] = fut
        try:
            msg = await asyncio.wait_for(fut, timeout=timeout)
            try:
                await asyncio.to_thread(
                    self.storage.resolve_hitl_request, req_id, "approved")
            except Exception:
                pass
            return msg.get("chosen") if isinstance(msg, dict) else None
        except asyncio.TimeoutError:
            try:
                await asyncio.to_thread(
                    self.storage.resolve_hitl_request, req_id, "timeout")
            except Exception:
                pass
            return candidates[0].get("solution_id")
        finally:
            async with self._lock:
                self._pending.pop(req_id, None)

    async def record_choice(self, user_id: str, solution_id: str,
                            metrics: Optional[Dict[str, float]] = None
                            ) -> None:
        prefs = self.preferences.setdefault(user_id, {})
        if metrics:
            for k in self._PREF_KEYS:
                if k in metrics:
                    try:
                        v = float(metrics[k])
                    except (TypeError, ValueError):
                        continue
                    if k == "quality_score":
                        prefs[k] = prefs.get(k, 0.25) + v * 0.01
                    else:
                        prefs[k] = prefs.get(k, 0.25) + \
                            1.0 / (abs(v) + 1e-6) * 0.01
            self.preferences[user_id] = prefs
        try:
            await asyncio.to_thread(
                self.storage.save_user_preference, user_id, prefs)
        except Exception:
            pass

    def get_preference_weights(self, user_id: str) -> Dict[str, float]:
        raw = self.preferences.get(user_id, {})
        if not raw:
            return {}
        s = sum(raw.values()) or 1.0
        return {k: v / s for k, v in raw.items()}


# =============================================================================
# UNIFIED ORCHESTRATOR
# =============================================================================
class MultiAgentOrchestratorV17:
    """Unified entry point wiring all ten enhancements around the coordinator."""

    def __init__(self, storage: Any, config: Any, dashboard: Any = None,
                 *, xai_sample_rate: float = 0.1,
                 chaos_sample_rate: float = 0.05,
                 carbon_off_hot_path: bool = True):
        self.storage = storage
        self.config = config
        self.instance_id = str(uuid.uuid4())[:8]

        self.quantum = QuantumDistillationEngine(temperature=2.0, alpha=0.5)
        self.causal_graph = CausalGraphLearner(storage)
        self.causal_rl = CausalPolicyAdapter(config, storage, self.causal_graph)
        self.federated = FederatedGreenAggregator(storage, self.instance_id)
        self.multi_agent = MultiAgentCoordinator(config, storage)
        self.temporal = TemporalLogicVerifier(storage, config)
        self.xai = XAIDecisionExplainer(config, storage)
        self.precision = AdaptivePrecisionSwitcher(config, storage)
        self.carbon_market = CarbonMarketIntegrator(config, storage)
        self.chaos = ChaosTestingEngine(config, storage)
        self.hitl = ActiveUserPreferenceLearner(storage, dashboard=dashboard)

        # Wire hooks.
        self.multi_agent.causal_rl = self.causal_rl
        self.multi_agent.temporal = self.temporal
        self.multi_agent.xai = self.xai
        self.multi_agent.carbon_market = self.carbon_market
        self.multi_agent.chaos = self.chaos
        self.multi_agent.hitl = self.hitl
        self.multi_agent.precision = self.precision
        self.multi_agent.federated = self.federated

        self.temporal.set_approval_callback(self._on_critical_violation)

        self.xai_sample_rate = float(xai_sample_rate)
        self.chaos_sample_rate = float(chaos_sample_rate)
        self.carbon_off_hot_path = bool(carbon_off_hot_path)

        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: set = set()
        self._side_effects: set = set()

    async def _on_critical_violation(self, rule_id: str,
                                     state: Dict[str, Any]) -> bool:
        """Ask HITL whether a critical violation should be approved."""
        try:
            q = float(state.get("quality", 0.8))
            c = float(state.get("carbon", 0.4))
        except (TypeError, ValueError):
            q, c = 0.8, 0.4
        candidates = [
            {"solution_id": "approve", "quality_score": q, "carbon_g": c,
             "cost_usd": 0.5, "latency_ms": 100.0},
            {"solution_id": "reject", "quality_score": 0.0, "carbon_g": 0.0,
             "cost_usd": 0.0, "latency_ms": 0.0},
        ]
        try:
            decision = await self.hitl.query_user_if_needed(
                "critical_user", candidates, timeout=2.0)
        except Exception as exc:
            log_event("warning", f"HITL critical query failed: {exc}")
            decision = None
        return decision == "approve"

    # ------------------------------------------------------------------
    def _spawn_side_effect(self, coro: Awaitable[Any], name: str) -> None:
        try:
            task = asyncio.create_task(coro, name=f"ma_side_{name}")
        except RuntimeError:
            return
        self._side_effects.add(task)

        def _done(t: asyncio.Task) -> None:
            self._side_effects.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                log_event("debug", f"side effect {name} failed: {exc}")

        task.add_done_callback(_done)

    async def coordinate_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Coordinate a task. Only the essential hot-path operations run
        synchronously: causal selection, bid, temporal gate. XAI, precision
        switching, carbon market, chaos, and multi-agent reward are
        scheduled off the critical path.
        """
        result: Dict[str, Any] = {}

        # 1. Causal RL strategy selection.
        try:
            strategy = await self.causal_rl.choose_action(task)
            result["strategy"] = strategy
            result["strategy_name"] = self.causal_rl.to_strategy(strategy)
        except Exception:
            result["strategy"] = "adaptive"
            result["strategy_name"] = "balanced"

        # 2. Bid.
        try:
            agent_id, score = await self.multi_agent.bid(task)
            result["agent_id"] = agent_id
            result["bid_score"] = score
        except Exception as exc:
            log_event("warning", f"bid failed: {exc}")
            result["agent_id"] = None

        # 3. Coalition for complex tasks.
        if task.get("complex", False):
            try:
                coalition = await self.multi_agent.form_coalition(
                    task, size=3)
                result["coalition"] = coalition
            except Exception:
                pass

        # 4. Decompose if requested.
        if task.get("decompose", False):
            try:
                result["assignments"] = \
                    await self.multi_agent.decompose_task(task)
            except Exception:
                pass

        # 5. Carbon market decision (non-committing; unit normalised).
        raw_intensity = float(task.get("carbon_intensity", 0.4) or 0.4)
        intensity = raw_intensity / 1000.0 if raw_intensity > 1.0 \
            else raw_intensity
        try:
            cm = await self.carbon_market.net_zero_schedule(
                workload_kwh=1.0, intensity=intensity, commit=False)
            result["carbon_decision"] = cm
        except Exception:
            result["carbon_decision"] = {"action": "run", "defer_hours": 0}

        # 6. Temporal gate.
        try:
            await self.temporal.push_state({
                "quality": float(task.get("quality", 0.8)),
                "active_agents": len(self.multi_agent.agents)})
            verify = await self.temporal.verify()
            result["temporal_violations"] = [
                k for k, ok in verify.items() if not ok]
        except Exception:
            result["temporal_violations"] = []

        # 7. Fire-and-forget side effects.
        if random.random() < self.xai_sample_rate and NUMPY_AVAILABLE:
            self._spawn_side_effect(
                self._xai_explain(result["strategy"], task, intensity), "xai")

        # Multi-agent reward uses an observable signal (bid score).
        bid_score = float(result.get("bid_score", 0.5) or 0.5)
        reward = max(0.0, min(1.0, bid_score))
        agent_id = result.get("agent_id")
        if agent_id:
            self._spawn_side_effect(
                self.multi_agent.reward(agent_id, reward), "reward")

        # Causal RL update using the same observable reward.
        self._spawn_side_effect(
            self.causal_rl.update(result["strategy"], reward, task),
            "causal_update")

        # Precision switch uses the rolling window.
        if self.precision is not None:
            self._spawn_side_effect(
                self.precision.auto_switch(0.0, 0.0), "precision")

        return result

    async def _xai_explain(self, strategy: str, task: Dict[str, Any],
                           intensity: float) -> None:
        if not NUMPY_AVAILABLE:
            return
        try:
            feats = np.array([
                float(task.get("quality", 0.8)),
                float(intensity),
                float(task.get("cost", 0.5)),
                float(task.get("latency_ms", 100) or 100) / 1000.0,
            ])

            def _score(x, _w=np.array([0.4, -0.3, -0.2, -0.1])):
                return float(np.dot(x, _w))

            await self.xai.explain(
                decision_id=f"ma_{uuid.uuid4().hex[:8]}",
                label=f"strategy={strategy}", features=feats,
                names=["quality", "carbon", "cost", "latency"],
                model_fn=_score)
        except Exception as exc:
            log_event("debug", f"xai side effect failed: {exc}")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def _spawn(self, coro: Awaitable[Any], name: str) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        self._background_tasks.add(task)

        def _done(t: asyncio.Task) -> None:
            self._background_tasks.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                log_event("error",
                          f"Background task {name} failed: {exc}")

        task.add_done_callback(_done)
        return task

    async def start(self) -> None:
        self._running = True
        _ = asyncio.get_running_loop()
        self._spawn(self._causal_rl_loop(), "causal_rl")
        self._spawn(self._federated_loop(), "federated")
        self._spawn(self._multi_agent_loop(), "multi_agent")
        self._spawn(self._temporal_loop(), "temporal")
        self._spawn(self._xai_loop(), "xai")
        self._spawn(self._precision_loop(), "precision")
        self._spawn(self._carbon_market_loop(), "carbon_market")
        self._spawn(self._chaos_loop(), "chaos")
        self._spawn(self._distillation_loop(), "distillation")

    async def shutdown(self) -> None:
        self._shutdown_event.set()
        self._running = False
        for t in list(self._background_tasks):
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks,
                                 return_exceptions=True)
        self._background_tasks.clear()
        for t in list(self._side_effects):
            t.cancel()
        if self._side_effects:
            await asyncio.gather(*self._side_effects,
                                 return_exceptions=True)
        self._side_effects.clear()

    async def aclose(self) -> None:
        await self.shutdown()

    async def __aenter__(self) -> "MultiAgentOrchestratorV17":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    # ---- background loops ----
    async def _causal_rl_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "causal_graph_update_interval", 900) or 900))
            try:
                samples = [{
                    "quality": random.uniform(0.5, 1.0),
                    "carbon": random.uniform(0.1, 0.8),
                    "cost": random.uniform(0.1, 0.9),
                    "latency": random.uniform(0.1, 0.9),
                } for _ in range(20)]
                await self.causal_graph.learn(
                    samples, ["quality", "carbon", "cost", "latency"])
            except Exception as exc:
                log_event("debug", f"causal_rl loop error: {exc}")

    async def _federated_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "federated_interval", 3600) or 3600))
            try:
                dummy = bytes(random.getrandbits(8) for _ in range(64))
                await self.federated.share_weights("agent_policy", dummy)
                await self.federated.pull_aggregated_weights("agent_policy")
            except Exception as exc:
                log_event("debug", f"federated loop error: {exc}")

    async def _multi_agent_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "agent_negotiation_interval", 600) or 600))
            try:
                await self.multi_agent.step()
            except Exception as exc:
                log_event("debug", f"multi_agent loop error: {exc}")

    async def _temporal_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "temporal_verification_interval", 300) or 300))
            try:
                await self.temporal.verify()
            except Exception as exc:
                log_event("debug", f"temporal loop error: {exc}")

    async def _xai_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "xai_interval", 300) or 300))
            if not NUMPY_AVAILABLE:
                continue
            try:
                feats = np.array([0.9, 0.4, 0.5, 0.4])

                def _score(x, _w=np.array([0.4, -0.3, -0.2, -0.1])):
                    return float(np.dot(x, _w))

                await self.xai.explain(
                    decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                    label="ma_health", features=feats,
                    names=["quality", "carbon", "cost", "latency"],
                    model_fn=_score)
            except Exception as exc:
                log_event("debug", f"xai loop error: {exc}")

    async def _precision_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "precision_check_interval", 300) or 300))
            try:
                await self.precision.auto_switch(0.0, 0.0)
            except Exception as exc:
                log_event("debug", f"precision loop error: {exc}")

    async def _carbon_market_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "carbon_market_interval", 3600) or 3600))
            try:
                await self.carbon_market.update_price()
            except Exception as exc:
                log_event("debug", f"carbon_market loop error: {exc}")

    async def _chaos_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "chaos_test_interval", 1800) or 1800))
            try:
                fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
                await self.chaos.run_experiment(
                    f"auto_{uuid.uuid4().hex[:6]}", fault)
            except Exception as exc:
                log_event("debug", f"chaos loop error: {exc}")

    async def _distillation_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "distillation_interval", 300) or 300))
            try:
                self.quantum.register_teacher(
                    "causal", self.causal_rl.get_policy())
                self.quantum.register_teacher(
                    "agents", self.multi_agent.get_policy())
                await self.quantum.step(self.storage, "ma_student")
            except Exception as exc:
                log_event("debug", f"distillation loop error: {exc}")

    # ------------------------------------------------------------------
    async def health_check(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "agents": {a.id: a.role
                       for a in self.multi_agent.agents.values()},
            "role_distribution": self.multi_agent.get_policy(),
            "coalitions": {k: sorted(v)
                           for k, v in self.multi_agent.coalitions.items()},
            "retired_agents": list(self.multi_agent.retired_agents),
            "precision": self.precision.current,
            "carbon_price": self.carbon_market.last_price,
            "side_effects": len(self._side_effects),
            "background_tasks": len(self._background_tasks),
        }


# =============================================================================
# MINIMAL IN-MEMORY STORAGE
# =============================================================================
class InMemoryStorage:
    """Standalone storage for demos/tests. Soft-fail for all methods."""

    def __init__(self):
        self._data: Dict[str, Any] = defaultdict(list)
        self._prefs: Dict[str, Dict[str, float]] = {}

    def save_agent(self, agent_id, role, reputation, utilities):
        self._data["agent_registry"].append({
            "agent_id": agent_id, "role": role,
            "reputation": reputation, "utilities": utilities})

    def save_agent_message(self, message_id, topic, sender, recipient, payload):
        self._data["agent_messages"].append({
            "message_id": message_id, "topic": topic, "sender": sender,
            "recipient": recipient, "payload": payload})

    def save_causal_edge(self, source, target, weight, confidence):
        self._data["causal_graph"].append({
            "source": source, "target": target,
            "weight": weight, "confidence": confidence})

    def save_causal_experiment(self, exp_id, treatment, outcome, ate,
                               samples, method=""):
        self._data["causal_experiments"].append({
            "exp_id": exp_id, "treatment": treatment, "outcome": outcome,
            "ate": ate, "samples": samples, "method": method})

    def save_federated_weights(self, instance_id, model_id, weights,
                               weight_norm=0.0, round_id=0):
        self._data["federated_weights"].append({
            "instance_id": instance_id, "model_id": model_id,
            "weights": weights, "weight_norm": weight_norm,
            "round_id": round_id})

    def get_federated_weights(self, model_id):
        return [r for r in self._data["federated_weights"]
                if r["model_id"] == model_id]

    def save_precision_switch(self, from_p, to_p, reason,
                              saved_wh=0.0, acc_delta=0.0):
        self._data["precision_history"].append({
            "from_p": from_p, "to_p": to_p, "reason": reason,
            "saved_wh": saved_wh, "acc_delta": acc_delta})

    def save_credit_price(self, price_usd, currency="USD",
                          source="oracle", region="global"):
        self._data["carbon_credit_prices"].append({
            "price_usd": price_usd, "currency": currency,
            "source": source, "region": region})

    def save_rec(self, mwh, price_per_mwh, source,
                 certificate_id="", region="global", retired=False):
        self._data["rec_ledger"].append({
            "mwh": mwh, "price_per_mwh": price_per_mwh, "source": source,
            "certificate_id": certificate_id, "region": region,
            "retired": retired})

    def get_rec_balance(self):
        return sum(r["mwh"] for r in self._data["rec_ledger"])

    def save_net_zero_match(self, match_id, workload_kwh, intensity, action,
                            carbon_kg, offset_cost, credit_price):
        self._data["net_zero_matches"].append({
            "match_id": match_id, "workload_kwh": workload_kwh,
            "intensity": intensity, "action": action,
            "carbon_kg": carbon_kg, "offset_cost": offset_cost,
            "credit_price": credit_price})

    def save_chaos_experiment(self, experiment_id, name, fault_type,
                              blast_radius, steady_before, steady_after,
                              status, duration_ms=0.0):
        self._data["chaos_experiments"].append({
            "experiment_id": experiment_id, "name": name,
            "fault_type": fault_type, "blast_radius": blast_radius,
            "steady_before": steady_before, "steady_after": steady_after,
            "status": status, "duration_ms": duration_ms})

    def save_xai_explanation(self, explanation_id, decision_id, method,
                             label, features, attributions, nl):
        self._data["xai_explanations"].append({
            "explanation_id": explanation_id, "decision_id": decision_id,
            "method": method, "label": label, "features": features,
            "attributions": attributions, "natural_language": nl})

    def save_temporal_rule(self, rule_id, formula, operator, severity,
                           description, window, active):
        self._data["temporal_rules"].append({
            "rule_id": rule_id, "formula": formula, "operator": operator,
            "severity": severity, "description": description,
            "window": window, "active": active})

    def save_temporal_trace(self, state, context=None):
        self._data["temporal_trace"].append({
            "state": state, "context": context})

    def save_temporal_violation(self, rule_id, formula, step, state,
                                severity="warning", approved=None):
        self._data["temporal_violations"].append({
            "rule_id": rule_id, "formula": formula, "step": step,
            "state": state, "severity": severity, "approved": approved})

    def save_teacher_superposition(self, student_id, teacher_id, weight,
                                   temperature, amplitude, kl):
        self._data["teacher_superpositions"].append({
            "student_id": student_id, "teacher_id": teacher_id,
            "weight": weight, "temperature": temperature,
            "amplitude": amplitude, "kl": kl})

    def enqueue_hitl_request(self, request_id, rule_id, state, severity):
        self._data["hitl_approval_queue"].append({
            "request_id": request_id, "rule_id": rule_id,
            "state": state, "severity": severity, "status": "pending"})

    def resolve_hitl_request(self, request_id, status="approved"):
        for r in self._data["hitl_approval_queue"]:
            if r["request_id"] == request_id:
                r["status"] = status

    def save_user_preference(self, user_id, weights):
        self._prefs[user_id] = dict(weights)

    def get_user_preference(self, user_id):
        return self._prefs.get(user_id)


# =============================================================================
# DEMO
# =============================================================================
async def _demo():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    config = {
        "causal_exploration_rate": 0.1,
        "agent_count": 6,
        "temporal_max_trace": 2000,
        "temporal_formulas": ["G (quality >= 0.5)"],
        "xai_method": "kernel_shap",
        "xai_depth": 5,
        "precision_levels": ["fp32", "fp16", "bf16", "int8"],
        "precision_switch_threshold": 0.02,
        "chaos_intensity": 0.05,
        "chaos_blast_radius": 0.1,
        "reputation_decay": 0.999,
        "reputation_decay_interval_s": 0.0,   # disable debounce in demo
        "byzantine_threshold": 0.35,
    }

    storage = InMemoryStorage()
    orch = MultiAgentOrchestratorV17(storage, config)
    await orch.start()

    print("=" * 80)
    print("multi_agent_coordinator.py v17.1.0 — Demo")
    print("=" * 80)

    # 1. Role specialisation.
    print("\n=== Emergent role specialisation ===")
    await orch.multi_agent.step()
    for aid, a in orch.multi_agent.agents.items():
        print(f"  {aid}: role={a.role} reputation={a.reputation:.3f}")

    # 2. Bidirectional messaging — verify per-recipient routing.
    print("\n=== Bidirectional messaging (per-recipient) ===")
    await orch.multi_agent.send(
        topic="query", sender="agent_00",
        payload={"question": "status"},
        recipient="agent_01", expects_reply=True)
    # agent_02 should NOT see agent_01's message.
    _ = await orch.multi_agent.consume_messages(max_count=10,
                                                agent_id="agent_02")
    agent_01_inbox = await orch.multi_agent.consume_messages(
        max_count=10, agent_id="agent_01")
    print(f"  agent_01 received: {len(agent_01_inbox)} (expected 1)")

    # 3. Bidding.
    print("\n=== Bidding ===")
    winner, score = await orch.multi_agent.bid({
        "name": "distill_mopd", "preferred_role": "optimizer"})
    print(f"  Winner: {winner} score={score:.3f}")

    # 4. Coalition.
    print("\n=== Coalition formation ===")
    coalition_id = await orch.multi_agent.form_coalition(
        {"name": "safety_audit", "preferred_role": "validator"}, size=3)
    members = orch.multi_agent.coalitions[coalition_id]
    print(f"  Coalition: {coalition_id}")
    print(f"  Members: {sorted(members)}")
    vote = await orch.multi_agent.coalition_vote(
        coalition_id, {"threshold": 0.4})
    print(f"  Vote approved: {vote['approved']}")

    # 5. Task decomposition.
    print("\n=== Task decomposition ===")
    assignments = await orch.multi_agent.decompose_task({
        "name": "training_pipeline",
        "subtasks": [
            {"name": "plan", "preferred_role": "orchestrator"},
            {"name": "execute", "preferred_role": "optimizer"},
            {"name": "verify", "preferred_role": "validator"},
        ]})
    for k, v in assignments.items():
        print(f"  {k} -> {v}")

    # 6. Reward feeds affinity.
    print("\n=== Reward & affinity ===")
    target = winner
    before_aff = list(orch.multi_agent.agents[target].affinity)
    for _ in range(20):
        await orch.multi_agent.reward(target, 0.9)
    after_aff = orch.multi_agent.agents[target].affinity
    print(f"  {target} affinity before: "
          f"{[round(x, 3) for x in before_aff]}")
    print(f"  {target} affinity after:  "
          f"{[round(x, 3) for x in after_aff]}")

    # 7. Reputation decay.
    print("\n=== Reputation decay ===")
    before = {aid: a.reputation
              for aid, a in orch.multi_agent.agents.items()}
    orch.multi_agent.decay_reputations(force=True)
    for aid in list(orch.multi_agent.agents.keys())[:2]:
        print(f"  {aid}: {before[aid]:.4f} -> "
              f"{orch.multi_agent.agents[aid].reputation:.4f}")

    # 8. Byzantine detection — uniform values should not flag.
    print("\n=== Byzantine detection ===")
    flagged_uniform = await orch.multi_agent.detect_byzantine({
        "agent_00": 0.90, "agent_01": 0.90, "agent_02": 0.90})
    print(f"  Uniform (0.90): flagged={flagged_uniform} (expected [])")
    flagged_outlier = await orch.multi_agent.detect_byzantine({
        "agent_00": 0.90, "agent_01": 0.88, "agent_02": 0.05})
    print(f"  Outlier (0.05): flagged={flagged_outlier}")

    # 9. Coordination task.
    print("\n=== Coordination task (all ten enhancements) ===")
    for i in range(3):
        result = await orch.coordinate_task({
            "name": f"task_{i}",
            "quality": random.uniform(0.6, 0.95),
            "carbon_intensity": random.uniform(200, 700),
            "cost": random.uniform(0.3, 0.8),
            "latency_ms": random.uniform(50, 300),
            "complex": True})
        print(f"  Task {i}:")
        print(f"    strategy: {result['strategy']}"
              f" -> {result.get('strategy_name')}")
        print(f"    agent_id: {result['agent_id']}")
        print(f"    coalition: {result.get('coalition', 'none')}")
        print(f"    carbon_action: "
              f"{result['carbon_decision']['action']}")

    # 10. Health check.
    print("\n=== Health check ===")
    print(json.dumps(await orch.health_check(), indent=2, default=str))

    # 11. Chaos experiment.
    print("\n=== Chaos experiment ===")
    print(json.dumps(
        await orch.chaos.run_experiment("demo_chaos", "latency"),
        indent=2, default=str))

    await orch.shutdown()
    print("\nShutdown complete.")


if __name__ == "__main__":
    asyncio.run(_demo())
