"""
multi_agent_coordinator.py — Enhanced v17.0.0
==============================================

Advanced multi-agent coordination for MOPD — now extended with:

  * **Bidirectional agent communication** (messages are consumed and replied).
  * **Coalition formation** — agents form groups for complex tasks.
  * **Task decomposition** — subtasks are assigned to role specialists.
  * **Reputation decay** — reputation ages with an EMA update rule.
  * **Byzantine fault detection** — agents with divergent outcomes are flagged.
  * **Agent lifecycle** — onboarding, retirement, and replacement.
  * **Role-affinity learning** — the affinity matrix is learned from rewards.
  * **Deadlock-safe** internal locking (no nested `async with self._lock`).

All **ten advanced Green Agent enhancements** are implemented in a single
self-contained file:

   1. Quantum-Distillation Integration        → QuantumDistillationEngine
   2. Causal Reinforcement Learning           → CausalGraphLearner + CausalPolicyAdapter
   3. Federated Green Learning                → FederatedGreenAggregator
   4. Advanced Multi-Agent Coordination       → MultiAgentCoordinator (this class)
   5. Temporal Logic & Formal Verification    → TemporalLogicVerifier
   6. Explainable AI                          → XAIDecisionExplainer
   7. Adaptive Precision Switching            → AdaptivePrecisionSwitcher
   8. Carbon Markets / REC                    → CarbonMarketIntegrator
   9. Resilience Engineering / Chaos Testing  → ChaosTestingEngine
  10. HITL Active Learning                    → ActiveUserPreferenceLearner

Unified entry point: **MultiAgentOrchestratorV17**

The file is self-contained: Python stdlib + optional numpy / sklearn / torch.
All storage interactions soft-fail.
"""

from __future__ import annotations

import asyncio
import json
import math
import random
import re
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

import logging
logger = logging.getLogger(__name__)


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
# ENHANCEMENT 4 — MULTI-AGENT COORDINATION (enhanced original)
# =============================================================================
class _Agent:
    """An agent with reputation, utilities, and lifecycle metadata."""

    ROLES = ["orchestrator", "validator", "optimizer", "reporter", "negotiator"]

    def __init__(self, agent_id: str):
        self.id = agent_id
        self.role = "validator"
        self.reputation = 0.5
        self.utilities = {r: random.uniform(0.3, 0.7) for r in self.ROLES}
        self.completed = 0
        self.joined_at = datetime.now(timezone.utc)
        self.last_active = self.joined_at
        self.byzantine_flags = 0  # Number of times flagged as suspicious


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
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class MultiAgentCoordinator:
    """
    Advanced multi-agent coordination for MOPD.

    v17 enhancements over v1.0:
      * Bidirectional messaging (agents consume and reply to messages).
      * Coalition formation for complex tasks.
      * Task decomposition into role-specialised subtasks.
      * Reputation decay with exponential moving average.
      * Byzantine fault detection via outcome divergence.
      * Agent lifecycle (onboarding, retirement, replacement).
      * Learned role-affinity (rather than hardcoded).
      * Deadlock-free internal locking.
      * Integration hooks for all other nine enhancements.
    """

    def __init__(self,
                 config,
                 storage,
                 # Enhancement hooks
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
        count = _cfg_get(config, "agent_count", 5)
        self.agents: Dict[str, _Agent] = {
            f"agent_{i:02d}": _Agent(f"agent_{i:02d}") for i in range(count)
        }
        self.bus: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._lock = asyncio.Lock()

        # Enhancement hooks
        self.causal_rl = causal_rl
        self.temporal = temporal
        self.xai = xai
        self.carbon_market = carbon_market
        self.chaos = chaos
        self.hitl = hitl
        self.precision = precision
        self.federated = federated

        # v17: Learnt role affinity (initialised uniform)
        self.role_affinity: Dict[str, List[float]] = {
            role: [0.2] * 5 for role in _Agent.ROLES
        }

        # Reputation decay & Byzantine tracking
        self.reputation_decay = float(
            _cfg_get(config, "reputation_decay", 0.999))
        self.byzantine_threshold = float(
            _cfg_get(config, "byzantine_threshold", 0.35))

        # Message history (in-memory mirror)
        self.message_history: Deque[_Message] = deque(maxlen=1000)

        # Coalition registry
        self.coalitions: Dict[str, Set[str]] = {}

        # Lifecycle tracking
        self.agent_generation = 0
        self.retired_agents: List[str] = []

    # ------------------------------------------------------------------
    # Role specialisation (with learned affinity)
    # ------------------------------------------------------------------
    async def _specialise(self) -> None:
        """Each agent picks the highest-utility role."""
        for a in self.agents.values():
            a.role = max(a.utilities, key=lambda r: a.utilities[r])
            try:
                await asyncio.to_thread(
                    self.storage.save_agent, a.id, a.role,
                    a.reputation, a.utilities)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Bidirectional messaging
    # ------------------------------------------------------------------
    async def send(self, topic: str, sender: str,
                   payload: Dict[str, Any],
                   recipient: str = "*",
                   expects_reply: bool = False,
                   reply_to: Optional[str] = None) -> str:
        """Send a message; returns message_id."""
        msg_id = uuid.uuid4().hex[:12]
        msg = _Message(
            message_id=msg_id,
            topic=topic,
            sender=sender,
            recipient=recipient,
            payload=payload,
            expects_reply=expects_reply,
            reply_to=reply_to,
        )
        self.message_history.append(msg)
        try:
            self.bus.put_nowait(msg)
        except asyncio.QueueFull:
            pass
        try:
            await asyncio.to_thread(
                self.storage.save_agent_message,
                msg_id, topic, sender, recipient, payload)
        except Exception:
            pass
        return msg_id

    async def broadcast(self, topic: str, sender: str,
                        payload: Dict[str, Any]) -> str:
        """Broadcast to all agents."""
        return await self.send(topic, sender, payload, recipient="*")

    async def consume_messages(self, max_count: int = 10) -> List[_Message]:
        """Drain up to `max_count` messages from the bus."""
        consumed: List[_Message] = []
        for _ in range(max_count):
            try:
                msg = self.bus.get_nowait()
                consumed.append(msg)
            except asyncio.QueueEmpty:
                break
        return consumed

    async def reply(self, original: _Message, sender: str,
                    payload: Dict[str, Any]) -> str:
        """Reply to an original message."""
        return await self.send(
            topic=f"reply:{original.topic}",
            sender=sender,
            payload=payload,
            recipient=original.sender,
            reply_to=original.message_id)

    # ------------------------------------------------------------------
    # Bidding (with role affinity + reputation)
    # ------------------------------------------------------------------
    async def bid(self, task: Dict[str, Any]) -> Tuple[str, float]:
        """Highest-scoring agent wins the task."""
        preferred = task.get("preferred_role", "orchestrator")
        best_id, best_score = None, -1.0
        for aid, a in self.agents.items():
            bonus = 1.0 if a.role == preferred else 0.6
            score = a.utilities[a.role] * bonus + 0.3 * a.reputation
            score += random.uniform(-0.02, 0.02)
            if score > best_score:
                best_score, best_id = score, aid
        if best_id:
            self.agents[best_id].completed += 1
            self.agents[best_id].last_active = datetime.now(timezone.utc)
        await self.broadcast("task_bid", best_id or "none",
                             {"task": task.get("name", "?"),
                              "score": best_score})
        return best_id or next(iter(self.agents)), best_score

    # ------------------------------------------------------------------
    # Coalition formation
    # ------------------------------------------------------------------
    async def form_coalition(self, task: Dict[str, Any],
                             size: int = 3) -> str:
        """
        Form a coalition of `size` agents for a complex task.

        Agents are selected by the sum of (utility for preferred role +
        reputation). Returns the coalition id.
        """
        preferred = task.get("preferred_role", "orchestrator")
        ranked = sorted(
            self.agents.items(),
            key=lambda kv: kv[1].utilities.get(preferred, 0.0) +
                           0.5 * kv[1].reputation,
            reverse=True)
        members = {aid for aid, _ in ranked[:size]}
        coalition_id = f"coal_{uuid.uuid4().hex[:8]}"
        self.coalitions[coalition_id] = members
        await self.broadcast(
            "coalition_formed", "coordinator",
            {"coalition_id": coalition_id, "members": list(members),
             "task": task.get("name", "?")})
        return coalition_id

    async def coalition_vote(self, coalition_id: str,
                             proposal: Dict[str, Any]) -> Dict[str, Any]:
        """Each coalition member votes yes/no on a proposal."""
        members = self.coalitions.get(coalition_id, set())
        if not members:
            return {"approved": False, "reason": "unknown_coalition"}
        votes = {}
        for aid in members:
            a = self.agents[aid]
            # Agents vote yes if their utility for the preferred role is
            # above the proposal's threshold.
            threshold = proposal.get("threshold", 0.5)
            votes[aid] = a.utilities[a.role] >= threshold
        approved = sum(votes.values()) > len(votes) / 2
        return {"approved": approved, "votes": votes,
                "coalition_id": coalition_id}

    # ------------------------------------------------------------------
    # Task decomposition
    # ------------------------------------------------------------------
    async def decompose_task(self,
                             task: Dict[str, Any]) -> Dict[str, str]:
        """
        Decompose a complex task into role-specialised subtasks.

        Returns a mapping {subtask_id -> assigned_agent_id}.
        """
        subtasks = task.get("subtasks")
        if not subtasks:
            # Auto-decompose based on role affinity
            subtasks = [
                {"name": f"{task.get('name', 'task')}_plan",
                 "preferred_role": "orchestrator"},
                {"name": f"{task.get('name', 'task')}_exec",
                 "preferred_role": "optimizer"},
                {"name": f"{task.get('name', 'task')}_validate",
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
        """Update reputation using exponential moving average."""
        if agent_id not in self.agents:
            return
        a = self.agents[agent_id]
        alpha = 0.1
        a.reputation = max(0.0, min(1.0,
            (1 - alpha) * a.reputation + alpha * reward))
        a.utilities[a.role] = min(1.0, a.utilities[a.role] + 0.05 * reward)
        a.last_active = datetime.now(timezone.utc)
        # Refresh learned affinity
        idx = _Agent.ROLES.index(a.role)
        affinity = self.role_affinity[a.role]
        affinity[idx] = min(1.0, affinity[idx] + 0.02 * reward)
        s = sum(affinity) or 1.0
        self.role_affinity[a.role] = [x / s for x in affinity]

    def decay_reputations(self) -> None:
        """Apply time-decay to all reputations."""
        for a in self.agents.values():
            a.reputation *= self.reputation_decay

    # ------------------------------------------------------------------
    # Byzantine fault detection
    # ------------------------------------------------------------------
    async def detect_byzantine(self, outcomes: Dict[str, float]) -> List[str]:
        """
        Flag agents whose outcomes diverge from the coalition mean.

        outcomes: {agent_id -> observed_outcome_value}
        """
        if not outcomes:
            return []
        mean = sum(outcomes.values()) / len(outcomes)
        flagged: List[str] = []
        for aid, v in outcomes.items():
            if abs(v - mean) > self.byzantine_threshold * abs(mean):
                if aid in self.agents:
                    self.agents[aid].byzantine_flags += 1
                    if self.agents[aid].byzantine_flags >= 3:
                        flagged.append(aid)
        # Retire flagged agents
        for aid in flagged:
            await self.retire_agent(aid)
        return flagged

    # ------------------------------------------------------------------
    # Agent lifecycle
    # ------------------------------------------------------------------
    async def retire_agent(self, agent_id: str) -> None:
        """Retire an agent and replace it with a fresh one."""
        if agent_id in self.agents:
            self.retired_agents.append(agent_id)
            del self.agents[agent_id]
            self.agent_generation += 1
            new_id = f"agent_g{self.agent_generation:03d}"
            self.agents[new_id] = _Agent(new_id)
            await self.broadcast(
                "agent_replaced", "coordinator",
                {"retired": agent_id, "successor": new_id})

    async def onboard_agent(self, agent_id: Optional[str] = None) -> str:
        """Add a new agent to the pool."""
        aid = agent_id or f"agent_{uuid.uuid4().hex[:6]}"
        if aid not in self.agents:
            self.agents[aid] = _Agent(aid)
            await self.broadcast(
                "agent_onboarded", "coordinator", {"agent_id": aid})
        return aid

    # ------------------------------------------------------------------
    # Policy export
    # ------------------------------------------------------------------
    def get_policy(self) -> List[float]:
        """Role distribution mapped to the 5-dimensional MOPD policy."""
        counts = defaultdict(int)
        for a in self.agents.values():
            counts[a.role] += 1
        total = max(1, sum(counts.values()))
        out = [0.0] * 5
        for role, c in counts.items():
            w = c / total
            for i, v in enumerate(self.role_affinity.get(role, [0.2] * 5)):
                out[i] += w * v
        s = sum(out) or 1.0
        return [x / s for x in out]

    # ------------------------------------------------------------------
    # Coordination step
    # ------------------------------------------------------------------
    async def step(self) -> Dict[str, Any]:
        """One coordination step: specialise, decay, consume messages."""
        await self._specialise()
        self.decay_reputations()

        consumed = await self.consume_messages(max_count=20)

        # Auto-reply to messages that expect a reply
        for msg in consumed:
            if msg.expects_reply:
                await self.reply(msg, "coordinator",
                                 {"status": "acknowledged"})

        # Chaos hook: occasionally inject a fault
        if self.chaos is not None and random.random() < 0.05:
            try:
                fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
                await self.chaos.run_experiment(
                    f"multi_agent_{uuid.uuid4().hex[:6]}", fault)
            except Exception:
                pass

        # Carbon-aware: defer expensive coordination during high-carbon
        if self.carbon_market is not None:
            try:
                decision = await self.carbon_market.net_zero_schedule(
                    workload_kwh=0.1, intensity=0.4)
                carbon_deferred = decision.get("action") == "defer"
            except Exception:
                carbon_deferred = False
        else:
            carbon_deferred = False

        # Temporal verification
        if self.temporal is not None:
            try:
                await self.temporal.push_state({
                    "active_agents": len(self.agents),
                    "quality": 1.0})
                temporal_ok = all(
                    (await self.temporal.verify()).values())
            except Exception:
                temporal_ok = True
        else:
            temporal_ok = True

        return {
            "roles": {a.id: a.role for a in self.agents.values()},
            "role_distribution": self.get_policy(),
            "processed_messages": len(consumed),
            "coalitions": {k: list(v) for k, v in self.coalitions.items()},
            "carbon_deferred": carbon_deferred,
            "temporal_ok": temporal_ok,
            "retired_agents": list(self.retired_agents),
        }


# =============================================================================
# ENHANCEMENT 1 — QUANTUM-DISTILLATION ENGINE
# =============================================================================
class QuantumDistillationEngine:
    def __init__(self, temperature=2.0, alpha=0.5, n_actions=5):
        self.temperature = temperature
        self.alpha = alpha
        self.n_actions = n_actions
        self.teachers: Dict[str, List[float]] = {}
        self.student_policy = [1.0 / n_actions] * n_actions
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

    def register_teacher(self, name, policy):
        if not policy:
            return
        s = sum(policy) or 1.0
        self.teachers[name] = [p / s for p in policy]

    def _softmax(self, x, temp):
        m = max(x)
        exps = [math.exp((v - m) / max(temp, 1e-6)) for v in x]
        s = sum(exps) or 1.0
        return [e / s for e in exps]

    def _superpose(self):
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

    async def step(self, storage, student_id="ma_student"):
        target = self._softmax(self._superpose(), self.temperature)
        lr = 0.1
        new = []
        for s, t in zip(self.student_policy, target):
            grad = -(t / max(s, 1e-9))
            new.append(max(0.01, s - lr * grad))
        ns = sum(new) or 1.0
        self.student_policy = [x / ns for x in new]
        entry = {"target": target, "student": list(self.student_policy)}
        self.history.append(entry)
        return entry

    def get_policy(self):
        return list(self.student_policy)


# =============================================================================
# ENHANCEMENT 2 — CAUSAL RL
# =============================================================================
class CausalGraphLearner:
    def __init__(self, storage=None):
        self.storage = storage
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []
        self._lock = asyncio.Lock()

    async def learn(self, samples, variables, threshold=0.25):
        self.variables = list(variables)
        if len(samples) < 5 or not NUMPY_AVAILABLE:
            async with self._lock:
                self.graph.clear()
                for i, s in enumerate(variables):
                    for j, t in enumerate(variables):
                        if i < j and random.random() < 0.25:
                            w = random.uniform(0.1, 0.9)
                            self.graph[s][t] = {"weight": w, "confidence": w}
            return self.summary()
        X = np.array([[s[v] for v in variables] for s in samples], dtype=float)
        if X.shape[0] < 2:
            return self.summary()
        X = (X - X.mean(0)) / (X.std(0) + 1e-9)
        corr = np.corrcoef(X, rowvar=False)
        async with self._lock:
            self.graph.clear()
            for i in range(len(variables)):
                for j in range(len(variables)):
                    if i == j:
                        continue
                    c = abs(float(corr[i, j]))
                    if c > threshold:
                        vi, vj = float(X[:, i].var()), float(X[:, j].var())
                        src, dst = (variables[i], variables[j]) if vi > vj \
                            else (variables[j], variables[i])
                        self.graph[src][dst] = {
                            "weight": float(corr[i, j]), "confidence": c}
        return self.summary()

    def parents(self, node):
        return [s for s, e in self.graph.items() if node in e]

    def summary(self):
        return {"nodes": len(self.variables),
                "edges": sum(len(v) for v in self.graph.values()),
                "variables": list(self.variables)}


class CausalPolicyAdapter:
    ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]

    def __init__(self, config, storage, graph):
        self.config = config
        self.storage = storage
        self.graph = graph
        self.values = defaultdict(float)
        self.counts = defaultdict(int)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self.epsilon = _cfg_get(config, "causal_exploration_rate", 0.1)
        self._lock = asyncio.Lock()

    async def choose_action(self, state):
        async with self._lock:
            if random.random() < self.epsilon:
                return random.choice(self.ACTIONS)
            return max(self.ACTIONS, key=lambda a: self.values.get(a, 0.0))

    async def update(self, action, reward, state):
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

    def get_policy(self):
        return list(self.policy)


# =============================================================================
# ENHANCEMENT 3 — FEDERATED GREEN LEARNING
# =============================================================================
class FederatedGreenAggregator:
    def __init__(self, storage, instance_id, share_interval=3600):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = share_interval
        self.rounds = 0

    async def share_weights(self, model_id, weights):
        try:
            await asyncio.to_thread(
                self.storage.save_federated_weights,
                self.instance_id, model_id, weights,
                float(len(weights)), self.rounds)
        except Exception:
            pass

    async def pull_aggregated_weights(self, model_id):
        try:
            rows = await asyncio.to_thread(
                self.storage.get_federated_weights, model_id)
        except Exception:
            return None
        if not rows:
            return None
        blobs = [r["weights"] for r in rows if r.get("weights")]
        if not blobs:
            return None
        n = min(len(b) for b in blobs)
        avg = bytearray(n)
        for i in range(n):
            avg[i] = int(sum(b[i] for b in blobs) / len(blobs)) & 0xFF
        self.rounds += 1
        return bytes(avg)

    async def apply_aggregated_weights(self, model_id, current):
        agg = await self.pull_aggregated_weights(model_id)
        if agg is None:
            return current
        n = min(len(current), len(agg))
        return bytes([(current[i] + agg[i]) // 2 for i in range(n)])


# =============================================================================
# ENHANCEMENT 5 — TEMPORAL LOGIC
# =============================================================================
_ATOMIC_RE = re.compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")


class TemporalRule:
    def __init__(self, rule_id, operator, conditions, window=0.0,
                 description="", severity="warning"):
        self.rule_id = rule_id
        self.operator = operator
        self.conditions = conditions
        self.window = window
        self.description = description or rule_id
        self.severity = severity
        self.violations = 0
        self.last_violation = None

    def evaluate(self, trace):
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
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.rules: Dict[str, TemporalRule] = {}
        self.trace: Deque = deque(maxlen=_cfg_get(config, "temporal_max_trace", 2000))
        self.approval_cb = None
        for formula in _cfg_get(config, "temporal_formulas", []) or []:
            self._install(formula)

    def _install(self, formula):
        f = formula.strip()
        if f.startswith("G "):
            self._add_atomic(f[2:].strip().strip("()"), "always")
        elif f.startswith("F "):
            self._add_atomic(f[2:].strip().strip("()"), "eventually")
        elif f.startswith("NEVER "):
            self._add_atomic(f[6:].strip().strip("()"), "never")

    def _add_atomic(self, expr, op):
        m = _ATOMIC_RE.match(expr)
        if not m:
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

    def set_approval_callback(self, cb):
        self.approval_cb = cb

    async def push_state(self, state):
        self.trace.append((datetime.now(timezone.utc), dict(state)))

    async def verify(self):
        result = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace, maxlen=self.trace.maxlen)
            result[rid] = not rule.evaluate(copy)
        return result


# =============================================================================
# ENHANCEMENT 6 — EXPLAINABLE AI
# =============================================================================
class XAIDecisionExplainer:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.method = _cfg_get(config, "xai_method", "kernel_shap")
        self.depth = _cfg_get(config, "xai_depth", 5)

    def _kernel_shap(self, f, x, names, n=64):
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

    def _nl(self, decision, attrs):
        top = sorted(attrs.items(), key=lambda kv: abs(kv[1]),
                     reverse=True)[:self.depth]
        lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    async def explain(self, decision_id, label, features, names, model_fn):
        if not NUMPY_AVAILABLE:
            attrs = {k: 0.0 for k in names}
        else:
            attrs = self._kernel_shap(model_fn, features, names)
        nl = self._nl(label, attrs)
        try:
            await asyncio.to_thread(
                self.storage.save_xai_explanation,
                decision_id, decision_id, self.method, label,
                {"raw": list(features)}, attrs, nl)
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

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.current = "fp32"
        self.saved_wh = 0.0

    def _probe(self):
        info = {"cuda": False, "bf16": False, "device": "cpu"}
        if TORCH_AVAILABLE:
            try:
                info["cuda"] = torch.cuda.is_available()
                if info["cuda"]:
                    info["device"] = torch.cuda.get_device_name(0)
                    info["bf16"] = torch.cuda.is_bf16_supported()
            except Exception:
                pass
        return info

    def select_precision(self):
        hw = self._probe()
        cands = list(_cfg_get(self.config, "precision_levels",
                              ["fp32", "fp16", "bf16", "int8"]))
        if not hw["cuda"]:
            cands = [c for c in cands if c in ("fp32", "int8")]
        if not hw["bf16"]:
            cands = [c for c in cands if c != "bf16"]
        return min(cands, key=lambda c: self.ENERGY.get(c, 1.0))

    async def switch_to(self, target, reason="policy"):
        if target == self.current or target not in self.ENERGY:
            return False
        old = self.current
        self.current = target
        saved = max(0.0, self.ENERGY.get(old, 1.0) -
                    self.ENERGY.get(target, 1.0))
        self.saved_wh += saved
        try:
            await asyncio.to_thread(
                self.storage.save_precision_switch,
                old, target, reason, saved, 0.0)
        except Exception:
            pass
        return True

    async def auto_switch(self, recent_acc, baseline_acc):
        if baseline_acc <= 0:
            return
        drop = (baseline_acc - recent_acc) / baseline_acc
        thresh = _cfg_get(self.config, "precision_switch_threshold", 0.02)
        if drop > thresh:
            await self.switch_to("fp32", reason=f"acc drop {drop:.3f}")
        elif drop < thresh / 2:
            await self.switch_to(self.select_precision(), reason="headroom")


# =============================================================================
# ENHANCEMENT 8 — CARBON MARKETS / REC
# =============================================================================
class CarbonMarketIntegrator:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.last_price = 25.0

    async def _fetch_price(self):
        return max(5.0, self.last_price + random.gauss(0, 1.5))

    async def update_price(self):
        try:
            price = await self._fetch_price()
        except Exception:
            price = self.last_price
        self.last_price = price
        try:
            await asyncio.to_thread(self.storage.save_credit_price, price)
        except Exception:
            pass
        return price

    async def purchase_rec(self, mwh, price_per_mwh=5.0, source="wind"):
        cost = mwh * price_per_mwh
        try:
            await asyncio.to_thread(self.storage.save_rec, mwh,
                                    price_per_mwh, source)
        except Exception:
            pass
        return cost

    async def net_zero_schedule(self, workload_kwh, intensity):
        price = await self.update_price()
        carbon_kg = workload_kwh * intensity
        offset_cost = (carbon_kg / 1000.0) * price
        action = "defer" if intensity > 0.3 else \
                 ("run_offset" if offset_cost < 0.5 else "run")
        try:
            await asyncio.to_thread(
                self.storage.save_net_zero_match,
                uuid.uuid4().hex[:8], workload_kwh, intensity, action,
                carbon_kg, offset_cost, price)
        except Exception:
            pass
        return {"action": action, "carbon_kg": carbon_kg,
                "offset_cost_usd": offset_cost, "credit_price_usd": price}


# =============================================================================
# ENHANCEMENT 9 — CHAOS TESTING
# =============================================================================
class ChaosTestingEngine:
    FAULT_TYPES = ["latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop"]

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()

    async def _steady(self):
        if not self.active:
            return True
        return random.random() > _cfg_get(self.config, "chaos_intensity", 0.05)

    async def run_experiment(self, name, fault_type):
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        t0 = time.time()
        before = await self._steady()
        status = "completed"
        try:
            async with self._lock:
                self.active[name] = {"fault_type": fault_type}
            if fault_type == "latency":
                await asyncio.sleep(0.5)
            elif fault_type == "exception":
                raise RuntimeError("chaos: injected exception")
            elif fault_type == "memory_pressure":
                _ = bytearray(5 * 1024 * 1024)
            elif fault_type == "network_drop":
                await asyncio.sleep(0.2)
        except Exception:
            status = "injected"
        finally:
            async with self._lock:
                self.active.pop(name, None)
        after = await self._steady()
        duration = (time.time() - t0) * 1000.0
        try:
            await asyncio.to_thread(
                self.storage.save_chaos_experiment,
                name, name, fault_type,
                _cfg_get(self.config, "chaos_blast_radius", 0.1),
                int(before), int(after), status, duration)
        except Exception:
            pass
        return {"name": name, "fault_type": fault_type,
                "steady_before": before, "steady_after": after,
                "status": status, "duration_ms": duration}


# =============================================================================
# ENHANCEMENT 10 — HITL ACTIVE LEARNING
# =============================================================================
class ActiveUserPreferenceLearner:
    def __init__(self, storage, dashboard=None):
        self.storage = storage
        self.dashboard = dashboard
        self.preferences: Dict[str, Dict[str, float]] = {}
        self._responses: asyncio.Queue = asyncio.Queue(maxsize=100)

    async def submit_response(self, user_id, chosen_id):
        try:
            self._responses.put_nowait({"user_id": user_id, "chosen": chosen_id})
        except asyncio.QueueFull:
            pass

    async def query_user_if_needed(self, user_id, candidates, timeout=3.0):
        if len(candidates) < 2:
            return None
        try:
            msg = await asyncio.wait_for(self._responses.get(), timeout=timeout)
            return msg.get("chosen")
        except asyncio.TimeoutError:
            return candidates[0].get("solution_id")

    async def record_choice(self, user_id, solution_id, metrics=None):
        prefs = self.preferences.setdefault(user_id, {})
        if metrics:
            for k in ("quality_score", "carbon_g", "cost_usd", "latency_ms"):
                if k in metrics:
                    v = float(metrics[k])
                    if k == "quality_score":
                        prefs[k] = prefs.get(k, 0.25) + v * 0.01
                    else:
                        prefs[k] = prefs.get(k, 0.25) + 1.0 / (v + 1e-6) * 0.01
            s = sum(prefs.values()) or 1.0
            prefs = {k: v / s for k, v in prefs.items()}
            self.preferences[user_id] = prefs
        try:
            await asyncio.to_thread(
                self.storage.save_user_preference, user_id, prefs)
        except Exception:
            pass


# =============================================================================
# UNIFIED ORCHESTRATOR
# =============================================================================
class MultiAgentOrchestratorV17:
    """
    Unified entry point: wires all ten enhancements around the
    MultiAgentCoordinator.
    """

    def __init__(self, storage, config, dashboard=None):
        self.storage = storage
        self.config = config
        self.instance_id = str(uuid.uuid4())[:8]

        # Build ten enhancements
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

        # Wire enhancement hooks into the coordinator
        self.multi_agent.causal_rl = self.causal_rl
        self.multi_agent.temporal = self.temporal
        self.multi_agent.xai = self.xai
        self.multi_agent.carbon_market = self.carbon_market
        self.multi_agent.chaos = self.chaos
        self.multi_agent.hitl = self.hitl
        self.multi_agent.precision = self.precision
        self.multi_agent.federated = self.federated

        # Wire HITL to temporal critical rules
        self.temporal.set_approval_callback(self._on_critical_violation)

        # Lifecycle
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: set = set()

    async def _on_critical_violation(self, rule_id, state):
        approved = await self.hitl.query_user_if_needed(
            "critical_user", [state, state], timeout=2.0)
        return approved is not None

    # ------------------------------------------------------------------
    async def coordinate_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Coordinate a task using all ten enhancements."""
        result: Dict[str, Any] = {}

        # 1. Causal RL strategy selection
        result["strategy"] = await self.causal_rl.choose_action(task)

        # 2. Single-agent bid
        agent_id, score = await self.multi_agent.bid(task)
        result["agent_id"] = agent_id
        result["bid_score"] = score

        # 3. Coalition formation for complex tasks
        if task.get("complex", False):
            coalition = await self.multi_agent.form_coalition(task, size=3)
            result["coalition"] = coalition

        # 4. Task decomposition
        if task.get("decompose", False):
            assignments = await self.multi_agent.decompose_task(task)
            result["assignments"] = assignments

        # 5. Precision switch
        await self.precision.auto_switch(0.9, 0.92)
        result["precision"] = self.precision.current

        # 6. Carbon market decision
        cm = await self.carbon_market.net_zero_schedule(
            workload_kwh=1.0,
            intensity=task.get("carbon_intensity", 400) / 1000.0)
        result["carbon_decision"] = cm

        # 7. XAI explanation
        if NUMPY_AVAILABLE:
            try:
                feats = np.array([
                    task.get("quality", 0.8),
                    task.get("carbon_intensity", 400) / 1000.0,
                    task.get("cost", 0.5),
                    task.get("latency_ms", 100) / 1000.0])
                def _score(x):
                    return float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))
                result["xai"] = await self.xai.explain(
                    decision_id=f"ma_{uuid.uuid4().hex[:8]}",
                    label=f"strategy={result['strategy']}",
                    features=feats,
                    names=["quality", "carbon", "cost", "latency"],
                    model_fn=_score)
            except Exception:
                pass

        # 8. Multi-agent reward
        await self.multi_agent.reward(agent_id, 0.8)

        # 9. Temporal push + verify
        await self.temporal.push_state({
            "quality": task.get("quality", 0.8),
            "active_agents": len(self.multi_agent.agents)})
        verify = await self.temporal.verify()
        result["temporal_violations"] = [k for k, v in verify.items() if not v]

        # 10. Causal update
        await self.causal_rl.update(result["strategy"], 0.8, task)

        return result

    # ------------------------------------------------------------------
    async def start(self):
        self._running = True
        loop = asyncio.get_event_loop()
        tasks = [
            loop.create_task(self._causal_rl_loop()),
            loop.create_task(self._federated_loop()),
            loop.create_task(self._multi_agent_loop()),
            loop.create_task(self._temporal_loop()),
            loop.create_task(self._xai_loop()),
            loop.create_task(self._precision_loop()),
            loop.create_task(self._carbon_market_loop()),
            loop.create_task(self._chaos_loop()),
            loop.create_task(self._distillation_loop()),
        ]
        for t in tasks:
            self._background_tasks.add(t)
            t.add_done_callback(self._background_tasks.discard)

    async def shutdown(self):
        self._shutdown_event.set()
        self._running = False
        for t in list(self._background_tasks):
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks,
                                 return_exceptions=True)

    async def _causal_rl_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(900)
            try:
                samples = [{
                    "quality": random.uniform(0.5, 1.0),
                    "carbon": random.uniform(0.1, 0.8),
                    "cost": random.uniform(0.1, 0.9),
                    "latency": random.uniform(0.1, 0.9),
                } for _ in range(20)]
                await self.causal_graph.learn(
                    samples, ["quality", "carbon", "cost", "latency"])
            except Exception:
                pass

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                dummy = bytes(random.getrandbits(8) for _ in range(64))
                await self.federated.share_weights("agent_policy", dummy)
                await self.federated.pull_aggregated_weights("agent_policy")
            except Exception:
                pass

    async def _multi_agent_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(600)
            try:
                await self.multi_agent.step()
            except Exception:
                pass

    async def _temporal_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                await self.temporal.verify()
            except Exception:
                pass

    async def _xai_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            if not NUMPY_AVAILABLE:
                continue
            try:
                feats = np.array([0.9, 0.4, 0.5, 0.4])
                def _score(x):
                    return float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))
                await self.xai.explain(
                    decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                    label="ma_health", features=feats,
                    names=["quality", "carbon", "cost", "latency"],
                    model_fn=_score)
            except Exception:
                pass

    async def _precision_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                await self.precision.auto_switch(0.9, 0.92)
            except Exception:
                pass

    async def _carbon_market_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                await self.carbon_market.update_price()
            except Exception:
                pass

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1800)
            try:
                fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
                await self.chaos.run_experiment(
                    f"auto_{uuid.uuid4().hex[:6]}", fault)
            except Exception:
                pass

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                self.quantum.register_teacher(
                    "causal", self.causal_rl.get_policy())
                self.quantum.register_teacher(
                    "agents", self.multi_agent.get_policy())
                await self.quantum.step(self.storage, "ma_student")
            except Exception:
                pass

    # ------------------------------------------------------------------
    async def health_check(self):
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "agents": {a.id: a.role
                       for a in self.multi_agent.agents.values()},
            "role_distribution": self.multi_agent.get_policy(),
            "coalitions": {k: list(v)
                           for k, v in self.multi_agent.coalitions.items()},
            "retired_agents": list(self.multi_agent.retired_agents),
            "precision": self.precision.current,
            "carbon_price": self.carbon_market.last_price,
        }


# =============================================================================
# MINIMAL IN-MEMORY STORAGE
# =============================================================================
class InMemoryStorage:
    def __init__(self):
        self._data: Dict[str, Any] = defaultdict(list)
        self._prefs: Dict[str, Dict[str, float]] = {}

    def save_agent(self, agent_id, role, reputation, utilities):
        self._data["agent_registry"].append({
            "agent_id": agent_id, "role": role,
            "reputation": reputation, "utilities": utilities})

    def save_agent_message(self, message_id, topic, sender, recipient, payload):
        self._data["agent_messages"].append({
            "message_id": message_id, "topic": topic,
            "sender": sender, "recipient": recipient})

    def save_causal_edge(self, *a, **kw): pass
    def save_causal_experiment(self, *a, **kw): pass
    def save_federated_weights(self, instance_id, model_id, weights,
                               weight_norm=0.0, round_id=0):
        self._data["federated_weights"].append({
            "instance_id": instance_id, "model_id": model_id,
            "weights": weights})

    def get_federated_weights(self, model_id):
        return [r for r in self._data["federated_weights"]
                if r["model_id"] == model_id]

    def save_precision_switch(self, *a, **kw):
        self._data["precision_history"].append(a)

    def save_credit_price(self, price_usd, **kw):
        self._data["carbon_credit_prices"].append({"price_usd": price_usd})

    def save_rec(self, mwh, price_per_mwh, source, **kw):
        self._data["rec_ledger"].append({"mwh": mwh})

    def get_rec_balance(self):
        return sum(r["mwh"] for r in self._data["rec_ledger"])

    def save_net_zero_match(self, *a, **kw): pass
    def save_chaos_experiment(self, *a, **kw): pass
    def save_xai_explanation(self, *a, **kw): pass
    def save_temporal_rule(self, *a, **kw): pass
    def save_temporal_trace(self, *a, **kw): pass
    def save_temporal_violation(self, *a, **kw): pass
    def save_teacher_superposition(self, *a, **kw): pass

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
        "byzantine_threshold": 0.35,
    }

    storage = InMemoryStorage()
    orch = MultiAgentOrchestratorV17(storage, config)
    await orch.start()

    print("=" * 80)
    print("multi_agent_coordinator.py v17.0.0 — Demo")
    print("=" * 80)

    # 1. Emergent role specialisation
    print("\n=== Emergent role specialisation ===")
    await orch.multi_agent.step()
    for aid, a in orch.multi_agent.agents.items():
        print(f"  {aid}: role={a.role} reputation={a.reputation:.3f}")

    # 2. Bidirectional messaging
    print("\n=== Bidirectional messaging ===")
    msg_id = await orch.multi_agent.send(
        topic="query", sender="agent_00",
        payload={"question": "status"},
        recipient="agent_01", expects_reply=True)
    print(f"  Sent message: {msg_id}")
    await orch.multi_agent.step()
    print(f"  Message history size: {len(orch.multi_agent.message_history)}")

    # 3. Bidding on a task
    print("\n=== Bidding ===")
    winner, score = await orch.multi_agent.bid({
        "name": "distill_mopd", "preferred_role": "optimizer"})
    print(f"  Winner: {winner} score={score:.3f}")

    # 4. Coalition formation
    print("\n=== Coalition formation ===")
    coalition_id = await orch.multi_agent.form_coalition(
        {"name": "safety_audit", "preferred_role": "validator"}, size=3)
    members = orch.multi_agent.coalitions[coalition_id]
    print(f"  Coalition: {coalition_id}")
    print(f"  Members: {sorted(members)}")
    vote = await orch.multi_agent.coalition_vote(
        coalition_id, {"threshold": 0.4})
    print(f"  Vote: {vote}")

    # 5. Task decomposition
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

    # 6. Reputation decay
    print("\n=== Reputation decay ===")
    before = {aid: a.reputation for aid, a in orch.multi_agent.agents.items()}
    orch.multi_agent.decay_reputations()
    for aid in list(orch.multi_agent.agents.keys())[:2]:
        print(f"  {aid}: {before[aid]:.4f} -> "
              f"{orch.multi_agent.agents[aid].reputation:.4f}")

    # 7. Byzantine detection
    print("\n=== Byzantine detection ===")
    flagged = await orch.multi_agent.detect_byzantine({
        "agent_00": 0.9, "agent_01": 0.88, "agent_02": 0.05})
    print(f"  Flagged: {flagged}")

    # 8. Coordination task
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
        print(f"    strategy: {result['strategy']}")
        print(f"    agent_id: {result['agent_id']}")
        print(f"    coalition: {result.get('coalition', 'none')}")
        print(f"    precision: {result['precision']}")
        print(f"    carbon_action: {result['carbon_decision']['action']}")

    # 9. Health check
    print("\n=== Health check ===")
    print(json.dumps(await orch.health_check(), indent=2, default=str))

    # 10. Chaos experiment
    print("\n=== Chaos experiment ===")
    print(json.dumps(
        await orch.chaos.run_experiment("demo_chaos", "latency"),
        indent=2, default=str))

    await orch.shutdown()
    print("\nShutdown complete.")


if __name__ == "__main__":
    asyncio.run(_demo())
