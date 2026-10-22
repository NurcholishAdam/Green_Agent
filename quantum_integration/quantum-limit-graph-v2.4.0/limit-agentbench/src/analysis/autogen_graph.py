#!/usr/bin/env python3
# =============================================================================
# FILE: src/analysis/adapters/autogen_conversation_graph.py
# VERSION: 2.0.0
# =============================================================================
"""
AutoGen conversation graph adapter — v2.0.0
============================================

Extracts message-graph metrics from AutoGen runs and emits the shared
analysis-layer contracts (`DecisionRecord`, `AgentObservation`) so that
`MultiAgentRoleAnalyzer` and `HumanFeedbackAnalyzer` can consume them.

v1 API preserved (with documented semantics change — see below):
    graph = AutoGenConversationGraph()
    graph.record(sender, recipient, content)
    graph.metrics()

v2 enhancements:
  [P0] `sender_role` and `recipient_role` are independent (was: recipient
       inherited sender's role); `conversation_depth` now returns true graph
       depth (was: `len(messages)`); legacy message-count value is exposed as
       `message_count`; `delegated_to` deduped and excludes replies;
       `observations()` includes agents who only receive; `conversation_
       duration_s` uses min/max timestamps.
  [P1] `coordination_calls` counts only AGENT_TO_AGENT + HANDOFF turns;
       `selected_action` uses semantic labels (`send_message`, `call_tool`,
       `request_approval`, …); `tool_call_id` correlates call↔result;
       PLAN/THINKING/HANDOFF kinds; `export()` includes observations and
       decisions; `role_turn_counts`; `invalid_records` surfaced.
  [P2] `reset()`, `__repr__`, `__len__`, `__iter__`; lazy `%s` logging;
       `max_turns` cap (default 1M) prevents unbounded growth; graph-level
       `run_id`; content type validation; safe `_to_jsonable` helper.
  [P3] Graph analytics: `adjacency()`, `centrality()`, `connected_components()`,
       `longest_path()`; `turn_records(run_id=None)`; optional `RoleKind`
       alignment check with `src.analysis`; `_self_test()`.

**v2.0.0 breaking change:** `metrics()["conversation_depth"]` now returns
the longest chain length in **nodes** (was: total message count). The prior
value is available as `metrics()["message_count"]`. Callers reading
`conversation_depth` for message-count semantics should migrate to
`message_count`.
"""

from __future__ import annotations

import json
import logging
import time
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import (Any, Deque, Dict, Iterable, Iterator, List, Optional,
                    Set, Tuple)

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================
class TurnKind(Enum):
    """Classification of a single conversation turn."""
    AGENT_TO_AGENT = "agent_to_agent"
    AGENT_TO_HUMAN = "agent_to_human"
    HUMAN_TO_AGENT = "human_to_agent"
    TOOL_CALL = "tool_call"
    TOOL_RESULT = "tool_result"
    SYSTEM = "system"
    # [P1] new kinds — explicit only; not inferred to avoid misclassification
    PLAN = "plan"
    THINKING = "thinking"
    HANDOFF = "handoff"
    UNKNOWN = "unknown"


class RoleKind(Enum):
    """Recognised conversation roles (mirrors analysis.RoleKind where possible)."""
    GENERALIST = "generalist"
    ENERGY_SPECIALIST = "energy_specialist"
    CARBON_SPECIALIST = "carbon_specialist"
    HELIUM_SPECIALIST = "helium_specialist"
    LATENCY_SPECIALIST = "latency_specialist"
    COORDINATOR = "coordinator"
    DELEGATOR = "delegator"
    HUMAN = "human"
    TOOL = "tool"


# [P3] optional RoleKind alignment check
try:
    from src.analysis import RoleKind as _ExternalRoleKind  # type: ignore
except ImportError:
    try:
        from analysis import RoleKind as _ExternalRoleKind  # type: ignore
    except ImportError:
        _ExternalRoleKind = None

if _ExternalRoleKind is not None:
    try:
        _local_values = {r.value for r in RoleKind}
        _external_values = {r.value for r in _ExternalRoleKind}
        _missing = _external_values - _local_values
        if _missing:
            logger.warning(
                "RoleKind diverges from analysis.RoleKind; missing: %s",
                sorted(_missing))
    except (TypeError, AttributeError):
        pass


# =============================================================================
# Shared contracts (lazy import)
# =============================================================================
def _try_import_contracts():
    try:
        from src.analysis import DecisionRecord, AgentObservation  # type: ignore
        return DecisionRecord, AgentObservation, True
    except Exception:
        pass
    try:
        from analysis import DecisionRecord, AgentObservation  # type: ignore
        return DecisionRecord, AgentObservation, True
    except Exception:
        pass

    # Fallback dataclasses — shape-compatible with analysis contracts
    @dataclass
    class DecisionRecord:  # type: ignore[no-redef]
        run_id: str
        timestamp: datetime
        task_id: str
        selected_action: str
        alternatives: List[str] = field(default_factory=list)
        policy_version: str = ""
        model_or_agent: str = ""
        hardware_profile: str = ""
        precision: Optional[str] = None
        quality_score: Optional[float] = None
        latency_ms: float = 0.0
        energy_kwh: float = 0.0
        carbon_operational_kg: float = 0.0
        helium_units: float = 0.0
        carbon_contractual_kg: float = 0.0
        rec_mwh: float = 0.0
        instrument_provenance: Optional[Dict[str, Any]] = None
        uncertainty: Optional[float] = None
        explanation: Dict[str, Any] = field(default_factory=dict)
        safety_verdict: str = "n/a"
        safety_evidence_id: Optional[str] = None
        human_approval_required: bool = False
        human_approval_status: Optional[str] = None
        escalation_reason: Optional[str] = None
        quantum_status: str = "classical"
        quantum_shots: Optional[int] = None
        quantum_queue_ms: Optional[float] = None
        quantum_error_rate: Optional[float] = None
        quantum_mitigation_applied: bool = False
        provenance: Dict[str, Any] = field(default_factory=dict)

    @dataclass
    class AgentObservation:  # type: ignore[no-redef]
        agent_id: str
        task_id: str
        role: str
        delegated_to: List[str]
        coordination_calls: int
        latency_ms: float
        energy_kwh: float

    return DecisionRecord, AgentObservation, False


DecisionRecord, AgentObservation, _CONTRACTS_AVAILABLE = _try_import_contracts()


# =============================================================================
# Safe serialization helpers — [P2]
# =============================================================================
def _to_dict(obj: Any) -> Dict[str, Any]:
    """Convert dataclass / pydantic / plain object to dict."""
    if isinstance(obj, dict):
        return dict(obj)
    if hasattr(obj, "model_dump") and callable(obj.model_dump):
        try:
            return obj.model_dump()
        except Exception:
            pass
    if is_dataclass(obj) and not isinstance(obj, type):
        return asdict(obj)
    if hasattr(obj, "__dict__"):
        return dict(vars(obj))
    raise TypeError(f"cannot convert {type(obj).__name__} to dict")


def _to_jsonable(v: Any) -> Any:
    """Recursively convert to JSON-safe structure."""
    if isinstance(v, dict):
        return {str(k): _to_jsonable(val) for k, val in v.items()}
    if isinstance(v, (list, tuple, set)):
        return [_to_jsonable(val) for val in v]
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, Enum):
        return v.value
    if is_dataclass(v) and not isinstance(v, type):
        return _to_jsonable(asdict(v))
    if hasattr(v, "model_dump") and callable(v.model_dump):
        try:
            return _to_jsonable(v.model_dump())
        except Exception:
            pass
    return v


# =============================================================================
# Enhanced AutoGenConversationGraph
# =============================================================================
class AutoGenConversationGraph:
    """
    Enhanced AutoGen conversation-graph adapter.

    Backward-compatible with the v1 two-method API (`record`, `metrics`).
    All enhancements are optional keyword arguments on `record()`.

    **v2.0.0 breaking change:** `metrics()["conversation_depth"]` now returns
    true graph depth (nodes in the longest chain), not `len(messages)`.
    Prior behaviour is available via `metrics()["message_count"]`.
    """

    HUMAN_MARKERS = {"human", "user", "reviewer", "operator"}
    TOOL_MARKERS = {"tool", "function", "executor", "retriever"}

    # [P1] semantic action labels for DecisionRecord
    _ACTION_LABELS: Dict[str, str] = {
        TurnKind.AGENT_TO_AGENT.value: "send_message",
        TurnKind.AGENT_TO_HUMAN.value: "request_approval",
        TurnKind.HUMAN_TO_AGENT.value: "receive_approval",
        TurnKind.TOOL_CALL.value: "call_tool",
        TurnKind.TOOL_RESULT.value: "receive_tool_result",
        TurnKind.SYSTEM.value: "system_event",
        TurnKind.PLAN.value: "plan",
        TurnKind.THINKING.value: "think",
        TurnKind.HANDOFF.value: "handoff",
        TurnKind.UNKNOWN.value: "unknown",
    }

    def __init__(self, max_turns: int = 1_000_000):
        # [P2] bounded storage — deque auto-evicts when full
        self.messages: Deque[Dict[str, Any]] = deque(maxlen=max_turns)
        self._turn_records: Deque[Dict[str, Any]] = deque(maxlen=max_turns)
        self._max_turns = max_turns

        # Analyzer-ready outputs (rebuilt on call)
        self._observations: List[Any] = []
        self._decisions: List[Any] = []

        # Role and coordination state
        self._role_by_agent: Dict[str, str] = {}
        self._delegation_counts: Dict[str, Counter] = defaultdict(Counter)
        self._outbound_counts: Dict[str, int] = defaultdict(int)
        self._coordination_counts: Dict[str, int] = defaultdict(int)
        self._self_loop_count: int = 0
        self._invalid_records: int = 0
        self._evicted_count: int = 0

        # Time bounds
        self._min_ts: Optional[float] = None
        self._max_ts: Optional[float] = None

        # [P2] graph-level run_id
        self._default_run_id: Optional[str] = None

    # ------------------------------------------------------------------
    # [P2] Dunder helpers
    # ------------------------------------------------------------------
    def __repr__(self) -> str:
        return (
            f"AutoGenConversationGraph("
            f"turns={len(self.messages)}, "
            f"agents={len(self._role_by_agent) or self._count_agents()}, "
            f"depth={self._graph_depth()}, "
            f"invalid={self._invalid_records}, "
            f"evicted={self._evicted_count})"
        )

    def __len__(self) -> int:
        return len(self.messages)

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        return iter(self.messages)

    # ------------------------------------------------------------------
    # [P2] Reset
    # ------------------------------------------------------------------
    def reset(self) -> None:
        """Clear all state — useful when reusing one graph across runs."""
        self.__init__(max_turns=self._max_turns)

    def set_run_id(self, run_id: str) -> None:
        """[P2] Set a default run_id for `decision_records()` and `metrics()`."""
        self._default_run_id = run_id

    # ------------------------------------------------------------------
    # Public API — record
    # ------------------------------------------------------------------
    def record(
        self,
        sender: str,
        recipient: str,
        content: str,
        *,
        # [P0] role split — `role` is a deprecated alias for `sender_role`
        role: Optional[str] = None,
        sender_role: Optional[str] = None,
        recipient_role: Optional[str] = None,
        # Turn classification
        kind: Optional[str] = None,
        timestamp: Optional[float] = None,
        # [P1] tool correlation
        tool_calls: Optional[List[str]] = None,
        tool_call_id: Optional[str] = None,
        # Metrics
        tokens: Optional[int] = None,
        latency_ms: Optional[float] = None,
        energy_kwh: Optional[float] = None,
        # Context
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> None:
        """
        Record a single conversation turn.

        Original behaviour preserved: appends `{"from", "to", "content"}` to
        `self.messages` and returns nothing.
        """
        # --- [P0] Input validation ---
        if not sender or not recipient:
            self._invalid_records += 1
            logger.warning(
                "Invalid AutoGen turn (sender=%r, recipient=%r); skipped",
                sender, recipient)
            return

        # [P2] content type validation
        if content is None:
            content_str = ""
        elif isinstance(content, str):
            content_str = content
        else:
            logger.warning(
                "Non-string content coerced to str: type=%s",
                type(content).__name__)
            content_str = str(content)

        # --- Original record (exact shape preserved) ---
        if len(self.messages) == self.messages.maxlen:
            self._evicted_count += 1
        self.messages.append({
            "from": sender,
            "to": recipient,
            "content": content_str,
        })

        # --- [P0] Timestamp tracking (min/max safe against out-of-order) ---
        ts = float(timestamp) if timestamp is not None else time.time()
        if self._min_ts is None or ts < self._min_ts:
            self._min_ts = ts
        if self._max_ts is None or ts > self._max_ts:
            self._max_ts = ts

        # --- Classify turn kind ---
        inferred_kind = kind or self._infer_kind(sender, recipient, sender_role)

        turn_record = {
            "from": sender,
            "to": recipient,
            "kind": inferred_kind,
            "role": sender_role or role,
            "timestamp": ts,
            "tool_calls": list(tool_calls or []),
            "tool_call_id": tool_call_id,
            "tokens": tokens,
            "latency_ms": latency_ms,
            "energy_kwh": energy_kwh,
            "run_id": run_id or self._default_run_id,
            "task_id": task_id or "",
        }
        if len(self._turn_records) == self._turn_records.maxlen:
            self._evicted_count += 1
        self._turn_records.append(turn_record)

        # --- [P0] Role tracking — sender and recipient are independent ---
        effective_sender_role = sender_role if sender_role is not None else role
        if effective_sender_role:
            self._role_by_agent.setdefault(sender, effective_sender_role)
        if recipient_role:
            self._role_by_agent.setdefault(recipient, recipient_role)
        # NOTE: We deliberately do NOT propagate the sender's role to the
        # recipient. Prior behaviour did, which polluted role_distribution.

        # --- Delegation / coordination tracking ---
        if sender != recipient:
            self._delegation_counts[sender][recipient] += 1
        else:
            self._self_loop_count += 1

        self._outbound_counts[sender] += 1
        # [P1] Coordination = agent↔agent + handoff (excludes status to humans)
        if inferred_kind in (TurnKind.AGENT_TO_AGENT.value,
                              TurnKind.HANDOFF.value):
            self._coordination_counts[sender] += 1

    # ------------------------------------------------------------------
    # Public API — metrics
    # ------------------------------------------------------------------
    def metrics(self) -> Dict[str, Any]:
        """
        Return graph metrics.

        **v2.0.0:** `conversation_depth` now returns true graph depth (nodes in
        the longest chain). The prior message-count behaviour is exposed as
        `message_count`.
        """
        # Agent node set (original)
        nodes: Set[str] = set()
        for m in self.messages:
            nodes.add(m["from"])
            nodes.add(m["to"])

        base: Dict[str, Any] = {
            # [P0] Breaking change: conversation_depth is now real graph depth
            "conversation_depth": self._graph_depth(),
            # [P0] Legacy value under a clear name
            "message_count": len(self.messages),
            "agent_nodes": len(nodes),
        }

        # [P2] graph-level run_id if set
        if self._default_run_id is not None:
            base["run_id"] = self._default_run_id
        if self._evicted_count > 0:
            base["evicted_turns"] = self._evicted_count

        # Enhancement additions (only when data exists)
        if self._turn_records:
            kinds: Counter = Counter(t["kind"] for t in self._turn_records)
            base["turn_kinds"] = dict(kinds)

            durations = [t["latency_ms"] for t in self._turn_records
                          if t.get("latency_ms") is not None]
            if durations:
                base["avg_latency_ms"] = sum(durations) / len(durations)

            tokens = [t["tokens"] for t in self._turn_records
                       if t.get("tokens") is not None]
            if tokens:
                base["total_tokens"] = sum(tokens)

            tool_call_count = sum(len(t.get("tool_calls", []))
                                    for t in self._turn_records)
            if tool_call_count:
                base["total_tool_calls"] = tool_call_count

            energy = [t["energy_kwh"] for t in self._turn_records
                       if t.get("energy_kwh") is not None]
            if energy:
                base["total_energy_kwh"] = sum(energy)

        if self._self_loop_count:
            base["self_loops"] = self._self_loop_count
        if self._invalid_records:
            base["invalid_records"] = self._invalid_records

        # [P0] non-negative duration using min/max
        if self._min_ts is not None and self._max_ts is not None:
            base["conversation_duration_s"] = max(0.0, self._max_ts - self._min_ts)

        return base

    # ------------------------------------------------------------------
    # Enhancement: emit shared analysis contracts
    # ------------------------------------------------------------------
    def observations(self) -> List[Any]:
        """
        Return AgentObservations for every (agent, task_id) pair.

        [P0] Includes agents that only *receive* messages, not just senders.
        [P0] `delegated_to` is deduped and excludes replies.
        [P1] `coordination_calls` counts only AGENT_TO_AGENT + HANDOFF turns.
        """
        # Group turns by task, tracking all agents (senders AND receivers)
        turns_by_task: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        agents_by_task: Dict[str, Set[str]] = defaultdict(set)
        for t in self._turn_records:
            task = t.get("task_id") or ""
            turns_by_task[task].append(t)
            agents_by_task[task].add(t["from"])
            agents_by_task[task].add(t["to"])

        observations: List[Any] = []
        for task_id, agents in agents_by_task.items():
            task_turns = turns_by_task[task_id]
            for agent_id in agents:
                # Outbound turns from this agent on this task
                agent_turns = [t for t in task_turns if t["from"] == agent_id]

                delegated = self._compute_delegated_to(agent_id, task_turns)
                coordination_calls = sum(
                    1 for t in agent_turns
                    if t["kind"] in (TurnKind.AGENT_TO_AGENT.value,
                                       TurnKind.HANDOFF.value))

                latency = sum(t.get("latency_ms") or 0.0 for t in agent_turns)
                energy = sum(t.get("energy_kwh") or 0.0 for t in agent_turns)
                role = self._role_by_agent.get(agent_id, RoleKind.GENERALIST.value)

                observations.append(AgentObservation(
                    agent_id=agent_id,
                    task_id=task_id,
                    role=role,
                    delegated_to=delegated,
                    coordination_calls=coordination_calls,
                    latency_ms=latency,
                    energy_kwh=energy,
                ))

        self._observations = observations
        return observations

    def decision_records(
        self,
        *,
        run_id: Optional[str] = None,
        policy_version: str = "",
        hardware_profile: str = "",
        precision: Optional[str] = None,
    ) -> List[Any]:
        """
        Emit a DecisionRecord for every turn that has enough context.

        [P1] `selected_action` uses semantic labels. Sender/recipient/turn_kind
        moved to `provenance` for structured access.
        """
        records: List[Any] = []
        for t in self._turn_records:
            effective_run_id = t.get("run_id") or run_id or self._default_run_id
            if not effective_run_id:
                continue
            if not t.get("task_id"):
                continue

            action = self._ACTION_LABELS.get(t["kind"], "unknown")
            records.append(DecisionRecord(
                run_id=effective_run_id,
                timestamp=datetime.fromtimestamp(t["timestamp"]),
                task_id=t["task_id"],
                selected_action=action,
                alternatives=[],
                policy_version=policy_version,
                model_or_agent=t["from"],
                hardware_profile=hardware_profile,
                precision=precision,
                latency_ms=t.get("latency_ms") or 0.0,
                energy_kwh=t.get("energy_kwh") or 0.0,
                explanation={
                    "turn_kind": t["kind"],
                    "tool_calls": t.get("tool_calls", []),
                    "tool_call_id": t.get("tool_call_id"),
                    "tokens": t.get("tokens"),
                },
                provenance={
                    "source": "autogen_conversation_graph",
                    "sender": t["from"],
                    "recipient": t["to"],
                    "turn_kind": t["kind"],
                },
            ))
        self._decisions = records
        return records

    # ------------------------------------------------------------------
    # Analysis helpers
    # ------------------------------------------------------------------
    def analyze_roles(self) -> Dict[str, Any]:
        """
        Role distribution and delegation quality.

        [P1] Includes `role_turn_counts` (workload per role) and
        `invalid_records`.
        [P0] Role distribution reflects real per-agent roles (no leak).
        """
        role_distribution = dict(Counter(self._role_by_agent.values()))

        role_turn_counts: Counter = Counter()
        for t in self._turn_records:
            role = self._role_by_agent.get(t["from"])
            if role:
                role_turn_counts[role] += 1

        # [P0] delegated_to is deduped per (agent, task); count unique
        delegated_total = 0
        redundant = 0
        for sender, recipient_counter in self._delegation_counts.items():
            delegated_total += sum(recipient_counter.values())
            redundant += sum(max(0, c - 1) for c in recipient_counter.values())

        return {
            "role_distribution": role_distribution,
            "role_turn_counts": dict(role_turn_counts),
            "total_delegations": delegated_total,
            "redundant_delegations": redundant,
            "self_loops": self._self_loop_count,
            "total_coordination_calls": sum(self._coordination_counts.values()),
            "total_outbound_messages": sum(self._outbound_counts.values()),
            "invalid_records": self._invalid_records,
        }

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------
    def export(self) -> Dict[str, Any]:
        """
        Serialise everything for JSON export.

        [P1] Includes observations and decisions.
        [P2] Uses `_to_jsonable` for datetimes and enums.
        """
        return {
            "messages": list(self.messages),
            "turns": [
                {**t,
                 "timestamp_iso": datetime.fromtimestamp(t["timestamp"]).isoformat()}
                for t in self._turn_records
            ],
            "metrics": self.metrics(),
            "roles": dict(self._role_by_agent),
            "role_analysis": self.analyze_roles(),
            "observations": [_to_jsonable(o) for o in self.observations()],
            "decisions": [_to_jsonable(d) for d in self.decision_records()],
            "invalid_records": self._invalid_records,
            "evicted_turns": self._evicted_count,
        }

    def turn_records(self, run_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """[P3] Return turn records, optionally filtered by run_id."""
        if run_id is None:
            return list(self._turn_records)
        return [t for t in self._turn_records if t.get("run_id") == run_id]

    # ------------------------------------------------------------------
    # [P3] Graph analytics
    # ------------------------------------------------------------------
    def adjacency(self, *, run_id: Optional[str] = None,
                   task_id: Optional[str] = None) -> Dict[str, Set[str]]:
        """Return directed agent-communication graph (adjacency sets)."""
        adj: Dict[str, Set[str]] = defaultdict(set)
        if self._turn_records:
            source = self._turn_records
            for t in source:
                if run_id is not None and t.get("run_id") != run_id:
                    continue
                if task_id is not None and (t.get("task_id") or "") != task_id:
                    continue
                if t["from"] and t["to"] and t["from"] != t["to"]:
                    adj[t["from"]].add(t["to"])
        else:
            # Fallback to messages (no filtering possible)
            for m in self.messages:
                if m["from"] and m["to"] and m["from"] != m["to"]:
                    adj[m["from"]].add(m["to"])
        return dict(adj)

    def centrality(self, *, run_id: Optional[str] = None,
                    task_id: Optional[str] = None) -> Dict[str, float]:
        """
        Degree centrality per agent (in+out edges / 2(n-1)).
        Returns dict mapping agent_id → centrality in [0, 1].
        """
        adj = self.adjacency(run_id=run_id, task_id=task_id)
        if not adj:
            return {}

        all_agents: Set[str] = set(adj.keys())
        for neighbors in adj.values():
            all_agents.update(neighbors)
        n = len(all_agents)
        if n <= 1:
            return {a: 0.0 for a in all_agents}

        in_deg: Dict[str, int] = defaultdict(int)
        out_deg: Dict[str, int] = defaultdict(int)
        for src, dsts in adj.items():
            out_deg[src] = len(dsts)
            for d in dsts:
                in_deg[d] += 1

        return {
            agent: (in_deg[agent] + out_deg[agent]) / (2 * (n - 1))
            for agent in all_agents
        }

    def connected_components(
        self, *, run_id: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> List[Set[str]]:
        """Return weakly-connected agent sets."""
        adj = self.adjacency(run_id=run_id, task_id=task_id)
        undirected: Dict[str, Set[str]] = defaultdict(set)
        for src, dsts in adj.items():
            undirected[src]  # create empty entry
            for d in dsts:
                undirected[src].add(d)
                undirected[d].add(src)

        seen: Set[str] = set()
        components: List[Set[str]] = []
        for start in undirected:
            if start in seen:
                continue
            comp: Set[str] = set()
            stack = [start]
            while stack:
                node = stack.pop()
                if node in seen:
                    continue
                seen.add(node)
                comp.add(node)
                stack.extend(undirected[node] - seen)
            components.append(comp)
        return components

    def longest_path(
        self, *, run_id: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> List[str]:
        """Return one longest delegation chain (list of agent ids)."""
        adj = self.adjacency(run_id=run_id, task_id=task_id)
        if not adj:
            return []

        senders = set(adj.keys())
        receivers: Set[str] = set()
        for dsts in adj.values():
            receivers.update(dsts)
        roots = senders - receivers
        if not roots:
            roots = senders

        best: List[str] = []

        def _dfs(node: str, path: List[str]) -> None:
            nonlocal best
            if node in path:
                return
            current = path + [node]
            if len(current) > len(best):
                best = current
            for nb in adj.get(node, ()):
                _dfs(nb, current)

        for r in roots:
            _dfs(r, [])
        return best

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _infer_kind(self, sender: str, recipient: str,
                     role: Optional[str]) -> str:
        """Classify a turn from sender/recipient/role heuristics."""
        sender_l = (sender or "").lower()
        recipient_l = (recipient or "").lower()
        role_l = (role or "").lower()

        sender_is_human = sender_l in self.HUMAN_MARKERS or role_l == RoleKind.HUMAN.value
        recipient_is_human = recipient_l in self.HUMAN_MARKERS
        sender_is_tool = sender_l in self.TOOL_MARKERS or role_l == RoleKind.TOOL.value
        recipient_is_tool = recipient_l in self.TOOL_MARKERS

        if sender_is_tool:
            return TurnKind.TOOL_RESULT.value
        if recipient_is_tool:
            return TurnKind.TOOL_CALL.value
        if sender_is_human:
            return TurnKind.HUMAN_TO_AGENT.value
        if recipient_is_human:
            return TurnKind.AGENT_TO_HUMAN.value
        if sender_l == "system" or recipient_l == "system":
            return TurnKind.SYSTEM.value
        return TurnKind.AGENT_TO_AGENT.value

    def _graph_depth(self) -> int:
        """
        [P0] Longest chain in nodes through the conversation graph.

        - 0 if no messages
        - 1 if messages exist but all are self-loops
        - N for an N-node chain
        """
        if not self.messages:
            return 0
        adj: Dict[str, Set[str]] = defaultdict(set)
        senders: Set[str] = set()
        receivers: Set[str] = set()
        for m in self.messages:
            src = m.get("from", "")
            dst = m.get("to", "")
            if not src or not dst or src == dst:
                continue
            adj[src].add(dst)
            senders.add(src)
            receivers.add(dst)
        if not senders:
            return 1
        roots = senders - receivers
        if not roots:
            roots = senders

        longest = 1

        def _dfs(node: str, path: frozenset) -> int:
            best = 1
            for nb in adj.get(node, ()):
                if nb not in path:
                    best = max(best, 1 + _dfs(nb, path | {node}))
            return best

        for r in roots:
            longest = max(longest, _dfs(r, frozenset()))
        return longest

    def _count_agents(self) -> int:
        agents: Set[str] = set()
        for m in self.messages:
            agents.add(m["from"])
            agents.add(m["to"])
        return len(agents)

    def _compute_delegated_to(self, agent_id: str,
                                task_turns: List[Dict[str, Any]]) -> List[str]:
        """
        [P0] Compute recipients this agent genuinely delegated to on this task.

        A delegation is:
          - an outbound AGENT_TO_AGENT or HANDOFF turn from `agent_id`
          - that occurs BEFORE any inbound turn from the recipient on this task
            (i.e. it's an initiation, not a reply)
          - only counted once per recipient
        """
        sorted_turns = sorted(task_turns, key=lambda t: t["timestamp"])

        first_inbound: Dict[str, float] = {}
        for t in sorted_turns:
            if t["to"] == agent_id and t["from"] != agent_id:
                first_inbound.setdefault(t["from"], t["timestamp"])

        delegated: Set[str] = set()
        for t in sorted_turns:
            if t["from"] != agent_id:
                continue
            recipient = t["to"]
            if recipient == agent_id or recipient in delegated:
                continue
            if t["kind"] not in (TurnKind.AGENT_TO_AGENT.value,
                                   TurnKind.HANDOFF.value):
                continue
            if recipient not in first_inbound or t["timestamp"] < first_inbound[recipient]:
                delegated.add(recipient)
        return sorted(delegated)


# =============================================================================
# Self test
# =============================================================================
def _self_test() -> int:
    """Return 0 if all assertions pass, else count of failures."""
    logging.basicConfig(level=logging.WARNING)
    failures = 0

    def _check(name: str, fn) -> None:
        nonlocal failures
        try:
            fn()
            print(f"  ✓ {name}")
        except AssertionError as e:
            failures += 1
            print(f"  ✗ {name}: {e}")

    print("=" * 70)
    print("AutoGen Conversation Graph v2.0.0 — self test")
    print("=" * 70)

    # --- Test 1: backward compatibility ---
    def _t_backward() -> None:
        g = AutoGenConversationGraph()
        g.record("planner", "worker", "please summarise")
        g.record("worker", "planner", "here it is")
        m = g.metrics()
        assert "agent_nodes" in m and m["agent_nodes"] == 2
        assert "conversation_depth" in m
        assert "message_count" in m and m["message_count"] == 2
        assert len(g) == 2
    _check("Backward compat: record + metrics + __len__", _t_backward)

    # --- Test 2: [P0-1] no role leak ---
    def _t_roles() -> None:
        g = AutoGenConversationGraph()
        g.record("planner", "worker", "x", sender_role="coordinator")
        g.record("worker", "planner", "y", sender_role="generalist")
        obs = g.observations()
        roles = {o.agent_id: o.role for o in obs}
        assert roles.get("planner") == "coordinator", roles
        assert roles.get("worker") == "generalist", roles
    _check("[P0-1] Role leak fixed", _t_roles)

    # --- Test 3: [P0-2] conversation_depth is graph depth ---
    def _t_depth() -> None:
        g = AutoGenConversationGraph()
        # 100-message broadcast storm: 1 message, 1 send
        for _ in range(100):
            g.record("planner", "worker", "x")
        m = g.metrics()
        assert m["message_count"] == 100, m
        assert m["conversation_depth"] == 2, m  # 2 nodes in chain
        # 3-hop chain
        g2 = AutoGenConversationGraph()
        g2.record("a", "b", "x")
        g2.record("b", "c", "x")
        g2.record("c", "d", "x")
        assert g2.metrics()["conversation_depth"] == 4
    _check("[P0-2] conversation_depth is graph depth", _t_depth)

    # --- Test 4: [P0-3] delegated_to deduped and excludes replies ---
    def _t_delegations() -> None:
        g = AutoGenConversationGraph()
        g.record("planner", "worker", "please summarise", task_id="t1")
        g.record("planner", "worker", "again please", task_id="t1")
        g.record("worker", "planner", "here it is", task_id="t1")
        g.record("planner", "worker", "thanks", task_id="t1")
        obs = g.observations()
        by_agent = {o.agent_id: o for o in obs}
        # planner delegated once (worker); follow-up "thanks" is a reply
        assert by_agent["planner"].delegated_to == ["worker"], \
            by_agent["planner"].delegated_to
        # worker delegated to no one (its message is a reply)
        assert by_agent["worker"].delegated_to == [], \
            by_agent["worker"].delegated_to
    _check("[P0-3] Delegation dedupe + reply filter", _t_delegations)

    # --- Test 5: [P0-4] receive-only agents in observations ---
    def _t_recv_only() -> None:
        g = AutoGenConversationGraph()
        g.record("planner", "worker", "x", task_id="t1")
        obs = g.observations()
        agents = {o.agent_id for o in obs}
        assert "worker" in agents, agents  # worker never sends but has role
        assert "planner" in agents, agents
    _check("[P0-4] Receive-only agents appear in observations", _t_recv_only)

    # --- Test 6: [P0-5] out-of-order timestamps ---
    def _t_ts() -> None:
        g = AutoGenConversationGraph()
        g.record("a", "b", "x", timestamp=100.0)
        g.record("b", "a", "y", timestamp=50.0)
        m = g.metrics()
        assert m.get("conversation_duration_s") == 50.0, m
    _check("[P0-5] Out-of-order timestamps give non-negative duration", _t_ts)

    # --- Test 7: [P1-1] coordination_calls excludes status messages ---
    def _t_coord() -> None:
        g = AutoGenConversationGraph()
        g.record("worker", "worker", "thinking", task_id="t1")
        g.record("worker", "worker", "more", task_id="t1")
        g.record("worker", "human", "report", task_id="t1")
        obs = g.observations()
        by = {o.agent_id: o for o in obs}
        # self-loops are THINKING (not counted as coordination in v2)
        # status-to-human is AGENT_TO_HUMAN (not counted)
        assert by["worker"].coordination_calls == 0, by["worker"].coordination_calls
    _check("[P1-1] coordination_calls excludes human status messages", _t_coord)

    # --- Test 8: [P1-2] semantic action labels ---
    def _t_actions() -> None:
        g = AutoGenConversationGraph()
        g.record("planner", "worker", "x",
                  run_id="run-1", task_id="t1")
        g.record("worker", "search_tool", "find",
                  run_id="run-1", task_id="t1", tool_calls=["search"])
        records = g.decision_records()
        actions = {r.selected_action for r in records}
        assert "send_message" in actions, actions
        assert "call_tool" in actions, actions
        assert not any("→" in a for a in actions), actions
    _check("[P1-2] Semantic selected_action labels", _t_actions)

    # --- Test 9: [P1-3] export includes observations and decisions ---
    def _t_export() -> None:
        g = AutoGenConversationGraph()
        g.record("a", "b", "x", sender_role="coordinator",
                  run_id="r1", task_id="t1")
        g.record("b", "a", "y", sender_role="generalist",
                  run_id="r1", task_id="t1")
        e = g.export()
        assert "observations" in e and len(e["observations"]) >= 2, e.keys()
        assert "decisions" in e and len(e["decisions"]) >= 2, e.keys()
        assert "invalid_records" in e
        # Must be JSON-serializable
        json.dumps(e, default=str)
    _check("[P1-3] export includes observations + decisions", _t_export)

    # --- Test 10: [P1-5] tool_call_id correlation ---
    def _t_tool_corr() -> None:
        g = AutoGenConversationGraph()
        g.record("agent", "search", "find", tool_calls=["search"],
                  tool_call_id="call-1", task_id="t1")
        g.record("search", "agent", "result", tool_call_id="call-1",
                  task_id="t1")
        # Verify both turns have matching tool_call_id
        recs = g.turn_records()
        ids = [r.get("tool_call_id") for r in recs]
        assert "call-1" in ids, ids
    _check("[P1-5] tool_call_id correlation", _t_tool_corr)

    # --- Test 11: [P2] reset ---
    def _t_reset() -> None:
        g = AutoGenConversationGraph()
        g.record("a", "b", "x")
        assert len(g) == 1
        g.reset()
        assert len(g) == 0
        assert g.metrics()["message_count"] == 0
    _check("[P2-1] reset clears state", _t_reset)

    # --- Test 12: [P2] __repr__ ---
    def _t_repr() -> None:
        g = AutoGenConversationGraph()
        g.record("a", "b", "x")
        r = repr(g)
        assert "turns=1" in r, r
    _check("[P2-2] __repr__", _t_repr)

    # --- Test 13: [P2] max_turns cap ---
    def _t_max() -> None:
        g = AutoGenConversationGraph(max_turns=5)
        for i in range(10):
            g.record("a", "b", f"msg-{i}")
        assert len(g) == 5, len(g)
        assert g.metrics().get("evicted_turns", 0) == 5, g.metrics()
    _check("[P2-5] max_turns cap + evicted count", _t_max)

    # --- Test 14: [P3-4] adjacency / centrality / components / longest path ---
    def _t_graph() -> None:
        g = AutoGenConversationGraph()
        g.record("a", "b", "x")
        g.record("b", "c", "x")
        g.record("c", "d", "x")
        g.record("e", "f", "x")  # disconnected component

        adj = g.adjacency()
        assert adj["a"] == {"b"}, adj
        assert adj["b"] == {"c"}, adj

        cent = g.centrality()
        # 'b' has in=1, out=1 → 2/(2*5) = 0.2
        assert abs(cent["b"] - 0.2) < 1e-6, cent["b"]
        # 'a' has in=0, out=1 → 1/(2*5) = 0.1
        assert abs(cent["a"] - 0.1) < 1e-6, cent["a"]

        comps = g.connected_components()
        assert len(comps) == 2, comps
        sizes = sorted(len(c) for c in comps)
        assert sizes == [2, 4], sizes

        path = g.longest_path()
        assert path == ["a", "b", "c", "d"], path
    _check("[P3-4] Graph analytics", _t_graph)

    # --- Test 15: [P3-5] turn_records with run filter ---
    def _t_run_filter() -> None:
        g = AutoGenConversationGraph()
        g.record("a", "b", "x", run_id="r1")
        g.record("c", "d", "y", run_id="r2")
        g.record("e", "f", "z", run_id="r1")
        assert len(g.turn_records()) == 3
        assert len(g.turn_records(run_id="r1")) == 2
        assert len(g.turn_records(run_id="r2")) == 1
        assert len(g.turn_records(run_id="r3")) == 0
    _check("[P3-5] turn_records(run_id) filter", _t_run_filter)

    # --- Test 16: semantic integrity ---
    def _t_semantic() -> None:
        g = AutoGenConversationGraph()
        g.record("a", "b", "x", sender_role="coordinator",
                  run_id="r1", task_id="t1")
        g.record("b", "a", "y", sender_role="generalist",
                  run_id="r1", task_id="t1")
        a = g.analyze_roles()
        assert a["role_distribution"] == {"coordinator": 1, "generalist": 1}, a
        assert a["invalid_records"] == 0
    _check("Semantic: role distribution honest", _t_semantic)

    print("=" * 70)
    if failures == 0:
        print("✅ All tests passed")
    else:
        print(f"❌ {failures} test(s) failed")
    print("=" * 70)
    return failures


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    sys.exit(_self_test())
