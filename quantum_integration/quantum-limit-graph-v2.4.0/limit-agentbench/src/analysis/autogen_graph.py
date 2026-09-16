# src/analysis/adapters/autogen_conversation_graph.py

"""
AutoGen conversation graph adapter (Enhanced)
==============================================

Extracts message-graph metrics from AutoGen runs and emits the shared
analysis-layer contracts (`DecisionRecord`, `AgentObservation`) so that
`MultiAgentRoleAnalyzer` and `HumanFeedbackAnalyzer` can consume them.

Original API preserved:
    graph = AutoGenConversationGraph()
    graph.record(sender, recipient, content)
    graph.metrics()   # {"conversation_depth": int, "agent_nodes": int}

Enhanced API:
    graph.record(sender, recipient, content,
                 role=None, kind=None, timestamp=None,
                 tool_calls=None, tokens=None,
                 latency_ms=None, energy_kwh=None,
                 run_id=None, task_id=None)
    graph.observations()   # -> list[AgentObservation]
    graph.decision_records()  # -> list[DecisionRecord]
    graph.analyze_roles()  # -> role distribution + delegation quality
    graph.export()         # -> dict for JSON serialisation
"""

from __future__ import annotations

import logging
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

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
    UNKNOWN = "unknown"


class RoleKind(Enum):
    """Recognised conversation roles (matches analysis.RoleKind where possible)."""
    GENERALIST = "generalist"
    ENERGY_SPECIALIST = "energy_specialist"
    CARBON_SPECIALIST = "carbon_specialist"
    HELIUM_SPECIALIST = "helium_specialist"
    LATENCY_SPECIALIST = "latency_specialist"
    COORDINATOR = "coordinator"
    DELEGATOR = "delegator"
    HUMAN = "human"
    TOOL = "tool"


# =============================================================================
# Shared contracts — lazily imported to avoid hard dependency on analysis/__init__
# =============================================================================

def _try_import_contracts():
    """
    Import DecisionRecord and AgentObservation from the analysis package
    if available; otherwise fall back to minimal inline dataclasses.
    """
    try:
        from src.analysis import DecisionRecord, AgentObservation  # type: ignore
        return DecisionRecord, AgentObservation, True
    except Exception:
        try:
            from analysis import DecisionRecord, AgentObservation  # type: ignore
            return DecisionRecord, AgentObservation, True
        except Exception:
            pass

    # Fallback dataclasses (shape-compatible with analysis contracts)
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
# Enhanced AutoGenConversationGraph
# =============================================================================

class AutoGenConversationGraph:
    """
    Enhanced AutoGen conversation-graph adapter.

    Backward-compatible with the original two-method API.
    """

    # Roles considered "human" for turn classification
    HUMAN_MARKERS = {"human", "user", "reviewer", "operator"}
    # Roles considered tools
    TOOL_MARKERS = {"tool", "function", "executor", "retriever"}

    def __init__(self):
        # --- Original state ---
        self.messages: List[Dict[str, Any]] = []

        # --- Enhancement state ---
        self._turn_records: List[Dict[str, Any]] = []
        self._observations: List[Any] = []          # list[AgentObservation]
        self._decisions: List[Any] = []             # list[DecisionRecord]
        self._role_by_agent: Dict[str, str] = {}
        self._delegation_counts: Dict[str, Counter] = defaultdict(Counter)
        self._coordination_calls: Dict[str, int] = defaultdict(int)
        self._self_loop_count: int = 0
        self._invalid_records: int = 0
        self._first_ts: Optional[float] = None
        self._last_ts: Optional[float] = None

    # ------------------------------------------------------------------
    # Original public API
    # ------------------------------------------------------------------

    def record(
        self,
        sender: str,
        recipient: str,
        content: str,
        *,
        # --- New optional kwargs (all default to None/empty) ---
        role: Optional[str] = None,
        kind: Optional[str] = None,
        timestamp: Optional[float] = None,
        tool_calls: Optional[List[str]] = None,
        tokens: Optional[int] = None,
        latency_ms: Optional[float] = None,
        energy_kwh: Optional[float] = None,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> None:
        """
        Record a single conversation turn.

        Original behavior: appends {"from", "to", "content"}.
        Enhanced: also classifies the turn, tracks tool calls, and can emit
        shared analysis contracts when the caller provides enough context.
        """
        # --- Input validation ---
        if not sender or not recipient:
            self._invalid_records += 1
            logger.warning(
                f"Invalid AutoGen turn (sender={sender!r}, recipient={recipient!r}); skipped"
            )
            return

        # --- Original record (preserved exactly) ---
        self.messages.append({
            "from": sender,
            "to": recipient,
            "content": content,
        })

        # --- Enhancement: timestamp tracking ---
        ts = float(timestamp) if timestamp is not None else time.time()
        if self._first_ts is None:
            self._first_ts = ts
        self._last_ts = ts

        # --- Enhancement: classify turn kind ---
        inferred_kind = kind or self._infer_kind(sender, recipient, role)
        self._turn_records.append({
            "from": sender,
            "to": recipient,
            "kind": inferred_kind,
            "role": role,
            "timestamp": ts,
            "tool_calls": list(tool_calls or []),
            "tokens": tokens,
            "latency_ms": latency_ms,
            "energy_kwh": energy_kwh,
            "run_id": run_id,
            "task_id": task_id,
        })

        # --- Enhancement: track roles per agent ---
        if role:
            self._role_by_agent[sender] = role
            self._role_by_agent[recipient] = self._role_by_agent.get(
                recipient, role
            )

        # --- Enhancement: track delegation and coordination ---
        if sender != recipient:
            self._delegation_counts[sender][recipient] += 1
        else:
            self._self_loop_count += 1

        # Coordination overhead: every outbound message is a coordination call
        self._coordination_calls[sender] += 1

    def metrics(self) -> Dict[str, Any]:
        """
        Return the two original metrics.

        Backward-compatible: always returns at least
        {"conversation_depth": int, "agent_nodes": int}.

        When enhancements have been used (timestamps, tool calls, delegation),
        additional keys are added.
        """
        # --- Original computation (preserved exactly) ---
        nodes = set()
        for m in self.messages:
            nodes.add(m["from"])
            nodes.add(m["to"])

        base = {
            "conversation_depth": len(self.messages),
            "agent_nodes": len(nodes),
        }

        # --- Enhancement additions (only when data exists) ---
        if self._turn_records:
            kinds: Counter = Counter(t["kind"] for t in self._turn_records)
            base["turn_kinds"] = dict(kinds)

            durations = [
                t["latency_ms"] for t in self._turn_records
                if t.get("latency_ms") is not None
            ]
            if durations:
                base["avg_latency_ms"] = sum(durations) / len(durations)

            tokens = [
                t["tokens"] for t in self._turn_records
                if t.get("tokens") is not None
            ]
            if tokens:
                base["total_tokens"] = sum(tokens)

            tool_call_count = sum(
                len(t.get("tool_calls", []))
                for t in self._turn_records
            )
            if tool_call_count:
                base["total_tool_calls"] = tool_call_count

            energy = [
                t["energy_kwh"] for t in self._turn_records
                if t.get("energy_kwh") is not None
            ]
            if energy:
                base["total_energy_kwh"] = sum(energy)

        if self._self_loop_count:
            base["self_loops"] = self._self_loop_count
        if self._invalid_records:
            base["invalid_records"] = self._invalid_records
        if self._first_ts is not None and self._last_ts is not None:
            base["conversation_duration_s"] = self._last_ts - self._first_ts

        return base

    # ------------------------------------------------------------------
    # Enhancement: emit shared analysis contracts
    # ------------------------------------------------------------------

    def observations(self) -> List[Any]:
        """
        Return AgentObservations for every (sender, task_id) pair.

        This is the primary output for `MultiAgentRoleAnalyzer`.
        """
        # Group turns by (sender, task_id)
        grouped: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
        for t in self._turn_records:
            key = (t["from"], t.get("task_id") or "")
            grouped[key].append(t)

        observations: List[Any] = []
        for (agent_id, task_id), turns in grouped.items():
            delegated_to: List[str] = []
            for t in turns:
                recipient = t["to"]
                # A "delegation" is any turn where the agent hands work off
                if recipient != agent_id and t["kind"] in (
                    TurnKind.AGENT_TO_AGENT.value,
                    TurnKind.AGENT_TO_HUMAN.value,
                ):
                    delegated_to.append(recipient)

            coordination_calls = len(turns)
            latency_ms = sum(
                t["latency_ms"] or 0.0 for t in turns
            )
            energy_kwh = sum(
                t["energy_kwh"] or 0.0 for t in turns
            )
            role = self._role_by_agent.get(agent_id, RoleKind.GENERALIST.value)

            observations.append(AgentObservation(
                agent_id=agent_id,
                task_id=task_id,
                role=role,
                delegated_to=delegated_to,
                coordination_calls=coordination_calls,
                latency_ms=latency_ms,
                energy_kwh=energy_kwh,
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

        Turns lacking run_id/task_id are skipped (no synthetic identifiers
        are fabricated).
        """
        records: List[Any] = []
        for t in self._turn_records:
            if not t.get("run_id") and not run_id:
                continue
            if not t.get("task_id"):
                continue
            records.append(DecisionRecord(
                run_id=t.get("run_id") or run_id or "",
                timestamp=datetime.fromtimestamp(t["timestamp"]),
                task_id=t["task_id"],
                selected_action=f"{t['from']}→{t['to']}:{t['kind']}",
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
                    "tokens": t.get("tokens"),
                },
                provenance={
                    "source": "autogen_conversation_graph",
                    "recipient": t["to"],
                },
            ))
        self._decisions = records
        return records

    def analyze_roles(self) -> Dict[str, Any]:
        """
        Convenience analysis compatible with `MultiAgentRoleAnalyzer`.

        Returns role distribution and delegation quality without requiring
        an external analyzer instance.
        """
        role_distribution = dict(Counter(self._role_by_agent.values()))
        total_delegations = sum(
            sum(counter.values())
            for counter in self._delegation_counts.values()
        )
        redundant = sum(
            max(0, count - 1)
            for counter in self._delegation_counts.values()
            for count in counter.values()
        )
        return {
            "role_distribution": role_distribution,
            "total_delegations": total_delegations,
            "redundant_delegations": redundant,
            "self_loops": self._self_loop_count,
            "total_coordination_calls": sum(self._coordination_calls.values()),
        }

    def export(self) -> Dict[str, Any]:
        """Serialise everything for JSON export."""
        return {
            "messages": list(self.messages),
            "turns": list(self._turn_records),
            "metrics": self.metrics(),
            "roles": dict(self._role_by_agent),
            "role_analysis": self.analyze_roles(),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _infer_kind(
        self,
        sender: str,
        recipient: str,
        role: Optional[str],
    ) -> str:
        """Classify a turn based on sender/recipient/role heuristics."""
        sender_l = (sender or "").lower()
        recipient_l = (recipient or "").lower()
        role_l = (role or "").lower()

        sender_is_human = (
            sender_l in self.HUMAN_MARKERS or role_l == RoleKind.HUMAN.value
        )
        recipient_is_human = recipient_l in self.HUMAN_MARKERS
        sender_is_tool = (
            sender_l in self.TOOL_MARKERS or role_l == RoleKind.TOOL.value
        )
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


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    g = AutoGenConversationGraph()

    # Original API
    g.record("planner", "worker", "please summarise")
    g.record("worker", "planner", "here it is")
    print("Original metrics:", g.metrics())

    # Enhanced API
    g.record(
        "planner", "worker", "refine the summary",
        role="coordinator", run_id="run-001", task_id="t1",
        latency_ms=42.0, energy_kwh=0.0008, tokens=120,
    )
    g.record(
        "worker", "tool_search", "search for details",
        role="generalist", run_id="run-001", task_id="t1",
        tool_calls=["web_search"], latency_ms=15.0, tokens=30,
    )
    g.record(
        "tool_search", "worker", "results...",
        latency_ms=210.0, energy_kwh=0.0003,
    )
    g.record(
        "worker", "human", "please confirm",
        run_id="run-001", task_id="t1",
        latency_ms=5.0,
    )
    g.record(
        "human", "worker", "approved",
        latency_ms=3.0,
    )

    print("\nEnhanced metrics:", g.metrics())
    print("\nRole analysis:", g.analyze_roles())

    print("\nObservations:")
    for o in g.observations():
        print(f"  {o}")

    print("\nDecision records:")
    for r in g.decision_records(run_id="run-001"):
        print(f"  {r.selected_action} | latency={r.latency_ms}ms "
              f"| energy={r.energy_kwh}kWh")
