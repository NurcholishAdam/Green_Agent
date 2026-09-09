#!/usr/bin/env python3
"""
Multi‑Agent Coordination with Emergent Role Specialisation.

Dynamically assigns roles (LEADER, WORKER, VERIFIER, OBSERVER) to agents
based on capabilities, performance history, environmental factors, and task‑specific
skills. Includes agent communication, leadership election, persistence, XAI,
human approval, and chaos testing.
"""

import asyncio
import logging
import json
import os
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Deque, Callable, Tuple
from collections import defaultdict, deque
from datetime import datetime, timezone
from enum import Enum

logger = logging.getLogger(__name__)


class AgentRole(Enum):
    LEADER = "leader"
    WORKER = "worker"
    VERIFIER = "verifier"
    OBSERVER = "observer"
    COORDINATOR = "coordinator"


@dataclass
class AgentInfo:
    agent_id: str
    capabilities: Dict[str, float]
    current_role: AgentRole = AgentRole.OBSERVER
    performance_history: Deque[float] = field(default_factory=lambda: deque(maxlen=100))
    task_performance: Dict[str, Deque[float]] = field(
        default_factory=lambda: defaultdict(lambda: deque(maxlen=100))
    )
    last_active: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    is_active: bool = True
    messages: asyncio.Queue = field(default_factory=asyncio.Queue)
    role_history: List[Dict[str, Any]] = field(default_factory=list)
    explanation: str = ""

    def average_performance(self, task_type: Optional[str] = None) -> float:
        if task_type and task_type in self.task_performance and self.task_performance[task_type]:
            return sum(self.task_performance[task_type]) / len(self.task_performance[task_type])
        if self.performance_history:
            return sum(self.performance_history) / len(self.performance_history)
        return 0.0


class MultiAgentCoordinator:
    """
    Coordinates multiple agents with emergent specialisation based on:
    - static capabilities
    - task‑specific performance
    - environmental factors (carbon, helium, energy) affecting thresholds
    - leadership election and fault recovery
    - message passing between agents
    - persistence (via central Storage or JSON file)
    - explainability (XAI) for role changes
    - human approval hook for critical role changes
    - chaos testing methods
    """

    def __init__(
        self,
        storage: Optional[Any] = None,
        persistence_path: str = "multi_agent_coordinator_state.json",
        approval_callback: Optional[Callable[[str, AgentRole, str], bool]] = None,
    ):
        self.agents: Dict[str, AgentInfo] = {}
        self._lock = asyncio.Lock()
        self.leader_id: Optional[str] = None
        self.storage = storage
        self.persistence_path = persistence_path
        self.approval_callback = approval_callback

        # Environmental factors adjust role thresholds
        self.environment_factors: Dict[str, float] = {
            'carbon': 400.0,   # gCO₂/kWh
            'helium': 0.5,     # scarcity index 0-1
            'energy': 0.5,     # energy availability index 0-1
        }

        # Baseline thresholds (will be adjusted by environment)
        self.base_thresholds = {
            'trust': 0.8,
            'compute': 0.7,
            'energy': 0.5,
            'min_performance': 0.7,
            'min_history': 10,
        }

        # Load persisted state if available
        self._load_state_sync()

    # ------------------ State Persistence ------------------
    async def save_state(self):
        async with self._lock:
            state = {
                'leader_id': self.leader_id,
                'environment_factors': self.environment_factors,
                'agents': {}
            }
            for aid, info in self.agents.items():
                state['agents'][aid] = {
                    'capabilities': info.capabilities,
                    'current_role': info.current_role.value,
                    'performance_history': list(info.performance_history),
                    'task_performance': {k: list(v) for k, v in info.task_performance.items()},
                    'last_active': info.last_active.isoformat(),
                    'is_active': info.is_active,
                    'role_history': info.role_history,
                    'explanation': info.explanation,
                }
            if self.storage:
                # Use central Storage if available
                self.storage.save_state("multi_agent_coordinator", json.dumps(state))
            else:
                # Fallback to JSON file
                with open(self.persistence_path, 'w') as f:
                    json.dump(state, f, indent=2)

    def _load_state_sync(self):
        """Synchronous load (called in __init__)."""
        try:
            state = None
            if self.storage:
                raw = self.storage.get_state("multi_agent_coordinator")
                if raw:
                    state = json.loads(raw)
            else:
                if os.path.exists(self.persistence_path):
                    with open(self.persistence_path, 'r') as f:
                        state = json.load(f)
            if state:
                self.leader_id = state.get('leader_id')
                self.environment_factors.update(state.get('environment_factors', {}))
                agents_data = state.get('agents', {})
                for aid, data in agents_data.items():
                    info = AgentInfo(
                        agent_id=aid,
                        capabilities=data.get('capabilities', {}),
                        current_role=AgentRole(data.get('current_role', 'observer')),
                        last_active=datetime.fromisoformat(data.get('last_active', datetime.now(timezone.utc).isoformat())),
                        is_active=data.get('is_active', True),
                        explanation=data.get('explanation', ''),
                    )
                    info.performance_history = deque(data.get('performance_history', []), maxlen=100)
                    for task, perf_list in data.get('task_performance', {}).items():
                        info.task_performance[task] = deque(perf_list, maxlen=100)
                    info.role_history = data.get('role_history', [])
                    self.agents[aid] = info
        except Exception as e:
            logger.warning(f"Failed to load coordinator state: {e}")

    # ------------------ Agent Registration ------------------
    async def register_agent(self, agent_id: str, capabilities: Dict[str, float]) -> bool:
        async with self._lock:
            if agent_id in self.agents:
                return False
            self.agents[agent_id] = AgentInfo(agent_id=agent_id, capabilities=capabilities)
            logger.info(f"Registered agent {agent_id}")
            await self._reassign_roles()
            await self.save_state()
            return True

    # ------------------ Performance Update ------------------
    async def update_agent_performance(self, agent_id: str, task_type: str, performance: float) -> None:
        async with self._lock:
            if agent_id not in self.agents:
                return
            agent = self.agents[agent_id]
            agent.performance_history.append(performance)
            agent.task_performance[task_type].append(performance)
            agent.last_active = datetime.now(timezone.utc)
            await self._reassign_roles()
            await self.save_state()

    # ------------------ Environment Factor Update ------------------
    async def set_environment_factor(self, name: str, value: float):
        async with self._lock:
            self.environment_factors[name] = value
            await self._reassign_roles()
            await self.save_state()

    # ------------------ Role Assignment ------------------
    async def _reassign_roles(self):
        """Assign roles based on adjusted thresholds and election logic."""
        if not self.agents:
            return

        # Adjust thresholds based on environmental factors
        adj = self.base_thresholds.copy()
        # Higher carbon -> prefer energy-efficient workers, lower energy threshold
        if self.environment_factors['carbon'] > 500:
            adj['energy'] = 0.4  # require higher energy efficiency
        # Higher helium scarcity -> prefer verifiers with high trust
        if self.environment_factors['helium'] > 0.7:
            adj['trust'] = 0.9
        # Lower energy availability -> make worker requirement stricter
        if self.environment_factors['energy'] < 0.3:
            adj['energy'] = 0.6

        # First pass: assign non-leader roles based on current metrics
        for agent in self.agents.values():
            if not agent.is_active:
                agent.current_role = AgentRole.OBSERVER
                agent.explanation = "Agent inactive; assigned OBSERVER."
                continue

            trust = agent.capabilities.get('trust', 0.5)
            compute = agent.capabilities.get('compute', 0.5)
            energy = agent.capabilities.get('energy', 0.5)
            avg_perf = agent.average_performance()
            history_len = len(agent.performance_history)

            # Determine candidate roles
            if trust >= adj['trust'] and avg_perf >= adj['min_performance']:
                new_role = AgentRole.VERIFIER
            elif compute >= adj['compute'] and history_len >= adj['min_history']:
                new_role = AgentRole.LEADER
            elif energy >= adj['energy']:
                new_role = AgentRole.WORKER
            else:
                new_role = AgentRole.OBSERVER

            # If agent is currently a LEADER, keep it unless it no longer meets criteria
            if agent.current_role == AgentRole.LEADER and new_role != AgentRole.LEADER:
                # Leader demotion possible; we'll handle in election
                pass

            agent.current_role = new_role
            agent.explanation = self._generate_role_explanation(agent, new_role, adj)

        # Second pass: elect a single leader among agents with LEADER role
        leaders = [agent for agent in self.agents.values() if agent.current_role == AgentRole.LEADER and agent.is_active]
        if leaders:
            # Choose best leader based on combined score (compute, trust, performance)
            best_leader = max(
                leaders,
                key=lambda a: (a.capabilities.get('compute', 0) * 0.4 +
                               a.capabilities.get('trust', 0) * 0.3 +
                               a.average_performance() * 0.3)
            )
            # Demote other leaders to WORKER
            for leader in leaders:
                if leader.agent_id != best_leader.agent_id:
                    leader.current_role = AgentRole.WORKER
                    leader.explanation = f"Not elected leader; assigned WORKER."
            # Ensure best_leader is set as current leader
            best_leader.current_role = AgentRole.LEADER
            best_leader.explanation = "Elected as LEADER based on compute, trust, and performance."
            self.leader_id = best_leader.agent_id
        else:
            # If no agent has LEADER role but there are active agents, elect one as leader
            active_agents = [a for a in self.agents.values() if a.is_active]
            if active_agents and self.leader_id is None:
                # Choose the best candidate for leader even if not initially assigned
                best = max(
                    active_agents,
                    key=lambda a: (a.capabilities.get('compute', 0) * 0.4 +
                                   a.capabilities.get('trust', 0) * 0.3 +
                                   a.average_performance() * 0.3)
                )
                best.current_role = AgentRole.LEADER
                best.explanation = "No leader existed; elected as LEADER."
                self.leader_id = best.agent_id
            elif self.leader_id and self.leader_id in self.agents and not self.agents[self.leader_id].is_active:
                # Leader inactive; elect new
                self.leader_id = None
                best = max(
                    active_agents,
                    key=lambda a: (a.capabilities.get('compute', 0) * 0.4 +
                                   a.capabilities.get('trust', 0) * 0.3 +
                                   a.average_performance() * 0.3)
                )
                best.current_role = AgentRole.LEADER
                best.explanation = "Previous leader inactive; elected as LEADER."
                self.leader_id = best.agent_id

    def _generate_role_explanation(self, agent: AgentInfo, role: AgentRole, adj: Dict[str, float]) -> str:
        """Generate a human-readable explanation for the role assignment."""
        trust = agent.capabilities.get('trust', 0)
        compute = agent.capabilities.get('compute', 0)
        energy = agent.capabilities.get('energy', 0)
        avg_perf = agent.average_performance()
        history_len = len(agent.performance_history)

        reasons = []
        if role == AgentRole.VERIFIER:
            reasons.append(f"trust {trust:.2f} >= {adj['trust']:.2f}")
            reasons.append(f"avg_perf {avg_perf:.2f} >= {adj['min_performance']:.2f}")
        elif role == AgentRole.LEADER:
            reasons.append(f"compute {compute:.2f} >= {adj['compute']:.2f}")
            reasons.append(f"history {history_len} >= {adj['min_history']}")
        elif role == AgentRole.WORKER:
            reasons.append(f"energy {energy:.2f} >= {adj['energy']:.2f}")
        else:
            reasons.append("no criteria met")
        return f"Role {role.value} assigned because " + "; ".join(reasons)

    # ------------------ Leadership Election ------------------
    async def elect_leader(self) -> Optional[str]:
        """Force a new leader election."""
        async with self._lock:
            self.leader_id = None
            await self._reassign_roles()
            await self.save_state()
            return self.leader_id

    # ------------------ Agent Communication ------------------
    async def send_message(self, sender_id: str, recipient_id: str, message: Dict[str, Any]) -> bool:
        """Send a message from one agent to another."""
        async with self._lock:
            if sender_id not in self.agents or recipient_id not in self.agents:
                return False
            await self.agents[recipient_id].messages.put({
                'from': sender_id,
                'message': message,
                'timestamp': datetime.now(timezone.utc).isoformat(),
            })
            return True

    async def receive_messages(self, agent_id: str) -> List[Dict[str, Any]]:
        """Retrieve all pending messages for an agent."""
        if agent_id not in self.agents:
            return []
        messages = []
        while not self.agents[agent_id].messages.empty():
            msg = self.agents[agent_id].messages.get_nowait()
            messages.append(msg)
        return messages

    # ------------------ Role Query ------------------
    async def get_agents_by_role(self, role: AgentRole) -> List[str]:
        async with self._lock:
            return [aid for aid, info in self.agents.items() if info.current_role == role]

    async def get_coordination_summary(self) -> Dict[str, int]:
        async with self._lock:
            summary = defaultdict(int)
            for info in self.agents.values():
                summary[info.current_role.value] += 1
            return dict(summary)

    # ------------------ XAI: Role Assignment Explanation ------------------
    async def explain_role(self, agent_id: str) -> Optional[str]:
        async with self._lock:
            agent = self.agents.get(agent_id)
            if agent:
                return agent.explanation
            return None

    # ------------------ Human Approval ------------------
    def set_approval_callback(self, callback: Callable[[str, AgentRole, str], bool]):
        self.approval_callback = callback

    async def request_approval(self, agent_id: str, new_role: AgentRole, reason: str) -> bool:
        """
        Request human approval for a critical role change.
        Returns True if approved or no callback set.
        """
        if self.approval_callback is None:
            return True
        result = self.approval_callback(agent_id, new_role, reason)
        if asyncio.iscoroutine(result):
            return await result
        return bool(result)

    # ------------------ Chaos Testing ------------------
    async def inject_fault(self, fault_type: str, agent_id: Optional[str] = None):
        """
        Inject a fault to test resilience.
        Supported faults:
          - 'agent_failure': mark an agent as inactive.
          - 'agent_recovery': reactivate an agent.
          - 'environment_spike': set carbon intensity to high value.
          - 'environment_normal': reset environment factors to defaults.
          - 'clear_leader': remove current leader.
        """
        async with self._lock:
            if fault_type == 'agent_failure' and agent_id and agent_id in self.agents:
                self.agents[agent_id].is_active = False
                logger.warning(f"Injected agent_failure on {agent_id}")
                await self._reassign_roles()
            elif fault_type == 'agent_recovery' and agent_id and agent_id in self.agents:
                self.agents[agent_id].is_active = True
                self.agents[agent_id].last_active = datetime.now(timezone.utc)
                logger.warning(f"Injected agent_recovery on {agent_id}")
                await self._reassign_roles()
            elif fault_type == 'environment_spike':
                self.environment_factors['carbon'] = 800.0
                self.environment_factors['helium'] = 0.9
                logger.warning("Injected environment_spike")
                await self._reassign_roles()
            elif fault_type == 'environment_normal':
                self.environment_factors = {'carbon': 400.0, 'helium': 0.5, 'energy': 0.5}
                logger.warning("Injected environment_normal")
                await self._reassign_roles()
            elif fault_type == 'clear_leader':
                if self.leader_id and self.leader_id in self.agents:
                    self.agents[self.leader_id].is_active = False
                self.leader_id = None
                logger.warning("Injected clear_leader")
                await self._reassign_roles()
            else:
                logger.warning(f"Unknown fault type: {fault_type}")
            await self.save_state()

    async def run_chaos_test(self) -> Dict[str, Any]:
        """Run a simple chaos test to verify resilience."""
        report = {'faults': [], 'results': {}}

        # Test agent failure
        if self.agents:
            test_agent = next(iter(self.agents))
            await self.inject_fault('agent_failure', agent_id=test_agent)
            report['faults'].append('agent_failure')
            summary_after_failure = await self.get_coordination_summary()
            report['results']['agent_failure'] = summary_after_failure

            # Test agent recovery
            await self.inject_fault('agent_recovery', agent_id=test_agent)
            report['faults'].append('agent_recovery')
            summary_after_recovery = await self.get_coordination_summary()
            report['results']['agent_recovery'] = summary_after_recovery

        # Test environment spike
        await self.inject_fault('environment_spike')
        report['faults'].append('environment_spike')
        # Check if roles changed appropriately (e.g., more verifiers due to high trust threshold)
        summary_after_spike = await self.get_coordination_summary()
        report['results']['environment_spike'] = summary_after_spike

        # Reset environment
        await self.inject_fault('environment_normal')
        report['faults'].append('environment_normal')
        summary_after_reset = await self.get_coordination_summary()
        report['results']['environment_normal'] = summary_after_reset

        return report

    # ------------------ Shutdown ------------------
    async def shutdown(self):
        await self.save_state()
        logger.info("MultiAgentCoordinator shutdown complete")
