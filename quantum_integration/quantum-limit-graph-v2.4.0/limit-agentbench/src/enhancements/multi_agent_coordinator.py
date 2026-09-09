#!/usr/bin/env python3
"""
Multi‑Agent Coordination with Emergent Role Specialisation.

Dynamically assigns roles (LEADER, WORKER, VERIFIER, OBSERVER) to agents
based on capabilities and performance history.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Deque
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
    last_active: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class MultiAgentCoordinator:
    def __init__(self):
        self.agents: Dict[str, AgentInfo] = {}
        self._lock = asyncio.Lock()

    async def register_agent(self, agent_id: str, capabilities: Dict[str, float]) -> bool:
        async with self._lock:
            if agent_id in self.agents:
                return False
            self.agents[agent_id] = AgentInfo(agent_id=agent_id, capabilities=capabilities)
            logger.info(f"Registered agent {agent_id}")
            await self._reassign_roles()
            return True

    async def update_agent_performance(self, agent_id: str, performance: float) -> None:
        async with self._lock:
            if agent_id not in self.agents:
                return
            self.agents[agent_id].performance_history.append(performance)
            self.agents[agent_id].last_active = datetime.now(timezone.utc)
            await self._reassign_roles()

    async def _reassign_roles(self) -> None:
        for agent in self.agents.values():
            avg_perf = sum(agent.performance_history) / len(agent.performance_history) if agent.performance_history else 0.0
            caps = agent.capabilities
            trust = caps.get('trust', 0.5)
            compute = caps.get('compute', 0.5)
            energy = caps.get('energy', 0.5)

            if trust > 0.8 and avg_perf > 0.7:
                agent.current_role = AgentRole.VERIFIER
            elif compute > 0.7 and len(agent.performance_history) > 10:
                agent.current_role = AgentRole.LEADER
            elif energy > 0.5:
                agent.current_role = AgentRole.WORKER
            else:
                agent.current_role = AgentRole.OBSERVER

    async def get_agents_by_role(self, role: AgentRole) -> List[str]:
        async with self._lock:
            return [aid for aid, info in self.agents.items() if info.current_role == role]

    async def get_coordination_summary(self) -> Dict[str, int]:
        async with self._lock:
            summary = defaultdict(int)
            for info in self.agents.values():
                summary[info.current_role.value] += 1
            return dict(summary)
