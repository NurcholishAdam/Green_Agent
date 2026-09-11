"""
multi_agent_coordinator.py
Advanced multi-agent coordination for MOPD.
"""
from __future__ import annotations
import asyncio
import random
import uuid
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple


class _Agent:
    ROLES = ["orchestrator", "validator", "optimizer", "reporter", "negotiator"]

    def __init__(self, agent_id: str):
        self.id = agent_id
        self.role = "validator"
        self.reputation = 0.5
        self.utilities = {r: random.uniform(0.3, 0.7) for r in self.ROLES}
        self.completed = 0


class MultiAgentCoordinator:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        count = getattr(config, "agent_count", 5)
        self.agents = {f"agent_{i:02d}": _Agent(f"agent_{i:02d}") for i in range(count)}
        self.bus: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._lock = asyncio.Lock()

    async def _specialise(self) -> None:
        async with self._lock:
            for a in self.agents.values():
                a.role = max(a.utilities, key=lambda r: a.utilities[r])
                await asyncio.to_thread(
                    self.storage.save_agent, a.id, a.role,
                    a.reputation, a.utilities)

    async def broadcast(self, topic: str, sender: str, payload: Dict[str, Any]) -> None:
        try:
            self.bus.put_nowait({"topic": topic, "sender": sender, "payload": payload})
        except asyncio.QueueFull:
            pass
        await asyncio.to_thread(
            self.storage.save_agent_message,
            uuid.uuid4().hex[:8], topic, sender, "*", payload)

    async def bid(self, task: Dict[str, Any]) -> Tuple[str, float]:
        preferred = task.get("preferred_role", "orchestrator")
        best_id, best_score = None, -1.0
        async with self._lock:
            for aid, a in self.agents.items():
                bonus = 1.0 if a.role == preferred else 0.6
                score = a.utilities[a.role] * bonus + 0.3 * a.reputation
                score += random.uniform(-0.02, 0.02)
                if score > best_score:
                    best_score, best_id = score, aid
            if best_id:
                self.agents[best_id].completed += 1
        await self.broadcast("task_bid", best_id or "none",
                             {"task": task.get("name", "?"), "score": best_score})
        return best_id or next(iter(self.agents)), best_score

    async def reward(self, agent_id: str, reward: float) -> None:
        async with self._lock:
            if agent_id in self.agents:
                a = self.agents[agent_id]
                n = max(1, a.completed)
                a.reputation = max(0.0, min(1.0, a.reputation + reward / n))
                a.utilities[a.role] = min(1.0, a.utilities[a.role] + 0.05 * reward)

    def get_policy(self) -> List[float]:
        affinity = {
            "orchestrator": [0.35, 0.20, 0.15, 0.15, 0.15],
            "validator":    [0.15, 0.15, 0.15, 0.35, 0.20],
            "optimizer":    [0.20, 0.15, 0.35, 0.15, 0.15],
            "reporter":     [0.15, 0.20, 0.15, 0.15, 0.35],
            "negotiator":   [0.15, 0.35, 0.20, 0.15, 0.15],
        }
        counts = defaultdict(int)
        for a in self.agents.values():
            counts[a.role] += 1
        total = max(1, sum(counts.values()))
        out = [0.0] * 5
        for role, c in counts.items():
            w = c / total
            for i, v in enumerate(affinity.get(role, [0.2] * 5)):
                out[i] += w * v
        s = sum(out) or 1.0
        return [x / s for x in out]

    async def step(self) -> Dict[str, Any]:
        await self._specialise()
        processed = 0
        while not self.bus.empty():
            try:
                self.bus.get_nowait()
                processed += 1
            except asyncio.QueueEmpty:
                break
        return {"roles": {a.id: a.role for a in self.agents.values()},
                "role_distribution": self.get_policy(),
                "processed_messages": processed}
