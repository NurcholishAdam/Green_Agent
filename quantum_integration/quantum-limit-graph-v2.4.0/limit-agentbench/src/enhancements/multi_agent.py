"""
multi_agent.py — Multi-agent coordination (Enhancement 4).

Emergent role specialisation based on which role actually contributed the
winning bid; epsilon-greedy role sampling; carbon-weighted voting.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np


class AgentRole(str, Enum):
    EXPLORER = "explorer"
    EXPLOITER = "exploiter"
    SAFETY_OFFICER = "safety_officer"
    CARBON_BROKER = "carbon_broker"
    VERIFIER = "verifier"


@dataclass
class AgentBid:
    agent_id: str
    role: AgentRole
    confidence: float
    proposed_action: int
    rationale: str
    carbon_score: float


class EmergentRoleRegistry:
    """Decay-weighted role performance tracker with success-rate."""

    def __init__(self, decay: float = 0.95, exploration: float = 0.1,
                 rng: Optional[random.Random] = None):
        self.decay = decay
        self.exploration = exploration
        self.rng = rng or random.Random(0)
        self._scores: Dict[AgentRole, float] = {r: 1.0 for r in AgentRole}
        self._counts: Dict[AgentRole, int] = {r: 0 for r in AgentRole}
        self._successes: Dict[AgentRole, int] = {r: 0 for r in AgentRole}

    def record(self, role: AgentRole, success: bool, reward: float) -> None:
        for r in self._scores:
            self._scores[r] *= self.decay
        signal = abs(reward) if success else -abs(reward) * 0.5
        self._scores[role] += signal
        self._counts[role] += 1
        if success:
            self._successes[role] += 1

    def success_rate(self, role: AgentRole) -> float:
        c = self._counts[role]
        return (self._successes[role] / c) if c > 0 else 0.5

    def weights(self) -> Dict[AgentRole, float]:
        total = sum(max(v, 1e-6) for v in self._scores.values())
        return {r: max(v, 1e-6) / total for r, v in self._scores.items()}

    def dominant_role(self) -> AgentRole:
        return max(self._scores, key=self._scores.get)

    def sample_role(self) -> AgentRole:
        if self.rng.random() < self.exploration:
            return self.rng.choice(list(AgentRole))
        return self.dominant_role()

    def snapshot(self) -> Dict[str, Any]:
        return {
            "scores": {r.value: round(v, 4) for r, v in self._scores.items()},
            "counts": {r.value: c for r, c in self._counts.items()},
            "success_rates": {r.value: round(self.success_rate(r), 4) for r in AgentRole},
            "dominant": self.dominant_role().value,
        }


class MultiAgentCoordinator:
    """Carbon-weighted voting returning (winner_idx, winning_role)."""

    def __init__(self, registry: EmergentRoleRegistry,
                 role_bias: Optional[Dict[AgentRole, float]] = None,
                 rng: Optional[random.Random] = None):
        self.registry = registry
        self.role_bias = role_bias or {
            AgentRole.EXPLORER: 0.6,
            AgentRole.EXPLOITER: 0.9,
            AgentRole.SAFETY_OFFICER: 1.2,
            AgentRole.CARBON_BROKER: 1.1,
            AgentRole.VERIFIER: 1.0,
        }
        self.rng = rng or random.Random(0)
        self._last_vote: Optional[Dict[str, Any]] = None

    def vote(self, bids: Sequence[AgentBid], n_candidates: int) -> Tuple[int, Optional[AgentRole]]:
        if not bids or n_candidates <= 0:
            return 0, None
        role_w = self.registry.weights()
        scores = np.zeros(n_candidates)
        contributions: Dict[int, List[Tuple[AgentRole, float]]] = {}
        for b in bids:
            idx = b.proposed_action % n_candidates
            role_success = self.registry.success_rate(b.role)
            weight = (
                role_w.get(b.role, 0.1)
                * self.role_bias.get(b.role, 1.0)
                * max(b.confidence, 0.0)
                * (1.0 + b.carbon_score)
                * (0.5 + role_success)
            )
            scores[idx] += weight
            contributions.setdefault(idx, []).append((b.role, weight))
        top = float(scores.max())
        tied = [i for i, s in enumerate(scores) if abs(s - top) < 1e-9]
        winner = self.rng.choice(tied) if len(tied) > 1 else tied[0]
        winning_roles = [r for r, _ in contributions.get(winner, [])]
        winning_role = winning_roles[0] if winning_roles else None
        self._last_vote = {
            "winner": int(winner),
            "scores": scores.tolist(),
            "winning_roles": [r.value for r in winning_roles],
        }
        return int(winner), winning_role

    def last_vote(self) -> Optional[Dict[str, Any]]:
        return self._last_vote
