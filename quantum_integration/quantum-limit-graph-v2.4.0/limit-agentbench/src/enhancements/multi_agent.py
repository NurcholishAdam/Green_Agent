"""
multi_agent.py — Multi-agent coordination (Enhancement 4, full).

Core:
  * Correct winning-role attribution (weighted per-role totals for the winner).
  * Clamped ``confidence`` / ``carbon_score`` (both in [0, 1]).
  * EMA-based success tracking (non-stationary friendly).
  * Per-agent role tracking via ``agent_id``.
  * Safety-officer veto and distinct-role quorum.
  * Structured ``VoteResult`` with full explainability.
  * Config dataclasses, serialization, and vote hooks.

Advanced coordination:
  * Negotiation rounds with confidence-weighted defection.
  * Coalition formation by action.
  * Byzantine-robust voting via agent reputation + trimmed weights.

Façade:
  * ``MultiAgentTeam`` chains negotiation → coalitions → vote in one call.
  * ``run_episode`` closes the loop with outcome feedback.
"""

from __future__ import annotations

import logging
import random
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Roles and bids
# --------------------------------------------------------------------------- #
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
    carbon_score: float  # 1.0 = greenest, 0.0 = worst

    def __post_init__(self) -> None:
        if not (0.0 <= self.confidence <= 1.0):
            logger.warning("Clamping confidence %.4f for %s", self.confidence, self.agent_id)
        if not (0.0 <= self.carbon_score <= 1.0):
            logger.warning("Clamping carbon_score %.4f for %s", self.carbon_score, self.agent_id)
        self.confidence = float(min(1.0, max(0.0, self.confidence)))
        self.carbon_score = float(min(1.0, max(0.0, self.carbon_score)))


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass
class RegistryConfig:
    decay: float = 0.95
    exploration: float = 0.1
    success_ema_decay: float = 0.9
    initial_score: float = 1.0
    min_weight: float = 1e-6


@dataclass
class VotingConfig:
    role_bias: Dict[AgentRole, float] = field(default_factory=lambda: {
        AgentRole.EXPLORER: 0.6,
        AgentRole.EXPLOITER: 0.9,
        AgentRole.SAFETY_OFFICER: 1.2,
        AgentRole.CARBON_BROKER: 1.1,
        AgentRole.VERIFIER: 1.0,
    })
    use_softmax: bool = False
    softmax_temperature: float = 1.0
    quorum_min_roles: int = 1
    allow_safety_veto: bool = True
    safety_veto_threshold: float = 0.75


@dataclass
class NegotiationConfig:
    max_rounds: int = 3
    convergence_threshold: float = 0.9
    defection_probability: float = 0.5
    low_confidence_threshold: float = 0.5


@dataclass
class ByzantineConfig:
    enabled: bool = False
    trim_quantile: float = 0.1
    min_reputation: float = 0.2
    reputation_decay: float = 0.98
    reputation_bonus: float = 0.05
    reputation_penalty: float = 0.10


@dataclass
class TeamConfig:
    voting: VotingConfig = field(default_factory=VotingConfig)
    negotiation: NegotiationConfig = field(default_factory=NegotiationConfig)
    byzantine: ByzantineConfig = field(default_factory=ByzantineConfig)
    enable_negotiation: bool = True
    enable_coalitions: bool = True


# --------------------------------------------------------------------------- #
# Role statistics
# --------------------------------------------------------------------------- #
@dataclass
class RoleStats:
    score: float = 1.0
    attempts: float = 0.0
    successes: float = 0.0
    success_ema: float = 0.5
    last_reward: float = 0.0


@dataclass
class AgentReputation:
    score: float = 0.5
    observations: int = 0


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #
class EmergentRoleRegistry:
    """Decay-weighted role/agent performance tracker with EMA success rate."""

    def __init__(
        self,
        config: Optional[RegistryConfig] = None,
        rng: Optional[random.Random] = None,
    ) -> None:
        self.config = config or RegistryConfig()
        # Unseeded by default to avoid correlated exploration across instances.
        self.rng = rng or random.Random()
        self._stats: Dict[AgentRole, RoleStats] = {
            r: RoleStats(score=self.config.initial_score, success_ema=0.5)
            for r in AgentRole
        }
        self._agent_stats: Dict[Tuple[str, AgentRole], RoleStats] = {}

    # ---- updates -------------------------------------------------------- #
    def record(
        self,
        role: AgentRole,
        success: bool,
        reward: float,
        agent_id: Optional[str] = None,
    ) -> None:
        for s in self._stats.values():
            s.score *= self.config.decay

        signal = abs(reward) if success else -abs(reward) * 0.5
        self._update_stats(self._stats[role], signal, success, reward)

        if agent_id is not None:
            key = (agent_id, role)
            a = self._agent_stats.setdefault(key, RoleStats())
            a.score = max(a.score * self.config.decay, self.config.min_weight)
            self._update_stats(a, signal, success, reward)

    def _update_stats(
        self,
        stats: RoleStats,
        signal: float,
        success: bool,
        reward: float,
    ) -> None:
        stats.score = max(stats.score + signal, self.config.min_weight)
        stats.attempts += 1.0
        if success:
            stats.successes += 1.0
        d = self.config.success_ema_decay
        stats.success_ema = d * stats.success_ema + (1 - d) * (1.0 if success else 0.0)
        stats.last_reward = reward

    # ---- queries -------------------------------------------------------- #
    def success_rate(self, role: AgentRole) -> float:
        return self._stats[role].success_ema

    def agent_success_rate(self, agent_id: str, role: AgentRole) -> float:
        stats = self._agent_stats.get((agent_id, role))
        return stats.success_ema if stats is not None else 0.5

    def weights(self) -> Dict[AgentRole, float]:
        w = {r: max(s.score, self.config.min_weight) for r, s in self._stats.items()}
        total = sum(w.values())
        return {r: v / total for r, v in w.items()}

    def dominant_role(self) -> AgentRole:
        return max(self._stats, key=lambda r: self._stats[r].score)

    def sample_role(self) -> AgentRole:
        if self.rng.random() < self.config.exploration:
            return self.rng.choice(list(AgentRole))
        return self.dominant_role()

    # ---- introspection / persistence ----------------------------------- #
    def snapshot(self) -> Dict[str, Any]:
        return {
            "scores": {r.value: round(s.score, 4) for r, s in self._stats.items()},
            "attempts": {r.value: s.attempts for r, s in self._stats.items()},
            "successes": {r.value: s.successes for r, s in self._stats.items()},
            "success_ema": {r.value: round(s.success_ema, 4) for r, s in self._stats.items()},
            "dominant": self.dominant_role().value,
            "agents": {
                f"{aid}:{role.value}": {
                    "success_ema": round(s.success_ema, 4),
                    "attempts": s.attempts,
                    "score": round(s.score, 4),
                }
                for (aid, role), s in self._agent_stats.items()
            },
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "config": asdict(self.config),
            "stats": {r.value: asdict(s) for r, s in self._stats.items()},
            "agent_stats": {
                f"{aid}:{role.value}": asdict(s)
                for (aid, role), s in self._agent_stats.items()
            },
        }

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
        rng: Optional[random.Random] = None,
    ) -> "EmergentRoleRegistry":
        cfg = RegistryConfig(**data.get("config", {}))
        reg = cls(config=cfg, rng=rng)
        for r_value, s_data in data.get("stats", {}).items():
            reg._stats[AgentRole(r_value)] = RoleStats(**s_data)
        for key, s_data in data.get("agent_stats", {}).items():
            aid, r_value = key.split(":", 1)
            reg._agent_stats[(aid, AgentRole(r_value))] = RoleStats(**s_data)
        return reg


# --------------------------------------------------------------------------- #
# Result types
# --------------------------------------------------------------------------- #
@dataclass
class VoteResult:
    winner: int
    winning_role: Optional[AgentRole]
    scores: List[float]
    role_totals: Dict[AgentRole, float]
    tied: bool
    vetoed: bool
    veto_reason: Optional[str]
    quorum_met: bool
    n_distinct_roles: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "winner": self.winner,
            "winning_role": self.winning_role.value if self.winning_role else None,
            "scores": self.scores,
            "role_totals": {r.value: round(v, 6) for r, v in self.role_totals.items()},
            "tied": self.tied,
            "vetoed": self.vetoed,
            "veto_reason": self.veto_reason,
            "quorum_met": self.quorum_met,
            "n_distinct_roles": self.n_distinct_roles,
        }


@dataclass
class NegotiationRound:
    round_index: int
    leader: int
    leader_share: float
    tally: List[float]
    shifted_agents: List[str]


@dataclass
class NegotiationResult:
    final_vote: VoteResult
    rounds: List[NegotiationRound]
    converged: bool
    final_bids: List[AgentBid]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "final_vote": self.final_vote.to_dict(),
            "rounds": [asdict(r) for r in self.rounds],
            "converged": self.converged,
            "final_bids": [
                {
                    "agent_id": b.agent_id,
                    "role": b.role.value,
                    "confidence": b.confidence,
                    "proposed_action": b.proposed_action,
                    "carbon_score": b.carbon_score,
                }
                for b in self.final_bids
            ],
        }


@dataclass
class Coalition:
    action: int
    agent_ids: List[str]
    roles: List[AgentRole]
    total_weight: float
    avg_confidence: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action": self.action,
            "agent_ids": list(self.agent_ids),
            "roles": [r.value for r in self.roles],
            "total_weight": self.total_weight,
            "avg_confidence": self.avg_confidence,
        }


@dataclass
class TeamResult:
    """Full record of one team decision."""

    winner: int
    winning_role: Optional[AgentRole]
    final_vote: VoteResult
    negotiation: Optional[NegotiationResult]
    coalitions: List[Coalition]
    converged: bool
    rounds_used: int
    final_bids: List[AgentBid]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "winner": self.winner,
            "winning_role": self.winning_role.value if self.winning_role else None,
            "final_vote": self.final_vote.to_dict(),
            "negotiation": self.negotiation.to_dict() if self.negotiation else None,
            "coalitions": [c.to_dict() for c in self.coalitions],
            "converged": self.converged,
            "rounds_used": self.rounds_used,
            "final_bids": [
                {
                    "agent_id": b.agent_id,
                    "role": b.role.value,
                    "confidence": b.confidence,
                    "proposed_action": b.proposed_action,
                    "carbon_score": b.carbon_score,
                }
                for b in self.final_bids
            ],
        }


# --------------------------------------------------------------------------- #
# Coordinator
# --------------------------------------------------------------------------- #
class MultiAgentCoordinator:
    """Carbon-weighted voting with optional negotiation/coalitions/Byzantine."""

    def __init__(
        self,
        registry: EmergentRoleRegistry,
        config: Optional[VotingConfig] = None,
        rng: Optional[random.Random] = None,
    ) -> None:
        self.registry = registry
        self.config = config or VotingConfig()
        self.rng = rng or random.Random()
        self._last_vote: Optional[VoteResult] = None
        self._hooks: List[Callable[[VoteResult], None]] = []
        self.byzantine_config = ByzantineConfig()
        self._reputations: Dict[str, AgentReputation] = {}

    # ---- hooks ---------------------------------------------------------- #
    def add_hook(self, hook: Callable[[VoteResult], None]) -> None:
        self._hooks.append(hook)

    def _notify_hooks(self, result: VoteResult) -> None:
        for hook in self._hooks:
            try:
                hook(result)
            except Exception:  # noqa: BLE001 - hooks must never break voting
                logger.exception("Vote hook failed")

    # ---- weight computation -------------------------------------------- #
    def _compute_weight(self, bid: AgentBid, role_w: Dict[AgentRole, float]) -> float:
        role_success = self.registry.success_rate(bid.role)
        agent_success = self.registry.agent_success_rate(bid.agent_id, bid.role)
        combined_success = 0.5 * role_success + 0.5 * agent_success
        carbon_factor = 1.0 + bid.carbon_score  # [1, 2]
        weight = (
            role_w.get(bid.role, 0.1)
            * self.config.role_bias.get(bid.role, 1.0)
            * bid.confidence
            * carbon_factor
            * (0.5 + combined_success)
        )
        return max(weight, 0.0)

    # ---- Byzantine filter ---------------------------------------------- #
    def get_reputation(self, agent_id: str) -> AgentReputation:
        return self._reputations.setdefault(agent_id, AgentReputation())

    def record_outcome(
        self,
        agent_id: str,
        agreed_with_winner: bool,
        reward: float = 0.0,
    ) -> None:
        rep = self.get_reputation(agent_id)
        bc = self.byzantine_config
        rep.score *= bc.reputation_decay
        if agreed_with_winner:
            rep.score = min(1.0, rep.score + bc.reputation_bonus)
        else:
            rep.score = max(0.0, rep.score - bc.reputation_penalty)
        rep.observations += 1

    def _filter_byzantine(
        self,
        bids: Sequence[AgentBid],
        n_candidates: int,
    ) -> List[AgentBid]:
        if not self.byzantine_config.enabled or len(bids) < 3:
            return list(bids)

        bc = self.byzantine_config

        # 1) Reputation gate.
        filtered = [
            b for b in bids
            if self.get_reputation(b.agent_id).score >= bc.min_reputation
        ]
        if len(filtered) < 2:
            filtered = list(bids)

        # 2) Trimmed weights: drop extreme weights on both ends.
        role_w = self.registry.weights()
        weights = [(b, self._compute_weight(b, role_w)) for b in filtered]
        if len(weights) < 4 or bc.trim_quantile <= 0.0:
            return [b for b, _ in weights]

        sorted_weights = sorted(w for _, w in weights)
        k = max(1, int(len(sorted_weights) * bc.trim_quantile))
        if 2 * k >= len(sorted_weights):
            return [b for b, _ in weights]
        low = sorted_weights[k]
        high = sorted_weights[-k - 1]
        kept = [b for b, w in weights if low <= w <= high]
        return kept if len(kept) >= 2 else [b for b, _ in weights]

    # ---- core voting ---------------------------------------------------- #
    def vote(
        self,
        bids: Sequence[AgentBid],
        n_candidates: int,
    ) -> Tuple[int, Optional[AgentRole]]:
        if not bids or n_candidates <= 0:
            result = VoteResult(
                winner=0,
                winning_role=None,
                scores=[0.0] * max(n_candidates, 0),
                role_totals={},
                tied=False,
                vetoed=False,
                veto_reason=None,
                quorum_met=False,
                n_distinct_roles=0,
            )
            self._last_vote = result
            self._notify_hooks(result)
            return 0, None

        working_bids = self._filter_byzantine(bids, n_candidates)
        role_w = self.registry.weights()
        scores = np.zeros(n_candidates, dtype=float)
        contributions: Dict[int, List[Tuple[AgentRole, float]]] = {}

        for bid in working_bids:
            idx = bid.proposed_action % n_candidates
            weight = self._compute_weight(bid, role_w)
            scores[idx] += weight
            contributions.setdefault(idx, []).append((bid.role, weight))

        distinct_roles = {b.role for b in working_bids}
        quorum_met = len(distinct_roles) >= self.config.quorum_min_roles

        if self.config.use_softmax and scores.size > 0:
            temp = max(self.config.softmax_temperature, 1e-6)
            shifted = scores / temp - (scores / temp).max()
            exp = np.exp(shifted)
            report_scores = (exp / exp.sum()).tolist()
        else:
            report_scores = scores.tolist()

        top = float(scores.max())
        tied_idx = [i for i, s in enumerate(scores) if abs(s - top) < 1e-9]
        winner = int(self.rng.choice(tied_idx)) if len(tied_idx) > 1 else int(tied_idx[0])

        # Correct winning-role attribution: aggregate weight per role for winner.
        role_totals: Dict[AgentRole, float] = {}
        for role, w in contributions.get(winner, []):
            role_totals[role] = role_totals.get(role, 0.0) + w
        winning_role = max(role_totals, key=role_totals.get) if role_totals else None

        # Safety-officer veto.
        vetoed = False
        veto_reason: Optional[str] = None
        if self.config.allow_safety_veto:
            for bid in working_bids:
                if bid.role is not AgentRole.SAFETY_OFFICER:
                    continue
                if bid.confidence < self.config.safety_veto_threshold:
                    continue
                if bid.proposed_action % n_candidates == winner:
                    continue
                vetoed = True
                veto_reason = (
                    f"safety_officer:{bid.agent_id} vetoed action {winner} "
                    f"in favour of {bid.proposed_action % n_candidates} "
                    f"(confidence={bid.confidence:.2f})"
                )
                break

        if not quorum_met:
            vetoed = True
            veto_reason = (
                f"quorum not met: {len(distinct_roles)} distinct role(s), "
                f"required {self.config.quorum_min_roles}"
            )

        result = VoteResult(
            winner=winner,
            winning_role=None if vetoed else winning_role,
            scores=report_scores,
            role_totals=role_totals,
            tied=len(tied_idx) > 1,
            vetoed=vetoed,
            veto_reason=veto_reason,
            quorum_met=quorum_met,
            n_distinct_roles=len(distinct_roles),
        )

        if vetoed:
            logger.warning("Vote vetoed: %s", veto_reason)

        self._last_vote = result
        self._notify_hooks(result)
        return winner, result.winning_role

    # ---- negotiation ---------------------------------------------------- #
    def negotiate(
        self,
        bids: Sequence[AgentBid],
        n_candidates: int,
        config: Optional[NegotiationConfig] = None,
    ) -> NegotiationResult:
        cfg = config or NegotiationConfig()
        rounds: List[NegotiationRound] = []
        current: List[AgentBid] = list(bids)
        converged = False

        if not current or n_candidates <= 0:
            self.vote(current, n_candidates)
            return NegotiationResult(
                final_vote=self._last_vote,
                rounds=rounds,
                converged=False,
                final_bids=current,
            )

        role_w = self.registry.weights()
        for r in range(cfg.max_rounds):
            scores = np.zeros(n_candidates, dtype=float)
            for b in current:
                idx = b.proposed_action % n_candidates
                scores[idx] += self._compute_weight(b, role_w)

            total = float(scores.sum()) if scores.sum() > 0 else 1.0
            leader = int(np.argmax(scores))
            leader_share = float(scores[leader] / total)

            shifted: List[str] = []
            new_bids: List[AgentBid] = []
            for b in current:
                idx = b.proposed_action % n_candidates
                should_defect = (
                    idx != leader
                    and b.confidence < cfg.low_confidence_threshold
                    and self.rng.random() < cfg.defection_probability
                )
                if should_defect:
                    new_bids.append(AgentBid(
                        agent_id=b.agent_id,
                        role=b.role,
                        confidence=b.confidence,
                        proposed_action=leader,
                        rationale=f"defected to leader action {leader} in round {r}",
                        carbon_score=b.carbon_score,
                    ))
                    shifted.append(b.agent_id)
                else:
                    new_bids.append(b)

            rounds.append(NegotiationRound(
                round_index=r,
                leader=leader,
                leader_share=leader_share,
                tally=scores.tolist(),
                shifted_agents=shifted,
            ))
            current = new_bids

            if leader_share >= cfg.convergence_threshold:
                converged = True
                break

        self.vote(current, n_candidates)
        return NegotiationResult(
            final_vote=self._last_vote,
            rounds=rounds,
            converged=converged,
            final_bids=current,
        )

    # ---- coalitions ----------------------------------------------------- #
    def form_coalitions(
        self,
        bids: Sequence[AgentBid],
        n_candidates: int,
    ) -> List[Coalition]:
        if not bids or n_candidates <= 0:
            return []
        role_w = self.registry.weights()
        groups: Dict[int, List[Tuple[AgentBid, float]]] = {}
        for b in bids:
            idx = b.proposed_action % n_candidates
            groups.setdefault(idx, []).append((b, self._compute_weight(b, role_w)))

        coalitions: List[Coalition] = []
        for action, members in groups.items():
            total_w = sum(w for _, w in members)
            avg_conf = sum(b.confidence for b, _ in members) / len(members)
            coalitions.append(Coalition(
                action=action,
                agent_ids=[b.agent_id for b, _ in members],
                roles=[b.role for b, _ in members],
                total_weight=total_w,
                avg_confidence=avg_conf,
            ))
        coalitions.sort(key=lambda c: c.total_weight, reverse=True)
        return coalitions

    # ---- accessors ------------------------------------------------------ #
    def last_vote(self) -> Optional[VoteResult]:
        return self._last_vote

    def last_vote_dict(self) -> Optional[Dict[str, Any]]:
        return self._last_vote.to_dict() if self._last_vote else None

    # ---- persistence ---------------------------------------------------- #
    def to_dict(self) -> Dict[str, Any]:
        cfg = asdict(self.config)
        cfg["role_bias"] = {r.value: v for r, v in self.config.role_bias.items()}
        return {
            "config": cfg,
            "registry": self.registry.to_dict(),
            "byzantine_config": asdict(self.byzantine_config),
            "reputations": {
                aid: asdict(rep) for aid, rep in self._reputations.items()
            },
        }

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
        rng: Optional[random.Random] = None,
    ) -> "MultiAgentCoordinator":
        cfg_data = dict(data.get("config", {}))
        role_bias_raw = cfg_data.pop("role_bias", None)
        cfg = VotingConfig(**cfg_data)
        if role_bias_raw:
            cfg.role_bias = {AgentRole(k): float(v) for k, v in role_bias_raw.items()}
        reg = EmergentRoleRegistry.from_dict(data.get("registry", {}), rng=rng)
        coord = cls(reg, cfg, rng=rng)
        if "byzantine_config" in data:
            coord.byzantine_config = ByzantineConfig(**data["byzantine_config"])
        for aid, rep_data in data.get("reputations", {}).items():
            coord._reputations[aid] = AgentReputation(**rep_data)
        return coord


# --------------------------------------------------------------------------- #
# Team façade
# --------------------------------------------------------------------------- #
class MultiAgentTeam:
    """
    Façade that chains: negotiation → coalitions → vote, in a single call.

    Typical usage::

        team = MultiAgentTeam(seed=0)
        result = team.decide(bids, n_candidates=4)
        # ... execute result.winner ...
        team.observe_outcome(result.winner, result.final_bids,
                             n_candidates=4, reward=1.0, success=True)

    Or with a closed-loop helper::

        team.run_episode(bids, 4, outcome_fn=lambda w, r: (True, 1.0))
    """

    def __init__(
        self,
        config: Optional[TeamConfig] = None,
        seed: Optional[int] = None,
        rng: Optional[random.Random] = None,
    ) -> None:
        self.config = config or TeamConfig()
        base_seed = 0 if seed is None else seed
        self.registry = EmergentRoleRegistry(
            config=RegistryConfig(),
            rng=random.Random(base_seed),
        )
        self.coordinator = MultiAgentCoordinator(
            self.registry,
            self.config.voting,
            rng=rng or random.Random(base_seed),
        )
        self.coordinator.byzantine_config = self.config.byzantine
        self._last_result: Optional[TeamResult] = None

    # ---- main entry ----------------------------------------------------- #
    def decide(
        self,
        bids: Sequence[AgentBid],
        n_candidates: int,
        *,
        negotiate: Optional[bool] = None,
        form_coalitions: Optional[bool] = None,
    ) -> TeamResult:
        """Run the full pipeline once. Overrides let callers skip stages."""
        do_negotiate = (
            self.config.enable_negotiation if negotiate is None else negotiate
        )
        do_coalitions = (
            self.config.enable_coalitions if form_coalitions is None else form_coalitions
        )

        # ---- guard: empty / degenerate inputs -------------------------- #
        if not bids or n_candidates <= 0:
            self.coordinator.vote(bids, n_candidates)
            empty_vote = self.coordinator.last_vote()
            result = TeamResult(
                winner=empty_vote.winner,
                winning_role=None,
                final_vote=empty_vote,
                negotiation=None,
                coalitions=[],
                converged=False,
                rounds_used=0,
                final_bids=list(bids),
            )
            self._last_result = result
            return result

        # ---- stage 1: negotiation -------------------------------------- #
        negotiation: Optional[NegotiationResult] = None
        if do_negotiate:
            negotiation = self.coordinator.negotiate(
                bids, n_candidates, config=self.config.negotiation
            )
            final_bids: List[AgentBid] = list(negotiation.final_bids)
            final_vote: VoteResult = negotiation.final_vote
            converged: bool = negotiation.converged
            rounds_used: int = len(negotiation.rounds)
        else:
            final_bids = list(bids)
            self.coordinator.vote(final_bids, n_candidates)
            final_vote = self.coordinator.last_vote()
            converged = True  # trivially "converged" when negotiation is skipped
            rounds_used = 0

        # ---- stage 2: coalitions --------------------------------------- #
        coalitions: List[Coalition] = []
        if do_coalitions:
            coalitions = self.coordinator.form_coalitions(final_bids, n_candidates)

        # ---- assemble -------------------------------------------------- #
        result = TeamResult(
            winner=final_vote.winner,
            winning_role=final_vote.winning_role,
            final_vote=final_vote,
            negotiation=negotiation,
            coalitions=coalitions,
            converged=converged,
            rounds_used=rounds_used,
            final_bids=final_bids,
        )
        self._last_result = result
        return result

    # ---- outcome feedback ---------------------------------------------- #
    def observe_outcome(
        self,
        winner: int,
        bids: Sequence[AgentBid],
        n_candidates: int,
        reward: float,
        success: bool,
    ) -> None:
        """Feed the realised outcome back into reputation + role stats."""
        if n_candidates <= 0:
            return

        for b in bids:
            agreed = (b.proposed_action % n_candidates) == winner
            self.coordinator.record_outcome(b.agent_id, agreed, reward)

        if self._last_result is not None and self._last_result.winning_role is not None:
            self.registry.record(self._last_result.winning_role, success, reward)

    def run_episode(
        self,
        bids: Sequence[AgentBid],
        n_candidates: int,
        outcome_fn: Callable[[int, Optional[AgentRole]], Tuple[bool, float]],
    ) -> TeamResult:
        """
        One full loop: decide → call ``outcome_fn(winner, winning_role)``
        → observe_outcome. ``outcome_fn`` must return ``(success, reward)``.
        """
        result = self.decide(bids, n_candidates)
        success, reward = outcome_fn(result.winner, result.winning_role)
        self.observe_outcome(
            result.winner, result.final_bids, n_candidates, reward, success
        )
        return result

    # ---- accessors ------------------------------------------------------ #
    def last_result(self) -> Optional[TeamResult]:
        return self._last_result

    def last_result_dict(self) -> Optional[Dict[str, Any]]:
        return self._last_result.to_dict() if self._last_result else None

    # ---- persistence ---------------------------------------------------- #
    def to_dict(self) -> Dict[str, Any]:
        coord = self.coordinator.to_dict()
        return {
            "config": {
                "voting": coord["config"],
                "negotiation": asdict(self.config.negotiation),
                "byzantine": asdict(self.config.byzantine),
                "enable_negotiation": self.config.enable_negotiation,
                "enable_coalitions": self.config.enable_coalitions,
            },
            "coordinator": coord,
        }

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
        seed: Optional[int] = None,
        rng: Optional[random.Random] = None,
    ) -> "MultiAgentTeam":
        cfg_data = data.get("config", {})
        voting_raw = dict(cfg_data.get("voting", {}))
        role_bias_raw = voting_raw.pop("role_bias", None)
        voting = VotingConfig(**voting_raw)
        if role_bias_raw:
            voting.role_bias = {
                AgentRole(k): float(v) for k, v in role_bias_raw.items()
            }
        negotiation = NegotiationConfig(**cfg_data.get("negotiation", {}))
        byzantine = ByzantineConfig(**cfg_data.get("byzantine", {}))
        cfg = TeamConfig(
            voting=voting,
            negotiation=negotiation,
            byzantine=byzantine,
            enable_negotiation=cfg_data.get("enable_negotiation", True),
            enable_coalitions=cfg_data.get("enable_coalitions", True),
        )

        team = cls(cfg, seed=seed, rng=rng)

        coord_data = data.get("coordinator", {})
        team.registry = EmergentRoleRegistry.from_dict(
            coord_data.get("registry", {}),
            rng=rng or random.Random(seed or 0),
        )
        team.coordinator = MultiAgentCoordinator(
            team.registry,
            voting,
            rng=rng or random.Random(seed or 0),
        )
        team.coordinator.byzantine_config = byzantine
        for aid, rep_data in coord_data.get("reputations", {}).items():
            team.coordinator._reputations[aid] = AgentReputation(**rep_data)
        return team
