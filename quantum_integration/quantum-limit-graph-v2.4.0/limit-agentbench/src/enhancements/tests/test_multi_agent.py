"""
tests/test_multi_agent.py — Unit tests for multi_agent.py.

Run with:
    python -m unittest tests.test_multi_agent -v
or:
    python -m pytest tests/test_multi_agent.py -v
"""

from __future__ import annotations

import random
import unittest

from multi_agent import (
    AgentBid,
    AgentReputation,
    AgentRole,
    ByzantineConfig,
    Coalition,
    EmergentRoleRegistry,
    MultiAgentCoordinator,
    MultiAgentTeam,
    NegotiationConfig,
    NegotiationResult,
    RegistryConfig,
    RoleStats,
    TeamConfig,
    TeamResult,
    VoteResult,
    VotingConfig,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def uniform_bias() -> dict:
    """Role bias of 1.0 for every role — isolates confidence/carbon effects."""
    return {r: 1.0 for r in AgentRole}


def make_coordinator(
    *,
    cfg: VotingConfig | None = None,
    seed: int = 0,
    quorum_min_roles: int = 1,
    allow_safety_veto: bool = True,
) -> MultiAgentCoordinator:
    reg = EmergentRoleRegistry(rng=random.Random(seed))
    vcfg = cfg or VotingConfig(
        role_bias=uniform_bias(),
        quorum_min_roles=quorum_min_roles,
        allow_safety_veto=allow_safety_veto,
    )
    return MultiAgentCoordinator(reg, vcfg, rng=random.Random(seed))


def make_team(
    *,
    enable_negotiation: bool = True,
    enable_coalitions: bool = True,
    quorum_min_roles: int = 1,
    allow_safety_veto: bool = True,
    max_rounds: int = 3,
    defection_probability: float = 0.5,
    low_confidence_threshold: float = 0.5,
    seed: int = 0,
) -> MultiAgentTeam:
    vcfg = VotingConfig(
        role_bias=uniform_bias(),
        quorum_min_roles=quorum_min_roles,
        allow_safety_veto=allow_safety_veto,
    )
    ncfg = NegotiationConfig(
        max_rounds=max_rounds,
        defection_probability=defection_probability,
        low_confidence_threshold=low_confidence_threshold,
    )
    cfg = TeamConfig(
        voting=vcfg,
        negotiation=ncfg,
        enable_negotiation=enable_negotiation,
        enable_coalitions=enable_coalitions,
    )
    return MultiAgentTeam(cfg, seed=seed)


# --------------------------------------------------------------------------- #
# 1. AgentBid clamping
# --------------------------------------------------------------------------- #
class TestAgentBid(unittest.TestCase):
    def test_confidence_clamped_high(self):
        b = AgentBid("a", AgentRole.EXPLORER, 5.0, 0, "", 0.5)
        self.assertEqual(b.confidence, 1.0)

    def test_confidence_clamped_low(self):
        b = AgentBid("a", AgentRole.EXPLORER, -2.0, 0, "", 0.5)
        self.assertEqual(b.confidence, 0.0)

    def test_carbon_clamped_high(self):
        b = AgentBid("a", AgentRole.EXPLORER, 0.5, 0, "", 7.5)
        self.assertEqual(b.carbon_score, 1.0)

    def test_carbon_clamped_low(self):
        b = AgentBid("a", AgentRole.EXPLORER, 0.5, 0, "", -3.0)
        self.assertEqual(b.carbon_score, 0.0)

    def test_valid_ranges_preserved(self):
        b = AgentBid("a", AgentRole.EXPLORER, 0.42, 0, "", 0.73)
        self.assertAlmostEqual(b.confidence, 0.42)
        self.assertAlmostEqual(b.carbon_score, 0.73)


# --------------------------------------------------------------------------- #
# 2. Registry
# --------------------------------------------------------------------------- #
class TestEmergentRoleRegistry(unittest.TestCase):
    def setUp(self):
        self.reg = EmergentRoleRegistry(rng=random.Random(0))

    def test_initial_weights_uniform(self):
        w = self.reg.weights()
        self.assertEqual(len(w), len(AgentRole))
        for v in w.values():
            self.assertAlmostEqual(v, 1.0 / len(AgentRole), places=6)

    def test_weights_sum_to_one(self):
        self.reg.record(AgentRole.EXPLORER, True, 1.0)
        self.reg.record(AgentRole.SAFETY_OFFICER, False, 0.4)
        self.assertAlmostEqual(sum(self.reg.weights().values()), 1.0, places=6)

    def test_decay_reduces_non_updated_scores(self):
        before = self.reg._stats[AgentRole.VERIFIER].score
        self.reg.record(AgentRole.EXPLORER, True, 1.0)
        after = self.reg._stats[AgentRole.VERIFIER].score
        self.assertLess(after, before)

    def test_success_updates_ema(self):
        initial = self.reg.success_rate(AgentRole.EXPLORER)
        self.reg.record(AgentRole.EXPLORER, True, 1.0)
        self.assertGreater(self.reg.success_rate(AgentRole.EXPLORER), initial)

    def test_failure_lowers_ema(self):
        initial = self.reg.success_rate(AgentRole.EXPLORER)
        self.reg.record(AgentRole.EXPLORER, False, 1.0)
        self.assertLess(self.reg.success_rate(AgentRole.EXPLORER), initial)

    def test_success_rate_bounded(self):
        for _ in range(50):
            self.reg.record(AgentRole.EXPLORER, True, 1.0)
        self.assertLessEqual(self.reg.success_rate(AgentRole.EXPLORER), 1.0)
        self.assertGreaterEqual(self.reg.success_rate(AgentRole.EXPLORER), 0.0)

    def test_agent_success_tracked(self):
        for _ in range(10):
            self.reg.record(AgentRole.EXPLORER, True, 1.0, agent_id="alpha")
        self.assertGreater(
            self.reg.agent_success_rate("alpha", AgentRole.EXPLORER), 0.5
        )

    def test_unknown_agent_defaults(self):
        self.assertEqual(
            self.reg.agent_success_rate("nobody", AgentRole.EXPLORER), 0.5
        )

    def test_dominant_role_tracks_updates(self):
        for _ in range(5):
            self.reg.record(AgentRole.CARBON_BROKER, True, 1.0)
        self.assertEqual(self.reg.dominant_role(), AgentRole.CARBON_BROKER)

    def test_sample_role_exploration(self):
        reg = EmergentRoleRegistry(
            config=RegistryConfig(exploration=1.0), rng=random.Random(0)
        )
        seen = {reg.sample_role() for _ in range(200)}
        self.assertGreater(len(seen), 1)

    def test_snapshot_has_expected_keys(self):
        snap = self.reg.snapshot()
        for k in ("scores", "attempts", "successes", "success_ema", "dominant", "agents"):
            self.assertIn(k, snap)


# --------------------------------------------------------------------------- #
# 3. Core voting
# --------------------------------------------------------------------------- #
class TestVotingCore(unittest.TestCase):
    def test_empty_bids_returns_default(self):
        coord = make_coordinator()
        winner, role = coord.vote([], 3)
        self.assertEqual(winner, 0)
        self.assertIsNone(role)
        self.assertIsInstance(coord.last_vote(), VoteResult)

    def test_zero_candidates_returns_default(self):
        coord = make_coordinator()
        winner, role = coord.vote(
            [AgentBid("a", AgentRole.EXPLORER, 0.9, 0, "", 0.5)], 0
        )
        self.assertEqual(winner, 0)
        self.assertIsNone(role)

    def test_winning_role_is_max_contributor(self):
        """Regression: winning_role must be the max-weight contributor, not the first."""
        coord = make_coordinator()
        bids = [
            AgentBid("a1", AgentRole.EXPLORER, 0.10, 0, "", 0.5),
            AgentBid("a2", AgentRole.EXPLOITER, 0.95, 0, "", 0.5),
            AgentBid("a3", AgentRole.SAFETY_OFFICER, 0.10, 0, "", 0.5),
        ]
        winner, role = coord.vote(bids, 3)
        self.assertEqual(winner, 0)
        self.assertEqual(role, AgentRole.EXPLOITER)
        totals = coord.last_vote().role_totals
        self.assertGreater(totals[AgentRole.EXPLOITER], totals[AgentRole.EXPLORER])

    def test_tie_breaking_is_deterministic_with_seed(self):
        coord_a = make_coordinator(seed=42)
        coord_b = make_coordinator(seed=42)
        bids = [
            AgentBid("a1", AgentRole.EXPLORER, 0.5, 0, "", 0.5),
            AgentBid("a2", AgentRole.EXPLORER, 0.5, 1, "", 0.5),
        ]
        wa, _ = coord_a.vote(bids, 2)
        wb, _ = coord_b.vote(bids, 2)
        self.assertEqual(wa, wb)
        self.assertTrue(coord_a.last_vote().tied)

    def test_softmax_scores_sum_to_one(self):
        cfg = VotingConfig(role_bias=uniform_bias(), use_softmax=True)
        coord = make_coordinator(cfg=cfg)
        bids = [
            AgentBid("a1", AgentRole.EXPLORER, 0.9, 0, "", 0.5),
            AgentBid("a2", AgentRole.EXPLORER, 0.5, 1, "", 0.5),
        ]
        coord.vote(bids, 2)
        self.assertAlmostEqual(sum(coord.last_vote().scores), 1.0, places=6)


# --------------------------------------------------------------------------- #
# 4. Carbon semantics
# --------------------------------------------------------------------------- #
class TestCarbonSemantics(unittest.TestCase):
    def test_higher_carbon_score_wins(self):
        coord = make_coordinator()
        bids = [
            AgentBid("a1", AgentRole.EXPLORER, 0.5, 0, "", 0.1),
            AgentBid("a2", AgentRole.EXPLORER, 0.5, 1, "", 0.9),
        ]
        winner, _ = coord.vote(bids, 2)
        self.assertEqual(winner, 1)

    def test_equal_carbon_ties(self):
        coord = make_coordinator(seed=7)
        bids = [
            AgentBid("a1", AgentRole.EXPLORER, 0.5, 0, "", 0.5),
            AgentBid("a2", AgentRole.EXPLORER, 0.5, 1, "", 0.5),
        ]
        coord.vote(bids, 2)
        self.assertTrue(coord.last_vote().tied)

    def test_carbon_clamped_at_bid_level(self):
        coord = make_coordinator()
        bids = [
            AgentBid("a1", AgentRole.EXPLORER, 0.5, 0, "", -5.0),
            AgentBid("a2", AgentRole.EXPLORER, 0.5, 1, "", 99.0),
        ]
        winner, _ = coord.vote(bids, 2)
        self.assertEqual(winner, 1)


# --------------------------------------------------------------------------- #
# 5. Safety & quorum
# --------------------------------------------------------------------------- #
class TestSafetyAndQuorum(unittest.TestCase):
    def test_safety_veto_triggers(self):
        coord = make_coordinator(allow_safety_veto=True)
        bids = [
            AgentBid("a1", AgentRole.EXPLOITER, 0.9, 0, "", 0.9),
            AgentBid("a2", AgentRole.SAFETY_OFFICER, 0.9, 1, "", 0.5),
        ]
        winner, role = coord.vote(bids, 2)
        self.assertEqual(winner, 0)
        self.assertIsNone(role)
        self.assertTrue(coord.last_vote().vetoed)

    def test_safety_no_veto_below_threshold(self):
        coord = make_coordinator(allow_safety_veto=True)
        bids = [
            AgentBid("a1", AgentRole.EXPLOITER, 0.9, 0, "", 0.9),
            AgentBid("a2", AgentRole.SAFETY_OFFICER, 0.3, 1, "", 0.5),
        ]
        winner, _ = coord.vote(bids, 2)
        self.assertEqual(winner, 0)
        self.assertFalse(coord.last_vote().vetoed)

    def test_safety_officer_agrees_no_veto(self):
        coord = make_coordinator(allow_safety_veto=True)
        bids = [
            AgentBid("a1", AgentRole.EXPLOITER, 0.9, 0, "", 0.9),
            AgentBid("a2", AgentRole.SAFETY_OFFICER, 0.9, 0, "", 0.5),
        ]
        coord.vote(bids, 2)
        self.assertFalse(coord.last_vote().vetoed)

    def test_quorum_not_met_vetoes(self):
        cfg = VotingConfig(
            role_bias=uniform_bias(),
            quorum_min_roles=3,
            allow_safety_veto=False,
        )
        coord = make_coordinator(cfg=cfg)
        bids = [
            AgentBid("a1", AgentRole.EXPLORER, 0.5, 0, "", 0.5),
            AgentBid("a2", AgentRole.EXPLORER, 0.5, 1, "", 0.5),
        ]
        coord.vote(bids, 2)
        last = coord.last_vote()
        self.assertFalse(last.quorum_met)
        self.assertTrue(last.vetoed)
        self.assertEqual(last.n_distinct_roles, 1)

    def test_quorum_met_passes(self):
        cfg = VotingConfig(
            role_bias=uniform_bias(),
            quorum_min_roles=2,
            allow_safety_veto=False,
        )
        coord = make_coordinator(cfg=cfg)
        bids = [
            AgentBid("a1", AgentRole.EXPLORER, 0.6, 0, "", 0.5),
            AgentBid("a2", AgentRole.EXPLOITER, 0.4, 1, "", 0.5),
        ]
        coord.vote(bids, 2)
        last = coord.last_vote()
        self.assertTrue(last.quorum_met)
        self.assertFalse(last.vetoed)


# --------------------------------------------------------------------------- #
# 6. Serialization
# --------------------------------------------------------------------------- #
class TestSerialization(unittest.TestCase):
    def test_registry_roundtrip(self):
        reg = EmergentRoleRegistry(rng=random.Random(0))
        reg.record(AgentRole.EXPLORER, True, 1.0)
        reg.record(AgentRole.EXPLORER, False, 0.5)
        reg.record(AgentRole.SAFETY_OFFICER, True, 1.0, agent_id="a1")

        reg2 = EmergentRoleRegistry.from_dict(reg.to_dict(), rng=random.Random(0))
        self.assertEqual(reg.snapshot()["dominant"], reg2.snapshot()["dominant"])
        for r in AgentRole:
            self.assertAlmostEqual(
                reg.success_rate(r), reg2.success_rate(r), places=6
            )

    def test_coordinator_roundtrip(self):
        coord = make_coordinator()
        coord.vote([AgentBid("a", AgentRole.EXPLORER, 0.7, 0, "", 0.5)], 1)
        data = coord.to_dict()
        coord2 = MultiAgentCoordinator.from_dict(data, rng=random.Random(0))
        self.assertEqual(
            set(coord2.config.role_bias.keys()),
            set(coord.config.role_bias.keys()),
        )
        for r in AgentRole:
            self.assertAlmostEqual(
                coord2.registry.success_rate(r),
                coord.registry.success_rate(r),
                places=6,
            )

    def test_vote_result_serializable(self):
        coord = make_coordinator()
        coord.vote([AgentBid("a", AgentRole.EXPLORER, 0.7, 0, "", 0.5)], 1)
        d = coord.last_vote_dict()
        self.assertIn("winner", d)
        self.assertIn("role_totals", d)
        self.assertIn("vetoed", d)


# --------------------------------------------------------------------------- #
# 7. Hooks
# --------------------------------------------------------------------------- #
class TestHooks(unittest.TestCase):
    def test_hook_receives_vote_result(self):
        coord = make_coordinator()
        seen = []
        coord.add_hook(seen.append)
        coord.vote([AgentBid("a", AgentRole.EXPLORER, 0.7, 0, "", 0.5)], 1)
        self.assertEqual(len(seen), 1)
        self.assertIsInstance(seen[0], VoteResult)

    def test_hook_exception_does_not_break_vote(self):
        coord = make_coordinator()

        def bad_hook(_):
            raise RuntimeError("boom")

        coord.add_hook(bad_hook)
        winner, _ = coord.vote(
            [AgentBid("a", AgentRole.EXPLORER, 0.7, 0, "", 0.5)], 1
        )
        self.assertEqual(winner, 0)


# --------------------------------------------------------------------------- #
# 8. Negotiation
# --------------------------------------------------------------------------- #
class TestNegotiation(unittest.TestCase):
    def test_negotiation_returns_rounds_and_final_vote(self):
        coord = make_coordinator()
        bids = [
            AgentBid("a1", AgentRole.EXPLORER, 0.9, 0, "", 0.5),
            AgentBid("a2", AgentRole.EXPLORER, 0.1, 1, "", 0.5),
            AgentBid("a3", AgentRole.EXPLORER, 0.1, 2, "", 0.5),
        ]
        result = coord.negotiate(bids, 3)
        self.assertIsInstance(result, NegotiationResult)
        self.assertGreaterEqual(len(result.rounds), 1)
        self.assertIsInstance(result.final_vote, VoteResult)

    def test_negotiation_converges_when_unanimous(self):
        coord = make_coordinator()
        bids = [
            AgentBid("a1", AgentRole.EXPLORER, 0.9, 0, "", 0.5),
            AgentBid("a2", AgentRole.EXPLORER, 0.9, 0, "", 0.5),
            AgentBid("a3", AgentRole.EXPLORER, 0.9, 0, "", 0.5),
        ]
        result = coord.negotiate(bids, 3)
        self.assertTrue(result.converged)
        self.assertEqual(result.rounds[0].leader, 0)

    def test_negotiation_respects_max_rounds(self):
        coord = make_coordinator()
        cfg = NegotiationConfig(
            max_rounds=1,
            convergence_threshold=0.99,
            defection_probability=0.0,
        )
        bids = [
            AgentBid("a1", AgentRole.EXPLORER, 0.5, 0, "", 0.5),
            AgentBid("a2", AgentRole.EXPLORER, 0.5, 1, "", 0.5),
        ]
        result = coord.negotiate(bids, 2, config=cfg)
        self.assertEqual(len(result.rounds), 1)

    def test_negotiation_defection_shifts_low_confidence_agents(self):
        coord = make_coordinator(seed=0)
        cfg = NegotiationConfig(
            max_rounds=2,
            convergence_threshold=2.0,
            defection_probability=1.0,
            low_confidence_threshold=0.9,
        )
        bids = [
            AgentBid("strong", AgentRole.EXPLORER, 0.95, 0, "", 0.5),
            AgentBid("weak", AgentRole.EXPLORER, 0.1, 1, "", 0.5),
        ]
        result = coord.negotiate(bids, 2, config=cfg)
        self.assertIn("weak", result.rounds[0].shifted_agents)

    def test_negotiation_empty_bids(self):
        coord = make_coordinator()
        result = coord.negotiate([], 3)
        self.assertFalse(result.converged)
        self.assertEqual(len(result.rounds), 0)

    def test_negotiation_result_serializable(self):
        coord = make_coordinator()
        result = coord.negotiate(
            [AgentBid("a", AgentRole.EXPLORER, 0.9, 0, "", 0.5)], 1
        )
        d = result.to_dict()
        self.assertIn("final_vote", d)
        self.assertIn("rounds", d)
        self.assertIn("converged", d)


# --------------------------------------------------------------------------- #
# 9. Coalitions
# --------------------------------------------------------------------------- #
class TestCoalitions(unittest.TestCase):
    def test_coalitions_group_by_action(self):
        coord = make_coordinator()
        bids = [
            AgentBid("a1", AgentRole.EXPLORER, 0.5, 0, "", 0.5),
            AgentBid("a2", AgentRole.EXPLOITER, 0.5, 0, "", 0.5),
            AgentBid("a3", AgentRole.VERIFIER, 0.5, 1, "", 0.5),
        ]
        cos = coord.form_coalitions(bids, 2)
        self.assertEqual(len(cos), 2)
        self.assertEqual(cos[0].action, 0)
        self.assertEqual(len(cos[0].agent_ids), 2)
        self.assertEqual(cos[1].action, 1)

    def test_coalitions_sorted_descending_by_weight(self):
        coord = make_coordinator()
        bids = [
            AgentBid("heavy", AgentRole.EXPLORER, 0.9, 0, "", 0.9),
            AgentBid("light", AgentRole.EXPLORER, 0.1, 1, "", 0.1),
        ]
        cos = coord.form_coalitions(bids, 2)
        self.assertGreaterEqual(cos[0].total_weight, cos[1].total_weight)
        self.assertEqual(cos[0].action, 0)

    def test_coalitions_empty_input(self):
        coord = make_coordinator()
        self.assertEqual(coord.form_coalitions([], 3), [])

    def test_coalition_serializable(self):
        coord = make_coordinator()
        cos = coord.form_coalitions(
            [AgentBid("a", AgentRole.EXPLORER, 0.5, 0, "", 0.5)], 1
        )
        d = cos[0].to_dict()
        self.assertIn("action", d)
        self.assertIn("agent_ids", d)
        self.assertIn("total_weight", d)


# --------------------------------------------------------------------------- #
# 10. Byzantine robustness
# --------------------------------------------------------------------------- #
class TestByzantine(unittest.TestCase):
    def test_disabled_by_default(self):
        coord = make_coordinator()
        self.assertFalse(coord.byzantine_config.enabled)

    def test_enabling_byzantine_does_not_crash(self):
        coord = make_coordinator()
        coord.byzantine_config = ByzantineConfig(enabled=True)
        bids = [
            AgentBid(f"a{i}", AgentRole.EXPLORER, 0.5, i % 2, "", 0.5)
            for i in range(6)
        ]
        winner, _ = coord.vote(bids, 2)
        self.assertIn(winner, (0, 1))

    def test_reputation_updates_on_agreement(self):
        coord = make_coordinator()
        rep = coord.get_reputation("a1")
        initial = rep.score
        coord.record_outcome("a1", agreed_with_winner=True)
        self.assertGreater(rep.score, initial)
        self.assertEqual(rep.observations, 1)

    def test_reputation_updates_on_disagreement(self):
        coord = make_coordinator()
        rep = coord.get_reputation("a1")
        initial = rep.score
        coord.record_outcome("a1", agreed_with_winner=False)
        self.assertLess(rep.score, initial)

    def test_reputation_bounded(self):
        coord = make_coordinator()
        for _ in range(200):
            coord.record_outcome("a1", agreed_with_winner=True)
        self.assertLessEqual(coord.get_reputation("a1").score, 1.0)
        for _ in range(200):
            coord.record_outcome("a2", agreed_with_winner=False)
        self.assertGreaterEqual(coord.get_reputation("a2").score, 0.0)

    def test_low_reputation_agent_filtered(self):
        coord = make_coordinator()
        coord.byzantine_config = ByzantineConfig(
            enabled=True,
            trim_quantile=0.0,
            min_reputation=0.5,
        )
        coord.get_reputation("byz").score = 0.0
        coord.get_reputation("honest1").score = 0.9
        coord.get_reputation("honest2").score = 0.9

        bids = [
            AgentBid("honest1", AgentRole.EXPLORER, 0.5, 0, "", 0.5),
            AgentBid("honest2", AgentRole.EXPLORER, 0.5, 0, "", 0.5),
            AgentBid("byz", AgentRole.EXPLORER, 0.99, 1, "", 0.99),
        ]
        winner, _ = coord.vote(bids, 2)
        self.assertEqual(winner, 0)

    def test_byzantine_config_roundtrip(self):
        coord = make_coordinator()
        coord.byzantine_config = ByzantineConfig(enabled=True, min_reputation=0.4)
        coord.get_reputation("a1").score = 0.77
        data = coord.to_dict()
        coord2 = MultiAgentCoordinator.from_dict(data, rng=random.Random(0))
        self.assertTrue(coord2.byzantine_config.enabled)
        self.assertAlmostEqual(
            coord2.get_reputation("a1").score, 0.77, places=6
        )


# --------------------------------------------------------------------------- #
# 11. MultiAgentTeam façade
# --------------------------------------------------------------------------- #
class TestMultiAgentTeam(unittest.TestCase):
    # ---- basic pipeline ------------------------------------------------ #
    def test_decide_returns_full_team_result(self):
        team = make_team()
        bids = [
            AgentBid("a1", AgentRole.EXPLORER, 0.9, 0, "", 0.5),
            AgentBid("a2", AgentRole.EXPLOITER, 0.5, 1, "", 0.5),
        ]
        result = team.decide(bids, 2)
        self.assertIsInstance(result, TeamResult)
        self.assertIsInstance(result.final_vote, VoteResult)
        self.assertIsInstance(result.negotiation, NegotiationResult)
        self.assertGreaterEqual(len(result.coalitions), 1)
        self.assertEqual(result.final_bids, result.negotiation.final_bids)

    def test_decide_deterministic_with_seed(self):
        bids = [
            AgentBid("a1", AgentRole.EXPLORER, 0.5, 0, "", 0.5),
            AgentBid("a2", AgentRole.EXPLORER, 0.5, 1, "", 0.5),
        ]
        r1 = make_team(seed=42).decide(bids, 2)
        r2 = make_team(seed=42).decide(bids, 2)
        self.assertEqual(r1.winner, r2.winner)
        self.assertEqual(r1.rounds_used, r2.rounds_used)

    def test_skip_negotiation(self):
        team = make_team(enable_negotiation=False)
        bids = [
            AgentBid("a1", AgentRole.EXPLORER, 0.9, 0, "", 0.5),
            AgentBid("a2", AgentRole.EXPLORER, 0.1, 1, "", 0.5),
        ]
        result = team.decide(bids, 2)
        self.assertIsNone(result.negotiation)
        self.assertTrue(result.converged)
        self.assertEqual(result.rounds_used, 0)
        self.assertEqual(result.final_bids, bids)
        self.assertEqual(result.winner, 0)

    def test_skip_coalitions(self):
        team = make_team(enable_coalitions=False)
        bids = [
            AgentBid("a1", AgentRole.EXPLORER, 0.9, 0, "", 0.5),
            AgentBid("a2", AgentRole.EXPLORER, 0.5, 1, "", 0.5),
        ]
        result = team.decide(bids, 2)
        self.assertEqual(result.coalitions, [])
        self.assertIsInstance(result.final_vote, VoteResult)

    def test_per_call_override_of_stages(self):
        team = make_team(enable_negotiation=True, enable_coalitions=True)
        bids = [AgentBid("a", AgentRole.EXPLORER, 0.5, 0, "", 0.5)]
        result = team.decide(bids, 1, negotiate=False, form_coalitions=False)
        self.assertIsNone(result.negotiation)
        self.assertEqual(result.coalitions, [])

    # ---- guard paths --------------------------------------------------- #
    def test_empty_bids(self):
        team = make_team()
        result = team.decide([], 3)
        self.assertEqual(result.winner, 0)
        self.assertIsNone(result.winning_role)
        self.assertEqual(result.rounds_used, 0)
        self.assertEqual(result.coalitions, [])

    def test_zero_candidates(self):
        team = make_team()
        result = team.decide(
            [AgentBid("a", AgentRole.EXPLORER, 0.5, 0, "", 0.5)], 0
        )
        self.assertEqual(result.winner, 0)
        self.assertEqual(result.rounds_used, 0)

    # ---- negotiation effects on final vote ----------------------------- #
    def test_negotiation_can_change_winner(self):
        team = make_team(
            seed=0,
            max_rounds=3,
            low_confidence_threshold=0.9,
            defection_probability=1.0,
        )
        bids = [
            AgentBid("strong", AgentRole.EXPLORER, 0.9, 0, "", 0.5),
            AgentBid("weak1", AgentRole.EXPLORER, 0.1, 1, "", 0.5),
            AgentBid("weak2", AgentRole.EXPLORER, 0.1, 1, "", 0.5),
        ]
        result = team.decide(bids, 2)
        self.assertEqual(result.winner, 0)
        shifted = {b.agent_id for b in result.final_bids if b.proposed_action == 0}
        self.assertIn("weak1", shifted)
        self.assertIn("weak2", shifted)

    # ---- coalitions reflect final bids --------------------------------- #
    def test_coalitions_use_negotiated_bids(self):
        team = make_team(
            seed=0,
            max_rounds=2,
            low_confidence_threshold=0.9,
            defection_probability=1.0,
        )
        bids = [
            AgentBid("strong", AgentRole.EXPLORER, 0.9, 0, "", 0.5),
            AgentBid("weak", AgentRole.EXPLORER, 0.1, 1, "", 0.5),
        ]
        result = team.decide(bids, 2)
        action_0_coalition = next(
            (c for c in result.coalitions if c.action == 0), None
        )
        self.assertIsNotNone(action_0_coalition)
        self.assertIn("weak", action_0_coalition.agent_ids)

    def test_coalitions_sorted_by_weight(self):
        team = make_team(enable_negotiation=False)
        bids = [
            AgentBid("heavy", AgentRole.EXPLORER, 0.95, 0, "", 0.9),
            AgentBid("light", AgentRole.EXPLORER, 0.05, 1, "", 0.05),
        ]
        result = team.decide(bids, 2)
        self.assertGreaterEqual(
            result.coalitions[0].total_weight,
            result.coalitions[-1].total_weight,
        )

    # ---- outcome feedback ---------------------------------------------- #
    def test_observe_outcome_updates_reputation(self):
        team = make_team(enable_negotiation=False)
        bids = [
            AgentBid("a1", AgentRole.EXPLORER, 0.9, 0, "", 0.5),
            AgentBid("a2", AgentRole.EXPLORER, 0.5, 1, "", 0.5),
        ]
        team.decide(bids, 2)
        before = team.coordinator.get_reputation("a1").score
        team.observe_outcome(
            winner=0, bids=bids, n_candidates=2, reward=1.0, success=True
        )
        after = team.coordinator.get_reputation("a1").score
        self.assertGreater(after, before)
        self.assertLess(team.coordinator.get_reputation("a2").score, before)

    def test_observe_outcome_updates_registry(self):
        team = make_team(enable_negotiation=False)
        bids = [AgentBid("a1", AgentRole.EXPLORER, 0.9, 0, "", 0.5)]
        team.decide(bids, 1)
        before = team.registry.success_rate(AgentRole.EXPLORER)
        team.observe_outcome(
            winner=0, bids=bids, n_candidates=1, reward=1.0, success=True
        )
        self.assertGreater(team.registry.success_rate(AgentRole.EXPLORER), before)

    def test_observe_outcome_noop_on_degenerate_input(self):
        team = make_team()
        team.observe_outcome(0, [], 0, reward=1.0, success=True)

    # ---- run_episode --------------------------------------------------- #
    def test_run_episode_calls_outcome_fn(self):
        team = make_team(enable_negotiation=False)
        calls = []

        def outcome_fn(winner, role):
            calls.append((winner, role))
            return True, 2.5

        bids = [AgentBid("a1", AgentRole.EXPLORER, 0.9, 0, "", 0.5)]
        result = team.run_episode(bids, 1, outcome_fn)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], result.winner)
        self.assertEqual(calls[0][1], result.winning_role)

    def test_run_episode_updates_after_success(self):
        team = make_team(enable_negotiation=False)
        bids = [AgentBid("a1", AgentRole.EXPLORER, 0.9, 0, "", 0.5)]
        before_rep = team.coordinator.get_reputation("a1").score
        before_reg = team.registry.success_rate(AgentRole.EXPLORER)
        team.run_episode(bids, 1, lambda w, r: (True, 1.0))
        self.assertGreater(team.coordinator.get_reputation("a1").score, before_rep)
        self.assertGreater(
            team.registry.success_rate(AgentRole.EXPLORER), before_reg
        )

    # ---- serialization ------------------------------------------------- #
    def test_team_result_serializable(self):
        team = make_team()
        bids = [AgentBid("a", AgentRole.EXPLORER, 0.5, 0, "", 0.5)]
        result = team.decide(bids, 1)
        d = result.to_dict()
        for key in (
            "winner", "winning_role", "final_vote",
            "negotiation", "coalitions", "converged",
            "rounds_used", "final_bids",
        ):
            self.assertIn(key, d)

    def test_team_roundtrip(self):
        team = make_team()
        team.decide([AgentBid("a", AgentRole.EXPLORER, 0.9, 0, "", 0.5)], 1)
        team.observe_outcome(
            winner=0,
            bids=[AgentBid("a", AgentRole.EXPLORER, 0.9, 0, "", 0.5)],
            n_candidates=1,
            reward=1.0,
            success=True,
        )
        data = team.to_dict()
        team2 = MultiAgentTeam.from_dict(data, seed=0)
        self.assertAlmostEqual(
            team2.coordinator.get_reputation("a").score,
            team.coordinator.get_reputation("a").score,
            places=6,
        )
        self.assertEqual(
            team2.config.enable_negotiation, team.config.enable_negotiation
        )
        self.assertEqual(
            set(team2.config.voting.role_bias.keys()),
            set(team.config.voting.role_bias.keys()),
        )

    def test_team_last_result_accessor(self):
        team = make_team()
        self.assertIsNone(team.last_result())
        team.decide([AgentBid("a", AgentRole.EXPLORER, 0.5, 0, "", 0.5)], 1)
        self.assertIsNotNone(team.last_result())
        self.assertIsNotNone(team.last_result_dict())

    # ---- integration: multi-episode learning --------------------------- #
    def test_multi_episode_reputation_diverges(self):
        team = make_team(enable_negotiation=False, seed=0)
        honest = AgentBid("honest", AgentRole.EXPLORER, 0.9, 0, "", 0.5)
        byz = AgentBid("byz", AgentRole.EXPLORER, 0.1, 1, "", 0.5)
        for _ in range(10):
            team.run_episode([honest, byz], 2, lambda w, r: (True, 1.0))
        self.assertGreater(
            team.coordinator.get_reputation("honest").score,
            team.coordinator.get_reputation("byz").score,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
