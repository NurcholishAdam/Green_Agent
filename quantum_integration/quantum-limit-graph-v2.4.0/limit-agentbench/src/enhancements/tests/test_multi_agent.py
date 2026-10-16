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
    NegotiationConfig,
    NegotiationResult,
    RegistryConfig,
    RoleStats,
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
        # With exploration=1.0, we should hit several roles.
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
        # role_totals exposes the breakdown.
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
            AgentBid("a1", AgentRole.EXPLORER, 0.5, 0, "", 0.1),  # low carbon
            AgentBid("a2", AgentRole.EXPLORER, 0.5, 1, "", 0.9),  # high carbon
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
        # Even after clamping, high raw carbon should still dominate.
        coord = make_coordinator()
        bids = [
            AgentBid("a1", AgentRole.EXPLORER, 0.5, 0, "", -5.0),  # → 0.0
            AgentBid("a2", AgentRole.EXPLORER, 0.5, 1, "", 99.0),  # → 1.0
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
            # Proposal 0 with more weight than the safety officer's proposal 1.
            AgentBid("a1", AgentRole.EXPLOITER, 0.9, 0, "", 0.9),
            AgentBid("a2", AgentRole.SAFETY_OFFICER, 0.9, 1, "", 0.5),
        ]
        winner, role = coord.vote(bids, 2)
        self.assertEqual(winner, 0)
        self.assertIsNone(role)  # vetoed → no winning role
        self.assertTrue(coord.last_vote().vetoed)

    def test_safety_no_veto_below_threshold(self):
        # Safety officer confidence below threshold → no veto.
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
        coord.vote(
            [AgentBid("a", AgentRole.EXPLORER, 0.7, 0, "", 0.5)], 1
        )
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
        coord.vote(
            [AgentBid("a", AgentRole.EXPLORER, 0.7, 0, "", 0.5)], 1
        )
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
        coord.vote(
            [AgentBid("a", AgentRole.EXPLORER, 0.7, 0, "", 0.5)], 1
        )
        self.assertEqual(len(seen), 1)
        self.assertIsInstance(seen[0], VoteResult)

    def test_hook_exception_does_not_break_vote(self):
        coord = make_coordinator()

        def bad_hook(_):
            raise RuntimeError("boom")

        coord.add_hook(bad_hook)
        # Should not raise.
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
            convergence_threshold=2.0,       # never converge on its own
            defection_probability=1.0,       # always defect
            low_confidence_threshold=0.9,
        )
        bids = [
            AgentBid("strong", AgentRole.EXPLORER, 0.95, 0, "", 0.5),
            AgentBid("weak", AgentRole.EXPLORER, 0.1, 1, "", 0.5),
        ]
        result = coord.negotiate(bids, 2, config=cfg)
        # After the first round, "weak" should have shifted to action 0.
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
        # The 2-agent coalition should rank first (higher total weight).
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
        # Ensure the "byzantine" agent has low reputation.
        coord.get_reputation("byz").score = 0.0
        coord.get_reputation("honest1").score = 0.9
        coord.get_reputation("honest2").score = 0.9

        bids = [
            AgentBid("honest1", AgentRole.EXPLORER, 0.5, 0, "", 0.5),
            AgentBid("honest2", AgentRole.EXPLORER, 0.5, 0, "", 0.5),
            # Without filtering, this would win (high conf + high carbon).
            AgentBid("byz", AgentRole.EXPLORER, 0.99, 1, "", 0.99),
        ]
        winner, _ = coord.vote(bids, 2)
        self.assertEqual(winner, 0)

    def test_byzantine_config_roundtrip(self):
        coord = make_coordinator()
        coord.byzantine_config = ByzantineConfig(
            enabled=True, min_reputation=0.4
        )
        coord.get_reputation("a1").score = 0.77
        data = coord.to_dict()
        coord2 = MultiAgentCoordinator.from_dict(data, rng=random.Random(0))
        self.assertTrue(coord2.byzantine_config.enabled)
        self.assertAlmostEqual(
            coord2.get_reputation("a1").score, 0.77, places=6
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
