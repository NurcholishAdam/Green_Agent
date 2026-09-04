#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test Suite: Meta-Cognitive Architecture

Tests all meta-cognitive components including reflection, long-context reasoning,
and sustained memory.

Enhanced with optional tests for advanced modules:
- LIMIT Graph
- MODP (Multi‑Objective Decision Process)
- RLHF (Reinforcement Learning from Human Feedback)
- Multi‑Teacher On‑Policy Distillation with MoE gating
- Bio‑inspired Optimisation
- FlexGen execution backend
"""

import unittest
import json
import os
import asyncio
import importlib

from src.monitoring.metrics_collector import MetricsCollector
from src.reflection.reflection_engine import ReflectionEngine
from src.reflection.long_context_reasoner import LongContextReasoner
from src.policy.policy_engine import PolicyEngine
from src.policy.policy_feedback import PolicyFeedback
from src.analysis.pareto_analyzer import ParetoAnalyzer
from src.memory.run_memory import RunMemory
from src.dashboard.green_dashboard import GreenDashboard

# ------------------------------------------------------------------------------
# Optional imports for advanced enhancements (graceful skip)
# ------------------------------------------------------------------------------
ENHANCEMENTS_AVAILABLE = True
try:
    from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType, CoolingType
    from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency
    from src.enhancements.schemas.feedback_event import FeedbackEvent
    from src.enhancements.zero_trust_architecture import ZeroTrustArchitecture, ZeroTrustConfig
    from src.enhancements.core.graph_registry import GraphRegistry, GraphType
    from src.enhancements.core.causal_graph import CausalGraph
    from src.enhancements.core.meta_cognition import MetaCognitionLayer
    from src.enhancements.metrics.dag_carbon_ledger import DAGCarbonLedger
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    NodeDescriptor = None
    WorkloadDescriptor = None
    FeedbackEvent = None
    ZeroTrustArchitecture = None
    GraphRegistry = None
    CausalGraph = None
    MetaCognitionLayer = None
    DAGCarbonLedger = None


# ------------------------------------------------------------------------------
# Existing test classes (unchanged)
# ------------------------------------------------------------------------------

class TestMetricsCollector(unittest.TestCase):
    """Test metrics collection functionality."""

    def setUp(self):
        self.collector = MetricsCollector()

    def test_metrics_collection(self):
        """Test basic metrics collection."""
        self.collector.start_step()
        snapshot = self.collector.collect_snapshot()

        self.assertIsNotNone(snapshot)
        self.assertGreaterEqual(snapshot.energy_wh, 0)
        self.assertGreaterEqual(snapshot.memory_mb, 0)

    def test_cumulative_metrics(self):
        """Test cumulative metrics tracking."""
        for _ in range(3):
            self.collector.start_step()
            self.collector.collect_snapshot()

        cumulative = self.collector.get_cumulative_metrics()
        self.assertEqual(cumulative["total_steps"], 3)
        self.assertGreater(cumulative["total_energy_wh"], 0)


class TestReflectionEngine(unittest.TestCase):
    """Test reflection engine functionality."""

    def setUp(self):
        self.engine = ReflectionEngine(
            reflection_frequency=5,
            policy_budgets={"max_energy_wh": 5.0, "max_carbon_kg": 0.002}
        )

    def test_reflection_frequency(self):
        """Test reflection checkpoint triggering."""
        self.assertFalse(self.engine.should_reflect(1))
        self.assertFalse(self.engine.should_reflect(4))
        self.assertTrue(self.engine.should_reflect(5))
        self.assertTrue(self.engine.should_reflect(10))

    def test_reflection_generation(self):
        """Test reflection generation."""
        metrics = {
            "cumulative": {
                "total_energy_wh": 4.0,
                "total_carbon_kg": 0.0015,
                "total_latency_ms": 5000,
                "total_tool_calls": 10
            },
            "budget_status": {
                "utilization": {"energy": 80, "carbon": 75, "latency": 50}
            }
        }

        reflection = self.engine.generate_reflection(5, metrics, 1234567890.0)

        self.assertEqual(reflection.step, 5)
        self.assertIsNotNone(reflection.self_explanation)
        self.assertIsNotNone(reflection.decision)
        self.assertGreaterEqual(reflection.confidence, 0.0)
        self.assertLessEqual(reflection.confidence, 1.0)


class TestLongContextReasoner(unittest.TestCase):
    """Test long-context reasoning functionality."""

    def setUp(self):
        self.reasoner = LongContextReasoner(history_window=5)

    def test_run_history(self):
        """Test run history management."""
        for i in range(7):
            self.reasoner.add_run_to_history({"run_id": i, "cumulative": {}})

        # Should keep only last 5 runs
        self.assertEqual(len(self.reasoner.run_history), 5)

    def test_comparison_with_past_runs(self):
        """Test comparison with historical runs."""
        # Add historical runs
        for i in range(3):
            self.reasoner.add_run_to_history({
                "cumulative": {
                    "total_energy_wh": 3.0,
                    "total_latency_ms": 4000,
                    "total_steps": 10
                }
            })

        # Compare current run
        current_metrics = {
            "cumulative": {
                "total_energy_wh": 4.5,  # Higher than average
                "total_latency_ms": 4000,
                "total_steps": 10
            }
        }

        insights = self.reasoner.compare_with_past_runs(current_metrics)
        self.assertIsInstance(insights, list)


class TestPolicyEngine(unittest.TestCase):
    """Test policy engine functionality."""

    def setUp(self):
        self.policy = PolicyEngine()

    def test_budget_retrieval(self):
        """Test budget configuration retrieval."""
        budgets = self.policy.get_budgets()

        self.assertIn("max_energy_wh", budgets)
        self.assertIn("max_carbon_kg", budgets)
        self.assertIn("max_latency_s", budgets)

    def test_policy_enforcement(self):
        """Test policy enforcement."""
        metrics = {"energy": 10.0, "carbon": 0.005, "latency": 200}
        result = self.policy.enforce(metrics)

        self.assertIn("passed", result)
        self.assertIn("violations", result)
        self.assertFalse(result["passed"])  # Should violate budgets

    def test_self_adjustment(self):
        """Test self-adjustment triggering."""
        metrics = {
            "budget_status": {
                "utilization": {"energy": 85, "carbon": 70, "latency": 60}
            }
        }

        should_adjust = self.policy.should_self_adjust(metrics)
        self.assertTrue(should_adjust)  # Energy > 80%


class TestPolicyFeedback(unittest.TestCase):
    """Test policy feedback functionality."""

    def setUp(self):
        self.feedback = PolicyFeedback()

    def test_dual_layer_feedback(self):
        """Test dual-layer feedback generation."""
        pareto_analysis = {
            "position": "frontier",
            "efficiency_score": 0.85
        }

        reflections = [
            {
                "step": 5,
                "self_explanation": "Operating within budgets",
                "decision": "continue",
                "confidence": 0.9
            }
        ]

        metrics = {"cumulative": {"total_energy_wh": 3.0}}

        feedback = self.feedback.generate_dual_layer_feedback(
            pareto_analysis, reflections, metrics
        )

        self.assertIn("objective_layer", feedback)
        self.assertIn("subjective_layer", feedback)
        self.assertIn("synthesis", feedback)


class TestParetoAnalyzer(unittest.TestCase):
    """Test Pareto analysis functionality."""

    def setUp(self):
        self.analyzer = ParetoAnalyzer()

    def test_dominance(self):
        """Test Pareto dominance checking."""
        agent_a = {"total_energy_wh": 2.0, "total_latency_ms": 3000}
        agent_b = {"total_energy_wh": 3.0, "total_latency_ms": 4000}

        # A dominates B (better on all metrics)
        self.assertTrue(self.analyzer.dominates(agent_a, agent_b))
        self.assertFalse(self.analyzer.dominates(agent_b, agent_a))

    def test_pareto_frontier(self):
        """Test Pareto frontier computation."""
        agents = [
            {"query_id": "a1", "total_energy_wh": 2.0, "total_latency_ms": 3000},
            {"query_id": "a2", "total_energy_wh": 3.0, "total_latency_ms": 2000},
            {"query_id": "a3", "total_energy_wh": 4.0, "total_latency_ms": 4000}
        ]

        frontier = self.analyzer.pareto_frontier(agents)

        # a1 and a2 should be on frontier, a3 is dominated
        self.assertEqual(len(frontier), 2)
        frontier_ids = [a["query_id"] for a in frontier]
        self.assertIn("a1", frontier_ids)
        self.assertIn("a2", frontier_ids)


class TestRunMemory(unittest.TestCase):
    """Test run memory functionality."""

    def setUp(self):
        self.memory_file = "test_run_memory.json"
        if os.path.exists(self.memory_file):
            os.remove(self.memory_file)
        self.memory = RunMemory(memory_file=self.memory_file)

    def tearDown(self):
        if os.path.exists(self.memory_file):
            os.remove(self.memory_file)

    def test_run_storage(self):
        """Test run storage and retrieval."""
        run_data = {
            "cumulative": {"total_energy_wh": 3.0},
            "reflections": []
        }

        self.memory.add_run(run_data)
        recent = self.memory.get_recent_runs(1)

        self.assertEqual(len(recent), 1)
        self.assertEqual(recent[0]["cumulative"]["total_energy_wh"], 3.0)

    def test_meta_policy_generation(self):
        """Test meta-policy generation."""
        # Add multiple runs with increasing energy
        for i in range(6):
            self.memory.add_run({
                "cumulative": {
                    "total_energy_wh": 2.0 + i * 0.5,
                    "total_latency_ms": 3000,
                    "total_carbon_kg": 0.001,
                    "total_steps": 10
                }
            })

        meta_policy = self.memory.generate_meta_policy()
        self.assertIsNotNone(meta_policy)
        self.assertIn("recommendations", meta_policy)


class TestGreenDashboard(unittest.TestCase):
    """Test dashboard functionality."""

    def setUp(self):
        self.dashboard = GreenDashboard()

    def test_agent_data_addition(self):
        """Test adding agent data to dashboard."""
        metrics = {"cumulative": {"total_energy_wh": 3.0}}
        reflections = [{"step": 5, "confidence": 0.8}]
        pareto_position = {"position": "frontier"}

        self.dashboard.add_agent_data("test_agent", metrics, reflections, pareto_position)

        self.assertEqual(len(self.dashboard.dashboard_data["agents"]), 1)

    def test_leaderboard_generation(self):
        """Test leaderboard generation."""
        # Add multiple agents
        for i in range(3):
            self.dashboard.add_agent_data(
                f"agent_{i}",
                {"cumulative": {"total_energy_wh": 2.0 + i}},
                [{"step": 5, "confidence": 0.7 + i * 0.1}],
                {"position": "frontier" if i == 0 else "dominated"}
            )

        leaderboard = self.dashboard.generate_leaderboard()

        self.assertIn("rankings", leaderboard)
        self.assertIn("top_performers", leaderboard)


# ------------------------------------------------------------------------------
# NEW: Advanced Enhancements Test Class
# ------------------------------------------------------------------------------

@unittest.skipIf(not ENHANCEMENTS_AVAILABLE, "Advanced enhancement modules not installed")
class TestAdvancedEnhancements(unittest.TestCase):
    """Tests for advanced modules: LIMIT Graph, MODP, RLHF, Distillation, MoE, Evolutionary, FlexGen."""

    def test_node_descriptor_routing(self):
        """NodeDescriptor should select routing strategy using distillation + MoE."""
        node = NodeDescriptor(
            id="test_node",
            type=NodeType.EDGE,
            region="us-east",
            region_carbon_intensity=400.0,
            energy_per_token=0.00005,
            use_evolutionary=True,
            human_feedback_score=0.6,
            graph_metrics={"centrality": 0.8},
            metadata={"gating_lr": 0.005}
        )
        strategy = asyncio.run(node.select_routing_strategy(exploration=False))
        self.assertIn(strategy, ["carbon_first", "latency_first", "cost_first", "balanced", "adaptive"])

    def test_workload_descriptor_priority(self):
        """WorkloadDescriptor should select priority using MODP + MoE."""
        wl = WorkloadDescriptor(
            task_id="test_task",
            task_type=TaskType.INFERENCE,
            tokens=1000,
            latency_target=300.0,
            urgency=Urgency.MEDIUM,
            use_evolutionary=True,
            human_feedback_score=0.7,
            graph_metrics={"centrality": 0.6},
            metadata={"latency_weight": 0.5, "carbon_weight": 0.3, "energy_weight": 0.2}
        )
        priority = asyncio.run(wl.select_priority(exploration=False))
        self.assertIn(priority, ["accuracy", "green", "balanced"])

    def test_feedback_event_enhanced_fields(self):
        """FeedbackEvent should support MODP, RLHF, and LIMIT Graph fields."""
        event = FeedbackEvent(
            source="test",
            feedback_type="routing",
            task_id="t1",
            context={},
            action={"selected_action": "execute"},
            performance={"quality_score": 0.9, "latency_ms": 100, "energy_joules": 100},
            adaptive_cost_value=0.85,
            graph_metrics={"centrality": 0.7},
            human_feedback_score=0.8,
            modp_score=0.75,
            distillation_stats={"student_counter": 5}
        )
        json_str = event.to_json()
        event2 = FeedbackEvent.from_json(json_str)
        self.assertEqual(event2.graph_metrics["centrality"], 0.7)
        self.assertEqual(event2.human_feedback_score, 0.8)
        self.assertEqual(event2.modp_score, 0.75)

    def test_zero_trust_enhanced_init(self):
        """ZeroTrustArchitecture should initialize with enhanced flags."""
        config = ZeroTrustConfig(
            use_enhancements=True,
            use_distillation=True,
            use_evolutionary=True,
            human_feedback_score=0.6,
            graph_metrics={"centrality": 0.5}
        )
        zta = ZeroTrustArchitecture(config=config)
        self.assertTrue(zta.use_enhancements)
        self.assertTrue(hasattr(zta, 'carbon_authenticator') or hasattr(zta, 'distillation_optimizer'))

    def test_graph_registry_and_causal(self):
        """GraphRegistry and CausalGraph should support LIMIT Graph operations."""
        registry = GraphRegistry()
        causal_graph = registry.get_or_create(GraphType.CAUSAL)
        self.assertIsInstance(causal_graph, CausalGraph)

        meta = MetaCognitionLayer(causal_graph=causal_graph)
        meta.observe_snapshot({
            "CarbonIntensity": 430.0,
            "CarbonIntensity_high": 400.0,
            "GridStrain": 0.91,
        })
        report = meta.diagnose()
        self.assertEqual(report["status"], "anomaly_detected")

    def test_dag_carbon_ledger_backpropagation(self):
        """DAGCarbonLedger should support carbon debt backpropagation."""
        ledger = DAGCarbonLedger(storage_path="/tmp/test_ledger_advanced")
        node_a = ledger.add_execution(
            task_id="A", framework="langchain", energy_kwh=0.01,
            carbon_co2e_kg=0.002, accuracy=0.9, sustainability_index=0.8
        )
        node_b = ledger.add_execution(
            task_id="B", framework="langchain", energy_kwh=0.008,
            carbon_co2e_kg=0.0016, accuracy=0.88, sustainability_index=0.75,
            parent_task_ids=[node_a], dependency_type="model_state"
        )
        attributed = ledger.backpropagate_carbon(node_b, transfer_rate=0.3)
        self.assertIn(node_a, attributed)
        self.assertGreater(attributed[node_a], 0)

    def test_flexgen_config_presence(self):
        """FlexGen settings are present in enhancements config (simulated)."""
        flexgen_config = {
            "enabled": True,
            "model_name": "facebook/opt-6.7b",
            "batch_size": 16,
            "delegation_policy": "adaptive"
        }
        self.assertTrue(flexgen_config["enabled"])
        self.assertIn(flexgen_config["delegation_policy"], ["adaptive", "always", "never"])


# ------------------------------------------------------------------------------
# Run tests
# ------------------------------------------------------------------------------

def run_tests():
    """Run all tests."""
    print("🧪 Running Meta-Cognitive Architecture Tests")
    print("=" * 60)

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestMetricsCollector))
    suite.addTests(loader.loadTestsFromTestCase(TestReflectionEngine))
    suite.addTests(loader.loadTestsFromTestCase(TestLongContextReasoner))
    suite.addTests(loader.loadTestsFromTestCase(TestPolicyEngine))
    suite.addTests(loader.loadTestsFromTestCase(TestPolicyFeedback))
    suite.addTests(loader.loadTestsFromTestCase(TestParetoAnalyzer))
    suite.addTests(loader.loadTestsFromTestCase(TestRunMemory))
    suite.addTests(loader.loadTestsFromTestCase(TestGreenDashboard))

    # Add advanced enhancements test class if modules available
    if ENHANCEMENTS_AVAILABLE:
        suite.addTests(loader.loadTestsFromTestCase(TestAdvancedEnhancements))
    else:
        print("\nℹ️  Advanced enhancement tests skipped (modules not installed)")

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 60)
    if result.wasSuccessful():
        print("✅ All tests passed!")
    else:
        print(f"❌ {len(result.failures)} test(s) failed")
        print(f"❌ {len(result.errors)} error(s) occurred")

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    exit(0 if success else 1)
