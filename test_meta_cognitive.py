#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test Suite: Meta-Cognitive Architecture

Tests all meta-cognitive components including reflection, long-context reasoning,
and sustained memory.
"""

import unittest
import json
import os
from src.monitoring.metrics_collector import MetricsCollector
from src.reflection.reflection_engine import ReflectionEngine
from src.reflection.long_context_reasoner import LongContextReasoner
from src.policy.policy_engine import PolicyEngine
from src.policy.policy_feedback import PolicyFeedback
from src.analysis.pareto_analyzer import ParetoAnalyzer
from src.memory.run_memory import RunMemory
from src.dashboard.green_dashboard import GreenDashboard


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
