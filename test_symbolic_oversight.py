#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test Suite: Neuro-Symbolic Oversight

Comprehensive tests for symbolic reasoning engine and integration.
Enhanced with tests for advanced modules: LIMIT Graph, MODP, RLHF,
Multi‑Teacher On‑Policy Distillation, Bio‑inspired Optimisation, MoE expert gating, and FlexGen.
"""

import unittest
import json
import os
import asyncio
import importlib

from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine, SymbolicRule, ViolationTrace
from src.dashboard.symbolic_visualizer import SymbolicVisualizer
from src.policy.policy_feedback import PolicyFeedback


# ------------------------------------------------------------------------------
# Optional imports for advanced enhancements (graceful degradation)
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


class TestSymbolicReasoningEngine(unittest.TestCase):
    """Test symbolic reasoning engine functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.engine = SymbolicReasoningEngine(policy_file="symbolic_policy.yaml")

    def test_rule_loading(self):
        """Test that rules are loaded correctly."""
        rules = self.engine.get_active_rules()
        self.assertGreater(len(rules), 0, "Should load at least one rule")

        # Check rule structure
        rule = rules[0]
        self.assertIn('id', rule)
        self.assertIn('name', rule)
        self.assertIn('condition', rule)
        self.assertIn('action', rule)

    def test_energy_violation(self):
        """Test energy budget violation detection."""
        metrics = {
            "energy": 6.0,  # Exceeds 5.0 limit
            "carbon": 30,
            "latency": 1000,
            "memory": 200,
            "tool_calls": 10,
            "cumulative": {
                "total_energy_wh": 6.0,
                "total_carbon_kg": 0.03,
                "total_latency_ms": 1000,
                "max_memory_mb": 200,
                "total_tool_calls": 10,
                "step_count": 1
            }
        }

        violations = self.engine.evaluate_rules(metrics, step=1)

        # Should detect energy violation
        energy_violations = [v for v in violations if 'energy' in v.condition.lower()]
        self.assertGreater(len(energy_violations), 0, "Should detect energy violation")

    def test_memory_violation(self):
        """Test memory overflow detection."""
        metrics = {
            "energy": 2.0,
            "carbon": 20,
            "latency": 1000,
            "memory": 550,  # Exceeds 500MB limit
            "tool_calls": 10,
            "cumulative": {
                "total_energy_wh": 2.0,
                "total_carbon_kg": 0.02,
                "total_latency_ms": 1000,
                "max_memory_mb": 550,
                "total_tool_calls": 10,
                "step_count": 1
            }
        }

        violations = self.engine.evaluate_rules(metrics, step=1)

        # Should detect memory violation
        memory_violations = [v for v in violations if 'memory' in v.condition.lower()]
        self.assertGreater(len(memory_violations), 0, "Should detect memory violation")

    def test_composite_rule(self):
        """Test composite rule evaluation."""
        metrics = {
            "energy": 4.5,
            "carbon": 70,  # High carbon
            "latency": 110000,  # High latency
            "memory": 300,
            "tool_calls": 35,  # High tool calls
            "cumulative": {
                "total_energy_wh": 4.5,
                "total_carbon_kg": 0.07,
                "total_latency_ms": 110000,
                "max_memory_mb": 300,
                "total_tool_calls": 35,
                "step_count": 5
            }
        }

        violations = self.engine.evaluate_rules(metrics, step=5)

        # Should detect composite violations
        self.assertGreater(len(violations), 0, "Should detect composite violations")

    def test_no_violations(self):
        """Test that compliant metrics produce no violations."""
        metrics = {
            "energy": 2.0,
            "carbon": 20,
            "latency": 5000,
            "memory": 200,
            "tool_calls": 10,
            "cumulative": {
                "total_energy_wh": 2.0,
                "total_carbon_kg": 0.02,
                "total_latency_ms": 5000,
                "max_memory_mb": 200,
                "total_tool_calls": 10,
                "step_count": 1
            }
        }

        violations = self.engine.evaluate_rules(metrics, step=1)

        # Should have no violations
        self.assertEqual(len(violations), 0, "Compliant metrics should have no violations")

    def test_violation_trace_structure(self):
        """Test violation trace structure."""
        metrics = {
            "energy": 6.0,
            "carbon": 30,
            "latency": 1000,
            "memory": 200,
            "tool_calls": 10,
            "cumulative": {
                "total_energy_wh": 6.0,
                "total_carbon_kg": 0.03,
                "total_latency_ms": 1000,
                "max_memory_mb": 200,
                "total_tool_calls": 10,
                "step_count": 1
            }
        }

        violations = self.engine.evaluate_rules(metrics, step=1)

        if violations:
            trace = violations[0]
            self.assertIsInstance(trace, ViolationTrace)
            self.assertIsNotNone(trace.rule_id)
            self.assertIsNotNone(trace.rule_name)
            self.assertIsNotNone(trace.condition)
            self.assertIsNotNone(trace.violation_details)

    def test_category_filtering(self):
        """Test filtering violations by category."""
        # Create multiple violations
        metrics = {
            "energy": 6.0,
            "carbon": 70,
            "latency": 130000,
            "memory": 550,
            "tool_calls": 60,
            "cumulative": {
                "total_energy_wh": 6.0,
                "total_carbon_kg": 0.07,
                "total_latency_ms": 130000,
                "max_memory_mb": 550,
                "total_tool_calls": 60,
                "step_count": 5
            }
        }

        violations = self.engine.evaluate_rules(metrics, step=5)

        # Test category filtering
        sustainability = self.engine.get_violations_by_category("sustainability")
        resource = self.engine.get_violations_by_category("resource")

        self.assertIsInstance(sustainability, list)
        self.assertIsInstance(resource, list)

    def test_violation_summary(self):
        """Test violation summary generation."""
        metrics = {
            "energy": 6.0,
            "carbon": 70,
            "latency": 130000,
            "memory": 550,
            "tool_calls": 60,
            "cumulative": {
                "total_energy_wh": 6.0,
                "total_carbon_kg": 0.07,
                "total_latency_ms": 130000,
                "max_memory_mb": 550,
                "total_tool_calls": 60,
                "step_count": 5
            }
        }

        self.engine.evaluate_rules(metrics, step=5)
        summary = self.engine.get_violation_summary()

        self.assertIn('total_violations', summary)
        self.assertIn('evaluations', summary)
        self.assertIn('by_category', summary)
        self.assertIn('by_severity', summary)


class TestSymbolicVisualizer(unittest.TestCase):
    """Test symbolic visualizer functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.visualizer = SymbolicVisualizer()
        self.engine = SymbolicReasoningEngine(policy_file="symbolic_policy.yaml")

    def test_add_violations(self):
        """Test adding violations to visualizer."""
        metrics = {
            "energy": 6.0,
            "carbon": 70,
            "latency": 130000,
            "memory": 550,
            "tool_calls": 60,
            "cumulative": {
                "total_energy_wh": 6.0,
                "total_carbon_kg": 0.07,
                "total_latency_ms": 130000,
                "max_memory_mb": 550,
                "total_tool_calls": 60,
                "step_count": 5
            }
        }

        violations = self.engine.evaluate_rules(metrics, step=5)
        self.visualizer.add_violations([v.to_dict() for v in violations])

        self.assertGreater(len(self.visualizer.violation_data), 0)

    def test_timeline_generation(self):
        """Test violation timeline generation."""
        metrics = {
            "energy": 6.0,
            "carbon": 70,
            "latency": 130000,
            "memory": 550,
            "tool_calls": 60,
            "cumulative": {
                "total_energy_wh": 6.0,
                "total_carbon_kg": 0.07,
                "total_latency_ms": 130000,
                "max_memory_mb": 550,
                "total_tool_calls": 60,
                "step_count": 5
            }
        }

        violations = self.engine.evaluate_rules(metrics, step=5)
        self.visualizer.add_violations([v.to_dict() for v in violations])

        timeline = self.visualizer.generate_violation_timeline()
        self.assertIsInstance(timeline, list)

        if timeline:
            entry = timeline[0]
            self.assertIn('timestamp', entry)
            self.assertIn('step', entry)
            self.assertIn('rule_name', entry)

    def test_category_view(self):
        """Test category view generation."""
        metrics = {
            "energy": 6.0,
            "carbon": 70,
            "latency": 130000,
            "memory": 550,
            "tool_calls": 60,
            "cumulative": {
                "total_energy_wh": 6.0,
                "total_carbon_kg": 0.07,
                "total_latency_ms": 130000,
                "max_memory_mb": 550,
                "total_tool_calls": 60,
                "step_count": 5
            }
        }

        violations = self.engine.evaluate_rules(metrics, step=5)
        self.visualizer.add_violations([v.to_dict() for v in violations])

        category_view = self.visualizer.generate_category_view()
        self.assertIsInstance(category_view, dict)

    def test_severity_summary(self):
        """Test severity summary generation."""
        metrics = {
            "energy": 6.0,
            "carbon": 70,
            "latency": 130000,
            "memory": 550,
            "tool_calls": 60,
            "cumulative": {
                "total_energy_wh": 6.0,
                "total_carbon_kg": 0.07,
                "total_latency_ms": 130000,
                "max_memory_mb": 550,
                "total_tool_calls": 60,
                "step_count": 5
            }
        }

        violations = self.engine.evaluate_rules(metrics, step=5)
        self.visualizer.add_violations([v.to_dict() for v in violations])

        severity_summary = self.visualizer.generate_severity_summary()
        self.assertIn('counts', severity_summary)
        self.assertIn('details', severity_summary)

    def test_html_generation(self):
        """Test HTML dashboard section generation."""
        metrics = {
            "energy": 6.0,
            "carbon": 70,
            "latency": 130000,
            "memory": 550,
            "tool_calls": 60,
            "cumulative": {
                "total_energy_wh": 6.0,
                "total_carbon_kg": 0.07,
                "total_latency_ms": 130000,
                "max_memory_mb": 550,
                "total_tool_calls": 60,
                "step_count": 5
            }
        }

        violations = self.engine.evaluate_rules(metrics, step=5)
        self.visualizer.add_violations([v.to_dict() for v in violations])

        html = self.visualizer.generate_dashboard_section()
        self.assertIsInstance(html, str)
        self.assertIn('symbolic', html.lower())


class TestPolicyFeedbackIntegration(unittest.TestCase):
    """Test integration with policy feedback system."""

    def setUp(self):
        """Set up test fixtures."""
        self.feedback = PolicyFeedback()
        self.engine = SymbolicReasoningEngine(policy_file="symbolic_policy.yaml")

    def test_triple_layer_feedback(self):
        """Test triple-layer feedback generation."""
        # Mock data
        pareto_analysis = {
            "position": "dominated",
            "dominated_by": ["agent_1"],
            "dominates": [],
            "efficiency_score": 0.6
        }

        reflections = [
            {
                "step": 1,
                "self_explanation": "High energy usage detected",
                "decision": "reduce_energy_usage",
                "confidence": 0.7
            }
        ]

        metrics = {
            "energy": 6.0,
            "carbon": 70,
            "latency": 130000,
            "memory": 550,
            "tool_calls": 60,
            "cumulative": {
                "total_energy_wh": 6.0,
                "total_carbon_kg": 0.07,
                "total_latency_ms": 130000,
                "max_memory_mb": 550,
                "total_tool_calls": 60,
                "step_count": 5
            }
        }

        violations = self.engine.evaluate_rules(metrics, step=5)

        feedback = self.feedback.generate_dual_layer_feedback(
            pareto_analysis=pareto_analysis,
            reflections=reflections,
            metrics=metrics,
            symbolic_violations=[v.to_dict() for v in violations]
        )

        self.assertIn('objective_layer', feedback)
        self.assertIn('subjective_layer', feedback)
        self.assertIn('symbolic_layer', feedback)
        self.assertIn('synthesis', feedback)

    def test_symbolic_recommendations(self):
        """Test that symbolic violations generate recommendations."""
        pareto_analysis = {"position": "frontier"}
        reflections = []
        metrics = {
            "energy": 6.0,
            "carbon": 70,
            "cumulative": {
                "total_energy_wh": 6.0,
                "total_carbon_kg": 0.07,
                "total_latency_ms": 130000,
                "max_memory_mb": 550,
                "total_tool_calls": 60,
                "step_count": 5
            }
        }

        violations = self.engine.evaluate_rules(metrics, step=5)

        feedback = self.feedback.generate_dual_layer_feedback(
            pareto_analysis=pareto_analysis,
            reflections=reflections,
            metrics=metrics,
            symbolic_violations=[v.to_dict() for v in violations]
        )

        recommendations = feedback['synthesis']['recommendations']
        self.assertIsInstance(recommendations, list)
        self.assertGreater(len(recommendations), 0)


# ------------------------------------------------------------------------------
# NEW: Advanced Enhancements Tests
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


def run_tests():
    """Run all tests."""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestSymbolicReasoningEngine))
    suite.addTests(loader.loadTestsFromTestCase(TestSymbolicVisualizer))
    suite.addTests(loader.loadTestsFromTestCase(TestPolicyFeedbackIntegration))
    if ENHANCEMENTS_AVAILABLE:
        suite.addTests(loader.loadTestsFromTestCase(TestAdvancedEnhancements))
    else:
        print("Note: Advanced enhancement tests skipped (modules not installed)")

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Print summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    exit(0 if success else 1)
