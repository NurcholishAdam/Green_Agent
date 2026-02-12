#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test Suite: Neuro-Symbolic Oversight

Comprehensive tests for symbolic reasoning engine and integration.
"""

import unittest
import json
import os
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine, SymbolicRule, ViolationTrace
from src.dashboard.symbolic_visualizer import SymbolicVisualizer
from src.policy.policy_feedback import PolicyFeedback


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


def run_tests():
    """Run all tests."""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestSymbolicReasoningEngine))
    suite.addTests(loader.loadTestsFromTestCase(TestSymbolicVisualizer))
    suite.addTests(loader.loadTestsFromTestCase(TestPolicyFeedbackIntegration))
    
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
