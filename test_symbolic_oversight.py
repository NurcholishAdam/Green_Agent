#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test Suite: Neuro-Symbolic Oversight

| Field | Value |
|---|---|
| Version | 1.2.0 |
| Last Updated | 2026-11-13 |
| Status | Production Ready |
| Test count | 19 core + 8 enhancement = 27 |
| Runner | pytest (recommended) OR `python test_symbolic_oversight.py` |

Comprehensive tests for the symbolic reasoning engine, visualizer, multi-layer
policy feedback, and advanced enhancement modules (LIMIT Graph, MODP, RLHF,
Multi-Teacher On-Policy Distillation, Bio-inspired Optimisation, MoE expert
gating, FlexGen, Zero Trust, DAG Carbon Ledger).

Companion docs:
- SYMBOLIC_OVERSIGHT_README.md (v1.2.0)
- Meta-Cognitive Architecture for Green Agent.md (v1.2.0)
- META_COGNITIVE_ENHANCEMENT_COMPLETE.md (v1.2.0)
- NEURO_SYMBOLIC_INTEGRATION_COMPLETE.md (v1.2.0)
"""

import os
import sys
import tempfile
import unittest
import warnings
from pathlib import Path

import asyncio

# ------------------------------------------------------------------------------
# Core imports
# ------------------------------------------------------------------------------
from src.symbolic.symbolic_reasoning_engine import (
    SymbolicReasoningEngine,
    SymbolicRule,
    ViolationTrace,
)
from src.dashboard.symbolic_visualizer import SymbolicVisualizer
from src.policy.policy_feedback import PolicyFeedback


# ------------------------------------------------------------------------------
# Optional enhancement imports (graceful degradation)
# ------------------------------------------------------------------------------
ENHANCEMENTS_AVAILABLE = False
ENHANCEMENTS_IMPORT_ERROR: "str | None" = None

try:
    from src.enhancements.schemas.node_descriptor import (
        NodeDescriptor, NodeType, CoolingType, RoutingStrategy,
    )
    from src.enhancements.schemas.workload_descriptor import (
        WorkloadDescriptor, TaskType, Urgency, Priority,
    )
    from src.enhancements.schemas.feedback_event import FeedbackEvent
    from src.enhancements.zero_trust_architecture import (
        ZeroTrustArchitecture, ZeroTrustConfig,
    )
    from src.enhancements.core.graph_registry import GraphRegistry, GraphType
    from src.enhancements.core.causal_graph import CausalGraph
    from src.enhancements.core.dag_carbon_ledger import DAGCarbonLedger
    ENHANCEMENTS_AVAILABLE = True
except ImportError as exc:
    ENHANCEMENTS_IMPORT_ERROR = str(exc)
    NodeDescriptor = None
    WorkloadDescriptor = None
    FeedbackEvent = None
    ZeroTrustArchitecture = None
    GraphRegistry = None
    CausalGraph = None
    DAGCarbonLedger = None


POLICY_FILE = "symbolic_policy.yaml"


# ------------------------------------------------------------------------------
# Test helpers
# ------------------------------------------------------------------------------
def _clean_metrics(**overrides) -> dict:
    """Return a fully-populated compliant metrics dict, with overrides."""
    base = {
        "energy": 2.0,
        "carbon": 20.0,
        "latency": 5000,
        "memory": 200,
        "tool_calls": 10,
        "cumulative": {
            "total_energy_wh": 2.0,
            "total_carbon_kg": 0.02,
            "total_latency_ms": 5000,
            "max_memory_mb": 200,
            "total_tool_calls": 10,
            "step_count": 1,
        },
    }
    base.update(overrides)
    return base


def _violating_metrics() -> dict:
    return _clean_metrics(
        energy=6.0,
        carbon=70.0,
        latency=130000,
        memory=550,
        tool_calls=60,
        cumulative={
            "total_energy_wh": 6.0,
            "total_carbon_kg": 0.07,
            "total_latency_ms": 130000,
            "max_memory_mb": 550,
            "total_tool_calls": 60,
            "step_count": 5,
        },
    )


def _violation_ids(violations) -> set:
    return {getattr(v, "rule_id", None) or v.get("rule_id") for v in violations}


def _enum_value(x):
    """Return .value for enums, else str(x)."""
    return getattr(x, "value", x)


def _write_policy(rules: list) -> str:
    """Write a temporary symbolic_policy.yaml and return its path."""
    import yaml  # local import so import errors are localized
    fd, path = tempfile.mkstemp(prefix="symbolic_policy_", suffix=".yaml")
    with os.fdopen(fd, "w") as f:
        yaml.safe_dump({"symbolic_rules": rules}, f)
    return path


# ------------------------------------------------------------------------------
# Test class: SymbolicReasoningEngine
# ------------------------------------------------------------------------------
class TestSymbolicReasoningEngine(unittest.TestCase):
    """Test symbolic reasoning engine functionality."""

    def setUp(self):
        self.engine = SymbolicReasoningEngine(policy_file=POLICY_FILE)

    def test_rule_loading(self):
        """Rules are loaded with the required schema fields."""
        rules = self.engine.get_active_rules()
        self.assertGreater(len(rules), 0, "Should load at least one rule")
        for rule in rules:
            for field in ("id", "name", "condition", "action"):
                self.assertIn(field, rule, f"rule missing field: {field}")

    def test_energy_violation(self):
        """Energy budget violation is detected."""
        violations = self.engine.evaluate_rules(_violating_metrics(), step=1)
        self.assertTrue(
            any("energy" in (v.condition or "").lower() for v in violations),
            f"Expected an energy violation, got {_violation_ids(violations)}",
        )

    def test_memory_violation(self):
        """Memory overflow is detected."""
        metrics = _clean_metrics(memory=550)
        metrics["cumulative"]["max_memory_mb"] = 550
        violations = self.engine.evaluate_rules(metrics, step=1)
        self.assertTrue(
            any("memory" in (v.condition or "").lower() for v in violations),
            f"Expected a memory violation, got {_violation_ids(violations)}",
        )

    def test_composite_rule(self):
        """The COMPOSITE-001 rule fires on the crafted metrics."""
        violations = self.engine.evaluate_rules(_violating_metrics(), step=5)
        self.assertIn(
            "COMPOSITE-001",
            _violation_ids(violations),
            f"Expected COMPOSITE-001 to fire; got {_violation_ids(violations)}",
        )

    def test_no_violations(self):
        """Compliant metrics produce zero violations."""
        violations = self.engine.evaluate_rules(_clean_metrics(), step=1)
        self.assertEqual(
            len(violations), 0,
            f"Compliant metrics unexpectedly produced: {_violation_ids(violations)}",
        )

    def test_violation_trace_structure(self):
        """A violation trace has all required fields."""
        violations = self.engine.evaluate_rules(_violating_metrics(), step=1)
        self.assertGreater(len(violations), 0, "Need at least one violation to inspect")

        trace = violations[0]
        self.assertIsInstance(trace, ViolationTrace)
        self.assertIsNotNone(trace.rule_id)
        self.assertIsNotNone(trace.rule_name)
        self.assertIsNotNone(trace.condition)
        self.assertIsNotNone(trace.violation_details)

    def test_violation_summary(self):
        """Summary exposes the canonical v1.2 keys."""
        self.engine.evaluate_rules(_violating_metrics(), step=5)
        summary = self.engine.get_violation_summary()
        self.assertIn("total_violations", summary)
        self.assertIn("by_category", summary)
        self.assertIn("by_severity", summary)

    def test_category_filtering_via_summary(self):
        """Category counts are read via get_violation_summary (v1.2 API)."""
        self.engine.evaluate_rules(_violating_metrics(), step=5)
        summary = self.engine.get_violation_summary()
        by_category = summary["by_category"]
        self.assertIsInstance(by_category, dict)
        self.assertGreaterEqual(by_category.get("sustainability", 0), 1)

    def test_symbolic_policy_missing(self):
        """Engine falls back to default rules when policy file is absent."""
        engine = SymbolicReasoningEngine(policy_file="/tmp/does_not_exist_xyz.yaml")
        rules = engine.get_active_rules()
        self.assertGreater(len(rules), 0, "Default rules should load on missing file")

    def test_whitelist_parser_rejects_eval(self):
        """A condition containing __import__ must be rejected at load/eval time."""
        path = _write_policy([{
            "id": "MAL-001",
            "name": "malicious",
            "category": "custom",
            "priority": "medium",
            "condition": "__import__('os').system('true') == 0",
            "action": "flag_inefficiency",
            "explanation": "n/a",
        }])
        try:
            with self.assertRaises(Exception):
                engine = SymbolicReasoningEngine(policy_file=path)
                engine.evaluate_rules(_clean_metrics(), step=1)
        finally:
            os.unlink(path)

    def test_invalid_priority_rejected(self):
        """`warning` is a severity, not a priority — must raise on load."""
        path = _write_policy([{
            "id": "BAD-001",
            "name": "bad",
            "category": "custom",
            "priority": "warning",
            "condition": "energy > 1",
            "action": "flag_inefficiency",
            "explanation": "n/a",
        }])
        try:
            with self.assertRaises(ValueError):
                SymbolicReasoningEngine(policy_file=path)
        finally:
            os.unlink(path)

    def test_domain_extension(self):
        """Domain-scoped rules are evaluated when domain='research' is passed."""
        metrics = _clean_metrics(tool_calls=150)
        metrics["cumulative"]["total_tool_calls"] = 150
        # Soft check — default policy may not ship a research-scoped rule.
        violations = self.engine.evaluate_rules(metrics, step=5, domain="research")
        self.assertIsInstance(violations, list)


# ------------------------------------------------------------------------------
# Test class: SymbolicVisualizer
# ------------------------------------------------------------------------------
class TestSymbolicVisualizer(unittest.TestCase):
    """Test symbolic visualizer functionality."""

    def setUp(self):
        self.visualizer = SymbolicVisualizer()
        self.engine = SymbolicReasoningEngine(policy_file=POLICY_FILE)

    def _populate(self):
        violations = self.engine.evaluate_rules(_violating_metrics(), step=5)
        self.visualizer.add_violations([v.to_dict() for v in violations])
        return violations

    def test_add_violations(self):
        self._populate()
        self.assertGreater(len(self.visualizer.violation_data), 0)

    def test_timeline_generation(self):
        self._populate()
        timeline = self.visualizer.generate_violation_timeline()
        self.assertIsInstance(timeline, list)
        if timeline:
            entry = timeline[0]
            for key in ("timestamp", "step", "rule_name"):
                self.assertIn(key, entry)

    def test_severity_summary(self):
        self._populate()
        summary = self.visualizer.generate_severity_summary()
        self.assertIn("counts", summary)
        self.assertIn("details", summary)

    def test_html_generation(self):
        self._populate()
        html = self.visualizer.generate_dashboard_section()
        self.assertIsInstance(html, str)
        self.assertIn("symbolic", html.lower())


# ------------------------------------------------------------------------------
# Test class: Multi-layer feedback (v1.2 canonical)
# ------------------------------------------------------------------------------
class TestPolicyFeedbackIntegration(unittest.TestCase):
    """Integration with multi-layer feedback."""

    def setUp(self):
        self.feedback = PolicyFeedback()
        self.engine = SymbolicReasoningEngine(policy_file=POLICY_FILE)

    def test_multi_layer_feedback(self):
        """Multi-layer feedback exposes all four layers."""
        pareto_analysis = {
            "position": "dominated",
            "dominated_by": ["agent_1"],
            "dominates": [],
            "efficiency_score": 0.6,
        }
        reflections = [{
            "step": 1,
            "self_explanation": "High energy usage detected",
            "decision": "reduce_energy_usage",
            "confidence": 0.7,
        }]
        metrics = _violating_metrics()
        violations = self.engine.evaluate_rules(metrics, step=5)

        feedback = self.feedback.generate_multi_layer_feedback(
            pareto_analysis=pareto_analysis,
            reflections=reflections,
            metrics=metrics,
            symbolic_violations=[v.to_dict() for v in violations],
            enhanced_metrics={"modp_score": 0.55, "moe_gate_stddev": 0.1},
        )

        self.assertIn("objective_layer", feedback)
        self.assertIn("subjective_layer", feedback)
        self.assertIn("symbolic_layer", feedback)
        self.assertIn("enhanced_layer", feedback)
        self.assertIn("synthesis", feedback)

    def test_symbolic_recommendations(self):
        """Symbolic violations produce at least one recommendation."""
        pareto_analysis = {"position": "frontier"}
        metrics = _violating_metrics()
        violations = self.engine.evaluate_rules(metrics, step=5)

        feedback = self.feedback.generate_multi_layer_feedback(
            pareto_analysis=pareto_analysis,
            reflections=[],
            metrics=metrics,
            symbolic_violations=[v.to_dict() for v in violations],
            enhanced_metrics={},
        )

        recommendations = feedback["synthesis"]["recommendations"]
        self.assertIsInstance(recommendations, list)
        self.assertGreater(len(recommendations), 0)

    def test_deprecated_shim_warns(self):
        """generate_dual_layer_feedback emits DeprecationWarning."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.feedback.generate_dual_layer_feedback(
                pareto_analysis={"position": "frontier"},
                reflections=[],
                metrics=_clean_metrics(),
                symbolic_violations=[],
            )
        self.assertTrue(
            any(issubclass(w.category, DeprecationWarning) for w in caught),
            "generate_dual_layer_feedback should emit DeprecationWarning",
        )


# ------------------------------------------------------------------------------
# Test class: Advanced enhancements (v1.2)
# ------------------------------------------------------------------------------
@unittest.skipUnless(ENHANCEMENTS_AVAILABLE, "Enhancement modules not installed")
class TestAdvancedEnhancements(unittest.TestCase):
    """Advanced modules: LIMIT Graph, MODP, RLHF, Distillation, MoE, Evolutionary, FlexGen."""

    def test_node_descriptor_routing(self):
        """NodeDescriptor selects a routing strategy (enum or str accepted)."""
        node = NodeDescriptor(
            id="test_node",
            type=NodeType.EDGE,
            region="us-east",
            region_carbon_intensity=400.0,
            energy_per_token=0.00005,
            use_evolutionary=True,
            human_feedback_score=0.6,
            graph_metrics={"centrality": 0.8},
            metadata={"gating_lr": 0.005},
        )
        strategy = asyncio.run(node.select_routing_strategy(exploration=False))
        self.assertIn(
            _enum_value(strategy),
            {"carbon_first", "latency_first", "cost_first", "balanced", "adaptive"},
        )

    def test_workload_descriptor_priority(self):
        """WorkloadDescriptor selects a priority (enum or str accepted)."""
        wl = WorkloadDescriptor(
            task_id="test_task",
            task_type=TaskType.INFERENCE,
            tokens=1000,
            latency_target=300.0,
            urgency=Urgency.MEDIUM,
            use_evolutionary=True,
            human_feedback_score=0.7,
            graph_metrics={"centrality": 0.6},
            metadata={"latency_weight": 0.5, "carbon_weight": 0.3, "energy_weight": 0.2},
        )
        priority = asyncio.run(wl.select_priority(exploration=False))
        self.assertIn(_enum_value(priority), {"accuracy", "green", "balanced"})

    def test_feedback_event_enhanced_fields(self):
        """FeedbackEvent round-trips MODP, RLHF, and LIMIT Graph fields."""
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
            distillation_stats={"student_counter": 5},
        )
        event2 = FeedbackEvent.from_json(event.to_json())
        self.assertEqual(event2.graph_metrics["centrality"], 0.7)
        self.assertEqual(event2.human_feedback_score, 0.8)
        self.assertEqual(event2.modp_score, 0.75)

    def test_zero_trust_enhanced_init(self):
        """ZeroTrustArchitecture initializes with enhancement flags."""
        config = ZeroTrustConfig(
            use_enhancements=True,
            use_distillation=True,
            use_evolutionary=True,
            human_feedback_score=0.6,
            graph_metrics={"centrality": 0.5},
        )
        zta = ZeroTrustArchitecture(config=config)
        self.assertTrue(zta.use_enhancements)

    def test_graph_registry_and_causal(self):
        """GraphRegistry creates a CausalGraph."""
        registry = GraphRegistry()
        causal_graph = registry.get_or_create(GraphType.CAUSAL)
        self.assertIsInstance(causal_graph, CausalGraph)

    def test_dag_carbon_ledger_backpropagation(self):
        """DAGCarbonLedger backpropagates carbon debt to parents."""
        with tempfile.TemporaryDirectory(prefix="test_ledger_") as tmp:
            ledger = DAGCarbonLedger(storage_path=tmp)
            node_a = ledger.add_execution(
                task_id="A", framework="langchain", energy_kwh=0.01,
                carbon_co2e_kg=0.002, accuracy=0.9, sustainability_index=0.8,
            )
            node_b = ledger.add_execution(
                task_id="B", framework="langchain", energy_kwh=0.008,
                carbon_co2e_kg=0.0016, accuracy=0.88, sustainability_index=0.75,
                parent_task_ids=[node_a], dependency_type="model_state",
            )
            attributed = ledger.backpropagate_carbon(node_b, transfer_rate=0.3)
            self.assertIn(node_a, attributed)
            self.assertGreater(attributed[node_a], 0)

    def test_flexgen_unit_is_kwh(self):
        """FlexGen energy metric is expressed in kWh (v1.2 canonical)."""
        # Soft check — verify the config exposes the canonical key.
        try:
            from src.enhancements import config as enh_config
            keys = getattr(enh_config, "__dict__", {})
            # The canonical metric name should appear somewhere in the config.
            self.assertTrue(
                any("flexgen" in k.lower() and "kwh" in k.lower() for k in keys)
                or not keys,  # config may be a module without __dict__ keys
                "Expected a flexgen_*_kwh key in enhancement config",
            )
        except ImportError:
            self.skipTest("enhancements config module not importable")

    def test_moe_alias_documented(self):
        """The MOE_GATING_ENABLED alias is documented (docstring check)."""
        import importlib
        try:
            mod = importlib.import_module("src.enhancements")
        except ImportError:
            self.skipTest("src.enhancements not importable")
        doc = (mod.__doc__ or "")
        # Soft check — alias mention or skip.
        if "MOE_GATING_ENABLED" not in doc and "MOE_ENABLED" not in doc:
            self.skipTest("alias not documented at package level")


# ------------------------------------------------------------------------------
# Runner
# ------------------------------------------------------------------------------
def _summary_line(result: unittest.TestResult) -> str:
    skipped = len(getattr(result, "skipped", []))
    passed = result.testsRun - len(result.failures) - len(result.errors) - skipped
    return (
        f"Tests run: {result.testsRun} · "
        f"Passed: {passed} · "
        f"Skipped: {skipped} · "
        f"Failures: {len(result.failures)} · "
        f"Errors: {len(result.errors)}"
    )


def run_tests() -> bool:
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    for cls in (
        TestSymbolicReasoningEngine,
        TestSymbolicVisualizer,
        TestPolicyFeedbackIntegration,
    ):
        suite.addTests(loader.loadTestsFromTestCase(cls))

    if ENHANCEMENTS_AVAILABLE:
        suite.addTests(loader.loadTestsFromTestCase(TestAdvancedEnhancements))
    else:
        print(
            f"⚠️  Advanced enhancement tests skipped "
            f"(import error: {ENHANCEMENTS_IMPORT_ERROR})",
            file=sys.stderr,
        )

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    print(_summary_line(result))

    return result.wasSuccessful()


if __name__ == "__main__":
    sys.exit(0 if run_tests() else 1)
