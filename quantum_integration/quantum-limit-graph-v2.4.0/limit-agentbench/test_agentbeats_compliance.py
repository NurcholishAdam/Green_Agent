# -*- coding: utf-8 -*-
"""
AgentBeats Compliance Test Suite (Enhanced)
Validates all four pillars of AgentBeats compliance,
plus advanced enhancement modules (LIMIT Graph, MODP, RLHF,
Multi‑Teacher On‑Policy Distillation, Bio‑inspired Optimisation, MoE expert gating).
"""

import unittest
import json
import asyncio
from pathlib import Path

from core.a2a_gateway import (
    A2AGateway, TaskStatus, A2ARequest, create_a2a_task
)
from core.rlhf_feedback_engine import RLHFFeedbackEngine, ReasoningQuality
from core.docker_orchestrator import DockerOrchestrator, ContainerConfig

# Optional imports for advanced enhancements (graceful skip if unavailable)
try:
    from enhancements.schemas.feedback_event import FeedbackEvent
    from enhancements.schemas.node_descriptor import NodeDescriptor, NodeType, CoolingType
    from enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency
    from enhancements.zero_trust_architecture import ZeroTrustArchitecture, ZeroTrustConfig
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False


# ------------------------------------------------------------------------------
# Original test classes (unchanged)
# ------------------------------------------------------------------------------

class TestA2ACompliance(unittest.TestCase):
    """Test A2A Protocol Compliance"""

    def setUp(self):
        self.gateway = A2AGateway()

    def test_valid_request_validation(self):
        task = create_a2a_task(task_id="test_001", task_type="research", query="Test query")
        validated = self.gateway.validate_request(task)
        self.assertEqual(validated.task_id, "test_001")
        self.assertEqual(validated.task_type, "research")
        self.assertTrue(validated.validate())

    def test_invalid_request_validation(self):
        invalid_task = {"task_id": "test"}  # Missing required fields
        with self.assertRaises(ValueError):
            self.gateway.validate_request(invalid_task)

    def test_success_response_format(self):
        response = self.gateway.create_success_response(
            task_id="test_001",
            output={"result": "Test answer"},
            execution_time=1.5,
            green_metrics={"energy_kwh": 0.01}
        )
        response_dict = response.to_dict()
        self.assertEqual(response_dict["task_id"], "test_001")
        self.assertEqual(response_dict["status"], "success")
        self.assertIn("timestamp", response_dict)
        self.assertIn("green_metrics", response_dict)

    def test_failure_response_format(self):
        response = self.gateway.create_failure_response(
            task_id="test_001",
            status=TaskStatus.TIMEOUT,
            error_message="Task timed out"
        )
        response_dict = response.to_dict()
        self.assertEqual(response_dict["status"], "timeout")
        self.assertIn("error", response_dict)

    def test_agent_output_transformation(self):
        response = self.gateway.transform_agent_output(
            task_id="test_001", agent_output={"answer": "Test"}, execution_time=1.0
        )
        self.assertEqual(response.status, TaskStatus.SUCCESS)

        response = self.gateway.transform_agent_output(
            task_id="test_002", agent_output="Simple string answer", execution_time=1.0
        )
        self.assertEqual(response.status, TaskStatus.SUCCESS)
        self.assertIn("result", response.output)

    def test_version_support(self):
        task_v1_0 = {
            "task_id": "test",
            "task_type": "qa",
            "input_data": {"query": "Test"},
            "version": "1.0"
        }
        validated = self.gateway.validate_request(task_v1_0)
        self.assertEqual(validated.version, "1.0")


class TestRLHFFeedback(unittest.TestCase):
    """Test RLHF Feedback Engine"""

    def setUp(self):
        self.rlhf = RLHFFeedbackEngine()

    def test_reasoning_trace_analysis(self):
        trace = [
            {"action": "plan", "thought": "Breaking down the task", "duration": 0.1},
            {"action": "search", "thought": "Searching for information", "tool": "web_search", "observation": "Found 5 results", "duration": 0.5},
            {"action": "synthesize", "thought": "Synthesizing findings", "duration": 0.3}
        ]
        feedback = self.rlhf.analyze_reasoning_trace(
            reasoning_trace=trace, task_type="research", execution_time=0.9, success=True
        )
        self.assertIn("overall_score", feedback)
        self.assertIn("reasoning_quality", feedback)
        self.assertIn("improvement_suggestions", feedback)
        self.assertGreater(feedback["overall_score"], 0)

    def test_quality_assessment(self):
        good_trace = [
            {"action": "plan", "thought": "Planning approach", "duration": 0.1},
            {"action": "search", "thought": "Searching", "tool": "search", "duration": 0.5},
            {"action": "conclude", "thought": "Concluding", "duration": 0.2}
        ]
        feedback = self.rlhf.analyze_reasoning_trace(good_trace, "research", 0.8, True)
        self.assertGreaterEqual(feedback["reasoning_score"], 0.6)

    def test_efficiency_analysis(self):
        redundant_trace = [
            {"action": "search", "thought": "First search", "duration": 0.5},
            {"action": "search", "thought": "Second search", "duration": 0.5},
            {"action": "search", "thought": "Third search", "duration": 0.5}
        ]
        feedback = self.rlhf.analyze_reasoning_trace(redundant_trace, "research", 1.5, True)
        self.assertGreater(feedback["metrics"]["redundant_steps"], 0)

    def test_feedback_items_generation(self):
        poor_trace = [{"action": "random", "thought": "Doing something", "duration": 0.5}]
        feedback = self.rlhf.analyze_reasoning_trace(poor_trace, "research", 0.5, False)
        self.assertGreater(len(feedback["feedback_items"]), 0)
        self.assertGreater(len(feedback["improvement_suggestions"]), 0)

    def test_comparative_analysis(self):
        for i in range(5):
            trace = [{"action": "test", "thought": "test", "duration": 0.1}]
            self.rlhf.analyze_reasoning_trace(trace, "research", 0.1, True)
        comparison = self.rlhf.get_comparative_analysis()
        self.assertEqual(comparison["total_executions"], 5)
        self.assertIn("average_score", comparison)
        self.assertIn("success_rate", comparison)


class TestRobustScoring(unittest.TestCase):
    """Test Robust Scoring with Failure Handling"""

    def test_success_scoring(self):
        score = self._calculate_score(TaskStatus.SUCCESS, {"result": "Complete"})
        self.assertEqual(score, 1.0)

    def test_timeout_with_output_scoring(self):
        score = self._calculate_score(TaskStatus.TIMEOUT, {"result": "Partial answer"})
        self.assertGreater(score, 0.0)
        self.assertLess(score, 1.0)

    def test_timeout_without_output_scoring(self):
        score = self._calculate_score(TaskStatus.TIMEOUT, None)
        self.assertEqual(score, 0.0)

    def test_oom_scoring(self):
        score = self._calculate_score(TaskStatus.OOM, None)
        self.assertEqual(score, 0.0)

    def test_invalid_output_scoring(self):
        score = self._calculate_score(TaskStatus.INVALID_OUTPUT, {"malformed": "data"})
        self.assertGreater(score, 0.0)
        self.assertLess(score, 0.5)

    def _calculate_score(self, status, output):
        if status == TaskStatus.SUCCESS:
            return 1.0
        elif status == TaskStatus.TIMEOUT and output:
            completeness = len(str(output)) / 500
            return min(0.8, 0.5 + completeness * 0.3)
        elif status == TaskStatus.INVALID_OUTPUT and output:
            return 0.3
        else:
            return 0.0


class TestDockerOrchestration(unittest.TestCase):
    """Test Docker Orchestration (unit tests only)"""

    def setUp(self):
        self.orchestrator = DockerOrchestrator(work_dir=Path("./test_work"))

    def tearDown(self):
        self.orchestrator.cleanup()

    def test_container_config_creation(self):
        config = ContainerConfig(image="test:latest", cpu_limit="2.0", memory_limit="4g", timeout_seconds=60)
        args = config.to_docker_args()
        self.assertIn("--cpus", args)
        self.assertIn("2.0", args)
        self.assertIn("--memory", args)
        self.assertIn("4g", args)

    def test_work_directory_creation(self):
        self.assertTrue(self.orchestrator.work_dir.exists())

    def test_cleanup(self):
        task_dir = self.orchestrator.work_dir / "test_task"
        task_dir.mkdir(exist_ok=True)
        self.orchestrator.cleanup("test_task")
        self.assertFalse(task_dir.exists())


class TestEndToEndIntegration(unittest.TestCase):
    """Test end-to-end AgentBeats integration"""

    def test_complete_workflow(self):
        gateway = A2AGateway()
        rlhf = RLHFFeedbackEngine()
        task = create_a2a_task(task_id="e2e_001", task_type="research", query="Test query")
        validated = gateway.validate_request(task)
        self.assertTrue(validated.validate())
        agent_output = {"answer": "Test answer"}
        reasoning_trace = [{"action": "search", "thought": "Searching", "duration": 0.5}]
        feedback = rlhf.analyze_reasoning_trace(reasoning_trace, "research", 0.5, True)
        response = gateway.create_success_response(
            task_id=validated.task_id,
            output=agent_output,
            execution_time=0.5,
            reasoning_trace=reasoning_trace,
            metadata={"rlhf_feedback": feedback}
        )
        response_dict = response.to_dict()
        self.assertEqual(response_dict["task_id"], "e2e_001")
        self.assertEqual(response_dict["status"], "success")
        self.assertIn("metadata", response_dict)
        self.assertIn("rlhf_feedback", response_dict["metadata"])


# ------------------------------------------------------------------------------
# Enhanced test class for advanced modules (optional)
# ------------------------------------------------------------------------------

@unittest.skipIf(not ENHANCEMENTS_AVAILABLE, "Enhanced modules not installed")
class TestAdvancedEnhancements(unittest.TestCase):
    """Test advanced enhancement modules: LIMIT Graph, MODP, RLHF, Distillation, MoE, Evolutionary."""

    def test_feedback_event_enhanced_fields(self):
        """FeedbackEvent should accept and serialise advanced fields."""
        event = FeedbackEvent(
            source="test",
            feedback_type="routing",
            task_id="t1",
            context={},
            action={"selected_action": "execute", "selected_rank": 1},
            performance={"quality_score": 0.9, "latency_ms": 100, "energy_joules": 100,
                         "carbon_g": 5, "helium_cost": 0, "duration_ms": 100},
            adaptive_cost_value=0.85,
            graph_metrics={"centrality": 0.7},
            human_feedback_score=0.8,
            modp_score=0.75,
            distillation_stats={"student_counter": 5}
        )
        json_str = event.to_json()
        self.assertIsInstance(json_str, str)
        # Deserialize and verify
        event2 = FeedbackEvent.from_json(json_str)
        self.assertEqual(event2.task_id, "t1")
        self.assertEqual(event2.graph_metrics["centrality"], 0.7)
        self.assertEqual(event2.human_feedback_score, 0.8)
        self.assertEqual(event2.modp_score, 0.75)

    def test_node_descriptor_enhanced_features(self):
        """NodeDescriptor should accept LIMIT Graph, RLHF, MoE, evolutionary flags."""
        node = NodeDescriptor(
            id="node1",
            type=NodeType.EDGE,
            region="us-east",
            region_carbon_intensity=400.0,
            energy_per_token=0.00005,
            use_evolutionary=True,
            human_feedback_score=0.6,
            graph_metrics={"centrality": 0.8},
            metadata={"gating_lr": 0.005}
        )
        self.assertTrue(node.use_evolutionary)
        self.assertEqual(node.human_feedback_score, 0.6)
        self.assertEqual(node.graph_metrics["centrality"], 0.8)

    def test_workload_descriptor_modp_rlhf(self):
        """WorkloadDescriptor should support MODP weights and RLHF."""
        wl = WorkloadDescriptor(
            task_id="task1",
            task_type=TaskType.INFERENCE,
            tokens=1000,
            latency_target=500.0,
            use_evolutionary=True,
            human_feedback_score=0.7,
            graph_metrics={"centrality": 0.6},
            metadata={"latency_weight": 0.5, "carbon_weight": 0.3}
        )
        self.assertEqual(wl.human_feedback_score, 0.7)
        self.assertEqual(wl.graph_metrics["centrality"], 0.6)

    def test_zero_trust_enhanced_init(self):
        """ZeroTrustArchitecture can be initialized with enhancement flags."""
        config = {"use_enhancements": True, "use_distillation": True, "use_evolutionary": True}
        zta = ZeroTrustArchitecture(config=config)
        self.assertTrue(zta.use_enhancements)
        # Should have carbon_authenticator or distillation_optimizer
        self.assertTrue(hasattr(zta, 'carbon_authenticator') or hasattr(zta, 'distillation_optimizer'))

    def test_distillation_selection_methods(self):
        """Distillation-based selection in NodeDescriptor and WorkloadDescriptor works."""
        node = NodeDescriptor(
            id="node_d",
            type=NodeType.EDGE,
            region="us-east",
            region_carbon_intensity=300,
            energy_per_token=0.00004,
            use_enhancements=True,
            human_feedback_score=0.6,
            graph_metrics={"centrality": 0.7}
        )
        strategy = asyncio.run(node.select_routing_strategy(exploration=True))
        self.assertIn(strategy, ["carbon_first", "latency_first", "cost_first", "balanced", "adaptive"])

        wl = WorkloadDescriptor(
            task_id="task_d",
            task_type=TaskType.INFERENCE,
            tokens=500,
            latency_target=200,
            use_enhancements=True,
            human_feedback_score=0.5,
            graph_metrics={"centrality": 0.5}
        )
        priority = asyncio.run(wl.select_priority(exploration=True))
        self.assertIn(priority, ["accuracy", "green", "balanced"])


def run_compliance_tests():
    """Run all compliance tests including enhanced module tests if available."""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Original test classes
    suite.addTests(loader.loadTestsFromTestCase(TestA2ACompliance))
    suite.addTests(loader.loadTestsFromTestCase(TestRLHFFeedback))
    suite.addTests(loader.loadTestsFromTestCase(TestRobustScoring))
    suite.addTests(loader.loadTestsFromTestCase(TestDockerOrchestration))
    suite.addTests(loader.loadTestsFromTestCase(TestEndToEndIntegration))

    # Enhanced test class (skipped automatically if modules not available)
    suite.addTests(loader.loadTestsFromTestCase(TestAdvancedEnhancements))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 80)
    print("AgentBeats Compliance Test Summary (Enhanced)")
    print("=" * 80)
    print(f"Tests Run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print("=" * 80)

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_compliance_tests()
    exit(0 if success else 1)
