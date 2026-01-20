# -*- coding: utf-8 -*-
"""
AgentBeats Compliance Test Suite
Validates all four pillars of AgentBeats compliance
"""

import unittest
import json
from pathlib import Path

from core.a2a_gateway import (
    A2AGateway, TaskStatus, A2ARequest, create_a2a_task
)
from core.rlhf_feedback_engine import RLHFFeedbackEngine, ReasoningQuality
from core.docker_orchestrator import DockerOrchestrator, ContainerConfig


class TestA2ACompliance(unittest.TestCase):
    """Test A2A Protocol Compliance"""
    
    def setUp(self):
        self.gateway = A2AGateway()
    
    def test_valid_request_validation(self):
        """Test validation of valid A2A request"""
        task = create_a2a_task(
            task_id="test_001",
            task_type="research",
            query="Test query"
        )
        
        validated = self.gateway.validate_request(task)
        
        self.assertEqual(validated.task_id, "test_001")
        self.assertEqual(validated.task_type, "research")
        self.assertTrue(validated.validate())
    
    def test_invalid_request_validation(self):
        """Test validation rejects invalid requests"""
        invalid_task = {"task_id": "test"}  # Missing required fields
        
        with self.assertRaises(ValueError):
            self.gateway.validate_request(invalid_task)
    
    def test_success_response_format(self):
        """Test A2A success response format"""
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
        """Test A2A failure response format"""
        response = self.gateway.create_failure_response(
            task_id="test_001",
            status=TaskStatus.TIMEOUT,
            error_message="Task timed out"
        )
        
        response_dict = response.to_dict()
        
        self.assertEqual(response_dict["status"], "timeout")
        self.assertIn("error", response_dict)
    
    def test_agent_output_transformation(self):
        """Test transformation of various agent output formats"""
        # Test dict output
        response = self.gateway.transform_agent_output(
            task_id="test_001",
            agent_output={"answer": "Test"},
            execution_time=1.0
        )
        self.assertEqual(response.status, TaskStatus.SUCCESS)
        
        # Test string output
        response = self.gateway.transform_agent_output(
            task_id="test_002",
            agent_output="Simple string answer",
            execution_time=1.0
        )
        self.assertEqual(response.status, TaskStatus.SUCCESS)
        self.assertIn("result", response.output)
    
    def test_version_support(self):
        """Test A2A version support"""
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
        """Test reasoning trace analysis"""
        trace = [
            {
                "action": "plan",
                "thought": "Breaking down the task",
                "duration": 0.1
            },
            {
                "action": "search",
                "thought": "Searching for information",
                "tool": "web_search",
                "observation": "Found 5 results",
                "duration": 0.5
            },
            {
                "action": "synthesize",
                "thought": "Synthesizing findings",
                "duration": 0.3
            }
        ]
        
        feedback = self.rlhf.analyze_reasoning_trace(
            reasoning_trace=trace,
            task_type="research",
            execution_time=0.9,
            success=True
        )
        
        self.assertIn("overall_score", feedback)
        self.assertIn("reasoning_quality", feedback)
        self.assertIn("improvement_suggestions", feedback)
        self.assertGreater(feedback["overall_score"], 0)
    
    def test_quality_assessment(self):
        """Test reasoning quality assessment"""
        # Good trace with planning
        good_trace = [
            {"action": "plan", "thought": "Planning approach", "duration": 0.1},
            {"action": "search", "thought": "Searching", "tool": "search", "duration": 0.5},
            {"action": "conclude", "thought": "Concluding", "duration": 0.2}
        ]
        
        feedback = self.rlhf.analyze_reasoning_trace(
            good_trace, "research", 0.8, True
        )
        
        self.assertGreaterEqual(feedback["reasoning_score"], 0.6)
    
    def test_efficiency_analysis(self):
        """Test efficiency analysis"""
        # Trace with redundant steps
        redundant_trace = [
            {"action": "search", "thought": "First search", "duration": 0.5},
            {"action": "search", "thought": "Second search", "duration": 0.5},
            {"action": "search", "thought": "Third search", "duration": 0.5}
        ]
        
        feedback = self.rlhf.analyze_reasoning_trace(
            redundant_trace, "research", 1.5, True
        )
        
        self.assertGreater(feedback["metrics"]["redundant_steps"], 0)
    
    def test_feedback_items_generation(self):
        """Test feedback items are generated"""
        # Poor trace without planning
        poor_trace = [
            {"action": "random", "thought": "Doing something", "duration": 0.5}
        ]
        
        feedback = self.rlhf.analyze_reasoning_trace(
            poor_trace, "research", 0.5, False
        )
        
        self.assertGreater(len(feedback["feedback_items"]), 0)
        self.assertGreater(len(feedback["improvement_suggestions"]), 0)
    
    def test_comparative_analysis(self):
        """Test comparative analysis across executions"""
        # Generate some history
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
        """Test scoring for successful execution"""
        score = self._calculate_score(TaskStatus.SUCCESS, {"result": "Complete"})
        self.assertEqual(score, 1.0)
    
    def test_timeout_with_output_scoring(self):
        """Test partial credit for timeout with output"""
        score = self._calculate_score(
            TaskStatus.TIMEOUT,
            {"result": "Partial answer"}
        )
        self.assertGreater(score, 0.0)
        self.assertLess(score, 1.0)
    
    def test_timeout_without_output_scoring(self):
        """Test no credit for timeout without output"""
        score = self._calculate_score(TaskStatus.TIMEOUT, None)
        self.assertEqual(score, 0.0)
    
    def test_oom_scoring(self):
        """Test scoring for out of memory"""
        score = self._calculate_score(TaskStatus.OOM, None)
        self.assertEqual(score, 0.0)
    
    def test_invalid_output_scoring(self):
        """Test minimal credit for invalid output"""
        score = self._calculate_score(
            TaskStatus.INVALID_OUTPUT,
            {"malformed": "data"}
        )
        self.assertGreater(score, 0.0)
        self.assertLess(score, 0.5)
    
    def _calculate_score(self, status, output):
        """Helper to calculate robust score"""
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
        """Test container configuration"""
        config = ContainerConfig(
            image="test:latest",
            cpu_limit="2.0",
            memory_limit="4g",
            timeout_seconds=60
        )
        
        args = config.to_docker_args()
        
        self.assertIn("--cpus", args)
        self.assertIn("2.0", args)
        self.assertIn("--memory", args)
        self.assertIn("4g", args)
    
    def test_work_directory_creation(self):
        """Test work directory is created"""
        self.assertTrue(self.orchestrator.work_dir.exists())
    
    def test_cleanup(self):
        """Test cleanup removes work directory"""
        # Create a test task directory
        task_dir = self.orchestrator.work_dir / "test_task"
        task_dir.mkdir(exist_ok=True)
        
        # Cleanup specific task
        self.orchestrator.cleanup("test_task")
        
        self.assertFalse(task_dir.exists())


class TestEndToEndIntegration(unittest.TestCase):
    """Test end-to-end AgentBeats integration"""
    
    def test_complete_workflow(self):
        """Test complete A2A workflow"""
        # Create components
        gateway = A2AGateway()
        rlhf = RLHFFeedbackEngine()
        
        # Create task
        task = create_a2a_task(
            task_id="e2e_001",
            task_type="research",
            query="Test query"
        )
        
        # Validate
        validated = gateway.validate_request(task)
        self.assertTrue(validated.validate())
        
        # Simulate agent execution
        agent_output = {"answer": "Test answer"}
        reasoning_trace = [
            {"action": "search", "thought": "Searching", "duration": 0.5}
        ]
        
        # Generate RLHF feedback
        feedback = rlhf.analyze_reasoning_trace(
            reasoning_trace, "research", 0.5, True
        )
        
        # Create response
        response = gateway.create_success_response(
            task_id=validated.task_id,
            output=agent_output,
            execution_time=0.5,
            reasoning_trace=reasoning_trace,
            metadata={"rlhf_feedback": feedback}
        )
        
        # Validate response format
        response_dict = response.to_dict()
        self.assertEqual(response_dict["task_id"], "e2e_001")
        self.assertEqual(response_dict["status"], "success")
        self.assertIn("metadata", response_dict)
        self.assertIn("rlhf_feedback", response_dict["metadata"])


def run_compliance_tests():
    """Run all compliance tests"""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestA2ACompliance))
    suite.addTests(loader.loadTestsFromTestCase(TestRLHFFeedback))
    suite.addTests(loader.loadTestsFromTestCase(TestRobustScoring))
    suite.addTests(loader.loadTestsFromTestCase(TestDockerOrchestration))
    suite.addTests(loader.loadTestsFromTestCase(TestEndToEndIntegration))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "=" * 80)
    print("AgentBeats Compliance Test Summary")
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
