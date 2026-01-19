# -*- coding: utf-8 -*-
"""
AgentBench Protocol Adapter
Provides standardized interface compatible with AgentBench protocol
"""

import json
import hashlib
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class AgentBenchAdapter:
    """
    Adapter for AgentBench protocol compatibility.
    
    Provides standardized task definition, execution, and result formats
    compatible with the AgentBench evaluation framework.
    """
    
    def __init__(self, protocol_version: str = "1.0"):
        self.protocol_version = protocol_version
        self.task_registry = {}
        
    def create_task(
        self,
        task_id: str,
        suite: str,
        task_type: str,
        input_data: Dict[str, Any],
        expected_output: Optional[Dict[str, Any]] = None,
        evaluation_metrics: Optional[List[str]] = None,
        difficulty: str = "medium",
        timeout_seconds: int = 30
    ) -> Dict[str, Any]:
        """
        Create a task in AgentBench format.
        
        Args:
            task_id: Unique task identifier
            suite: Task suite name (e.g., "question_answering")
            task_type: Type of task
            input_data: Task input data
            expected_output: Expected output for evaluation
            evaluation_metrics: List of metrics to evaluate
            difficulty: Task difficulty level
            timeout_seconds: Maximum execution time
            
        Returns:
            Task definition in AgentBench format
        """
        if evaluation_metrics is None:
            evaluation_metrics = ["accuracy", "latency", "energy_kwh", "carbon_co2e_kg"]
            
        task = {
            "task_id": task_id,
            "suite": suite,
            "type": task_type,
            "difficulty": difficulty,
            "input": input_data,
            "expected_output": expected_output,
            "evaluation": {
                "metrics": evaluation_metrics,
                "timeout_seconds": timeout_seconds
            },
            "protocol_version": self.protocol_version,
            "created_at": datetime.utcnow().isoformat() + "Z"
        }
        
        self.task_registry[task_id] = task
        logger.info(f"Created task {task_id} in suite {suite}")
        
        return task
    
    def evaluate_agent(
        self,
        agent: Any,
        task: Dict[str, Any],
        track_energy: bool = True,
        track_carbon: bool = True,
        backend: Optional[str] = None,
        rank: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Evaluate an agent on a task with AgentBench protocol.
        
        Args:
            agent: Agent instance to evaluate
            task: Task definition
            track_energy: Whether to track energy consumption
            track_carbon: Whether to track carbon footprint
            backend: Quantum backend (if applicable)
            rank: NSN rank (if applicable)
            
        Returns:
            Evaluation result in AgentBench format
        """
        from .green_metrics import GreenMetricsTracker
        
        task_id = task["task_id"]
        logger.info(f"Evaluating agent on task {task_id}")
        
        # Initialize green metrics tracker
        tracker = None
        if track_energy or track_carbon:
            tracker = GreenMetricsTracker(
                track_energy=track_energy,
                track_carbon=track_carbon
            )
            tracker.start()
        
        # Execute agent
        start_time = datetime.utcnow()
        try:
            # Call agent with task input
            if hasattr(agent, 'run'):
                output = agent.run(task["input"])
            elif callable(agent):
                output = agent(task["input"])
            else:
                raise ValueError("Agent must have 'run' method or be callable")
                
            success = True
            error = None
        except Exception as e:
            logger.error(f"Agent execution failed: {e}")
            output = None
            success = False
            error = str(e)
        
        end_time = datetime.utcnow()
        latency_ms = (end_time - start_time).total_seconds() * 1000
        
        # Stop tracking
        green_metrics = {}
        if tracker:
            tracker.stop()
            green_metrics = tracker.get_metrics()
        
        # Calculate accuracy if expected output provided
        accuracy = None
        if task.get("expected_output") and output:
            accuracy = self._calculate_accuracy(output, task["expected_output"])
        
        # Build result
        result = {
            "task_id": task_id,
            "agent_name": getattr(agent, 'name', agent.__class__.__name__),
            "framework": getattr(agent, 'framework', 'unknown'),
            "success": success,
            "output": output,
            "error": error,
            "metrics": {
                "latency_ms": latency_ms,
                **green_metrics
            },
            "provenance": {
                "hash": self._compute_hash(task, output),
                "timestamp": end_time.isoformat() + "Z",
                "backend": backend,
                "rank": rank
            },
            "protocol_version": self.protocol_version
        }
        
        if accuracy is not None:
            result["metrics"]["accuracy"] = accuracy
        
        logger.info(f"Evaluation complete for task {task_id}")
        return result
    
    def _calculate_accuracy(
        self,
        output: Any,
        expected: Any
    ) -> float:
        """Calculate accuracy score between output and expected."""
        if isinstance(output, dict) and isinstance(expected, dict):
            # For dict outputs, check key-value matches
            matches = sum(1 for k, v in expected.items() 
                         if k in output and output[k] == v)
            return matches / len(expected) if expected else 0.0
        elif isinstance(output, str) and isinstance(expected, str):
            # For string outputs, use exact match or similarity
            return 1.0 if output.strip().lower() == expected.strip().lower() else 0.0
        else:
            # For other types, use equality
            return 1.0 if output == expected else 0.0
    
    def _compute_hash(self, task: Dict[str, Any], output: Any) -> str:
        """Compute provenance hash."""
        data = json.dumps({
            "task_id": task["task_id"],
            "output": str(output),
            "timestamp": datetime.utcnow().isoformat()
        }, sort_keys=True)
        return hashlib.sha256(data.encode()).hexdigest()
    
    def validate_result(self, result: Dict[str, Any]) -> bool:
        """Validate result format against AgentBench protocol."""
        required_fields = ["task_id", "agent_name", "framework", "success", 
                          "metrics", "provenance", "protocol_version"]
        return all(field in result for field in required_fields)
    
    def export_to_json(self, result: Dict[str, Any], filepath: str):
        """Export result to JSON file."""
        with open(filepath, 'w') as f:
            json.dump(result, f, indent=2)
        logger.info(f"Result exported to {filepath}")
    
    def load_from_json(self, filepath: str) -> Dict[str, Any]:
        """Load result from JSON file."""
        with open(filepath, 'r') as f:
            result = json.load(f)
        logger.info(f"Result loaded from {filepath}")
        return result
