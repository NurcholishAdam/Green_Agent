# -*- coding: utf-8 -*-
"""
A2A Protocol Gateway - AgentBeats Compliance Layer
Validates and transforms agent I/O to A2A standard format
"""

from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum
import json
from datetime import datetime


class A2AVersion(Enum):
    """Supported A2A protocol versions"""
    V1_0 = "1.0"
    V1_1 = "1.1"


class TaskStatus(Enum):
    """A2A-compliant task status codes"""
    SUCCESS = "success"
    FAILURE = "failure"
    TIMEOUT = "timeout"
    OOM = "out_of_memory"
    CRASH = "crash"
    INVALID_OUTPUT = "invalid_output"


@dataclass
class A2ARequest:
    """A2A-compliant task request"""
    task_id: str
    task_type: str
    input_data: Dict[str, Any]
    constraints: Optional[Dict[str, Any]] = None
    version: str = "1.1"
    
    def validate(self) -> bool:
        """Validate request against A2A schema"""
        required_fields = ['task_id', 'task_type', 'input_data']
        return all(hasattr(self, field) and getattr(self, field) is not None 
                   for field in required_fields)


@dataclass
class A2AResponse:
    """A2A-compliant task response"""
    task_id: str
    status: TaskStatus
    output: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    execution_time: Optional[float] = None
    green_metrics: Optional[Dict[str, Any]] = None
    reasoning_trace: Optional[List[Dict[str, Any]]] = None
    metadata: Optional[Dict[str, Any]] = None
    version: str = "1.1"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to A2A-compliant dictionary"""
        result = {
            "task_id": self.task_id,
            "status": self.status.value,
            "version": self.version,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        if self.output is not None:
            result["output"] = self.output
        if self.error_message:
            result["error"] = self.error_message
        if self.execution_time is not None:
            result["execution_time_seconds"] = self.execution_time
        if self.green_metrics:
            result["green_metrics"] = self.green_metrics
        if self.reasoning_trace:
            result["reasoning_trace"] = self.reasoning_trace
        if self.metadata:
            result["metadata"] = self.metadata
            
        return result


class A2AGateway:
    """
    A2A Protocol Gateway for AgentBeats Compliance
    
    Ensures all agent interactions conform to A2A standard:
    - Validates incoming requests
    - Transforms agent outputs to A2A format
    - Handles protocol versioning
    - Provides graceful error handling
    """
    
    def __init__(self, default_version: A2AVersion = A2AVersion.V1_1):
        self.default_version = default_version
        self.request_count = 0
        self.error_count = 0
        
    def validate_request(self, request_data: Dict[str, Any]) -> A2ARequest:
        """
        Validate and parse incoming A2A request
        
        Args:
            request_data: Raw request dictionary
            
        Returns:
            Validated A2ARequest object
            
        Raises:
            ValueError: If request doesn't conform to A2A schema
        """
        self.request_count += 1
        
        try:
            # Extract required fields
            task_id = request_data.get('task_id')
            task_type = request_data.get('task_type')
            input_data = request_data.get('input_data')
            
            if not all([task_id, task_type, input_data]):
                raise ValueError(
                    "Missing required fields. A2A request must include: "
                    "task_id, task_type, input_data"
                )
            
            # Create request object
            a2a_request = A2ARequest(
                task_id=task_id,
                task_type=task_type,
                input_data=input_data,
                constraints=request_data.get('constraints'),
                version=request_data.get('version', self.default_version.value)
            )
            
            # Validate
            if not a2a_request.validate():
                raise ValueError("Request validation failed")
                
            return a2a_request
            
        except Exception as e:
            self.error_count += 1
            raise ValueError(f"A2A request validation failed: {str(e)}")
    
    def create_success_response(
        self,
        task_id: str,
        output: Dict[str, Any],
        execution_time: float,
        green_metrics: Optional[Dict[str, Any]] = None,
        reasoning_trace: Optional[List[Dict[str, Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> A2AResponse:
        """Create A2A-compliant success response"""
        return A2AResponse(
            task_id=task_id,
            status=TaskStatus.SUCCESS,
            output=output,
            execution_time=execution_time,
            green_metrics=green_metrics,
            reasoning_trace=reasoning_trace,
            metadata=metadata,
            version=self.default_version.value
        )
    
    def create_failure_response(
        self,
        task_id: str,
        status: TaskStatus,
        error_message: str,
        execution_time: Optional[float] = None,
        partial_output: Optional[Dict[str, Any]] = None,
        reasoning_trace: Optional[List[Dict[str, Any]]] = None
    ) -> A2AResponse:
        """Create A2A-compliant failure response"""
        return A2AResponse(
            task_id=task_id,
            status=status,
            output=partial_output,
            error_message=error_message,
            execution_time=execution_time,
            reasoning_trace=reasoning_trace,
            version=self.default_version.value
        )
    
    def transform_agent_output(
        self,
        task_id: str,
        agent_output: Any,
        execution_time: float,
        green_metrics: Optional[Dict[str, Any]] = None,
        reasoning_trace: Optional[List[Dict[str, Any]]] = None
    ) -> A2AResponse:
        """
        Transform arbitrary agent output to A2A format
        
        Args:
            task_id: Task identifier
            agent_output: Raw agent output (any format)
            execution_time: Execution time in seconds
            green_metrics: Optional sustainability metrics
            reasoning_trace: Optional reasoning steps
            
        Returns:
            A2A-compliant response
        """
        try:
            # Normalize output to dictionary
            if isinstance(agent_output, dict):
                output = agent_output
            elif isinstance(agent_output, str):
                output = {"result": agent_output}
            elif isinstance(agent_output, (list, tuple)):
                output = {"results": list(agent_output)}
            else:
                output = {"value": str(agent_output)}
            
            return self.create_success_response(
                task_id=task_id,
                output=output,
                execution_time=execution_time,
                green_metrics=green_metrics,
                reasoning_trace=reasoning_trace
            )
            
        except Exception as e:
            return self.create_failure_response(
                task_id=task_id,
                status=TaskStatus.INVALID_OUTPUT,
                error_message=f"Failed to transform agent output: {str(e)}",
                execution_time=execution_time
            )
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get gateway statistics"""
        return {
            "total_requests": self.request_count,
            "total_errors": self.error_count,
            "error_rate": self.error_count / max(self.request_count, 1),
            "version": self.default_version.value
        }


def create_a2a_task(
    task_id: str,
    task_type: str,
    query: str,
    max_tokens: Optional[int] = None,
    timeout_seconds: Optional[int] = None
) -> Dict[str, Any]:
    """
    Helper function to create A2A-compliant task request
    
    Args:
        task_id: Unique task identifier
        task_type: Type of task (e.g., 'research', 'qa', 'summarization')
        query: Task query/question
        max_tokens: Optional token limit
        timeout_seconds: Optional timeout
        
    Returns:
        A2A-compliant request dictionary
    """
    request = {
        "task_id": task_id,
        "task_type": task_type,
        "input_data": {
            "query": query
        },
        "version": "1.1"
    }
    
    if max_tokens or timeout_seconds:
        request["constraints"] = {}
        if max_tokens:
            request["constraints"]["max_tokens"] = max_tokens
        if timeout_seconds:
            request["constraints"]["timeout_seconds"] = timeout_seconds
    
    return request
