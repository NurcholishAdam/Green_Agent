# -*- coding: utf-8 -*-
"""
Docker Orchestrator - Independent Execution Manager
Manages containerized agent execution for AgentBeats compliance
"""

import json
import subprocess
import time
from typing import Dict, Any, Optional
from pathlib import Path
from dataclasses import dataclass


@dataclass
class ContainerConfig:
    """Docker container configuration"""
    image: str
    cpu_limit: str = "2.0"
    memory_limit: str = "4g"
    gpu_limit: Optional[str] = None
    timeout_seconds: int = 300
    
    def to_docker_args(self) -> list:
        """Convert to docker run arguments"""
        args = [
            "--cpus", self.cpu_limit,
            "--memory", self.memory_limit,
        ]
        
        if self.gpu_limit:
            args.extend(["--gpus", self.gpu_limit])
        
        return args


class DockerOrchestrator:
    """
    Docker Orchestrator for Independent Agent Execution
    
    Provides:
    - Automated container lifecycle management
    - Resource isolation and limits
    - Zero manual intervention
    - A2A JSON input/output handling
    """
    
    def __init__(self, work_dir: Path = Path("./work")):
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(exist_ok=True)
        self.containers = {}
        
    def execute_task(
        self,
        task_request: Dict[str, Any],
        config: ContainerConfig,
        agent_image: str = "limit-graph-agent:latest"
    ) -> Dict[str, Any]:
        """
        Execute agent task in isolated Docker container
        
        Args:
            task_request: A2A-compliant task request
            config: Container configuration
            agent_image: Docker image name
            
        Returns:
            A2A-compliant response
        """
        task_id = task_request.get("task_id", "unknown")
        
        # Prepare input/output directories
        task_dir = self.work_dir / task_id
        task_dir.mkdir(exist_ok=True)
        
        input_file = task_dir / "input.json"
        output_file = task_dir / "output.json"
        
        # Write input
        with open(input_file, 'w') as f:
            json.dump(task_request, f, indent=2)
        
        # Build docker command
        docker_cmd = self._build_docker_command(
            config=config,
            image=agent_image,
            task_dir=task_dir,
            input_file=input_file,
            output_file=output_file
        )
        
        # Execute container
        start_time = time.time()
        result = self._run_container(
            docker_cmd,
            timeout=config.timeout_seconds,
            task_id=task_id
        )
        execution_time = time.time() - start_time
        
        # Read output
        if output_file.exists():
            with open(output_file, 'r') as f:
                response = json.load(f)
        else:
            # Container failed to produce output
            response = {
                "task_id": task_id,
                "status": "failure",
                "error": "Container failed to produce output",
                "execution_time_seconds": execution_time
            }
        
        # Add execution metadata
        response["container_metadata"] = {
            "image": agent_image,
            "exit_code": result["exit_code"],
            "execution_time": execution_time,
            "resource_limits": {
                "cpu": config.cpu_limit,
                "memory": config.memory_limit
            }
        }
        
        return response
    
    def _build_docker_command(
        self,
        config: ContainerConfig,
        image: str,
        task_dir: Path,
        input_file: Path,
        output_file: Path
    ) -> list:
        """Build docker run command"""
        cmd = [
            "docker", "run",
            "--rm",  # Remove container after execution
            "--network", "none",  # Isolated network (optional)
        ]
        
        # Add resource limits
        cmd.extend(config.to_docker_args())
        
        # Mount volumes
        cmd.extend([
            "-v", f"{input_file.absolute()}:/app/input.json:ro",
            "-v", f"{output_file.absolute()}:/app/output.json:rw",
            "-v", f"{task_dir.absolute()}:/app/work:rw"
        ])
        
        # Environment variables
        cmd.extend([
            "-e", "A2A_INPUT=/app/input.json",
            "-e", "A2A_OUTPUT=/app/output.json",
            "-e", "ENABLE_GREEN_METRICS=true"
        ])
        
        # Image and command
        cmd.append(image)
        
        return cmd
    
    def _run_container(
        self,
        docker_cmd: list,
        timeout: int,
        task_id: str
    ) -> Dict[str, Any]:
        """
        Run Docker container with timeout
        
        Returns:
            Execution result with exit code and logs
        """
        try:
            # Start container
            process = subprocess.Popen(
                docker_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Store container reference
            self.containers[task_id] = process
            
            # Wait with timeout
            try:
                stdout, stderr = process.communicate(timeout=timeout)
                exit_code = process.returncode
                
            except subprocess.TimeoutExpired:
                # Kill container on timeout
                process.kill()
                stdout, stderr = process.communicate()
                exit_code = -1
                stderr = f"Container timeout after {timeout}s\n{stderr}"
            
            return {
                "exit_code": exit_code,
                "stdout": stdout,
                "stderr": stderr,
                "timed_out": exit_code == -1
            }
            
        except Exception as e:
            return {
                "exit_code": -2,
                "stdout": "",
                "stderr": str(e),
                "timed_out": False
            }
        
        finally:
            # Cleanup
            if task_id in self.containers:
                del self.containers[task_id]
    
    def cleanup(self, task_id: Optional[str] = None):
        """Clean up work directories"""
        if task_id:
            task_dir = self.work_dir / task_id
            if task_dir.exists():
                import shutil
                shutil.rmtree(task_dir)
        else:
            # Clean all
            import shutil
            if self.work_dir.exists():
                shutil.rmtree(self.work_dir)
                self.work_dir.mkdir()
    
    def get_container_stats(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get container resource usage statistics"""
        try:
            # Get container ID from docker ps
            result = subprocess.run(
                ["docker", "ps", "-q", "-f", f"label=task_id={task_id}"],
                capture_output=True,
                text=True
            )
            
            container_id = result.stdout.strip()
            if not container_id:
                return None
            
            # Get stats
            result = subprocess.run(
                ["docker", "stats", container_id, "--no-stream", "--format", "json"],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                return json.loads(result.stdout)
            
        except Exception:
            pass
        
        return None


def create_dockerfile(output_path: Path = Path("Dockerfile.agent")):
    """
    Create Dockerfile for AgentBeats-compliant agent
    
    Args:
        output_path: Where to write Dockerfile
    """
    dockerfile_content = """# AgentBeats-Compliant Agent Container
FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy agent code
COPY agent/ ./agent/
COPY config/ ./config/

# Copy A2A integration
COPY core/a2a_gateway.py ./core/
COPY core/rlhf_feedback_engine.py ./core/
COPY core/green_metrics.py ./core/

# Entry point script
COPY docker_entrypoint.py .

# Set environment
ENV PYTHONUNBUFFERED=1
ENV A2A_VERSION=1.1

# Run agent
ENTRYPOINT ["python", "docker_entrypoint.py"]
"""
    
    with open(output_path, 'w') as f:
        f.write(dockerfile_content)
    
    print(f"✓ Dockerfile created: {output_path}")


def create_entrypoint(output_path: Path = Path("docker_entrypoint.py")):
    """
    Create Docker entrypoint script
    
    Args:
        output_path: Where to write entrypoint
    """
    entrypoint_content = """#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Docker Entrypoint for AgentBeats Agent
Reads A2A input, executes agent, writes A2A output
'''

import os
import json
import sys
from pathlib import Path

# Import agent components
from core.a2a_gateway import A2AGateway
from core.rlhf_feedback_engine import RLHFFeedbackEngine
from core.green_metrics import GreenMetricsCollector
from agent.research_agent import ResearchAgent


def main():
    # Read A2A input
    input_file = os.getenv('A2A_INPUT', '/app/input.json')
    output_file = os.getenv('A2A_OUTPUT', '/app/output.json')
    
    try:
        with open(input_file, 'r') as f:
            task_request = json.load(f)
        
        # Initialize components
        gateway = A2AGateway()
        rlhf_engine = RLHFFeedbackEngine()
        green_metrics = GreenMetricsCollector()
        
        # Validate request
        validated_request = gateway.validate_request(task_request)
        
        # Start metrics collection
        green_metrics.start_collection()
        
        # Execute agent
        agent = ResearchAgent()
        result = agent.execute(validated_request.input_data)
        
        # Stop metrics collection
        metrics = green_metrics.stop_collection()
        
        # Generate RLHF feedback
        feedback = rlhf_engine.analyze_reasoning_trace(
            reasoning_trace=result.get('reasoning_trace', []),
            task_type=validated_request.task_type,
            execution_time=result.get('execution_time', 0),
            success=result.get('success', False)
        )
        
        # Create A2A response
        response = gateway.create_success_response(
            task_id=validated_request.task_id,
            output=result.get('output', {}),
            execution_time=result.get('execution_time', 0),
            green_metrics=metrics,
            reasoning_trace=result.get('reasoning_trace', []),
            metadata={'rlhf_feedback': feedback}
        )
        
    except Exception as e:
        # Create failure response
        response = gateway.create_failure_response(
            task_id=task_request.get('task_id', 'unknown'),
            status='failure',
            error_message=str(e)
        )
    
    # Write output
    with open(output_file, 'w') as f:
        json.dump(response.to_dict(), f, indent=2)
    
    # Exit with appropriate code
    sys.exit(0 if response.status.value == 'success' else 1)


if __name__ == '__main__':
    main()
"""
    
    with open(output_path, 'w') as f:
        f.write(entrypoint_content)
    
    # Make executable
    output_path.chmod(0o755)
    
    print(f"✓ Entrypoint created: {output_path}")
