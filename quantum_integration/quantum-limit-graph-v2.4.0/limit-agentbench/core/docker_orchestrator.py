# -*- coding: utf-8 -*-
"""
Docker Orchestrator - Independent Execution Manager (Enhanced)
Manages containerized agent execution for AgentBeats compliance.

Enhancements (enabled via `config` or `use_enhancements` flag):
  - LIMIT Graph: graph metrics passed to decision state.
  - MODP: reward computed from energy, latency, carbon with configurable weights.
  - RLHF: human feedback score influences resource allocation decision.
  - Multi‑Teacher On‑Policy Distillation + MoE: selects container resource limits.
  - Bio‑inspired optimisation: evolutionary tuning of resource selection.
  - All original functionality preserved.
"""

import json
import subprocess
import time
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path
from dataclasses import dataclass, field
import random
import numpy as np
from collections import deque


@dataclass
class ContainerConfig:
    """Docker container configuration."""
    image: str
    cpu_limit: str = "2.0"
    memory_limit: str = "4g"
    gpu_limit: Optional[str] = None
    timeout_seconds: int = 300
    # Enhanced resource choices (discrete options for distillation)
    cpu_options: List[str] = field(default_factory=lambda: ["1.0", "2.0", "4.0", "8.0"])
    memory_options: List[str] = field(default_factory=lambda: ["2g", "4g", "8g", "16g"])

    def to_docker_args(self) -> list:
        """Convert to docker run arguments."""
        args = [
            "--cpus", self.cpu_limit,
            "--memory", self.memory_limit,
        ]
        if self.gpu_limit:
            args.extend(["--gpus", self.gpu_limit])
        return args


@dataclass
class OrchestratorConfig:
    """Configuration for enhanced orchestrator features."""
    use_enhancements: bool = False
    # LIMIT Graph
    graph_metrics: Dict[str, float] = field(default_factory=lambda: {
        "centrality": 0.5,
        "connectivity": 0.5,
        "density": 0.4
    })
    # MODP weights: [energy, latency, carbon]
    modp_weights: Optional[List[float]] = None  # default [0.4, 0.3, 0.3]
    # RLHF
    human_feedback_score: float = 0.5
    # Distillation + MoE
    use_distillation: bool = True
    distillation_lr: float = 0.01
    gating_lr: float = 0.005
    replay_size: int = 2000
    train_every: int = 10
    epsilon: float = 0.1
    # Bio‑inspired
    use_evolutionary: bool = False
    population_size: int = 10
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2


class ResourceAllocationState:
    """State representation for choosing CPU and memory limits."""
    def __init__(self, task_request: Dict[str, Any],
                 graph_metrics: Dict[str, float],
                 human_feedback: float):
        # Extract task features
        self.task_type = task_request.get('task_type', 'unknown')
        self.input_size = len(str(task_request.get('input_data', {})))
        self.priority = task_request.get('priority', 1)
        self.has_constraints = 1.0 if task_request.get('constraints') else 0.0
        # Graph and RLHF
        self.graph_centrality = graph_metrics.get('centrality', 0.5)
        self.graph_connectivity = graph_metrics.get('connectivity', 0.5)
        self.human_feedback = human_feedback
        # Normalized features
        self.features = np.array([
            min(self.input_size / 10000.0, 1.0),
            min(self.priority / 10.0, 1.0),
            self.has_constraints,
            self.graph_centrality,
            self.graph_connectivity,
            self.human_feedback,
            len(self.task_type) / 50.0,
        ], dtype=np.float32)


class ResourceDistillationOptimizer:
    """
    Distillation + MoE gating to select CPU/memory options.
    Actions: indices into cpu_options and memory_options (treated as combined).
    Simplified: one action selecting a combined resource profile index.
    """
    def __init__(self, n_profiles: int, config: OrchestratorConfig):
        self.n_profiles = n_profiles  # e.g., 4 profiles
        self.config = config
        self.feature_dim = 7  # from ResourceAllocationState
        self.student_weights = np.zeros((self.feature_dim, self.n_profiles))
        self.student_bias = np.zeros(self.n_profiles)
        self.lr = config.distillation_lr
        self.epsilon = config.epsilon
        self.distill_w = 0.7
        self.rl_w = 0.3
        self.replay = deque(maxlen=config.replay_size)
        self.counter = 0
        self.train_every = config.train_every

        # Teachers (rule-based, RLHF, historical)
        self.teachers = [
            self._rule_teacher,
            self._rlhf_teacher,
            self._historical_teacher,
        ]
        # MoE gating
        self.gate_weights = np.random.randn(self.feature_dim, len(self.teachers)) * 0.01
        self.gate_bias = np.zeros(len(self.teachers))
        self.gate_lr = config.gating_lr

    def _rule_teacher(self, state: ResourceAllocationState) -> np.ndarray:
        # Higher priority or larger input -> more resources
        if state.features[1] > 0.7 or state.features[0] > 0.7:
            probs = np.array([0.0, 0.1, 0.3, 0.6])
        elif state.features[1] > 0.3:
            probs = np.array([0.1, 0.4, 0.4, 0.1])
        else:
            probs = np.array([0.6, 0.3, 0.1, 0.0])
        return probs / probs.sum()

    def _rlhf_teacher(self, state: ResourceAllocationState) -> np.ndarray:
        # Human feedback: high -> more resources (perceived quality), low -> fewer
        probs = np.ones(self.n_profiles) / self.n_profiles
        if state.human_feedback > 0.7:
            probs[-1] += 0.3
            probs[0] -= 0.1
        elif state.human_feedback < 0.3:
            probs[0] += 0.3
            probs[-1] -= 0.1
        return probs / probs.sum()

    def _historical_teacher(self, state: ResourceAllocationState) -> np.ndarray:
        # Simulate a trained model
        if state.features[2] > 0.5:  # has constraints
            return np.array([0.0, 0.1, 0.3, 0.6])
        else:
            return np.array([0.4, 0.4, 0.2, 0.0])

    def _gate_forward(self, state_vec):
        logits = state_vec @ self.gate_weights + self.gate_bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def select_profile(self, state: ResourceAllocationState, exploration=True) -> Tuple[int, np.ndarray, np.ndarray]:
        x = state.features
        teacher_outputs = []
        for teacher in self.teachers:
            prob = teacher(state)
            if len(prob) != self.n_profiles:
                prob = np.pad(prob, (0, self.n_profiles - len(prob)), 'constant')[:self.n_profiles]
            teacher_outputs.append(prob)
        teacher_outputs = np.array(teacher_outputs)
        gate = self._gate_forward(x)
        teacher_probs = np.sum(gate[:, None] * teacher_outputs, axis=0)
        teacher_probs /= teacher_probs.sum()

        student_logits = x @ self.student_weights + self.student_bias
        student_probs = np.exp(student_logits - np.max(student_logits))
        student_probs /= student_probs.sum()

        if exploration and random.random() < self.epsilon:
            action = random.randint(0, self.n_profiles - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action = int(np.argmax(combined))

        return action, x, teacher_probs

    def update(self, state_vec, action, reward, next_state_vec, teacher_probs):
        self.replay.append((state_vec, action, reward, next_state_vec, teacher_probs))
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay) >= 8:
            batch = random.sample(self.replay, min(8, len(self.replay)))
            for s, a, r, ns, tp in batch:
                # Update student
                logits = s @ self.student_weights + self.student_bias
                cur = np.exp(logits - np.max(logits))
                cur /= cur.sum()
                grad_distill = -(tp - cur)
                one_hot = np.zeros(self.n_profiles); one_hot[a] = 1.0
                grad_rl = -r * (one_hot - cur)
                grad = self.distill_w * grad_distill + self.rl_w * grad_rl
                self.student_weights -= self.lr * np.outer(s, grad)
                self.student_bias -= self.lr * grad

                # Update gating
                gate = self._gate_forward(s)
                combined_teacher = np.sum(gate[:, None] * tp, axis=0)
                error = combined_teacher - cur
                grad_gate = np.dot(tp, error)
                self.gate_weights -= self.gate_lr * np.outer(s, grad_gate)
                self.gate_bias -= self.gate_lr * grad_gate


class DockerOrchestrator:
    """
    Docker Orchestrator for Independent Agent Execution with optional enhancements.
    """

    def __init__(self, work_dir: Path = Path("./work"),
                 orchestrator_config: Optional[OrchestratorConfig] = None):
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(exist_ok=True)
        self.containers = {}
        self.config = orchestrator_config or OrchestratorConfig()

        # Enhanced components
        if self.config.use_enhancements:
            # Determine number of resource profiles from ContainerConfig defaults
            # Use fixed 4 profiles (can be overridden)
            n_profiles = 4
            self.resource_optimizer = ResourceDistillationOptimizer(n_profiles, self.config)
            # Optional evolutionary optimizer for weights (not implemented fully here)
            if self.config.use_evolutionary:
                # Placeholder; would evolve MODP weights or profiles
                pass
        else:
            self.resource_optimizer = None

    def execute_task(
        self,
        task_request: Dict[str, Any],
        config: ContainerConfig,
        agent_image: str = "limit-graph-agent:latest",
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Execute agent task in isolated Docker container.

        If enhancements are enabled, resource limits are chosen by distillation
        based on task features, graph metrics, and human feedback.
        """
        task_id = task_request.get("task_id", "unknown")

        # Prepare directories
        task_dir = self.work_dir / task_id
        task_dir.mkdir(exist_ok=True)
        input_file = task_dir / "input.json"
        output_file = task_dir / "output.json"

        with open(input_file, 'w') as f:
            json.dump(task_request, f, indent=2)

        # Use provided or default graph metrics / human feedback
        if graph_metrics is None:
            graph_metrics = self.config.graph_metrics
        if human_feedback_score is None:
            human_feedback_score = self.config.human_feedback_score

        # Enhanced resource selection
        selected_cpu = config.cpu_limit
        selected_mem = config.memory_limit
        selected_profile = None
        if self.resource_optimizer:
            state = ResourceAllocationState(task_request, graph_metrics, human_feedback_score)
            profile_idx, state_vec, teacher_probs = self.resource_optimizer.select_profile(state)
            # Map profile index to CPU/memory options
            cpu_options = config.cpu_options
            mem_options = config.memory_options
            # Ensure index within range
            profile_idx = min(profile_idx, len(cpu_options)-1, len(mem_options)-1)
            selected_cpu = cpu_options[profile_idx]
            selected_mem = mem_options[profile_idx]
            selected_profile = profile_idx
            # Update config with selected limits
            config.cpu_limit = selected_cpu
            config.memory_limit = selected_mem
            # Store decision for later update
            self._last_decision = (state_vec, profile_idx, teacher_probs, task_request, selected_cpu, selected_mem)

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
            response = {
                "task_id": task_id,
                "status": "failure",
                "error": "Container failed to produce output",
                "execution_time_seconds": execution_time
            }

        # Add container metadata
        response["container_metadata"] = {
            "image": agent_image,
            "exit_code": result["exit_code"],
            "execution_time": execution_time,
            "resource_limits": {
                "cpu": selected_cpu,
                "memory": selected_mem
            },
            "profile_index": selected_profile,
            "graph_metrics": graph_metrics,
            "human_feedback_score": human_feedback_score,
        }

        # Enhanced reward calculation and update
        if self.resource_optimizer and hasattr(self, '_last_decision'):
            state_vec, action, teacher_probs, req, cpu, mem = self._last_decision
            # Estimate energy/carbon (simplified)
            energy_kwh = execution_time * 0.0001  # arbitrary
            carbon_kg = energy_kwh * 0.4
            latency_norm = 1.0 - min(execution_time / config.timeout_seconds, 1.0)
            energy_norm = 1.0 - min(energy_kwh * 1000, 1.0)
            carbon_norm = 1.0 - min(carbon_kg, 1.0)
            # MODP weights default
            weights = self.config.modp_weights or [0.4, 0.3, 0.3]
            reward = float(np.dot([energy_norm, latency_norm, carbon_norm], weights))
            # Update optimizer
            self.resource_optimizer.update(
                state_vec, action, reward, state_vec, teacher_probs
            )
            # Add to response
            response["modp_reward"] = reward
            response["distillation_stats"] = {
                "student_counter": self.resource_optimizer.counter,
                "buffer_size": len(self.resource_optimizer.replay)
            }
            del self._last_decision

        return response

    def _build_docker_command(
        self,
        config: ContainerConfig,
        image: str,
        task_dir: Path,
        input_file: Path,
        output_file: Path
    ) -> list:
        """Build docker run command (unchanged)."""
        cmd = ["docker", "run", "--rm", "--network", "none"]
        cmd.extend(config.to_docker_args())
        cmd.extend([
            "-v", f"{input_file.absolute()}:/app/input.json:ro",
            "-v", f"{output_file.absolute()}:/app/output.json:rw",
            "-v", f"{task_dir.absolute()}:/app/work:rw"
        ])
        cmd.extend([
            "-e", "A2A_INPUT=/app/input.json",
            "-e", "A2A_OUTPUT=/app/output.json",
            "-e", "ENABLE_GREEN_METRICS=true"
        ])
        cmd.append(image)
        return cmd

    def _run_container(
        self,
        docker_cmd: list,
        timeout: int,
        task_id: str
    ) -> Dict[str, Any]:
        """Run Docker container with timeout (unchanged)."""
        try:
            process = subprocess.Popen(
                docker_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            self.containers[task_id] = process
            try:
                stdout, stderr = process.communicate(timeout=timeout)
                exit_code = process.returncode
            except subprocess.TimeoutExpired:
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
            if task_id in self.containers:
                del self.containers[task_id]

    def cleanup(self, task_id: Optional[str] = None):
        """Clean up work directories (unchanged)."""
        import shutil
        if task_id:
            task_dir = self.work_dir / task_id
            if task_dir.exists():
                shutil.rmtree(task_dir)
        else:
            if self.work_dir.exists():
                shutil.rmtree(self.work_dir)
                self.work_dir.mkdir()

    def get_container_stats(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get container resource usage statistics (unchanged)."""
        try:
            result = subprocess.run(
                ["docker", "ps", "-q", "-f", f"label=task_id={task_id}"],
                capture_output=True, text=True
            )
            container_id = result.stdout.strip()
            if not container_id:
                return None
            result = subprocess.run(
                ["docker", "stats", container_id, "--no-stream", "--format", "json"],
                capture_output=True, text=True
            )
            if result.returncode == 0:
                return json.loads(result.stdout)
        except Exception:
            pass
        return None


# ---------------------------------------------------------------------------
# Helper functions (unchanged, but included for completeness)
# ---------------------------------------------------------------------------
def create_dockerfile(output_path: Path = Path("Dockerfile.agent")):
    """Create Dockerfile for AgentBeats-compliant agent (unchanged)."""
    dockerfile_content = """# AgentBeats-Compliant Agent Container
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY agent/ ./agent/
COPY config/ ./config/
COPY core/a2a_gateway.py ./core/
COPY core/rlhf_feedback_engine.py ./core/
COPY core/green_metrics.py ./core/
COPY docker_entrypoint.py .
ENV PYTHONUNBUFFERED=1
ENV A2A_VERSION=1.1
ENTRYPOINT ["python", "docker_entrypoint.py"]
"""
    with open(output_path, 'w') as f:
        f.write(dockerfile_content)
    print(f"✓ Dockerfile created: {output_path}")


def create_entrypoint(output_path: Path = Path("docker_entrypoint.py")):
    """Create Docker entrypoint script (unchanged)."""
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

from core.a2a_gateway import A2AGateway
from core.rlhf_feedback_engine import RLHFFeedbackEngine
from core.green_metrics import GreenMetricsCollector
from agent.research_agent import ResearchAgent


def main():
    input_file = os.getenv('A2A_INPUT', '/app/input.json')
    output_file = os.getenv('A2A_OUTPUT', '/app/output.json')

    try:
        with open(input_file, 'r') as f:
            task_request = json.load(f)

        gateway = A2AGateway()
        rlhf_engine = RLHFFeedbackEngine()
        green_metrics = GreenMetricsCollector()

        validated_request = gateway.validate_request(task_request)
        green_metrics.start_collection()

        agent = ResearchAgent()
        result = agent.execute(validated_request.input_data)

        metrics = green_metrics.stop_collection()
        feedback = rlhf_engine.analyze_reasoning_trace(
            reasoning_trace=result.get('reasoning_trace', []),
            task_type=validated_request.task_type,
            execution_time=result.get('execution_time', 0),
            success=result.get('success', False)
        )

        response = gateway.create_success_response(
            task_id=validated_request.task_id,
            output=result.get('output', {}),
            execution_time=result.get('execution_time', 0),
            green_metrics=metrics,
            reasoning_trace=result.get('reasoning_trace', []),
            metadata={'rlhf_feedback': feedback}
        )

    except Exception as e:
        response = gateway.create_failure_response(
            task_id=task_request.get('task_id', 'unknown'),
            status='failure',
            error_message=str(e)
        )

    with open(output_file, 'w') as f:
        json.dump(response.to_dict(), f, indent=2)

    sys.exit(0 if response.status.value == 'success' else 1)


if __name__ == '__main__':
    main()
"""
    with open(output_path, 'w') as f:
        f.write(entrypoint_content)
    output_path.chmod(0o755)
    print(f"✓ Entrypoint created: {output_path}")
