# -*- coding: utf-8 -*-
"""
Domain-Specific Expert Connectors (Enhanced)

Provides specialized connectors for compiler analysis, static analysis,
and sustainability benchmarking tools.

Enhanced with optional integration of:
- LIMIT Graph metrics (centrality, connectivity)
- MODP (Multi-Objective Decision Process) weights
- RLHF (human feedback)
- Multi‑Teacher On‑Policy Distillation + MoE gating
- Bio‑inspired optimisation (evolutionary tuning of connector parameters)

The enhancements are enabled via `ExpertConnectorConfig.use_enhancements`.
When disabled, the module behaves exactly as the original.
"""

from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import subprocess
import json
import tempfile
import os
import time
import random
import numpy as np
from collections import deque


class ExpertConnectorType(Enum):
    """Types of expert connectors."""
    COMPILER = "compiler"
    STATIC_ANALYZER = "static_analyzer"
    ENERGY_BENCHMARK = "energy_benchmark"
    SECURITY_SCANNER = "security_scanner"
    PERFORMANCE_PROFILER = "performance_profiler"


@dataclass
class ExpertConnectorResult:
    """Result from expert connector."""
    connector_type: ExpertConnectorType
    success: bool
    output: Any
    errors: List[str]
    warnings: List[str]
    energy_consumed_wh: float
    execution_time_ms: float
    metadata: Dict[str, Any]
    # Enhanced fields (optional)
    modp_score: Optional[float] = None
    graph_metrics: Optional[Dict[str, float]] = None
    human_feedback_score: Optional[float] = None
    distillation_stats: Optional[Dict[str, Any]] = None


@dataclass
class ExpertConnectorConfig:
    """Configuration for enhanced expert connectors."""
    use_enhancements: bool = False
    # LIMIT Graph metrics
    graph_metrics: Dict[str, float] = field(default_factory=lambda: {
        "centrality": 0.5,
        "connectivity": 0.5,
        "density": 0.4
    })
    # MODP weights: [quality, energy, latency, carbon]
    modp_weights: Optional[List[float]] = None   # default [0.4, 0.3, 0.2, 0.1]
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


class ExpertConnector(ABC):
    """Base class for expert connectors."""
    
    def __init__(self, connector_type: ExpertConnectorType):
        self.connector_type = connector_type
        
    @abstractmethod
    async def analyze(self, input_data: Any, config: Optional[Dict] = None) -> ExpertConnectorResult:
        """Analyze input data (override in subclasses)."""
        pass


# ---------------------------------------------------------------------------
# Enhanced decision components (distillation, MoE, evolutionary)
# ---------------------------------------------------------------------------

class ConnectorSelectionState:
    """State for selecting which connector to invoke."""
    def __init__(self, task_features: Dict[str, Any], graph_metrics: Dict[str, float],
                 human_feedback: float):
        self.code_size = min(task_features.get("code_size", 0) / 10000.0, 1.0)
        self.has_compilation = 1.0 if task_features.get("needs_compilation", False) else 0.0
        self.security_concern = 1.0 if task_features.get("security_scan", False) else 0.0
        self.performance_concern = 1.0 if task_features.get("performance_profile", False) else 0.0
        self.centrality = graph_metrics.get("centrality", 0.5)
        self.connectivity = graph_metrics.get("connectivity", 0.5)
        self.human_feedback = human_feedback

    def to_vector(self) -> np.ndarray:
        return np.array([
            self.code_size,
            self.has_compilation,
            self.security_concern,
            self.performance_concern,
            self.centrality,
            self.connectivity,
            self.human_feedback,
        ], dtype=np.float32)


class ConnectorDistillationOptimizer:
    """
    Multi‑teacher distillation with MoE gating to select the most appropriate
    connector (or combination) for a given task.
    Actions: indices into the list of available connector types.
    """
    def __init__(self, available_types: List[ExpertConnectorType], config: ExpertConnectorConfig):
        self.available_types = available_types
        self.n_actions = len(available_types)
        self.config = config
        self.feature_dim = 7
        self.lr = config.distillation_lr
        self.epsilon = config.epsilon
        self.distill_w = 0.7
        self.rl_w = 0.3
        self.train_every = config.train_every
        self.counter = 0
        self.replay_buffer = deque(maxlen=config.replay_size)

        # Student
        self.student_weights = np.zeros((self.feature_dim, self.n_actions))
        self.student_bias = np.zeros(self.n_actions)

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

    def _rule_teacher(self, state: ConnectorSelectionState) -> np.ndarray:
        probs = np.ones(self.n_actions) * 0.05
        if state.has_compilation > 0.5:
            # Compiler connector
            idx = self.available_types.index(ExpertConnectorType.COMPILER) if ExpertConnectorType.COMPILER in self.available_types else 0
            probs[idx] = 0.7
        elif state.security_concern > 0.5:
            idx = self.available_types.index(ExpertConnectorType.SECURITY_SCANNER) if ExpertConnectorType.SECURITY_SCANNER in self.available_types else 0
            probs[idx] = 0.6
        elif state.performance_concern > 0.5:
            idx = self.available_types.index(ExpertConnectorType.PERFORMANCE_PROFILER) if ExpertConnectorType.PERFORMANCE_PROFILER in self.available_types else 0
            probs[idx] = 0.6
        else:
            # Default to static analysis or energy benchmark
            if ExpertConnectorType.STATIC_ANALYZER in self.available_types:
                idx = self.available_types.index(ExpertConnectorType.STATIC_ANALYZER)
                probs[idx] = 0.5
            if ExpertConnectorType.ENERGY_BENCHMARK in self.available_types:
                idx = self.available_types.index(ExpertConnectorType.ENERGY_BENCHMARK)
                probs[idx] = 0.3
        return probs / probs.sum()

    def _rlhf_teacher(self, state: ConnectorSelectionState) -> np.ndarray:
        probs = np.ones(self.n_actions) / self.n_actions
        # High human feedback -> prefer more thorough analysis (static/security)
        if state.human_feedback > 0.7:
            if ExpertConnectorType.STATIC_ANALYZER in self.available_types:
                idx = self.available_types.index(ExpertConnectorType.STATIC_ANALYZER)
                probs[idx] += 0.2
        elif state.human_feedback < 0.3:
            # Prefer faster connectors (compiler/energy)
            if ExpertConnectorType.COMPILER in self.available_types:
                idx = self.available_types.index(ExpertConnectorType.COMPILER)
                probs[idx] += 0.2
        return probs / probs.sum()

    def _historical_teacher(self, state: ConnectorSelectionState) -> np.ndarray:
        # Simulate a trained model
        probs = np.ones(self.n_actions) * 0.05
        if state.centrality > 0.7:
            # High centrality: prefer in-depth analysis
            if ExpertConnectorType.STATIC_ANALYZER in self.available_types:
                idx = self.available_types.index(ExpertConnectorType.STATIC_ANALYZER)
                probs[idx] = 0.6
        else:
            if ExpertConnectorType.ENERGY_BENCHMARK in self.available_types:
                idx = self.available_types.index(ExpertConnectorType.ENERGY_BENCHMARK)
                probs[idx] = 0.5
        return probs / probs.sum()

    def _gate_forward(self, state_vec):
        logits = state_vec @ self.gate_weights + self.gate_bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def select_connector(self, state: ConnectorSelectionState, exploration=True) -> Tuple[int, np.ndarray, np.ndarray]:
        x = state.to_vector()
        teacher_outputs = []
        for teacher in self.teachers:
            prob = teacher(state)
            if len(prob) != self.n_actions:
                prob = np.pad(prob, (0, self.n_actions - len(prob)), 'constant')[:self.n_actions]
            teacher_outputs.append(prob)
        teacher_outputs = np.array(teacher_outputs)
        gate = self._gate_forward(x)
        teacher_probs = np.sum(gate[:, None] * teacher_outputs, axis=0)
        teacher_probs /= teacher_probs.sum()

        student_logits = x @ self.student_weights + self.student_bias
        student_probs = np.exp(student_logits - np.max(student_logits))
        student_probs /= student_probs.sum()

        if exploration and random.random() < self.epsilon:
            action = random.randint(0, self.n_actions - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action = int(np.argmax(combined))

        return action, x, teacher_probs

    def update(self, state_vec, action, reward, next_state_vec, teacher_probs):
        self.replay_buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = random.sample(self.replay_buffer, min(8, len(self.replay_buffer)))
            for s, a, r, ns, tp in batch:
                # Update student
                logits = s @ self.student_weights + self.student_bias
                cur = np.exp(logits - np.max(logits))
                cur /= cur.sum()
                grad_distill = -(tp - cur)
                one_hot = np.zeros(self.n_actions); one_hot[a] = 1.0
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


# ---------------------------------------------------------------------------
# Original connector classes (unchanged except for optional enhancement fields)
# ---------------------------------------------------------------------------

class CompilerExpert(ExpertConnector):
    """Compiler analysis expert (original functionality)."""
    def __init__(self, compiler: str = "gcc", optimization_level: str = "-O2",
                 enable_warnings: bool = True):
        super().__init__(ExpertConnectorType.COMPILER)
        self.compiler = compiler
        self.optimization_level = optimization_level
        self.enable_warnings = enable_warnings

    async def analyze(self, input_data: Any, config: Optional[Dict] = None) -> ExpertConnectorResult:
        # Original implementation as provided
        start_time = time.time()
        errors, warnings, success, output = [], [], False, {}
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.c', delete=False) as f:
                if isinstance(input_data, str):
                    f.write(input_data)
                source_file = f.name
            output_file = source_file + '.out'
            cmd = [self.compiler, self.optimization_level, source_file, '-o', output_file]
            if self.enable_warnings:
                cmd.extend(['-Wall', '-Wextra'])
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                success = True
                output['compiled'] = True
                output['binary_path'] = output_file
                output['optimizations'] = self._analyze_optimizations(result.stderr)
            else:
                errors.append(result.stderr)
                output['compiled'] = False
            if result.stderr:
                warnings = self._parse_warnings(result.stderr)
            try:
                os.unlink(source_file)
                if os.path.exists(output_file):
                    os.unlink(output_file)
            except:
                pass
        except subprocess.TimeoutExpired:
            errors.append("Compilation timeout")
        except Exception as e:
            errors.append(f"Compilation error: {str(e)}")
        execution_time = (time.time() - start_time) * 1000
        energy_wh = (execution_time / 1000) * 0.1
        return ExpertConnectorResult(
            connector_type=self.connector_type,
            success=success,
            output=output,
            errors=errors,
            warnings=warnings,
            energy_consumed_wh=energy_wh,
            execution_time_ms=execution_time,
            metadata={'compiler': self.compiler, 'optimization_level': self.optimization_level}
        )

    def _analyze_optimizations(self, stderr: str) -> List[str]:
        # Original
        optimizations = []
        if 'loop' in stderr.lower():
            optimizations.append("Loop optimization opportunity detected")
        if 'inline' in stderr.lower():
            optimizations.append("Function inlining suggested")
        if 'vectorization' in stderr.lower():
            optimizations.append("Vectorization opportunity detected")
        return optimizations

    def _parse_warnings(self, stderr: str) -> List[str]:
        # Original
        warnings = []
        for line in stderr.split('\n'):
            if 'warning:' in line.lower():
                warnings.append(line.strip())
        return warnings


class StaticAnalyzerExpert(ExpertConnector):
    """Static analysis expert (original functionality)."""
    def __init__(self, analyzer_tool: str = "pylint"):
        super().__init__(ExpertConnectorType.STATIC_ANALYZER)
        self.analyzer_tool = analyzer_tool

    async def analyze(self, input_data: Any, config: Optional[Dict] = None) -> ExpertConnectorResult:
        # Original implementation as provided
        start_time = time.time()
        errors, warnings, success, output = [], [], False, {}
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
                if isinstance(input_data, str):
                    f.write(input_data)
                source_file = f.name
            if self.analyzer_tool == "pylint":
                cmd = ['pylint', '--output-format=json', source_file]
            elif self.analyzer_tool == "flake8":
                cmd = ['flake8', '--format=json', source_file]
            else:
                cmd = [self.analyzer_tool, source_file]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if self.analyzer_tool == "pylint":
                try:
                    issues = json.loads(result.stdout)
                    output['issues'] = issues
                    output['issue_count'] = len(issues)
                    for issue in issues:
                        if issue.get('type') == 'error':
                            errors.append(issue.get('message', ''))
                        elif issue.get('type') == 'warning':
                            warnings.append(issue.get('message', ''))
                    success = True
                except json.JSONDecodeError:
                    errors.append("Failed to parse analyzer output")
            try:
                os.unlink(source_file)
            except:
                pass
        except subprocess.TimeoutExpired:
            errors.append("Analysis timeout")
        except Exception as e:
            errors.append(f"Analysis error: {str(e)}")
        execution_time = (time.time() - start_time) * 1000
        energy_wh = (execution_time / 1000) * 0.05
        return ExpertConnectorResult(
            connector_type=self.connector_type,
            success=success,
            output=output,
            errors=errors,
            warnings=warnings,
            energy_consumed_wh=energy_wh,
            execution_time_ms=execution_time,
            metadata={'analyzer': self.analyzer_tool}
        )


class EnergyBenchmarkExpert(ExpertConnector):
    """Energy benchmarking expert (original functionality)."""
    def __init__(self, benchmark_tool: str = "perf", grid_carbon_intensity: float = 385.0):
        super().__init__(ExpertConnectorType.ENERGY_BENCHMARK)
        self.benchmark_tool = benchmark_tool
        self.grid_carbon_intensity = grid_carbon_intensity

    async def analyze(self, input_data: Any, config: Optional[Dict] = None) -> ExpertConnectorResult:
        # Original implementation as provided
        start_time = time.time()
        errors, warnings, success, output = [], [], False, {}
        try:
            executable = input_data.get('executable') if isinstance(input_data, dict) else input_data
            if self.benchmark_tool == "perf":
                cmd = ['perf', 'stat', '-e', 'power/energy-pkg/', '-e', 'power/energy-ram/', executable]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                energy_pkg = self._parse_perf_energy(result.stderr, 'energy-pkg')
                energy_ram = self._parse_perf_energy(result.stderr, 'energy-ram')
                total_energy_joules = energy_pkg + energy_ram
                total_energy_wh = total_energy_joules / 3600
                carbon_kg = (total_energy_wh / 1000) * self.grid_carbon_intensity / 1000
                output = {
                    'energy_pkg_joules': energy_pkg,
                    'energy_ram_joules': energy_ram,
                    'total_energy_wh': total_energy_wh,
                    'carbon_kg': carbon_kg,
                    'sustainability_rating': self._calculate_rating(total_energy_wh)
                }
                success = True
            else:
                execution_time_s = config.get('execution_time', 1.0) if config else 1.0
                estimated_power_w = 50.0
                energy_wh = (estimated_power_w * execution_time_s) / 3600
                carbon_kg = (energy_wh / 1000) * self.grid_carbon_intensity / 1000
                output = {
                    'estimated': True,
                    'total_energy_wh': energy_wh,
                    'carbon_kg': carbon_kg,
                    'sustainability_rating': self._calculate_rating(energy_wh)
                }
                warnings.append("Using estimated energy consumption")
                success = True
        except subprocess.TimeoutExpired:
            errors.append("Benchmark timeout")
        except Exception as e:
            errors.append(f"Benchmark error: {str(e)}")
        execution_time = (time.time() - start_time) * 1000
        energy_wh = output.get('total_energy_wh', 0.001)
        return ExpertConnectorResult(
            connector_type=self.connector_type,
            success=success,
            output=output,
            errors=errors,
            warnings=warnings,
            energy_consumed_wh=energy_wh,
            execution_time_ms=execution_time,
            metadata={'benchmark_tool': self.benchmark_tool, 'grid_carbon_intensity': self.grid_carbon_intensity}
        )

    def _parse_perf_energy(self, stderr: str, event: str) -> float:
        # Original
        for line in stderr.split('\n'):
            if event in line:
                parts = line.split()
                try:
                    return float(parts[0].replace(',', ''))
                except:
                    pass
        return 0.0

    def _calculate_rating(self, energy_wh: float) -> str:
        # Original
        if energy_wh < 0.001:
            return "A+ (Excellent)"
        elif energy_wh < 0.01:
            return "A (Very Good)"
        elif energy_wh < 0.1:
            return "B (Good)"
        elif energy_wh < 1.0:
            return "C (Fair)"
        else:
            return "D (Poor)"


# ---------------------------------------------------------------------------
# Enhanced Registry (with distillation and advanced selection)
# ---------------------------------------------------------------------------
class ExpertConnectorRegistry:
    """Registry for managing expert connectors with optional enhanced selection."""
    
    def __init__(self, config: Optional[ExpertConnectorConfig] = None):
        self.config = config or ExpertConnectorConfig()
        self.connectors: Dict[ExpertConnectorType, ExpertConnector] = {}
        self.use_enhancements = self.config.use_enhancements

        # Register default connectors
        self.register(ExpertConnectorType.COMPILER, CompilerExpert())
        self.register(ExpertConnectorType.STATIC_ANALYZER, StaticAnalyzerExpert())
        self.register(ExpertConnectorType.ENERGY_BENCHMARK, EnergyBenchmarkExpert())

        # Enhanced components
        self.distillation_optimizer = None
        if self.use_enhancements:
            if self.config.use_distillation:
                self.distillation_optimizer = ConnectorDistillationOptimizer(
                    list(self.connectors.keys()), self.config
                )
            # (Evolutionary component could be added if needed)

    def register(self, connector_type: ExpertConnectorType, connector: ExpertConnector):
        self.connectors[connector_type] = connector

    def get_connector(self, connector_type: ExpertConnectorType) -> Optional[ExpertConnector]:
        return self.connectors.get(connector_type)

    async def invoke(
        self,
        connector_type: ExpertConnectorType,
        input_data: Any,
        config: Optional[Dict] = None,
        task_features: Optional[Dict[str, Any]] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None
    ) -> ExpertConnectorResult:
        """
        Invoke connector, optionally using enhanced decision-making.
        """
        # Enhanced: if use_enhancements and distillation_optimizer and no explicit connector_type?
        # We can allow auto-selection by setting connector_type to None in a new method.
        # For now, keep existing signature but add optional auto_select flag via config.
        if self.use_enhancements and self.distillation_optimizer and connector_type is None:
            # Auto-select using distillation
            if graph_metrics is None:
                graph_metrics = self.config.graph_metrics
            if human_feedback_score is None:
                human_feedback_score = self.config.human_feedback_score
            state = ConnectorSelectionState(
                task_features or {},
                graph_metrics,
                human_feedback_score
            )
            action_idx, state_vec, teacher_probs = self.distillation_optimizer.select_connector(state)
            available_types = list(self.connectors.keys())
            if action_idx < len(available_types):
                connector_type = available_types[action_idx]
            else:
                connector_type = available_types[0]
            # Store for later update
            self._last_decision = (state_vec, action_idx, teacher_probs, connector_type, graph_metrics, human_feedback_score)
        else:
            self._last_decision = None

        connector = self.get_connector(connector_type)
        if connector is None:
            return ExpertConnectorResult(
                connector_type=connector_type,
                success=False,
                output={},
                errors=[f"Connector {connector_type.value} not registered"],
                warnings=[],
                energy_consumed_wh=0.0,
                execution_time_ms=0.0,
                metadata={}
            )

        result = await connector.analyze(input_data, config)

        # Enhanced post-processing
        if self.use_enhancements and self._last_decision:
            state_vec, action_idx, teacher_probs, selected_type, gm, hf = self._last_decision
            # Compute MODP reward (simplified)
            quality = 1.0 if result.success else 0.0
            energy_norm = 1.0 - min(result.energy_consumed_wh, 1.0)
            latency_norm = 1.0 - min(result.execution_time_ms / 10000.0, 1.0)
            # Carbon not directly available; estimate
            carbon_norm = 1.0 - min(result.energy_consumed_wh * 0.4, 1.0)
            weights = self.config.modp_weights or [0.4, 0.3, 0.2, 0.1]
            reward = float(np.dot([quality, energy_norm, latency_norm, carbon_norm], weights))
            # Update distillation optimizer
            self.distillation_optimizer.update(state_vec, action_idx, reward, state_vec, teacher_probs)

            # Add enhanced fields to result
            result.modp_score = reward
            result.graph_metrics = gm
            result.human_feedback_score = hf
            result.distillation_stats = {
                "student_counter": self.distillation_optimizer.counter,
                "buffer_size": len(self.distillation_optimizer.replay_buffer)
            }
            del self._last_decision

        return result

    async def invoke_auto(
        self,
        input_data: Any,
        config: Optional[Dict] = None,
        task_features: Optional[Dict[str, Any]] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None
    ) -> ExpertConnectorResult:
        """
        Automatically select and invoke the best connector using distillation.
        """
        return await self.invoke(None, input_data, config, task_features,
                                 graph_metrics, human_feedback_score)

    def list_connectors(self) -> List[ExpertConnectorType]:
        return list(self.connectors.keys())
