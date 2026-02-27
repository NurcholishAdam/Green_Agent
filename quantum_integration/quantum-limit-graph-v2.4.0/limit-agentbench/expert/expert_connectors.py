# -*- coding: utf-8 -*-
"""
Domain-Specific Expert Connectors

Provides specialized connectors for compiler analysis, static analysis,
and sustainability benchmarking tools.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum
from abc import ABC, abstractmethod
import subprocess
import json
import tempfile
import os


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


class ExpertConnector(ABC):
    """Base class for expert connectors."""
    
    def __init__(self, connector_type: ExpertConnectorType):
        self.connector_type = connector_type
        
    @abstractmethod
    async def analyze(self, input_data: Any, config: Optional[Dict] = None) -> ExpertConnectorResult:
        """Analyze input data (override in subclasses)."""
        pass


class CompilerExpert(ExpertConnector):
    """
    Compiler analysis expert.
    
    Provides:
    - Compilation validation
    - Optimization suggestions
    - Error detection and reporting
    - Performance analysis
    """
    
    def __init__(
        self,
        compiler: str = "gcc",
        optimization_level: str = "-O2",
        enable_warnings: bool = True
    ):
        super().__init__(ExpertConnectorType.COMPILER)
        self.compiler = compiler
        self.optimization_level = optimization_level
        self.enable_warnings = enable_warnings
        
    async def analyze(
        self,
        input_data: Any,
        config: Optional[Dict] = None
    ) -> ExpertConnectorResult:
        """
        Analyze code with compiler.
        
        Args:
            input_data: Source code (string or file path)
            config: Compiler configuration
            
        Returns:
            Compilation result with suggestions
        """
        import time
        start_time = time.time()
        
        errors = []
        warnings = []
        success = False
        output = {}
        
        try:
            # Create temporary file for source code
            with tempfile.NamedTemporaryFile(
                mode='w',
                suffix='.c',
                delete=False
            ) as f:
                if isinstance(input_data, str):
                    f.write(input_data)
                source_file = f.name
            
            # Build compiler command
            output_file = source_file + '.out'
            cmd = [
                self.compiler,
                self.optimization_level,
                source_file,
                '-o', output_file
            ]
            
            if self.enable_warnings:
                cmd.extend(['-Wall', '-Wextra'])
            
            # Run compiler
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            # Parse output
            if result.returncode == 0:
                success = True
                output['compiled'] = True
                output['binary_path'] = output_file
                
                # Analyze optimizations
                optimizations = self._analyze_optimizations(result.stderr)
                output['optimizations'] = optimizations
            else:
                errors.append(result.stderr)
                output['compiled'] = False
            
            # Parse warnings
            if result.stderr:
                warnings = self._parse_warnings(result.stderr)
            
            # Clean up
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
        
        # Estimate energy (compilation is CPU-intensive)
        energy_wh = (execution_time / 1000) * 0.1  # ~100W CPU for duration
        
        return ExpertConnectorResult(
            connector_type=self.connector_type,
            success=success,
            output=output,
            errors=errors,
            warnings=warnings,
            energy_consumed_wh=energy_wh,
            execution_time_ms=execution_time,
            metadata={
                'compiler': self.compiler,
                'optimization_level': self.optimization_level
            }
        )
    
    def _analyze_optimizations(self, stderr: str) -> List[str]:
        """Extract optimization suggestions from compiler output."""
        optimizations = []
        
        # Look for optimization opportunities
        if 'loop' in stderr.lower():
            optimizations.append("Loop optimization opportunity detected")
        if 'inline' in stderr.lower():
            optimizations.append("Function inlining suggested")
        if 'vectorization' in stderr.lower():
            optimizations.append("Vectorization opportunity detected")
        
        return optimizations
    
    def _parse_warnings(self, stderr: str) -> List[str]:
        """Parse warnings from compiler output."""
        warnings = []
        
        for line in stderr.split('\n'):
            if 'warning:' in line.lower():
                warnings.append(line.strip())
        
        return warnings


class StaticAnalyzerExpert(ExpertConnector):
    """
    Static analysis expert.
    
    Provides:
    - Code quality analysis
    - Bug detection
    - Security vulnerability scanning
    - Complexity metrics
    """
    
    def __init__(self, analyzer_tool: str = "pylint"):
        super().__init__(ExpertConnectorType.STATIC_ANALYZER)
        self.analyzer_tool = analyzer_tool
        
    async def analyze(
        self,
        input_data: Any,
        config: Optional[Dict] = None
    ) -> ExpertConnectorResult:
        """
        Analyze code with static analyzer.
        
        Args:
            input_data: Source code or file path
            config: Analyzer configuration
            
        Returns:
            Analysis result
        """
        import time
        start_time = time.time()
        
        errors = []
        warnings = []
        success = False
        output = {}
        
        try:
            # Create temporary file
            with tempfile.NamedTemporaryFile(
                mode='w',
                suffix='.py',
                delete=False
            ) as f:
                if isinstance(input_data, str):
                    f.write(input_data)
                source_file = f.name
            
            # Run static analyzer
            if self.analyzer_tool == "pylint":
                cmd = ['pylint', '--output-format=json', source_file]
            elif self.analyzer_tool == "flake8":
                cmd = ['flake8', '--format=json', source_file]
            else:
                cmd = [self.analyzer_tool, source_file]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            # Parse results
            if self.analyzer_tool == "pylint":
                try:
                    issues = json.loads(result.stdout)
                    output['issues'] = issues
                    output['issue_count'] = len(issues)
                    
                    # Categorize
                    for issue in issues:
                        if issue.get('type') == 'error':
                            errors.append(issue.get('message', ''))
                        elif issue.get('type') == 'warning':
                            warnings.append(issue.get('message', ''))
                    
                    success = True
                except json.JSONDecodeError:
                    errors.append("Failed to parse analyzer output")
            
            # Clean up
            try:
                os.unlink(source_file)
            except:
                pass
            
        except subprocess.TimeoutExpired:
            errors.append("Analysis timeout")
        except Exception as e:
            errors.append(f"Analysis error: {str(e)}")
        
        execution_time = (time.time() - start_time) * 1000
        energy_wh = (execution_time / 1000) * 0.05  # Lower power than compilation
        
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
    """
    Energy benchmarking expert.
    
    Provides:
    - Code energy profiling
    - Sustainability metrics
    - Carbon impact assessment
    - Optimization recommendations
    """
    
    def __init__(
        self,
        benchmark_tool: str = "perf",
        grid_carbon_intensity: float = 385.0
    ):
        super().__init__(ExpertConnectorType.ENERGY_BENCHMARK)
        self.benchmark_tool = benchmark_tool
        self.grid_carbon_intensity = grid_carbon_intensity
        
    async def analyze(
        self,
        input_data: Any,
        config: Optional[Dict] = None
    ) -> ExpertConnectorResult:
        """
        Benchmark energy consumption.
        
        Args:
            input_data: Executable or code to benchmark
            config: Benchmark configuration
            
        Returns:
            Energy benchmark result
        """
        import time
        start_time = time.time()
        
        errors = []
        warnings = []
        success = False
        output = {}
        
        try:
            # Prepare benchmark
            executable = input_data.get('executable') if isinstance(input_data, dict) else input_data
            
            if self.benchmark_tool == "perf":
                # Use perf to measure energy
                cmd = [
                    'perf', 'stat',
                    '-e', 'power/energy-pkg/',
                    '-e', 'power/energy-ram/',
                    executable
                ]
                
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=60
                )
                
                # Parse perf output
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
                # Fallback: estimate from execution time
                execution_time_s = config.get('execution_time', 1.0) if config else 1.0
                estimated_power_w = 50.0  # Assume 50W average
                
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
            metadata={
                'benchmark_tool': self.benchmark_tool,
                'grid_carbon_intensity': self.grid_carbon_intensity
            }
        )
    
    def _parse_perf_energy(self, stderr: str, event: str) -> float:
        """Parse energy value from perf output."""
        for line in stderr.split('\n'):
            if event in line:
                # Extract value (typically in Joules)
                parts = line.split()
                try:
                    value = float(parts[0].replace(',', ''))
                    return value
                except:
                    pass
        return 0.0
    
    def _calculate_rating(self, energy_wh: float) -> str:
        """Calculate sustainability rating."""
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


class ExpertConnectorRegistry:
    """
    Registry for managing expert connectors.
    
    Allows dynamic registration and invocation of domain-specific experts.
    """
    
    def __init__(self):
        self.connectors: Dict[ExpertConnectorType, ExpertConnector] = {}
        
        # Register default connectors
        self.register(ExpertConnectorType.COMPILER, CompilerExpert())
        self.register(ExpertConnectorType.STATIC_ANALYZER, StaticAnalyzerExpert())
        self.register(ExpertConnectorType.ENERGY_BENCHMARK, EnergyBenchmarkExpert())
        
    def register(
        self,
        connector_type: ExpertConnectorType,
        connector: ExpertConnector
    ):
        """Register expert connector."""
        self.connectors[connector_type] = connector
        
    def get_connector(
        self,
        connector_type: ExpertConnectorType
    ) -> Optional[ExpertConnector]:
        """Get registered connector."""
        return self.connectors.get(connector_type)
    
    async def invoke(
        self,
        connector_type: ExpertConnectorType,
        input_data: Any,
        config: Optional[Dict] = None
    ) -> ExpertConnectorResult:
        """Invoke connector."""
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
        
        return await connector.analyze(input_data, config)
    
    def list_connectors(self) -> List[ExpertConnectorType]:
        """List registered connectors."""
        return list(self.connectors.keys())
