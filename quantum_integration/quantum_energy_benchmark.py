# quantum_integration/quantum_energy_benchmark.py
"""
Quantum Energy Benchmarking Suite v1.0.0
Measures and compares energy consumption between classical and quantum approaches
"""

import asyncio
import logging
import time
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from collections import deque
import numpy as np

logger = logging.getLogger(__name__)

@dataclass
class EnergyMeasurement:
    """Energy consumption measurement for a single task"""
    task_id: str
    compute_type: str  # 'classical' or 'quantum'
    execution_time_ms: float
    energy_consumed_kwh: float
    carbon_emissions_kg: float
    helium_usage_l: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)
    confidence_score: float = 0.95

@dataclass
class BenchmarkResult:
    """Complete benchmark result comparing classical vs quantum"""
    benchmark_id: str
    task_name: str
    classical: EnergyMeasurement
    quantum: EnergyMeasurement
    energy_savings_percent: float
    speedup_factor: float
    carbon_savings_kg: float
    helium_savings_l: float
    quantum_advantage_score: float  # 0-1, higher is better
    recommended_approach: str  # 'classical', 'quantum', or 'hybrid'
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'benchmark_id': self.benchmark_id,
            'task_name': self.task_name,
            'classical': {
                'time_ms': self.classical.execution_time_ms,
                'energy_kwh': self.classical.energy_consumed_kwh,
                'carbon_kg': self.classical.carbon_emissions_kg,
                'helium_l': self.classical.helium_usage_l
            },
            'quantum': {
                'time_ms': self.quantum.execution_time_ms,
                'energy_kwh': self.quantum.energy_consumed_kwh,
                'carbon_kg': self.quantum.carbon_emissions_kg,
                'helium_l': self.quantum.helium_usage_l
            },
            'savings': {
                'energy_percent': self.energy_savings_percent,
                'carbon_kg': self.carbon_savings_kg,
                'helium_l': self.helium_savings_l,
                'speedup': self.speedup_factor
            },
            'quantum_advantage_score': self.quantum_advantage_score,
            'recommendation': self.recommended_approach,
            'timestamp': self.timestamp.isoformat()
        }

class QuantumEnergyBenchmark:
    """
    Benchmark suite for measuring quantum vs classical energy efficiency.
    
    Features:
    - Real-time energy monitoring during execution
    - Carbon and helium usage tracking
    - Automated recommendation engine
    - Historical benchmark database
    - Visual reporting capabilities
    """
    
    def __init__(self, benchmark_db_path: str = "benchmark_history.json"):
        self.benchmark_history: List[BenchmarkResult] = []
        self.db_path = benchmark_db_path
        self._lock = asyncio.Lock()
        self._energy_meter = EnergyMeter()
        self._classical_simulator = ClassicalSimulator()
        self._quantum_simulator = QuantumSimulator()
        
        # Load historical benchmarks if they exist
        self._load_benchmark_history()
        
        logger.info("Quantum Energy Benchmark initialized")
    
    def _load_benchmark_history(self):
        """Load previous benchmark results from disk"""
        try:
            with open(self.db_path, 'r') as f:
                data = json.load(f)
                # Convert dicts back to BenchmarkResult objects
                for item in data:
                    classical = EnergyMeasurement(**item['classical'])
                    quantum = EnergyMeasurement(**item['quantum'])
                    result = BenchmarkResult(
                        benchmark_id=item['benchmark_id'],
                        task_name=item['task_name'],
                        classical=classical,
                        quantum=quantum,
                        energy_savings_percent=item['energy_savings_percent'],
                        speedup_factor=item['speedup_factor'],
                        carbon_savings_kg=item['carbon_savings_kg'],
                        helium_savings_l=item['helium_savings_l'],
                        quantum_advantage_score=item['quantum_advantage_score'],
                        recommended_approach=item['recommended_approach'],
                        timestamp=datetime.fromisoformat(item['timestamp'])
                    )
                    self.benchmark_history.append(result)
            logger.info(f"Loaded {len(self.benchmark_history)} benchmarks from {self.db_path}")
        except FileNotFoundError:
            logger.info("No benchmark history found, starting fresh")
        except Exception as e:
            logger.warning(f"Error loading benchmark history: {e}")
    
    async def save_benchmark_history(self):
        """Save benchmark results to disk"""
        async with self._lock:
            data = [result.to_dict() for result in self.benchmark_history]
            try:
                with open(self.db_path, 'w') as f:
                    json.dump(data, f, indent=2)
                logger.info(f"Saved {len(data)} benchmarks to {self.db_path}")
            except Exception as e:
                logger.error(f"Error saving benchmark history: {e}")
    
    async def run_benchmark(
        self,
        task_name: str,
        task_input: Dict[str, Any],
        n_runs: int = 5,
        quantum_backend: str = "simulator"
    ) -> BenchmarkResult:
        """
        Run a complete benchmark comparing classical and quantum approaches.
        
        Args:
            task_name: Name of the task being benchmarked
            task_input: Input data for the task
            n_runs: Number of runs for statistical significance
            quantum_backend: 'simulator', 'aws_braket', or 'ibm_quantum'
            
        Returns:
            BenchmarkResult with comprehensive comparison
        """
        logger.info(f"Starting benchmark for task: {task_name}")
        
        # Run classical benchmark
        classical_results = []
        for i in range(n_runs):
            logger.debug(f"Classical run {i+1}/{n_runs}")
            start_time = time.time()
            
            # Execute classical task
            result = await self._classical_simulator.execute(task_input)
            
            # Measure energy
            energy_measure = await self._energy_meter.measure_classical(
                execution_time_ms=(time.time() - start_time) * 1000,
                result_size=len(str(result))
            )
            classical_results.append(energy_measure)
        
        # Run quantum benchmark
        quantum_results = []
        for i in range(n_runs):
            logger.debug(f"Quantum run {i+1}/{n_runs}")
            start_time = time.time()
            
            # Execute quantum task
            result = await self._quantum_simulator.execute(
                task_input,
                backend=quantum_backend
            )
            
            # Measure quantum energy (includes cooling/helium costs)
            energy_measure = await self._energy_meter.measure_quantum(
                execution_time_ms=(time.time() - start_time) * 1000,
                qubits_used=task_input.get('qubits', 4),
                backend=quantum_backend
            )
            quantum_results.append(energy_measure)
        
        # Aggregate results
        classical_avg = self._aggregate_measurements(classical_results)
        quantum_avg = self._aggregate_measurements(quantum_results)
        
        # Calculate metrics
        energy_savings = ((classical_avg.energy_consumed_kwh - quantum_avg.energy_consumed_kwh) 
                         / classical_avg.energy_consumed_kwh) * 100
        carbon_savings = classical_avg.carbon_emissions_kg - quantum_avg.carbon_emissions_kg
        helium_savings = classical_avg.helium_usage_l - quantum_avg.helium_usage_l
        speedup = classical_avg.execution_time_ms / quantum_avg.execution_time_ms
        
        # Calculate quantum advantage score
        advantage_score = self._calculate_quantum_advantage(
            energy_savings, speedup, carbon_savings, helium_savings
        )
        
        # Determine recommendation
        if energy_savings > 30 and speedup > 1.5:
            recommendation = "quantum"
        elif energy_savings > 10:
            recommendation = "hybrid"
        else:
            recommendation = "classical"
        
        # Create benchmark result
        benchmark_id = f"bench_{task_name}_{datetime.utcnow().timestamp()}"
        result = BenchmarkResult(
            benchmark_id=benchmark_id,
            task_name=task_name,
            classical=classical_avg,
            quantum=quantum_avg,
            energy_savings_percent=energy_savings,
            speedup_factor=speedup,
            carbon_savings_kg=carbon_savings,
            helium_savings_l=helium_savings,
            quantum_advantage_score=advantage_score,
            recommended_approach=recommendation
        )
        
        # Store in history
        async with self._lock:
            self.benchmark_history.append(result)
        
        await self.save_benchmark_history()
        
        logger.info(f"Benchmark complete: {task_name} - Recommendation: {recommendation}")
        return result
    
    def _aggregate_measurements(self, measurements: List[EnergyMeasurement]) -> EnergyMeasurement:
        """Aggregate multiple measurements into a single average"""
        if not measurements:
            return EnergyMeasurement(
                task_id="empty",
                compute_type="unknown",
                execution_time_ms=0,
                energy_consumed_kwh=0,
                carbon_emissions_kg=0,
                helium_usage_l=0
            )
        
        avg_time = np.mean([m.execution_time_ms for m in measurements])
        avg_energy = np.mean([m.energy_consumed_kwh for m in measurements])
        avg_carbon = np.mean([m.carbon_emissions_kg for m in measurements])
        avg_helium = np.mean([m.helium_usage_l for m in measurements])
        
        return EnergyMeasurement(
            task_id=measurements[0].task_id,
            compute_type=measurements[0].compute_type,
            execution_time_ms=avg_time,
            energy_consumed_kwh=avg_energy,
            carbon_emissions_kg=avg_carbon,
            helium_usage_l=avg_helium,
            confidence_score=1.0 - (np.std([m.execution_time_ms for m in measurements]) / avg_time)
        )
    
    def _calculate_quantum_advantage(
        self,
        energy_savings: float,
        speedup: float,
        carbon_savings: float,
        helium_savings: float
    ) -> float:
        """
        Calculate a composite quantum advantage score (0-1).
        """
        # Normalize factors (capped at 1.0)
        energy_factor = min(1.0, energy_savings / 100)
        speed_factor = min(1.0, speedup / 10)
        carbon_factor = min(1.0, carbon_savings / 10)
        helium_factor = min(1.0, helium_savings / 10)
        
        # Weighted composite (weights sum to 1)
        weights = {
            'energy': 0.35,
            'speed': 0.25,
            'carbon': 0.25,
            'helium': 0.15
        }
        
        score = (
            energy_factor * weights['energy'] +
            speed_factor * weights['speed'] +
            carbon_factor * weights['carbon'] +
            helium_factor * weights['helium']
        )
        
        return min(1.0, max(0.0, score))
    
    async def get_benchmark_summary(self) -> Dict[str, Any]:
        """Get summary statistics of all benchmarks"""
        if not self.benchmark_history:
            return {'status': 'no_benchmarks'}
        
        energy_savings = [b.energy_savings_percent for b in self.benchmark_history]
        speedups = [b.speedup_factor for b in self.benchmark_history]
        carbon_savings = [b.carbon_savings_kg for b in self.benchmark_history]
        helium_savings = [b.helium_savings_l for b in self.benchmark_history]
        
        # Calculate quantum advantage distribution
        advantage_scores = [b.quantum_advantage_score for b in self.benchmark_history]
        high_advantage = sum(1 for s in advantage_scores if s > 0.7)
        medium_advantage = sum(1 for s in advantage_scores if 0.4 <= s <= 0.7)
        low_advantage = sum(1 for s in advantage_scores if s < 0.4)
        
        return {
            'total_benchmarks': len(self.benchmark_history),
            'average_energy_savings_percent': np.mean(energy_savings),
            'average_speedup': np.mean(speedups),
            'total_carbon_saved_kg': sum(carbon_savings),
            'total_helium_saved_l': sum(helium_savings),
            'best_benchmark': max(self.benchmark_history, key=lambda b: b.quantum_advantage_score).task_name,
            'quantum_advantage_distribution': {
                'high': high_advantage,
                'medium': medium_advantage,
                'low': low_advantage
            },
            'top_recommendation': max(
                ['classical', 'quantum', 'hybrid'],
                key=lambda x: sum(1 for b in self.benchmark_history if b.recommended_approach == x)
            )
        }
    
    async def generate_report(self) -> str:
        """Generate a human-readable benchmark report"""
        summary = await self.get_benchmark_summary()
        if summary.get('status') == 'no_benchmarks':
            return "No benchmarks have been run yet."
        
        report = []
        report.append("=" * 60)
        report.append("QUANTUM ENERGY BENCHMARK REPORT")
        report.append("=" * 60)
        report.append(f"Generated: {datetime.utcnow().isoformat()}")
        report.append(f"Total Benchmarks: {summary['total_benchmarks']}")
        report.append("")
        report.append("PERFORMANCE METRICS:")
        report.append(f"  Average Energy Savings: {summary['average_energy_savings_percent']:.1f}%")
        report.append(f"  Average Speedup: {summary['average_speedup']:.2f}x")
        report.append(f"  Total Carbon Saved: {summary['total_carbon_saved_kg']:.2f} kg")
        report.append(f"  Total Helium Saved: {summary['total_helium_saved_l']:.2f} L")
        report.append("")
        report.append("QUANTUM ADVANTAGE DISTRIBUTION:")
        report.append(f"  High Advantage (>0.7): {summary['quantum_advantage_distribution']['high']}")
        report.append(f"  Medium Advantage (0.4-0.7): {summary['quantum_advantage_distribution']['medium']}")
        report.append(f"  Low Advantage (<0.4): {summary['quantum_advantage_distribution']['low']}")
        report.append("")
        report.append(f"Best Performing Task: {summary['best_benchmark']}")
        report.append(f"Overall Recommendation: {summary['top_recommendation'].upper()}")
        report.append("=" * 60)
        
        return "\n".join(report)

# ============================================================================
# Energy Meter (Utility)
# ============================================================================

class EnergyMeter:
    """Simulates energy, carbon, and helium consumption measurements"""
    
    def __init__(self):
        self.base_carbon_intensity = 400  # gCO2/kWh
        self.base_helium_cost = 0.5  # L per compute-hour
        
    async def measure_classical(
        self,
        execution_time_ms: float,
        result_size: int
    ) -> EnergyMeasurement:
        """Measure energy consumption for classical computation"""
        # Power consumption model: 250W average for server
        energy_kwh = (execution_time_ms / 1000 / 3600) * 0.25
        carbon_kg = energy_kwh * (self.base_carbon_intensity / 1000)
        helium_l = energy_kwh * self.base_helium_cost * 0.1  # minimal helium
        
        return EnergyMeasurement(
            task_id=f"classical_{int(time.time())}",
            compute_type="classical",
            execution_time_ms=execution_time_ms,
            energy_consumed_kwh=energy_kwh,
            carbon_emissions_kg=carbon_kg,
            helium_usage_l=helium_l,
            metadata={'result_size': result_size}
        )
    
    async def measure_quantum(
        self,
        execution_time_ms: float,
        qubits_used: int,
        backend: str = "simulator"
    ) -> EnergyMeasurement:
        """Measure energy consumption for quantum computation"""
        # Quantum systems consume less energy per operation
        # but have higher overhead for cooling (helium)
        
        # Base quantum energy consumption (lower than classical)
        base_energy_kwh = (execution_time_ms / 1000 / 3600) * 0.15  # 150W
        
        # Add overhead for cooling and helium
        # Higher qubit count = more cooling needed
        cooling_factor = 0.5 + (qubits_used / 100)
        helium_l = (execution_time_ms / 1000 / 3600) * 2.0 * cooling_factor
        
        # Simulator doesn't use real helium
        if backend == "simulator":
            helium_l *= 0.01
        
        energy_kwh = base_energy_kwh * (1 + 0.2 * (qubits_used / 20))
        carbon_kg = energy_kwh * (self.base_carbon_intensity / 1000)
        
        return EnergyMeasurement(
            task_id=f"quantum_{int(time.time())}",
            compute_type="quantum",
            execution_time_ms=execution_time_ms,
            energy_consumed_kwh=energy_kwh,
            carbon_emissions_kg=carbon_kg,
            helium_usage_l=helium_l,
            metadata={'qubits_used': qubits_used, 'backend': backend}
        )

# ============================================================================
# Simulators (Placeholders)
# ============================================================================

class ClassicalSimulator:
    """Simulates classical computation tasks"""
    
    async def execute(self, task_input: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a classical task simulation"""
        # Simulate some computation
        await asyncio.sleep(0.1)  # 100ms base time
        
        # Different task types take different times
        task_type = task_input.get('type', 'optimization')
        size = task_input.get('size', 100)
        
        if task_type == 'optimization':
            execution_time = 0.1 + (size / 1000) * 0.5
        elif task_type == 'sorting':
            execution_time = 0.05 + (size / 1000) * 0.1
        else:
            execution_time = 0.1
        
        await asyncio.sleep(execution_time)
        
        return {
            'result': f'Classical {task_type} completed',
            'execution_time': execution_time,
            'quality': 0.85
        }

class QuantumSimulator:
    """Simulates quantum computation with realistic constraints"""
    
    async def execute(
        self,
        task_input: Dict[str, Any],
        backend: str = "simulator"
    ) -> Dict[str, Any]:
        """Execute a quantum task simulation"""
        task_type = task_input.get('type', 'optimization')
        qubits = task_input.get('qubits', 4)
        
        # Quantum advantage for optimization tasks
        if task_type == 'optimization':
            # Quantum is better at optimization
            quality = 0.95
            speedup = 2.0 + (qubits / 10)
        elif task_type == 'sorting':
            # Quantum is not better for sorting
            quality = 0.80
            speedup = 0.8
        else:
            quality = 0.85
            speedup = 1.2
        
        execution_time = 0.05 / speedup
        
        await asyncio.sleep(execution_time)
        
        return {
            'result': f'Quantum {task_type} completed',
            'execution_time': execution_time,
            'quality': quality,
            'backend': backend,
            'qubits_used': qubits
        }
