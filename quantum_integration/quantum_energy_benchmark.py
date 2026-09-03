# quantum_integration/quantum_energy_benchmark.py
"""
Quantum Energy Benchmarking Suite v2.0.0
Measures and compares energy consumption between classical and quantum approaches
with advanced decision-making enhancements:
- LIMIT Graph integration (graph metrics influence decision state)
- Multi-Objective Decision Process (MODP) with configurable weights
- Reinforcement Learning from Human Feedback (RLHF)
- Multi-Teacher On-Policy Distillation for approach recommendation
- Bio-inspired (Evolutionary) optimisation of decision weights
- Mixture-of-Experts (MoE) gating for expert combination
"""

import asyncio
import logging
import time
import json
import random
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple, Callable
from collections import deque
import numpy as np
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

# ============================================================================
# Data Classes (original + enhanced)
# ============================================================================

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

# ============================================================================
# Enhanced State for Decision Making
# ============================================================================

@dataclass
class BenchmarkState:
    """State used by distillation for recommendation."""
    energy_savings: float      # percentage (can be negative)
    speedup: float
    carbon_savings: float
    helium_savings: float
    quantum_advantage_score: float  # pre-computed score 0-1
    human_feedback_score: float = 0.5  # 0-1 (RLHF)
    graph_centrality: float = 0.5      # from LIMIT Graph metrics
    graph_connectivity: float = 0.5
    task_complexity: float = 0.5
    recent_success_rate: float = 0.5   # historical performance
    avg_reward: float = 0.5

    def to_feature_vector(self) -> np.ndarray:
        """Convert to normalized feature vector for distillation."""
        features = [
            min(max(self.energy_savings / 100.0, -1.0), 1.0),
            min(self.speedup / 10.0, 1.0),
            min(self.carbon_savings / 10.0, 1.0),
            min(self.helium_savings / 10.0, 1.0),
            self.quantum_advantage_score,
            self.human_feedback_score,
            self.graph_centrality,
            self.graph_connectivity,
            self.task_complexity,
            self.recent_success_rate,
            self.avg_reward,
        ]
        return np.array(features, dtype=np.float32)

# ============================================================================
# Teacher and Student Components for Distillation
# ============================================================================

class Teacher(ABC):
    @abstractmethod
    def predict(self, state: BenchmarkState) -> np.ndarray:
        """Return probability distribution over 3 approaches: classical, quantum, hybrid"""
        pass

    @abstractmethod
    def confidence(self, state: BenchmarkState) -> float:
        pass

class RuleBasedTeacher(Teacher):
    """Simple rule-based recommendation based on thresholds."""
    def predict(self, state: BenchmarkState) -> np.ndarray:
        probs = np.ones(3) * 0.1
        if state.energy_savings > 30 and state.speedup > 1.5:
            probs[1] = 0.8  # quantum
        elif state.energy_savings > 10:
            probs[2] = 0.6  # hybrid
        else:
            probs[0] = 0.6  # classical
        return probs / probs.sum()

    def confidence(self, state: BenchmarkState) -> float:
        return 0.6

class RLHFTeacher(Teacher):
    """Teacher influenced by human feedback."""
    def predict(self, state: BenchmarkState) -> np.ndarray:
        probs = np.ones(3) / 3
        if state.human_feedback_score > 0.7:
            probs[1] += 0.2  # prefer quantum
        elif state.human_feedback_score < 0.3:
            probs[0] += 0.2  # prefer classical
        else:
            probs[2] += 0.1  # hybrid
        return probs / probs.sum()

    def confidence(self, state: BenchmarkState) -> float:
        return 0.7 if abs(state.human_feedback_score - 0.5) > 0.3 else 0.4

class HistoricalMLTeacher(Teacher):
    """Teacher based on historical benchmark outcomes (simulated)."""
    def __init__(self):
        # In a real implementation, this would load a trained model.
        self.model = None

    def predict(self, state: BenchmarkState) -> np.ndarray:
        # Simulate a model: if energy savings high, quantum is better
        if state.energy_savings > 20:
            return np.array([0.1, 0.7, 0.2])
        elif state.speedup > 2.0:
            return np.array([0.2, 0.6, 0.2])
        else:
            return np.array([0.5, 0.2, 0.3])

    def confidence(self, state: BenchmarkState) -> float:
        return 0.5

class StatefulQTeacher(Teacher):
    """Simple Q-learning teacher."""
    def __init__(self, lr: float = 0.1):
        self.lr = lr
        self.weights = np.zeros((11, 3))  # 11 features -> 3 actions

    def predict(self, state: BenchmarkState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: BenchmarkState) -> float:
        return 0.5

    def update(self, state: BenchmarkState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x

class DistillationStudent:
    """Linear softmax student for distillation."""
    def __init__(self, feature_dim: int = 11, n_classes: int = 3, lr: float = 0.01):
        self.feature_dim = feature_dim
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray) -> np.ndarray:
        logits = state_vector @ self.weights + self.biases
        exp_logits = np.exp(logits - np.max(logits))
        return exp_logits / exp_logits.sum()

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action: int, distill_weight: float = 0.7, rl_weight: float = 0.3):
        current_probs = self.predict_proba(state_vector)
        grad_distill = -(teacher_probs - current_probs)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current_probs)
        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1

class ReplayBuffer:
    def __init__(self, max_size: int = 2000):
        self.buffer = deque(maxlen=max_size)

    def push(self, state_vec, action, reward, next_state_vec, teacher_probs):
        self.buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))

    def sample(self, batch_size=32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (np.array(states), actions, np.array(rewards),
                np.array(next_states), np.array(teacher_probs))

    def __len__(self):
        return len(self.buffer)

class MoEGatingNetwork:
    """Classical gating network for MoE."""
    def __init__(self, feature_dim: int = 11, n_experts: int = 4, lr: float = 0.005):
        self.feature_dim = feature_dim
        self.n_experts = n_experts
        self.lr = lr
        self.weights = np.random.randn(feature_dim, n_experts) * 0.01
        self.bias = np.zeros(n_experts)

    def forward(self, state_vec: np.ndarray) -> np.ndarray:
        logits = state_vec @ self.weights + self.bias
        exp_logits = np.exp(logits - np.max(logits))
        return exp_logits / exp_logits.sum()

    def update(self, state_vec, teacher_probs, student_probs):
        gate_weights = self.forward(state_vec)
        combined = np.sum(gate_weights[:, None] * teacher_probs, axis=0)
        error = combined - student_probs
        grad_gate = np.dot(teacher_probs, error)
        self.weights -= self.lr * np.outer(state_vec, grad_gate)
        self.bias -= self.lr * grad_gate

class BenchmarkDistillationOptimizer:
    """Manages teachers, student, and MoE gating for recommendation."""
    RECOMMENDATIONS = ['classical', 'quantum', 'hybrid']

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.feature_dim = 11
        self.student = DistillationStudent(feature_dim=self.feature_dim,
                                           lr=config.get('distillation_learning_rate', 0.01))
        self.teachers = [
            RuleBasedTeacher(),
            RLHFTeacher(),
            HistoricalMLTeacher(),
            StatefulQTeacher(lr=config.get('q_learning_rate', 0.1))
        ]
        self.n_teachers = len(self.teachers)
        self.gating = MoEGatingNetwork(feature_dim=self.feature_dim,
                                       n_experts=self.n_teachers,
                                       lr=config.get('gating_learning_rate', 0.005))
        self.replay_buffer = ReplayBuffer(max_size=config.get('replay_size', 2000))
        self.epsilon = config.get('epsilon', 0.1)
        self.train_every = config.get('train_every', 10)
        self.counter = 0
        self.distill_weight = config.get('distill_weight', 0.7)
        self.rl_weight = config.get('rl_weight', 0.3)

    def _compute_teacher_probs(self, state: BenchmarkState) -> Tuple[np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        teacher_outputs = []
        for teacher in self.teachers:
            prob = teacher.predict(state)
            if len(prob) != 3:
                prob = np.pad(prob, (0, 3 - len(prob)), 'constant')
            teacher_outputs.append(prob)
        teacher_outputs = np.array(teacher_outputs)  # (n_teachers, 3)
        gate_weights = self.gating.forward(state_vec)
        combined = np.sum(gate_weights[:, None] * teacher_outputs, axis=0)
        combined = combined / combined.sum()
        return combined, gate_weights

    def select_recommendation(self, state: BenchmarkState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        teacher_probs, gate_weights = self._compute_teacher_probs(state)
        student_probs = self.student.predict_proba(state_vec)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, 2)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = np.argmax(combined)

        return self.RECOMMENDATIONS[action_idx], action_idx, state_vec, teacher_probs

    def update(self, state_vec, action_idx, reward, next_state_vec, teacher_probs):
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = self.replay_buffer.sample(8)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i],
                                    distill_weight=self.distill_weight, rl_weight=self.rl_weight)
                student_out = self.student.predict_proba(states[i])
                self.gating.update(states[i], teacher_probs_batch[i], student_out)

    def get_stats(self) -> Dict:
        return {'student_counter': self.student.counter, 'buffer_size': len(self.replay_buffer)}

# ============================================================================
# Evolutionary Optimizer for Weights
# ============================================================================

class EvolutionaryWeightOptimizer:
    """Evolves the weights used in quantum advantage score calculation."""
    def __init__(self, population_size: int = 20, mutation_rate: float = 0.1,
                 crossover_rate: float = 0.7, elitism: int = 2):
        self.population_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.elitism = elitism
        # Initialize population of weight vectors (4 objectives: energy, speed, carbon, helium)
        self.population = [np.random.dirichlet(np.ones(4)) for _ in range(population_size)]
        self.fitness = np.zeros(population_size)
        self.best_weights = self.population[0]
        self.best_fitness = 0.0

    def get_weights(self) -> np.ndarray:
        return self.best_weights

    def update_fitness(self, reward: float, index: int = 0):
        self.fitness[index] = reward
        best_idx = np.argmax(self.fitness)
        self.best_weights = self.population[best_idx]
        self.best_fitness = self.fitness[best_idx]
        self._evolve()

    def _evolve(self):
        sorted_indices = np.argsort(self.fitness)[::-1]
        new_population = [self.population[i] for i in sorted_indices[:self.elitism]]
        while len(new_population) < self.population_size:
            p1 = self.population[random.randint(0, self.population_size-1)]
            p2 = self.population[random.randint(0, self.population_size-1)]
            if random.random() < self.crossover_rate:
                alpha = random.random()
                child = alpha * p1 + (1 - alpha) * p2
            else:
                child = p1.copy()
            if random.random() < self.mutation_rate:
                child += np.random.dirichlet(np.ones(4)) * 0.1
            child = child / child.sum()
            new_population.append(child)
        self.population = new_population
        self.fitness = np.zeros(self.population_size)

# ============================================================================
# Main Benchmark Class with Enhancements
# ============================================================================

class QuantumEnergyBenchmark:
    """
    Benchmark suite for measuring quantum vs classical energy efficiency.
    Now includes distillation-based recommendation, MODP, RLHF, MoE, evolutionary weights, and LIMIT Graph.
    """
    
    def __init__(self, benchmark_db_path: str = "benchmark_history.json",
                 use_enhanced: bool = False,
                 config: Optional[Dict[str, Any]] = None):
        self.benchmark_history: List[BenchmarkResult] = []
        self.db_path = benchmark_db_path
        self._lock = asyncio.Lock()
        self._energy_meter = EnergyMeter()
        self._classical_simulator = ClassicalSimulator()
        self._quantum_simulator = QuantumSimulator()
        
        self.use_enhanced = use_enhanced
        self.config = config or {}
        
        # Enhanced components
        if self.use_enhanced:
            self.distillation_optimizer = BenchmarkDistillationOptimizer(self.config)
            self.evolutionary_weights = EvolutionaryWeightOptimizer(
                population_size=self.config.get('population_size', 20),
                mutation_rate=self.config.get('mutation_rate', 0.1),
                crossover_rate=self.config.get('crossover_rate', 0.7),
                elitism=self.config.get('elitism', 2)
            )
        else:
            self.distillation_optimizer = None
            self.evolutionary_weights = None
        
        # Load historical benchmarks if they exist
        self._load_benchmark_history()
        
        logger.info(f"Quantum Energy Benchmark initialized (enhanced={self.use_enhanced})")
    
    def _load_benchmark_history(self):
        """Load previous benchmark results from disk"""
        try:
            with open(self.db_path, 'r') as f:
                data = json.load(f)
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
        quantum_backend: str = "simulator",
        human_feedback_score: float = 0.5,
        graph_metrics: Optional[Dict[str, float]] = None
    ) -> BenchmarkResult:
        """
        Run a complete benchmark comparing classical and quantum approaches.
        
        Args:
            task_name: Name of the task being benchmarked
            task_input: Input data for the task
            n_runs: Number of runs for statistical significance
            quantum_backend: 'simulator', 'aws_braket', or 'ibm_quantum'
            human_feedback_score: RLHF input (0-1)
            graph_metrics: LIMIT Graph metrics (e.g., {'centrality': 0.6, 'connectivity': 0.7})
            
        Returns:
            BenchmarkResult with comprehensive comparison and recommendation
        """
        logger.info(f"Starting benchmark for task: {task_name}")
        
        # Run classical benchmark
        classical_results = []
        for i in range(n_runs):
            start_time = time.time()
            result = await self._classical_simulator.execute(task_input)
            energy_measure = await self._energy_meter.measure_classical(
                execution_time_ms=(time.time() - start_time) * 1000,
                result_size=len(str(result))
            )
            classical_results.append(energy_measure)
        
        # Run quantum benchmark
        quantum_results = []
        for i in range(n_runs):
            start_time = time.time()
            result = await self._quantum_simulator.execute(
                task_input,
                backend=quantum_backend
            )
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
        
        # Determine weights for quantum advantage score
        if self.evolutionary_weights:
            weights = self.evolutionary_weights.get_weights()
        else:
            weights = np.array([0.35, 0.25, 0.25, 0.15])  # default
        
        advantage_score = self._calculate_quantum_advantage(
            energy_savings, speedup, carbon_savings, helium_savings, weights
        )
        
        # Decide recommendation (either rule-based or distillation-enhanced)
        if self.use_enhanced and self.distillation_optimizer:
            # Build state
            state = BenchmarkState(
                energy_savings=energy_savings,
                speedup=speedup,
                carbon_savings=carbon_savings,
                helium_savings=helium_savings,
                quantum_advantage_score=advantage_score,
                human_feedback_score=human_feedback_score,
                graph_centrality=graph_metrics.get('centrality', 0.5) if graph_metrics else 0.5,
                graph_connectivity=graph_metrics.get('connectivity', 0.5) if graph_metrics else 0.5,
                task_complexity=task_input.get('complexity', 0.5),
                recent_success_rate=self._get_recent_success_rate(),
                avg_reward=self._get_avg_reward()
            )
            recommendation, action_idx, state_vec, teacher_probs = self.distillation_optimizer.select_recommendation(
                state, exploration=True
            )
            # Store for later update
            self._last_decision = {
                'state_vec': state_vec,
                'action_idx': action_idx,
                'teacher_probs': teacher_probs,
                'state': state
            }
        else:
            # Original rule-based recommendation
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
        
        # Update distillation if enhanced
        if self.use_enhanced and self.distillation_optimizer and hasattr(self, '_last_decision'):
            # Compute reward based on actual performance (for simplicity, use advantage score)
            reward = advantage_score
            state_vec = self._last_decision['state_vec']
            action_idx = self._last_decision['action_idx']
            teacher_probs = self._last_decision['teacher_probs']
            next_state_vec = state_vec  # no change
            self.distillation_optimizer.update(state_vec, action_idx, reward, next_state_vec, teacher_probs)
            # Update evolutionary weights
            if self.evolutionary_weights:
                self.evolutionary_weights.update_fitness(reward, index=0)
            del self._last_decision
        
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
        helium_savings: float,
        weights: Optional[np.ndarray] = None
    ) -> float:
        """Calculate a composite quantum advantage score (0-1) with configurable weights."""
        if weights is None:
            weights = np.array([0.35, 0.25, 0.25, 0.15])
        # Normalize factors (capped at 1.0)
        energy_factor = min(1.0, max(0.0, energy_savings / 100))
        speed_factor = min(1.0, speedup / 10)
        carbon_factor = min(1.0, carbon_savings / 10)
        helium_factor = min(1.0, helium_savings / 10)
        factors = np.array([energy_factor, speed_factor, carbon_factor, helium_factor])
        score = np.dot(factors, weights)
        return float(min(1.0, max(0.0, score)))
    
    def _get_recent_success_rate(self) -> float:
        """Get recent success rate of quantum recommendations."""
        if not self.benchmark_history:
            return 0.5
        recent = self.benchmark_history[-20:]
        if not recent:
            return 0.5
        # Consider success if quantum advantage score > 0.5
        successes = sum(1 for b in recent if b.quantum_advantage_score > 0.5)
        return successes / len(recent)
    
    def _get_avg_reward(self) -> float:
        """Get average quantum advantage score from history."""
        if not self.benchmark_history:
            return 0.5
        return np.mean([b.quantum_advantage_score for b in self.benchmark_history[-50:]])
    
    async def get_benchmark_summary(self) -> Dict[str, Any]:
        """Get summary statistics of all benchmarks"""
        if not self.benchmark_history:
            return {'status': 'no_benchmarks'}
        
        energy_savings = [b.energy_savings_percent for b in self.benchmark_history]
        speedups = [b.speedup_factor for b in self.benchmark_history]
        carbon_savings = [b.carbon_savings_kg for b in self.benchmark_history]
        helium_savings = [b.helium_savings_l for b in self.benchmark_history]
        
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
# Energy Meter (Utility) - unchanged
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
        energy_kwh = (execution_time_ms / 1000 / 3600) * 0.25
        carbon_kg = energy_kwh * (self.base_carbon_intensity / 1000)
        helium_l = energy_kwh * self.base_helium_cost * 0.1
        
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
        base_energy_kwh = (execution_time_ms / 1000 / 3600) * 0.15
        cooling_factor = 0.5 + (qubits_used / 100)
        helium_l = (execution_time_ms / 1000 / 3600) * 2.0 * cooling_factor
        
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
# Simulators (unchanged)
# ============================================================================

class ClassicalSimulator:
    """Simulates classical computation tasks"""
    
    async def execute(self, task_input: Dict[str, Any]) -> Dict[str, Any]:
        await asyncio.sleep(0.1)
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
        task_type = task_input.get('type', 'optimization')
        qubits = task_input.get('qubits', 4)
        
        if task_type == 'optimization':
            quality = 0.95
            speedup = 2.0 + (qubits / 10)
        elif task_type == 'sorting':
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
