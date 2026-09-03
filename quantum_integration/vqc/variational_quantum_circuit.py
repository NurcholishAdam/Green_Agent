# File: quantum_integration/vqc/variational_quantum_circuit.py

import pennylane as qml
from pennylane import numpy as np
import numpy as np
from typing import Dict, Optional, Tuple, List, Any
from dataclasses import dataclass, field
import random
from abc import ABC, abstractmethod

@dataclass
class VQCConfig:
    """Configuration for Variational Quantum Circuit with enhancements."""
    n_qubits: int = 4
    n_layers: int = 3
    encoding_type: str = 'angle'          # angle, amplitude, qaoa
    ansatz_type: str = 'strongly_entangling'  # strongly_entangling, real_amplitudes
    measurement_type: str = 'expectation' # expectation, probability
    device: str = 'default.qubit'

    # --- Enhanced parameters ---
    # Multi-Objective Decision Process (MODP)
    n_objectives: int = 3                 # e.g., carbon, latency, cost
    objective_weights: Optional[List[float]] = None  # default equal weights

    # Reinforcement Learning from Human Feedback (RLHF)
    use_rlhf: bool = False
    human_feedback_dim: int = 1           # dimension of human feedback vector
    rlhf_scale: float = 0.1               # how much feedback shifts output

    # LIMIT Graph integration
    graph_input_dim: int = 0              # if >0, graph embedding is appended to input

    # Bio-inspired (Evolutionary) optimisation
    use_evolutionary: bool = False
    population_size: int = 10
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2

    # Mixture-of-Experts (MoE)
    use_moe_gating: bool = False
    n_experts: int = 2                    # number of VQC experts
    gating_lr: float = 0.01               # learning rate for classical gating network


class VariationalQuantumCircuit:
    """
    Variational Quantum Circuit for Green_Agent with enhancements.
    
    Supports:
    - Multiple encoding strategies
    - Trainable variational ansatz
    - Carbon-aware execution (MODP)
    - RLHF integration via human feedback input
    - LIMIT Graph embedding input
    - Evolutionary optimisation of parameters
    - MoE gating wrapper (handled in separate class)
    """

    def __init__(self, config: VQCConfig):
        self.config = config
        self.n_qubits = config.n_qubits
        self.n_layers = config.n_layers

        # Initialize quantum device
        self.dev = qml.device(config.device, wires=config.n_qubits)

        # Create circuit components
        self.encoding_fn = self._create_encoding(config.encoding_type)
        self.ansatz_fn = self._create_ansatz(config.ansatz_type)
        self.measurement_fn = self._create_measurement(config.measurement_type)

        # Initialize parameters
        self.params = self._initialize_parameters()

        # Create QNode
        self.qnode = self._create_qnode()

        # For evolutionary optimisation
        self.population = None  # will be set if use_evolutionary is True
        if config.use_evolutionary:
            self._initialize_population()

        # Objective weights (MODP)
        if config.objective_weights is None:
            self.objective_weights = np.ones(config.n_objectives) / config.n_objectives
        else:
            self.objective_weights = np.array(config.objective_weights)
            self.objective_weights /= self.objective_weights.sum()

    def _create_encoding(self, encoding_type: str):
        """Create feature encoding function."""
        if encoding_type == 'angle':
            def angle_encoding(x):
                # Ensure x length matches n_qubits (or pad)
                x_padded = np.zeros(self.n_qubits)
                x_padded[:min(len(x), self.n_qubits)] = x[:min(len(x), self.n_qubits)]
                for i in range(self.n_qubits):
                    qml.RY(x_padded[i], wires=i)
                    qml.RZ(x_padded[i], wires=i)
            return angle_encoding

        elif encoding_type == 'amplitude':
            def amplitude_encoding(x):
                # x may include graph features; use all features for amplitude encoding
                x_norm = x / np.linalg.norm(x)
                qml.AmplitudeEmbedding(
                    features=x_norm,
                    wires=range(self.n_qubits),
                    normalize=True,
                    pad_with=0
                )
            return amplitude_encoding

        else:
            raise ValueError(f"Unknown encoding type: {encoding_type}")

    def _create_ansatz(self, ansatz_type: str):
        """Create variational ansatz."""
        if ansatz_type == 'strongly_entangling':
            def strongly_entangling(params):
                for layer in range(self.n_layers):
                    for i in range(self.n_qubits):
                        qml.Rot(
                            params[layer, i, 0],
                            params[layer, i, 1],
                            params[layer, i, 2],
                            wires=i
                        )
                    for i in range(self.n_qubits - 1):
                        qml.CNOT(wires=[i, i + 1])
            return strongly_entangling

        elif ansatz_type == 'real_amplitudes':
            def real_amplitudes(params):
                for layer in range(self.n_layers):
                    for i in range(self.n_qubits):
                        qml.RY(params[layer, i], wires=i)
                    for i in range(self.n_qubits - 1):
                        qml.CNOT(wires=[i, i + 1])
            return real_amplitudes

        else:
            raise ValueError(f"Unknown ansatz type: {ansatz_type}")

    def _create_measurement(self, measurement_type: str):
        """Create measurement function."""
        if measurement_type == 'expectation':
            def expectation_measurement():
                return [qml.expval(qml.PauliZ(i)) for i in range(self.n_qubits)]
            return expectation_measurement

        elif measurement_type == 'probability':
            def probability_measurement():
                return qml.probs(wires=range(self.n_qubits))
            return probability_measurement

        else:
            raise ValueError(f"Unknown measurement type: {measurement_type}")

    def _initialize_parameters(self):
        """Initialize trainable parameters."""
        if self.config.ansatz_type == 'strongly_entangling':
            shape = (self.n_layers, self.n_qubits, 3)
        else:
            shape = (self.n_layers, self.n_qubits)
        std_dev = np.sqrt(2.0 / (self.n_qubits + self.n_layers))
        return np.random.normal(0, std_dev, size=shape)

    def _create_qnode(self):
        """Create PennyLane QNode."""
        @qml.qnode(self.dev, interface='autograd', diff_method='parameter-shift')
        def circuit(x, params):
            self.encoding_fn(x)
            self.ansatz_fn(params)
            return self.measurement_fn()
        return circuit

    def _initialize_population(self):
        """Initialize population for evolutionary optimisation."""
        self.population = []
        for _ in range(self.config.population_size):
            self.population.append(self._initialize_parameters())
        # Keep elitism best
        self.population_fitness = np.zeros(self.config.population_size)
        self.best_params = self.population[0]
        self.best_fitness = 0.0

    def forward(self, x: np.ndarray, params: Optional[np.ndarray] = None) -> np.ndarray:
        """Forward pass through VQC with optional parameters."""
        if params is None:
            params = self.params
        if len(x.shape) == 1:
            # Single sample
            x = self._preprocess_input(x)
            return self.qnode(x, params)
        else:
            # Batch
            results = []
            for xi in x:
                xi_pre = self._preprocess_input(xi)
                results.append(self.qnode(xi_pre, params))
            return np.array(results)

    def _preprocess_input(self, x: np.ndarray) -> np.ndarray:
        """
        Preprocess input by padding/truncating to expected length.
        If graph_input_dim > 0, we expect x to already include graph features;
        no additional change needed here because encoding functions handle it.
        """
        expected_dim = self.n_qubits
        if self.config.graph_input_dim > 0:
            expected_dim += self.config.graph_input_dim
        # Pad if necessary
        if len(x) < expected_dim:
            x = np.pad(x, (0, expected_dim - len(x)), 'constant')
        else:
            x = x[:expected_dim]
        return x

    def predict(self, x: np.ndarray, human_feedback: Optional[np.ndarray] = None,
                graph_embedding: Optional[np.ndarray] = None,
                params: Optional[np.ndarray] = None) -> np.ndarray:
        """
        Make prediction with post-processing and RLHF shift.
        Args:
            x: base input features (e.g., state vector)
            human_feedback: optional human feedback vector
            graph_embedding: optional graph embedding
            params: optional parameters (for evolutionary use)
        """
        # Combine inputs
        combined = x
        if graph_embedding is not None:
            combined = np.concatenate([combined, graph_embedding])
        if human_feedback is not None:
            combined = np.concatenate([combined, human_feedback])

        output = self.forward(combined, params)

        if self.config.measurement_type == 'expectation':
            # Map from [-1, 1] to [0, 1]
            probability = (np.array(output) + 1) / 2
            return probability
        else:
            return output

    def get_gradients(self, x: np.ndarray) -> np.ndarray:
        """Calculate gradients using parameter-shift rule."""
        x_processed = self._preprocess_input(x)
        return qml.grad(self.qnode, argnum=1)(x_processed, self.params)

    def calculate_efficiency(self, objectives: Dict[str, float]) -> float:
        """
        Calculate multi-objective efficiency metric (E_eff) using MODP weights.
        objectives: dict with keys like 'accuracy', 'energy', 'carbon', 'latency', 'cost'
        """
        # Expected keys in order matching self.objective_weights
        # We'll assume objectives are given as a dict; map to numeric values
        values = []
        for key in ['quality', 'energy', 'carbon', 'latency', 'cost'][:self.config.n_objectives]:
            values.append(objectives.get(key, 0.0))
        values = np.array(values)
        # Normalize for efficiency: higher quality is better, lower others better
        norm_values = np.clip(values, 0, 1)
        # For energy, carbon, latency, cost: lower is better -> invert
        if len(norm_values) >= 2:
            norm_values[1:] = 1 - norm_values[1:]
        E_eff = np.dot(norm_values, self.objective_weights)
        return float(E_eff)

    # --------------------------------------------------------------------------
    # Evolutionary optimisation methods
    # --------------------------------------------------------------------------
    def mutate(self, params: np.ndarray) -> np.ndarray:
        """Apply Gaussian mutation."""
        mutated = params.copy()
        noise = np.random.normal(0, self.config.mutation_rate, size=params.shape)
        return mutated + noise

    def crossover(self, p1: np.ndarray, p2: np.ndarray) -> np.ndarray:
        """Apply uniform crossover."""
        mask = np.random.random(p1.shape) < 0.5
        child = np.where(mask, p1, p2)
        return child

    def evolve_step(self, reward: float):
        """
        Perform one generation of evolutionary update.
        reward: fitness of current best individual (used to update population).
        """
        if not self.config.use_evolutionary:
            return
        # Assign fitness to best individual
        self.population_fitness[0] = reward
        best_idx = np.argmax(self.population_fitness)
        self.best_params = self.population[best_idx]
        self.best_fitness = self.population_fitness[best_idx]
        # Create new population
        new_population = [self.best_params]
        sorted_indices = np.argsort(self.population_fitness)[::-1]
        # Elitism
        for i in range(1, self.config.elitism):
            if i < len(sorted_indices):
                new_population.append(self.population[sorted_indices[i]])
        # Fill rest with crossover/mutation
        while len(new_population) < self.config.population_size:
            p1 = self.population[random.randint(0, self.config.population_size-1)]
            p2 = self.population[random.randint(0, self.config.population_size-1)]
            if random.random() < self.config.crossover_rate:
                child = self.crossover(p1, p2)
            else:
                child = p1.copy()
            child = self.mutate(child)
            new_population.append(child)
        self.population = new_population
        self.population_fitness = np.zeros(self.config.population_size)
        # Set current best as main params
        self.params = self.best_params

    def update_from_feedback(self, human_feedback: np.ndarray):
        """
        Simple RLHF update: shift output weights based on feedback.
        Not a full gradient update, but can be used as bias.
        """
        # Placeholder; in practice, one would compute a gradient step.
        pass


class QuantumDistillationStudent:
    """
    Wraps a VQC as a student model for multi-teacher distillation with RL.
    """
    def __init__(self, vqc: VariationalQuantumCircuit, n_actions: int = 3,
                 learning_rate: float = 0.01):
        self.vqc = vqc
        self.n_actions = n_actions
        self.lr = learning_rate
        self.counter = 0

    def predict_proba(self, x: np.ndarray, **kwargs) -> np.ndarray:
        """Return probability distribution over actions."""
        raw = self.vqc.predict(x, **kwargs)
        # Ensure we have n_actions probabilities; if raw is longer, take first n_actions
        if len(raw) > self.n_actions:
            raw = raw[:self.n_actions]
        elif len(raw) < self.n_actions:
            raw = np.pad(raw, (0, self.n_actions - len(raw)), 'constant', constant_values=0.5)
        # Normalize
        return raw / raw.sum()

    def update(self, x: np.ndarray, teacher_probs: np.ndarray, reward: float,
               action: int, distill_weight: float = 0.7, rl_weight: float = 0.3):
        """
        Update VQC parameters using a simple gradient approximation.
        In a full implementation, we would compute gradients of VQC output w.r.t params.
        Here we use a random perturbation method (evolutionary style) as a placeholder.
        """
        current_probs = self.predict_proba(x)
        # Compute target distribution
        target = distill_weight * teacher_probs + rl_weight * np.eye(self.n_actions)[action] * reward
        target /= target.sum()
        # Update VQC params via a small random search
        for _ in range(5):  # few iterations
            grad_approx = np.random.normal(0, 0.1, size=self.vqc.params.shape)
            new_params = self.vqc.params + self.lr * grad_approx
            new_probs = self.predict_proba(x, params=new_params)
            # Compute loss (MSE)
            loss_old = np.mean((current_probs - target) ** 2)
            loss_new = np.mean((new_probs - target) ** 2)
            if loss_new < loss_old:
                self.vqc.params = new_params
                break
        self.counter += 1


class QuantumMoE:
    """
    Mixture-of-Experts using multiple VQCs and a classical gating network.
    """
    def __init__(self, experts: List[VariationalQuantumCircuit], feature_dim: int,
                 n_actions: int = 3, gating_lr: float = 0.01):
        self.experts = experts
        self.n_experts = len(experts)
        self.feature_dim = feature_dim
        self.n_actions = n_actions
        self.gating_lr = gating_lr
        # Classical gating network (simple linear)
        self.gate_weights = np.random.randn(feature_dim, self.n_experts) * 0.01
        self.gate_bias = np.zeros(self.n_experts)

    def forward(self, x: np.ndarray, **kwargs) -> np.ndarray:
        """Compute weighted combination of expert outputs."""
        # Compute gate probabilities
        logits = x @ self.gate_weights + self.gate_bias
        gate_probs = np.exp(logits - np.max(logits))
        gate_probs /= gate_probs.sum()
        # Get expert outputs
        outputs = []
        for expert in self.experts:
            out = expert.predict(x, **kwargs)
            # Ensure output length matches n_actions
            if len(out) > self.n_actions:
                out = out[:self.n_actions]
            elif len(out) < self.n_actions:
                out = np.pad(out, (0, self.n_actions - len(out)), 'constant', constant_values=0.5)
            outputs.append(out)
        outputs = np.array(outputs)  # (n_experts, n_actions)
        combined = np.sum(gate_probs[:, None] * outputs, axis=0)
        return combined / combined.sum()

    def update_gating(self, x: np.ndarray, expert_outputs: np.ndarray, target: np.ndarray):
        """Update gating network to reduce MSE between combined output and target."""
        logits = x @ self.gate_weights + self.gate_bias
        gate_probs = np.exp(logits - np.max(logits))
        gate_probs /= gate_probs.sum()
        combined = np.sum(gate_probs[:, None] * expert_outputs, axis=0)
        error = combined - target
        grad_gate = np.dot(expert_outputs, error)  # (n_experts,)
        self.gate_weights -= self.gating_lr * np.outer(x, grad_gate)
        self.gate_bias -= self.gating_lr * grad_gate


def create_vqc(
    n_qubits: int = 4,
    n_layers: int = 3,
    encoding: str = 'angle',
    ansatz: str = 'strongly_entangling',
    **kwargs
) -> VariationalQuantumCircuit:
    """Factory function to create VQC with enhanced options."""
    config = VQCConfig(
        n_qubits=n_qubits,
        n_layers=n_layers,
        encoding_type=encoding,
        ansatz_type=ansatz,
        **kwargs
    )
    return VariationalQuantumCircuit(config)
