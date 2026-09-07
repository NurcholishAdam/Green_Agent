# src/enhancements/quantum_teacher.py
"""
Quantum Teacher for Multi‑Teacher Distillation.

Integrates a variational quantum circuit (VQC) as a teacher in the
distillation ensemble. The circuit encodes the state vector and outputs
a probability distribution over the available actions.

Usage:
    from src.enhancements.quantum_teacher import QuantumTeacher
    teacher = QuantumTeacher(n_actions=5, n_qubits=4, n_layers=2)
    probs = teacher.predict(state)   # state is NodeState or WorkloadState
"""

import numpy as np
import pennylane as qml
from typing import Any, Optional

class QuantumTeacher:
    """Quantum teacher that produces action probabilities from a state."""
    def __init__(self, n_actions: int = 5, n_qubits: int = 4, n_layers: int = 2,
                 backend: str = "default.qubit"):
        self.n_actions = n_actions
        self.n_qubits = n_qubits
        self.n_layers = n_layers
        self.dev = qml.device(backend, wires=n_qubits)
        # Parameters for variational circuit
        self.params = np.random.normal(0, 0.1, (n_layers, n_qubits, 3))
        self.qnode = self._build_qnode()

    def _build_qnode(self):
        @qml.qnode(self.dev, interface='autograd')
        def circuit(state_vec, params):
            # Encode state features into rotation angles
            for i in range(min(len(state_vec), self.n_qubits)):
                qml.RY(state_vec[i], wires=i)
                qml.RZ(state_vec[i], wires=i)
            # Variational layers
            for layer in range(self.n_layers):
                for i in range(self.n_qubits):
                    qml.Rot(params[layer, i, 0],
                            params[layer, i, 1],
                            params[layer, i, 2], wires=i)
                # Entangling gates
                for i in range(self.n_qubits - 1):
                    qml.CNOT(wires=[i, i + 1])
            # Return probabilities over first n_actions qubits
            if self.n_actions > self.n_qubits:
                # If more actions than qubits, return expvals and map to softmax later
                expvals = [qml.expval(qml.PauliZ(i)) for i in range(self.n_qubits)]
                return expvals
            else:
                # Use probability distribution over computational basis states of first n_actions qubits
                return qml.probs(wires=range(self.n_actions))
        return circuit

    def predict(self, state) -> np.ndarray:
        """Return action probability distribution."""
        # Convert state to feature vector
        if hasattr(state, 'to_feature_vector'):
            state_vec = state.to_feature_vector()
        else:
            state_vec = state  # assume already array
        # Pad/truncate to n_qubits
        if len(state_vec) < self.n_qubits:
            state_vec = np.pad(state_vec, (0, self.n_qubits - len(state_vec)))
        else:
            state_vec = state_vec[:self.n_qubits]

        result = self.qnode(state_vec, self.params)
        # Handle different output types
        if isinstance(result, (list, tuple)):
            # Convert expvals to probabilities via softmax
            expvals = np.array(result)
            # Map to n_actions by softmax over all expvals; then take first n_actions
            logits = np.tanh(expvals)  # squash to [-1,1]
            probs = np.exp(logits) / np.sum(np.exp(logits))
            if len(probs) >= self.n_actions:
                probs = probs[:self.n_actions]
            else:
                probs = np.pad(probs, (0, self.n_actions - len(probs)), 'constant', constant_values=1e-6)
        else:
            # Already probabilities (n_actions dimension)
            probs = np.array(result)
        # Normalize
        probs = probs / np.sum(probs)
        return probs

    def confidence(self, state) -> float:
        """Return a fixed confidence (can be improved with variance)."""
        return 0.6
