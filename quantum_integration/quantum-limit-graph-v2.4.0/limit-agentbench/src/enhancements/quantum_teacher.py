# src/enhancements/quantum_teacher.py
"""
Quantum Teacher for Multi‑Teacher Distillation (Enhanced).

Integrates a variational quantum circuit (VQC) as a teacher in the
distillation ensemble. The circuit encodes the state vector, applies a
trainable ansatz, and outputs a probability distribution over actions.

Usage:
    teacher = QuantumTeacher(n_actions=5, n_qubits=4, n_layers=2, seed=42)
    probs = teacher.predict(state)          # single sample
    probs_batch = teacher.predict_batch(states)  # batch
    teacher.update_params(new_params)        # for training
"""

import logging
from typing import Any, List, Optional, Sequence, Union

import numpy as np
import pennylane as qml

logger = logging.getLogger(__name__)


class QuantumTeacher:
    """
    Variational quantum teacher that maps a state vector to action probabilities.

    Attributes:
        n_actions: Number of possible actions.
        n_qubits: Number of qubits used in the circuit.
        n_layers: Number of variational layers.
        backend: PennyLane device name.
        params: Trainable parameters (including final measurement weights).
    """

    def __init__(
        self,
        n_actions: int = 5,
        n_qubits: int = 4,
        n_layers: int = 2,
        backend: str = "default.qubit",
        seed: Optional[int] = None,
    ):
        """
        Initialize the quantum teacher.

        Args:
            n_actions: Number of output actions.
            n_qubits: Number of qubits.
            n_layers: Number of variational layers.
            backend: PennyLane device name (e.g., 'default.qubit', 'lightning.qubit').
            seed: Random seed for reproducibility.
        """
        if n_qubits < 1:
            raise ValueError("n_qubits must be >= 1")
        if n_actions < 2:
            raise ValueError("n_actions must be >= 2")
        if n_layers < 1:
            raise ValueError("n_layers must be >= 1")

        self.n_actions = n_actions
        self.n_qubits = n_qubits
        self.n_layers = n_layers
        self.backend = backend

        # Set random seed if provided
        if seed is not None:
            np.random.seed(seed)

        self.dev = qml.device(backend, wires=n_qubits)

        # Initialize parameters:
        # - Variational part: (n_layers, n_qubits, 3) for Rot gates
        # - Measurement weights: (n_qubits, n_actions) to map PauliZ expectations to action logits
        self.params = {
            "variational": np.random.normal(0, 0.1, (n_layers, n_qubits, 3)),
            "measurement": np.random.normal(0, 0.1, (n_qubits, n_actions)),
        }

        # Build the quantum node (QNode)
        self.qnode = self._build_qnode()
        logger.debug(f"QuantumTeacher initialized with {n_qubits} qubits, {n_layers} layers, {n_actions} actions.")

    def _build_qnode(self):
        """Construct the QNode with encoding, variational layers, and measurement."""
        dev = self.dev

        @qml.qnode(dev, interface="autograd")
        def circuit(state_vec, variational_params, measurement_weights):
            # --- Encoding: angle embedding with scaled features ---
            # Normalize state_vec to [0, 2π] or use raw angles (assume already in radians)
            # We use the raw values, but clamp to avoid extreme rotations.
            clipped = np.clip(state_vec, -np.pi, np.pi)
            for i in range(self.n_qubits):
                if i < len(clipped):
                    qml.RY(clipped[i], wires=i)
                else:
                    qml.RY(0.0, wires=i)  # pad with zero rotation

            # --- Variational layers: strongly entangling ansatz ---
            for layer in range(self.n_layers):
                for i in range(self.n_qubits):
                    qml.Rot(
                        variational_params[layer, i, 0],
                        variational_params[layer, i, 1],
                        variational_params[layer, i, 2],
                        wires=i,
                    )
                # Entangling gates (nearest-neighbour CNOTs)
                for i in range(self.n_qubits - 1):
                    qml.CNOT(wires=[i, i + 1])
                # Optional: connect last to first for cyclic entanglement
                if self.n_qubits > 2:
                    qml.CNOT(wires=[self.n_qubits - 1, 0])

            # --- Measurement: expectation values of PauliZ on each qubit ---
            expvals = [qml.expval(qml.PauliZ(i)) for i in range(self.n_qubits)]
            # Convert to tensor to allow matrix multiplication
            expvals = qml.math.stack(expvals)

            # --- Output: linear transformation to n_actions logits, then softmax ---
            logits = expvals @ measurement_weights  # shape (n_actions,)
            return qml.math.softmax(logits)

        return circuit

    def _prepare_state(self, state: Union[Any, np.ndarray]) -> np.ndarray:
        """Convert state to a numpy feature vector of length n_qubits."""
        if hasattr(state, "to_feature_vector"):
            vec = state.to_feature_vector()
        else:
            vec = state
        vec = np.asarray(vec, dtype=np.float32)
        # Pad or truncate to n_qubits
        if len(vec) < self.n_qubits:
            vec = np.pad(vec, (0, self.n_qubits - len(vec)), mode="constant")
        else:
            vec = vec[: self.n_qubits]
        return vec

    def predict(self, state: Union[Any, np.ndarray]) -> np.ndarray:
        """
        Predict action probabilities for a single state.

        Args:
            state: State object or raw feature vector.

        Returns:
            Array of length `n_actions` with probabilities summing to 1.
        """
        state_vec = self._prepare_state(state)
        probs = self.qnode(state_vec, self.params["variational"], self.params["measurement"])
        return np.asarray(probs, dtype=np.float64)

    def predict_batch(self, states: Sequence[Union[Any, np.ndarray]]) -> np.ndarray:
        """
        Predict action probabilities for a batch of states.

        Args:
            states: List of state objects or feature vectors.

        Returns:
            2D array of shape (batch_size, n_actions).
        """
        batch_vecs = np.stack([self._prepare_state(s) for s in states])
        # Use batch_input to vectorize
        batched_qnode = qml.batch_input(self.qnode, argnum=0)
        probs = batched_qnode(batch_vecs, self.params["variational"], self.params["measurement"])
        return np.asarray(probs, dtype=np.float64)

    def get_params(self) -> Dict[str, np.ndarray]:
        """Return a copy of the current parameters (useful for training)."""
        import copy
        return copy.deepcopy(self.params)

    def set_params(self, new_params: Dict[str, np.ndarray]) -> None:
        """Set the parameters (e.g., after gradient update)."""
        for key in self.params:
            if key not in new_params:
                raise KeyError(f"Missing parameter group '{key}' in new_params")
            if self.params[key].shape != new_params[key].shape:
                raise ValueError(f"Shape mismatch for '{key}'")
        self.params = {k: np.array(v, dtype=np.float64) for k, v in new_params.items()}
        logger.debug("Parameters updated.")

    def confidence(self, state: Union[Any, np.ndarray]) -> float:
        """
        Return a confidence estimate based on the entropy of the output distribution.

        Lower entropy → higher confidence. The confidence is normalized to [0,1].
        """
        probs = self.predict(state)
        entropy = -np.sum(probs * np.log(probs + 1e-12))
        max_entropy = np.log(self.n_actions)
        if max_entropy == 0:
            return 1.0
        confidence = 1.0 - (entropy / max_entropy)
        return float(np.clip(confidence, 0.0, 1.0))

    def __repr__(self) -> str:
        return f"QuantumTeacher(n_actions={self.n_actions}, n_qubits={self.n_qubits}, n_layers={self.n_layers}, backend={self.backend})"
