#!/usr/bin/env python3
"""
Quantum Distillation Integration for Green Agent MoE.
Distills simplified classical policies from quantum circuits (e.g., QAOA/VQE)
and exposes them as a teacher for the MoE router.

Enhanced version: true distillation via a lightweight student model,
robust penalty handling, explainability, temporal safety, drift detection,
human approval, and chaos testing.
"""

import asyncio
import logging
import numpy as np
from typing import Dict, Any, Optional, List, Callable, Tuple, Union
from collections import deque
import os
import json
import time

logger = logging.getLogger(__name__)

# Try to import PyTorch for student model, else fallback to numpy logistic regression
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# Attempt multiple import paths for QuantumBridge
QUANTUM_BRIDGE_AVAILABLE = False
QuantumBridge = None
try:
    from enhancements.bio_inspired.quantum_bridge import QuantumBridge
    QUANTUM_BRIDGE_AVAILABLE = True
except ImportError:
    try:
        from ..bio_inspired.quantum_bridge import QuantumBridge
        QUANTUM_BRIDGE_AVAILABLE = True
    except ImportError:
        try:
            from ...bio_inspired.quantum_bridge import QuantumBridge
            QUANTUM_BRIDGE_AVAILABLE = True
        except ImportError:
            logger.warning("QuantumBridge not available; using simulated distillation")


class ClassicalStudentModel:
    """
    Simple classical student model that learns to mimic teacher distributions.
    Can be a PyTorch MLP or a numpy logistic regression.
    """
    def __init__(self, input_dim: int, num_actions: int, use_torch: bool = TORCH_AVAILABLE):
        self.input_dim = input_dim
        self.num_actions = num_actions
        self.use_torch = use_torch
        self.trained = False

        if self.use_torch:
            class SimpleMLP(nn.Module):
                def __init__(self, in_dim, out_dim):
                    super().__init__()
                    self.net = nn.Sequential(
                        nn.Linear(in_dim, 64),
                        nn.ReLU(),
                        nn.Linear(64, 32),
                        nn.ReLU(),
                        nn.Linear(32, out_dim),
                    )
                def forward(self, x):
                    return self.net(x)
            self.model = SimpleMLP(input_dim, num_actions)
            self.optimizer = optim.Adam(self.model.parameters(), lr=0.001)
            self.loss_fn = nn.KLDivLoss(reduction='batchmean')
        else:
            # Numpy implementation: softmax regression
            self.weights = np.random.randn(input_dim, num_actions) * 0.01
            self.biases = np.zeros(num_actions)
            self.lr = 0.01

    def predict(self, x: np.ndarray) -> np.ndarray:
        """Return softmax probabilities over actions."""
        if self.use_torch:
            self.model.eval()
            with torch.no_grad():
                x_t = torch.FloatTensor(x).unsqueeze(0)
                logits = self.model(x_t)
                probs = torch.softmax(logits, dim=1).squeeze(0).numpy()
            return probs
        else:
            logits = np.dot(x, self.weights) + self.biases
            exp_logits = np.exp(logits - np.max(logits))
            return exp_logits / np.sum(exp_logits)

    def train_step(self, x: np.ndarray, target_dist: np.ndarray) -> float:
        """Single training step with one (context, target) pair."""
        if self.use_torch:
            self.model.train()
            x_t = torch.FloatTensor(x).unsqueeze(0)
            target_t = torch.FloatTensor(target_dist).unsqueeze(0)
            self.optimizer.zero_grad()
            logits = self.model(x_t)
            log_probs = torch.log_softmax(logits, dim=1)
            loss = self.loss_fn(log_probs, target_t)
            loss.backward()
            self.optimizer.step()
            return loss.item()
        else:
            # Gradient of cross-entropy with softmax
            probs = self.predict(x)
            error = probs - target_dist
            grad_w = np.outer(x, error)
            grad_b = error
            self.weights -= self.lr * grad_w
            self.biases -= self.lr * grad_b
            loss = -np.sum(target_dist * np.log(probs + 1e-8))
            return loss


class QuantumPolicyDistiller:
    """
    Distills a classical probability distribution over actions
    from quantum measurement outcomes. If a real QuantumBridge is unavailable,
    a simulated teacher is used based on context features.
    """

    def __init__(self, num_actions: int = 5, bridge: Optional[Any] = None,
                 input_dim: Optional[int] = None):
        self.num_actions = num_actions
        self.bridge = bridge if bridge else (QuantumBridge() if QUANTUM_BRIDGE_AVAILABLE else None)
        self.input_dim = input_dim if input_dim else num_actions  # default simple
        self.cache: Dict[str, np.ndarray] = {}
        self._lock = asyncio.Lock()
        self.student = ClassicalStudentModel(self.input_dim, num_actions, use_torch=TORCH_AVAILABLE)
        self.constraints: List[Callable[[Dict, int], bool]] = []
        self.approval_callback: Optional[Callable[[int, Dict], bool]] = None
        self.drift_history: deque = deque(maxlen=50)
        self.last_dist: Optional[np.ndarray] = None
        self.fault_injected = False

        # Fallback penalty defaults if bridge missing or keys absent
        self.default_penalties = {
            'penalty_carbon': 0.1,
            'penalty_helium_shortage': 0.1,
            'penalty_energy': 0.1,
            'penalty_cost': 0.1,
            'penalty_latency': 0.1,
        }

    # ------------------ Context Feature Extraction ------------------
    def _get_context_features(self, context: Dict[str, Any]) -> np.ndarray:
        """
        Convert context dict to a fixed-size vector for student input.
        Uses a simple mapping of common keys; if input_dim differs, pad/truncate.
        """
        # We'll create a vector from selected numeric features
        features = []
        for key in ['carbon_intensity', 'avg_carbon', 'helium_scarcity', 'energy_price',
                    'renewable_ratio', 'num_clients', 'trust_score', 'latency_ms']:
            if key in context:
                val = context[key]
                if isinstance(val, (int, float)):
                    features.append(float(val))
                else:
                    features.append(0.0)
            else:
                features.append(0.0)
        # Normalize a few known values
        if len(features) >= 1 and 'carbon_intensity' in context:
            features[0] = features[0] / 800.0
        elif len(features) >= 1:
            features[0] = features[0] / 800.0
        # Ensure fixed length
        if len(features) < self.input_dim:
            features += [0.0] * (self.input_dim - len(features))
        else:
            features = features[:self.input_dim]
        return np.array(features)

    # ------------------ Teacher Distribution (heuristic or bridge) ------------------
    def _get_teacher_distribution(self, context: Dict[str, Any]) -> np.ndarray:
        """
        Obtain a soft distribution from quantum bridge or simulated penalties.
        """
        if self.bridge and hasattr(self.bridge, 'get_qubo_parameters'):
            try:
                params = self.bridge.get_qubo_parameters()
                # Use penalties as energies; robust to missing keys
                energies = np.array([
                    params.get('penalty_carbon', self.default_penalties['penalty_carbon']),
                    params.get('penalty_helium_shortage', self.default_penalties['penalty_helium_shortage']),
                    params.get('penalty_energy', self.default_penalties['penalty_energy']),
                    params.get('penalty_cost', self.default_penalties['penalty_cost']),
                    params.get('penalty_latency', self.default_penalties['penalty_latency']),
                ])
                # Ensure length matches num_actions; truncate or pad
                if len(energies) > self.num_actions:
                    energies = energies[:self.num_actions]
                elif len(energies) < self.num_actions:
                    pad = np.full(self.num_actions - len(energies), 0.5)
                    energies = np.concatenate([energies, pad])
                beta = 1.0
                exp_neg = np.exp(-beta * energies)
                probs = exp_neg / np.sum(exp_neg)
                return probs
            except Exception as e:
                logger.warning(f"QuantumBridge error: {e}; using simulated penalties")

        # Simulated teacher: generate deterministic penalties based on context
        carbon = context.get('carbon_intensity', context.get('avg_carbon', 400)) / 800.0
        helium = context.get('helium_scarcity', 0.5)
        energy = context.get('energy_price', 0.5)
        # Simple linear combination
        penalties = np.array([
            max(0.1, carbon),                # action 0: carbon-aware
            max(0.1, helium),                # action 1: helium-conserving
            max(0.1, energy),                # action 2: energy-efficient
            max(0.1, 0.2),                   # action 3: cost-effective
            max(0.1, 0.1),                   # action 4: default
        ])
        if self.num_actions != 5:
            # Generalize: create dummy penalties
            penalties = np.full(self.num_actions, 0.2)
            penalties[0] = max(0.1, carbon)
            penalties[1] = max(0.1, helium)
        exp_neg = np.exp(-penalties)
        probs = exp_neg / np.sum(exp_neg)
        return probs

    # ------------------ Student Training ------------------
    async def train_from_circuit(self, circuit_results: Optional[List[Dict]] = None,
                                 contexts: Optional[List[Dict]] = None):
        """
        Train the student model using teacher distributions on a set of contexts.
        If circuit_results is None, generate synthetic contexts and labels.
        """
        async with self._lock:
            if contexts is None:
                # Generate synthetic training data
                contexts = []
                for _ in range(100):
                    contexts.append({
                        'carbon_intensity': np.random.uniform(100, 800),
                        'helium_scarcity': np.random.uniform(0.1, 0.9),
                        'energy_price': np.random.uniform(0.1, 1.0),
                        'renewable_ratio': np.random.uniform(0.1, 0.9),
                        'num_clients': np.random.randint(1, 20),
                        'trust_score': np.random.uniform(0.1, 0.9),
                        'latency_ms': np.random.uniform(10, 200),
                    })
            losses = []
            for ctx in contexts:
                x = self._get_context_features(ctx)
                target = self._get_teacher_distribution(ctx)
                loss = self.student.train_step(x, target)
                losses.append(loss)
                await asyncio.sleep(0)  # yield control
            self.student.trained = True
            logger.info(f"Student model trained on {len(contexts)} samples. Avg loss: {np.mean(losses):.4f}")
            return {'status': 'success', 'samples': len(contexts), 'avg_loss': np.mean(losses)}

    # ------------------ Main Distillation Interface ------------------
    async def get_distilled_policy(self, context: Dict[str, Any],
                                   use_cache: bool = True) -> np.ndarray:
        """
        Return a probability distribution over actions.
        Prefers trained student model; falls back to teacher heuristic.
        """
        cache_key = str(sorted(context.items()))
        async with self._lock:
            if use_cache and cache_key in self.cache:
                probs = self.cache[cache_key]
            else:
                x = self._get_context_features(context)
                if self.student.trained:
                    probs = self.student.predict(x)
                else:
                    probs = self._get_teacher_distribution(context)
                # Apply safety constraints (mask disallowed actions)
                probs = self._apply_constraints(context, probs)
                # Normalise
                if probs.sum() > 0:
                    probs = probs / probs.sum()
                else:
                    probs = np.ones(self.num_actions) / self.num_actions
                self.cache[cache_key] = probs

                # Drift detection
                if self.last_dist is not None:
                    drift = np.linalg.norm(probs - self.last_dist)
                    self.drift_history.append(drift)
                    if drift > 0.3:
                        logger.warning(f"High policy drift detected: {drift:.3f}")
                        # Optionally clear cache or retrain
                self.last_dist = probs

            return probs

    # ------------------ Safety Constraints ------------------
    def add_constraint(self, constraint_fn: Callable[[Dict, int], bool], description: str = ""):
        """
        Add a safety constraint. The function receives (context, action_index)
        and returns True if the action is disallowed.
        """
        self.constraints.append(constraint_fn)
        logger.info(f"Added quantum policy constraint: {description}")

    def _apply_constraints(self, context: Dict, probs: np.ndarray) -> np.ndarray:
        masked = probs.copy()
        for i in range(self.num_actions):
            for constraint in self.constraints:
                if constraint(context, i):
                    masked[i] = 0.0
                    break
        return masked

    # ------------------ Explainability ------------------
    async def explain_policy(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Return an explanation of the current policy distribution.
        """
        x = self._get_context_features(context)
        if self.student.trained:
            probs = self.student.predict(x)
            source = "student_model"
        else:
            probs = self._get_teacher_distribution(context)
            source = "heuristic_teacher"
        # Also get teacher distribution for comparison
        teacher_probs = self._get_teacher_distribution(context)
        # Prepare explanation
        explanations = []
        for i, p in enumerate(probs):
            explanations.append({
                'action_index': i,
                'probability': float(p),
                'teacher_probability': float(teacher_probs[i]),
                'difference': float(p - teacher_probs[i]),
            })
        return {
            'source': source,
            'probabilities': probs.tolist(),
            'teacher_probabilities': teacher_probs.tolist(),
            'explanations': explanations,
            'context_snapshot': {k: context.get(k) for k in ['carbon_intensity', 'helium_scarcity', 'energy_price'] if k in context},
        }

    # ------------------ Human Approval ------------------
    def set_approval_callback(self, callback: Callable[[int, Dict], bool]):
        self.approval_callback = callback

    async def request_approval(self, action_index: int, context: Dict) -> bool:
        if self.approval_callback is None:
            return True
        result = self.approval_callback(action_index, context)
        if asyncio.iscoroutine(result):
            return await result
        return bool(result)

    # ------------------ Drift Detection ------------------
    async def get_drift_stats(self) -> Dict[str, Any]:
        async with self._lock:
            if not self.drift_history:
                return {'status': 'insufficient_data'}
            return {
                'current_drift': self.drift_history[-1],
                'avg_drift': np.mean(self.drift_history),
                'max_drift': max(self.drift_history),
                'samples': len(self.drift_history),
            }

    # ------------------ Chaos Testing ------------------
    async def inject_fault(self, fault_type: str):
        async with self._lock:
            if fault_type == 'teacher_failure':
                self.bridge = None
                self.default_penalties = {k: 0.9 for k in self.default_penalties}
                logger.warning("Injected teacher_failure: bridge disabled, high penalties")
            elif fault_type == 'student_corruption':
                if TORCH_AVAILABLE:
                    for param in self.student.model.parameters():
                        param.data.fill_(0.0)
                else:
                    self.student.weights.fill(0.0)
                    self.student.biases.fill(0.0)
                logger.warning("Injected student_corruption: weights zeroed")
            elif fault_type == 'clear_cache':
                self.cache.clear()
                logger.warning("Injected clear_cache")
            else:
                logger.warning(f"Unknown fault type: {fault_type}")

    async def run_chaos_test(self) -> Dict[str, Any]:
        report = {'faults': [], 'results': {}}
        # Test teacher failure
        await self.inject_fault('teacher_failure')
        report['faults'].append('teacher_failure')
        probs = await self.get_distilled_policy({'carbon_intensity': 500, 'helium_scarcity': 0.7, 'energy_price': 0.6})
        report['results']['teacher_failure'] = {
            'sum': float(np.sum(probs)),
            'probs': probs.tolist()
        }
        # Reset bridge?
        # Test student corruption
        await self.inject_fault('student_corruption')
        report['faults'].append('student_corruption')
        probs = await self.get_distilled_policy({'carbon_intensity': 500, 'helium_scarcity': 0.7, 'energy_price': 0.6})
        report['results']['student_corruption'] = {
            'sum': float(np.sum(probs)),
            'probs': probs.tolist()
        }
        # Clear cache
        await self.inject_fault('clear_cache')
        report['faults'].append('clear_cache')
        return report
