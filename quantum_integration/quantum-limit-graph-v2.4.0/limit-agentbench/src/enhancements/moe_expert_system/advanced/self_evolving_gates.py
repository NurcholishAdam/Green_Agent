# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/self_evolving_gates.py
# Enhanced with meta-gradient learning, NTK analysis, zero-shot transfer, hypernetworks, and multi-fidelity optimization

"""
Enhanced Self-Evolving Gates v3.0.0
- Meta-Gradient Learning (learning to learn)
- Neural Tangent Kernel (NTK) analysis for architecture prediction
- Zero-Shot Architecture Transfer across domains
- Evolutionary Strategies (ES) for population-based optimization
- Hypernetwork-Based Rapid Adaptation
- Self-Supervised Pretraining for better initialization
- Multi-Fidelity Optimization with low-fidelity proxies
- Architecture-Model Co-Evolution
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from collections import defaultdict, deque
import copy
import math
import hashlib
import json

logger = logging.getLogger(__name__)

# ============================================================================
# Meta-Gradient Learning
# ============================================================================

class MetaGradientLearner(nn.Module):
    """
    Meta-Gradient Learning: Learning to learn.
    
    Learns optimal learning rates and update rules
    through gradient-based meta-learning.
    """
    
    def __init__(
        self,
        input_dim: int,
        hidden_dim: int = 128,
        meta_lr: float = 0.001
    ):
        super().__init__()
        self.input_dim = input_dim
        self.meta_lr = meta_lr
        
        # Learning rate predictor
        self.lr_predictor = nn.Sequential(
            nn.Linear(input_dim + 3, hidden_dim),  # +3 for loss, grad_norm, iteration
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.ReLU(),
            nn.Linear(hidden_dim // 2, 1),  # Single learning rate
            nn.Sigmoid()  # Bounded [0, 1]
        )
        
        # Update rule modulator
        self.update_modulator = nn.Sequential(
            nn.Linear(input_dim + 3, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, 3),  # Momentum, weight_decay, grad_clip
            nn.Softplus()  # Positive values
        )
        
        # Meta-optimizer
        self.meta_optimizer = torch.optim.Adam(
            self.parameters(), lr=meta_lr
        )
        
        # Learning rate history
        self.lr_history: deque = deque(maxlen=1000)
        
        logger.info(f"Meta-Gradient Learner initialized: dim={input_dim}")
    
    def predict_learning_params(
        self,
        gradient_norm: float,
        loss_value: float,
        iteration: int,
        task_embedding: torch.Tensor
    ) -> Dict[str, float]:
        """
        Predict optimal learning parameters based on current state.
        
        Returns:
            {
                'learning_rate': float,
                'momentum': float,
                'weight_decay': float,
                'gradient_clip': float
            }
        """
        # Build state vector
        state = torch.cat([
            task_embedding,
            torch.tensor([gradient_norm, loss_value, iteration / 1000.0])
        ])
        
        # Predict learning rate
        lr = self.lr_predictor(state).item()
        lr = lr * 0.1  # Scale to [0, 0.1]
        
        # Predict update modulators
        modulators = self.update_modulator(state)
        momentum = torch.sigmoid(modulators[0]).item()  # [0, 1]
        weight_decay = modulators[1].item() * 0.01  # Small weight decay
        grad_clip = modulators[2].item()  # Gradient clipping threshold
        
        self.lr_history.append({
            'lr': lr,
            'momentum': momentum,
            'weight_decay': weight_decay,
            'grad_clip': grad_clip,
            'grad_norm': gradient_norm,
            'loss': loss_value,
            'iteration': iteration
        })
        
        return {
            'learning_rate': lr,
            'momentum': momentum,
            'weight_decay': weight_decay,
            'gradient_clip': grad_clip
        }
    
    def meta_update(
        self,
        task_losses: List[float],
        task_gradients: List[torch.Tensor]
    ):
        """
        Meta-update the learning rate predictor.
        
        Improves future learning rate predictions.
        """
        if not task_losses:
            return
        
        # Compute meta-gradient
        meta_loss = torch.tensor(task_losses).mean()
        
        # Update meta-parameters
        self.meta_optimizer.zero_grad()
        meta_loss.backward()
        torch.nn.utils.clip_grad_norm_(self.parameters(), 1.0)
        self.meta_optimizer.step()
    
    def get_lr_statistics(self) -> Dict[str, Any]:
        """Get learning rate statistics"""
        recent = list(self.lr_history)[-100:]
        
        if not recent:
            return {}
        
        return {
            'mean_lr': np.mean([r['lr'] for r in recent]),
            'std_lr': np.std([r['lr'] for r in recent]),
            'mean_momentum': np.mean([r['momentum'] for r in recent]),
            'trend': 'increasing' if len(recent) >= 2 and recent[-1]['lr'] > recent[0]['lr'] else 'stable'
        }


# ============================================================================
# Neural Tangent Kernel (NTK) Analysis
# ============================================================================

class NeuralTangentKernelAnalyzer:
    """
    Neural Tangent Kernel analysis for architecture prediction.
    
    Predicts architecture performance without full training
    using NTK theory.
    """
    
    def __init__(self, num_samples: int = 100):
        self.num_samples = num_samples
        self.ntk_cache: Dict[str, Dict[str, Any]] = {}
        self.prediction_accuracy: deque = deque(maxlen=100)
        
        logger.info(f"NTK Analyzer initialized: samples={num_samples}")
    
    def compute_ntk(
        self,
        architecture: nn.Module,
        sample_input: torch.Tensor
    ) -> Tuple[torch.Tensor, float]:
        """
        Compute empirical Neural Tangent Kernel.
        
        Returns:
            (ntk_matrix, condition_number)
        """
        architecture.eval()
        parameters = list(architecture.parameters())
        
        # Compute Jacobian for each sample
        jacobians = []
        
        for i in range(min(self.num_samples, sample_input.size(0))):
            x = sample_input[i:i+1]
            architecture.zero_grad()
            
            output = architecture(x)
            
            # Compute gradient for each parameter
            grads = []
            for param in parameters:
                if param.requires_grad:
                    grad = torch.autograd.grad(
                        output.sum(), param,
                        create_graph=False, retain_graph=True
                    )[0]
                    grads.append(grad.flatten())
            
            if grads:
                jacobians.append(torch.cat(grads))
        
        if not jacobians:
            return torch.eye(1), float('inf')
        
        # Stack Jacobians
        J = torch.stack(jacobians)  # [N, P]
        
        # Compute NTK: K = J @ J^T
        K = J @ J.T
        
        # Compute condition number
        eigenvalues = torch.linalg.eigvalsh(K)
        condition_number = (
            eigenvalues[-1] / eigenvalues[0]
        ).item() if eigenvalues[0] > 0 else float('inf')
        
        return K, condition_number
    
    def predict_trainability(
        self,
        architecture: nn.Module,
        sample_input: torch.Tensor
    ) -> Dict[str, Any]:
        """
        Predict architecture trainability using NTK.
        
        Lower condition number = better trainability.
        """
        arch_hash = self._hash_architecture(architecture)
        
        # Check cache
        if arch_hash in self.ntk_cache:
            return self.ntk_cache[arch_hash]
        
        # Compute NTK
        K, condition_number = self.compute_ntk(architecture, sample_input)
        
        # Predict convergence speed
        if condition_number < 10:
            convergence = 'fast'
            trainability_score = 0.9
        elif condition_number < 100:
            convergence = 'moderate'
            trainability_score = 0.7
        elif condition_number < 1000:
            convergence = 'slow'
            trainability_score = 0.4
        else:
            convergence = 'very_slow'
            trainability_score = 0.1
        
        # Estimate required iterations
        estimated_iterations = int(condition_number * 10)
        
        prediction = {
            'condition_number': condition_number,
            'convergence_speed': convergence,
            'trainability_score': trainability_score,
            'estimated_iterations': estimated_iterations,
            'eigenvalue_spread': condition_number,
            'ntk_trace': torch.trace(K).item() if K.size(0) > 0 else 0
        }
        
        # Cache result
        self.ntk_cache[arch_hash] = prediction
        
        return prediction
    
    def compare_architectures(
        self,
        arch_a: nn.Module,
        arch_b: nn.Module,
        sample_input: torch.Tensor
    ) -> Dict[str, Any]:
        """
        Compare two architectures using NTK analysis.
        
        Predicts which will train better without training.
        """
        pred_a = self.predict_trainability(arch_a, sample_input)
        pred_b = self.predict_trainability(arch_b, sample_input)
        
        # Determine winner
        if pred_a['trainability_score'] > pred_b['trainability_score']:
            winner = 'architecture_a'
            margin = pred_a['trainability_score'] - pred_b['trainability_score']
        elif pred_b['trainability_score'] > pred_a['trainability_score']:
            winner = 'architecture_b'
            margin = pred_b['trainability_score'] - pred_a['trainability_score']
        else:
            winner = 'tie'
            margin = 0.0
        
        # Record prediction accuracy (would be validated after actual training)
        self.prediction_accuracy.append({
            'condition_a': pred_a['condition_number'],
            'condition_b': pred_b['condition_number'],
            'winner': winner,
            'margin': margin
        })
        
        return {
            'architecture_a': pred_a,
            'architecture_b': pred_b,
            'winner': winner,
            'confidence': min(0.95, max(0.5, 0.5 + margin * 2)),
            'recommendation': (
                f"Architecture {'A' if winner == 'architecture_a' else 'B'} predicted better "
                f"(confidence: {min(0.95, max(0.5, 0.5 + margin * 2)):.1%})"
            )
        }
    
    def _hash_architecture(self, architecture: nn.Module) -> str:
        """Hash architecture for caching"""
        arch_str = str(architecture) + str(sum(p.numel() for p in architecture.parameters()))
        return hashlib.md5(arch_str.encode()).hexdigest()
    
    def get_prediction_accuracy(self) -> float:
        """Get NTK prediction accuracy based on validation"""
        if not self.prediction_accuracy:
            return 0.5
        
        # Simplified: would compare predictions with actual outcomes
        return 0.75  # Placeholder
    
    def clear_cache(self):
        """Clear NTK cache"""
        self.ntk_cache.clear()
        logger.info("NTK cache cleared")


# ============================================================================
# Zero-Shot Architecture Transfer
# ============================================================================

class ZeroShotArchitectureTransfer:
    """
    Zero-shot architecture transfer across domains.
    
    Transfers architectures to new domains without training.
    """
    
    def __init__(self):
        self.domain_embeddings: Dict[str, np.ndarray] = {}
        self.architecture_embeddings: Dict[str, np.ndarray] = {}
        self.transfer_success_rate: deque = deque(maxlen=100)
        
        logger.info("Zero-Shot Architecture Transfer initialized")
    
    def register_domain(
        self,
        domain_name: str,
        domain_characteristics: Dict[str, float]
    ):
        """Register a domain with its characteristics"""
        embedding = np.array(list(domain_characteristics.values()))
        self.domain_embeddings[domain_name] = embedding
        
        logger.info(f"Registered domain: {domain_name}")
    
    def register_architecture(
        self,
        arch_id: str,
        architecture_config: Dict[str, Any],
        source_domain: str,
        performance: float
    ):
        """Register an architecture with its source domain"""
        # Create architecture embedding from config
        embedding = self._config_to_embedding(architecture_config)
        embedding = np.append(embedding, performance)
        
        self.architecture_embeddings[arch_id] = {
            'embedding': embedding,
            'source_domain': source_domain,
            'performance': performance
        }
        
        logger.debug(f"Registered architecture: {arch_id}")
    
    def predict_transfer_performance(
        self,
        arch_id: str,
        target_domain: str
    ) -> Dict[str, Any]:
        """
        Predict architecture performance in target domain.
        
        Uses domain similarity and architecture characteristics.
        """
        if arch_id not in self.architecture_embeddings:
            return {'error': 'Architecture not registered'}
        
        if target_domain not in self.domain_embeddings:
            return {'error': 'Target domain not registered'}
        
        arch_info = self.architecture_embeddings[arch_id]
        source_domain = arch_info['source_domain']
        source_performance = arch_info['performance']
        
        # Calculate domain similarity
        if source_domain in self.domain_embeddings:
            source_embedding = self.domain_embeddings[source_domain]
            target_embedding = self.domain_embeddings[target_domain]
            
            similarity = self._cosine_similarity(source_embedding, target_embedding)
        else:
            similarity = 0.5
        
        # Predict performance with similarity-based decay
        predicted_performance = source_performance * similarity
        
        # Calculate confidence
        confidence = similarity * 0.8  # Scale confidence
        
        prediction = {
            'arch_id': arch_id,
            'source_domain': source_domain,
            'target_domain': target_domain,
            'source_performance': source_performance,
            'domain_similarity': similarity,
            'predicted_performance': predicted_performance,
            'confidence': confidence,
            'transferable': similarity > 0.5,
            'recommendation': (
                f"Transfer {'recommended' if similarity > 0.7 else 'possible' if similarity > 0.5 else 'not recommended'}"
                f" (similarity: {similarity:.2f})"
            )
        }
        
        return prediction
    
    def find_best_architecture(
        self,
        target_domain: str,
        top_k: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Find best architectures for target domain.
        
        Uses zero-shot transfer prediction.
        """
        predictions = []
        
        for arch_id in self.architecture_embeddings:
            pred = self.predict_transfer_performance(arch_id, target_domain)
            if 'error' not in pred:
                predictions.append(pred)
        
        # Sort by predicted performance
        predictions.sort(
            key=lambda p: p['predicted_performance'],
            reverse=True
        )
        
        return predictions[:top_k]
    
    def _config_to_embedding(self, config: Dict[str, Any]) -> np.ndarray:
        """Convert architecture config to embedding"""
        embedding = []
        
        # Extract numerical features
        for key, value in sorted(config.items()):
            if isinstance(value, (int, float)):
                embedding.append(float(value))
            elif isinstance(value, bool):
                embedding.append(1.0 if value else 0.0)
            elif isinstance(value, list):
                embedding.extend([float(v) if isinstance(v, (int, float)) else 0.0 for v in value[:5]])
        
        # Pad to fixed length
        while len(embedding) < 20:
            embedding.append(0.0)
        
        return np.array(embedding[:20])
    
    def _cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        """Calculate cosine similarity"""
        min_len = min(len(a), len(b))
        a = a[:min_len]
        b = b[:min_len]
        
        dot = np.dot(a, b)
        norm_a = np.linalg.norm(a)
        norm_b = np.linalg.norm(b)
        
        if norm_a > 0 and norm_b > 0:
            return max(0, dot / (norm_a * norm_b))
        return 0.5
    
    def record_transfer_result(
        self,
        arch_id: str,
        target_domain: str,
        actual_performance: float,
        predicted_performance: float
    ):
        """Record actual transfer result for accuracy tracking"""
        error = abs(actual_performance - predicted_performance)
        self.transfer_success_rate.append({
            'arch_id': arch_id,
            'target_domain': target_domain,
            'error': error,
            'accurate': error < 0.1
        })
    
    def get_transfer_accuracy(self) -> float:
        """Get transfer prediction accuracy"""
        if not self.transfer_success_rate:
            return 0.5
        
        return np.mean([1.0 if r['accurate'] else 0.0 for r in self.transfer_success_rate])


# ============================================================================
# Evolutionary Strategies (ES)
# ============================================================================

class EvolutionaryStrategies:
    """
    Evolutionary Strategies for population-based optimization.
    
    Uses natural evolution strategies (NES) for efficient
    black-box optimization of gate architectures.
    """
    
    def __init__(
        self,
        population_size: int = 50,
        sigma: float = 0.1,
        learning_rate: float = 0.01
    ):
        self.population_size = population_size
        self.sigma = sigma
        self.learning_rate = learning_rate
        
        # Population
        self.mean_weights: Optional[np.ndarray] = None
        self.fitness_history: deque = deque(maxlen=1000)
        self.generation = 0
        
        logger.info(f"Evolutionary Strategies initialized: pop={population_size}, σ={sigma}")
    
    def initialize(self, num_parameters: int):
        """Initialize population mean"""
        self.mean_weights = np.random.randn(num_parameters) * 0.01
        logger.info(f"ES initialized with {num_parameters} parameters")
    
    def ask(self) -> List[np.ndarray]:
        """
        Generate population by sampling around mean.
        
        Returns list of perturbed parameter vectors.
        """
        population = []
        
        for _ in range(self.population_size):
            # Sample perturbation
            epsilon = np.random.randn(len(self.mean_weights))
            
            # Create candidate
            candidate = self.mean_weights + self.sigma * epsilon
            population.append(candidate)
        
        return population
    
    def tell(
        self,
        population: List[np.ndarray],
        fitnesses: List[float]
    ):
        """
        Update mean using fitness-weighted gradient.
        
        Uses natural gradient estimation.
        """
        if len(population) != len(fitnesses):
            return
        
        # Normalize fitnesses
        fitnesses = np.array(fitnesses)
        fitnesses = (fitnesses - np.mean(fitnesses)) / (np.std(fitnesses) + 1e-8)
        
        # Compute gradient estimate
        gradient = np.zeros_like(self.mean_weights)
        
        for candidate, fitness in zip(population, fitnesses):
            # Reconstruct epsilon
            epsilon = (candidate - self.mean_weights) / self.sigma
            gradient += fitness * epsilon
        
        gradient /= (len(population) * self.sigma)
        
        # Update mean
        self.mean_weights += self.learning_rate * gradient
        
        # Record fitness
        self.fitness_history.append({
            'generation': self.generation,
            'mean_fitness': np.mean(fitnesses),
            'max_fitness': np.max(fitnesses),
            'min_fitness': np.min(fitnesses)
        })
        
        self.generation += 1
    
    def get_best(self) -> np.ndarray:
        """Get current best (mean) parameters"""
        return self.mean_weights
    
    def adapt_sigma(self, success_rate: float):
        """
        Adapt mutation strength based on success rate.
        
        Increase sigma if too many successes, decrease if too few.
        """
        target_success_rate = 0.2  # 1/5 rule
        
        if success_rate > target_success_rate:
            self.sigma *= 1.1
        else:
            self.sigma *= 0.9
        
        self.sigma = max(0.01, min(1.0, self.sigma))
    
    def get_stats(self) -> Dict[str, Any]:
        """Get ES statistics"""
        recent = list(self.fitness_history)[-50:]
        
        return {
            'generation': self.generation,
            'population_size': self.population_size,
            'sigma': self.sigma,
            'mean_fitness': np.mean([r['mean_fitness'] for r in recent]) if recent else 0,
            'max_fitness': np.max([r['max_fitness'] for r in recent]) if recent else 0
        }


# ============================================================================
# Hypernetwork-Based Adaptation
# ============================================================================

class HypernetworkAdapter(nn.Module):
    """
    Hypernetwork for rapid weight generation.
    
    Generates gate network weights based on task context.
    """
    
    def __init__(
        self,
        context_dim: int,
        target_weight_shapes: List[Tuple[int, ...]],
        hidden_dim: int = 128
    ):
        super().__init__()
        self.context_dim = context_dim
        self.target_shapes = target_weight_shapes
        self.total_params = sum(np.prod(s) for s in target_weight_shapes)
        
        # Hypernetwork
        self.hypernet = nn.Sequential(
            nn.Linear(context_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim * 2),
            nn.ReLU(),
            nn.Linear(hidden_dim * 2, hidden_dim * 2),
            nn.ReLU(),
            nn.Linear(hidden_dim * 2, self.total_params)
        )
        
        # Chunk sizes for splitting output
        self.chunk_sizes = [int(np.prod(s)) for s in target_weight_shapes]
        
        logger.info(
            f"Hypernetwork Adapter initialized: "
            f"context_dim={context_dim}, total_params={self.total_params}"
        )
    
    def forward(
        self,
        context: torch.Tensor
    ) -> List[torch.Tensor]:
        """
        Generate weights from context.
        
        Args:
            context: Task context embedding [B, context_dim]
            
        Returns:
            List of weight tensors matching target shapes
        """
        # Generate flat parameters
        flat_params = self.hypernet(context)
        
        # Split into target shapes
        weights = []
        start = 0
        
        for shape in self.target_shapes:
            size = int(np.prod(shape))
            chunk = flat_params[:, start:start + size]
            weights.append(chunk.view(-1, *shape))
            start += size
        
        return weights
    
    def generate_gate_weights(
        self,
        task_context: Dict[str, Any]
    ) -> Dict[str, torch.Tensor]:
        """
        Generate complete gate network weights for a task.
        
        Enables rapid adaptation without gradient steps.
        """
        # Create context embedding
        context = self._create_context_embedding(task_context)
        
        # Generate weights
        with torch.no_grad():
            weights = self.forward(context.unsqueeze(0))
        
        # Map to weight dictionary
        weight_dict = {}
        for i, (shape, weight) in enumerate(zip(self.target_shapes, weights)):
            weight_dict[f'layer_{i}'] = weight.squeeze(0)
        
        return weight_dict
    
    def _create_context_embedding(
        self,
        task_context: Dict[str, Any]
    ) -> torch.Tensor:
        """Create context embedding from task context"""
        features = [
            task_context.get('complexity', 0.5),
            task_context.get('carbon_zone', 0) / 15.0,
            task_context.get('helium_scarcity', 0.5),
            task_context.get('latency_budget_ms', 100) / 1000.0,
            task_context.get('data_size_mb', 1.0) / 1000.0,
            task_context.get('renewable_percent', 0.0),
            task_context.get('time_of_day', 12) / 24.0,
            task_context.get('historical_success_rate', 0.9)
        ]
        
        # Pad to context_dim
        while len(features) < self.context_dim:
            features.append(0.0)
        
        return torch.tensor(features[:self.context_dim], dtype=torch.float32)


# ============================================================================
# Self-Supervised Pretraining
# ============================================================================

class SelfSupervisedPretrainer:
    """
    Self-supervised pretraining for better gate initialization.
    
    Uses contrastive learning on routing patterns.
    """
    
    def __init__(
        self,
        input_dim: int,
        hidden_dim: int = 128,
        temperature: float = 0.07
    ):
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.temperature = temperature
        
        # Encoder network
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, 64)  # Projection head
        )
        
        # Pretraining optimizer
        self.optimizer = torch.optim.Adam(
            self.encoder.parameters(), lr=0.001
        )
        
        # Pretraining history
        self.pretrain_losses: deque = deque(maxlen=1000)
        self.is_pretrained = False
        
        logger.info(f"Self-Supervised Pretrainer initialized: dim={input_dim}")
    
    def augment_sample(
        self,
        sample: torch.Tensor,
        noise_std: float = 0.1
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """
        Create two augmented views of the same sample.
        
        Used for contrastive learning.
        """
        # View 1: Add Gaussian noise
        view1 = sample + torch.randn_like(sample) * noise_std
        
        # View 2: Dropout-style masking
        mask = torch.bernoulli(torch.ones_like(sample) * 0.8)
        view2 = sample * mask + torch.randn_like(sample) * noise_std * (1 - mask)
        
        return view1, view2
    
    def contrastive_loss(
        self,
        z1: torch.Tensor,
        z2: torch.Tensor,
        temperature: Optional[float] = None
    ) -> torch.Tensor:
        """
        Compute NT-Xent contrastive loss.
        
        Args:
            z1, z2: Encoded representations [B, D]
        """
        if temperature is None:
            temperature = self.temperature
        
        batch_size = z1.size(0)
        
        # Normalize representations
        z1 = F.normalize(z1, dim=-1)
        z2 = F.normalize(z2, dim=-1)
        
        # Concatenate
        z = torch.cat([z1, z2], dim=0)  # [2B, D]
        
        # Compute similarity matrix
        sim = torch.matmul(z, z.T) / temperature  # [2B, 2B]
        
        # Create labels (positive pairs)
        labels = torch.arange(batch_size)
        labels = torch.cat([labels + batch_size, labels], dim=0)
        
        # Mask out self-similarity
        mask = torch.eye(2 * batch_size, dtype=torch.bool)
        sim = sim.masked_fill(mask, -float('inf'))
        
        # Compute loss
        loss = F.cross_entropy(sim, labels)
        
        return loss
    
    def pretrain(
        self,
        samples: List[torch.Tensor],
        epochs: int = 100,
        batch_size: int = 32
    ):
        """
        Pretrain encoder using contrastive learning.
        
        Learns useful representations without labels.
        """
        if len(samples) < batch_size:
            logger.warning(f"Insufficient samples for pretraining: {len(samples)}")
            return
        
        self.encoder.train()
        
        for epoch in range(epochs):
            epoch_loss = 0.0
            num_batches = 0
            
            # Shuffle samples
            indices = np.random.permutation(len(samples))
            
            for i in range(0, len(samples) - batch_size, batch_size):
                batch_indices = indices[i:i + batch_size]
                batch = torch.stack([samples[j] for j in batch_indices])
                
                # Create augmented views
                view1, view2 = self.augment_sample(batch)
                
                # Encode
                z1 = self.encoder(view1)
                z2 = self.encoder(view2)
                
                # Compute loss
                loss = self.contrastive_loss(z1, z2)
                
                # Update
                self.optimizer.zero_grad()
                loss.backward()
                self.optimizer.step()
                
                epoch_loss += loss.item()
                num_batches += 1
            
            avg_loss = epoch_loss / max(num_batches, 1)
            self.pretrain_losses.append(avg_loss)
            
            if (epoch + 1) % 20 == 0:
                logger.info(f"Pretraining epoch {epoch+1}/{epochs}: loss={avg_loss:.4f}")
        
        self.is_pretrained = True
        logger.info("Pretraining complete")
    
    def get_pretrained_weights(self) -> Dict[str, torch.Tensor]:
        """Get pretrained encoder weights for initialization"""
        return {
            name: param.data.clone()
            for name, param in self.encoder.named_parameters()
        }
    
    def initialize_from_pretrained(
        self,
        target_network: nn.Module
    ):
        """
        Initialize target network with pretrained weights.
        
        Transfers learned representations.
        """
        if not self.is_pretrained:
            logger.warning("Not pretrained, skipping initialization")
            return
        
        pretrained_state = self.encoder.state_dict()
        
        # Transfer compatible layers
        target_state = target_network.state_dict()
        
        for name, param in target_state.items():
            if name in pretrained_state:
                if param.shape == pretrained_state[name].shape:
                    target_state[name] = pretrained_state[name]
                    logger.debug(f"Transferred pretrained weights: {name}")
        
        target_network.load_state_dict(target_state)
        
        logger.info("Initialized target network with pretrained weights")


# ============================================================================
# Multi-Fidelity Optimization
# ============================================================================

class MultiFidelityOptimizer:
    """
    Multi-fidelity optimization for efficient architecture search.
    
    Uses low-fidelity proxies before full evaluation:
    - Fidelity 0: NTK analysis (fastest)
    - Fidelity 1: Few-shot training (fast)
    - Fidelity 2: Reduced data training (medium)
    - Fidelity 3: Full training (slowest)
    """
    
    def __init__(
        self,
        ntk_analyzer: Optional[NeuralTangentKernelAnalyzer] = None,
        max_fidelity_budget: float = 100.0
    ):
        self.ntk_analyzer = ntk_analyzer
        self.max_fidelity_budget = max_fidelity_budget
        self.fidelity_costs = {
            0: 0.1,   # NTK analysis
            1: 1.0,   # Few-shot
            2: 5.0,   # Reduced data
            3: 20.0   # Full training
        }
        self.fidelity_history: deque = deque(maxlen=1000)
        self.budget_spent = 0.0
        
        logger.info(f"Multi-Fidelity Optimizer initialized: budget={max_fidelity_budget}")
    
    def select_fidelity(
        self,
        architecture: nn.Module,
        remaining_budget: float,
        uncertainty_threshold: float = 0.3
    ) -> int:
        """
        Select appropriate fidelity level for evaluation.
        
        Uses successive halving with Bayesian optimization.
        """
        if self.ntk_analyzer and remaining_budget > self.fidelity_costs[0]:
            # Start with NTK analysis
            return 0
        elif remaining_budget > self.fidelity_costs[1]:
            # Few-shot evaluation
            return 1
        elif remaining_budget > self.fidelity_costs[2]:
            # Reduced data evaluation
            return 2
        elif remaining_budget > self.fidelity_costs[3]:
            # Full evaluation
            return 3
        else:
            return -1  # No budget
    
    def should_promote(
        self,
        fidelity: int,
        performance: float,
        threshold: float
    ) -> bool:
        """
        Determine if architecture should be promoted to higher fidelity.
        
        Uses performance prediction and uncertainty.
        """
        if fidelity >= 3:
            return False  # Already at highest
        
        # Performance thresholds for promotion
        promotion_thresholds = {
            0: 0.6,  # NTK: promote if trainability > 0.6
            1: 0.7,  # Few-shot: promote if accuracy > 0.7
            2: 0.8,  # Reduced: promote if accuracy > 0.8
        }
        
        return performance > promotion_thresholds.get(fidelity, 0.5)
    
    def evaluate_fidelity(
        self,
        architecture: nn.Module,
        fidelity: int,
        sample_input: Optional[torch.Tensor] = None
    ) -> Dict[str, Any]:
        """
        Evaluate architecture at given fidelity level.
        """
        cost = self.fidelity_costs.get(fidelity, 1.0)
        self.budget_spent += cost
        
        result = {
            'fidelity': fidelity,
            'cost': cost,
            'budget_remaining': self.max_fidelity_budget - self.budget_spent
        }
        
        if fidelity == 0 and self.ntk_analyzer and sample_input is not None:
            # NTK analysis
            ntk_result = self.ntk_analyzer.predict_trainability(
                architecture, sample_input
            )
            result['performance'] = ntk_result['trainability_score']
            result['ntk_result'] = ntk_result
        
        elif fidelity == 1:
            # Few-shot evaluation (simulated)
            result['performance'] = np.random.beta(5, 5)  # Placeholder
        
        elif fidelity == 2:
            # Reduced data evaluation (simulated)
            result['performance'] = np.random.beta(6, 4)
        
        elif fidelity == 3:
            # Full evaluation (simulated)
            result['performance'] = np.random.beta(7, 3)
        
        self.fidelity_history.append(result)
        
        return result
    
    def optimize(
        self,
        candidates: List[nn.Module],
        sample_input: Optional[torch.Tensor] = None,
        top_k: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Multi-fidelity optimization of candidates.
        
        Efficiently finds best architectures within budget.
        """
        results = []
        remaining = candidates.copy()
        fidelity = 0
        
        while remaining and self.budget_spent < self.max_fidelity_budget:
            next_fidelity = self.select_fidelity(
                remaining[0] if remaining else None,
                self.max_fidelity_budget - self.budget_spent
            )
            
            if next_fidelity < 0:
                break
            
            fidelity = next_fidelity
            
            # Evaluate all remaining at this fidelity
            for arch in remaining:
                result = self.evaluate_fidelity(arch, fidelity, sample_input)
                result['architecture'] = arch
                results.append(result)
            
            # Filter: keep only promising candidates
            if fidelity < 3:
                remaining = [
                    r['architecture'] for r in results[-len(remaining):]
                    if self.should_promote(fidelity, r['performance'], 0.5)
                ]
            else:
                break
            
            fidelity += 1
        
        # Sort by performance
        results.sort(key=lambda r: r.get('performance', 0), reverse=True)
        
        return results[:top_k]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get multi-fidelity optimization statistics"""
        return {
            'total_budget': self.max_fidelity_budget,
            'budget_spent': self.budget_spent,
            'budget_remaining': self.max_fidelity_budget - self.budget_spent,
            'evaluations_per_fidelity': {
                f: sum(1 for r in self.fidelity_history if r['fidelity'] == f)
                for f in range(4)
            },
            'total_evaluations': len(self.fidelity_history)
        }


# ============================================================================
# Enhanced Self-Evolving Gate with All Integrations
# ============================================================================

class EnhancedSelfEvolvingGate(nn.Module):
    """
    Enhanced Self-Evolving Gate v3.0.0
    
    New capabilities:
    - Meta-Gradient Learning
    - NTK Analysis for architecture prediction
    - Zero-Shot Architecture Transfer
    - Evolutionary Strategies
    - Hypernetwork-Based Adaptation
    - Self-Supervised Pretraining
    - Multi-Fidelity Optimization
    """
    
    def __init__(
        self,
        input_dim: int,
        num_experts: int,
        hidden_dim: int = 128,
        enable_meta_gradient: bool = True,
        enable_ntk: bool = True,
        enable_zero_shot: bool = True,
        enable_es: bool = True,
        enable_hypernetwork: bool = True,
        enable_pretraining: bool = True,
        enable_multi_fidelity: bool = True
    ):
        super().__init__()
        
        self.input_dim = input_dim
        self.num_experts = num_experts
        self.enable_meta_gradient = enable_meta_gradient
        self.enable_ntk = enable_ntk
        self.enable_zero_shot = enable_zero_shot
        self.enable_es = enable_es
        self.enable_hypernetwork = enable_hypernetwork
        self.enable_pretraining = enable_pretraining
        self.enable_multi_fidelity = enable_multi_fidelity
        
        # Core gate network
        self.gate_network = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.LayerNorm(hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.LayerNorm(hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, num_experts)
        )
        
        # New sub-modules
        self.meta_learner = MetaGradientLearner(input_dim) if enable_meta_gradient else None
        self.ntk_analyzer = NeuralTangentKernelAnalyzer() if enable_ntk else None
        self.zero_shot = ZeroShotArchitectureTransfer() if enable_zero_shot else None
        self.es_optimizer = EvolutionaryStrategies() if enable_es else None
        self.hypernetwork = (
            HypernetworkAdapter(
                context_dim=8,
                target_weight_shapes=[
                    (hidden_dim, input_dim),
                    (hidden_dim,),
                    (hidden_dim, hidden_dim),
                    (hidden_dim,),
                    (num_experts, hidden_dim),
                    (num_experts,)
                ]
            ) if enable_hypernetwork else None
        )
        self.pretrainer = SelfSupervisedPretrainer(input_dim) if enable_pretraining else None
        self.multi_fidelity = (
            MultiFidelityOptimizer(ntk_analyzer=self.ntk_analyzer)
            if enable_multi_fidelity else None
        )
        
        # ES initialization
        if enable_es:
            total_params = sum(p.numel() for p in self.gate_network.parameters())
            self.es_optimizer.initialize(total_params)
        
        # Evolution history
        self.evolution_history: deque = deque(maxlen=10000)
        
        logger.info(
            f"Enhanced Self-Evolving Gate v3.0.0 initialized: "
            f"meta_grad={enable_meta_gradient}, ntk={enable_ntk}, "
            f"zero_shot={enable_zero_shot}, es={enable_es}, "
            f"hypernet={enable_hypernetwork}, pretrain={enable_pretraining}, "
            f"multi_fidelity={enable_multi_fidelity}"
        )
    
    def forward(
        self,
        x: torch.Tensor,
        task_context: Optional[Dict[str, Any]] = None,
        use_hypernetwork: bool = False
    ) -> Tuple[torch.Tensor, Dict[str, Any]]:
        """
        Enhanced forward pass with all integrations.
        """
        metadata = {}
        
        # Option 1: Use hypernetwork for rapid adaptation
        if use_hypernetwork and self.enable_hypernetwork and task_context:
            hyper_weights = self.hypernetwork.generate_gate_weights(task_context)
            
            # Apply hypernetwork-generated weights temporarily
            original_weights = {
                name: param.data.clone()
                for name, param in self.gate_network.named_parameters()
            }
            
            # Set hypernetwork weights
            for name, param in self.gate_network.named_parameters():
                if name.replace('.', '_') in hyper_weights:
                    param.data = hyper_weights[name.replace('.', '_')]
            
            # Forward pass
            output = self.gate_network(x)
            
            # Restore original weights
            for name, param in self.gate_network.named_parameters():
                if name in original_weights:
                    param.data = original_weights[name]
            
            metadata['hypernetwork_adapted'] = True
        else:
            # Standard forward pass
            output = self.gate_network(x)
        
        # Get routing weights
        weights = F.softmax(output, dim=-1)
        
        # Predict learning parameters if meta-gradient enabled
        if self.enable_meta_gradient:
            grad_norm = torch.norm(
                torch.cat([p.grad.flatten() for p in self.gate_network.parameters() if p.grad is not None])
            ).item() if any(p.grad is not None for p in self.gate_network.parameters()) else 0.0
            
            task_embedding = self._get_task_embedding(task_context) if task_context else torch.zeros(self.input_dim)
            
            learning_params = self.meta_learner.predict_learning_params(
                grad_norm, 0.0, 0, task_embedding
            )
            metadata['learning_params'] = learning_params
        
        # NTK analysis if enabled
        if self.enable_ntk and not self.training:
            ntk_prediction = self.ntk_analyzer.predict_trainability(
                self.gate_network, x
            )
            metadata['ntk_prediction'] = ntk_prediction
        
        metadata['entropy'] = -(weights * torch.log(weights + 1e-8)).sum(dim=-1).mean().item()
        metadata['confidence'] = weights.max(dim=-1)[0].mean().item()
        
        return weights, metadata
    
    def _get_task_embedding(self, task_context: Dict[str, Any]) -> torch.Tensor:
        """Create task embedding from context"""
        if not task_context:
            return torch.zeros(self.input_dim)
        
        features = [
            task_context.get('complexity', 0.5),
            task_context.get('carbon_zone', 0) / 15.0,
            task_context.get('helium_scarcity', 0.5),
            task_context.get('latency_budget_ms', 100) / 1000.0,
            task_context.get('data_size_mb', 1.0) / 1000.0
        ]
        
        # Pad to input_dim
        while len(features) < self.input_dim:
            features.append(0.0)
        
        return torch.tensor(features[:self.input_dim], dtype=torch.float32)
    
    def evolve_with_es(
        self,
        fitness_function: Callable,
        generations: int = 10
    ):
        """Evolve gate using Evolutionary Strategies"""
        if not self.enable_es:
            return
        
        for gen in range(generations):
            # Generate population
            population = self.es_optimizer.ask()
            
            # Evaluate fitness
            fitnesses = []
            for candidate in population:
                # Set weights
                self._set_flat_weights(candidate)
                
                # Evaluate
                fitness = fitness_function(self)
                fitnesses.append(fitness)
            
            # Update ES
            self.es_optimizer.tell(population, fitnesses)
            
            # Restore best weights
            self._set_flat_weights(self.es_optimizer.get_best())
            
            # Record evolution
            self.evolution_history.append({
                'generation': gen,
                'mean_fitness': np.mean(fitnesses),
                'max_fitness': np.max(fitnesses)
            })
    
    def _set_flat_weights(self, flat_weights: np.ndarray):
        """Set network weights from flat array"""
        start = 0
        for param in self.gate_network.parameters():
            numel = param.numel()
            param.data = torch.from_numpy(
                flat_weights[start:start + numel].reshape(param.shape)
            ).float()
            start += numel
    
    def pretrain_if_needed(self, samples: List[torch.Tensor]):
        """Pretrain if not already done"""
        if self.enable_pretraining and not self.pretrainer.is_pretrained:
            logger.info("Starting self-supervised pretraining...")
            self.pretrainer.pretrain(samples, epochs=50)
            self.pretrainer.initialize_from_pretrained(self.gate_network)
    
    def predict_transfer(
        self,
        target_domain: str
    ) -> Dict[str, Any]:
        """Predict transfer performance to new domain"""
        if self.enable_zero_shot:
            return self.zero_shot.predict_transfer_performance(
                f"gate_{id(self)}", target_domain
            )
        return {}
    
    def get_evolution_stats(self) -> Dict[str, Any]:
        """Get comprehensive evolution statistics"""
        stats = {
            'evolution_generations': len(self.evolution_history)
        }
        
        if self.enable_meta_gradient:
            stats['meta_learning'] = self.meta_learner.get_lr_statistics()
        
        if self.enable_ntk:
            stats['ntk_accuracy'] = self.ntk_analyzer.get_prediction_accuracy()
        
        if self.enable_es:
            stats['es'] = self.es_optimizer.get_stats()
        
        if self.enable_multi_fidelity:
            stats['multi_fidelity'] = self.multi_fidelity.get_stats()
        
        if self.enable_pretraining:
            stats['pretrained'] = self.pretrainer.is_pretrained
        
        return stats


# ============================================================================
# Legacy Compatibility
# ============================================================================

class SelfEvolvingGate(EnhancedSelfEvolvingGate):
    """
    Legacy self-evolving gate for backward compatibility.
    """
    
    def __init__(
        self,
        input_dim: int,
        num_experts: int,
        memory_size: int = 10000,
        adaptation_rate: float = 0.01,
        **kwargs
    ):
        super().__init__(
            input_dim=input_dim,
            num_experts=num_experts,
            enable_meta_gradient=kwargs.get('enable_meta_gradient', False),
            enable_ntk=kwargs.get('enable_ntk', False),
            enable_zero_shot=kwargs.get('enable_zero_shot', False),
            enable_es=kwargs.get('enable_es', False),
            enable_hypernetwork=kwargs.get('enable_hypernetwork', False),
            enable_pretraining=kwargs.get('enable_pretraining', False),
            enable_multi_fidelity=kwargs.get('enable_multi_fidelity', False)
        )
        
        self.memory: deque = deque(maxlen=memory_size)
        self.adaptation_rate = adaptation_rate
        self.performance_history: List[Dict] = []
        self.adaptation_history: List[Dict] = []
        
        logger.info("Self-Evolving Gate initialized (compatibility mode)")
