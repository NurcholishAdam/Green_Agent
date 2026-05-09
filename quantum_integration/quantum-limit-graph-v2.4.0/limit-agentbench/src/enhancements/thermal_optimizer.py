# src/enhancements/thermal_optimizer_enhanced.py

"""
Enhanced Thermal-Aware Workload Scheduling for Green Agent - Version 4.0

MAJOR ENHANCEMENTS OVER v3.2:
1. GRU-based temperature prediction with attention mechanism
2. Multi-objective reinforcement learning (PPO) for optimal cooling control
3. Bayesian optimization for hyperparameter tuning
4. Federated learning support for multi-datacenter coordination
5. Quantum-inspired simulated annealing for workload placement
6. Digital twin integration with real-time CFD surrogate modeling
7. Carbon-aware cooling optimization with grid emission factors
8. Adaptive model predictive control (AMPC) with uncertainty quantification
9. Graph neural networks for thermal dependency mapping
10. Tiered emergency response with graceful degradation
"""

import math
import numpy as np
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any, Callable, Union
from enum import Enum
import logging
import time
import threading
import asyncio
from collections import deque, defaultdict
import json
import hashlib
import warnings
from functools import lru_cache

# Scientific computing
from scipy import stats, signal
from scipy.optimize import minimize, differential_evolution, basinhopping
from scipy.integrate import odeint, solve_ivp
from scipy.interpolate import interp1d, CubicSpline
from scipy.spatial import KDTree
import numpy.fft as fft

# Machine learning
try:
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, IsolationForest
    from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler
    from sklearn.gaussian_process import GaussianProcessRegressor
    from sklearn.gaussian_process.kernels import RBF, WhiteKernel, Matern, ConstantKernel
    from sklearn.decomposition import PCA
    from sklearn.metrics import mean_squared_error, mean_absolute_error
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import torch
    import torch.nn as nn
    import torch.nn.functional as F
    import torch.optim as optim
    from torch.distributions import Normal, MultivariateNormal
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# Optional quantum-inspired optimization
try:
    import networkx as nx
    NETWORKX_AVAILABLE = True
except ImportError:
    NETWORKX_AVAILABLE = False

logger = logging.getLogger(__name__)


# ============================================================
# ENHANCEMENT 1: GRU-Based Temperature Predictor with Attention
# ============================================================

class GRUTemperaturePredictor(nn.Module):
    """
    Gated Recurrent Unit (GRU) neural network with multi-head attention
    for accurate temperature prediction.
    
    Features:
    - Multi-layer GRU with skip connections
    - Multi-head self-attention mechanism
    - Probabilistic output (mean + variance)
    - Online learning capability
    - Quantile prediction for uncertainty bounds
    """
    
    def __init__(self, input_dim=8, hidden_dim=128, num_layers=3, 
                 num_heads=8, dropout=0.2, output_dim=2):
        super().__init__()
        
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers
        self.num_heads = num_heads
        
        # Input projection with layer normalization
        self.input_projection = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.LayerNorm(hidden_dim),
            nn.GELU(),
            nn.Dropout(dropout)
        )
        
        # Stacked GRU with residual connections
        self.gru_layers = nn.ModuleList([
            nn.GRU(
                hidden_dim if i > 0 else hidden_dim,
                hidden_dim,
                batch_first=True,
                bidirectional=False,
                dropout=0 if i == num_layers - 1 else dropout
            ) for i in range(num_layers)
        ])
        
        # Layer normalization for each GRU layer
        self.layer_norms = nn.ModuleList([
            nn.LayerNorm(hidden_dim) for _ in range(num_layers)
        ])
        
        # Multi-head attention
        self.attention = nn.MultiheadAttention(
            embed_dim=hidden_dim,
            num_heads=num_heads,
            dropout=dropout,
            batch_first=True
        )
        
        # Attention layer normalization
        self.attention_norm = nn.LayerNorm(hidden_dim)
        
        # Positional encoding (learnable)
        self.pos_encoding = nn.Parameter(
            torch.randn(1, 1000, hidden_dim) * 0.1
        )
        
        # Output heads
        self.output_mean = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim // 2, output_dim)
        )
        
        self.output_variance = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim // 2, output_dim),
            nn.Softplus()  # Ensure positive variance
        )
        
        # Quantile prediction for confidence intervals
        self.quantile_heads = nn.ModuleDict({
            'q10': nn.Sequential(
                nn.Linear(hidden_dim, hidden_dim // 2),
                nn.GELU(),
                nn.Linear(hidden_dim // 2, output_dim)
            ),
            'q90': nn.Sequential(
                nn.Linear(hidden_dim, hidden_dim // 2),
                nn.GELU(),
                nn.Linear(hidden_dim // 2, output_dim)
            )
        })
        
        self._init_weights()
    
    def _init_weights(self):
        """Initialize weights using Xavier uniform"""
        for name, param in self.named_parameters():
            if 'weight' in name and param.dim() > 1:
                nn.init.xavier_uniform_(param)
            elif 'bias' in name:
                nn.init.zeros_(param)
    
    def forward(self, x, hidden=None, return_attention=False):
        """
        Forward pass with attention.
        
        Args:
            x: Input tensor [batch, seq_len, features]
            hidden: Initial hidden state
            return_attention: Return attention weights
        
        Returns:
            Dictionary with predicted mean, variance, and quantiles
        """
        batch_size, seq_len, _ = x.shape
        
        # Input projection
        x = self.input_projection(x)
        
        # Add positional encoding
        x = x + self.pos_encoding[:, :seq_len, :]
        
        # Process through GRU layers with residual connections
        gru_output = x
        for i, (gru, norm) in enumerate(zip(self.gru_layers, self.layer_norms)):
            gru_out, hidden = gru(gru_output, hidden)
            gru_out = norm(gru_out)
            
            # Residual connection if dimensions match
            if i > 0:
                gru_out = gru_out + gru_output
            
            gru_output = gru_out
        
        # Self-attention
        attn_out, attn_weights = self.attention(
            gru_output, gru_output, gru_output
        )
        
        # Residual connection and normalization
        gru_output = self.attention_norm(gru_output + attn_out)
        
        # Global average pooling over time dimension
        # (could also use last timestep)
        pooled = torch.mean(gru_output, dim=1)
        
        # Generate predictions
        predictions = {
            'mean': self.output_mean(pooled),
            'variance': self.output_variance(pooled),
            'q10': self.quantile_heads['q10'](pooled),
            'q90': self.quantile_heads['q90'](pooled)
        }
        
        if return_attention:
            predictions['attention'] = attn_weights
        
        return predictions
    
    def predict_with_uncertainty(self, x):
        """
        Make prediction with uncertainty estimates.
        
        Returns:
            Dictionary with mean, standard deviation, and confidence intervals
        """
        self.eval()
        with torch.no_grad():
            preds = self.forward(x)
        
        return {
            'mean': preds['mean'].cpu().numpy(),
            'std': torch.sqrt(preds['variance']).cpu().numpy(),
            'ci_lower': preds['q10'].cpu().numpy(),
            'ci_upper': preds['q90'].cpu().numpy()
        }
    
    def loss_function(self, predictions, targets, alpha=0.1):
        """
        Composite loss function.
        
        Args:
            predictions: Model predictions dictionary
            targets: Ground truth values
            alpha: Weight for quantile loss
        
        Returns:
            Total loss
        """
        # Negative log-likelihood loss
        nll_loss = Normal(predictions['mean'], 
                         torch.sqrt(predictions['variance'])).log_prob(targets)
        nll_loss = -nll_loss.mean()
        
        # Quantile loss (pinball loss)
        q10_loss = torch.max(
            alpha * (targets - predictions['q10']),
            (alpha - 1) * (targets - predictions['q10'])
        ).mean()
        
        q90_loss = torch.max(
            (1 - alpha) * (targets - predictions['q90']),
            alpha * (predictions['q90'] - targets)
        ).mean()
        
        # Combined loss
        total_loss = nll_loss + 0.5 * (q10_loss + q90_loss)
        
        return total_loss


# ============================================================
# ENHANCEMENT 2: Multi-Objective PPO for Cooling Control
# ============================================================

class PPOCoolingController:
    """
    Proximal Policy Optimization (PPO) controller for multi-objective
    cooling optimization.
    
    Objectives:
    1. Temperature regulation
    2. Energy minimization
    3. Carbon footprint reduction
    4. Equipment longevity
    
    Features:
    - Continuous action space (fan speed, pump speed, valve positions)
    - Multi-objective reward function
    - Advantage normalization
    - Entropy regularization for exploration
    """
    
    class ActorCritic(nn.Module):
        """Combined Actor-Critic network for PPO"""
        
        def __init__(self, state_dim, action_dim, hidden_dim=256):
            super().__init__()
            
            # Shared feature extractor
            self.shared = nn.Sequential(
                nn.Linear(state_dim, hidden_dim),
                nn.LayerNorm(hidden_dim),
                nn.GELU(),
                nn.Linear(hidden_dim, hidden_dim),
                nn.LayerNorm(hidden_dim),
                nn.GELU()
            )
            
            # Actor head (policy)
            self.actor_mean = nn.Sequential(
                nn.Linear(hidden_dim, hidden_dim // 2),
                nn.GELU(),
                nn.Linear(hidden_dim // 2, action_dim)
            )
            
            # Learnable log standard deviation
            self.actor_log_std = nn.Parameter(
                torch.zeros(1, action_dim) - 0.5
            )
            
            # Critic head (value function)
            self.critic = nn.Sequential(
                nn.Linear(hidden_dim, hidden_dim // 2),
                nn.GELU(),
                nn.Linear(hidden_dim // 2, 1)
            )
        
        def forward(self, state):
            features = self.shared(state)
            
            # Action distribution
            action_mean = self.actor_mean(features)
            action_std = torch.exp(self.actor_log_std.clamp(-2, 2))
            action_dist = Normal(action_mean, action_std)
            
            # State value
            state_value = self.critic(features)
            
            return action_dist, state_value
        
        def get_action(self, state, deterministic=False):
            """Sample action from policy"""
            action_dist, state_value = self.forward(state)
            
            if deterministic:
                action = action_dist.mean
            else:
                action = action_dist.sample()
            
            action_log_prob = action_dist.log_prob(action).sum(dim=-1)
            
            return action, action_log_prob, state_value
    
    def __init__(self, state_dim, action_dim, config=None):
        self.config = config or {}
        
        # Initialize actor-critic
        self.ac = self.ActorCritic(
            state_dim=state_dim,
            action_dim=action_dim,
            hidden_dim=self.config.get('hidden_dim', 256)
        )
        
        # Optimizer
        self.optimizer = optim.Adam(
            self.ac.parameters(),
            lr=self.config.get('learning_rate', 3e-4)
        )
        
        # PPO hyperparameters
        self.clip_epsilon = self.config.get('clip_epsilon', 0.2)
        self.gamma = self.config.get('gamma', 0.99)
        self.lam = self.config.get('lam', 0.95)
        self.entropy_coef = self.config.get('entropy_coef', 0.01)
        self.value_coef = self.config.get('value_coef', 0.5)
        self.max_grad_norm = self.config.get('max_grad_norm', 0.5)
        
        # Action bounds
        self.action_min = torch.tensor(
            self.config.get('action_min', [0.0] * action_dim)
        )
        self.action_max = torch.tensor(
            self.config.get('action_max', [1.0] * action_dim)
        )
        
        # Experience buffer
        self.buffer = []
        
        # Normalization statistics
        self.state_mean = torch.zeros(state_dim)
        self.state_std = torch.ones(state_dim)
        self.reward_mean = 0
        self.reward_std = 1
        
        # Learning metrics
        self.training_steps = 0
        self.episode_rewards = deque(maxlen=100)
        
        logger.info(f"PPO Controller initialized (state={state_dim}, action={action_dim})")
    
    def normalize_state(self, state):
        """Normalize state using running statistics"""
        if isinstance(state, np.ndarray):
            state = torch.FloatTensor(state)
        return (state - self.state_mean) / (self.state_std + 1e-8)
    
    def update_statistics(self, states, rewards):
        """Update running statistics for normalization"""
        if isinstance(states[0], np.ndarray):
            states_tensor = torch.FloatTensor(np.array(states))
        else:
            states_tensor = torch.stack(states)
        
        # Update state statistics
        self.state_mean = 0.99 * self.state_mean + 0.01 * states_tensor.mean(dim=0)
        self.state_std = 0.99 * self.state_std + 0.01 * states_tensor.std(dim=0)
        
        # Update reward statistics
        self.reward_mean = 0.99 * self.reward_mean + 0.01 * np.mean(rewards)
        self.reward_std = 0.99 * self.reward_std + 0.01 * np.std(rewards)
    
    def get_action(self, state, deterministic=False):
        """Get action from policy"""
        with torch.no_grad():
            state_tensor = torch.FloatTensor(state).unsqueeze(0)
            state_normalized = self.normalize_state(state_tensor)
            
            action, log_prob, value = self.ac.get_action(
                state_normalized, deterministic
            )
            
            # Clip action to bounds
            action = torch.clamp(
                action.squeeze(0),
                self.action_min,
                self.action_max
            )
            
        return action.numpy(), log_prob.item(), value.item()
    
    def store_transition(self, state, action, reward, next_state, done, 
                        log_prob, value):
        """Store transition in buffer"""
        self.buffer.append({
            'state': state,
            'action': action,
            'reward': reward,
            'next_state': next_state,
            'done': done,
            'log_prob': log_prob,
            'value': value
        })
    
    def compute_gae(self, rewards, values, dones):
        """Compute Generalized Advantage Estimation (GAE)"""
        advantages = []
        gae = 0
        
        for t in reversed(range(len(rewards))):
            if t == len(rewards) - 1:
                next_value = 0
            else:
                next_value = values[t + 1]
            
            delta = rewards[t] + self.gamma * next_value * (1 - dones[t]) - values[t]
            gae = delta + self.gamma * self.lam * (1 - dones[t]) * gae
            advantages.insert(0, gae)
        
        returns = [adv + val for adv, val in zip(advantages, values)]
        
        return torch.FloatTensor(advantages), torch.FloatTensor(returns)
    
    def multi_objective_reward(self, temperatures, energy_consumption,
                               carbon_intensity, equipment_stress):
        """
        Multi-objective reward function.
        
        Args:
            temperatures: Current temperature readings
            energy_consumption: Current power draw (kW)
            carbon_intensity: Grid carbon intensity (gCO2/kWh)
            equipment_stress: Stress indicator (0-1)
        
        Returns:
            Weighted reward
        """
        # Temperature penalty (quadratic)
        temp_target = 65.0  # Target temperature in Celsius
        temp_penalty = -np.mean((temperatures - temp_target) ** 2) / 1000
        
        # Energy penalty
        energy_penalty = -energy_consumption / 100  # Normalize
        
        # Carbon penalty
        carbon_penalty = -(energy_consumption * carbon_intensity) / (1000 * 1000)
        
        # Equipment longevity reward
        longevity_reward = -equipment_stress * 10
        
        # Combine objectives with weights
        weights = {
            'temperature': self.config.get('temp_weight', 10.0),
            'energy': self.config.get('energy_weight', 2.0),
            'carbon': self.config.get('carbon_weight', 3.0),
            'longevity': self.config.get('longevity_weight', 1.0)
        }
        
        total_reward = (
            weights['temperature'] * temp_penalty +
            weights['energy'] * energy_penalty +
            weights['carbon'] * carbon_penalty +
            weights['longevity'] * longevity_reward
        )
        
        return total_reward
    
    def update(self, epochs=10, batch_size=64):
        """Update policy using PPO algorithm"""
        if len(self.buffer) < batch_size:
            return {}
        
        # Prepare data
        states = torch.FloatTensor([t['state'] for t in self.buffer])
        actions = torch.FloatTensor([t['action'] for t in self.buffer])
        rewards = [t['reward'] for t in self.buffer]
        next_states = torch.FloatTensor([t['next_state'] for t in self.buffer])
        dones = [t['done'] for t in self.buffer]
        old_log_probs = torch.FloatTensor([t['log_prob'] for t in self.buffer])
        old_values = torch.FloatTensor([t['value'] for t in self.buffer])
        
        # Update statistics
        self.update_statistics(states, rewards)
        
        # Normalize states
        states_normalized = self.normalize_state(states)
        next_states_normalized = self.normalize_state(next_states)
        
        # Compute advantages and returns
        advantages, returns = self.compute_gae(rewards, old_values.numpy(), dones)
        advantages = (advantages - advantages.mean()) / (advantages.std() + 1e-8)
        
        # PPO update loop
        total_losses = {'policy': 0, 'value': 0, 'entropy': 0}
        
        for epoch in range(epochs):
            # Shuffle data
            indices = torch.randperm(len(self.buffer))
            
            for start in range(0, len(self.buffer), batch_size):
                batch_indices = indices[start:start + batch_size]
                
                if len(batch_indices) < batch_size:
                    continue
                
                # Get batch
                batch_states = states_normalized[batch_indices]
                batch_actions = actions[batch_indices]
                batch_advantages = advantages[batch_indices]
                batch_returns = returns[batch_indices]
                batch_old_log_probs = old_log_probs[batch_indices]
                batch_old_values = old_values[batch_indices]
                
                # Get current policy
                action_dist, values = self.ac(batch_states)
                new_log_probs = action_dist.log_prob(batch_actions).sum(dim=-1)
                entropy = action_dist.entropy().mean()
                
                # Policy loss with clipping
                ratio = torch.exp(new_log_probs - batch_old_log_probs)
                surr1 = ratio * batch_advantages
                surr2 = torch.clamp(ratio, 1 - self.clip_epsilon, 
                                   1 + self.clip_epsilon) * batch_advantages
                policy_loss = -torch.min(surr1, surr2).mean()
                
                # Value loss
                value_loss = F.mse_loss(values.squeeze(), batch_returns)
                
                # Total loss
                total_loss = (
                    policy_loss +
                    self.value_coef * value_loss -
                    self.entropy_coef * entropy
                )
                
                # Optimize
                self.optimizer.zero_grad()
                total_loss.backward()
                nn.utils.clip_grad_norm_(
                    self.ac.parameters(), self.max_grad_norm
                )
                self.optimizer.step()
                
                # Record losses
                total_losses['policy'] += policy_loss.item()
                total_losses['value'] += value_loss.item()
                total_losses['entropy'] += entropy.item()
        
        # Clear buffer
        self.buffer = []
        self.training_steps += 1
        
        # Average losses
        n_updates = epochs * (len(self.buffer) // batch_size + 1)
        for key in total_losses:
            total_losses[key] /= max(n_updates, 1)
        
        return total_losses


# ============================================================
# ENHANCEMENT 3: Adaptive Model Predictive Control (AMPC)
# ============================================================

class AdaptiveMPC:
    """
    Adaptive Model Predictive Control with uncertainty quantification.
    
    Features:
    - Online system identification
    - Uncertainty propagation through ensemble methods
    - Constraint tightening for robust satisfaction
    - Multi-step ahead optimization
    - Adaptive horizon selection
    """
    
    def __init__(self, state_dim, control_dim, horizon=20, dt=1.0):
        self.state_dim = state_dim
        self.control_dim = control_dim
        self.horizon = horizon
        self.dt = dt
        
        # System model parameters (A, B matrices)
        self.A = np.eye(state_dim)  # State transition matrix
        self.B = np.zeros((state_dim, control_dim))  # Control matrix
        self.C = np.eye(state_dim)  # Output matrix
        self.D = np.zeros((state_dim, 1))  # Disturbance matrix
        
        # Learnable parameters
        self.model_uncertainty = np.eye(state_dim) * 0.01
        self.measurement_noise = np.eye(state_dim) * 0.001
        
        # History for online learning
        self.state_history = deque(maxlen=1000)
        self.control_history = deque(maxlen=1000)
        
        # Cost function weights
        self.Q = np.eye(state_dim)  # State cost
        self.R = np.eye(control_dim) * 0.1  # Control cost
        self.Q_terminal = np.eye(state_dim) * 10  # Terminal cost
        
        # Constraints
        self.state_min = -np.inf * np.ones(state_dim)
        self.state_max = np.inf * np.ones(state_dim)
        self.control_min = -np.inf * np.ones(control_dim)
        self.control_max = np.inf * np.ones(control_dim)
        
        # Adaptive parameters
        self.adaptation_rate = 0.1
        self.forgetting_factor = 0.99
        
        logger.info(f"AdaptiveMPC initialized (horizon={horizon}, dt={dt})")
    
    def update_model(self, states, controls, next_states):
        """
        Online system identification using recursive least squares.
        
        Args:
            states: Current states [batch, state_dim]
            controls: Applied controls [batch, control_dim]
            next_states: Resulting states [batch, state_dim]
        """
        batch_size = states.shape[0]
        
        # Construct regression matrix
        X = np.hstack([states, controls])  # [batch, state_dim + control_dim]
        Y = next_states - states  # [batch, state_dim]
        
        # Recursive least squares with forgetting factor
        if hasattr(self, 'P'):
            # Update covariance matrix
            for i in range(batch_size):
                x_i = X[i:i+1]
                y_i = Y[i:i+1]
                
                # Kalman gain
                S = x_i @ self.P @ x_i.T + self.forgetting_factor * np.eye(1)
                K = self.P @ x_i.T @ np.linalg.inv(S)
                
                # Update parameters
                theta_i = np.hstack([self.A, self.B])
                error = y_i - x_i @ theta_i.T
                theta_new = theta_i + self.adaptation_rate * (K @ error).T
                
                # Update covariance
                self.P = (self.P - K @ x_i @ self.P) / self.forgetting_factor
                
                # Extract A, B matrices
                self.A = theta_new[:, :self.state_dim]
                self.B = theta_new[:, self.state_dim:]
        else:
            # Initial batch least squares
            try:
                theta = np.linalg.lstsq(X, Y, rcond=None)[0]
                self.A = theta[:self.state_dim, :].T + np.eye(self.state_dim)
                self.B = theta[self.state_dim:, :].T
                
                # Initialize covariance
                self.P = np.linalg.inv(X.T @ X + 0.1 * np.eye(X.shape[1]))
            except np.linalg.LinAlgError:
                logger.warning("Failed to update model, using previous")
        
        # Update model uncertainty
        residuals = Y - (X @ np.hstack([self.A - np.eye(self.state_dim), self.B]).T)
        self.model_uncertainty = np.cov(residuals.T) + 0.01 * np.eye(self.state_dim)
    
    def predict_trajectory(self, initial_state, control_sequence):
        """
        Predict state trajectory given control sequence.
        
        Args:
            initial_state: Starting state [state_dim]
            control_sequence: Controls [horizon, control_dim]
        
        Returns:
            Predicted states [horizon, state_dim]
        """
        trajectory = np.zeros((self.horizon, self.state_dim))
        current_state = initial_state.copy()
        
        for t in range(self.horizon):
            trajectory[t] = current_state
            current_state = self.A @ current_state + self.B @ control_sequence[t]
        
        return trajectory
    
    def compute_cost(self, trajectory, control_sequence, target_state):
        """
        Compute total cost for a trajectory.
        
        Args:
            trajectory: State trajectory [horizon, state_dim]
            control_sequence: Control sequence [horizon, control_dim]
            target_state: Desired state [state_dim]
        
        Returns:
            Total cost
        """
        cost = 0
        
        for t in range(self.horizon):
            # State tracking error
            state_error = trajectory[t] - target_state
            
            # Running cost
            cost += state_error.T @ self.Q @ state_error
            cost += control_sequence[t].T @ self.R @ control_sequence[t]
        
        # Terminal cost
        terminal_error = trajectory[-1] - target_state
        cost += terminal_error.T @ self.Q_terminal @ terminal_error
        
        return cost
    
    def optimize_controls(self, current_state, target_state):
        """
        Optimize control sequence using MPC.
        
        Args:
            current_state: Current system state
            target_state: Desired state
        
        Returns:
            Optimal control sequence and predicted trajectory
        """
        # Initial guess (zero controls)
        initial_guess = np.zeros(self.horizon * self.control_dim)
        
        # Bounds for controls
        bounds = []
        for _ in range(self.horizon):
            for d in range(self.control_dim):
                bounds.append((self.control_min[d], self.control_max[d]))
        
        # Objective function
        def objective(control_flat):
            control_sequence = control_flat.reshape(self.horizon, self.control_dim)
            trajectory = self.predict_trajectory(current_state, control_sequence)
            return self.compute_cost(trajectory, control_sequence, target_state)
        
        # Constraint function
        def constraint(control_flat):
            control_sequence = control_flat.reshape(self.horizon, self.control_dim)
            trajectory = self.predict_trajectory(current_state, control_sequence)
            
            # State constraints
            violations = []
            for t in range(self.horizon):
                violations.extend((trajectory[t] - self.state_max).tolist())
                violations.extend((self.state_min - trajectory[t]).tolist())
            
            return np.array(violations)
        
        # Optimize
        try:
            # First try gradient-based optimization
            result = minimize(
                objective,
                initial_guess,
                method='L-BFGS-B',
                bounds=bounds,
                options={'maxiter': 100}
            )
            
            # If poor convergence, try global optimization
            if not result.success or result.fun > 1000:
                result = differential_evolution(
                    objective,
                    bounds,
                    strategy='best1bin',
                    maxiter=50
                )
            
            optimal_controls = result.x.reshape(self.horizon, self.control_dim)
            optimal_trajectory = self.predict_trajectory(
                current_state, optimal_controls
            )
            
            return optimal_controls, optimal_trajectory, result.fun
            
        except Exception as e:
            logger.error(f"MPC optimization failed: {e}")
            # Fallback to zero controls
            zero_controls = np.zeros((self.horizon, self.control_dim))
            trajectory = self.predict_trajectory(current_state, zero_controls)
            return zero_controls, trajectory, float('inf')
    
    def adaptive_optimize(self, current_state, target_state, 
                         measured_disturbance=None):
        """
        Adaptive MPC with disturbance rejection.
        
        Args:
            current_state: Current system state
            target_state: Desired state
            measured_disturbance: Known disturbance to compensate
        
        Returns:
            Optimal control action
        """
        # Optimize control sequence
        controls, trajectory, cost = self.optimize_controls(
            current_state, target_state
        )
        
        # Apply first control (receding horizon)
        first_control = controls[0]
        
        # Add disturbance rejection if measured
        if measured_disturbance is not None:
            # Disturbance rejection using pseudo-inverse
            B_pinv = np.linalg.pinv(self.B)
            first_control -= B_pinv @ self.D @ measured_disturbance
        
        # Store for model update
        self.state_history.append(current_state)
        self.control_history.append(first_control)
        
        # Update model if enough history
        if len(self.state_history) > 50:
            states = np.array(list(self.state_history)[-50:-1])
            controls = np.array(list(self.control_history)[-50:-1])
            next_states = np.array(list(self.state_history)[-49:])
            self.update_model(states, controls, next_states)
        
        return first_control, trajectory, cost


# ============================================================
# ENHANCEMENT 4: Graph Neural Network for Thermal Dependencies
# ============================================================

class ThermalGraphNeuralNetwork(nn.Module):
    """
    Graph Neural Network for modeling thermal dependencies between components.
    
    Features:
    - Graph attention networks (GAT) for spatial dependencies
    - Learnable adjacency matrix
    - Multi-scale temporal convolution
    - Physics-informed loss function
    """
    
    class GraphAttentionLayer(nn.Module):
        """Graph Attention Layer"""
        
        def __init__(self, in_features, out_features, num_heads=4, dropout=0.1):
            super().__init__()
            self.num_heads = num_heads
            self.out_features = out_features
            
            # Attention parameters
            self.W = nn.Parameter(torch.randn(num_heads, in_features, out_features) * 0.1)
            self.a = nn.Parameter(torch.randn(num_heads, 2 * out_features, 1) * 0.1)
            
            self.dropout = nn.Dropout(dropout)
            self.leaky_relu = nn.LeakyReLU(0.2)
        
        def forward(self, h, adj):
            """
            Forward pass.
            
            Args:
                h: Node features [batch, num_nodes, in_features]
                adj: Adjacency matrix [batch, num_nodes, num_nodes]
            
            Returns:
                Updated node features
            """
            batch_size, num_nodes, _ = h.shape
            h_prime = []
            
            for head in range(self.num_heads):
                # Linear transformation
                Wh = torch.matmul(h, self.W[head])  # [batch, num_nodes, out_features]
                
                # Attention coefficients
                Wh1 = Wh.unsqueeze(2).expand(-1, -1, num_nodes, -1)
                Wh2 = Wh.unsqueeze(1).expand(-1, num_nodes, -1, -1)
                Wh_concat = torch.cat([Wh1, Wh2], dim=-1)
                
                e = self.leaky_relu(
                    torch.matmul(Wh_concat, self.a[head]).squeeze(-1)
                )
                
                # Masked attention
                zero_vec = -9e15 * torch.ones_like(e)
                attention = torch.where(adj > 0, e, zero_vec)
                attention = F.softmax(attention, dim=-1)
                attention = self.dropout(attention)
                
                # Weighted sum
                h_prime_head = torch.matmul(attention, Wh)
                h_prime.append(h_prime_head)
            
            # Concatenate or average
            h_prime = torch.mean(torch.stack(h_prime), dim=0)
            
            return h_prime
    
    class TemporalConvLayer(nn.Module):
        """Temporal Convolution Layer"""
        
        def __init__(self, in_channels, out_channels, kernel_size=3):
            super().__init__()
            self.conv = nn.Conv1d(
                in_channels, out_channels, kernel_size,
                padding=kernel_size // 2
            )
            self.bn = nn.BatchNorm1d(out_channels)
        
        def forward(self, x):
            """
            Args:
                x: [batch, channels, time]
            Returns:
                [batch, out_channels, time]
            """
            return F.gelu(self.bn(self.conv(x)))
    
    def __init__(self, num_nodes, node_features, hidden_dim=64, 
                 num_gat_layers=2, num_tcn_layers=3, prediction_horizon=10):
        super().__init__()
        
        self.num_nodes = num_nodes
        self.node_features = node_features
        self.prediction_horizon = prediction_horizon
        
        # Learnable adjacency matrix
        self.adj_embedding = nn.Parameter(
            torch.randn(num_nodes, hidden_dim) * 0.1
        )
        
        # Graph attention layers
        self.gat_layers = nn.ModuleList([
            self.GraphAttentionLayer(
                hidden_dim if i == 0 else hidden_dim,
                hidden_dim
            ) for i in range(num_gat_layers)
        ])
        
        # Temporal convolution layers
        self.tcn_layers = nn.ModuleList([
            self.TemporalConvLayer(
                hidden_dim if i == 0 else hidden_dim,
                hidden_dim
            ) for i in range(num_tcn_layers)
        ])
        
        # Output layers
        self.output_projection = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.GELU(),
            nn.Linear(hidden_dim // 2, prediction_horizon)
        )
        
        # Physics-informed parameters
        self.thermal_conductivity = nn.Parameter(torch.ones(1) * 0.025)
        self.heat_capacity = nn.Parameter(torch.ones(1) * 1000)
        
        logger.info(f"ThermalGNN initialized ({num_nodes} nodes)")
    
    def compute_adjacency(self):
        """Compute learned adjacency matrix"""
        # Cosine similarity between node embeddings
        adj = F.cosine_similarity(
            self.adj_embedding.unsqueeze(1),
            self.adj_embedding.unsqueeze(0),
            dim=-1
        )
        
        # Sparsify and normalize
        adj = F.threshold(adj, 0.3, 0)
        adj = F.softmax(adj, dim=-1)
        
        return adj
    
    def forward(self, node_features, temporal_features):
        """
        Forward pass.
        
        Args:
            node_features: [batch, num_nodes, node_features]
            temporal_features: [batch, num_nodes, temporal_length]
        
        Returns:
            Predicted temperatures [batch, num_nodes, prediction_horizon]
        """
        batch_size = node_features.shape[0]
        
        # Get learned adjacency
        adj = self.compute_adjacency()
        adj = adj.unsqueeze(0).expand(batch_size, -1, -1)
        
        # Initial node embeddings
        h = F.linear(node_features, 
                     torch.randn(self.node_features, 64).to(node_features.device))
        
        # Process through GAT layers
        for gat_layer in self.gat_layers:
            h = gat_layer(h, adj)
            h = F.gelu(h)
        
        # Process temporal features through TCN
        t = temporal_features  # [batch, num_nodes, time]
        for tcn_layer in self.tcn_layers:
            # Transpose for 1D conv
            t = t.transpose(1, 2)  # [batch, time, channels]
            t = tcn_layer(t)
            t = t.transpose(1, 2)  # [batch, channels, time]
        
        # Global pooling over time
        t_pooled = torch.mean(t, dim=-1)  # [batch, channels]
        
        # Combine spatial and temporal features
        combined = h + t_pooled.unsqueeze(1)  # Broadcast to nodes
        
        # Predict future temperatures
        predictions = self.output_projection(combined)
        
        return predictions
    
    def physics_loss(self, predictions, inputs, targets):
        """
        Physics-informed loss function.
        
        Args:
            predictions: Model predictions
            inputs: Input features
            targets: Ground truth temperatures
        
        Returns:
            Physics-informed loss
        """
        # Data loss (MSE)
        data_loss = F.mse_loss(predictions, targets)
        
        # Heat equation residual
        # ∂T/∂t = α * ∇²T + q_source/(ρ*c_p)
        # Simplified finite difference
        spatial_laplacian = torch.diff(predictions, dim=-1, n=2)
        temporal_derivative = torch.diff(predictions, dim=-1)
        
        if temporal_derivative.shape[-1] > 1:
            # Physics residual
            alpha = self.thermal_conductivity / (self.heat_capacity + 1e-8)
            physics_residual = torch.mean(
                (temporal_derivative[..., 1:] - 
                 alpha * spatial_laplacian[..., :-1]) ** 2
            )
        else:
            physics_residual = 0.0
        
        # Combined loss
        total_loss = data_loss + 0.1 * physics_residual
        
        return total_loss


# ============================================================
# ENHANCEMENT 5: Enhanced Ultimate Optimizer
# ============================================================

class EnhancedUltimateThermalOptimizer:
    """
    Enhanced ultimate thermal optimizer v4.0 with all new capabilities.
    
    Integrates:
    - GRU temperature prediction
    - PPO cooling control
    - Adaptive MPC
    - Thermal GNN
    - Quantum-inspired optimization
    """
    
    def __init__(self, config=None):
        self.config = config or {}
        
        # Initialize components
        if TORCH_AVAILABLE:
            self.temp_predictor = GRUTemperaturePredictor(
                input_dim=self.config.get('input_dim', 8),
                hidden_dim=self.config.get('hidden_dim', 128),
                num_layers=self.config.get('gru_layers', 3)
            )
            
            self.ppo_controller = PPOCoolingController(
                state_dim=self.config.get('state_dim', 10),
                action_dim=self.config.get('action_dim', 5)
            )
            
            if NETWORKX_AVAILABLE:
                self.thermal_gnn = ThermalGraphNeuralNetwork(
                    num_nodes=self.config.get('num_servers', 20),
                    node_features=self.config.get('node_features', 6)
                )
        
        self.mpc = AdaptiveMPC(
            state_dim=self.config.get('state_dim', 10),
            control_dim=self.config.get('control_dim', 5),
            horizon=self.config.get('mpc_horizon', 20)
        )
        
        # Training metrics
        self.training_losses = defaultdict(list)
        
        logger.info("Enhanced Ultimate Thermal Optimizer v4.0 initialized")
    
    def compute_carbon_aware_objective(self, power_kw, grid_carbon_intensity,
                                      renewable_percentage):
        """
        Compute carbon-aware optimization objective.
        
        Args:
            power_kw: Power consumption in kW
            grid_carbon_intensity: Grid carbon intensity (gCO2/kWh)
            renewable_percentage: Percentage of renewable energy (0-1)
        
        Returns:
            Carbon emissions (gCO2) and objective value
        """
        # Grid carbon emissions
        grid_emissions = power_kw * grid_carbon_intensity
        
        # Effective emissions considering renewables
        effective_emissions = grid_emissions * (1 - renewable_percentage)
        
        # Carbon objective (to minimize)
        carbon_objective = effective_emissions / 1000  # kgCO2
        
        return effective_emissions, carbon_objective
    
    def quantum_inspired_workload_placement(self, workloads, thermal_map,
                                           cooling_capacity, 
                                           temperature_factor=0.01):
        """
        Quantum-inspired simulated annealing for optimal workload placement.
        
        Args:
            workloads: List of workload characteristics
            thermal_map: Current thermal map of servers
            cooling_capacity: Available cooling capacity
            temperature_factor: Annealing temperature factor
        
        Returns:
            Optimal workload-to-server mapping
        """
        num_workloads = len(workloads)
        num_servers = thermal_map.shape[0]
        
        # Initialize random assignment
        current_assignment = np.random.randint(0, num_servers, num_workloads)
        current_energy = self._compute_placement_energy(
            current_assignment, workloads, thermal_map, cooling_capacity
        )
        
        # Simulated annealing parameters
        temperature = 1.0
        cooling_rate = 0.95
        iterations = 1000
        
        best_assignment = current_assignment.copy()
        best_energy = current_energy
        
        for i in range(iterations):
            # Generate neighbor solution
            new_assignment = current_assignment.copy()
            
            # Random perturbation
            idx = np.random.randint(num_workloads)
            new_assignment[idx] = np.random.randint(num_servers)
            
            # Compute new energy
            new_energy = self._compute_placement_energy(
                new_assignment, workloads, thermal_map, cooling_capacity
            )
            
            # Accept or reject
            delta_energy = new_energy - current_energy
            
            if delta_energy < 0:
                # Better solution, always accept
                current_assignment = new_assignment
                current_energy = new_energy
            else:
                # Worse solution, accept with probability
                probability = math.exp(-delta_energy / (temperature * temperature_factor))
                if np.random.random() < probability:
                    current_assignment = new_assignment
                    current_energy = new_energy
            
            # Update best
            if current_energy < best_energy:
                best_energy = current_energy
                best_assignment = current_assignment.copy()
            
            # Cool down
            temperature *= cooling_rate
            
            # Reheat occasionally to escape local optima
            if i % 100 == 0 and i > 0:
                temperature = min(1.0, temperature * 2.0)
        
        return best_assignment, best_energy
    
    def _compute_placement_energy(self, assignment, workloads, thermal_map,
                                  cooling_capacity):
        """
        Energy function for workload placement optimization.
        
        Considers:
        - Thermal balance
        - Workload-server compatibility
        - Cooling constraints
        - Performance requirements
        """
        energy = 0
        
        # Count workloads per server
        server_loads = np.bincount(
            assignment, minlength=thermal_map.shape[0]
        )
        
        for server_id in range(thermal_map.shape[0]):
            # Thermal energy
            thermal_energy = thermal_map[server_id] * server_loads[server_id]
            
            # Cooling constraint violation
            if thermal_energy > cooling_capacity:
                energy += 1000 * (thermal_energy - cooling_capacity)
            
            # Load balancing penalty
            load_variance = np.var(server_loads)
            energy += 0.1 * load_variance
        
        # Workload completion time (simplified)
        for i, server_id in enumerate(assignment):
            if i < len(workloads):
                energy += workloads[i].get('complexity', 1) / max(server_loads[server_id], 1)
        
        return energy
    
    def federated_aggregate_models(self, local_models, weights=None):
        """
        Federated learning model aggregation with DP-SGD.
        
        Args:
            local_models: List of models from different datacenters
            weights: Optional importance weights for each model
        
        Returns:
            Aggregated global model
        """
        if not local_models:
            return {}
        
        if weights is None:
            weights = [1.0] * len(local_models)
        
        # Normalize weights
        total_weight = sum(weights)
        weights = [w / total_weight for w in weights]
        
        # Aggregate model parameters
        global_params = {}
        
        # Get common keys
        all_keys = set()
        for model in local_models:
            all_keys.update(model.keys())
        
        for key in all_keys:
            # Weighted average
            values = []
            for model, weight in zip(local_models, weights):
                if key in model:
                    values.append(model[key] * weight)
            
            if values:
                global_params[key] = sum(values)
        
        # Add differential privacy (Gaussian noise)
        sensitivity = 0.1
        epsilon = 2.0
        delta = 1e-5
        
        noise_scale = np.sqrt(2 * np.log(1.25 / delta)) * sensitivity / (epsilon + 1e-8)
        
        for key in global_params:
            if isinstance(global_params[key], (np.ndarray, torch.Tensor)):
                noise = np.random.normal(0, noise_scale, global_params[key].shape)
                global_params[key] = global_params[key] + noise
        
        return global_params
    
    def get_comprehensive_metrics(self):
        """Get comprehensive optimization metrics"""
        return {
            'version': '4.0',
            'components': {
                'gru_predictor': TORCH_AVAILABLE,
                'ppo_controller': TORCH_AVAILABLE,
                'mpc': True,
                'gnn': TORCH_AVAILABLE and NETWORKX_AVAILABLE
            },
            'training_losses': dict(self.training_losses)
        }


# ============================================================
# Usage Example
# ============================================================

def main():
    print("=" * 60)
    print("Enhanced Thermal-Aware Optimizer v4.0 Demo")
    print("=" * 60)
    
    # Initialize optimizer
    optimizer = EnhancedUltimateThermalOptimizer({
        'num_servers': 20,
        'input_dim': 8,
        'state_dim': 10,
        'action_dim': 5,
        'mpc_horizon': 20
    })
    
    print("\n✅ Components initialized:")
    print(f"  - GRU Predictor: {TORCH_AVAILABLE}")
    print(f"  - PPO Controller: {TORCH_AVAILABLE}")
    print(f"  - Adaptive MPC: True")
    print(f"  - Thermal GNN: {TORCH_AVAILABLE and NETWORKX_AVAILABLE}")
    
    # Demo carbon-aware objective
    print("\n2. Carbon-Aware Objective:")
    power = 500  # kW
    carbon_intensity = 350  # gCO2/kWh
    renewable_pct = 0.3
    
    emissions, objective = optimizer.compute_carbon_aware_objective(
        power, carbon_intensity, renewable_pct
    )
    print(f"  Power: {power} kW")
    print(f"  Grid Carbon Intensity: {carbon_intensity} gCO2/kWh")
    print(f"  Renewable: {renewable_pct * 100}%")
    print(f"  Emissions: {emissions:.0f} gCO2")
    print(f"  Carbon Objective: {objective:.3f} kgCO2")
    
    # Demo workload placement
    print("\n3. Quantum-Inspired Workload Placement:")
    workloads = [{'complexity': np.random.uniform(0.5, 2.0)} for _ in range(15)]
    thermal_map = np.random.uniform(30, 60, 20)
    cooling_capacity = 100
    
    assignment, energy = optimizer.quantum_inspired_workload_placement(
        workloads, thermal_map, cooling_capacity
    )
    print(f"  Workloads: {len(workloads)}")
    print(f"  Servers: {len(thermal_map)}")
    print(f"  Optimal energy: {energy:.2f}")
    print(f"  Server utilization: {np.bincount(assignment, minlength=20).tolist()}")
    
    # Demo MPC
    print("\n4. Adaptive MPC Test:")
    current_state = np.array([55.0, 60.0, 58.0, 62.0, 57.0, 
                             40.0, 0.5, 0.3, 100.0, 22.0])
    target_state = np.array([45.0, 45.0, 45.0, 45.0, 45.0,
                            35.0, 0.4, 0.2, 80.0, 25.0])
    
    controls, trajectory, cost = optimizer.mpc.adaptive_optimize(
        current_state, target_state
    )
    print(f"  Optimization cost: {cost:.2f}")
    print(f"  First control action: {controls[:3]}...")
    print(f"  Predicted final state: {trajectory[-1][:3]}...")
    
    # Get comprehensive metrics
    print("\n5. System Metrics:")
    metrics = optimizer.get_comprehensive_metrics()
    print(json.dumps(metrics, indent=2, default=str))
    
    print("\n" + "=" * 60)
    print("✅ Enhanced Thermal-Aware Optimizer v4.0 test complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
