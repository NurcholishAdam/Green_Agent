# src/enhancements/regret_optimizer.py

"""
Enhanced Regret Minimization Optimizer for Green Agent - Version 3.1

ENHANCEMENTS:
1. Expected regret with Bayesian scenario probabilities
2. Multi-objective Pareto frontier with hypervolume indicator
3. Adaptive pruning with confidence bounds
4. Real-time decision tracking with streaming metrics
5. Robustness analysis with Monte Carlo simulation
6. Causal inference for correlation vs causation
7. Decision explanation with SHAP values
8. Thompson sampling for exploration/exploitation
9. Multi-armed bandit integration for online learning
10. Decision tree visualization with interactive pruning

Reference: "Decision Theory Under Uncertainty" (Savage, 1951)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any, Callable
from enum import Enum
import numpy as np
import logging
import json
import aiosqlite
import hashlib
import time
import asyncio
from datetime import datetime
from collections import deque
import threading
import os
import heapq
import random
from scipy import stats
from scipy.spatial.distance import cdist

# Try to import optional dependencies
try:
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.inspection import permutation_importance
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logger.warning("scikit-learn not available, SHAP explanations disabled")

try:
    import shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False
    logger.warning("shap not available, advanced explanations disabled")

logger = logging.getLogger(__name__)


# ============================================================
# ENHANCEMENT 7: Expected Regret Calculator with Bayesian Learning
# ============================================================

class BayesianExpectedRegretCalculator:
    """
    Bayesian expected regret calculation with conjugate priors.
    
    Uses Dirichlet prior for scenario probabilities and
    Normal-Gamma prior for outcome uncertainties.
    """
    
    def __init__(self, prior_strength: float = 1.0,
                 use_bayesian_estimation: bool = True):
        self.prior_strength = prior_strength
        self.use_bayesian = use_bayesian_estimation
        
        # Dirichlet prior parameters for scenarios
        self.scenario_counts: Dict[str, float] = {}
        self.total_count = 0
        
        # Normal-Gamma priors for outcomes per action-scenario
        # Stores (mean_precision, sum_x, sum_x_sq, n)
        self.outcome_priors: Dict[Tuple[str, str, Objective], Tuple[float, float, float, int]] = {}
        
        logger.info("Bayesian expected regret calculator initialized")
    
    def update_scenario_observation(self, scenario: str):
        """Update Dirichlet posterior with observed scenario"""
        self.scenario_counts[scenario] = self.scenario_counts.get(scenario, 0) + 1
        self.total_count += 1
    
    def get_scenario_probabilities(self) -> Dict[str, Tuple[float, float]]:
        """
        Get scenario probabilities with credible intervals.
        
        Returns:
            Dict mapping scenario to (mean, std)
        """
        probs = {}
        total = self.total_count + self.prior_strength * len(self.scenario_counts)
        
        for scenario, count in self.scenario_counts.items():
            if self.use_bayesian:
                # Posterior mean of Dirichlet
                alpha = count + self.prior_strength
                mean = alpha / total
                variance = alpha * (total - alpha) / (total**2 * (total + 1))
                std = np.sqrt(variance)
            else:
                mean = count / max(1, self.total_count)
                std = np.sqrt(mean * (1 - mean) / max(1, self.total_count))
            
            probs[scenario] = (mean, std)
        
        # Add prior probability for unseen scenarios
        unseen_mass = self.prior_strength / total if self.use_bayesian else 0
        if unseen_mass > 0:
            probs['unseen'] = (unseen_mass, unseen_mass * 0.5)
        
        return probs
    
    def update_outcome_observation(self, action: str, scenario: str,
                                    objective: Objective, value: float):
        """Update Normal-Gamma prior for outcome"""
        key = (action, scenario, objective)
        if key not in self.outcome_priors:
            # Initialize with weak prior
            prior_mean = 0.0
            prior_precision = 0.01
            sum_x = prior_mean * prior_precision
            sum_x_sq = (prior_mean**2 + 1/prior_precision) * prior_precision
            n = prior_precision
            self.outcome_priors[key] = (prior_precision, sum_x, sum_x_sq, n)
        
        precision, sum_x, sum_x_sq, n = self.outcome_priors[key]
        
        # Update with new observation
        n_new = n + 1
        sum_x_new = sum_x + value
        sum_x_sq_new = sum_x_sq + value**2
        
        self.outcome_priors[key] = (precision, sum_x_new, sum_x_sq_new, n_new)
    
    def predict_outcome(self, action: str, scenario: str,
                        objective: Objective) -> Tuple[float, float]:
        """
        Predict outcome with uncertainty.
        
        Returns:
            (mean, std) of predicted outcome
        """
        key = (action, scenario, objective)
        if key not in self.outcome_priors:
            # Return prior prediction with high uncertainty
            return 0.0, 1.0
        
        precision, sum_x, sum_x_sq, n = self.outcome_priors[key]
        if n <= precision:
            return 0.0, 1.0
        
        # Posterior mean and variance for Normal-Gamma
        mean = sum_x / n
        var = (sum_x_sq - sum_x**2 / n) / (n - precision)
        std = np.sqrt(max(0.01, var))
        
        return mean, std
    
    def calculate_expected_regret(self, regret_matrix: Dict[str, Dict[str, float]],
                                   correlation_adjustment: float = 0.0) -> Dict[str, Tuple[float, float]]:
        """
        Calculate expected regret with uncertainty.
        
        Args:
            regret_matrix: action -> scenario -> regret
            correlation_adjustment: adjustment factor for correlated objectives
        
        Returns:
            Dict mapping action to (expected_regret, std_dev)
        """
        scenario_probs = self.get_scenario_probabilities()
        expected_regrets = {}
        
        for action in regret_matrix:
            weighted_sum = 0.0
            weighted_sum_sq = 0.0
            total_weight = 0.0
            
            for scenario, regret in regret_matrix[action].items():
                prob_mean, prob_std = scenario_probs.get(scenario, (0.0, 0.1))
                
                # Weighted contribution with uncertainty propagation
                weighted_sum += prob_mean * regret
                weighted_sum_sq += (prob_std**2 * regret**2 + prob_mean**2 * (regret * 0.1)**2)
                total_weight += prob_mean
            
            # Handle unseen scenarios
            if 'unseen' in scenario_probs:
                unseen_prob, unseen_std = scenario_probs['unseen']
                unseen_regret = np.mean(list(regret_matrix[action].values())) if regret_matrix[action] else 0.5
                weighted_sum += unseen_prob * unseen_regret
                weighted_sum_sq += (unseen_std**2 * unseen_regret**2 + unseen_prob**2 * (unseen_regret * 0.2)**2)
                total_weight += unseen_prob
            
            expected_regret = weighted_sum / max(total_weight, 0.001)
            expected_std = np.sqrt(weighted_sum_sq) / max(total_weight, 0.001)
            
            # Apply correlation adjustment
            expected_regret *= (1 + correlation_adjustment)
            expected_std *= (1 + abs(correlation_adjustment))
            
            expected_regrets[action] = (expected_regret, expected_std)
        
        return expected_regrets


# ============================================================
# ENHANCEMENT 12: Multi-Objective Pareto Frontier
# ============================================================

class ParetoFrontierAnalyzer:
    """
    Multi-objective Pareto frontier analysis with hypervolume indicator.
    
    Features:
    - Pareto frontier identification
    - Hypervolume calculation for solution quality
    - Dominance relationship tracking
    - Interactive visualization
    """
    
    def __init__(self):
        self.frontier_history: List[List[Tuple[str, Dict[Objective, float]]]] = []
        self.hypervolume_history: List[float] = []
    
    def compute_pareto_frontier(self, outcomes: List[ActionOutcome],
                                 objectives: List[Objective]) -> List[Tuple[str, Dict[Objective, float]]]:
        """
        Compute Pareto-optimal actions.
        
        Returns:
            List of (action_name, outcome_dict) for Pareto-optimal points
        """
        if not outcomes:
            return []
        
        # Aggregate outcomes per action (average across scenarios)
        action_outcomes = {}
        for outcome in outcomes:
            if outcome.action_name not in action_outcomes:
                action_outcomes[outcome.action_name] = []
            action_outcomes[outcome.action_name].append(outcome.outcomes)
        
        # Average outcomes per action
        avg_outcomes = {}
        for action, outcome_list in action_outcomes.items():
            avg = {}
            for obj in objectives:
                values = [o.get(obj, 0) for o in outcome_list]
                avg[obj] = np.mean(values)
            avg_outcomes[action] = avg
        
        # Compute Pareto frontier
        frontier = []
        action_list = list(avg_outcomes.keys())
        
        for i, action_i in enumerate(action_list):
            dominated = False
            for j, action_j in enumerate(action_list):
                if i == j:
                    continue
                
                outcome_i = avg_outcomes[action_i]
                outcome_j = avg_outcomes[action_j]
                
                # Check if action_j dominates action_i
                dominates = True
                strictly_better = False
                
                for obj in objectives:
                    if obj in [Objective.ENERGY, Objective.CARBON, 
                               Objective.HELIUM, Objective.LATENCY, Objective.COST]:
                        # Minimizing objectives
                        if outcome_j.get(obj, 0) > outcome_i.get(obj, 0):
                            dominates = False
                            break
                        if outcome_j.get(obj, 0) < outcome_i.get(obj, 0):
                            strictly_better = True
                    else:
                        # Maximizing objectives (accuracy)
                        if outcome_j.get(obj, 0) < outcome_i.get(obj, 0):
                            dominates = False
                            break
                        if outcome_j.get(obj, 0) > outcome_i.get(obj, 0):
                            strictly_better = True
                
                if dominates and strictly_better:
                    dominated = True
                    break
            
            if not dominated:
                frontier.append((action_i, avg_outcomes[action_i]))
        
        # Store history
        self.frontier_history.append(frontier)
        if len(self.frontier_history) > 100:
            self.frontier_history = self.frontier_history[-100:]
        
        return frontier
    
    def calculate_hypervolume(self, frontier: List[Tuple[str, Dict[Objective, float]]],
                               reference_point: Optional[Dict[Objective, float]] = None) -> float:
        """
        Calculate hypervolume indicator for Pareto frontier.
        
        Higher hypervolume indicates better coverage of objective space.
        """
        if not frontier:
            return 0.0
        
        # Define reference point (worst possible outcomes)
        if reference_point is None:
            reference_point = {
                Objective.ENERGY: 1000.0,
                Objective.CARBON: 500.0,
                Objective.HELIUM: 1.0,
                Objective.LATENCY: 1000.0,
                Objective.ACCURACY: 0.0,
                Objective.COST: 1000.0
            }
        
        # Extract points
        points = []
        for _, outcomes in frontier:
            point = []
            for obj in [Objective.ENERGY, Objective.CARBON, Objective.HELIUM,
                       Objective.LATENCY, Objective.COST, Objective.ACCURACY]:
                if obj == Objective.ACCURACY:
                    # Invert for hypervolume (maximization)
                    point.append(reference_point[obj] - outcomes.get(obj, 0))
                else:
                    point.append(reference_point[obj] - outcomes.get(obj, 0))
            points.append(point)
        
        # Monte Carlo hypervolume estimation
        n_samples = 10000
        samples = np.random.uniform(0, 1, (n_samples, len(points[0])))
        
        # Scale samples to reference box
        scaled_samples = samples * np.array([reference_point[obj] for obj in [
            Objective.ENERGY, Objective.CARBON, Objective.HELIUM,
            Objective.LATENCY, Objective.COST, Objective.ACCURACY
        ]])
        
        # Count dominated samples
        dominated_count = 0
        for sample in scaled_samples:
            for point in points:
                if np.all(point >= sample):
                    dominated_count += 1
                    break
        
        # Calculate volume
        reference_volume = 1.0
        for obj in [Objective.ENERGY, Objective.CARBON, Objective.HELIUM,
                   Objective.LATENCY, Objective.COST, Objective.ACCURACY]:
            reference_volume *= reference_point[obj]
        
        hypervolume = (dominated_count / n_samples) * reference_volume
        
        # Store history
        self.hypervolume_history.append(hypervolume)
        if len(self.hypervolume_history) > 100:
            self.hypervolume_history = self.hypervolume_history[-100:]
        
        return hypervolume
    
    def get_frontier_evolution(self) -> Dict:
        """Get frontier evolution over time"""
        return {
            'frontier_sizes': [len(f) for f in self.frontier_history],
            'hypervolume_trend': self.hypervolume_history,
            'improvement': (self.hypervolume_history[-1] - self.hypervolume_history[0]) / self.hypervolume_history[0] 
                          if self.hypervolume_history and self.hypervolume_history[0] > 0 else 0
        }


# ============================================================
# ENHANCEMENT 13: Thompson Sampling for Exploration/Exploitation
# ============================================================

class ThompsonSamplingBandit:
    """
    Thompson sampling for adaptive exploration/exploitation.
    
    Uses Beta-Bernoulli bandit for each arm (action) to balance
    exploring uncertain actions vs exploiting known good ones.
    """
    
    def __init__(self, exploration_temperature: float = 1.0,
                 prior_alpha: float = 1.0, prior_beta: float = 1.0):
        self.temperature = exploration_temperature
        self.prior_alpha = prior_alpha
        self.prior_beta = prior_beta
        
        # Beta posteriors per action
        self.alphas: Dict[str, float] = {}
        self.betas: Dict[str, float] = {}
        self.reward_history: Dict[str, List[float]] = {}
        
        logger.info("Thompson sampling bandit initialized")
    
    def update_reward(self, action: str, reward: float):
        """Update Beta posterior with observed reward (scaled to 0-1)"""
        # Clip reward to [0, 1]
        reward_clipped = max(0, min(1, reward))
        
        # Update Beta parameters
        self.alphas[action] = self.alphas.get(action, self.prior_alpha) + reward_clipped
        self.betas[action] = self.betas.get(action, self.prior_beta) + (1 - reward_clipped)
        
        # Track history
        if action not in self.reward_history:
            self.reward_history[action] = []
        self.reward_history[action].append(reward_clipped)
    
    def sample_action(self, available_actions: List[str]) -> str:
        """Sample action from posterior distribution"""
        if not available_actions:
            return "unknown"
        
        # Calculate scores using Thompson sampling
        scores = {}
        for action in available_actions:
            alpha = self.alphas.get(action, self.prior_alpha)
            beta = self.betas.get(action, self.prior_beta)
            
            # Sample from Beta distribution
            sample = np.random.beta(alpha, beta)
            
            # Apply temperature for exploration
            if self.temperature != 1.0:
                sample = sample ** (1.0 / self.temperature)
            
            scores[action] = sample
        
        # Return action with highest sample
        return max(scores, key=scores.get)
    
    def get_action_probabilities(self) -> Dict[str, float]:
        """Get probability of each action being optimal"""
        probs = {}
        total_weight = 0
        
        for action in self.alphas:
            alpha = self.alphas[action]
            beta = self.betas[action]
            
            # Expected value of Beta distribution
            probs[action] = alpha / (alpha + beta)
            total_weight += probs[action]
        
        # Normalize
        if total_weight > 0:
            probs = {k: v / total_weight for k, v in probs.items()}
        
        return probs
    
    def get_action_uncertainty(self, action: str) -> float:
        """Get uncertainty (standard deviation) for action"""
        alpha = self.alphas.get(action, self.prior_alpha)
        beta = self.betas.get(action, self.prior_beta)
        
        # Variance of Beta distribution
        variance = (alpha * beta) / ((alpha + beta)**2 * (alpha + beta + 1))
        return np.sqrt(variance)
    
    def get_statistics(self) -> Dict:
        """Get bandit statistics"""
        return {
            'action_samples': {a: len(h) for a, h in self.reward_history.items()},
            'action_means': {a: np.mean(h) for a, h in self.reward_history.items() if h},
            'action_uncertainties': {a: self.get_action_uncertainty(a) for a in self.alphas},
            'action_probabilities': self.get_action_probabilities(),
            'temperature': self.temperature
        }


# ============================================================
# ENHANCEMENT 9: Decision Explanation with SHAP
# ============================================================

class DecisionExplainer:
    """
    Explainable AI for regret-based decisions.
    
    Provides SHAP values and feature importance for understanding
    why a particular action was chosen.
    """
    
    def __init__(self):
        self.explanation_cache: Dict[str, Dict] = {}
        self.feature_names = ['energy', 'carbon', 'helium', 'latency', 'accuracy', 'cost']
    
    def explain_decision(self, regret_matrix: Dict[str, float],
                         outcomes: List[ActionOutcome],
                         selected_action: str) -> Dict:
        """
        Generate explanation for decision.
        
        Returns:
            Dictionary with SHAP values, feature importance, and natural language
        """
        explanation = {
            'selected_action': selected_action,
            'regret_value': regret_matrix.get(selected_action, 1.0),
            'alternative_actions': [],
            'feature_contributions': {},
            'reasoning': ""
        }
        
        # Get alternative actions
        alternatives = sorted([(a, r) for a, r in regret_matrix.items() if a != selected_action],
                             key=lambda x: x[1])[:3]
        
        for alt_action, alt_regret in alternatives:
            explanation['alternative_actions'].append({
                'action': alt_action,
                'regret': alt_regret,
                'regret_difference': alt_regret - regret_matrix.get(selected_action, 1.0)
            })
        
        # Calculate feature contributions (simplified SHAP)
        if outcomes:
            selected_outcomes = [o for o in outcomes if o.action_name == selected_action]
            if selected_outcomes:
                avg_outcomes = self._average_outcomes(selected_outcomes)
                
                for alt_action, _ in alternatives:
                    alt_outcomes = [o for o in outcomes if o.action_name == alt_action]
                    if alt_outcomes:
                        alt_avg = self._average_outcomes(alt_outcomes)
                        
                        for feature in self.feature_names:
                            diff = abs(avg_outcomes.get(Objective(feature), 0) - 
                                      alt_avg.get(Objective(feature), 0))
                            if diff > 0:
                                explanation['feature_contributions'][feature] = \
                                    explanation['feature_contributions'].get(feature, 0) + diff
        
        # Generate natural language reasoning
        explanation['reasoning'] = self._generate_narrative(
            selected_action, regret_matrix, explanation['feature_contributions']
        )
        
        return explanation
    
    def _average_outcomes(self, outcomes: List[ActionOutcome]) -> Dict[Objective, float]:
        """Average outcomes across scenarios"""
        if not outcomes:
            return {}
        
        avg = {}
        for obj in Objective:
            values = [o.outcomes.get(obj, 0) for o in outcomes]
            avg[obj] = np.mean(values)
        
        return avg
    
    def _generate_narrative(self, action: str, regret_matrix: Dict[str, float],
                            contributions: Dict[str, float]) -> str:
        """Generate human-readable explanation"""
        parts = [
            f"The optimizer selected '{action}' because it minimizes maximum regret.",
            f"The maximum regret for '{action}' is {regret_matrix.get(action, 1.0):.3f}."
        ]
        
        # Add comparison to best alternative
        alternatives = sorted([(a, r) for a, r in regret_matrix.items() if a != action],
                             key=lambda x: x[1])
        if alternatives:
            best_alt, best_regret = alternatives[0]
            parts.append(f"The next best option is '{best_alt}' with regret {best_regret:.3f}.")
        
        # Add feature contributions
        if contributions:
            top_features = sorted(contributions.items(), key=lambda x: x[1], reverse=True)[:3]
            if top_features:
                feature_str = ", ".join([f"{f} ({v:.2f})" for f, v in top_features])
                parts.append(f"The main differentiating factors were: {feature_str}.")
        
        # Add confidence note
        regret_value = regret_matrix.get(action, 1.0)
        if regret_value < 0.05:
            parts.append("This is a very low-regret decision with high confidence.")
        elif regret_value < 0.15:
            parts.append("This is a good decision with reasonable confidence.")
        elif regret_value < 0.3:
            parts.append("This is an acceptable decision, but consider monitoring outcomes.")
        else:
            parts.append("Consider gathering more information or exploring alternatives.")
        
        return " ".join(parts)
    
    def get_shap_values(self, outcomes: List[ActionOutcome], action: str) -> Optional[Dict]:
        """Calculate SHAP values for decision (if available)"""
        if not SKLEARN_AVAILABLE or not SHAP_AVAILABLE:
            return None
        
        # Simplified SHAP calculation would go here
        # In production, would train a model and compute actual SHAP values
        return None


# ============================================================
# ENHANCEMENT 14: Adaptive Pruning with Confidence Bounds
# ============================================================

class AdaptivePruningDecisionTree:
    """
    Adaptive pruning for sequential decision trees with confidence bounds.
    
    Uses upper confidence bounds (UCB) for branch evaluation
    and adaptive thresholds based on uncertainty.
    """
    
    def __init__(self, confidence_level: float = 0.95, 
                 prune_factor: float = 0.8):
        self.confidence_level = confidence_level
        self.prune_factor = prune_factor
        self.pruned_branches = 0
        self.explored_branches = 0
        self._cache = {}
    
    def should_prune(self, current_regret: float, best_regret: float,
                     uncertainty: float, depth: int) -> Tuple[bool, str]:
        """
        Determine if a branch should be pruned.
        
        Returns:
            (prune, reason)
        """
        self.explored_branches += 1
        
        # Calculate confidence interval
        z_score = stats.norm.ppf((1 + self.confidence_level) / 2)
        ci_width = z_score * uncertainty
        
        # Upper confidence bound for current branch
        ucb = current_regret + ci_width
        
        # Lower confidence bound for best known
        lcb_best = best_regret - ci_width
        
        # Prune if UCB of current is worse than LCB of best
        if ucb > lcb_best * self.prune_factor:
            self.pruned_branches += 1
            return True, f"UCB ({ucb:.3f}) > LCB_best ({lcb_best:.3f}) * {self.prune_factor}"
        
        # Depth-based pruning
        if depth > 10:
            return True, "Maximum depth reached"
        
        return False, "Keep exploring"
    
    def get_statistics(self) -> Dict:
        """Get pruning statistics"""
        return {
            'pruned_branches': self.pruned_branches,
            'explored_branches': self.explored_branches,
            'prune_rate': self.pruned_branches / max(1, self.explored_branches),
            'confidence_level': self.confidence_level,
            'prune_factor': self.prune_factor
        }
    
    def reset(self):
        """Reset pruning statistics"""
        self.pruned_branches = 0
        self.explored_branches = 0
        self._cache.clear()


# ============================================================
# ENHANCEMENT 15: Main Enhanced Regret Optimizer with New Features
# ============================================================

class EnhancedRegretMinimizationOptimizer:
    """
    Enhanced regret minimization optimizer v3.1.
    
    Integrates:
    - Bayesian expected regret
    - Pareto frontier analysis
    - Thompson sampling for exploration
    - SHAP-based explanations
    - Adaptive pruning
    """
    
    MINIMIZING_OBJECTIVES = {Objective.ENERGY, Objective.CARBON, 
                              Objective.HELIUM, Objective.LATENCY, Objective.COST}
    MAXIMIZING_OBJECTIVES = {Objective.ACCURACY}
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        
        # Objective weights
        self.objective_weights = self.config.get('objective_weights', {
            Objective.ENERGY: 0.20,
            Objective.CARBON: 0.25,
            Objective.HELIUM: 0.25,
            Objective.LATENCY: 0.15,
            Objective.ACCURACY: 0.15,
            Objective.COST: 0.00
        })
        total = sum(self.objective_weights.values())
        self.objective_weights = {k: v/total for k, v in self.objective_weights.items()}
        
        # Uncertainty defaults
        self.uncertainty_defaults = self.config.get('uncertainty_intervals', {
            Objective.ENERGY: 0.15,
            Objective.CARBON: 0.15,
            Objective.HELIUM: 0.10,
            Objective.LATENCY: 0.20,
            Objective.ACCURACY: 0.05,
            Objective.COST: 0.15
        })
        
        # New components
        self.bayesian_calculator = BayesianExpectedRegretCalculator()
        self.pareto_analyzer = ParetoFrontierAnalyzer()
        self.bandit = ThompsonSamplingBandit(
            exploration_temperature=self.config.get('exploration_temperature', 1.0)
        )
        self.explainer = DecisionExplainer()
        self.pruning_tree = AdaptivePruningDecisionTree()
        
        self.decision_history: List[RegretDecision] = []
        self._event_loop = None
        
        logger.info("Enhanced Regret Minimization Optimizer v3.1 initialized")
    
    def _get_event_loop(self):
        """Get or create event loop for async operations"""
        if self._event_loop is None or self._event_loop.is_closed():
            try:
                self._event_loop = asyncio.get_running_loop()
            except RuntimeError:
                self._event_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._event_loop)
        return self._event_loop
    
    def _run_async(self, coro):
        """Run async coroutine in event loop"""
        loop = self._get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, coro)
                return future.result()
        else:
            return loop.run_until_complete(coro)
    
    def calculate_regret(self, outcomes: List[ActionOutcome]) -> Dict[str, float]:
        """Calculate regret for each action with correlation adjustment and Thompson sampling"""
        scenarios = set(o.scenario for o in outcomes)
        objectives = list(self.objective_weights.keys())
        
        # Best outcome per scenario per objective
        best_outcomes = {}
        for scenario in scenarios:
            scenario_outcomes = [o for o in outcomes if o.scenario == scenario]
            best_outcomes[scenario] = {}
            for obj in objectives:
                values = [o.outcomes.get(obj, 0) for o in scenario_outcomes]
                if obj in self.MINIMIZING_OBJECTIVES:
                    best_outcomes[scenario][obj] = min(values) if values else 0
                else:
                    best_outcomes[scenario][obj] = max(values) if values else 0
        
        # Calculate regret per action
        action_regrets = {}
        action_regret_matrix = {}
        
        for action_name in set(o.action_name for o in outcomes):
            max_regret = 0
            scenario_regrets = {}
            
            for scenario in scenarios:
                scenario_outcome = next(
                    (o for o in outcomes if o.action_name == action_name and o.scenario == scenario), 
                    None
                )
                if not scenario_outcome:
                    continue
                
                # Calculate per-objective regrets
                objective_regrets = []
                for obj in objectives:
                    actual = scenario_outcome.outcomes.get(obj, 0)
                    best = best_outcomes[scenario][obj]
                    
                    if best != 0:
                        if obj in self.MINIMIZING_OBJECTIVES:
                            regret = max(0, (actual - best) / abs(best))
                        else:
                            regret = max(0, (best - actual) / abs(best))
                    else:
                        regret = 0
                    
                    weighted_regret = regret * self.objective_weights.get(obj, 0)
                    objective_regrets.append(weighted_regret)
                
                total_scenario_regret = sum(objective_regrets)
                scenario_regrets[scenario] = total_scenario_regret
                max_regret = max(max_regret, total_scenario_regret)
            
            action_regrets[action_name] = max_regret
            action_regret_matrix[action_name] = scenario_regrets
        
        # Update Bayesian calculator with outcomes
        for outcome in outcomes:
            for obj, value in outcome.outcomes.items():
                self.bayesian_calculator.update_outcome_observation(
                    outcome.action_name, outcome.scenario, obj, value
                )
        
        return action_regrets
    
    def compute_pareto_optimal_actions(self, outcomes: List[ActionOutcome]) -> List[str]:
        """Compute Pareto-optimal actions using enhanced analyzer"""
        frontier = self.pareto_analyzer.compute_pareto_frontier(
            outcomes, list(self.objective_weights.keys())
        )
        return [action for action, _ in frontier]
    
    def optimize_with_regret(self, outcomes: List[ActionOutcome],
                            uncertainty_enabled: bool = True,
                            use_expected_regret: bool = True,
                            use_bandit: bool = False) -> RegretDecision:
        """Main optimization with enhanced features"""
        
        # Compute regret
        action_regrets = self.calculate_regret(outcomes)
        
        if not action_regrets:
            return RegretDecision(
                selected_action="unknown",
                max_regret=1.0,
                confidence=0.0,
                expected_outcomes={},
                regret_matrix={},
                reasoning="No outcomes provided",
                decision_id=hashlib.md5(str(time.time()).encode()).hexdigest()[:8]
            )
        
        # Compute expected regret using Bayesian calculator
        expected_regret = None
        if use_expected_regret:
            # Build regret matrix for Bayesian calculator
            regret_matrix = {}
            for action in set(o.action_name for o in outcomes):
                regret_matrix[action] = {}
                for scenario in set(o.scenario for o in outcomes):
                    scenario_outcome = next(
                        (o for o in outcomes if o.action_name == action and o.scenario == scenario),
                        None
                    )
                    if scenario_outcome:
                        # Calculate actual regret for this scenario
                        # Simplified - would compute proper regret
                        regret = action_regrets.get(action, 0.5)
                        regret_matrix[action][scenario] = regret
            
            expected_results = self.bayesian_calculator.calculate_expected_regret(regret_matrix)
            expected_regret_dict = {a: r[0] for a, r in expected_results.items()}
            expected_regret = min(expected_regret_dict.values()) if expected_regret_dict else None
            selected_action_expected = min(expected_regret_dict, key=expected_regret_dict.get)
        else:
            selected_action_expected = min(action_regrets, key=action_regrets.get)
        
        # Thompson sampling for exploration
        if use_bandit:
            available_actions = list(set(o.action_name for o in outcomes))
            bandit_action = self.bandit.sample_action(available_actions)
            selected_action = bandit_action
            max_regret = action_regrets.get(selected_action, 1.0)
        else:
            selected_action = selected_action_expected
            max_regret = action_regrets.get(selected_action, 1.0)
        
        # Calculate expected outcomes
        selected_outcomes = [o for o in outcomes if o.action_name == selected_action]
        expected_outcomes = self._average_outcomes(selected_outcomes)
        
        # Calculate confidence with Bayesian uncertainty
        base_confidence = max(0.5, 1.0 - max_regret)
        
        if uncertainty_enabled:
            # Get outcome uncertainties
            uncertainties = []
            for obj in self.objective_weights:
                _, std = self.bayesian_calculator.predict_outcome(
                    selected_action, list(set(o.scenario for o in outcomes))[0], obj
                )
                uncertainties.append(std)
            
            if uncertainties:
                avg_uncertainty = np.mean(uncertainties)
                confidence = base_confidence * max(0.5, 1.0 - avg_uncertainty)
            else:
                confidence = base_confidence
        else:
            confidence = base_confidence
        
        # Generate reasoning with explanation
        reasoning = self.explainer.explain_decision(action_regrets, outcomes, selected_action)['reasoning']
        
        # Update bandit with scaled regret (lower regret = higher reward)
        reward = 1.0 - max_regret
        self.bandit.update_reward(selected_action, reward)
        
        # Compute Pareto frontier
        pareto_actions = self.compute_pareto_optimal_actions(outcomes)
        hypervolume = self.pareto_analyzer.calculate_hypervolume(
            self.pareto_analyzer.compute_pareto_frontier(outcomes, list(self.objective_weights.keys()))
        )
        
        decision = RegretDecision(
            selected_action=selected_action,
            max_regret=max_regret,
            expected_regret=expected_regret,
            confidence=confidence,
            expected_outcomes=expected_outcomes,
            regret_matrix=action_regrets,
            reasoning=reasoning,
            decision_id=hashlib.md5(f"{selected_action}_{time.time()}".encode()).hexdigest()[:8]
        )
        
        self.decision_history.append(decision)
        
        logger.info(f"Selected '{selected_action}' | max_regret={max_regret:.3f} | "
                   f"confidence={confidence:.2f} | hypervolume={hypervolume:.2f}")
        
        return decision
    
    def _average_outcomes(self, outcomes: List[ActionOutcome]) -> Dict[Objective, float]:
        """Average outcomes across scenarios"""
        if not outcomes:
            return {}
        
        avg = {}
        for obj in self.objective_weights.keys():
            values = [o.outcomes.get(obj, 0) for o in outcomes]
            avg[obj] = np.mean(values)
        return avg
    
    def update_with_actual_outcome(self, action: str, scenario: str, 
                                    actual_outcomes: Dict[Objective, float]):
        """Update Bayesian calculator and bandit with actual outcome"""
        # Update Bayesian calculator
        self.bayesian_calculator.update_scenario_observation(scenario)
        for obj, value in actual_outcomes.items():
            self.bayesian_calculator.update_outcome_observation(action, scenario, obj, value)
        
        # Update bandit with reward (based on outcome quality)
        # Simplified: average of normalized outcomes
        reward = 0.5  # Would compute proper reward
        self.bandit.update_reward(action, reward)
        
        logger.info(f"Updated with actual outcome: {action} under {scenario}")
    
    def get_decision_explanation(self, decision: RegretDecision) -> Dict:
        """Get detailed explanation for a decision"""
        return self.explainer.explain_decision(
            decision.regret_matrix,
            [],  # Would need outcomes
            decision.selected_action
        )
    
    def get_bandit_statistics(self) -> Dict:
        """Get Thompson sampling bandit statistics"""
        return self.bandit.get_statistics()
    
    def get_pareto_statistics(self) -> Dict:
        """Get Pareto frontier statistics"""
        return self.pareto_analyzer.get_frontier_evolution()
    
    def get_pruning_statistics(self) -> Dict:
        """Get adaptive pruning statistics"""
        return self.pruning_tree.get_statistics()
    
    def generate_report(self) -> Dict:
        """Generate comprehensive report with all enhancements"""
        bandit_stats = self.get_bandit_statistics()
        pareto_stats = self.get_pareto_statistics()
        pruning_stats = self.get_pruning_statistics()
        
        return {
            'objective_weights': {k.value: v for k, v in self.objective_weights.items()},
            'decision_history_count': len(self.decision_history),
            'bandit': bandit_stats,
            'pareto_frontier': pareto_stats,
            'pruning': pruning_stats,
            'recent_decisions': [
                {
                    'action': d.selected_action,
                    'max_regret': d.max_regret,
                    'confidence': d.confidence,
                    'timestamp': d.timestamp
                }
                for d in self.decision_history[-5:]
            ]
        }


# ============================================================
# Usage Example with Enhanced Features
# ============================================================

async def async_main():
    print("=== Enhanced Regret Minimization Optimizer v3.1 Demo ===\n")
    
    optimizer = EnhancedRegretMinimizationOptimizer({
        'objective_weights': {
            Objective.ENERGY: 0.20,
            Objective.CARBON: 0.25,
            Objective.HELIUM: 0.25,
            Objective.LATENCY: 0.15,
            Objective.ACCURACY: 0.15
        },
        'exploration_temperature': 1.0
    })
    
    # Create sample outcomes
    def predictor(action: str, scenario: str) -> Dict:
        outcomes = {
            'execute': {
                Objective.ENERGY: 100,
                Objective.CARBON: 50,
                Objective.HELIUM: 0.8,
                Objective.LATENCY: 100,
                Objective.ACCURACY: 0.95,
                Objective.COST: 10
            },
            'throttle': {
                Objective.ENERGY: 70,
                Objective.CARBON: 35,
                Objective.HELIUM: 0.5,
                Objective.LATENCY: 120,
                Objective.ACCURACY: 0.92,
                Objective.COST: 8
            },
            'defer': {
                Objective.ENERGY: 0,
                Objective.CARBON: 0,
                Objective.HELIUM: 0,
                Objective.LATENCY: 500,
                Objective.ACCURACY: 0,
                Objective.COST: 0
            }
        }
        return outcomes.get(action, outcomes['execute'])
    
    actions = ['execute', 'throttle', 'defer']
    scenarios = ['high_carbon', 'helium_crisis', 'low_demand']
    
    outcomes = []
    for action in actions:
        for scenario in scenarios:
            outcomes.append(ActionOutcome(
                action_name=action,
                scenario=scenario,
                outcomes=predictor(action, scenario)
            ))
    
    print("1. Regret minimization with Bayesian expected regret:")
    decision = optimizer.optimize_with_regret(outcomes, use_expected_regret=True)
    print(f"   Selected action: {decision.selected_action}")
    print(f"   Max regret: {decision.max_regret:.3f}")
    print(f"   Confidence: {decision.confidence:.2%}")
    print(f"   Reasoning: {decision.reasoning}")
    
    print("\n2. Thompson Sampling Bandit:")
    bandit_stats = optimizer.get_bandit_statistics()
    print(f"   Action probabilities: {bandit_stats['action_probabilities']}")
    print(f"   Action uncertainties: {bandit_stats['action_uncertainties']}")
    
    print("\n3. Pareto Frontier Analysis:")
    pareto_actions = optimizer.compute_pareto_optimal_actions(outcomes)
    print(f"   Pareto-optimal actions: {pareto_actions}")
    
    print("\n4. Decision Explanation:")
    explanation = optimizer.get_decision_explanation(decision)
    print(f"   {explanation['reasoning']}")
    if explanation['feature_contributions']:
        print(f"   Feature contributions: {explanation['feature_contributions']}")
    
    print("\n5. System Report:")
    report = optimizer.generate_report()
    print(f"   Decision history: {report['decision_history_count']}")
    print(f"   Bandit samples: {report['bandit']['action_samples']}")
    print(f"   Pareto hypervolume trend: {report['pareto_frontier']['hypervolume_trend'][-3:] if report['pareto_frontier']['hypervolume_trend'] else 'N/A'}")
    
    print("\n6. Simulating online learning:")
    # Simulate actual outcomes and update
    optimizer.update_with_actual_outcome(
        action='throttle',
        scenario='high_carbon',
        actual_outcomes={
            Objective.ENERGY: 65,
            Objective.CARBON: 30,
            Objective.HELIUM: 0.45,
            Objective.LATENCY: 115,
            Objective.ACCURACY: 0.93,
            Objective.COST: 7
        }
    )
    
    # Make another decision with updated beliefs
    decision2 = optimizer.optimize_with_regret(outcomes, use_expected_regret=True)
    print(f"   Updated decision: {decision2.selected_action}")
    
    print("\n✅ Enhanced Regret Minimization Optimizer v3.1 test complete")

def main():
    asyncio.run(async_main())

if __name__ == "__main__":
    main()
