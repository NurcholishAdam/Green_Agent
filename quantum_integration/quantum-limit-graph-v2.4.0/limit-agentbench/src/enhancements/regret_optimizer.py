# src/enhancements/regret_optimizer.py

"""
Regret Minimization Optimizer for Green Agent
Scientific basis: Minimax regret decision theory under uncertainty

Reference: "Decision Theory Under Uncertainty" (Savage, 1951)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
import numpy as np
import logging

logger = logging.getLogger(__name__)


class Objective(Enum):
    """Optimization objectives"""
    ENERGY = "energy"
    CARBON = "carbon"
    HELIUM = "helium"
    LATENCY = "latency"
    ACCURACY = "accuracy"
    COST = "cost"


@dataclass
class ActionOutcome:
    """Outcome of an action under a scenario"""
    action_name: str
    scenario: str
    outcomes: Dict[Objective, float]


@dataclass
class UncertaintyInterval:
    """Uncertainty interval for an outcome"""
    objective: Objective
    mean: float
    lower_bound: float
    upper_bound: float
    confidence: float


@dataclass
class RegretDecision:
    """Decision output from regret minimizer"""
    selected_action: str
    max_regret: float
    confidence: float
    expected_outcomes: Dict[Objective, float]
    regret_matrix: Dict[str, float]  # action -> max_regret
    reasoning: str


class RegretMinimizationOptimizer:
    """
    Regret minimization optimizer for multi-objective decisions under uncertainty.
    
    Minimax regret: choose action that minimizes maximum regret across scenarios.
    Regret = outcome(best_action) - outcome(current_action)
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.objective_weights = self.config.get('objective_weights', {
            Objective.ENERGY: 0.20,
            Objective.CARBON: 0.25,
            Objective.HELIUM: 0.25,
            Objective.LATENCY: 0.15,
            Objective.ACCURACY: 0.15
        })
        self.uncertainty_defaults = self.config.get('uncertainty_intervals', {
            Objective.ENERGY: 0.15,   # ±15%
            Objective.CARBON: 0.15,
            Objective.HELIUM: 0.10,
            Objective.LATENCY: 0.20,
            Objective.ACCURACY: 0.05
        })
        
    def calculate_regret(self, outcomes: List[ActionOutcome]) -> Dict[str, float]:
        """
        Calculate regret for each action.
        
        For each scenario, find best outcome for each objective,
        then calculate regret as difference from best.
        """
        # Group by scenario
        scenarios = set(o.scenario for o in outcomes)
        objectives = list(self.objective_weights.keys())
        
        # Best outcome per scenario per objective
        best_outcomes = {}
        for scenario in scenarios:
            scenario_outcomes = [o for o in outcomes if o.scenario == scenario]
            best_outcomes[scenario] = {}
            for obj in objectives:
                values = [o.outcomes.get(obj, 0) for o in scenario_outcomes]
                # For minimizing objectives, best is minimum
                if obj in [Objective.ENERGY, Objective.CARBON, Objective.HELIUM, Objective.LATENCY, Objective.COST]:
                    best_outcomes[scenario][obj] = min(values) if values else 0
                else:  # Maximizing objectives (Accuracy)
                    best_outcomes[scenario][obj] = max(values) if values else 0
        
        # Calculate regret per action
        action_regrets = {}
        
        for action_name in set(o.action_name for o in outcomes):
            max_regret = 0
            
            for scenario in scenarios:
                scenario_outcome = next((o for o in outcomes if o.action_name == action_name and o.scenario == scenario), None)
                if not scenario_outcome:
                    continue
                
                scenario_regret = 0
                for obj in objectives:
                    actual = scenario_outcome.outcomes.get(obj, 0)
                    best = best_outcomes[scenario][obj]
                    
                    if best != 0:
                        # Normalized regret
                        if obj in [Objective.ENERGY, Objective.CARBON, Objective.HELIUM, Objective.LATENCY, Objective.COST]:
                            regret = max(0, (actual - best) / abs(best))
                        else:
                            regret = max(0, (best - actual) / abs(best))
                    else:
                        regret = 0
                    
                    # Weighted regret
                    weighted_regret = regret * self.objective_weights.get(obj, 0)
                    scenario_regret += weighted_regret
                
                max_regret = max(max_regret, scenario_regret)
            
            action_regrets[action_name] = max_regret
        
        return action_regrets
    
    def compute_pareto_optimal_actions(self, outcomes: List[ActionOutcome]) -> List[str]:
        """Compute Pareto-optimal actions (not dominated by any other action)"""
        actions = list(set(o.action_name for o in outcomes))
        pareto_optimal = []
        
        for i, action_i in enumerate(actions):
            dominated = False
            outcomes_i = [o for o in outcomes if o.action_name == action_i]
            avg_i = self._average_outcomes(outcomes_i)
            
            for j, action_j in enumerate(actions):
                if i == j:
                    continue
                outcomes_j = [o for o in outcomes if o.action_name == action_j]
                avg_j = self._average_outcomes(outcomes_j)
                
                # Check if action_j dominates action_i
                dominates = all(
                    avg_j.get(obj, 0) <= avg_i.get(obj, 0) 
                    for obj in [Objective.ENERGY, Objective.CARBON, Objective.HELIUM, Objective.LATENCY, Objective.COST]
                ) and avg_j.get(Objective.ACCURACY, 0) >= avg_i.get(Objective.ACCURACY, 0)
                
                # Strict dominance
                strict = any(
                    avg_j.get(obj, 0) < avg_i.get(obj, 0) 
                    for obj in [Objective.ENERGY, Objective.CARBON, Objective.HELIUM, Objective.LATENCY, Objective.COST]
                ) or avg_j.get(Objective.ACCURACY, 0) > avg_i.get(Objective.ACCURACY, 0)
                
                if dominates and strict:
                    dominated = True
                    break
            
            if not dominated:
                pareto_optimal.append(action_i)
        
        return pareto_optimal
    
    def _average_outcomes(self, outcomes: List[ActionOutcome]) -> Dict[Objective, float]:
        """Average outcomes across scenarios"""
        if not outcomes:
            return {}
        
        avg = {}
        for obj in self.objective_weights.keys():
            values = [o.outcomes.get(obj, 0) for o in outcomes]
            avg[obj] = np.mean(values)
        
        return avg
    
    def generate_uncertainty_intervals(self, outcomes: List[ActionOutcome]) -> List[UncertaintyInterval]:
        """Generate uncertainty intervals for each objective"""
        intervals = []
        
        for obj in self.objective_weights.keys():
            values = []
            for outcome in outcomes:
                if obj in outcome.outcomes:
                    values.append(outcome.outcomes[obj])
            
            if values:
                mean = np.mean(values)
                std = np.std(values)
                # Use default uncertainty if std is too small
                if std < mean * 0.01:
                    uncertainty = self.uncertainty_defaults[obj]
                    lower = mean * (1 - uncertainty)
                    upper = mean * (1 + uncertainty)
                    confidence = 0.68
                else:
                    lower = mean - std
                    upper = mean + std
                    confidence = 0.68
                
                intervals.append(UncertaintyInterval(
                    objective=obj,
                    mean=mean,
                    lower_bound=lower,
                    upper_bound=upper,
                    confidence=confidence
                ))
        
        return intervals
    
    def optimize_with_regret(self, outcomes: List[ActionOutcome],
                            uncertainty_enabled: bool = True) -> RegretDecision:
        """
        Main optimization function using minimax regret.
        
        Args:
            outcomes: List of outcomes for each action-scenario pair
            uncertainty_enabled: Whether to consider uncertainty intervals
            
        Returns:
            RegretDecision with selected action
        """
        # Compute regret
        action_regrets = self.calculate_regret(outcomes)
        
        # Find action with minimum maximum regret
        if not action_regrets:
            return RegretDecision(
                selected_action="unknown",
                max_regret=1.0,
                confidence=0.0,
                expected_outcomes={},
                regret_matrix={},
                reasoning="No outcomes provided"
            )
        
        selected_action = min(action_regrets, key=action_regrets.get)
        max_regret = action_regrets[selected_action]
        
        # Calculate expected outcomes for selected action
        selected_outcomes = [o for o in outcomes if o.action_name == selected_action]
        expected_outcomes = self._average_outcomes(selected_outcomes)
        
        # Calculate confidence based on regret
        # Lower regret = higher confidence
        confidence = max(0.5, 1.0 - max_regret)
        
        if uncertainty_enabled:
            # Adjust confidence based on uncertainty
            intervals = self.generate_uncertainty_intervals(selected_outcomes)
            avg_uncertainty = np.mean([(i.upper_bound - i.lower_bound) / i.mean for i in intervals]) if intervals else 0
            confidence *= max(0.5, 1.0 - avg_uncertainty)
        
        # Generate reasoning
        reasoning = self._generate_reasoning(selected_action, max_regret, action_regrets)
        
        logger.info(f"Regret optimization selected '{selected_action}' with max_regret={max_regret:.3f}, confidence={confidence:.2f}")
        
        return RegretDecision(
            selected_action=selected_action,
            max_regret=max_regret,
            confidence=confidence,
            expected_outcomes=expected_outcomes,
            regret_matrix=action_regrets,
            reasoning=reasoning
        )
    
    def _generate_reasoning(self, selected_action: str, max_regret: float,
                           action_regrets: Dict[str, float]) -> str:
        """Generate human-readable reasoning"""
        sorted_actions = sorted(action_regrets.items(), key=lambda x: x[1])
        
        reasoning = f"Selected '{selected_action}' (max regret={max_regret:.3f})"
        
        if len(sorted_actions) > 1:
            second_best = sorted_actions[1]
            reasoning += f" vs '{second_best[0]}' (regret={second_best[1]:.3f})"
        
        if max_regret < 0.1:
            reasoning += " - High confidence decision"
        elif max_regret < 0.3:
            reasoning += " - Moderate confidence"
        else:
            reasoning += " - Consider gathering more information"
        
        return reasoning
    
    def create_outcomes_from_scenarios(self, actions: List[str],
                                        scenarios: List[str],
                                        predictor_func) -> List[ActionOutcome]:
        """
        Create outcomes by evaluating predictor function for each action-scenario pair.
        
        Args:
            actions: List of action names
            scenarios: List of scenario names
            predictor_func: Function that takes (action, scenario) and returns outcome dict
        """
        outcomes = []
        
        for action in actions:
            for scenario in scenarios:
                outcome_dict = predictor_func(action, scenario)
                outcomes.append(ActionOutcome(
                    action_name=action,
                    scenario=scenario,
                    outcomes=outcome_dict
                ))
        
        return outcomes
