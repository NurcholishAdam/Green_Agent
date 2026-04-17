"""
Green Agent v5.0.0 - Carbon-Aware Decision Core
Layer 3: Makes sustainability-focused scheduling decisions
File: src/decision/carbon_aware_decision_core.py
"""

from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class ExecutionDecision:
    """Execution decision with carbon awareness"""
    action: str
    power_budget: float
    carbon_zone: str
    reasoning: List[str]
    deferred_until: Optional[datetime] = None


class CarbonAwareDecisionCore:
    """
    Carbon-aware decision engine that schedules tasks based on grid carbon intensity
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.thresholds = {
            'green': 50,
            'yellow': 200,
            'red': 400,
            'critical': 600
        }
        self.policy_mode = config.get('policy', {}).get('mode', 'moderate')
        self.weights = config.get('policy', {}).get('weights', {
            'carbon_importance': 0.4,
            'performance_importance': 0.3,
            'cost_importance': 0.2,
            'deadline_importance': 0.1
        })
    
    async def initialize(self):
        """Initialize the decision core"""
        logger.info("CarbonAwareDecisionCore initialized")
    
    async def evaluate(self, profile, carbon_intensity: float) -> ExecutionDecision:
        """
        Evaluate task and make carbon-aware execution decision
        
        Args:
            profile: WorkloadProfile from interpreter
            carbon_intensity: Current grid carbon intensity (gCO2/kWh)
            
        Returns:
            ExecutionDecision with action and power budget
        """
        zone = self._get_carbon_zone(carbon_intensity)
        reasoning = [f"Carbon intensity: {carbon_intensity} gCO2/kWh ({zone})"]
        
        # Make decision based on carbon zone and task properties
        if zone == 'green':
            # Optimal conditions - run at full power
            return ExecutionDecision(
                action='execute_full',
                power_budget=1.0,
                carbon_zone=zone,
                reasoning=reasoning
            )
        elif zone == 'yellow':
            # Moderate carbon - throttle execution
            return ExecutionDecision(
                action='execute_throttled',
                power_budget=0.6,
                carbon_zone=zone,
                reasoning=reasoning
            )
        elif zone == 'red':
            # High carbon - defer if possible, minimal execution otherwise
            if profile.deferrable:
                return ExecutionDecision(
                    action='defer',
                    power_budget=0.0,
                    carbon_zone=zone,
                    reasoning=reasoning + ["Task deferred due to high carbon"],
                    deferred_until=datetime.now()
                )
            else:
                return ExecutionDecision(
                    action='execute_minimal',
                    power_budget=0.3,
                    carbon_zone=zone,
                    reasoning=reasoning
                )
        else:  # critical
            # Critical carbon - defer all non-essential tasks
            return ExecutionDecision(
                action='defer',
                power_budget=0.0,
                carbon_zone=zone,
                reasoning=reasoning + ["Critical carbon zone - all non-essential tasks deferred"]
            )
    
    def _get_carbon_zone(self, intensity: float) -> str:
        """Determine carbon zone from intensity value"""
        if intensity < self.thresholds['green']:
            return 'green'
        elif intensity < self.thresholds['yellow']:
            return 'yellow'
        elif intensity < self.thresholds['red']:
            return 'red'
        else:
            return 'critical'
