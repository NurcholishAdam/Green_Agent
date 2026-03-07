"""
Carbon-Aware Decision Core

NEW: Makes sustainability-focused execution decisions
"""

from typing import Dict, Any
from dataclasses import dataclass

@dataclass
class ExecutionDecision:
    """Decision for task execution"""
    action: str  # execute_full, execute_throttled, defer, reject
    carbon_zone: str  # green, yellow, red, critical
    power_budget: float  # 0.0 to 1.0
    carbon_intensity: float
    reason: str
    policy: str
    estimated_savings: float = 0.0

class CarbonAwareDecisionCore:
    """
    Make carbon-aware decisions for task execution
    
    Features:
    - Zone-based decision making
    - Policy enforcement
    - Carbon budget tracking
    - Deadline awareness
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
    
    async def initialize(self):
        """Initialize decision core"""
        print(f"✅ Carbon-aware decision core initialized (policy: {self.policy_mode})")
    
    async def evaluate(self, profile: Any, carbon_intensity: float) -> ExecutionDecision:
        """
        Evaluate task and make decision based on carbon intensity
        
        Args:
            profile: WorkloadProfile from interpreter
            carbon_intensity: Current grid carbon intensity (gCO2/kWh)
        
        Returns:
            ExecutionDecision with action and metadata
        """
        # Determine carbon zone
        zone = self._get_carbon_zone(carbon_intensity)
        
        # Apply policy based on zone and task priority
        decision = self._apply_policy(profile, zone, carbon_intensity)
        
        return decision
    
    def _get_carbon_zone(self, intensity: float) -> str:
        """Determine carbon zone from intensity"""
        if intensity < self.thresholds['green']:
            return 'green'
        elif intensity < self.thresholds['yellow']:
            return 'yellow'
        elif intensity < self.thresholds['red']:
            return 'red'
        else:
            return 'critical'
    
    def _apply_policy(self, profile: Any, zone: str, intensity: float) -> ExecutionDecision:
        """Apply policy rules to make decision"""
        
        if zone == 'green':
            # Green zone: full execution
            return ExecutionDecision(
                action='execute_full',
                carbon_zone='green',
                power_budget=1.0,
                carbon_intensity=intensity,
                reason=f'Low carbon ({intensity:.0f} gCO2/kWh) - optimal for compute',
                policy=self.policy_mode,
                estimated_savings=0.0
            )
        
        elif zone == 'yellow':
            # Yellow zone: throttled execution
            return ExecutionDecision(
                action='execute_throttled',
                carbon_zone='yellow',
                power_budget=0.6,
                carbon_intensity=intensity,
                reason=f'Medium carbon ({intensity:.0f} gCO2/kWh) - throttling recommended',
                policy=self.policy_mode,
                estimated_savings=profile.energy_estimate * 0.4
            )
        
        elif zone == 'red':
            # Red zone: minimal execution or defer
            if profile.deferrable and profile.priority < 7:
                return ExecutionDecision(
                    action='defer',
                    carbon_zone='red',
                    power_budget=0.0,
                    carbon_intensity=intensity,
                    reason=f'High carbon ({intensity:.0f} gCO2/kWh) - deferring non-urgent task',
                    policy=self.policy_mode,
                    estimated_savings=profile.energy_estimate
                )
            else:
                return ExecutionDecision(
                    action='execute_minimal',
                    carbon_zone='red',
                    power_budget=0.3,
                    carbon_intensity=intensity,
                    reason=f'High carbon ({intensity:.0f} gCO2/kWh) - minimal execution',
                    policy=self.policy_mode,
                    estimated_savings=profile.energy_estimate * 0.7
                )
        
        else:  # critical
            # Critical zone: only urgent tasks
            if profile.priority >= 9 and not profile.deferrable:
                return ExecutionDecision(
                    action='execute_minimal',
                    carbon_zone='critical',
                    power_budget=0.1,
                    carbon_intensity=intensity,
                    reason=f'Critical carbon ({intensity:.0f} gCO2/kWh) - emergency execution only',
                    policy=self.policy_mode,
                    estimated_savings=profile.energy_estimate * 0.9
                )
            else:
                return ExecutionDecision(
                    action='defer',
                    carbon_zone='critical',
                    power_budget=0.0,
                    carbon_intensity=intensity,
                    reason=f'Critical carbon ({intensity:.0f} gCO2/kWh) - task deferred',
                    policy=self.policy_mode,
                    estimated_savings=profile.energy_estimate
                )
