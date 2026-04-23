# src/decision/carbon_aware_decision_core.py (EXTENDED)

from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class CarbonZone(Enum):
    GREEN = "green"
    YELLOW = "yellow"
    RED = "red"
    CRITICAL = "critical"

class HeliumZone(Enum):
    """Helium scarcity zones for workload execution"""
    HELIUM_GREEN = "helium_green"      # Normal supply, all workloads OK
    HELIUM_YELLOW = "helium_yellow"    # Constrained supply, throttle high-dependency
    HELIUM_RED = "helium_red"          # Severe shortage, defer high-dependency
    HELIUM_CRITICAL = "helium_critical" # No helium available, block GPU workloads

@dataclass
class ExecutionDecision:
    """Enhanced execution decision with helium awareness"""
    action: str  # 'execute_full', 'execute_throttled', 'execute_minimal', 'defer'
    power_budget: float  # 0.0 to 1.0
    carbon_zone: CarbonZone
    helium_zone: Optional[HeliumZone] = None
    helium_aware_flag: bool = False
    reasoning: str = ""
    target_hardware: Optional[str] = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

class CarbonAwareDecisionCore:
    """
    Enhanced decision core with dual-axis carbon + helium awareness
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        
        # Carbon thresholds (gCO2/kWh)
        self.carbon_thresholds = {
            'green': self.config.get('carbon_green_threshold', 50),
            'yellow': self.config.get('carbon_yellow_threshold', 200),
            'red': self.config.get('carbon_red_threshold', 400)
        }
        
        # Helium thresholds (dependency scores)
        self.helium_thresholds = {
            'yellow': self.config.get('helium_yellow_threshold', 0.6),
            'red': self.config.get('helium_red_threshold', 0.8),
            'critical': self.config.get('helium_critical_threshold', 0.95)
        }
        
        # Enable/disable helium awareness
        self.helium_aware_enabled = self.config.get('helium_aware_enabled', True)
        
        # Decision weights (for multi-objective optimization)
        self.weights = {
            'carbon': self.config.get('carbon_weight', 0.6),
            'helium': self.config.get('helium_weight', 0.4)
        }
    
    def make_decision(self, workload_profile, carbon_intensity: float,
                     helium_supply_status=None) -> ExecutionDecision:
        """
        Make execution decision considering both carbon and helium constraints
        """
        
        # Get carbon zone
        carbon_zone = self._get_carbon_zone(carbon_intensity)
        
        # Initialize helium zone
        helium_zone = None
        helium_aware = False
        
        # Get helium zone if enabled and helium data available
        if self.helium_aware_enabled and helium_supply_status:
            helium_profile = getattr(workload_profile, 'helium_profile', None)
            if helium_profile:
                helium_zone = self._get_helium_zone(helium_profile, helium_supply_status)
                helium_aware = True
        
        # Combine decisions
        if helium_aware and helium_zone:
            final_action, power_budget, reasoning = self._combine_decisions(
                carbon_zone, helium_zone, workload_profile
            )
        else:
            # Fallback to carbon-only decision
            final_action, power_budget, reasoning = self._carbon_only_decision(carbon_zone, workload_profile)
        
        return ExecutionDecision(
            action=final_action,
            power_budget=power_budget,
            carbon_zone=carbon_zone,
            helium_zone=helium_zone,
            helium_aware_flag=helium_aware,
            reasoning=reasoning
        )
    
    def _get_carbon_zone(self, carbon_intensity: float) -> CarbonZone:
        """Determine carbon zone based on intensity"""
        
        if carbon_intensity < self.carbon_thresholds['green']:
            return CarbonZone.GREEN
        elif carbon_intensity < self.carbon_thresholds['yellow']:
            return CarbonZone.YELLOW
        elif carbon_intensity < self.carbon_thresholds['red']:
            return CarbonZone.RED
        else:
            return CarbonZone.CRITICAL
    
    def _get_helium_zone(self, helium_profile, helium_supply_status) -> HeliumZone:
        """
        Determine helium zone based on workload dependency and global supply
        """
        dependency_score = helium_profile.dependency_score
        
        # Supply overrides individual task dependency
        if helium_supply_status.scarcity_level.value == 'severe':
            # In severe scarcity, even moderate dependency is problematic
            if dependency_score > 0.5:
                return HeliumZone.HELIUM_CRITICAL
            elif dependency_score > 0.3:
                return HeliumZone.HELIUM_RED
            else:
                return HeliumZone.HELIUM_YELLOW
                
        elif helium_supply_status.scarcity_level.value == 'critical':
            if dependency_score >= self.helium_thresholds['critical']:
                return HeliumZone.HELIUM_CRITICAL
            elif dependency_score >= self.helium_thresholds['red']:
                return HeliumZone.HELIUM_RED
            elif dependency_score >= self.helium_thresholds['yellow']:
                return HeliumZone.HELIUM_YELLOW
            else:
                return HeliumZone.HELIUM_GREEN
                
        elif helium_supply_status.scarcity_level.value == 'caution':
            if dependency_score >= self.helium_thresholds['critical']:
                return HeliumZone.HELIUM_RED
            elif dependency_score >= self.helium_thresholds['red']:
                return HeliumZone.HELIUM_YELLOW
            else:
                return HeliumZone.HELIUM_GREEN
        else:
            # Normal supply - use dependency thresholds
            if dependency_score >= self.helium_thresholds['critical']:
                return HeliumZone.HELIUM_YELLOW  # Even high-dependency can run, but monitor
            else:
                return HeliumZone.HELIUM_GREEN
    
    def _combine_decisions(self, carbon_zone: CarbonZone, helium_zone: HeliumZone,
                          workload_profile) -> Tuple[str, float, str]:
        """
        Combine carbon and helium decisions using weighted approach
        
        Decision Matrix (Conservative: worst of both)
        """
        
        # Map zones to numeric scores (higher = more constrained)
        carbon_scores = {
            CarbonZone.GREEN: 0,
            CarbonZone.YELLOW: 1,
            CarbonZone.RED: 2,
            CarbonZone.CRITICAL: 3
        }
        
        helium_scores = {
            HeliumZone.HELIUM_GREEN: 0,
            HeliumZone.HELIUM_YELLOW: 1,
            HeliumZone.HELIUM_RED: 2,
            HeliumZone.HELIUM_CRITICAL: 3
        }
        
        # Weighted combined score
        carbon_score = carbon_scores[carbon_zone]
        helium_score = helium_scores[helium_zone]
        
        combined_score = (carbon_score * self.weights['carbon'] + 
                         helium_score * self.weights['helium'])
        
        # Determine if task is deferrable
        deferrable = getattr(workload_profile, 'deferrable', True)
        priority = getattr(workload_profile, 'priority', 5)
        
        # Decision logic based on combined score
        if combined_score >= 2.5:  # Critical
            action = 'defer'
            power_budget = 0.0
            reasoning = f"Critical constraints: Carbon={carbon_zone.value}, Helium={helium_zone.value}"
            
        elif combined_score >= 1.8:  # Red
            if deferrable:
                action = 'defer'
                power_budget = 0.0
                reasoning = f"Red zone - deferring task due to {carbon_zone.value}/{helium_zone.value}"
            else:
                action = 'execute_minimal'
                power_budget = 0.2
                reasoning = f"Red zone but non-deferrable - minimal execution"
                
        elif combined_score >= 1.0:  # Yellow
            action = 'execute_throttled'
            power_budget = 0.5
            reasoning = f"Yellow zone - throttled execution"
            
        else:  # Green
            action = 'execute_full'
            power_budget = 1.0
            reasoning = f"Green zone - full execution"
        
        return action, power_budget, reasoning
    
    def _carbon_only_decision(self, carbon_zone: CarbonZone, workload_profile) -> Tuple[str, float, str]:
        """Fallback carbon-only decision logic"""
        
        deferrable = getattr(workload_profile, 'deferrable', True)
        
        if carbon_zone == CarbonZone.GREEN:
            return 'execute_full', 1.0, "Green carbon zone"
        elif carbon_zone == CarbonZone.YELLOW:
            return 'execute_throttled', 0.6, "Yellow carbon zone"
        elif carbon_zone == CarbonZone.RED:
            if deferrable:
                return 'defer', 0.0, "Red carbon zone - deferring"
            else:
                return 'execute_minimal', 0.3, "Red carbon zone but non-deferrable"
        else:  # CRITICAL
            return 'defer', 0.0, "Critical carbon zone"
