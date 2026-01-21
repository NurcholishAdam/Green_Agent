"""
Budget Manager for Green_Agent

Manages and tracks consumption against energy/carbon/latency budgets.
Transforms evaluation from "who is greenest?" to "who succeeds within constraints?"
"""

from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class BudgetStatus(Enum):
    """Budget consumption status"""
    UNDER_BUDGET = "under_budget"      # Safely under budget
    NEAR_LIMIT = "near_limit"          # Approaching budget limit
    AT_LIMIT = "at_limit"              # At budget limit
    EXCEEDED = "exceeded"              # Budget exceeded


@dataclass
class Budget:
    """
    Budget constraints for agent execution
    
    Defines hard limits on resource consumption. Agents must operate
    within these constraints.
    
    Attributes:
        max_energy_wh: Maximum energy in watt-hours
        max_carbon_g: Maximum CO₂ in grams
        max_latency_ms: Maximum latency per task
        max_cost_usd: Optional cost budget
        warning_threshold: Percentage of budget that triggers warning (0.0-1.0)
        critical_threshold: Percentage that triggers critical alert (0.0-1.0)
    """
    max_energy_wh: float          # Maximum energy in watt-hours
    max_carbon_g: float           # Maximum CO₂ in grams
    max_latency_ms: float         # Maximum latency per task
    max_cost_usd: Optional[float] = None    # Optional cost budget
    
    # Warning thresholds (percentage of budget)
    warning_threshold: float = 0.80   # 80% triggers warning
    critical_threshold: float = 0.95  # 95% triggers critical alert
    
    # Metadata
    name: str = "Default Budget"
    description: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        """Validate budget constraints"""
        if self.max_energy_wh <= 0:
            raise ValueError(f"Energy budget must be positive: {self.max_energy_wh}")
        if self.max_carbon_g <= 0:
            raise ValueError(f"Carbon budget must be positive: {self.max_carbon_g}")
        if self.max_latency_ms <= 0:
            raise ValueError(f"Latency budget must be positive: {self.max_latency_ms}")
        if self.max_cost_usd is not None and self.max_cost_usd <= 0:
            raise ValueError(f"Cost budget must be positive: {self.max_cost_usd}")
        
        if not 0 <= self.warning_threshold <= 1:
            raise ValueError(f"Warning threshold must be in [0, 1]: {self.warning_threshold}")
        if not 0 <= self.critical_threshold <= 1:
            raise ValueError(f"Critical threshold must be in [0, 1]: {self.critical_threshold}")
        
        logger.info(f"Created budget: {self.name} - "
                   f"E:{self.max_energy_wh}Wh, C:{self.max_carbon_g}g, L:{self.max_latency_ms}ms")
    
    def to_dict(self) -> Dict:
        """Serialize to dictionary"""
        return {
            'name': self.name,
            'description': self.description,
            'max_energy_wh': self.max_energy_wh,
            'max_carbon_g': self.max_carbon_g,
            'max_latency_ms': self.max_latency_ms,
            'max_cost_usd': self.max_cost_usd,
            'warning_threshold': self.warning_threshold,
            'critical_threshold': self.critical_threshold,
            'created_at': self.created_at.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Budget':
        """Deserialize from dictionary"""
        created_at = datetime.fromisoformat(data['created_at']) if 'created_at' in data else datetime.now()
        
        return cls(
            max_energy_wh=data['max_energy_wh'],
            max_carbon_g=data['max_carbon_g'],
            max_latency_ms=data['max_latency_ms'],
            max_cost_usd=data.get('max_cost_usd'),
            warning_threshold=data.get('warning_threshold', 0.80),
            critical_threshold=data.get('critical_threshold', 0.95),
            name=data.get('name', 'Default Budget'),
            description=data.get('description', ''),
            created_at=created_at
        )
    
    @classmethod
    def eco_budget(cls) -> 'Budget':
        """Pre-defined eco-friendly budget (very strict)"""
        return cls(
            max_energy_wh=5.0,      # 5 Wh per task
            max_carbon_g=1.0,       # 1g CO₂ per task
            max_latency_ms=10000,   # 10 seconds
            name="Eco Budget",
            description="Strict energy/carbon limits for eco-friendly deployment"
        )
    
    @classmethod
    def balanced_budget(cls) -> 'Budget':
        """Pre-defined balanced budget (moderate)"""
        return cls(
            max_energy_wh=20.0,     # 20 Wh per task
            max_carbon_g=4.0,       # 4g CO₂ per task
            max_latency_ms=5000,    # 5 seconds
            name="Balanced Budget",
            description="Moderate limits balancing efficiency and performance"
        )
    
    @classmethod
    def performance_budget(cls) -> 'Budget':
        """Pre-defined performance budget (lenient on energy, strict on latency)"""
        return cls(
            max_energy_wh=100.0,    # 100 Wh per task
            max_carbon_g=20.0,      # 20g CO₂ per task
            max_latency_ms=1000,    # 1 second (strict!)
            name="Performance Budget",
            description="Optimized for low latency, lenient on energy"
        )


class BudgetManager:
    """
    Manages and tracks budget consumption
    
    Tracks actual consumption against budget limits and provides
    budget status monitoring.
    """
    
    def __init__(self, budget: Budget):
        """
        Initialize budget manager
        
        Args:
            budget: Budget constraints to enforce
        """
        self.budget = budget
        self.consumed = {
            'energy_wh': 0.0,
            'carbon_g': 0.0,
            'latency_ms': 0.0,  # Per-task latency (checked against limit)
            'cost_usd': 0.0
        }
        self.execution_history: List[Dict] = []
        self.violation_history: List[Dict] = []
        
        logger.info(f"Initialized BudgetManager with {budget.name}")
    
    def check_budget(self, metric: str) -> BudgetStatus:
        """
        Check budget status for a specific metric
        
        Args:
            metric: One of 'energy_wh', 'carbon_g', 'latency_ms', 'cost_usd'
        
        Returns:
            BudgetStatus indicating current status
        
        Example:
            status = manager.check_budget('energy_wh')
            if status == BudgetStatus.EXCEEDED:
                print("Energy budget exceeded!")
        """
        if metric not in self.consumed:
            raise ValueError(f"Unknown metric: {metric}")
        
        # Get limit
        if metric == 'energy_wh':
            limit = self.budget.max_energy_wh
        elif metric == 'carbon_g':
            limit = self.budget.max_carbon_g
        elif metric == 'latency_ms':
            limit = self.budget.max_latency_ms
        elif metric == 'cost_usd':
            limit = self.budget.max_cost_usd or float('inf')
        else:
            limit = float('inf')
        
        consumed = self.consumed[metric]
        
        # For latency, we check per-task (not cumulative)
        if metric == 'latency_ms':
            # Latency is checked per-task, so we use the last recorded value
            if not self.execution_history:
                ratio = 0.0
            else:
                last_latency = self.execution_history[-1].get('latency_ms', 0.0)
                ratio = last_latency / limit if limit > 0 else 0.0
        else:
            ratio = consumed / limit if limit > 0 else 0.0
        
        # Determine status
        if ratio >= 1.0:
            return BudgetStatus.EXCEEDED
        elif ratio >= self.budget.critical_threshold:
            return BudgetStatus.AT_LIMIT
        elif ratio >= self.budget.warning_threshold:
            return BudgetStatus.NEAR_LIMIT
        else:
            return BudgetStatus.UNDER_BUDGET
    
    def can_execute(self, estimated_consumption: Dict[str, float]) -> Tuple[bool, List[str]]:
        """
        Check if execution is allowed given estimated consumption
        
        Args:
            estimated_consumption: Dict with estimated consumption:
                {
                    'energy_wh': float,
                    'carbon_g': float,
                    'latency_ms': float,
                    'cost_usd': float (optional)
                }
        
        Returns:
            Tuple of (can_execute: bool, violations: List[str])
        
        Example:
            can_run, violations = manager.can_execute({
                'energy_wh': 2.0,
                'carbon_g': 0.4,
                'latency_ms': 500
            })
            
            if not can_run:
                print(f"Cannot execute: {violations}")
        """
        violations = []
        
        # Check energy budget
        if (self.consumed['energy_wh'] + estimated_consumption.get('energy_wh', 0.0)
            > self.budget.max_energy_wh):
            violations.append('energy_wh')
            logger.warning(f"Energy budget would be exceeded")
        
        # Check carbon budget
        if (self.consumed['carbon_g'] + estimated_consumption.get('carbon_g', 0.0)
            > self.budget.max_carbon_g):
            violations.append('carbon_g')
            logger.warning(f"Carbon budget would be exceeded")
        
        # Check latency budget (per-task)
        if estimated_consumption.get('latency_ms', 0.0) > self.budget.max_latency_ms:
            violations.append('latency_ms')
            logger.warning(f"Latency budget would be exceeded")
        
        # Check cost budget
        if self.budget.max_cost_usd is not None:
            if (self.consumed['cost_usd'] + estimated_consumption.get('cost_usd', 0.0)
                > self.budget.max_cost_usd):
                violations.append('cost_usd')
                logger.warning(f"Cost budget would be exceeded")
        
        can_execute = len(violations) == 0
        
        if not can_execute:
            self.violation_history.append({
                'timestamp': datetime.now(),
                'violations': violations,
                'estimated_consumption': estimated_consumption,
                'current_consumed': self.consumed.copy()
            })
        
        return can_execute, violations
    
    def record_consumption(self, actual_consumption: Dict[str, float], metadata: Dict = None):
        """
        Record actual consumption after execution
        
        Args:
            actual_consumption: Dict with actual consumption
            metadata: Optional metadata about execution
        
        Example:
            manager.record_consumption({
                'energy_wh': 1.8,
                'carbon_g': 0.36,
                'latency_ms': 450
            }, metadata={'task_id': 'task_123'})
        """
        for metric, value in actual_consumption.items():
            if metric in self.consumed:
                # For latency, we track the max per-task latency
                if metric == 'latency_ms':
                    self.consumed[metric] = max(self.consumed[metric], value)
                else:
                    self.consumed[metric] += value
        
        # Record in history
        execution_record = {
            'timestamp': datetime.now(),
            'consumption': actual_consumption.copy(),
            'cumulative_consumed': self.consumed.copy(),
            'metadata': metadata or {}
        }
        self.execution_history.append(execution_record)
        
        # Check for warnings
        for metric in ['energy_wh', 'carbon_g']:
            status = self.check_budget(metric)
            if status in [BudgetStatus.NEAR_LIMIT, BudgetStatus.AT_LIMIT]:
                logger.warning(f"{metric} {status.value}: "
                             f"{self.consumed[metric]:.2f} / {getattr(self.budget, 'max_' + metric):.2f}")
        
        logger.debug(f"Recorded consumption: {actual_consumption}")
    
    def get_remaining_budget(self) -> Dict[str, float]:
        """
        Get remaining budget for each metric
        
        Returns:
            Dict with remaining budget for each metric
        
        Example:
            remaining = manager.get_remaining_budget()
            print(f"Energy remaining: {remaining['energy_wh']:.2f} Wh")
        """
        return {
            'energy_wh': max(0, self.budget.max_energy_wh - self.consumed['energy_wh']),
            'carbon_g': max(0, self.budget.max_carbon_g - self.consumed['carbon_g']),
            'latency_ms': self.budget.max_latency_ms,  # Per-task limit
            'cost_usd': max(0, (self.budget.max_cost_usd or 0) - self.consumed['cost_usd'])
        }
    
    def get_utilization(self) -> Dict[str, float]:
        """
        Get budget utilization percentages
        
        Returns:
            Dict with utilization ratio for each metric [0.0, 1.0+]
        
        Example:
            util = manager.get_utilization()
            print(f"Energy used: {util['energy_wh']:.1%}")
        """
        return {
            'energy_wh': self.consumed['energy_wh'] / self.budget.max_energy_wh,
            'carbon_g': self.consumed['carbon_g'] / self.budget.max_carbon_g,
            'latency_ms': self.consumed['latency_ms'] / self.budget.max_latency_ms,
            'cost_usd': (self.consumed['cost_usd'] / self.budget.max_cost_usd 
                        if self.budget.max_cost_usd else 0.0)
        }
    
    def reset(self):
        """Reset consumption tracking"""
        logger.info("Resetting budget consumption")
        self.consumed = {k: 0.0 for k in self.consumed}
        self.execution_history = []
    
    def get_summary(self) -> Dict:
        """
        Get comprehensive budget summary
        
        Returns:
            Dict with budget, consumption, and status information
        """
        return {
            'budget': self.budget.to_dict(),
            'consumed': self.consumed.copy(),
            'remaining': self.get_remaining_budget(),
            'utilization': self.get_utilization(),
            'status': {
                metric: self.check_budget(metric).value
                for metric in ['energy_wh', 'carbon_g', 'latency_ms', 'cost_usd']
            },
            'execution_count': len(self.execution_history),
            'violation_count': len(self.violation_history)
        }
