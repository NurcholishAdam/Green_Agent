# src/enhancements/helium_elasticity.py

"""
Helium Price Elasticity Model for Green Agent
Scientific basis: Price elasticity of demand (PED) for resource allocation

Reference: "Demand Response in Critical Material Markets" (Nature Sustainability, 2024)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import Enum
import numpy as np
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class WorkloadPriority(Enum):
    """Workload priority levels with different elasticities"""
    CRITICAL = "critical"          # Must run (PED ~ -0.1)
    HIGH = "high"                  # Important (PED ~ -0.2)
    MEDIUM = "medium"              # Normal (PED ~ -0.4)
    LOW = "low"                    # Deferrable (PED ~ -0.6)
    BATCH = "batch"                # Elastic (PED ~ -1.0)


@dataclass
class HeliumMarketData:
    """Real-time helium market data"""
    timestamp: datetime
    spot_price_usd_per_liter: float
    futures_price_usd_per_liter: Dict[int, float]  # months ahead
    global_inventory_days: int
    demand_growth_rate: float
    supply_disruption_risk: float  # 0-1
    source: str = "api"


@dataclass
class DemandResponse:
    """Recommended demand response based on elasticity"""
    priority: WorkloadPriority
    recommended_reduction_percent: float
    optimal_execution_window_hours: int
    price_threshold_usd: float
    expected_savings_usd: float
    helium_saved_liters: float


@dataclass
class ElasticityDecision:
    """Decision output from elasticity model"""
    action: str  # 'defer', 'throttle', 'execute', 'substitute'
    throttle_factor: float
    optimal_delay_hours: int
    economic_savings_usd: float
    helium_reduction_percent: float
    reasoning: str
    confidence: float


class HeliumPriceElasticityModel:
    """
    Helium price elasticity model for optimal demand response.
    
    Price Elasticity of Demand (PED) = (% change quantity) / (% change price)
    
    For helium: estimated PED = -0.3 to -1.0 depending on application
    """
    
    # Elasticity by priority (more elastic = more responsive to price)
    ELASTICITY_VALUES = {
        WorkloadPriority.CRITICAL: -0.1,   # Very inelastic
        WorkloadPriority.HIGH: -0.2,
        WorkloadPriority.MEDIUM: -0.4,
        WorkloadPriority.LOW: -0.6,
        WorkloadPriority.BATCH: -1.0       # Unit elastic
    }
    
    # Price thresholds for different actions (USD per liter)
    PRICE_THRESHOLDS = {
        'defer': 8.0,
        'throttle': 6.0,
        'warn': 5.0,
        'normal': 4.0
    }
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.current_price = self.config.get('baseline_price', 4.0)
        self.baseline_price = self.config.get('baseline_price', 4.0)
        self.price_history: List[Tuple[datetime, float]] = []
        self.market_volatility = self.config.get('market_volatility', 0.2)
        self.inventory_days = self.config.get('initial_inventory_days', 30)
        
    def update_market_data(self, market_data: HeliumMarketData):
        """Update current market conditions"""
        self.current_price = market_data.spot_price_usd_per_liter
        self.inventory_days = market_data.global_inventory_days
        self.price_history.append((market_data.timestamp, self.current_price))
        
        # Keep last 1000 entries
        if len(self.price_history) > 1000:
            self.price_history = self.price_history[-1000:]
        
        logger.info(f"Helium price updated: ${self.current_price:.2f}/L "
                   f"(inventory: {self.inventory_days} days, risk: {market_data.supply_disruption_risk:.0%})")
    
    def calculate_elasticity(self, priority: WorkloadPriority) -> float:
        """Get elasticity value for workload priority"""
        return self.ELASTICITY_VALUES.get(priority, -0.3)
    
    def calculate_optimal_reduction(self, priority: WorkloadPriority, 
                                    price_increase_ratio: float) -> float:
        """
        Calculate optimal demand reduction based on price elasticity.
        
        % reduction = PED × % price increase
        
        Args:
            priority: Workload priority
            price_increase_ratio: current_price / baseline_price
            
        Returns:
            Recommended reduction percentage (0-1)
        """
        elasticity = self.calculate_elasticity(priority)
        price_increase_percent = price_increase_ratio - 1
        
        # PED formula
        reduction_percent = -elasticity * price_increase_percent
        
        # Clamp to reasonable bounds
        reduction_percent = max(0.0, min(0.9, reduction_percent))
        
        return reduction_percent
    
    def calculate_price_forecast(self, days_ahead: int = 30) -> List[float]:
        """
        Forecast helium prices using mean reversion + GARCH volatility.
        
        Returns:
            List of forecasted prices per day
        """
        forecast = []
        current = self.current_price
        
        for day in range(days_ahead):
            # Mean reversion to baseline
            reversion = (self.baseline_price - current) * 0.05
            
            # Volatility clustering (simplified GARCH)
            volatility = self.market_volatility * (1 + 0.3 * np.sin(day / 30 * 2 * np.pi))
            shock = np.random.normal(0, volatility * 0.5)
            
            # Inventory effect (low inventory drives prices up)
            inventory_effect = max(0, (20 - self.inventory_days) / 100) if self.inventory_days < 20 else 0
            
            # Update price
            current = current + reversion + shock + inventory_effect
            current = max(2.0, min(20.0, current))
            forecast.append(current)
        
        return forecast
    
    def find_optimal_window(self, helium_requirement_liters: float,
                            workload_priority: WorkloadPriority,
                            max_delay_hours: int = 168) -> Tuple[int, float, float]:
        """
        Find optimal execution window based on price forecast.
        
        Returns:
            (optimal_delay_hours, expected_savings_usd, expected_price_at_window)
        """
        # Get price forecast for max_delay_hours
        days_forecast = max_delay_hours // 24 + 1
        price_forecast = self.calculate_price_forecast(days_forecast)
        
        # Find minimum price in forecast
        min_price = min(price_forecast)
        min_index = price_forecast.index(min_price)
        
        # Calculate savings if executed at min price
        current_cost = helium_requirement_liters * self.current_price
        optimal_cost = helium_requirement_liters * min_price
        savings = current_cost - optimal_cost
        
        # Convert days to hours
        optimal_hours = min_index * 24
        
        # Apply elasticity: higher priority workloads have shorter optimal windows
        elasticity_factor = abs(self.calculate_elasticity(workload_priority))
        optimal_hours = int(optimal_hours * (1 - elasticity_factor * 0.5))
        optimal_hours = max(0, min(max_delay_hours, optimal_hours))
        
        logger.info(f"Optimal window for {workload_priority.value}: delay {optimal_hours}h, "
                   f"savings: ${savings:.2f} (${current_cost:.2f} → ${optimal_cost:.2f})")
        
        return optimal_hours, max(0, savings), min_price
    
    def optimize_allocation(self, workloads: List[Tuple[WorkloadPriority, float, float]]) -> List[DemandResponse]:
        """
        Optimize helium allocation across multiple workloads.
        
        Uses economic optimization: allocate helium to highest value per liter.
        
        Args:
            workloads: List of (priority, helium_requirement_liters, business_value)
            
        Returns:
            List of demand responses for each workload
        """
        price_ratio = self.current_price / self.baseline_price
        responses = []
        
        # Sort by value density (business_value / helium_requirement)
        sorted_workloads = sorted(workloads, key=lambda x: x[2] / x[1] if x[1] > 0 else 0, reverse=True)
        
        cumulative_helium = 0
        total_helium = sum(w[1] for w in workloads)
        
        for priority, requirement, value in sorted_workloads:
            reduction = self.calculate_optimal_reduction(priority, price_ratio)
            optimal_hours, savings, min_price = self.find_optimal_window(requirement, priority)
            
            # Economic threshold: reduce if price exceeds value density
            value_density = value / requirement if requirement > 0 else 0
            if self.current_price > value_density * 0.5:
                reduction = max(reduction, 0.3)
            
            # Calculate price threshold
            if priority == WorkloadPriority.CRITICAL:
                price_threshold = 15.0
            elif priority == WorkloadPriority.HIGH:
                price_threshold = 10.0
            elif priority == WorkloadPriority.MEDIUM:
                price_threshold = 7.0
            else:
                price_threshold = 5.0
            
            response = DemandResponse(
                priority=priority,
                recommended_reduction_percent=reduction * 100,
                optimal_execution_window_hours=optimal_hours if reduction > 0.2 else 0,
                price_threshold_usd=price_threshold,
                expected_savings_usd=savings * reduction,
                helium_saved_liters=requirement * reduction
            )
            responses.append(response)
            
            cumulative_helium += requirement * (1 - reduction)
        
        total_savings = sum(r.expected_savings_usd for r in responses)
        total_helium_saved = sum(r.helium_saved_liters for r in responses)
        
        logger.info(f"Helium allocation optimization: total savings ${total_savings:.2f}, "
                   f"helium saved {total_helium_saved:.2f}L ({total_helium_saved/total_helium*100:.1f}%)")
        
        return responses
    
    def should_defer(self, workload_priority: WorkloadPriority, 
                     carbon_zone: str,
                     helium_requirement_liters: float = 1.0) -> Tuple[bool, str, float]:
        """
        Determine if workload should be deferred based on price and elasticity.
        
        Integration point with Layer 3 decision core.
        
        Returns:
            (should_defer, reason, reduction_percent)
        """
        price_ratio = self.current_price / self.baseline_price
        reduction = self.calculate_optimal_reduction(workload_priority, price_ratio)
        
        # Defer if recommended reduction > 30%
        if reduction > 0.3:
            return True, f"Price ${self.current_price:.2f}/L exceeds elasticity threshold (reduction {reduction:.0%})", reduction
        
        # Defer if inventory critically low
        if self.inventory_days < 10 and workload_priority in [WorkloadPriority.MEDIUM, WorkloadPriority.LOW, WorkloadPriority.BATCH]:
            return True, f"Inventory critically low ({self.inventory_days} days remaining)", 0.5
        
        # Defer based on price thresholds
        if self.current_price > self.PRICE_THRESHOLDS['defer'] and workload_priority in [WorkloadPriority.MEDIUM, WorkloadPriority.LOW, WorkloadPriority.BATCH]:
            return True, f"Price ${self.current_price:.2f}/L exceeds deferral threshold", reduction
        
        # Combined carbon-helium deferral
        if carbon_zone in ['red', 'critical'] and self.current_price > self.PRICE_THRESHOLDS['throttle']:
            return True, f"Combined carbon ({carbon_zone}) and helium (${self.current_price:.2f}) constraints", 0.4
        
        return False, "Within price tolerance", reduction
    
    def calculate_throttle_factor(self, workload_priority: WorkloadPriority) -> float:
        """Calculate throttle factor based on current price"""
        price_ratio = self.current_price / self.baseline_price
        
        if price_ratio <= 1.0:
            return 1.0
        elif price_ratio <= 1.5:
            return 0.9
        elif price_ratio <= 2.0:
            return 0.7
        elif price_ratio <= 2.5:
            return 0.5
        else:
            return 0.3
    
    def get_elasticity_decision(self, workload_priority: WorkloadPriority,
                                helium_requirement_liters: float,
                                execution_decision,
                                carbon_zone: str = "green") -> ElasticityDecision:
        """
        Main interface for Layer 1 integration.
        
        Returns elasticity-based decision for workload execution.
        """
        should_defer, reason, reduction = self.should_defer(
            workload_priority, carbon_zone, helium_requirement_liters
        )
        
        price_ratio = self.current_price / self.baseline_price
        optimal_hours, savings, min_price = self.find_optimal_window(
            helium_requirement_liters, workload_priority
        )
        
        # Calculate confidence based on market volatility
        confidence = max(0.5, 1.0 - self.market_volatility)
        
        if should_defer:
            action = 'defer'
            throttle = 0.0
            helium_reduction = 1.0
        else:
            # Throttle based on price
            if price_ratio > 1.5:
                action = 'throttle'
                throttle = self.calculate_throttle_factor(workload_priority)
                helium_reduction = reduction
            else:
                action = 'execute'
                throttle = 1.0
                helium_reduction = 0.0
        
        return ElasticityDecision(
            action=action,
            throttle_factor=throttle,
            optimal_delay_hours=optimal_hours if should_defer else 0,
            economic_savings_usd=savings * reduction,
            helium_reduction_percent=helium_reduction * 100,
            reasoning=reason,
            confidence=confidence
        )
    
    def get_market_metrics(self) -> Dict:
        """Get current market metrics for Prometheus export"""
        price_trend = 0
        if len(self.price_history) >= 2:
            price_trend = (self.price_history[-1][1] - self.price_history[-2][1]) / self.price_history[-2][1]
        
        return {
            'current_price_usd': self.current_price,
            'baseline_price_usd': self.baseline_price,
            'price_ratio': self.current_price / self.baseline_price,
            'inventory_days': self.inventory_days,
            'price_trend_percent': price_trend * 100,
            'market_volatility': self.market_volatility,
            'price_forecast_7d': self.calculate_price_forecast(7)
        }
