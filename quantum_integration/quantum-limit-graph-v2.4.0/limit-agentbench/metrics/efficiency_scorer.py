# -*- coding: utf-8 -*-
"""
Efficiency Scorer
Performance efficiency metrics calculation
"""

from typing import Dict
import logging

logger = logging.getLogger(__name__)


class EfficiencyScorer:
    """
    Performance efficiency scorer.
    
    Calculates:
    - Performance per watt
    - Accuracy per joule
    - Throughput per kWh
    - Cost efficiency
    """
    
    # Cost per kWh by region (USD)
    ELECTRICITY_COST = {
        "US-CA": 0.25,
        "US-TX": 0.12,
        "US-NY": 0.20,
        "EU-DE": 0.35,
        "EU-FR": 0.18,
        "CN": 0.08,
        "IN": 0.07,
        "GLOBAL": 0.15
    }
    
    def __init__(self, grid_region: str = "GLOBAL"):
        """
        Initialize efficiency scorer.
        
        Args:
            grid_region: Grid region for cost calculations
        """
        self.grid_region = grid_region
        self.electricity_cost = self.ELECTRICITY_COST.get(grid_region, 0.15)
        
    def calculate_efficiency_score(
        self,
        accuracy: float,
        energy_kwh: float
    ) -> float:
        """
        Calculate efficiency score.
        
        Formula: accuracy / energy_kwh
        Higher is better.
        
        Args:
            accuracy: Task accuracy (0-1)
            energy_kwh: Energy consumption
            
        Returns:
            Efficiency score
        """
        if energy_kwh == 0:
            return 0.0
        return accuracy / energy_kwh
    
    def calculate_performance_per_watt(
        self,
        accuracy: float,
        power_watts: float,
        duration_seconds: float
    ) -> float:
        """
        Calculate performance per watt.
        
        Args:
            accuracy: Task accuracy
            power_watts: Average power consumption
            duration_seconds: Execution duration
            
        Returns:
            Performance per watt score
        """
        if power_watts == 0:
            return 0.0
        return accuracy / power_watts
    
    def calculate_cost_efficiency(
        self,
        accuracy: float,
        energy_kwh: float
    ) -> Dict[str, float]:
        """
        Calculate cost efficiency metrics.
        
        Args:
            accuracy: Task accuracy
            energy_kwh: Energy consumption
            
        Returns:
            Dictionary with cost efficiency metrics
        """
        cost_usd = energy_kwh * self.electricity_cost
        
        if cost_usd == 0:
            accuracy_per_dollar = 0.0
        else:
            accuracy_per_dollar = accuracy / cost_usd
        
        return {
            "cost_usd": cost_usd,
            "accuracy_per_dollar": accuracy_per_dollar,
            "electricity_cost_per_kwh": self.electricity_cost,
            "grid_region": self.grid_region
        }
    
    def calculate_throughput_efficiency(
        self,
        num_tasks: int,
        energy_kwh: float,
        duration_seconds: float
    ) -> Dict[str, float]:
        """
        Calculate throughput efficiency.
        
        Args:
            num_tasks: Number of tasks completed
            energy_kwh: Total energy consumption
            duration_seconds: Total duration
            
        Returns:
            Dictionary with throughput metrics
        """
        tasks_per_kwh = num_tasks / energy_kwh if energy_kwh > 0 else 0.0
        tasks_per_second = num_tasks / duration_seconds if duration_seconds > 0 else 0.0
        energy_per_task = energy_kwh / num_tasks if num_tasks > 0 else 0.0
        
        return {
            "tasks_per_kwh": tasks_per_kwh,
            "tasks_per_second": tasks_per_second,
            "energy_per_task": energy_per_task,
            "num_tasks": num_tasks
        }
    
    def compare_efficiency(
        self,
        agent_a_metrics: Dict[str, float],
        agent_b_metrics: Dict[str, float]
    ) -> Dict[str, any]:
        """
        Compare efficiency between two agents.
        
        Args:
            agent_a_metrics: Metrics for agent A
            agent_b_metrics: Metrics for agent B
            
        Returns:
            Comparison results
        """
        eff_a = self.calculate_efficiency_score(
            agent_a_metrics.get("accuracy", 0),
            agent_a_metrics.get("energy_kwh", 1)
        )
        eff_b = self.calculate_efficiency_score(
            agent_b_metrics.get("accuracy", 0),
            agent_b_metrics.get("energy_kwh", 1)
        )
        
        improvement = ((eff_b - eff_a) / eff_a * 100) if eff_a > 0 else 0.0
        
        return {
            "agent_a_efficiency": eff_a,
            "agent_b_efficiency": eff_b,
            "improvement_percent": improvement,
            "winner": "agent_b" if eff_b > eff_a else "agent_a"
        }
