# -*- coding: utf-8 -*-
"""
Sustainability Index
Composite green performance metric
"""

from typing import Dict
import logging

logger = logging.getLogger(__name__)


class SustainabilityIndex:
    """
    Sustainability index calculator.
    
    Combines multiple metrics into a single sustainability score:
    - Accuracy (performance)
    - Energy efficiency
    - Carbon footprint
    - Cost efficiency
    """
    
    def __init__(
        self,
        accuracy_weight: float = 0.4,
        efficiency_weight: float = 0.3,
        carbon_weight: float = 0.3
    ):
        """
        Initialize sustainability index calculator.
        
        Args:
            accuracy_weight: Weight for accuracy component
            efficiency_weight: Weight for efficiency component
            carbon_weight: Weight for carbon component
        """
        total = accuracy_weight + efficiency_weight + carbon_weight
        self.accuracy_weight = accuracy_weight / total
        self.efficiency_weight = efficiency_weight / total
        self.carbon_weight = carbon_weight / total
        
        logger.info(f"Initialized SustainabilityIndex with weights: "
                   f"accuracy={self.accuracy_weight:.2f}, "
                   f"efficiency={self.efficiency_weight:.2f}, "
                   f"carbon={self.carbon_weight:.2f}")
    
    def calculate(
        self,
        accuracy: float,
        energy_kwh: float,
        carbon_co2e_kg: float
    ) -> float:
        """
        Calculate sustainability index.
        
        Formula: (accuracy × w1 + efficiency × w2) / (carbon × w3)
        Higher is better.
        
        Args:
            accuracy: Task accuracy (0-1)
            energy_kwh: Energy consumption
            carbon_co2e_kg: Carbon emissions
            
        Returns:
            Sustainability index score
        """
        if carbon_co2e_kg == 0 or energy_kwh == 0:
            return 0.0
        
        # Normalize efficiency (inverse of energy)
        efficiency = 1.0 / energy_kwh
        
        # Calculate weighted numerator
        numerator = (
            accuracy * self.accuracy_weight +
            efficiency * self.efficiency_weight
        )
        
        # Calculate weighted denominator
        denominator = carbon_co2e_kg * self.carbon_weight
        
        sustainability_index = numerator / denominator if denominator > 0 else 0.0
        
        return sustainability_index
    
    def calculate_detailed(
        self,
        accuracy: float,
        energy_kwh: float,
        carbon_co2e_kg: float,
        latency_ms: float = None,
        cost_usd: float = None
    ) -> Dict[str, float]:
        """
        Calculate detailed sustainability metrics.
        
        Args:
            accuracy: Task accuracy
            energy_kwh: Energy consumption
            carbon_co2e_kg: Carbon emissions
            latency_ms: Execution latency (optional)
            cost_usd: Execution cost (optional)
            
        Returns:
            Dictionary with detailed metrics
        """
        sustainability_index = self.calculate(accuracy, energy_kwh, carbon_co2e_kg)
        
        # Calculate component scores
        efficiency = 1.0 / energy_kwh if energy_kwh > 0 else 0.0
        carbon_efficiency = accuracy / carbon_co2e_kg if carbon_co2e_kg > 0 else 0.0
        
        result = {
            "sustainability_index": sustainability_index,
            "accuracy": accuracy,
            "efficiency": efficiency,
            "carbon_efficiency": carbon_efficiency,
            "energy_kwh": energy_kwh,
            "carbon_co2e_kg": carbon_co2e_kg
        }
        
        if latency_ms is not None:
            result["latency_ms"] = latency_ms
            result["throughput"] = 1000 / latency_ms if latency_ms > 0 else 0.0
        
        if cost_usd is not None:
            result["cost_usd"] = cost_usd
            result["roi"] = accuracy / cost_usd if cost_usd > 0 else 0.0
        
        return result
    
    def rank_agents(
        self,
        agent_metrics: Dict[str, Dict[str, float]]
    ) -> list:
        """
        Rank agents by sustainability index.
        
        Args:
            agent_metrics: Dictionary mapping agent names to their metrics
            
        Returns:
            List of (agent_name, sustainability_index) tuples, sorted
        """
        rankings = []
        
        for agent_name, metrics in agent_metrics.items():
            si = self.calculate(
                accuracy=metrics.get("accuracy", 0),
                energy_kwh=metrics.get("energy_kwh", 1),
                carbon_co2e_kg=metrics.get("carbon_co2e_kg", 1)
            )
            rankings.append((agent_name, si))
        
        # Sort by sustainability index (descending)
        rankings.sort(key=lambda x: x[1], reverse=True)
        
        return rankings
    
    def compare_agents(
        self,
        agent_a_metrics: Dict[str, float],
        agent_b_metrics: Dict[str, float]
    ) -> Dict[str, any]:
        """
        Compare two agents on sustainability.
        
        Args:
            agent_a_metrics: Metrics for agent A
            agent_b_metrics: Metrics for agent B
            
        Returns:
            Comparison results
        """
        si_a = self.calculate(
            accuracy=agent_a_metrics.get("accuracy", 0),
            energy_kwh=agent_a_metrics.get("energy_kwh", 1),
            carbon_co2e_kg=agent_a_metrics.get("carbon_co2e_kg", 1)
        )
        
        si_b = self.calculate(
            accuracy=agent_b_metrics.get("accuracy", 0),
            energy_kwh=agent_b_metrics.get("energy_kwh", 1),
            carbon_co2e_kg=agent_b_metrics.get("carbon_co2e_kg", 1)
        )
        
        improvement = ((si_b - si_a) / si_a * 100) if si_a > 0 else 0.0
        
        return {
            "agent_a_sustainability": si_a,
            "agent_b_sustainability": si_b,
            "improvement_percent": improvement,
            "winner": "agent_b" if si_b > si_a else "agent_a",
            "difference": si_b - si_a
        }
    
    @staticmethod
    def get_rating(sustainability_index: float) -> str:
        """
        Get qualitative rating for sustainability index.
        
        Args:
            sustainability_index: Sustainability index value
            
        Returns:
            Rating string
        """
        if sustainability_index >= 200:
            return "Excellent"
        elif sustainability_index >= 150:
            return "Very Good"
        elif sustainability_index >= 100:
            return "Good"
        elif sustainability_index >= 50:
            return "Fair"
        else:
            return "Poor"
