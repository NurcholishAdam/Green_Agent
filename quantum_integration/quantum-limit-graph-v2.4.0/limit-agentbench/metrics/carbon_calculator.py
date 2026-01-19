# -*- coding: utf-8 -*-
"""
Carbon Calculator
Carbon footprint calculation and analysis
"""

from typing import Dict
import logging

logger = logging.getLogger(__name__)


class CarbonCalculator:
    """
    Carbon footprint calculator.
    
    Calculates CO2e emissions based on:
    - Energy consumption
    - Grid carbon intensity
    - Geographic region
    """
    
    # Carbon intensity by region (kg CO2e per kWh)
    CARBON_INTENSITY = {
        "US-CA": 0.2,      # California
        "US-TX": 0.4,      # Texas
        "US-WV": 0.7,      # West Virginia
        "US-NY": 0.25,     # New York
        "EU-FR": 0.05,     # France (nuclear)
        "EU-DE": 0.35,     # Germany
        "EU-NO": 0.02,     # Norway (hydro)
        "EU-PL": 0.65,     # Poland (coal)
        "CN": 0.6,         # China
        "IN": 0.7,         # India
        "JP": 0.45,        # Japan
        "AU": 0.7,         # Australia
        "BR": 0.1,         # Brazil (hydro)
        "GLOBAL": 0.475    # Global average
    }
    
    def __init__(self, grid_region: str = "GLOBAL"):
        """
        Initialize carbon calculator.
        
        Args:
            grid_region: Grid region code
        """
        self.grid_region = grid_region
        self.carbon_intensity = self.CARBON_INTENSITY.get(grid_region, 0.475)
        
        logger.info(f"Initialized CarbonCalculator: region={grid_region}, "
                   f"intensity={self.carbon_intensity} kg CO2e/kWh")
    
    def calculate_emissions(self, energy_kwh: float) -> float:
        """
        Calculate CO2e emissions.
        
        Args:
            energy_kwh: Energy consumption in kWh
            
        Returns:
            CO2e emissions in kg
        """
        return energy_kwh * self.carbon_intensity
    
    def calculate_savings(
        self,
        baseline_energy_kwh: float,
        optimized_energy_kwh: float
    ) -> Dict[str, float]:
        """
        Calculate carbon savings from optimization.
        
        Args:
            baseline_energy_kwh: Baseline energy consumption
            optimized_energy_kwh: Optimized energy consumption
            
        Returns:
            Dictionary with savings metrics
        """
        baseline_carbon = self.calculate_emissions(baseline_energy_kwh)
        optimized_carbon = self.calculate_emissions(optimized_energy_kwh)
        
        carbon_saved_kg = baseline_carbon - optimized_carbon
        reduction_percent = (carbon_saved_kg / baseline_carbon * 100 
                            if baseline_carbon > 0 else 0.0)
        
        # Convert to equivalent metrics
        trees_equivalent = carbon_saved_kg / 21  # 1 tree absorbs ~21 kg CO2/year
        miles_driven = carbon_saved_kg / 0.404   # 1 mile driven = ~0.404 kg CO2
        
        return {
            "baseline_carbon_kg": baseline_carbon,
            "optimized_carbon_kg": optimized_carbon,
            "carbon_saved_kg": carbon_saved_kg,
            "reduction_percent": reduction_percent,
            "trees_equivalent": trees_equivalent,
            "miles_driven_equivalent": miles_driven,
            "grid_region": self.grid_region
        }
    
    def compare_regions(self, energy_kwh: float) -> Dict[str, float]:
        """
        Compare carbon emissions across different regions.
        
        Args:
            energy_kwh: Energy consumption
            
        Returns:
            Dictionary with emissions by region
        """
        return {
            region: energy_kwh * intensity
            for region, intensity in self.CARBON_INTENSITY.items()
        }
    
    @classmethod
    def get_cleanest_region(cls) -> str:
        """Get region with lowest carbon intensity."""
        return min(cls.CARBON_INTENSITY.items(), key=lambda x: x[1])[0]
    
    @classmethod
    def get_dirtiest_region(cls) -> str:
        """Get region with highest carbon intensity."""
        return max(cls.CARBON_INTENSITY.items(), key=lambda x: x[1])[0]
