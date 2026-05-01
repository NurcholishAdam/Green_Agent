# src/enhancements/material_substitution.py

"""
Material Substitution Engine for Green Agent
Scientific basis: Material substitution in supply chain management

Reference: "Critical Material Substitution in Semiconductor Manufacturing" (JOM, 2024)
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from enum import Enum
import numpy as np
import logging

logger = logging.getLogger(__name__)


class SubstituteMaterial(Enum):
    """Alternative cooling materials to helium"""
    CRYOCOOLER = "cryocooler"
    NEON = "neon"
    HYDROGEN = "hydrogen"
    NITROGEN = "nitrogen"
    ADIABATIC_DEMAG = "adiabatic_demagnetization"
    THERMOELECTRIC = "thermoelectric"


@dataclass
class SubstituteCharacteristics:
    """Characteristics of a substitute material"""
    name: SubstituteMaterial
    feasibility_score: float  # 0-1 (technical feasibility)
    cost_premium: float  # multiplier vs baseline helium
    helium_reduction: float  # 0-1 reduction in helium use
    carbon_impact: float  # multiplier vs baseline
    power_overhead: float  # power multiplier
    reliability_score: float  # 0-1
    readiness_level: int  # 1-9 (TRL)
    supply_availability: float  # 0-1


@dataclass
class SubstitutionDecision:
    """Decision output from substitution engine"""
    adopt_substitute: bool
    recommended_substitute: Optional[SubstituteMaterial]
    helium_savings_liters: float
    cost_increase_usd: float
    carbon_impact_kg: float
    power_increase_watts: float
    feasibility: float
    recommendation_reasoning: str


@dataclass
class SubstitutionEvaluation:
    """Complete evaluation of substitution options"""
    current_helium_usage_liters: float
    alternatives: List[Tuple[SubstituteMaterial, SubstituteCharacteristics]]
    best_alternative: Optional[SubstituteMaterial]
    switching_threshold_price_usd: float
    switching_recommended: bool


class MaterialSubstitutionEngine:
    """
    Material substitution decision engine for helium-intensive operations.
    
    Evaluates alternatives based on:
    - Technical feasibility
    - Cost premium
    - Helium reduction
    - Carbon impact
    - Readiness level
    """
    
    # Substitute material data
    SUBSTITUTE_DATA = {
        SubstituteMaterial.CRYOCOOLER: SubstituteCharacteristics(
            name=SubstituteMaterial.CRYOCOOLER,
            feasibility_score=0.95,
            cost_premium=2.5,
            helium_reduction=0.90,
            carbon_impact=1.2,
            power_overhead=3.0,
            reliability_score=0.95,
            readiness_level=9,  # Commercial
            supply_availability=0.85
        ),
        SubstituteMaterial.NEON: SubstituteCharacteristics(
            name=SubstituteMaterial.NEON,
            feasibility_score=0.70,
            cost_premium=1.8,
            helium_reduction=0.50,
            carbon_impact=0.9,
            power_overhead=1.5,
            reliability_score=0.85,
            readiness_level=7,  # Demonstration
            supply_availability=0.70
        ),
        SubstituteMaterial.HYDROGEN: SubstituteCharacteristics(
            name=SubstituteMaterial.HYDROGEN,
            feasibility_score=0.65,
            cost_premium=2.0,
            helium_reduction=0.60,
            carbon_impact=0.8,
            power_overhead=2.0,
            reliability_score=0.80,
            readiness_level=6,  # Prototype
            supply_availability=0.60
        ),
        SubstituteMaterial.NITROGEN: SubstituteCharacteristics(
            name=SubstituteMaterial.NITROGEN,
            feasibility_score=0.50,
            cost_premium=0.5,
            helium_reduction=0.95,
            carbon_impact=1.5,
            power_overhead=4.0,
            reliability_score=0.70,
            readiness_level=5,  # Validation
            supply_availability=0.95
        ),
        SubstituteMaterial.ADIABATIC_DEMAG: SubstituteCharacteristics(
            name=SubstituteMaterial.ADIABATIC_DEMAG,
            feasibility_score=0.60,
            cost_premium=4.0,
            helium_reduction=0.95,
            carbon_impact=0.8,
            power_overhead=2.0,
            reliability_score=0.75,
            readiness_level=4,  # Lab
            supply_availability=0.40
        ),
        SubstituteMaterial.THERMOELECTRIC: SubstituteCharacteristics(
            name=SubstituteMaterial.THERMOELECTRIC,
            feasibility_score=0.55,
            cost_premium=3.0,
            helium_reduction=0.98,
            carbon_impact=1.8,
            power_overhead=5.0,
            reliability_score=0.65,
            readiness_level=4,  # Lab
            supply_availability=0.80
        )
    }
    
    # Weight factors for multi-criteria decision analysis
    MCDA_WEIGHTS = {
        'feasibility': 0.25,
        'cost': 0.20,
        'helium_reduction': 0.25,
        'carbon': 0.15,
        'reliability': 0.10,
        'readiness': 0.05
    }
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.helium_price = self.config.get('helium_price_usd', 8.0)
        self.carbon_price_usd_per_kg = self.config.get('carbon_price_usd_per_kg', 50.0)
        self.electricity_price_usd_per_kwh = self.config.get('electricity_price_usd_per_kwh', 0.10)
        
    def evaluate_substitutes(self, helium_requirement_liters: float,
                            power_consumption_watts: float) -> SubstitutionEvaluation:
        """
        Evaluate all substitute materials for a given helium requirement.
        
        Args:
            helium_requirement_liters: Helium needed for operation
            power_consumption_watts: Power consumption of original system
            
        Returns:
            SubstitutionEvaluation with rankings and recommendations
        """
        alternatives = []
        
        for material, data in self.SUBSTITUTE_DATA.items():
            # Calculate total cost with substitute
            helium_cost_saved = helium_requirement_liters * self.helium_price * data.helium_reduction
            
            # Additional power cost
            additional_power_watts = power_consumption_watts * (data.power_overhead - 1)
            additional_power_cost = additional_power_watts * 24 * 365 * self.electricity_price_usd_per_kwh / 1000
            
            # Additional carbon cost
            base_carbon = power_consumption_watts * 24 * 365 * 0.4 / 1000  # ~0.4 kg CO2/kWh
            additional_carbon = base_carbon * (data.carbon_impact - 1)
            carbon_cost = additional_carbon * self.carbon_price_usd_per_kg
            
            total_cost_increase = (data.cost_premium - 1) * helium_requirement_liters * self.helium_price
            total_cost_increase += additional_power_cost + carbon_cost - helium_cost_saved
            
            # Calculate weighted score for MCDA
            normalized_scores = self._normalize_scores(data)
            mcda_score = sum(normalized_scores[key] * self.MCDA_WEIGHTS[key] 
                           for key in self.MCDA_WEIGHTS.keys())
            
            alternatives.append((material, data, mcda_score, total_cost_increase))
        
        # Sort by MCDA score (higher is better)
        alternatives.sort(key=lambda x: x[2], reverse=True)
        
        best_material = alternatives[0][0] if alternatives else None
        best_score = alternatives[0][2] if alternatives else 0
        
        # Determine switching threshold (helium price where best alternative becomes economical)
        switching_threshold = self._calculate_switching_threshold(helium_requirement_liters, 
                                                                   power_consumption_watts,
                                                                   best_material)
        
        switching_recommended = self.helium_price >= switching_threshold and best_score > 0.6
        
        return SubstitutionEvaluation(
            current_helium_usage_liters=helium_requirement_liters,
            alternatives=[(a[0], a[1]) for a in alternatives],
            best_alternative=best_material,
            switching_threshold_price_usd=switching_threshold,
            switching_recommended=switching_recommended
        )
    
    def _normalize_scores(self, data: SubstituteCharacteristics) -> Dict[str, float]:
        """Normalize scores for MCDA (0-1 scale, higher is better)"""
        # For cost_premium, lower is better
        cost_score = 1 / data.cost_premium if data.cost_premium > 0 else 0
        
        # For carbon_impact, lower is better
        carbon_score = 1 / data.carbon_impact if data.carbon_impact > 0 else 0
        
        return {
            'feasibility': data.feasibility_score,
            'cost': cost_score,
            'helium_reduction': data.helium_reduction,
            'carbon': carbon_score,
            'reliability': data.reliability_score,
            'readiness': data.readiness_level / 9.0
        }
    
    def _calculate_switching_threshold(self, helium_requirement_liters: float,
                                        power_consumption_watts: float,
                                        substitute_material: SubstituteMaterial) -> float:
        """
        Calculate helium price threshold where switching becomes economical.
        
        Returns price in USD per liter.
        """
        data = self.SUBSTITUTE_DATA[substitute_material]
        
        # Additional costs from power and carbon
        additional_power_watts = power_consumption_watts * (data.power_overhead - 1)
        additional_power_cost = additional_power_watts * 24 * 365 * self.electricity_price_usd_per_kwh / 1000
        
        base_carbon = power_consumption_watts * 24 * 365 * 0.4 / 1000
        additional_carbon = base_carbon * (data.carbon_impact - 1)
        annual_carbon_cost = additional_carbon * self.carbon_price_usd_per_kg
        
        # Annual operating cost increase (excluding helium)
        annual_opex_increase = additional_power_cost + annual_carbon_cost
        
        # Helium savings per year (assuming continuous operation)
        helium_saved_annual = helium_requirement_liters * 365 * data.helium_reduction
        
        # Price threshold = (capex + opex) / helium_saved
        capex_increase = (data.cost_premium - 1) * helium_requirement_liters * self.helium_price
        
        threshold = (capex_increase + annual_opex_increase) / helium_saved_annual if helium_saved_annual > 0 else float('inf')
        
        return max(5.0, min(20.0, threshold))
    
    def should_switch(self, helium_requirement_liters: float,
                     power_consumption_watts: float,
                     current_helium_price: float) -> SubstitutionDecision:
        """
        Determine if switching to a substitute material is recommended.
        
        Main interface for Layer 10 integration.
        """
        evaluation = self.evaluate_substitutes(helium_requirement_liters, power_consumption_watts)
        
        if not evaluation.switching_recommended or evaluation.best_alternative is None:
            return SubstitutionDecision(
                adopt_substitute=False,
                recommended_substitute=None,
                helium_savings_liters=0,
                cost_increase_usd=0,
                carbon_impact_kg=0,
                power_increase_watts=0,
                feasibility=0,
                recommendation_reasoning=f"Helium price ${current_helium_price:.2f}/L below switching threshold ${evaluation.switching_threshold_price_usd:.2f}/L"
            )
        
        best_material = evaluation.best_alternative
        best_data = self.SUBSTITUTE_DATA[best_material]
        
        helium_savings = helium_requirement_liters * best_data.helium_reduction
        cost_increase = (best_data.cost_premium - 1) * helium_requirement_liters * current_helium_price
        carbon_impact = power_consumption_watts * 24 * 365 * 0.4 / 1000 * (best_data.carbon_impact - 1)
        power_increase = power_consumption_watts * (best_data.power_overhead - 1)
        
        return SubstitutionDecision(
            adopt_substitute=True,
            recommended_substitute=best_material,
            helium_savings_liters=helium_savings,
            cost_increase_usd=max(0, cost_increase),
            carbon_impact_kg=max(0, carbon_impact),
            power_increase_watts=power_increase,
            feasibility=best_data.feasibility_score,
            recommendation_reasoning=f"Switch to {best_material.value} saves {helium_savings:.1f}L helium at ${cost_increase:.2f} cost increase"
        )
    
    def get_substitution_metrics(self) -> Dict:
        """Get substitution metrics for dashboard"""
        return {
            'available_substitutes': [m.value for m in self.SUBSTITUTE_DATA.keys()],
            'highest_readiness': max(self.SUBSTITUTE_DATA.items(), key=lambda x: x[1].readiness_level)[0].value,
            'best_helium_reduction': max(self.SUBSTITUTE_DATA.items(), key=lambda x: x[1].helium_reduction)[0].value,
            'most_feasible': max(self.SUBSTITUTE_DATA.items(), key=lambda x: x[1].feasibility_score)[0].value
        }
