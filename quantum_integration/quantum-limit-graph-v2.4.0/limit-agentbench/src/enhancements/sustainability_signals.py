# src/enhancements/sustainability_signals.py
"""
Enhanced sustainability signals for data center selection.

Adds water usage effectiveness (WUE), embodied carbon, e-waste policies,
and other ESG metrics to the data center evaluation.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class WaterMetrics:
    """Water-related sustainability metrics"""
    wue_water_usage_effectiveness: float = 1.8  # L/kWh
    water_source_renewable_pct: float = 50.0
    water_stress_index: float = 0.5  # 0-1, higher = more stressed
    cooling_water_recycled_pct: float = 70.0
    wastewater_treatment_score: float = 0.8  # 0-1


@dataclass
class CarbonMetrics:
    """Carbon-related metrics beyond operational emissions"""
    embodied_carbon_kgco2_per_kw: float = 1000  # kg CO2 per kW capacity
    construction_carbon_kgco2: float = 5000000
    grid_carbon_intensity_gco2_per_kwh: float = 400
    renewable_energy_certificates_pct: float = 0
    carbon_offset_program: Optional[str] = None


@dataclass
class EwasteMetrics:
    """E-waste and circular economy metrics"""
    e_waste_recycling_rate_pct: float = 80.0
    server_lifetime_years: float = 4.0
    circular_economy_score: float = 0.7  # 0-1
    hazardous_material_compliance: bool = True
    rohs_compliant: bool = True


@dataclass
class SocialMetrics:
    """Social responsibility metrics"""
    local_employment_rate_pct: float = 90.0
    community_investment_usd_per_mw: float = 5000
    safety_record_score: float = 0.95
    diversity_score: float = 0.7


@dataclass
class EnhancedSustainabilitySignals:
    """Complete sustainability profile for a data center"""
    water: WaterMetrics = field(default_factory=WaterMetrics)
    carbon: CarbonMetrics = field(default_factory=CarbonMetrics)
    ewaste: EwasteMetrics = field(default_factory=EwasteMetrics)
    social: SocialMetrics = field(default_factory=SocialMetrics)
    
    # Combined scores
    overall_sustainability_score: float = 0.0
    water_score: float = 0.0
    carbon_score: float = 0.0
    circular_score: float = 0.0
    social_score: float = 0.0


class SustainabilitySignalEnricher:
    """
    Enriches data center projects with enhanced sustainability signals.
    
    Features:
    - Water Usage Effectiveness (WUE) estimation
    - Embodied carbon calculation
    - E-waste and circular economy metrics
    - Social responsibility indicators
    """
    
    def __init__(self):
        # Regional water stress indices (0-1, higher = more stressed)
        self.water_stress_by_country = {
            "USA": 0.4,
            "Finland": 0.1,
            "Sweden": 0.1,
            "Denmark": 0.2,
            "Ireland": 0.3,
            "Indonesia": 0.6,
            "Singapore": 0.9,
            "Saudi Arabia": 0.95,
            "UAE": 0.9,
            "Australia": 0.7,
            "China": 0.7,
            "Japan": 0.5,
            "South Korea": 0.5,
        }
        
        # Regional renewable energy percentages (approximate)
        self.renewable_by_country = {
            "Finland": 85, "Sweden": 95, "Denmark": 70, "Norway": 98,
            "Ireland": 45, "UK": 40, "Germany": 45, "France": 25,
            "USA": 22, "Indonesia": 15, "Singapore": 3, "Australia": 25,
            "China": 30, "Japan": 22, "South Korea": 8, "Saudi Arabia": 5, "UAE": 7,
        }
        
        # Cooling type WUE factors
        self.cooling_wue_factors = {
            "free": 0.5,      # L/kWh - free cooling uses minimal water
            "liquid": 1.2,    # L/kWh - direct-to-chip liquid cooling
            "air": 1.8,       # L/kWh - traditional air cooling with evaporative cooling
        }
    
    def estimate_water_metrics(self, country: str, cooling_type: str) -> WaterMetrics:
        """Estimate water-related metrics based on location and cooling"""
        water_stress = self.water_stress_by_country.get(country, 0.5)
        wue_base = self.cooling_wue_factors.get(cooling_type, 1.8)
        
        # Adjust WUE based on water stress (more stressed = more efficient systems)
        wue = wue_base * (1 - water_stress * 0.3)
        
        # Higher renewable cooling water source in water-scarce regions
        renewable_pct = 80 if water_stress > 0.7 else 50
        
        return WaterMetrics(
            wue_water_usage_effectiveness=wue,
            water_source_renewable_pct=renewable_pct,
            water_stress_index=water_stress,
            cooling_water_recycled_pct=70 if water_stress > 0.5 else 60,
            wastewater_treatment_score=0.9 if water_stress > 0.5 else 0.7
        )
    
    def estimate_embodied_carbon(self, capacity_mw: float, country: str) -> float:
        """Estimate embodied carbon for data center construction"""
        # Base embodied carbon per MW (kg CO2)
        base_embodied = 800  # tons CO2 per MW (simplified)
        
        # Regional construction carbon intensity factors
        regional_factors = {
            "Finland": 0.6, "Sweden": 0.6, "Denmark": 0.7,
            "USA": 1.0, "Singapore": 1.2, "Indonesia": 1.1,
            "China": 1.3, "Japan": 1.0, "Australia": 1.0,
        }
        factor = regional_factors.get(country, 1.0)
        
        return capacity_mw * base_embodied * factor * 1000  # kg CO2
    
    def estimate_ewaste_metrics(self, country: str, operator: str) -> EwasteMetrics:
        """
        Estimate e-waste and circular economy metrics.
        Uses operator reputation and country regulations.
        """
        # Operator e-waste scores (0-1)
        operator_ewaste_scores = {
            "Google": 0.9, "Microsoft": 0.85, "Amazon": 0.7, "Meta": 0.75,
            "Apple": 0.9, "Equinix": 0.7, "Digital Realty": 0.65,
        }
        operator_score = operator_ewaste_scores.get(operator, 0.5)
        
        # Country e-waste regulation strength
        country_regulation = {
            "Finland": 0.9, "Sweden": 0.9, "Denmark": 0.85, "Germany": 0.85,
            "USA": 0.6, "Singapore": 0.7, "Japan": 0.8, "South Korea": 0.75,
            "China": 0.5, "Indonesia": 0.4, "Australia": 0.7,
        }
        regulation_score = country_regulation.get(country, 0.5)
        
        # Combined score
        circular_score = (operator_score + regulation_score) / 2
        
        # Recycling rate based on regulation
        recycling_rate = 50 + regulation_score * 40
        
        return EwasteMetrics(
            e_waste_recycling_rate_pct=recycling_rate,
            server_lifetime_years=4.0,
            circular_economy_score=circular_score,
            hazardous_material_compliance=regulation_score > 0.5,
            rohs_compliant=regulation_score > 0.4
        )
    
    def estimate_social_metrics(self, country: str, capacity_mw: float) -> SocialMetrics:
        """Estimate social responsibility metrics"""
        # Employment rate by country (approximate)
        employment_rates = {
            "Finland": 95, "Sweden": 95, "Denmark": 94, "USA": 90,
            "Singapore": 95, "Indonesia": 85, "Australia": 92,
        }
        employment = employment_rates.get(country, 85)
        
        # Community investment (USD per MW)
        community_investment = 5000 + (capacity_mw / 10) * 100
        
        return SocialMetrics(
            local_employment_rate_pct=employment,
            community_investment_usd_per_mw=community_investment,
            safety_record_score=0.95,
            diversity_score=0.7
        )
    
    def calculate_scores(self, signals: EnhancedSustainabilitySignals) -> EnhancedSustainabilitySignals:
        """Calculate component scores and overall sustainability score"""
        # Water score (0-100)
        water_score = (
            (1 - signals.water.water_stress_index) * 40 +
            signals.water.cooling_water_recycled_pct / 100 * 30 +
            signals.water.wastewater_treatment_score * 30
        )
        
        # Carbon score (0-100)
        carbon_score = (
            (1 - signals.carbon.grid_carbon_intensity_gco2_per_kwh / 1000) * 50 +
            signals.carbon.renewable_energy_certificates_pct / 100 * 30 +
            (1 - signals.carbon.embodied_carbon_kgco2_per_kw / 2000) * 20
        )
        carbon_score = max(0, min(100, carbon_score))
        
        # Circular economy score (0-100)
        circular_score = (
            signals.ewaste.e_waste_recycling_rate_pct * 0.4 +
            signals.ewaste.circular_economy_score * 60
        )
        
        # Social score (0-100)
        social_score = (
            signals.social.local_employment_rate_pct * 0.4 +
            min(100, signals.social.community_investment_usd_per_mw / 100) * 0.3 +
            signals.social.safety_record_score * 30
        )
        
        # Overall sustainability score (weighted average)
        overall = (
            water_score * 0.25 +
            carbon_score * 0.35 +
            circular_score * 0.25 +
            social_score * 0.15
        )
        
        signals.water_score = water_score
        signals.carbon_score = carbon_score
        signals.circular_score = circular_score
        signals.social_score = social_score
        signals.overall_sustainability_score = overall
        
        return signals
    
    def enrich_project(self, project: Dict) -> EnhancedSustainabilitySignals:
        """
        Enrich a data center project with all sustainability signals.
        
        Args:
            project: Dict with keys: location_country, cooling_type, 
                    planned_power_capacity_mw, company
        """
        country = project.get('location_country', 'USA')
        cooling = project.get('cooling_type', 'air')
        capacity = project.get('planned_power_capacity_mw', 100)
        operator = project.get('company', 'Unknown')
        
        water = self.estimate_water_metrics(country, cooling)
        embodied = self.estimate_embodied_carbon(capacity, country)
        
        carbon = CarbonMetrics(
            embodied_carbon_kgco2_per_kw=embodied / capacity if capacity > 0 else 1000,
            construction_carbon_kgco2=embodied,
            grid_carbon_intensity_gco2_per_kwh=self._get_grid_carbon(country),
            renewable_energy_certificates_pct=self.renewable_by_country.get(country, 20),
            carbon_offset_program="Verified Carbon Standard" if country in ["Finland", "Sweden"] else None
        )
        
        ewaste = self.estimate_ewaste_metrics(country, operator)
        social = self.estimate_social_metrics(country, capacity)
        
        signals = EnhancedSustainabilitySignals(
            water=water, carbon=carbon, ewaste=ewaste, social=social
        )
        
        return self.calculate_scores(signals)
    
    def _get_grid_carbon(self, country: str) -> float:
        """Get grid carbon intensity (gCO2/kWh)"""
        carbon_intensities = {
            "Finland": 85, "Sweden": 45, "Denmark": 120, "Norway": 35,
            "Ireland": 250, "UK": 200, "Germany": 350, "France": 60,
            "USA": 380, "Indonesia": 680, "Singapore": 400, "Australia": 550,
            "China": 550, "Japan": 450, "South Korea": 420, "Saudi Arabia": 550, "UAE": 480,
        }
        return carbon_intensities.get(country, 400)


# Demo
if __name__ == "__main__":
    enricher = SustainabilitySignalEnricher()
    
    # Example project
    project = {
        "location_country": "Indonesia",
        "cooling_type": "air",
        "planned_power_capacity_mw": 100,
        "company": "Princeton Digital"
    }
    
    signals = enricher.enrich_project(project)
    
    print("\n=== Enhanced Sustainability Signals ===")
    print(f"Water Score: {signals.water_score:.1f}/100")
    print(f"  - WUE: {signals.water.wue_water_usage_effectiveness:.2f} L/kWh")
    print(f"  - Water Stress Index: {signals.water.water_stress_index:.2f}")
    print(f"\nCarbon Score: {signals.carbon_score:.1f}/100")
    print(f"  - Grid Carbon: {signals.carbon.grid_carbon_intensity_gco2_per_kwh:.0f} gCO2/kWh")
    print(f"  - Embodied Carbon: {signals.carbon.embodied_carbon_kgco2_per_kw:.0f} kg CO2/kW")
    print(f"\nCircular Score: {signals.circular_score:.1f}/100")
    print(f"  - E-waste Recycling: {signals.ewaste.e_waste_recycling_rate_pct:.0f}%")
    print(f"\nSocial Score: {signals.social_score:.1f}/100")
    print(f"  - Local Employment: {signals.social.local_employment_rate_pct:.0f}%")
    print(f"\nOverall Sustainability Score: {signals.overall_sustainability_score:.1f}/100")
