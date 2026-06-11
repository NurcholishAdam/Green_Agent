# File: enhancements/moe_expert_system/sustainability/biodiversity_impact.py

"""
Biodiversity Impact Assessment for Green Agent
Evaluates and mitigates biodiversity impacts of computational decisions.
"""

import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class EcosystemType(Enum):
    TROPICAL_FOREST = "tropical_forest"
    TEMPERATE_FOREST = "temperate_forest"
    GRASSLAND = "grassland"
    WETLAND = "wetland"
    MARINE = "marine"
    FRESHWATER = "freshwater"
    URBAN = "urban"
    DESERT = "desert"

class ImpactCategory(Enum):
    HABITAT_LOSS = "habitat_loss"
    SPECIES_DISPLACEMENT = "species_displacement"
    WATER_POLLUTION = "water_pollution"
    AIR_POLLUTION = "air_pollution"
    NOISE_POLLUTION = "noise_pollution"
    LIGHT_POLLUTION = "light_pollution"
    THERMAL_POLLUTION = "thermal_pollution"
    RESOURCE_DEPLETION = "resource_depletion"

@dataclass
class BiodiversityMetric:
    """Track biodiversity metrics for decision making"""
    ecosystem_type: EcosystemType
    species_richness: int
    endangered_species_count: int
    habitat_area_km2: float
    fragmentation_index: float
    ecological_connectivity: float
    last_assessment: datetime

class BiodiversityImpactAssessor:
    """
    Assesses and mitigates biodiversity impacts of Green Agent operations.
    
    Integrates with:
    - Expert routing decisions
    - Hardware lifecycle management
    - Carbon offset strategies
    - Location-based computing decisions
    """
    
    def __init__(self):
        self.ecosystems: Dict[str, BiodiversityMetric] = {}
        self.impact_history: List[Dict] = []
        self.mitigation_strategies: Dict[str, List[Dict]] = {}
        
        # Biodiversity scores
        self.local_biodiversity_score = 0.0
        self.global_biodiversity_score = 0.0
        
        # Initialize with sample ecosystems
        self._initialize_ecosystems()
        
        logger.info("Biodiversity Impact Assessor initialized")
    
    def _initialize_ecosystems(self):
        """Initialize ecosystem tracking"""
        sample_ecosystems = {
            'amazon_rainforest': BiodiversityMetric(
                ecosystem_type=EcosystemType.TROPICAL_FOREST,
                species_richness=16000,
                endangered_species_count=120,
                habitat_area_km2=5500000,
                fragmentation_index=0.15,
                ecological_connectivity=0.85,
                last_assessment=datetime.utcnow()
            ),
            'coral_reef_pacific': BiodiversityMetric(
                ecosystem_type=EcosystemType.MARINE,
                species_richness=4000,
                endangered_species_count=45,
                habitat_area_km2=50000,
                fragmentation_index=0.30,
                ecological_connectivity=0.70,
                last_assessment=datetime.utcnow()
            ),
            'european_wetlands': BiodiversityMetric(
                ecosystem_type=EcosystemType.WETLAND,
                species_richness=2500,
                endangered_species_count=30,
                habitat_area_km2=150000,
                fragmentation_index=0.25,
                ecological_connectivity=0.60,
                last_assessment=datetime.utcnow()
            )
        }
        
        self.ecosystems = sample_ecosystems
    
    async def assess_expert_impact(
        self,
        expert_type: str,
        location: Dict[str, Any],
        energy_source: str,
        cooling_method: str
    ) -> Dict[str, Any]:
        """
        Assess biodiversity impact of expert execution
        
        Args:
            expert_type: Type of expert being executed
            location: Geographic location of execution
            energy_source: Energy source (renewable, fossil, etc.)
            cooling_method: Cooling method (air, water, helium, etc.)
            
        Returns:
            Impact assessment with mitigation recommendations
        """
        impact_scores = {}
        total_impact = 0.0
        
        # Assess habitat impact based on location
        habitat_impact = self._assess_habitat_impact(location)
        impact_scores['habitat'] = habitat_impact
        total_impact += habitat_impact['score']
        
        # Assess energy source impact
        energy_impact = self._assess_energy_impact(energy_source, location)
        impact_scores['energy'] = energy_impact
        total_impact += energy_impact['score']
        
        # Assess cooling method impact
        cooling_impact = self._assess_cooling_impact(cooling_method, location)
        impact_scores['cooling'] = cooling_impact
        total_impact += cooling_impact['score']
        
        # Assess resource extraction impact
        resource_impact = self._assess_resource_impact(expert_type)
        impact_scores['resources'] = resource_impact
        total_impact += resource_impact['score']
        
        # Normalize total impact
        total_impact = total_impact / 4.0
        
        # Generate mitigation strategies
        mitigation = self._generate_mitigation_strategies(
            impact_scores, expert_type, location
        )
        
        assessment = {
            'expert_type': expert_type,
            'location': location,
            'total_biodiversity_impact': total_impact,
            'impact_breakdown': impact_scores,
            'mitigation_strategies': mitigation,
            'recommendations': self._generate_recommendations(impact_scores),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        self.impact_history.append(assessment)
        
        # Update scores
        self._update_biodiversity_scores(assessment)
        
        return assessment
    
    def _assess_habitat_impact(
        self,
        location: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Assess habitat impact of computing location"""
        # Get nearest ecosystem
        nearest_ecosystem = self._find_nearest_ecosystem(location)
        
        if not nearest_ecosystem:
            return {'score': 0.1, 'category': 'minimal', 'ecosystem': None}
        
        # Calculate impact based on proximity and ecosystem sensitivity
        distance_km = location.get('distance_to_ecosystem_km', 100)
        ecosystem = self.ecosystems[nearest_ecosystem]
        
        # Proximity impact
        if distance_km < 1:
            proximity_factor = 1.0
        elif distance_km < 10:
            proximity_factor = 0.7
        elif distance_km < 50:
            proximity_factor = 0.3
        else:
            proximity_factor = 0.1
        
        # Sensitivity based on endangered species
        sensitivity = ecosystem.endangered_species_count / 200.0
        sensitivity = min(sensitivity, 1.0)
        
        # Fragmentation impact
        fragmentation_factor = ecosystem.fragmentation_index
        
        # Combined score
        score = (proximity_factor * 0.4 + sensitivity * 0.4 + fragmentation_factor * 0.2)
        
        return {
            'score': score,
            'category': 'critical' if score > 0.7 else 'moderate' if score > 0.3 else 'low',
            'ecosystem': nearest_ecosystem,
            'proximity_factor': proximity_factor,
            'sensitivity': sensitivity
        }
    
    def _assess_energy_impact(
        self,
        energy_source: str,
        location: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Assess impact of energy source on biodiversity"""
        # Energy source impact factors
        impact_factors = {
            'solar': 0.05,
            'wind': 0.08,
            'hydroelectric': 0.15,
            'geothermal': 0.03,
            'nuclear': 0.10,
            'natural_gas': 0.40,
            'coal': 0.80,
            'oil': 0.90,
            'biomass': 0.30,
            'mixed_grid': 0.35
        }
        
        base_impact = impact_factors.get(energy_source, 0.5)
        
        # Adjust for location-specific factors
        if location.get('near_water_body'):
            if energy_source in ['hydroelectric', 'nuclear']:
                base_impact *= 1.5  # Increased impact near water
        
        if location.get('in_migration_corridor'):
            if energy_source in ['wind']:
                base_impact *= 1.3  # Wind turbines in migration paths
        
        return {
            'score': base_impact,
            'energy_source': energy_source,
            'category': 'high' if base_impact > 0.5 else 'moderate' if base_impact > 0.2 else 'low'
        }
    
    def _assess_cooling_impact(
        self,
        cooling_method: str,
        location: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Assess impact of cooling method on biodiversity"""
        # Cooling method impact factors
        impact_factors = {
            'air_cooling': 0.05,
            'evaporative_cooling': 0.15,
            'water_cooling': 0.25,
            'helium_cooling': 0.10,
            'geothermal_cooling': 0.03,
            'liquid_immersion': 0.20,
            'free_cooling': 0.02
        }
        
        base_impact = impact_factors.get(cooling_method, 0.15)
        
        # Water-based cooling in water-scarce areas
        if cooling_method in ['water_cooling', 'evaporative_cooling']:
            if location.get('water_scarcity_index', 0) > 0.7:
                base_impact *= 2.0
            elif location.get('water_scarcity_index', 0) > 0.4:
                base_impact *= 1.5
        
        # Thermal pollution risk
        if cooling_method in ['water_cooling', 'liquid_immersion']:
            if location.get('near_water_body'):
                base_impact *= 1.3
        
        return {
            'score': base_impact,
            'cooling_method': cooling_method,
            'category': 'high' if base_impact > 0.5 else 'moderate' if base_impact > 0.2 else 'low'
        }
    
    def _assess_resource_impact(
        self,
        expert_type: str
    ) -> Dict[str, Any]:
        """Assess impact of resource extraction for expert hardware"""
        # Resource intensity by expert type
        resource_impacts = {
            'energy_expert': {'rare_earth': 0.1, 'copper': 0.05, 'overall': 0.08},
            'data_expert': {'rare_earth': 0.15, 'copper': 0.1, 'overall': 0.12},
            'iot_expert': {'rare_earth': 0.05, 'copper': 0.02, 'overall': 0.04},
            'quantum_expert': {'rare_earth': 0.3, 'copper': 0.2, 'overall': 0.25},
            'helium_expert': {'rare_earth': 0.08, 'copper': 0.05, 'overall': 0.06}
        }
        
        impact = resource_impacts.get(expert_type, {'overall': 0.1})
        
        return {
            'score': impact['overall'],
            'expert_type': expert_type,
            'category': 'high' if impact['overall'] > 0.2 else 'moderate' if impact['overall'] > 0.1 else 'low'
        }
    
    def _find_nearest_ecosystem(
        self,
        location: Dict[str, Any]
    ) -> Optional[str]:
        """Find nearest ecosystem to computing location"""
        # Simplified - in production would use GIS data
        if location.get('latitude', 0) < 0:
            return 'amazon_rainforest'
        elif location.get('latitude', 0) > 45:
            return 'european_wetlands'
        else:
            return 'coral_reef_pacific'
    
    def _generate_mitigation_strategies(
        self,
        impact_scores: Dict[str, Any],
        expert_type: str,
        location: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Generate mitigation strategies for biodiversity impacts"""
        strategies = []
        
        # Habitat mitigation
        if impact_scores['habitat']['score'] > 0.5:
            strategies.append({
                'type': 'habitat_protection',
                'action': 'Relocate computation to lower-impact area',
                'impact_reduction': 0.6,
                'cost': 'medium',
                'implementation_time': 'short'
            })
            strategies.append({
                'type': 'habitat_restoration',
                'action': 'Invest in local habitat restoration project',
                'impact_reduction': 0.4,
                'cost': 'high',
                'implementation_time': 'long'
            })
        
        # Energy mitigation
        if impact_scores['energy']['score'] > 0.3:
            strategies.append({
                'type': 'renewable_energy',
                'action': 'Switch to renewable energy sources',
                'impact_reduction': 0.7,
                'cost': 'medium',
                'implementation_time': 'medium'
            })
        
        # Cooling mitigation
        if impact_scores['cooling']['score'] > 0.3:
            strategies.append({
                'type': 'efficient_cooling',
                'action': 'Implement free cooling or geothermal cooling',
                'impact_reduction': 0.5,
                'cost': 'medium',
                'implementation_time': 'medium'
            })
        
        # Resource mitigation
        if impact_scores['resources']['score'] > 0.15:
            strategies.append({
                'type': 'circular_economy',
                'action': 'Use recycled materials and extend hardware life',
                'impact_reduction': 0.4,
                'cost': 'low',
                'implementation_time': 'short'
            })
        
        return strategies
    
    def _generate_recommendations(
        self,
        impact_scores: Dict[str, Any]
    ) -> List[str]:
        """Generate actionable recommendations"""
        recommendations = []
        
        scores = {
            category: scores['score']
            for category, scores in impact_scores.items()
        }
        
        # Find highest impact category
        highest_impact = max(scores.items(), key=lambda x: x[1])
        
        if highest_impact[0] == 'habitat' and highest_impact[1] > 0.5:
            recommendations.append(
                "HIGH PRIORITY: Relocate computation to avoid sensitive ecosystems"
            )
        elif highest_impact[0] == 'energy' and highest_impact[1] > 0.5:
            recommendations.append(
                "HIGH PRIORITY: Switch to renewable energy to reduce biodiversity impact"
            )
        elif highest_impact[0] == 'cooling' and highest_impact[1] > 0.5:
            recommendations.append(
                "HIGH PRIORITY: Implement water-free cooling to protect aquatic ecosystems"
            )
        
        # General recommendations
        if all(score < 0.2 for score in scores.values()):
            recommendations.append(
                "Current setup has minimal biodiversity impact - maintain standards"
            )
        else:
            recommendations.append(
                "Consider biodiversity offsets equivalent to 110% of calculated impact"
            )
        
        return recommendations
    
    def _update_biodiversity_scores(self, assessment: Dict[str, Any]):
        """Update biodiversity scores based on assessment"""
        # Weighted moving average
        alpha = 0.1
        
        self.local_biodiversity_score = (
            (1 - alpha) * self.local_biodiversity_score +
            alpha * assessment['total_biodiversity_impact']
        )
        
        self.global_biodiversity_score = (
            (1 - alpha * 0.5) * self.global_biodiversity_score +
            alpha * 0.5 * assessment['total_biodiversity_impact']
        )
    
    def get_biodiversity_report(self) -> Dict[str, Any]:
        """Generate comprehensive biodiversity impact report"""
        recent_impacts = self.impact_history[-50:] if self.impact_history else []
        
        return {
            'local_biodiversity_score': self.local_biodiversity_score,
            'global_biodiversity_score': self.global_biodiversity_score,
            'ecosystems_tracked': len(self.ecosystems),
            'recent_impacts': [
                {
                    'expert_type': i['expert_type'],
                    'impact': i['total_biodiversity_impact'],
                    'timestamp': i['timestamp']
                }
                for i in recent_impacts[-10:]
            ],
            'high_risk_ecosystems': [
                name for name, eco in self.ecosystems.items()
                if eco.endangered_species_count > 50
            ],
            'mitigation_effectiveness': self._calculate_mitigation_effectiveness(),
            'recommendations': self._generate_global_recommendations()
        }
    
    def _calculate_mitigation_effectiveness(self) -> float:
        """Calculate effectiveness of mitigation strategies"""
        if not self.impact_history:
            return 0.0
        
        # Compare recent impacts with historical
        recent = self.impact_history[-20:]
        historical = self.impact_history[:-20]
        
        if not historical:
            return 0.5
        
        recent_avg = np.mean([i['total_biodiversity_impact'] for i in recent])
        historical_avg = np.mean([i['total_biodiversity_impact'] for i in historical])
        
        if historical_avg > 0:
            improvement = (historical_avg - recent_avg) / historical_avg
            return max(improvement, 0.0)
        
        return 0.0
    
    def _generate_global_recommendations(self) -> List[str]:
        """Generate global biodiversity recommendations"""
        recommendations = []
        
        if self.local_biodiversity_score > 0.5:
            recommendations.append(
                "CRITICAL: Implement immediate biodiversity protection measures"
            )
        
        if any(
            eco.endangered_species_count > 100
            for eco in self.ecosystems.values()
        ):
            recommendations.append(
                "URGENT: Avoid computing operations near critical habitats"
            )
        
        # Helium-related recommendation
        recommendations.append(
            "Implement helium recovery systems to reduce mining impact on biodiversity"
        )
        
        return recommendations
    
    def get_expert_routing_guidance(
        self,
        expert_options: List[str],
        location_options: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Get biodiversity-aware routing guidance for expert selection
        
        Returns:
            Guidance on which expert/location combinations minimize biodiversity impact
        """
        impact_assessments = []
        
        for expert in expert_options:
            for location in location_options:
                # Quick assessment
                assessment = {
                    'expert': expert,
                    'location': location.get('name', 'unknown'),
                    'estimated_impact': self._quick_impact_estimate(expert, location)
                }
                impact_assessments.append(assessment)
        
        # Sort by impact
        impact_assessments.sort(key=lambda x: x['estimated_impact'])
        
        return {
            'best_option': impact_assessments[0] if impact_assessments else None,
            'worst_option': impact_assessments[-1] if impact_assessments else None,
            'all_options': impact_assessments,
            'recommendation': (
                f"Use {impact_assessments[0]['expert']} at {impact_assessments[0]['location']}"
                if impact_assessments else "No options available"
            )
        }
    
    def _quick_impact_estimate(
        self,
        expert_type: str,
        location: Dict[str, Any]
    ) -> float:
        """Quick estimate of biodiversity impact"""
        # Simplified scoring
        location_sensitivity = location.get('biodiversity_sensitivity', 0.5)
        expert_intensity = {
            'energy': 0.3,
            'data': 0.4,
            'iot': 0.2,
            'quantum': 0.6,
            'helium': 0.35
        }.get(expert_type, 0.4)
        
        return location_sensitivity * expert_intensity
