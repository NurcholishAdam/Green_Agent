# src/enhancements/green_datacenter_selector.py
"""
Green Data Center Selector for Green Agent

Given workload specifications (GPU hours, latency tolerance, jurisdiction),
selects the optimal data center maximizing Green Score subject to constraints.
"""

from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import math
import logging
from .ai_data_center_loader import AIDataCenterLoader, AIDataCenterProject

logger = logging.getLogger(__name__)


@dataclass
class WorkloadSpec:
    """Specification of a workload to be placed"""
    gpu_hours: float = 100.0
    model_size_gb: float = 10.0
    latency_tolerance_ms: float = 100.0
    jurisdiction_requirements: List[str] = None  # e.g., ["EU", "GDPR"]
    workload_type: str = "training"  # training, inference, batch
    carbon_budget_kg: Optional[float] = None
    max_cost_usd: Optional[float] = None


@dataclass
class SelectionResult:
    """Result of data center selection"""
    selected_project: AIDataCenterProject
    green_score: float
    estimated_energy_kwh: float
    estimated_carbon_kg: float
    estimated_cost_usd: float
    latency_ms: float
    reasoning: str
    alternatives: List[Tuple[AIDataCenterProject, float]]  # (project, green_score)


class GreenDatacenterSelector:
    """
    Selects optimal data center for workload based on Green Score and constraints.
    
    Features:
    - Multi-objective optimization (green score + latency + cost)
    - Constraint satisfaction (jurisdiction, carbon budget, cost)
    - Explanation generation for selections
    - Ranking and alternative suggestions
    """
    
    def __init__(self, loader: Optional[AIDataCenterLoader] = None):
        self.loader = loader or AIDataCenterLoader()
        self._latency_cache = {}
    
    def _estimate_latency(self, project: AIDataCenterProject, user_region: str = "us-east") -> float:
        """
        Estimate network latency based on geographic distance.
        Rough approximation: 0.5 ms per 100 km + fixed overhead.
        """
        # Simplified: distance-based estimation
        # In production, use actual cloud provider latency data
        region_centers = {
            "us-east": (39.0, -77.0),   # Ashburn
            "us-west": (37.8, -122.4),  # San Francisco
            "eu-west": (53.3, -6.2),    # Dublin
            "asia-east": (22.3, 114.2), # Hong Kong
            "apac-southeast": (-6.2, 106.8),  # Jakarta
        }
        
        center = region_centers.get(user_region, (0, 0))
        dx = (project.longitude - center[1]) * 85  # km per degree
        dy = (project.latitude - center[0]) * 111
        distance_km = math.sqrt(dx*dx + dy*dy)
        
        # 0.5 ms per 100 km + 10 ms baseline
        latency = 10 + distance_km / 200
        return latency
    
    def _estimate_energy(self, project: AIDataCenterProject, workload: WorkloadSpec) -> float:
        """
        Estimate energy consumption for workload at this data center.
        Energy ∝ GPU hours × PUE (less efficient cooling = more energy)
        """
        # Base energy per GPU hour (kWh)
        base_energy_per_hour = 0.65  # ~650W per GPU
        pue = project.sustainability.pue_estimated
        energy_kwh = workload.gpu_hours * base_energy_per_hour * pue
        return energy_kwh
    
    def _estimate_cost(self, project: AIDataCenterProject, energy_kwh: float) -> float:
        """
        Estimate cost based on local electricity price and other factors.
        """
        # Rough electricity prices by region (USD/kWh)
        regional_prices = {
            "USA": 0.07,
            "Finland": 0.05,
            "Ireland": 0.10,
            "Sweden": 0.04,
            "Denmark": 0.08,
            "Indonesia": 0.09,
            "Saudi Arabia": 0.03,
            "China": 0.08,
            "Japan": 0.12,
            "Singapore": 0.11,
            "South Korea": 0.10,
            "UAE": 0.06,
            "Australia": 0.09,
        }
        price = regional_prices.get(project.location_country, 0.08)
        return energy_kwh * price
    
    def _calculate_carbon(self, energy_kwh: float, project: AIDataCenterProject) -> float:
        """Calculate carbon emissions (kg CO2)"""
        intensity = project.sustainability.grid_carbon_intensity_gco2_per_kwh / 1000  # kg/kWh
        return energy_kwh * intensity
    
    def select_datacenter(self, workload: WorkloadSpec, user_region: str = "us-east") -> SelectionResult:
        """
        Select optimal data center for the workload.
        
        Returns the best matching data center with explanation.
        """
        candidates = self.loader.get_all_projects()
        
        # Filter by jurisdiction requirements
        if workload.jurisdiction_requirements:
            filtered = []
            for p in candidates:
                # Simple jurisdiction matching (extend as needed)
                if "EU" in workload.jurisdiction_requirements and p.location_country in ["Finland", "Ireland", "Sweden", "Denmark"]:
                    filtered.append(p)
                elif "US" in workload.jurisdiction_requirements and p.location_country == "USA":
                    filtered.append(p)
                else:
                    filtered.append(p)  # no jurisdiction filter
            candidates = filtered
        
        scored = []
        for p in candidates:
            energy = self._estimate_energy(p, workload)
            carbon = self._calculate_carbon(energy, p)
            cost = self._estimate_cost(p, energy)
            latency = self._estimate_latency(p, user_region)
            
            # Check constraints
            if workload.carbon_budget_kg and carbon > workload.carbon_budget_kg:
                continue
            if workload.max_cost_usd and cost > workload.max_cost_usd:
                continue
            if latency > workload.latency_tolerance_ms:
                continue
            
            # Score: weighted combination of green score, latency, cost
            normalized_latency = max(0, 1 - latency / workload.latency_tolerance_ms) if workload.latency_tolerance_ms > 0 else 1
            normalized_cost = max(0, 1 - cost / workload.max_cost_usd) if workload.max_cost_usd else 1
            
            # Weights: green score 50%, latency 30%, cost 20%
            combined_score = (
                p.green_score / 100 * 0.5 +
                normalized_latency * 0.3 +
                normalized_cost * 0.2
            )
            
            scored.append((p, combined_score, energy, carbon, cost, latency))
        
        if not scored:
            # Fallback: best green score without constraints
            scored = [(p, p.green_score / 100, self._estimate_energy(p, workload), 0, 0, 0) 
                     for p in candidates]
        
        # Sort by combined score
        scored.sort(key=lambda x: x[1], reverse=True)
        
        best = scored[0]
        best_project = best[0]
        best_energy = best[2]
        best_carbon = best[3]
        best_cost = best[4]
        best_latency = best[5]
        
        # Generate explanation
        reasoning = self._generate_explanation(best_project, workload, best_carbon, best_latency)
        
        # Alternatives (next 3 best)
        alternatives = [(s[0], s[0].green_score) for s in scored[1:4]]
        
        return SelectionResult(
            selected_project=best_project,
            green_score=best_project.green_score,
            estimated_energy_kwh=best_energy,
            estimated_carbon_kg=best_carbon,
            estimated_cost_usd=best_cost,
            latency_ms=best_latency,
            reasoning=reasoning,
            alternatives=alternatives
        )
    
    def _generate_explanation(self, project: AIDataCenterProject, workload: WorkloadSpec,
                             carbon_kg: float, latency_ms: float) -> str:
        """Generate human-readable explanation for selection"""
        signals = project.sustainability
        
        # Carbon intensity comparison
        carbon_desc = "very low" if signals.grid_carbon_intensity_gco2_per_kwh < 100 else \
                     "low" if signals.grid_carbon_intensity_gco2_per_kwh < 300 else \
                     "medium" if signals.grid_carbon_intensity_gco2_per_kwh < 500 else "high"
        
        renewable_desc = "high" if signals.renewable_share_pct > 70 else \
                        "moderate" if signals.renewable_share_pct > 30 else "low"
        
        explanation = (
            f"I selected **{project.project_name}** in {project.location_city}, {project.location_country} "
            f"because its Green Score is {project.green_score:.1f}/100. "
            f"This site has {carbon_desc} carbon intensity ({signals.grid_carbon_intensity_gco2_per_kwh:.0f} gCO₂/kWh) "
            f"and {renewable_desc} renewable energy share ({signals.renewable_share_pct:.0f}%). "
            f"Estimated carbon for this workload: {carbon_kg:.2f} kg CO₂. "
            f"Latency is estimated at {latency_ms:.0f} ms, which meets your requirement of {workload.latency_tolerance_ms:.0f} ms."
        )
        
        # Add cooling efficiency note if applicable
        if signals.cooling_type == "free":
            explanation += f" Additionally, this data center uses free-air cooling (PUE {signals.pue_estimated:.2f}), "
            explanation += "further reducing energy waste."
        
        return explanation
    
    def rank_by_green_score(self, n: int = 10) -> List[AIDataCenterProject]:
        """Simple ranking by green score only"""
        return self.loader.get_top_green_projects(n)
    
    def get_statistics(self) -> Dict:
        return self.loader.get_statistics()


# Demo
if __name__ == "__main__":
    selector = GreenDatacenterSelector()
    
    # Example workload: train a 10B model for 500 GPU hours
    workload = WorkloadSpec(
        gpu_hours=500,
        model_size_gb=50,
        latency_tolerance_ms=200,
        workload_type="training",
        carbon_budget_kg=1000,
        max_cost_usd=5000
    )
    
    result = selector.select_datacenter(workload, user_region="us-east")
    
    print(f"\n=== Selection Result ===")
    print(f"Selected: {result.selected_project.project_name} (Green Score: {result.green_score:.1f})")
    print(f"Energy: {result.estimated_energy_kwh:.0f} kWh")
    print(f"Carbon: {result.estimated_carbon_kg:.2f} kg CO₂")
    print(f"Cost: ${result.estimated_cost_usd:.2f}")
    print(f"\nReasoning: {result.reasoning}")
    print(f"\nAlternatives:")
    for alt, score in result.alternatives:
        print(f"  - {alt.project_name} (Green Score: {score:.1f})")
