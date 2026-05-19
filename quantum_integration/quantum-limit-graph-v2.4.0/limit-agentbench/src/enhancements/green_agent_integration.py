# src/enhancements/green_agent_integration.py
"""
Green Agent Integration Module

Extends the Green Agent with data center selection capabilities,
enabling it to choose optimal sites based on green scores and workload requirements.
"""

from typing import Dict, List, Optional, Any
import logging
from .ai_data_center_loader import AIDataCenterLoader
from .green_datacenter_selector import GreenDatacenterSelector, WorkloadSpec, SelectionResult
from .green_datacenter_map import GreenDatacenterMap

logger = logging.getLogger(__name__)


class GreenAgentDataCenterExtension:
    """
    Extension to the Green Agent for data center selection.
    
    Features:
    - Integrates data center selection into agent's decision loop
    - Provides rationales for site selection
    - Tracks carbon savings over time
    - Recommends workload distribution across sites
    """
    
    def __init__(self):
        self.loader = AIDataCenterLoader()
        self.selector = GreenDatacenterSelector(self.loader)
        self.map_generator = GreenDatacenterMap(self.loader)
        
        # Carbon savings tracking
        self.total_carbon_saved_kg = 0.0
        self.selection_history: List[SelectionResult] = []
    
    def select_for_workload(self, workload_params: Dict[str, Any],
                           user_region: str = "us-east") -> Dict[str, Any]:
        """
        Select optimal data center for a workload.
        
        Args:
            workload_params: Dictionary with keys:
                - gpu_hours: float
                - model_size_gb: float (optional)
                - latency_tolerance_ms: float (optional)
                - jurisdiction_requirements: List[str] (optional)
                - workload_type: str (training, inference, batch)
                - carbon_budget_kg: float (optional)
                - max_cost_usd: float (optional)
            user_region: Approximate user region for latency estimation
            
        Returns:
            Selection result as dictionary with decision and rationale.
        """
        workload = WorkloadSpec(
            gpu_hours=workload_params.get('gpu_hours', 100),
            model_size_gb=workload_params.get('model_size_gb', 10),
            latency_tolerance_ms=workload_params.get('latency_tolerance_ms', 200),
            jurisdiction_requirements=workload_params.get('jurisdiction_requirements'),
            workload_type=workload_params.get('workload_type', 'training'),
            carbon_budget_kg=workload_params.get('carbon_budget_kg'),
            max_cost_usd=workload_params.get('max_cost_usd')
        )
        
        result = self.selector.select_datacenter(workload, user_region)
        self.selection_history.append(result)
        
        # Track carbon savings compared to average site
        avg_site = self.loader.get_all_projects()
        if avg_site:
            avg_green = sum(p.green_score for p in avg_site) / len(avg_site)
            avg_carbon_intensity = sum(p.sustainability.grid_carbon_intensity_gco2_per_kwh for p in avg_site) / len(avg_site)
            avg_carbon_kg = (workload.gpu_hours * 0.65 * 1.3 * (avg_carbon_intensity / 1000))
            saved = avg_carbon_kg - result.estimated_carbon_kg
            if saved > 0:
                self.total_carbon_saved_kg += saved
        
        return {
            "decision": {
                "project_id": result.selected_project.project_id,
                "project_name": result.selected_project.project_name,
                "location": f"{result.selected_project.location_city}, {result.selected_project.location_country}",
                "green_score": result.green_score,
                "estimated_carbon_kg": result.estimated_carbon_kg,
                "estimated_cost_usd": result.estimated_cost_usd,
                "latency_ms": result.latency_ms
            },
            "rationale": result.reasoning,
            "alternatives": [
                {
                    "project_name": alt.project_name,
                    "green_score": score
                }
                for alt, score in result.alternatives
            ],
            "carbon_saved_vs_average_kg": saved if 'saved' in locals() else 0
        }
    
    def get_site_details(self, project_id: str) -> Optional[Dict]:
        """Get detailed sustainability information for a site"""
        project = self.loader.get_project(project_id)
        if not project:
            return None
        
        return {
            "project_name": project.project_name,
            "company": project.company,
            "location": f"{project.location_city}, {project.location_country}",
            "capacity_mw": project.planned_power_capacity_mw,
            "status": project.status,
            "green_score": project.green_score,
            "carbon_intensity_gco2_kwh": project.sustainability.grid_carbon_intensity_gco2_per_kwh,
            "renewable_share_pct": project.sustainability.renewable_share_pct,
            "pue": project.sustainability.pue_estimated,
            "cooling_type": project.sustainability.cooling_type,
            "water_stress_index": project.sustainability.water_stress_index,
            "climate_risk_score": project.sustainability.climate_risk_score
        }
    
    def get_top_sites(self, n: int = 10) -> List[Dict]:
        """Get top N sites by green score"""
        projects = self.loader.get_top_green_projects(n)
        return [
            {
                "project_name": p.project_name,
                "company": p.company,
                "location": f"{p.location_city}, {p.location_country}",
                "green_score": p.green_score,
                "carbon_intensity": p.sustainability.grid_carbon_intensity_gco2_per_kwh,
                "renewable_share": p.sustainability.renewable_share_pct
            }
            for p in projects
        ]
    
    def get_statistics(self) -> Dict:
        """Get overall statistics"""
        return {
            "total_projects": self.loader.get_statistics()['total_projects'],
            "total_capacity_mw": self.loader.get_statistics()['total_capacity_mw'],
            "avg_green_score": self.loader.get_statistics()['avg_green_score'],
            "selections_made": len(self.selection_history),
            "total_carbon_saved_kg": self.total_carbon_saved_kg,
            "last_selection": self.selection_history[-1].selected_project.project_name if self.selection_history else None
        }
    
    def generate_map_html(self, output_path: str = "green_datacenter_map.html"):
        """Generate interactive map HTML file"""
        self.map_generator.generate_map_html(Path(output_path))
        logger.info(f"Map saved to {output_path}")


# Demo
if __name__ == "__main__":
    agent = GreenAgentDataCenterExtension()
    
    # Example workload
    workload = {
        "gpu_hours": 1000,
        "latency_tolerance_ms": 100,
        "workload_type": "training",
        "carbon_budget_kg": 500
    }
    
    result = agent.select_for_workload(workload, user_region="us-east")
    
    print("\n=== Green Agent Decision ===")
    print(f"Selected: {result['decision']['project_name']}")
    print(f"Green Score: {result['decision']['green_score']}")
    print(f"Estimated Carbon: {result['decision']['estimated_carbon_kg']:.2f} kg")
    print(f"Rationale: {result['rationale']}")
    print(f"Carbon saved vs average: {result['carbon_saved_vs_average_kg']:.2f} kg")
    
    print("\n=== Statistics ===")
    print(agent.get_statistics())
    
    # Generate map
    agent.generate_map_html("green_agent_map.html")
