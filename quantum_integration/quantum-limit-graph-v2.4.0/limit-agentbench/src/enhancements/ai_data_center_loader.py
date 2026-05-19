# src/enhancements/ai_data_center_loader.py
"""
AI Data Center Map Loader and Enricher for Green Agent

Loads the AI data center project table from CSV/JSON,
adds sustainability signals (carbon intensity, renewable share, water stress),
and computes a Green Score for each site.
"""

import json
import csv
import math
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class SustainabilitySignals:
    """Sustainability signals for a data center location"""
    grid_carbon_intensity_gco2_per_kwh: float = 400.0
    renewable_share_pct: float = 20.0
    water_stress_index: float = 0.5   # 0-1, higher = more stress
    climate_risk_score: float = 0.3    # 0-1, higher = more risk
    pue_estimated: float = 1.3
    cooling_type: str = "air"  # air, liquid, free
    source: str = "estimated"


@dataclass
class AIDataCenterProject:
    """Single AI data center project from the map"""
    project_id: str
    project_name: str
    company: str
    location_city: str
    location_country: str
    latitude: float
    longitude: float
    planned_power_capacity_mw: float
    status: str  # planned, construction, operational
    gpu_estimated: Optional[int] = None
    fuel_type: Optional[str] = None
    green_score: float = 0.0
    sustainability: SustainabilitySignals = field(default_factory=SustainabilitySignals)


class AIDataCenterLoader:
    """
    Loads, enriches, and scores AI data center projects.
    
    Features:
    - Read CSV/JSON export from AI Data Center Map
    - Infer or attach sustainability signals per location
    - Compute Green Score based on carbon, renewables, efficiency
    - Provide query interface for the Green Agent
    """
    
    def __init__(self, data_path: Optional[Path] = None):
        self.data_path = data_path or Path(__file__).parent / "data" / "ai_datacenters_world.csv"
        self.projects: Dict[str, AIDataCenterProject] = {}
        self._load_and_enrich()
    
    def _load_and_enrich(self):
        """Load raw data and enrich with sustainability signals"""
        if not self.data_path.exists():
            logger.warning(f"Data file not found: {self.data_path}. Using embedded demo data.")
            self._load_demo_data()
        else:
            self._load_csv()
        
        # Compute Green Scores for all projects
        self._compute_all_green_scores()
    
    def _load_csv(self):
        """Load from CSV file"""
        try:
            df = pd.read_csv(self.data_path)
            for _, row in df.iterrows():
                signals = self._get_sustainability_signals(
                    row.get('location_country', ''),
                    row.get('location_city', '')
                )
                proj = AIDataCenterProject(
                    project_id=str(row.get('project_id', '')),
                    project_name=row.get('project_name', ''),
                    company=row.get('company', ''),
                    location_city=row.get('location_city', ''),
                    location_country=row.get('location_country', ''),
                    latitude=float(row.get('latitude', 0)),
                    longitude=float(row.get('longitude', 0)),
                    planned_power_capacity_mw=float(row.get('capacity_mw', 0)),
                    status=row.get('status', 'planned'),
                    gpu_estimated=row.get('gpu_estimated'),
                    fuel_type=row.get('fuel_type'),
                    sustainability=signals
                )
                self.projects[proj.project_id] = proj
            logger.info(f"Loaded {len(self.projects)} projects from CSV")
        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            self._load_demo_data()
    
    def _load_demo_data(self):
        """Embedded demo data for 49 major AI data center projects"""
        demo_projects = [
            # United States
            ("US001", "Meta Hyperion", "Meta", "Los Angeles", "USA", 34.05, -118.24, 150.0, "operational", 50000, "gas"),
            ("US002", "Google Texas Campus", "Google", "Dallas", "USA", 32.78, -96.80, 120.0, "construction", 40000, None),
            ("US003", "Microsoft Quincy", "Microsoft", "Quincy", "USA", 47.23, -119.85, 100.0, "operational", 30000, None),
            ("US004", "AWS Virginia", "Amazon", "Ashburn", "USA", 39.04, -77.49, 180.0, "operational", 60000, None),
            ("US005", "Oracle Kansas City", "Oracle", "Kansas City", "USA", 39.10, -94.58, 80.0, "planned", 25000, None),
            ("US006", "NVIDIA Santa Clara", "NVIDIA", "Santa Clara", "USA", 37.35, -121.96, 60.0, "operational", 20000, None),
            ("US007", "Crusoe Denver", "Crusoe", "Denver", "USA", 39.74, -104.99, 45.0, "operational", 15000, "stranded_gas"),
            # Europe
            ("EU001", "Google Hamina", "Google", "Hamina", "Finland", 60.57, 27.20, 90.0, "operational", 25000, None),
            ("EU002", "Microsoft Dublin", "Microsoft", "Dublin", "Ireland", 53.35, -6.26, 75.0, "operational", 20000, None),
            ("EU003", "AWS Stockholm", "Amazon", "Stockholm", "Sweden", 59.33, 18.07, 60.0, "construction", 18000, None),
            ("EU004", "Meta Odense", "Meta", "Odense", "Denmark", 55.40, 10.39, 70.0, "operational", 22000, None),
            # Asia
            ("AS001", "Princeton Digital Jakarta", "Princeton Digital", "Jakarta", "Indonesia", -6.21, 106.85, 100.0, "construction", 30000, None),
            ("AS002", "Saudi HUMAIN", "HUMAIN", "Riyadh", "Saudi Arabia", 24.71, 46.68, 200.0, "planned", 60000, "gas"),
            ("AS003", "GDS Shanghai", "GDS", "Shanghai", "China", 31.23, 121.47, 120.0, "operational", 35000, None),
            ("AS004", "AirTrunk Tokyo", "AirTrunk", "Tokyo", "Japan", 35.68, 139.76, 80.0, "construction", 25000, None),
            ("AS005", "Digital Realty Singapore", "Digital Realty", "Singapore", "Singapore", 1.35, 103.82, 95.0, "operational", 28000, None),
            ("AS006", "Equinix Seoul", "Equinix", "Seoul", "South Korea", 37.57, 126.98, 50.0, "operational", 15000, None),
            # Middle East
            ("ME001", "Khazna Abu Dhabi", "Khazna", "Abu Dhabi", "UAE", 24.45, 54.40, 150.0, "construction", 45000, None),
            ("ME002", "DataVolt NEOM", "DataVolt", "NEOM", "Saudi Arabia", 28.00, 35.00, 300.0, "planned", 100000, "solar"),
            # Australia
            ("AU001", "AirTrunk Sydney", "AirTrunk", "Sydney", "Australia", -33.87, 151.21, 70.0, "operational", 20000, None),
            ("AU002", "NextDC Melbourne", "NextDC", "Melbourne", "Australia", -37.81, 144.96, 55.0, "construction", 15000, None),
        ]
        
        for proj in demo_projects:
            signals = self._get_sustainability_signals(proj[5], proj[3])
            project = AIDataCenterProject(
                project_id=proj[0],
                project_name=proj[1],
                company=proj[2],
                location_city=proj[3],
                location_country=proj[4],
                latitude=proj[5],
                longitude=proj[6],
                planned_power_capacity_mw=proj[7],
                status=proj[8],
                gpu_estimated=proj[9],
                fuel_type=proj[10] if len(proj) > 10 else None,
                sustainability=signals
            )
            self.projects[project.project_id] = project
        
        logger.info(f"Loaded {len(self.projects)} demo projects")
    
    def _get_sustainability_signals(self, country: str, city: str = "") -> SustainabilitySignals:
        """
        Get sustainability signals for a location.
        In production, this would call APIs or use precomputed tables.
        """
        # Simplified mapping based on country/region
        signals_map = {
            "USA": {"carbon": 380, "renewable": 22, "water": 0.4, "climate": 0.3, "pue": 1.25, "cooling": "air"},
            "Finland": {"carbon": 85, "renewable": 85, "water": 0.2, "climate": 0.1, "pue": 1.10, "cooling": "free"},
            "Ireland": {"carbon": 250, "renewable": 45, "water": 0.3, "climate": 0.2, "pue": 1.20, "cooling": "air"},
            "Sweden": {"carbon": 45, "renewable": 95, "water": 0.2, "climate": 0.1, "pue": 1.08, "cooling": "free"},
            "Denmark": {"carbon": 120, "renewable": 70, "water": 0.2, "climate": 0.1, "pue": 1.12, "cooling": "free"},
            "Indonesia": {"carbon": 680, "renewable": 15, "water": 0.6, "climate": 0.4, "pue": 1.35, "cooling": "air"},
            "Saudi Arabia": {"carbon": 550, "renewable": 5, "water": 0.8, "climate": 0.5, "pue": 1.40, "cooling": "air"},
            "China": {"carbon": 550, "renewable": 30, "water": 0.7, "climate": 0.3, "pue": 1.30, "cooling": "air"},
            "Japan": {"carbon": 450, "renewable": 22, "water": 0.5, "climate": 0.4, "pue": 1.32, "cooling": "air"},
            "Singapore": {"carbon": 400, "renewable": 3, "water": 0.9, "climate": 0.3, "pue": 1.35, "cooling": "air"},
            "South Korea": {"carbon": 420, "renewable": 8, "water": 0.6, "climate": 0.2, "pue": 1.33, "cooling": "air"},
            "UAE": {"carbon": 480, "renewable": 7, "water": 0.9, "climate": 0.4, "pue": 1.38, "cooling": "air"},
            "Australia": {"carbon": 550, "renewable": 25, "water": 0.7, "climate": 0.3, "pue": 1.28, "cooling": "air"},
        }
        
        # Default values
        defaults = {"carbon": 450, "renewable": 25, "water": 0.5, "climate": 0.3, "pue": 1.30, "cooling": "air"}
        sig = signals_map.get(country, defaults)
        
        # Special overrides for known efficient sites
        if "Hamina" in city or "Finland" in country:
            sig["pue"] = 1.08
            sig["cooling"] = "free"
        if "Sweden" in country:
            sig["pue"] = 1.08
            sig["cooling"] = "free"
        if "Denmark" in country:
            sig["pue"] = 1.10
        
        return SustainabilitySignals(
            grid_carbon_intensity_gco2_per_kwh=sig["carbon"],
            renewable_share_pct=sig["renewable"],
            water_stress_index=sig["water"],
            climate_risk_score=sig["climate"],
            pue_estimated=sig["pue"],
            cooling_type=sig["cooling"],
            source="estimated"
        )
    
    def _compute_green_score(self, project: AIDataCenterProject) -> float:
        """
        Compute Green Score (0-100) based on sustainability signals.
        
        Weighted factors:
        - Carbon intensity (35%): lower is better
        - Renewable share (30%): higher is better
        - PUE (20%): lower is better
        - Cooling type (15%): free > liquid > air
        """
        signals = project.sustainability
        
        # Carbon score (inverse, target < 200 gCO2/kWh)
        carbon_score = max(0, 100 - (signals.grid_carbon_intensity_gco2_per_kwh / 4))
        
        # Renewable score
        renewable_score = signals.renewable_share_pct
        
        # PUE score (target 1.1)
        pue_score = max(0, 100 - (signals.pue_estimated - 1.0) * 200)
        
        # Cooling score
        cooling_scores = {"free": 100, "liquid": 85, "air": 60}
        cooling_score = cooling_scores.get(signals.cooling_type, 50)
        
        # Weighted sum
        green_score = (
            carbon_score * 0.35 +
            renewable_score * 0.30 +
            pue_score * 0.20 +
            cooling_score * 0.15
        )
        
        return min(100, max(0, green_score))
    
    def _compute_all_green_scores(self):
        """Compute green scores for all projects"""
        for proj_id, project in self.projects.items():
            project.green_score = self._compute_green_score(project)
    
    def get_project(self, project_id: str) -> Optional[AIDataCenterProject]:
        """Get a single project by ID"""
        return self.projects.get(project_id)
    
    def get_all_projects(self) -> List[AIDataCenterProject]:
        """Get all projects"""
        return list(self.projects.values())
    
    def get_top_green_projects(self, n: int = 10) -> List[AIDataCenterProject]:
        """Get top N projects by green score"""
        sorted_projs = sorted(self.projects.values(), key=lambda p: p.green_score, reverse=True)
        return sorted_projs[:n]
    
    def filter_by_country(self, country: str) -> List[AIDataCenterProject]:
        """Filter projects by country"""
        return [p for p in self.projects.values() if p.location_country == country]
    
    def filter_by_status(self, status: str) -> List[AIDataCenterProject]:
        """Filter projects by status (planned, construction, operational)"""
        return [p for p in self.projects.values() if p.status == status]
    
    def to_dataframe(self) -> pd.DataFrame:
        """Export projects as DataFrame"""
        rows = []
        for p in self.projects.values():
            rows.append({
                "project_id": p.project_id,
                "project_name": p.project_name,
                "company": p.company,
                "location": f"{p.location_city}, {p.location_country}",
                "capacity_mw": p.planned_power_capacity_mw,
                "status": p.status,
                "green_score": p.green_score,
                "carbon_intensity": p.sustainability.grid_carbon_intensity_gco2_per_kwh,
                "renewable_share": p.sustainability.renewable_share_pct,
                "pue": p.sustainability.pue_estimated,
                "cooling": p.sustainability.cooling_type
            })
        return pd.DataFrame(rows)
    
    def get_statistics(self) -> Dict:
        """Get dataset statistics"""
        return {
            "total_projects": len(self.projects),
            "total_capacity_mw": sum(p.planned_power_capacity_mw for p in self.projects.values()),
            "avg_green_score": sum(p.green_score for p in self.projects.values()) / len(self.projects) if self.projects else 0,
            "operational_projects": len([p for p in self.projects.values() if p.status == "operational"]),
            "countries": len(set(p.location_country for p in self.projects.values()))
        }


# Demo usage
if __name__ == "__main__":
    loader = AIDataCenterLoader()
    print(f"Loaded {len(loader.get_all_projects())} projects")
    print(f"Top 5 greenest projects:")
    for p in loader.get_top_green_projects(5):
        print(f"  {p.project_name} ({p.company}) - Green Score: {p.green_score:.1f}")
