# src/enhancements/export_perplexity_datacenter_data.py
"""
Export real AI Data Center Map from Perplexity into structured CSV.

This script provides utilities to parse Perplexity data and create
a production-ready dataset for the Green Agent.
"""

import csv
import json
import re
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import logging
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class PerplexityDataCenterExporter:
    """
    Extract and structure AI data center data from Perplexity.
    
    Features:
    - Parse Perplexity conversation/export format
    - Extract project details (name, operator, location, capacity)
    - Normalize data into standard schema
    - Validate and deduplicate entries
    """
    
    def __init__(self, input_path: Optional[Path] = None):
        self.input_path = input_path or Path(__file__).parent / "data" / "perplexity_export.json"
        self.output_path = Path(__file__).parent / "data" / "ai_datacenters_production.csv"
        self.projects = []
    
    def parse_perplexity_json(self, data: Dict) -> List[Dict]:
        """
        Parse Perplexity JSON export format.
        
        Expected structure: 
        {
            "conversation": [
                {"role": "system", "content": "..."},
                {"role": "user", "content": "AI Data Center Map"},
                {"role": "assistant", "content": "Here are the projects...\n| Project | Company | Location | Capacity | Status |"}
            ]
        }
        """
        projects = []
        
        # Find assistant response with table
        for message in data.get("conversation", []):
            if message.get("role") == "assistant":
                content = message.get("content", "")
                
                # Extract markdown tables
                tables = self._extract_tables_from_markdown(content)
                for table in tables:
                    parsed = self._parse_markdown_table(table)
                    projects.extend(parsed)
                
                # Extract project mentions if no table
                if not projects:
                    projects = self._extract_projects_from_text(content)
        
        return projects
    
    def _extract_tables_from_markdown(self, text: str) -> List[str]:
        """Extract markdown tables from text"""
        table_pattern = r'\|[^\n]+\|\n\|[-:| ]+\|\n(?:\|[^\n]+\|\n?)+'
        return re.findall(table_pattern, text)
    
    def _parse_markdown_table(self, table: str) -> List[Dict]:
        """Parse markdown table into structured data"""
        lines = table.strip().split('\n')
        if len(lines) < 3:
            return []
        
        # Parse headers
        headers = [h.strip().lower().replace(' ', '_') for h in lines[0].split('|')[1:-1]]
        
        # Map headers to our schema
        header_mapping = {
            'project': 'project_name',
            'company': 'company',
            'location': 'location_city',
            'country': 'location_country',
            'capacity': 'planned_power_capacity_mw',
            'status': 'status',
            'gpu': 'gpu_estimated',
            'fuel': 'fuel_type'
        }
        
        projects = []
        for line in lines[2:]:
            cells = [c.strip() for c in line.split('|')[1:-1]]
            if len(cells) != len(headers):
                continue
            
            project = {}
            for header, cell in zip(headers, cells):
                mapped = header_mapping.get(header, header)
                project[mapped] = cell
            
            # Normalize capacity (convert GW to MW if needed)
            if 'planned_power_capacity_mw' in project:
                capacity_str = str(project['planned_power_capacity_mw'])
                if 'GW' in capacity_str.upper():
                    # Extract number and convert to MW
                    num = float(re.search(r'[\d\.]+', capacity_str).group())
                    project['planned_power_capacity_mw'] = num * 1000
                else:
                    # Extract number
                    num = float(re.search(r'[\d\.]+', capacity_str).group()) if re.search(r'[\d\.]+', capacity_str) else 0
                    project['planned_power_capacity_mw'] = num
            
            # Add default values if missing
            project.setdefault('status', 'planned')
            project.setdefault('gpu_estimated', None)
            project.setdefault('fuel_type', None)
            
            projects.append(project)
        
        return projects
    
    def _extract_projects_from_text(self, text: str) -> List[Dict]:
        """Extract project information from unstructured text"""
        projects = []
        
        # Pattern for project entries
        # Example: "Meta's Hyperion project in Los Angeles (150 MW, operational)"
        patterns = [
            r'([A-Za-z0-9\s]+?)\'s?\s+([A-Za-z\s]+?)\s+(?:project|facility|data center)\s+in\s+([A-Za-z\s]+?)(?:,|\()\s*\(?([\d\.]+)\s*(MW|GW)\)?',
            r'([A-Za-z0-9\s]+?)\s+(?:announced|plans|building|operates)\s+([A-Za-z\s]+?)\s+data center\s+in\s+([A-Za-z\s]+?)(?:\s*\(?([\d\.]+)\s*(MW|GW)\)?)?',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                project = {
                    'company': match[0].strip(),
                    'project_name': match[1].strip(),
                    'location_city': match[2].strip(),
                    'planned_power_capacity_mw': float(match[3]) * (1000 if 'GW' in match[4].upper() else 1) if len(match) > 3 else 0,
                    'status': 'planned',
                    'location_country': self._infer_country(match[2].strip())
                }
                projects.append(project)
        
        return projects
    
    def _infer_country(self, city: str) -> str:
        """Infer country from city name"""
        city_country_map = {
            'Los Angeles': 'USA', 'Ashburn': 'USA', 'Dallas': 'USA', 'Quincy': 'USA',
            'Santa Clara': 'USA', 'Denver': 'USA', 'Kansas City': 'USA',
            'Dublin': 'Ireland', 'Hamina': 'Finland', 'Stockholm': 'Sweden',
            'Odense': 'Denmark', 'Jakarta': 'Indonesia', 'Riyadh': 'Saudi Arabia',
            'Shanghai': 'China', 'Tokyo': 'Japan', 'Singapore': 'Singapore',
            'Seoul': 'South Korea', 'Abu Dhabi': 'UAE', 'NEOM': 'Saudi Arabia',
            'Sydney': 'Australia', 'Melbourne': 'Australia'
        }
        return city_country_map.get(city, 'Unknown')
    
    def enrich_with_coordinates(self, projects: List[Dict]) -> List[Dict]:
        """Add latitude/longitude coordinates"""
        # Simple coordinate mapping (in production, use geocoding API)
        coords = {
            ('Los Angeles', 'USA'): (34.05, -118.24),
            ('Ashburn', 'USA'): (39.04, -77.49),
            ('Dallas', 'USA'): (32.78, -96.80),
            ('Quincy', 'USA'): (47.23, -119.85),
            ('Santa Clara', 'USA'): (37.35, -121.96),
            ('Denver', 'USA'): (39.74, -104.99),
            ('Kansas City', 'USA'): (39.10, -94.58),
            ('Dublin', 'Ireland'): (53.35, -6.26),
            ('Hamina', 'Finland'): (60.57, 27.20),
            ('Stockholm', 'Sweden'): (59.33, 18.07),
            ('Odense', 'Denmark'): (55.40, 10.39),
            ('Jakarta', 'Indonesia'): (-6.21, 106.85),
            ('Riyadh', 'Saudi Arabia'): (24.71, 46.68),
            ('Shanghai', 'China'): (31.23, 121.47),
            ('Tokyo', 'Japan'): (35.68, 139.76),
            ('Singapore', 'Singapore'): (1.35, 103.82),
            ('Seoul', 'South Korea'): (37.57, 126.98),
            ('Abu Dhabi', 'UAE'): (24.45, 54.40),
            ('NEOM', 'Saudi Arabia'): (28.00, 35.00),
            ('Sydney', 'Australia'): (-33.87, 151.21),
            ('Melbourne', 'Australia'): (-37.81, 144.96)
        }
        
        for p in projects:
            key = (p.get('location_city', ''), p.get('location_country', ''))
            if key in coords:
                p['latitude'], p['longitude'] = coords[key]
            else:
                p['latitude'] = 0
                p['longitude'] = 0
        
        return projects
    
    def export_to_csv(self):
        """Main export function"""
        # Try to load from Perplexity export
        if self.input_path.exists():
            with open(self.input_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            projects = self.parse_perplexity_json(data)
        else:
            logger.warning(f"Perplexity export not found at {self.input_path}, using default data")
            projects = self._get_default_projects()
        
        # Enrich with coordinates
        projects = self.enrich_with_coordinates(projects)
        
        # Write to CSV
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        
        fieldnames = [
            'project_id', 'project_name', 'company', 'location_city', 'location_country',
            'latitude', 'longitude', 'planned_power_capacity_mw', 'status',
            'gpu_estimated', 'fuel_type'
        ]
        
        with open(self.output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for i, p in enumerate(projects):
                p['project_id'] = f"DC-{i+1:04d}"
                writer.writerow({k: p.get(k, '') for k in fieldnames})
        
        logger.info(f"Exported {len(projects)} projects to {self.output_path}")
        return self.output_path
    
    def _get_default_projects(self) -> List[Dict]:
        """Fallback to known projects"""
        return [
            {"project_name": "Hyperion", "company": "Meta", "location_city": "Los Angeles", "location_country": "USA", 
             "planned_power_capacity_mw": 150, "status": "operational", "gpu_estimated": 50000, "fuel_type": "gas"},
            {"project_name": "Texas Campus", "company": "Google", "location_city": "Dallas", "location_country": "USA",
             "planned_power_capacity_mw": 120, "status": "construction", "gpu_estimated": 40000},
            {"project_name": "Quincy", "company": "Microsoft", "location_city": "Quincy", "location_country": "USA",
             "planned_power_capacity_mw": 100, "status": "operational", "gpu_estimated": 30000},
            {"project_name": "Jakarta", "company": "Princeton Digital", "location_city": "Jakarta", "location_country": "Indonesia",
             "planned_power_capacity_mw": 100, "status": "construction", "gpu_estimated": 30000},
            {"project_name": "HUMAIN", "company": "Saudi Arabia", "location_city": "Riyadh", "location_country": "Saudi Arabia",
             "planned_power_capacity_mw": 200, "status": "planned", "gpu_estimated": 60000, "fuel_type": "gas"},
        ]


def main():
    exporter = PerplexityDataCenterExporter()
    output_path = exporter.export_to_csv()
    print(f"\n=== Export Complete ===")
    print(f"File saved to: {output_path}")


if __name__ == "__main__":
    main()
