# src/enhancements/green_datacenter_map.py
"""
Interactive Map Visualization for Green Data Centers

Generates a Leaflet map with color-coded markers for Green Scores
and can highlight recommended sites for a given workload.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional
import logging
from .ai_data_center_loader import AIDataCenterLoader
from .green_datacenter_selector import GreenDatacenterSelector, WorkloadSpec

logger = logging.getLogger(__name__)


class GreenDatacenterMap:
    """
    Generates an interactive Leaflet map with green score visualization.
    
    Features:
    - Color-coded markers (green = high score, red = low score)
    - Tooltips with project details
    - Recommended sites highlighting for given workload
    - Export to HTML file
    """
    
    def __init__(self, loader: Optional[AIDataCenterLoader] = None):
        self.loader = loader or AIDataCenterLoader()
        self.selector = GreenDatacenterSelector(self.loader)
    
    def _get_marker_color(self, green_score: float) -> str:
        """Get marker color based on green score"""
        if green_score >= 80:
            return "#2ecc71"  # bright green
        elif green_score >= 60:
            return "#27ae60"  # green
        elif green_score >= 40:
            return "#f1c40f"  # yellow
        elif green_score >= 20:
            return "#e67e22"  # orange
        else:
            return "#e74c3c"  # red
    
    def generate_map_html(self, output_path: Optional[Path] = None,
                         workload: Optional[WorkloadSpec] = None,
                         user_region: str = "us-east") -> str:
        """
        Generate HTML with interactive map.
        
        If workload is provided, highlights recommended sites.
        """
        projects = self.loader.get_all_projects()
        
        # Get recommendations if workload provided
        recommended_ids = []
        recommendation_text = ""
        if workload:
            result = self.selector.select_datacenter(workload, user_region)
            recommended_ids = [result.selected_project.project_id]
            for alt, _ in result.alternatives:
                recommended_ids.append(alt.project_id)
            recommendation_text = f"<p><strong>Recommendation:</strong> {result.reasoning}</p>"
        
        # Build markers
        markers = []
        for p in projects:
            color = self._get_marker_color(p.green_score)
            is_recommended = p.project_id in recommended_ids
            marker_size = 12 if is_recommended else 8
            marker_opacity = 1.0 if is_recommended else 0.7
            marker_zindex = 1000 if is_recommended else 100
            
            # Tooltip content
            tooltip = f"""
                <div style="font-family: Arial, sans-serif; min-width: 250px;">
                    <strong>{p.project_name}</strong><br>
                    <em>{p.company}</em><br>
                    📍 {p.location_city}, {p.location_country}<br>
                    ⚡ Capacity: {p.planned_power_capacity_mw:.0f} MW<br>
                    🟢 Green Score: {p.green_score:.1f}/100<br>
                    🌿 Carbon: {p.sustainability.grid_carbon_intensity_gco2_per_kwh:.0f} gCO₂/kWh<br>
                    ☀️ Renewable: {p.sustainability.renewable_share_pct:.0f}%<br>
                    🔧 Cooling: {p.sustainability.cooling_type.upper()}<br>
                    📊 Status: {p.status}
                </div>
            """
            
            markers.append({
                "lat": p.latitude,
                "lon": p.longitude,
                "name": p.project_name,
                "color": color,
                "size": marker_size,
                "opacity": marker_opacity,
                "zindex": marker_zindex,
                "tooltip": tooltip,
                "green_score": p.green_score
            })
        
        # Generate HTML
        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Green Data Center Map - AI Data Centers by Green Score</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <style>
        body {{ margin: 0; padding: 0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }}
        #map {{ height: 100vh; width: 100%; }}
        .info-panel {{
            position: absolute;
            top: 20px;
            right: 20px;
            background: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
            z-index: 1000;
            max-width: 300px;
            font-size: 14px;
            pointer-events: auto;
        }}
        .legend {{
            position: absolute;
            bottom: 20px;
            right: 20px;
            background: white;
            padding: 10px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
            z-index: 1000;
            font-size: 12px;
        }}
        .legend-color {{
            width: 20px;
            height: 20px;
            display: inline-block;
            margin-right: 5px;
            border-radius: 50%;
        }}
        h3 {{ margin: 0 0 10px 0; }}
        .green-badge {{ color: #2ecc71; font-weight: bold; }}
    </style>
</head>
<body>
    <div id="map"></div>
    <div class="info-panel">
        <h3>🌿 Green Data Center Map</h3>
        <p>AI data centers colored by <strong class="green-badge">Green Score</strong> (0-100)<br>
        Higher score = lower carbon + higher efficiency.</p>
        {recommendation_text}
        <hr>
        <p><small>📊 {len(projects)} projects • {self.loader.get_statistics()['total_capacity_mw']:.0f} MW capacity</small></p>
    </div>
    <div class="legend">
        <h4>Green Score</h4>
        <div><span class="legend-color" style="background: #2ecc71;"></span> 80-100 (Excellent)</div>
        <div><span class="legend-color" style="background: #27ae60;"></span> 60-79 (Good)</div>
        <div><span class="legend-color" style="background: #f1c40f;"></span> 40-59 (Moderate)</div>
        <div><span class="legend-color" style="background: #e67e22;"></span> 20-39 (Poor)</div>
        <div><span class="legend-color" style="background: #e74c3c;"></span> 0-19 (Very Poor)</div>
        <div><span class="legend-color" style="background: #3498db; border-radius: 2px;"></span> Recommended</div>
    </div>
    <script>
        var map = L.map('map').setView([30, 0], 2);
        
        L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; CartoDB',
            subdomains: 'abcd',
            maxZoom: 19
        }}).addTo(map);
        
        // Marker data
        var markers = {json.dumps(markers)};
        
        // Add markers
        markers.forEach(function(m) {{
            var marker = L.circleMarker([m.lat, m.lon], {{
                radius: m.size,
                fillColor: m.color,
                color: '#fff',
                weight: 2,
                opacity: 1,
                fillOpacity: m.opacity,
                zIndexOffset: m.zindex
            }}).addTo(map);
            
            marker.bindTooltip(m.tooltip, {{ sticky: true, className: 'custom-tooltip' }});
            
            // Optional: add popup with more details
            marker.bindPopup(`<b>${{m.name}}</b><br>Green Score: ${{m.green_score}}/100`);
        }});
        
        // Add a simple scale bar
        L.control.scale().addTo(map);
    </script>
</body>
</html>
"""
        
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html)
            logger.info(f"Map saved to {output_path}")
        
        return html
    
    def generate_recommendation_map(self, workload: WorkloadSpec, user_region: str = "us-east",
                                   output_path: Optional[Path] = None) -> str:
        """Generate map highlighting recommended sites for a workload"""
        return self.generate_map_html(output_path, workload, user_region)


# Demo
if __name__ == "__main__":
    map_gen = GreenDatacenterMap()
    
    # Generate map with all sites
    map_gen.generate_map_html(Path("green_datacenter_map.html"))
    print("Map generated: green_datacenter_map.html")
    
    # Generate recommendation map for a workload
    workload = WorkloadSpec(
        gpu_hours=1000,
        model_size_gb=50,
        latency_tolerance_ms=150,
        workload_type="training",
        carbon_budget_kg=500
    )
    map_gen.generate_recommendation_map(workload, user_region="us-east", output_path=Path("green_recommendation_map.html"))
    print("Recommendation map generated: green_recommendation_map.html")
