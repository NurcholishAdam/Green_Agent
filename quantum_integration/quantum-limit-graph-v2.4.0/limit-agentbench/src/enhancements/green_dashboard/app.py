# src/enhancements/green_dashboard/app.py
"""
Live Green Data Center Dashboard Web Application

Interactive dashboard using FastAPI + Leaflet for real-time exploration.
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Dict, Optional
import asyncio
from pathlib import Path
import json
import logging

from ..ai_data_center_loader import AIDataCenterLoader
from ..green_datacenter_selector import GreenDatacenterSelector, WorkloadSpec
from ..real_carbon_intensity_api import RealCarbonIntensityClient
from ..cloud_latency_estimator import CloudLatencyEstimator
from ..sustainability_signals import SustainabilitySignalEnricher

logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI(title="Green Data Center Dashboard", description="AI Data Center Sustainability Explorer")

# Initialize components
loader = AIDataCenterLoader()
selector = GreenDatacenterSelector(loader)
carbon_client = RealCarbonIntensityClient()
latency_estimator = CloudLatencyEstimator()
sustainability_enricher = SustainabilitySignalEnricher()


class WorkloadRequest(BaseModel):
    gpu_hours: float = 100
    latency_tolerance_ms: float = 200
    workload_type: str = "training"
    carbon_budget_kg: Optional[float] = None
    max_cost_usd: Optional[float] = None
    user_region: str = "us-east"


class RecommendationResponse(BaseModel):
    selected_project: Dict
    alternatives: List[Dict]
    rationale: str
    carbon_savings_kg: float


@app.get("/", response_class=HTMLResponse)
async def get_map():
    """Serve interactive map"""
    html_content = generate_map_html()
    return HTMLResponse(content=html_content)


@app.get("/api/projects")
async def get_projects():
    """Get all data center projects with sustainability scores"""
    projects = loader.get_all_projects()
    
    # Enrich with real-time carbon data
    for p in projects:
        try:
            intensity = await carbon_client.get_intensity(p.location_country)
            p.sustainability.grid_carbon_intensity_gco2_per_kwh = intensity
            # Recompute green score with real data
            p.green_score = loader._compute_green_score(p)
        except Exception as e:
            logger.error(f"Failed to get carbon data for {p.location_country}: {e}")
    
    return {
        "projects": [
            {
                "id": p.project_id,
                "name": p.project_name,
                "company": p.company,
                "location": f"{p.location_city}, {p.location_country}",
                "lat": p.latitude,
                "lon": p.longitude,
                "green_score": p.green_score,
                "capacity_mw": p.planned_power_capacity_mw,
                "status": p.status,
                "carbon_intensity": p.sustainability.grid_carbon_intensity_gco2_per_kwh,
                "renewable_share": p.sustainability.renewable_share_pct,
                "pue": p.sustainability.pue_estimated,
                "cooling_type": p.sustainability.cooling_type,
                "water_stress": p.sustainability.water_stress_index
            }
            for p in projects
        ],
        "statistics": loader.get_statistics()
    }


@app.post("/api/recommend", response_model=RecommendationResponse)
async def recommend_workload(request: WorkloadRequest):
    """Get data center recommendation for a workload"""
    workload = WorkloadSpec(
        gpu_hours=request.gpu_hours,
        latency_tolerance_ms=request.latency_tolerance_ms,
        workload_type=request.workload_type,
        carbon_budget_kg=request.carbon_budget_kg,
        max_cost_usd=request.max_cost_usd
    )
    
    result = selector.select_datacenter(workload, request.user_region)
    
    # Calculate carbon savings vs average
    avg_carbon = sum(p.sustainability.grid_carbon_intensity_gco2_per_kwh for p in loader.get_all_projects()) / len(loader.get_all_projects())
    avg_emissions = request.gpu_hours * 0.65 * 1.3 * (avg_carbon / 1000)
    savings = avg_emissions - result.estimated_carbon_kg
    
    return RecommendationResponse(
        selected_project={
            "id": result.selected_project.project_id,
            "name": result.selected_project.project_name,
            "location": f"{result.selected_project.location_city}, {result.selected_project.location_country}",
            "green_score": result.green_score,
            "estimated_carbon_kg": result.estimated_carbon_kg,
            "estimated_cost_usd": result.estimated_cost_usd,
            "latency_ms": result.latency_ms
        },
        alternatives=[
            {
                "name": alt.project_name,
                "green_score": score
            }
            for alt, score in result.alternatives
        ],
        rationale=result.reasoning,
        carbon_savings_kg=max(0, savings)
    )


@app.get("/api/regions/{country}/carbon")
async def get_country_carbon(country: str):
    """Get real-time carbon intensity for a country"""
    intensity = await carbon_client.get_intensity(country)
    forecast = await carbon_client.get_forecast(country, 12)
    
    return {
        "country": country,
        "current_intensity_gco2_kwh": intensity,
        "forecast_12h": forecast,
        "source": "electricitymap" if carbon_client.electricitymap_key else "watttime"
    }


@app.get("/api/latency/{data_center_id}")
async def get_latency(data_center_id: str, user_region: str = "us-east"):
    """Get latency estimates for a data center"""
    project = loader.get_project(data_center_id)
    if not project:
        raise HTTPException(status_code=404, detail="Data center not found")
    
    latency = latency_estimator.estimate_to_data_center(
        project.latitude, project.longitude, user_region
    )
    
    all_latencies = latency_estimator.get_all_latencies(project.latitude, project.longitude)
    
    return {
        "data_center": project.project_name,
        "estimated_latency_ms": latency,
        "by_region": all_latencies
    }


def generate_map_html() -> str:
    """Generate interactive map HTML with JavaScript API integration"""
    return """
<!DOCTYPE html>
<html>
<head>
    <title>Green Data Center Dashboard</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
        #map { height: 60vh; width: 100%; }
        .dashboard { padding: 20px; background: #1a1a2e; color: #eee; }
        .controls { display: flex; gap: 20px; margin-bottom: 20px; flex-wrap: wrap; }
        .control-group { background: #16213e; padding: 15px; border-radius: 8px; flex: 1; min-width: 200px; }
        .control-group label { display: block; margin-bottom: 8px; font-weight: bold; color: #00d4ff; }
        .control-group input, .control-group select { width: 100%; padding: 8px; border-radius: 4px; border: none; background: #0f3460; color: #eee; }
        button { background: #00d4ff; color: #1a1a2e; border: none; padding: 10px 20px; border-radius: 5px; cursor: pointer; font-weight: bold; }
        button:hover { background: #00b8d4; }
        .result { background: #0f3460; padding: 15px; border-radius: 8px; margin-top: 20px; }
        .result h3 { color: #00d4ff; margin-bottom: 10px; }
        .metrics { display: flex; gap: 15px; flex-wrap: wrap; margin-top: 10px; }
        .metric { background: #16213e; padding: 10px; border-radius: 5px; flex: 1; text-align: center; }
        .metric-value { font-size: 24px; font-weight: bold; color: #00d4ff; }
        .metric-label { font-size: 12px; color: #aaa; }
        .loading { text-align: center; padding: 20px; color: #00d4ff; }
        .error { color: #ff6b6b; text-align: center; padding: 20px; }
        .green-badge { color: #2ecc71; }
        .legend { position: absolute; bottom: 20px; right: 20px; background: white; padding: 10px; border-radius: 8px; z-index: 1000; font-size: 12px; }
    </style>
</head>
<body>
    <div id="map"></div>
    <div class="dashboard">
        <h2>🌿 Green Data Center Dashboard</h2>
        <div class="controls">
            <div class="control-group">
                <label>GPU Hours</label>
                <input type="number" id="gpu_hours" value="100" step="10">
            </div>
            <div class="control-group">
                <label>Latency Tolerance (ms)</label>
                <input type="number" id="latency_tolerance" value="200" step="10">
            </div>
            <div class="control-group">
                <label>Workload Type</label>
                <select id="workload_type">
                    <option value="training">Training</option>
                    <option value="inference">Inference</option>
                    <option value="batch">Batch Processing</option>
                </select>
            </div>
            <div class="control-group">
                <label>User Region</label>
                <select id="user_region">
                    <option value="us-east">US East</option>
                    <option value="us-west">US West</option>
                    <option value="eu-west">EU West</option>
                    <option value="asia-east">Asia East</option>
                    <option value="asia-southeast">Asia Southeast</option>
                </select>
            </div>
            <button onclick="getRecommendation()">Find Greenest Data Center</button>
        </div>
        <div id="result" class="result">
            <div class="loading">Enter workload parameters and click "Find Greenest Data Center"</div>
        </div>
        <div id="chart" style="height: 300px; margin-top: 20px;"></div>
    </div>
    <div class="legend">
        <h4>Green Score</h4>
        <div><span style="background:#2ecc71; display:inline-block; width:20px; height:20px; border-radius:50%;"></span> 80-100 (Excellent)</div>
        <div><span style="background:#27ae60; display:inline-block; width:20px; height:20px; border-radius:50%;"></span> 60-79 (Good)</div>
        <div><span style="background:#f1c40f; display:inline-block; width:20px; height:20px; border-radius:50%;"></span> 40-59 (Moderate)</div>
        <div><span style="background:#e67e22; display:inline-block; width:20px; height:20px; border-radius:50%;"></span> 20-39 (Poor)</div>
        <div><span style="background:#e74c3c; display:inline-block; width:20px; height:20px; border-radius:50%;"></span> 0-19 (Very Poor)</div>
    </div>

    <script>
        var map = L.map('map').setView([30, 0], 2);
        L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; CartoDB',
            subdomains: 'abcd',
            maxZoom: 19
        }).addTo(map);
        
        var markers = {};
        var projectsData = {};
        
        function getMarkerColor(score) {
            if (score >= 80) return '#2ecc71';
            if (score >= 60) return '#27ae60';
            if (score >= 40) return '#f1c40f';
            if (score >= 20) return '#e67e22';
            return '#e74c3c';
        }
        
        async function loadProjects() {
            try {
                const response = await fetch('/api/projects');
                const data = await response.json();
                projectsData = data.projects;
                
                for (const p of projectsData) {
                    const color = getMarkerColor(p.green_score);
                    const marker = L.circleMarker([p.lat, p.lon], {
                        radius: 10,
                        fillColor: color,
                        color: '#fff',
                        weight: 2,
                        opacity: 1,
                        fillOpacity: 0.8
                    }).addTo(map);
                    
                    marker.bindTooltip(`
                        <div style="min-width: 200px;">
                            <strong>${p.name}</strong><br>
                            ${p.company}<br>
                            📍 ${p.location}<br>
                            🟢 Green Score: ${p.green_score}/100<br>
                            🌿 Carbon: ${p.carbon_intensity} gCO₂/kWh<br>
                            ☀️ Renewable: ${p.renewable_share}%
                        </div>
                    `, { sticky: true });
                    
                    markers[p.id] = marker;
                }
                
                // Create comparison chart
                createComparisonChart(projectsData);
            } catch (error) {
                console.error('Failed to load projects:', error);
            }
        }
        
        function createComparisonChart(projects) {
            const sorted = [...projects].sort((a, b) => b.green_score - a.green_score).slice(0, 15);
            
            const trace = {
                x: sorted.map(p => p.name),
                y: sorted.map(p => p.green_score),
                type: 'bar',
                marker: {
                    color: sorted.map(p => getMarkerColor(p.green_score)),
                    line: { color: 'white', width: 1 }
                },
                text: sorted.map(p => `${p.green_score}/100`),
                textposition: 'auto',
                hoverinfo: 'text',
                hovertext: sorted.map(p => `${p.name}<br>Carbon: ${p.carbon_intensity} gCO₂/kWh<br>Renewable: ${p.renewable_share}%`)
            };
            
            const layout = {
                title: 'Top 15 Data Centers by Green Score',
                xaxis: { title: 'Data Center', tickangle: -45 },
                yaxis: { title: 'Green Score (0-100)', range: [0, 100] },
                plot_bgcolor: '#1a1a2e',
                paper_bgcolor: '#1a1a2e',
                font: { color: '#eee' },
                margin: { bottom: 100 }
            };
            
            Plotly.newPlot('chart', [trace], layout);
        }
        
        async function getRecommendation() {
            const resultDiv = document.getElementById('result');
            resultDiv.innerHTML = '<div class="loading">Analyzing workload and finding optimal data center...</div>';
            
            const workload = {
                gpu_hours: parseInt(document.getElementById('gpu_hours').value),
                latency_tolerance_ms: parseInt(document.getElementById('latency_tolerance').value),
                workload_type: document.getElementById('workload_type').value,
                user_region: document.getElementById('user_region').value
            };
            
            try {
                const response = await fetch('/api/recommend', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(workload)
                });
                const data = await response.json();
                
                // Highlight selected data center on map
                for (const [id, marker] of Object.entries(markers)) {
                    marker.setStyle({ radius: 10, fillOpacity: 0.6 });
                    if (id === data.selected_project.id) {
                        marker.setStyle({ radius: 18, fillOpacity: 1, color: '#ffd700', weight: 3 });
                        marker.openTooltip();
                        map.setView([marker.getLatLng().lat, marker.getLatLng().lng], 4);
                    }
                }
                
                resultDiv.innerHTML = `
                    <h3>✅ Recommendation: <span class="green-badge">${data.selected_project.name}</span></h3>
                    <p>📍 ${data.selected_project.location}</p>
                    <div class="metrics">
                        <div class="metric">
                            <div class="metric-value">${data.selected_project.green_score}</div>
                            <div class="metric-label">Green Score /100</div>
                        </div>
                        <div class="metric">
                            <div class="metric-value">${data.selected_project.estimated_carbon_kg.toFixed(1)}</div>
                            <div class="metric-label">kg CO₂</div>
                        </div>
                        <div class="metric">
                            <div class="metric-value">$${data.selected_project.estimated_cost_usd.toFixed(0)}</div>
                            <div class="metric-label">Estimated Cost</div>
                        </div>
                        <div class="metric">
                            <div class="metric-value">${data.selected_project.latency_ms.toFixed(0)} ms</div>
                            <div class="metric-label">Latency</div>
                        </div>
                    </div>
                    <p><strong>💡 Why this choice:</strong> ${data.rationale}</p>
                    <p><strong>🌱 Carbon savings vs average:</strong> ${data.carbon_savings_kg.toFixed(1)} kg CO₂</p>
                    <h4>Alternatives:</h4>
                    <ul>
                        ${data.alternatives.map(alt => `<li>${alt.name} (Green Score: ${alt.green_score})</li>`).join('')}
                    </ul>
                `;
            } catch (error) {
                resultDiv.innerHTML = `<div class="error">Error: ${error.message}</div>`;
            }
        }
        
        // Initialize
        loadProjects();
    </script>
</body>
</html>
    """


# To run: uvicorn src.enhancements.green_dashboard.app:app --reload --port 8000
