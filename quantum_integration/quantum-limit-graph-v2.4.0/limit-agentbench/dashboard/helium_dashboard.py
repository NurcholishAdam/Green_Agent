# dashboard/helium_dashboard.py (NEW)

from fastapi import APIRouter, WebSocket
from typing import Dict, List
import asyncio

router = APIRouter(prefix="/api/helium", tags=["helium"])

class HeliumDashboard:
    """
    Helium dashboard integration for Layer 11
    Provides real-time visualizations and alerts
    """
    
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.websocket_clients = []
    
    @router.get("/status")
    async def get_helium_status(self):
        """Get current helium supply status"""
        return await self.orchestrator.get_helium_status()
    
    @router.get("/report")
    async def get_helium_report(self):
        """Get comprehensive helium report"""
        return await self.orchestrator.get_helium_report()
    
    @router.get("/metrics")
    async def get_helium_metrics(self):
        """Get helium metrics for Prometheus"""
        report = await self.orchestrator.get_helium_report()
        
        # Format for Prometheus
        return {
            'helium_scarcity_score': report.get('current_supply', {}).get('scarcity_score', 0),
            'helium_spot_price': report.get('current_supply', {}).get('spot_price_usd', 0),
            'helium_efficiency_avg': report.get('efficiency_report', {}).get('helium_per_energy_ratio', 0),
            'helium_fallback_rate': report.get('efficiency_report', {}).get('fallback_rate', 0)
        }
    
    @router.websocket("/ws")
    async def websocket_endpoint(self, websocket: WebSocket):
        """WebSocket for real-time helium updates"""
        await websocket.accept()
        self.websocket_clients.append(websocket)
        
        try:
            while True:
                # Send updates every 30 seconds
                status = await self.orchestrator.get_helium_status()
                await websocket.send_json(status)
                await asyncio.sleep(30)
        except Exception:
            self.websocket_clients.remove(websocket)
    
    def get_grafana_dashboard_config(self) -> Dict:
        """Generate Grafana dashboard configuration for helium metrics"""
        
        return {
            "dashboard": {
                "title": "Helium-Aware AI Orchestration",
                "panels": [
                    {
                        "title": "Helium Supply Scarcity Trend",
                        "type": "timeseries",
                        "targets": [{"expr": "helium_scarcity_score"}],
                        "alert": {
                            "conditions": [
                                {"type": "gt", "value": 0.7, "message": "Helium scarcity critical"}
                            ]
                        }
                    },
                    {
                        "title": "Helium Spot Price (USD/Liter)",
                        "type": "timeseries",
                        "targets": [{"expr": "helium_spot_price"}]
                    },
                    {
                        "title": "Helium Efficiency by Hardware",
                        "type": "barchart",
                        "targets": [{"expr": "helium_efficiency_per_hardware"}]
                    },
                    {
                        "title": "Fallback Usage Rate",
                        "type": "gauge",
                        "targets": [{"expr": "helium_fallback_rate"}],
                        "thresholds": [{"value": 0.2, "color": "green"}, {"value": 0.5, "color": "orange"}]
                    },
                    {
                        "title": "Carbon-Helium Trade-off",
                        "type": "scatter",
                        "targets": [
                            {"expr": "carbon_emissions", "name": "Carbon"},
                            {"expr": "helium_usage", "name": "Helium"}
                        ]
                    },
                    {
                        "title": "Worker Pool Helium Footprint",
                        "type": "piechart",
                        "targets": [{"expr": "worker_pool_helium_footprint"}]
                    }
                ]
            }
        }
