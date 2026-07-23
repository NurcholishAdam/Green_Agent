# explainable_ui.py
"""
Explainable Green Decisions – Enhanced User Interface
======================================================

Provides:
- Natural‑language explanations for routing decisions (CO₂ savings, latency trade‑offs).
- Interactive dashboard with request‑level cost breakdowns and drill‑down to experts/nodes.
- “What‑if” mode to simulate alternative routing choices and compare sustainability impact.
- REST API extensions (via FastAPI/Flask) to serve explanation data.

Integrates with:
- SustainabilityAwareExpertProfile, SustainabilityFitnessScorer (from sustainability/__init__.py)
- VisualizationEngine (Plotly or other charting library)
- APIGateway (adds /api/explain/* endpoints)
"""

import json
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import uuid

# ---------- Dependencies (adjust imports to your project) ----------
try:
    # If you have the sustainability module from previous step
    from sustainability import (
        SustainabilityAwareExpertProfile,
        SUSTAINABILITY_CONFIG,
        SustainabilityFitnessScorer,
    )
except ImportError:
    # Fallback dummy definitions for demonstration
    class SustainabilityAwareExpertProfile:
        def __init__(self, expert_id, **kwargs):
            self.expert_id = expert_id
            self.energy_per_inference_full = 0.0
            self.energy_per_inference_compressed = None
            self.accuracy_full = 0.0
            self.accuracy_compressed = None
            self.compressed_flag = False
            self.sustainability_fitness_score = 0.0

    SUSTAINABILITY_CONFIG = {"energy_per_mac": 0.5e-12}

# Optional: VisualizationEngine – we'll use Plotly if available, else fallback to dict
try:
    import plotly.graph_objects as go
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    import matplotlib.pyplot as plt  # fallback (not fully interactive)

logger = logging.getLogger(__name__)

# ============================================================================
# 1. DATA MODELS
# ============================================================================
@dataclass
class RequestLog:
    """Log entry for a single routing request."""
    request_id: str
    timestamp: datetime
    query: str
    chosen_expert_id: str
    chosen_expert_profile: SustainabilityAwareExpertProfile
    alternative_experts: List[Tuple[str, SustainabilityAwareExpertProfile]]
    latency_ms: float
    energy_joules: float
    co2_kg: float  # estimated CO₂ per inference (e.g., 0.2 kg/kWh * energy)
    accuracy: float
    explanation: str = ""


@dataclass
class WhatIfResult:
    """Result of a what‑if simulation."""
    scenario_id: str
    alternative_expert_id: str
    expected_energy_joules: float
    expected_co2_kg: float
    expected_latency_ms: float
    expected_accuracy: float
    difference_energy: float
    difference_co2: float
    difference_latency: float
    difference_accuracy: float


# ============================================================================
# 2. EXPLANATION GENERATOR
# ============================================================================
class ExplanationGenerator:
    """
    Produces human‑readable, natural‑language explanations for each routing decision.
    """

    def __init__(self):
        self.co2_per_kwh = 0.2  # kg CO₂ per kWh (average)
        self.energy_to_co2_factor = self.co2_per_kwh / 3600000  # J -> kWh -> kg CO₂

    def generate(
        self,
        request: RequestLog,
        chosen_expert: SustainabilityAwareExpertProfile,
        alternatives: List[Tuple[str, SustainabilityAwareExpertProfile]],
    ) -> str:
        """
        Generate a comprehensive explanation string.
        """
        # Compute savings vs. the best alternative (by energy)
        if alternatives:
            best_alt = min(alternatives, key=lambda x: x[1].energy_per_inference_full)
            best_alt_id, best_alt_profile = best_alt
            alt_energy = best_alt_profile.energy_per_inference_full
            if chosen_expert.compressed_flag:
                chosen_energy = chosen_expert.energy_per_inference_compressed or chosen_expert.energy_per_inference_full
            else:
                chosen_energy = chosen_expert.energy_per_inference_full

            energy_saved = alt_energy - chosen_energy
            co2_saved = energy_saved * self.energy_to_co2_factor
            latency_diff = request.latency_ms - (best_alt_profile.energy_per_inference_full / 1e-6 * 0.5)  # rough estimate
        else:
            energy_saved = 0.0
            co2_saved = 0.0
            latency_diff = 0.0

        # Build explanation
        parts = []
        parts.append(f"This request was routed to expert **{chosen_expert.expert_id}**")
        if chosen_expert.compressed_flag:
            parts.append(f"(compressed version – {chosen_expert.compression_method})")
        parts.append(".")

        if co2_saved > 0:
            parts.append(f" This decision saved approximately **{co2_saved:.4f} kg CO₂**")
            parts.append(f" compared to the most energy‑intensive alternative.")
        else:
            parts.append(f" (No CO₂ savings over the best alternative).")

        if abs(latency_diff) > 1.0:  # only mention if significant
            if latency_diff > 0:
                parts.append(f" It increased latency by {latency_diff:.1f} ms.")
            else:
                parts.append(f" It reduced latency by {-latency_diff:.1f} ms.")

        parts.append(f" The chosen expert achieved accuracy of {request.accuracy:.2%}.")

        return " ".join(parts)


# ============================================================================
# 3. DASHBOARD ENGINE
# ============================================================================
class DashboardEngine:
    """
    Provides data for interactive dashboards:
    - Request‑level cost breakdown (energy, CO₂, latency, accuracy)
    - Drill‑down to individual experts and their profiles
    - Generates Plotly charts if available.
    """

    def __init__(self):
        self.request_logs: Dict[str, RequestLog] = {}  # in‑memory store

    def log_request(self, request_log: RequestLog) -> None:
        """Store a completed routing decision."""
        self.request_logs[request_log.request_id] = request_log

    def get_request_data(self, request_id: str) -> Dict[str, Any]:
        """Return a JSON‑serializable dict for a single request (for charts & UI)."""
        req = self.request_logs.get(request_id)
        if not req:
            return {"error": "Request not found"}
        return {
            "request_id": req.request_id,
            "timestamp": req.timestamp.isoformat(),
            "query": req.query,
            "chosen_expert": req.chosen_expert_id,
            "latency_ms": req.latency_ms,
            "energy_joules": req.energy_joules,
            "co2_kg": req.co2_kg,
            "accuracy": req.accuracy,
            "explanation": req.explanation,
            "alternatives": [
                {
                    "expert_id": eid,
                    "energy_joules": prof.energy_per_inference_full,
                    "accuracy": prof.accuracy_full,
                    "compressed": prof.compressed_flag,
                }
                for eid, prof in req.alternative_experts
            ],
        }

    def get_expert_details(self, expert_id: str) -> Dict[str, Any]:
        """Retrieve aggregated metrics for a specific expert."""
        # In practice, you'd query a database; here we derive from logs.
        related_requests = [r for r in self.request_logs.values() if r.chosen_expert_id == expert_id]
        if not related_requests:
            return {"expert_id": expert_id, "error": "No data"}
        avg_latency = sum(r.latency_ms for r in related_requests) / len(related_requests)
        avg_energy = sum(r.energy_joules for r in related_requests) / len(related_requests)
        avg_accuracy = sum(r.accuracy for r in related_requests) / len(related_requests)
        total_co2 = sum(r.co2_kg for r in related_requests)
        return {
            "expert_id": expert_id,
            "total_requests": len(related_requests),
            "avg_latency_ms": avg_latency,
            "avg_energy_joules": avg_energy,
            "avg_accuracy": avg_accuracy,
            "total_co2_kg": total_co2,
            "compressed": related_requests[0].chosen_expert_profile.compressed_flag if related_requests else False,
        }

    def get_dashboard_charts(self, request_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate interactive charts (Plotly JSON) for the dashboard.
        If request_id given, show drill‑down for that request.
        """
        if not PLOTLY_AVAILABLE:
            return {"error": "Plotly not installed; cannot generate charts."}

        if request_id:
            data = self.get_request_data(request_id)
            if "error" in data:
                return data
            # Create a bar chart comparing energy consumption across alternatives
            alt = data["alternatives"]
            labels = [a["expert_id"] for a in alt] + [data["chosen_expert"]]
            energies = [a["energy_joules"] for a in alt] + [data["energy_joules"]]
            colors = ["gray"] * len(alt) + ["green"]
            fig = go.Figure(data=[go.Bar(x=labels, y=energies, marker_color=colors)])
            fig.update_layout(
                title=f"Energy per Inference – Request {request_id}",
                xaxis_title="Expert",
                yaxis_title="Energy (Joules)",
            )
            return fig.to_json()
        else:
            # Overall dashboard: energy vs. accuracy scatter for all experts
            expert_data = {}
            for req in self.request_logs.values():
                eid = req.chosen_expert_id
                if eid not in expert_data:
                    expert_data[eid] = {"energies": [], "accuracies": [], "count": 0}
                expert_data[eid]["energies"].append(req.energy_joules)
                expert_data[eid]["accuracies"].append(req.accuracy)
                expert_data[eid]["count"] += 1
            # Average per expert
            experts = []
            avg_energies = []
            avg_accuracies = []
            sizes = []
            for eid, vals in expert_data.items():
                experts.append(eid)
                avg_energies.append(sum(vals["energies"]) / vals["count"])
                avg_accuracies.append(sum(vals["accuracies"]) / vals["count"])
                sizes.append(vals["count"] * 10)  # bubble size
            fig = go.Figure(data=[go.Scatter(
                x=avg_energies,
                y=avg_accuracies,
                mode="markers+text",
                text=experts,
                marker=dict(size=sizes, color=avg_energies, colorscale="Viridis", showscale=True),
            )])
            fig.update_layout(
                title="Expert Sustainability Trade‑offs (avg per expert)",
                xaxis_title="Average Energy per Inference (J)",
                yaxis_title="Average Accuracy",
                hovermode="closest",
            )
            return fig.to_json()


# ============================================================================
# 4. WHAT‑IF SIMULATOR
# ============================================================================
class WhatIfSimulator:
    """
    Simulates alternative routing choices and computes the sustainability impact.
    """

    def __init__(self, dashboard: DashboardEngine):
        self.dashboard = dashboard
        self.co2_per_kwh = 0.2  # kg CO₂ per kWh

    def simulate(self, request_id: str, alternative_expert_id: str) -> WhatIfResult:
        """
        Given a past request, compute what would have happened if it was routed
        to a different expert.
        """
        req = self.dashboard.request_logs.get(request_id)
        if not req:
            raise ValueError(f"Request {request_id} not found")

        # Find the alternative expert profile
        alt_profile = None
        for eid, prof in req.alternative_experts:
            if eid == alternative_expert_id:
                alt_profile = prof
                break
        if not alt_profile:
            raise ValueError(f"Expert {alternative_expert_id} not in alternatives for request {request_id}")

        # Use the alternative's energy (use compressed if available and flag is set)
        if alt_profile.compressed_flag and alt_profile.energy_per_inference_compressed:
            alt_energy = alt_profile.energy_per_inference_compressed
        else:
            alt_energy = alt_profile.energy_per_inference_full

        # Estimate latency from energy (rough scaling)
        alt_latency = alt_energy * 1e-6 * 0.5  # 0.5 ms per Joule (example)

        # Accuracy
        alt_accuracy = alt_profile.accuracy_compressed if alt_profile.compressed_flag else alt_profile.accuracy_full

        # CO₂
        alt_co2 = alt_energy * self.co2_per_kwh / 3600000

        # Differences
        diff_energy = alt_energy - req.energy_joules
        diff_co2 = alt_co2 - req.co2_kg
        diff_latency = alt_latency - req.latency_ms
        diff_accuracy = alt_accuracy - req.accuracy

        return WhatIfResult(
            scenario_id=str(uuid.uuid4()),
            alternative_expert_id=alternative_expert_id,
            expected_energy_joules=alt_energy,
            expected_co2_kg=alt_co2,
            expected_latency_ms=alt_latency,
            expected_accuracy=alt_accuracy,
            difference_energy=diff_energy,
            difference_co2=diff_co2,
            difference_latency=diff_latency,
            difference_accuracy=diff_accuracy,
        )


# ============================================================================
# 5. API GATEWAY EXTENSION
# ============================================================================
class APIGatewayExtension:
    """
    Extends your API Gateway (FastAPI/Flask) with /api/explain endpoints.
    """

    def __init__(self, dashboard_engine: DashboardEngine, explanation_generator: ExplanationGenerator, what_if: WhatIfSimulator):
        self.dashboard = dashboard_engine
        self.generator = explanation_generator
        self.what_if = what_if

    def register_routes(self, app):
        """
        Register routes on a FastAPI or Flask app.
        Example for FastAPI:
            app = FastAPI()
            ext.register_routes(app)
        """
        # We use a simple decorator style; adapt to your framework.
        # This example assumes FastAPI with `@app.get` etc.
        try:
            from fastapi import FastAPI, HTTPException
            from fastapi.responses import JSONResponse
            if not isinstance(app, FastAPI):
                # Fallback to Flask
                raise ImportError
        except ImportError:
            # Flask fallback
            from flask import Flask, jsonify, request
            if not isinstance(app, Flask):
                raise TypeError("app must be FastAPI or Flask")

            @app.route("/api/explain/request/<request_id>", methods=["GET"])
            def explain_request(request_id):
                return self._handle_explain_request(request_id)

            @app.route("/api/explain/dashboard", methods=["GET"])
            def dashboard_data():
                request_id = request.args.get("request_id")
                return self._handle_dashboard(request_id)

            @app.route("/api/explain/whatif", methods=["POST"])
            def whatif_sim():
                data = request.json
                return self._handle_whatif(data)

            return

        # FastAPI implementation
        @app.get("/api/explain/request/{request_id}")
        async def explain_request(request_id: str):
            return self._handle_explain_request(request_id)

        @app.get("/api/explain/dashboard")
        async def dashboard_data(request_id: Optional[str] = None):
            return self._handle_dashboard(request_id)

        @app.post("/api/explain/whatif")
        async def whatif_sim(data: dict):
            return self._handle_whatif(data)

    # ---- Internal handlers ----
    def _handle_explain_request(self, request_id: str):
        req = self.dashboard.request_logs.get(request_id)
        if not req:
            return {"error": "Request not found"}, 404
        # Ensure explanation is generated
        if not req.explanation:
            req.explanation = self.generator.generate(req, req.chosen_expert_profile, req.alternative_experts)
        return {
            "request_id": req.request_id,
            "explanation": req.explanation,
            "metrics": {
                "energy_joules": req.energy_joules,
                "co2_kg": req.co2_kg,
                "latency_ms": req.latency_ms,
                "accuracy": req.accuracy,
            }
        }

    def _handle_dashboard(self, request_id: Optional[str] = None):
        if request_id:
            data = self.dashboard.get_request_data(request_id)
            if "error" in data:
                return data, 404
            # Add chart JSON
            chart_json = self.dashboard.get_dashboard_charts(request_id)
            data["chart"] = json.loads(chart_json) if isinstance(chart_json, str) else chart_json
            return data
        else:
            # Return list of recent requests
            recent = list(self.dashboard.request_logs.values())[-50:]  # last 50
            return {
                "recent_requests": [
                    {
                        "request_id": r.request_id,
                        "timestamp": r.timestamp.isoformat(),
                        "chosen_expert": r.chosen_expert_id,
                        "co2_kg": r.co2_kg,
                    }
                    for r in recent
                ],
                "overall_chart": json.loads(self.dashboard.get_dashboard_charts()),
            }

    def _handle_whatif(self, data: dict):
        request_id = data.get("request_id")
        alternative_expert_id = data.get("alternative_expert_id")
        if not request_id or not alternative_expert_id:
            return {"error": "Missing request_id or alternative_expert_id"}, 400
        try:
            result = self.what_if.simulate(request_id, alternative_expert_id)
        except ValueError as e:
            return {"error": str(e)}, 400
        return {
            "scenario_id": result.scenario_id,
            "alternative_expert": result.alternative_expert_id,
            "expected": {
                "energy_joules": result.expected_energy_joules,
                "co2_kg": result.expected_co2_kg,
                "latency_ms": result.expected_latency_ms,
                "accuracy": result.expected_accuracy,
            },
            "differences": {
                "energy_joules": result.difference_energy,
                "co2_kg": result.difference_co2,
                "latency_ms": result.difference_latency,
                "accuracy": result.difference_accuracy,
            }
        }


# ============================================================================
# 6. CONVENIENCE: SINGLE ENTRY POINT
# ============================================================================
def create_explainable_ui():
    """
    Factory to create all components and return them for integration.
    """
    dashboard = DashboardEngine()
    generator = ExplanationGenerator()
    what_if = WhatIfSimulator(dashboard)
    api_extension = APIGatewayExtension(dashboard, generator, what_if)
    return {
        "dashboard": dashboard,
        "explanation_generator": generator,
        "what_if": what_if,
        "api_extension": api_extension,
    }


# ============================================================================
# 7. EXAMPLE USAGE (if run directly)
# ============================================================================
if __name__ == "__main__":
    # Quick test: create a mock request log
    components = create_explainable_ui()
    dash = components["dashboard"]
    gen = components["explanation_generator"]

    # Create a dummy profile
    prof = SustainabilityAwareExpertProfile("expert_A")
    prof.energy_per_inference_full = 2.5  # J
    prof.accuracy_full = 0.92
    prof.compressed_flag = False

    alt_prof = SustainabilityAwareExpertProfile("expert_B")
    alt_prof.energy_per_inference_full = 3.8
    alt_prof.accuracy_full = 0.94

    req = RequestLog(
        request_id="test-123",
        timestamp=datetime.now(),
        query="What is the weather?",
        chosen_expert_id="expert_A",
        chosen_expert_profile=prof,
        alternative_experts=[("expert_B", alt_prof)],
        latency_ms=120.0,
        energy_joules=2.5,
        co2_kg=2.5 * 0.2 / 3600000,
        accuracy=0.92,
    )
    dash.log_request(req)

    # Generate explanation
    explanation = gen.generate(req, prof, [("expert_B", alt_prof)])
    print("Explanation:", explanation)

    # Simulate what‑if
    simulator = components["what_if"]
    result = simulator.simulate("test-123", "expert_B")
    print("What‑if result:", result)

    print("✅ Explainable UI module ready.")
